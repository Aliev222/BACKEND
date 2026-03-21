from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
import asyncio
import uvicorn
import random
import time
import json
import os
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from sqlalchemy import select
from DATABASE.base import User, AsyncSessionLocal
from collections import defaultdict, deque
from dataclasses import dataclass
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from DATABASE.base import (
    get_user, add_user as create_user, update_user,
    init_db, get_completed_tasks, add_completed_task
)
from schemas import (
    BoostActivateRequest,
    ClicksBatchRequest,
    EnergySyncRequest,
    GameRequest,
    PassiveIncomeRequest,
    RegisterRequest,
    RewardVideoClaimRequest,
    RewardVideoStartRequest,
    SkinRequest,
    TaskCompleteRequest,
    TournamentData,
    UpgradeRequest,
    UserIdRequest,
)
from core.game_config import (
    BASE_MAX_ENERGY,
    CLICK_BURST_ALLOWANCE,
    CLICK_BUFFER_KEY,
    CLICK_FLUSH_INTERVAL,
    ENERGY_REGEN_SECONDS,
    HOUR_VALUES,
    MAX_BET,
    MAX_CLICK_BATCH_SIZE,
    MAX_REAL_CLICKS_PER_SECOND,
    MAX_REWARD_PER_VIDEO,
    MIN_BET,
    RATE_LIMITS,
    TOURNAMENT_KEY,
    TOURNAMENT_PRIZE_POOL,
    UPGRADE_PRICES,
    USER_CACHE_PREFIX,
    USER_CACHE_TTL,
)
from core.game_logic import (
    build_energy_payload,
    calculate_current_energy,
    get_allowed_clicks,
    get_hour_value,
    get_max_energy,
    get_tap_value,
    mask_username,
    normalize_dt,
)
from core.telegram_auth import verify_telegram_init_data


REDIS_URL = os.getenv("REDIS_URL")
redis_client = None
LOCAL_LOCKS: dict[str, float] = {}
LOCAL_IDEMPOTENCY_KEYS: dict[str, float] = {}
LOCAL_RATE_LIMITS_STATE: dict[str, deque[float]] = defaultdict(deque)
# Single lightweight reconnect helper to avoid code duplication
async def try_reconnect_redis() -> None:
    global redis_client
    if not REDIS_URL or redis_client is not None:
        return
    client = redis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        socket_timeout=2,
        socket_connect_timeout=2,
        retry_on_timeout=True,
    )
    try:
        await client.ping()
        redis_client = client
        logger.info("âœ“ Redis reconnected")
    except Exception as e:
        logger.warning(f"Redis reconnect failed: {e}")
        redis_client = None


async def get_redis_or_none() -> redis.Redis | None:
    """
    Best-effort Redis with single reconnect attempt. No exceptions.
    """
    if redis_client is None:
        await try_reconnect_redis()
    return redis_client


async def redis_or_503() -> redis.Redis:
    """
    Strong guarantee: return redis connection or raise 503.
    """
    conn = await get_redis_or_none()
    if conn is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return conn


# ==================== METRICS ====================
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
REDIS_ERRORS = Counter("redis_errors_total", "Redis operation errors")
DB_ERRORS = Counter("db_errors_total", "Database operation errors")
RATE_LIMIT_REJECTS = Counter(
    "rate_limit_rejects_total",
    "Rate-limit rejections",
    ["namespace"],
)
# ==================== LOGGING ====================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def require_telegram_user(request: Request, expected_user_id: int | None = None) -> dict:
    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )

    if expected_user_id is not None and int(telegram_user.get("id", 0)) != int(expected_user_id):
        raise HTTPException(status_code=403, detail="Telegram user mismatch")

    return telegram_user




# ==================== Ğ¢Ğ£Ğ ĞĞ˜Ğ ĞĞ«Ğ• Ğ”ĞĞĞĞ«Ğ• ==================

async def get_user_cached(user_id: int) -> dict | None:
    conn = await get_redis_or_none()
    if conn:
        cached = await conn.get(f"{USER_CACHE_PREFIX}{user_id}")
        if cached:
            try:
                return json.loads(cached)
            except Exception:
                pass

    user = await get_user(user_id)
    if not user:
        return None

    conn = await get_redis_or_none()
    if conn:
        await conn.setex(
            f"{USER_CACHE_PREFIX}{user_id}",
            USER_CACHE_TTL,
            json.dumps(user, default=str)
        )

    return user

# ==================== Ğ’ÑĞ¿Ğ¾Ğ¼Ğ¾Ğ³Ğ°Ñ‚ĞµĞ»ÑŒĞ½Ñ‹Ğµ Ñ„ÑƒĞ½ĞºÑ†Ğ¸Ğ¸ Ğ°Ğ½Ñ‚Ğ¸ÑĞ¿Ğ°Ğ¼Ğ° ====================


async def invalidate_user_cache(user_id: int):
    conn = await get_redis_or_none()
    if conn:
        await conn.delete(f"{USER_CACHE_PREFIX}{user_id}")




async def reset_tournament_loop():
    while True:
        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_seconds = max(1, int((tomorrow - now).total_seconds()))

        await asyncio.sleep(sleep_seconds)

        try:
            redis_conn = await get_redis_or_none()
            if redis_conn:
                await redis_conn.delete(TOURNAMENT_KEY)
                logger.info("ğŸ† Tournament leaderboard reset")
        except Exception as e:
            logger.error(f"Error resetting tournament leaderboard: {e}")



# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client

    logger.info("ğŸš€ Starting Ryoho Clicker API")

    await init_db()
    logger.info("âœ… Database initialized")

    if REDIS_URL:
        redis_client = redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry_on_timeout=True,
        )
        try:
            await redis_client.ping()
            logger.info("âœ… Redis connected")
        except Exception as e:
            logger.error(f"âŒ Redis connection failed: {e}")
            redis_client = None
    else:
        logger.warning("âš ï¸ REDIS_URL is not set")

    if redis_client:
        asyncio.create_task(reset_tournament_loop())
        asyncio.create_task(flush_click_buffer_loop())

    logger.info("âœ… Background tasks started")
    yield

    if redis_client:
        await redis_client.close()

    logger.info("ğŸ›‘ Shutting down")

# ==================== CORS ====================
app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://spirix.vercel.app",
        "https://web.telegram.org",
        "https://telegram.org",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    path = request.url.path
    method = request.method
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration = time.perf_counter() - start
        HTTP_REQUESTS_TOTAL.labels(method=method, path=path, status=str(status_code)).inc()
        HTTP_REQUEST_DURATION.labels(method=method, path=path).observe(duration)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ==================== ĞœĞĞ”Ğ•Ğ›Ğ˜ ====================

# ==================== Ğ’Ğ¡ĞŸĞĞœĞĞ“ĞĞ¢Ğ•Ğ›Ğ¬ĞĞ«Ğ• Ğ¤Ğ£ĞĞšĞ¦Ğ˜Ğ˜ ====================
async def acquire_once_lock(key: str, ttl: int = 10) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            result = await conn.set(key, "1", ex=ttl, nx=True)
            return bool(result)
        except Exception as e:
            logger.warning(f"Redis acquire_once_lock failed, fallback to local: {e}")

    now = time.monotonic()
    expires_at = LOCAL_LOCKS.get(key)
    if expires_at and expires_at > now:
        return False

    LOCAL_LOCKS[key] = now + ttl
    return True

async def acquire_idempotency_key(key: str, ttl: int = 60) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            result = await conn.set(key, "1", ex=ttl, nx=True)
            return bool(result)
        except Exception as e:
            logger.warning(f"Redis acquire_idempotency_key failed, fallback to local: {e}")

    now = time.monotonic()
    expires_at = LOCAL_IDEMPOTENCY_KEYS.get(key)
    if expires_at and expires_at > now:
        return False

    LOCAL_IDEMPOTENCY_KEYS[key] = now + ttl
    return True


async def require_user_action_lock(namespace: str, user_id: int, ttl: int = 5):
    lock_key = f"lock:{namespace}:{user_id}"
    locked = await acquire_once_lock(lock_key, ttl=ttl)
    if not locked:
        raise HTTPException(status_code=429, detail="Action already in progress")


async def ensure_redis_available() -> redis.Redis:
    """
    Try reconnect once and guarantee redis_client or raise 503.
    """
    return await redis_or_503()

# ==================== Ğ­ĞĞ”ĞŸĞĞ˜ĞĞ¢Ğ« ====================

def _local_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    now = time.monotonic()
    bucket = LOCAL_RATE_LIMITS_STATE[key]

    while bucket and (now - bucket[0]) >= window_seconds:
        bucket.popleft()

    if len(bucket) >= limit:
        return False

    bucket.append(now)
    return True


async def redis_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """
    True = Ğ¼Ğ¾Ğ¶Ğ½Ğ¾ Ğ¿Ñ€Ğ¾Ğ¿ÑƒÑÑ‚Ğ¸Ñ‚ÑŒ
    False = Ğ»Ğ¸Ğ¼Ğ¸Ñ‚ Ğ¿Ñ€ĞµĞ²Ñ‹ÑˆĞµĞ½
    """
    global redis_client

    conn = await get_redis_or_none()

    if conn is None:
        return _local_rate_limit(key, limit, window_seconds)

    try:
        current = await conn.incr(key)
        if current == 1:
            await conn.expire(key, window_seconds)
        return current <= limit
    except Exception as e:
        logger.warning(f"Redis rate_limit failed, fallback to local: {e}")
        REDIS_ERRORS.inc()
        redis_client = None
        return _local_rate_limit(key, limit, window_seconds)


async def require_redis_rate_limit(namespace: str, user_id: int, limit: int, window_seconds: int):
    allowed = await redis_rate_limit(f"rl:{namespace}:{user_id}", limit, window_seconds)
    if not allowed:
        RATE_LIMIT_REJECTS.labels(namespace=namespace).inc()
        raise HTTPException(status_code=429, detail="Too many requests")


async def flush_click_buffer_loop():
    while True:
        try:
            conn = await get_redis_or_none()
            if not conn:
                await asyncio.sleep(5)
                continue

            data = await conn.hgetall(CLICK_BUFFER_KEY)

            if not data:
                await asyncio.sleep(5)
                continue

            for user_id, coins in data.items():
                user_id = int(user_id)
                coins = int(coins)

                if coins <= 0:
                    continue

                user = await get_user(user_id)
                if not user:
                    continue

                new_coins = int(user.get("coins", 0)) + coins

                await update_user(user_id, {
                    "coins": new_coins
                })

                await invalidate_user_cache(user_id)

            await conn.delete(CLICK_BUFFER_KEY)

            logger.info(f"Flushed {len(data)} users from Redis buffer")

        except Exception as e:
            logger.error(f"Flush error: {e}")

        await asyncio.sleep(5)


@app.get("/health")
async def health():
    details: dict[str, Any] = {}
    overall = "ok"

    # Redis check
    redis_status = "skipped"
    if REDIS_URL:
        try:
            conn = await get_redis_or_none()
            if conn:
                await asyncio.wait_for(conn.ping(), timeout=0.5)
                redis_status = "ok"
            else:
                redis_status = "unavailable"
        except Exception as e:
            redis_status = f"error: {e}"
            logger.warning(f"Health redis check failed: {e}")
            REDIS_ERRORS.inc()
    details["redis"] = redis_status

    # DB check
    db_status = "ok"
    try:
        async with AsyncSessionLocal() as session:
            await asyncio.wait_for(session.execute(select(1)), timeout=0.5)
    except Exception as e:
        db_status = f"error: {e}"
        logger.warning(f"Health db check failed: {e}")
        DB_ERRORS.inc()
    details["db"] = db_status

    if any(s != "ok" and not str(s).startswith("skipped") for s in details.values()):
        overall = "degraded"

    return {"status": overall, "details": details}

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        current_energy = calculate_current_energy(user, now)
        max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))
        

        return {
            "user_id": user["user_id"],
            "username": user.get("username"),
            "coins": user.get("coins", 0),
            "energy": current_energy,
            "max_energy": max_energy,
            "profit_per_tap": user.get("profit_per_tap", 1),
            "profit_per_hour": user.get("profit_per_hour", 100),
            "multitap_level": user.get("multitap_level", 0),
            "profit_level": user.get("profit_level", 0),
            "energy_level": user.get("energy_level", 0),
            "owned_skins": (user.get("extra_data", {}) or {}).get("owned_skins", ["default_SP"]),
            "selected_skin": (user.get("extra_data", {}) or {}).get("selected_skin", "default_SP"),
            "ads_watched": (user.get("extra_data", {}) or {}).get("ads_watched", 0),
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int, request: Request):
    """Get mega boost status"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now > expires:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
                    await invalidate_user_cache(user_id)
                    return {"active": False}
                else:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "active": True, 
                        "expires_at": active_boosts["mega_boost"]["expires_at"], 
                        "remaining_seconds": remaining
                    }
            except:
                pass
        
        return {"active": False}
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/activate-mega-boost")
async def activate_mega_boost(payload: BoostActivateRequest, request: Request):
    """Activate mega boost (x2 coins + infinite energy for 5 minutes)"""
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)
        await require_redis_rate_limit("activate_mega_boost", payload.user_id, 10, 60)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")


        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        # ĞŸÑ€Ğ¾Ğ²ĞµÑ€ÑĞµĞ¼, Ğ½Ğµ Ğ°ĞºÑ‚Ğ¸Ğ²ĞµĞ½ Ğ»Ğ¸ ÑƒĞ¶Ğµ Ğ±ÑƒÑÑ‚
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now < expires:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "success": False,
                        "message": f"Boost already active! {remaining // 60}:{remaining % 60:02d} remaining",
                        "already_active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"]
                    }
            except:
                del active_boosts["mega_boost"]
        
        # ĞĞºÑ‚Ğ¸Ğ²Ğ¸Ñ€ÑƒĞµĞ¼ Ğ½Ğ° 5 Ğ¼Ğ¸Ğ½ÑƒÑ‚
        expires_at = (now + timedelta(minutes=5)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["active_boosts"] = active_boosts
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        
        return {
            "success": True,
            "message": "ğŸ”¥âš¡ MEGA BOOST activated for 5 minutes! x2 coins + infinite energy",
            "expires_at": expires_at
        }
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video")
async def reward_video(payload: RewardVideoClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_redis_rate_limit("reward_video", payload.user_id, 5, 60)

        redis_conn = await ensure_redis_available()

        lock_key = f"lock:reward_video:{payload.user_id}"
        locked = await acquire_once_lock(lock_key, ttl=15)
        if not locked:
            raise HTTPException(status_code=429, detail="Reward already being processed")

        session_key = f"adsession:reward:{payload.ad_session_id}"
        raw = await redis_conn.get(session_key)
        if not raw:
            raise HTTPException(status_code=400, detail="Invalid or expired ad session")

        session = json.loads(raw)

        if int(session.get("user_id")) != payload.user_id:
            raise HTTPException(status_code=400, detail="Ad session does not belong to user")

        if session.get("claimed") is True:
            raise HTTPException(status_code=409, detail="Reward already claimed")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        reward = 5000
        new_coins = int(user.get("coins", 0)) + reward

        extra = user.get("extra_data", {}) or {}
        if not isinstance(extra, dict):
            extra = {}

        extra["ads_watched"] = int(extra.get("ads_watched", 0)) + 1

        await update_user(payload.user_id, {
            "coins": new_coins,
            "extra_data": extra
        })
        await invalidate_user_cache(payload.user_id)

        session["claimed"] = True
        await redis_conn.setex(session_key, 120, json.dumps(session))

        return {
            "success": True,
            "coins": new_coins,
            "ads_watched": extra["ads_watched"]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reward_video: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video/start")
async def reward_video_start(payload: RewardVideoStartRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_redis_rate_limit("reward_video_start", payload.user_id, 10, 60)

        redis_conn = await ensure_redis_available()

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        ad_session_id = f"{payload.user_id}:{int(time.time())}:{random.randint(100000, 999999)}"
        key = f"adsession:reward:{ad_session_id}"

        await redis_conn.setex(
            key,
            120,
            json.dumps({
                "user_id": payload.user_id,
                "claimed": False,
                "created_at": time.time()
            })
        )

        return {
            "success": True,
            "ad_session_id": ad_session_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reward_video_start: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/ad-watched")
async def ad_watched(payload: dict, request: Request):
    """Track ad watch statistics"""
    try:
        user_id = payload.get("user_id")
        reward_type = payload.get("reward_type")
        await require_telegram_user(request, user_id)
        await require_user_action_lock("ad_watched", user_id, ttl=3)
        
        user = await get_user_cached(user_id)
        if not user:
            return {"success": False}
        
        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}
        
        # Ğ¡Ğ¾Ñ…Ñ€Ğ°Ğ½ÑĞµĞ¼ ÑÑ‚Ğ°Ñ‚Ğ¸ÑÑ‚Ğ¸ĞºÑƒ
        ads_history = extra.get("ads_history", [])
        ads_history.append({
            "type": reward_type,
            "timestamp": datetime.utcnow().isoformat()
        })
        extra["ads_history"] = ads_history[-50:]
        
        await update_user(user_id, {"extra_data": extra})
        await invalidate_user_cache(user_id)
        
        return {"success": True}
        
    except Exception as e:
        logger.error(f"Error in ad_watched: {e}")
        return {"success": False}

@app.post("/api/ads/increment")
async def increment_ads_watched(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        await require_redis_rate_limit("ads_increment", payload.user_id, 20, 60)

        lock_key = f"lock:ads_increment:{payload.user_id}"
        locked = await acquire_once_lock(lock_key, ttl=5)
        if not locked:
            raise HTTPException(status_code=429, detail="Ad already being processed")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        ads_watched = int(extra.get("ads_watched", 0)) + 1
        extra["ads_watched"] = ads_watched

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "ads_watched": ads_watched
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in increment_ads_watched: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/upgrade")
async def process_upgrade(payload: UpgradeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("upgrade", payload.user_id, ttl=3)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        boost_type = payload.boost_type
        if boost_type not in UPGRADE_PRICES:
            raise HTTPException(status_code=400, detail="Invalid boost type")
        current_level = user.get(f"{boost_type}_level", 0)
        
        if current_level >= len(UPGRADE_PRICES[boost_type]):
            raise HTTPException(status_code=400, detail="Max level reached")
        
        price = UPGRADE_PRICES[boost_type][current_level]
        if user.get("coins", 0) < price:
            raise HTTPException(status_code=400, detail="Not enough coins")

        user["coins"] -= price
        user[f"{boost_type}_level"] = current_level + 1
        updates = {"coins": user["coins"], f"{boost_type}_level": current_level + 1}

        if boost_type == "energy":
            new_max = get_max_energy(current_level + 1)
            updates["max_energy"] = new_max
            updates["energy"] = new_max

        await update_user(payload.user_id, updates)
        await invalidate_user_cache(payload.user_id)
        
        # ĞĞ±Ğ½Ğ¾Ğ²Ğ»ÑĞµĞ¼ ĞºÑÑˆ
        
        return {
            "success": True,
            "coins": user["coins"],
            "new_level": current_level + 1,
            "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] 
                if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
            "profit_per_tap": get_tap_value(user.get("multitap_level", 0) + 
                (1 if boost_type == "multitap" else 0)),
            "profit_per_hour": get_hour_value(user.get("profit_level", 0) + 
                (1 if boost_type == "profit" else 0)),
            "max_energy": get_max_energy(user.get("energy_level", 0) + 
                (1 if boost_type == "energy" else 0))
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/update-energy")
async def update_energy(payload: dict, request: Request):
    try:
        user_id = payload.get("user_id")
        await require_telegram_user(request, user_id)
        await require_redis_rate_limit("update_energy", user_id, 10, 60)
        await require_user_action_lock("update_energy", user_id, ttl=3)
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id required")
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        max_energy = int(user.get("max_energy", 500))

        await update_user(user_id, {
            "energy": max_energy,
            "last_energy_update": now
        })
        await invalidate_user_cache(user_id)

        return {
            "success": True,
            "energy": max_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/recover-energy")
async def recover_energy_legacy(payload: UserIdRequest, request: Request):
    """Ğ¡Ñ‚Ğ°Ñ€Ñ‹Ğ¹ ÑĞ½Ğ´Ğ¿Ğ¾Ğ¸Ğ½Ñ‚ Ğ´Ğ»Ñ Ğ¾Ğ±Ñ€Ğ°Ñ‚Ğ½Ğ¾Ğ¹ ÑĞ¾Ğ²Ğ¼ĞµÑÑ‚Ğ¸Ğ¼Ğ¾ÑÑ‚Ğ¸"""
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("recover_energy", payload.user_id, ttl=3)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        max_energy = user.get("max_energy", BASE_MAX_ENERGY)
        current_energy = user.get("energy", 0)
        
       
        
        if current_energy < max_energy:
            new_energy = min(max_energy, current_energy + 3)
            
            await update_user(payload.user_id, {
                "energy": new_energy,
                "last_energy_update": datetime.utcnow()
            })
            await invalidate_user_cache(payload.user_id)
            
            
            
            
            return {"energy": new_energy}
        
        return {"energy": current_energy}
    except Exception as e:
       
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/sync-energy")
async def sync_energy(payload: EnergySyncRequest, request: Request):
    """Ğ¡ĞµÑ€Ğ²ĞµÑ€Ğ½Ñ‹Ğ¹ sync ÑĞ½ĞµÑ€Ğ³Ğ¸Ğ¸ Ğ±ĞµĞ· ÑĞ±Ñ€Ğ¾ÑĞ° Ñ‚Ğ°Ğ¹Ğ¼ĞµÑ€Ğ° Ñ€ĞµĞ³ĞµĞ½Ğ°."""
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()

        old_energy = int(user.get("energy", 0))
        max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))
        last_update = normalize_dt(user.get("last_energy_update"))

        current_energy = calculate_current_energy(user, now)

        update_data = {}

        # ĞĞ±Ğ½Ğ¾Ğ²Ğ»ÑĞµĞ¼ baseline Ñ‚Ğ¾Ğ»ÑŒĞºĞ¾ ĞµÑĞ»Ğ¸ ÑĞ½ĞµÑ€Ğ³Ğ¸Ñ Ñ€ĞµĞ°Ğ»ÑŒĞ½Ğ¾ Ğ²Ñ‹Ñ€Ğ¾ÑĞ»Ğ°
        if current_energy != old_energy:
            update_data["energy"] = current_energy

            if last_update:
                seconds_passed = max(0, int((now - last_update).total_seconds()))
                gained = seconds_passed // ENERGY_REGEN_SECONDS

                if gained > 0:
                    update_data["last_energy_update"] = last_update + timedelta(
                        seconds=gained * ENERGY_REGEN_SECONDS
                    )
            else:
                update_data["last_energy_update"] = now

        # Ğ•ÑĞ»Ğ¸ ÑĞ½ĞµÑ€Ğ³Ğ¸Ñ ÑƒĞ¶Ğµ Ğ¿Ğ¾Ğ»Ğ½Ğ°Ñ, Ğ´ĞµÑ€Ğ¶Ğ¸Ğ¼ baseline ĞºĞ¾Ğ½ÑĞ¸ÑÑ‚ĞµĞ½Ñ‚Ğ½Ñ‹Ğ¼
        if current_energy >= max_energy and not update_data.get("last_energy_update"):
            update_data["last_energy_update"] = now
            update_data["energy"] = max_energy

        if update_data:
            await update_user(payload.user_id, update_data)
            await invalidate_user_cache(payload.user_id)

            

        return {
            "success": True,
            "energy": current_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in sync_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

SKIN_MULTIPLIERS = {
    "default_SP": 1.0,

    "skin_lvl_1": 1.1,
    "skin_lvl_2": 1.2,
    "skin_lvl_3": 1.3,
    "skin_lvl_4": 1.4,
    "skin_lvl_5": 1.5,
    "skin_lvl_6": 1.6,
    "skin_lvl_7": 2.0,

    "skin_video_1": 1.2,
    "skin_video_2": 1.3,
    "skin_video_3": 1.4,
    "skin_video_4": 1.5,
    "skin_video_5": 1.75,
    "skin_video_6": 2.0,

    "skin_friend_1": 1.1,
    "skin_friend_2": 1.2,
    "skin_friend_3": 1.3,
    "skin_friend_4": 1.5,
    "skin_friend_5": 1.75,
    "skin_friend_6": 2.0,
}


def get_selected_skin_multiplier(user: dict) -> float:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    selected_skin = extra.get("selected_skin", "default_SP")
    return SKIN_MULTIPLIERS.get(selected_skin, 1.0)


def is_mega_boost_active(user: dict) -> bool:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("mega_boost")
    if not boost:
        return False

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False

    try:
        expires_dt = datetime.fromisoformat(expires_at)
        return datetime.utcnow() < expires_dt
    except Exception:
        return False

SKIN_REQUIREMENTS = {
    "skin_lvl_1": {"type": "level", "value": 1},
    "skin_lvl_2": {"type": "level", "value": 10},
    "skin_lvl_3": {"type": "level", "value": 25},
    "skin_lvl_4": {"type": "level", "value": 50},
    "skin_lvl_5": {"type": "level", "value": 75},
    "skin_lvl_6": {"type": "level", "value": 100},
    "skin_lvl_7": {"type": "level", "value": 150},

    "skin_video_1": {"type": "ads", "count": 1},
    "skin_video_2": {"type": "ads", "count": 5},
    "skin_video_3": {"type": "ads", "count": 10},
    "skin_video_4": {"type": "ads", "count": 20},
    "skin_video_5": {"type": "ads", "count": 50},
    "skin_video_6": {"type": "ads", "count": 100},

    "skin_friend_1": {"type": "friends", "count": 1},
    "skin_friend_2": {"type": "friends", "count": 3},
    "skin_friend_3": {"type": "friends", "count": 5},
    "skin_friend_4": {"type": "friends", "count": 10},
    "skin_friend_5": {"type": "friends", "count": 20},
    "skin_friend_6": {"type": "friends", "count": 50},
}


async def can_unlock_skin(user: dict, skin_id: str) -> bool:
    req = SKIN_REQUIREMENTS.get(skin_id)
    if not req:
        return False

    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    if req["type"] == "level":
        level = int(user.get("multitap_level", 0))
        return level >= int(req["value"])

    if req["type"] == "ads":
        ads_watched = int(extra.get("ads_watched", 0))
        return ads_watched >= int(req["count"])

    if req["type"] == "friends":
        referral_count = int(user.get("referral_count", 0))
        return referral_count >= int(req["count"])

    return False

@app.post("/api/clicks")
async def process_clicks_batch(payload: ClicksBatchRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)
        
        if payload.clicks > MAX_CLICK_BATCH_SIZE:
            raise HTTPException(status_code=400, detail="Too many clicks in batch")

        batch_key = f"idem:clicks:{payload.user_id}:{payload.batch_id}"
        is_new_batch = await acquire_idempotency_key(batch_key, ttl=120)
        if not is_new_batch:
            logger.warning(f"Duplicate click batch rejected: user={payload.user_id}, batch_id={payload.batch_id}")
            raise HTTPException(status_code=409, detail="Duplicate batch")


        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()

        max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))
        current_energy = calculate_current_energy(user, now)

        multitap_level = int(user.get("multitap_level", 0))
        tap_value = get_tap_value(multitap_level)

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        selected_skin = extra.get("selected_skin", "default_SP")
        skin_multiplier = float(SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = is_mega_boost_active(user)

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2

        # Ğ·Ğ°Ñ‰Ğ¸Ñ‚Ğ°
        safe_requested_clicks = min(payload.clicks, MAX_CLICK_BATCH_SIZE)
        allowed_clicks = get_allowed_clicks(user, now, safe_requested_clicks)

        effective_clicks = min(allowed_clicks, current_energy)
        gained = effective_clicks * coin_per_tap

        # Ğ½Ğ¾Ğ²Ñ‹Ğµ Ğ·Ğ½Ğ°Ñ‡ĞµĞ½Ğ¸Ñ
        new_energy = max(0, current_energy - effective_clicks)
        new_coins = int(user.get("coins", 0)) + gained

        # Ğ¡Ğ¾Ñ…Ñ€Ğ°Ğ½ÑĞµĞ¼ ÑĞ½ĞµÑ€Ğ³Ğ¸Ñ Ğ¸ Ğ±Ğ°Ğ»Ğ°Ğ½Ñ Ğ¾Ğ´Ğ½Ğ¸Ğ¼ server-side update Ğ½Ğ° Ğ±Ğ°Ñ‚Ñ‡ ĞºĞ»Ğ¸ĞºĞ¾Ğ².
        await update_user(payload.user_id, {
            "coins": new_coins,
            "energy": new_energy,
            "last_energy_update": now
        })

        conn = await get_redis_or_none()
        if conn and gained > 0:
            # Ğ¢ÑƒÑ€Ğ½Ğ¸Ñ€ Ğ¾ÑÑ‚Ğ°Ğ²Ğ»ÑĞµĞ¼ Ğ² Redis ĞºĞ°Ğº Ğ±Ñ‹ÑÑ‚Ñ€Ñ‹Ğ¹ leaderboard ÑĞ»Ğ¾Ğ¹.
            await conn.zincrby(
                TOURNAMENT_KEY,
                gained,
                str(payload.user_id)
            )

        # âœ… Ğ¸Ğ½Ğ²Ğ°Ğ»Ğ¸Ğ´Ğ¸Ñ€ÑƒĞµĞ¼ ĞºÑÑˆ
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "coins": new_coins,
            "energy": new_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "gained": gained,
            "effective_clicks": effective_clicks,
            "coin_per_tap": coin_per_tap,
            "profit_per_tap": tap_value,
            "profit_per_hour": get_hour_value(int(user.get("profit_level", 0))),
            "mega_boost_active": mega_boost_active
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_clicks_batch: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        prices = {}
        for boost in UPGRADE_PRICES:
            level = user.get(f"{boost}_level", 0)
            prices[boost] = UPGRADE_PRICES[boost][level] if level < len(UPGRADE_PRICES[boost]) else 0
        
        return prices
    except Exception as e:
        logger.error(f"Error in get_upgrade_prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/register")
async def register_user(payload: RegisterRequest, request: Request):
    try:
        telegram_user = await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("register", payload.user_id, ttl=5)
        existing = await get_user(payload.user_id)
        if existing:
            username = telegram_user.get("username") or payload.username
            if username and username != existing.get("username"):
                await update_user(payload.user_id, {"username": username})
                await invalidate_user_cache(payload.user_id)
            return {"status": "exists", "user": existing}

        await create_user(
            user_id=payload.user_id,
            username=telegram_user.get("username") or payload.username,
            referrer_id=payload.referrer_id
        )

        created_user = await get_user_cached(payload.user_id)
        return {"status": "created", "user": created_user}
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")




# ==================== REFERRALS ====================

@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int, request: Request):
    """Get referral statistics"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "count": user.get("referral_count", 0),
            "earnings": user.get("referral_earnings", 0)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== ĞœĞ˜ĞĞ˜-Ğ˜Ğ“Ğ Ğ« ====================

@app.post("/api/game/coinflip")
async def play_coinflip(payload: GameRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:coinflip", payload.user_id, ttl=3)
        await require_redis_rate_limit("game_action", payload.user_id, 30, 60)
        
        user = await get_user_cached(payload.user_id)
        if not user or user.get("coins", 0) < payload.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")

        win = random.choice([True, False])
        if win:
            user["coins"] += payload.bet
            message = f"ğŸ‰ You won +{payload.bet} coins!"
        else:
            user["coins"] -= payload.bet
            message = f"ğŸ˜ You lost {payload.bet} coins"

        await update_user(payload.user_id, {"coins": user["coins"]})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "coins": user["coins"], "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/slots")
async def play_slots(payload: GameRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:slots", payload.user_id, ttl=3)
        await require_redis_rate_limit("game_action", payload.user_id, 30, 60)

        user = await get_user_cached(payload.user_id)
        if not user or user.get("coins", 0) < payload.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")

        symbols = ["ğŸ’", "ğŸ‹", "ğŸŠ", "7ï¸âƒ£", "ğŸ’"]
        slots = [random.choice(symbols) for _ in range(3)]
        win = len(set(slots)) == 1
        multiplier = 10 if "7ï¸âƒ£" in slots and win else 5 if "ğŸ’" in slots and win else 3

        if win:
            win_amount = payload.bet * multiplier
            user["coins"] += win_amount
            message = f"ğŸ° JACKPOT! +{win_amount} coins!"
        else:
            user["coins"] -= payload.bet
            message = f"ğŸ˜ You lost {payload.bet} coins"

        await update_user(payload.user_id, {"coins": user["coins"]})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "coins": user["coins"], "slots": slots, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/dice")
async def play_dice(payload: GameRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:dice", payload.user_id, ttl=3)
        await require_redis_rate_limit("game_action", payload.user_id, 30, 60)

        user = await get_user_cached(payload.user_id)
        if not user or user.get("coins", 0) < payload.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")

        dice1 = random.randint(1, 6)
        dice2 = random.randint(1, 6)
        total = dice1 + dice2
        win = False
        multiplier = 1

        if payload.prediction == "7" and total == 7:
            win = True
            multiplier = 5
        elif payload.prediction == "even" and total % 2 == 0:
            win = True
            multiplier = 2
        elif payload.prediction == "odd" and total % 2 == 1:
            win = True
            multiplier = 2

        if win:
            win_amount = payload.bet * multiplier
            user["coins"] += win_amount
            message = f"ğŸ² You won +{win_amount} coins!"
        else:
            user["coins"] -= payload.bet
            message = f"ğŸ˜ You lost {payload.bet} coins"

        await update_user(payload.user_id, {"coins": user["coins"]})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "coins": user["coins"],
            "dice1": dice1,
            "dice2": dice2,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/game/roulette")
async def play_roulette(payload: GameRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:roulette", payload.user_id, ttl=3)
        await require_redis_rate_limit("game_action", payload.user_id, 30, 60)

        user = await get_user_cached(payload.user_id)
        if not user or user.get("coins", 0) < payload.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")

        red_numbers = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]
        result = random.randint(0, 36)

        if result == 0:
            result_color = 'green'
            result_symbol = 'ğŸŸ¢'
        elif result in red_numbers:
            result_color = 'red'
            result_symbol = 'ğŸ”´'
        else:
            result_color = 'black'
            result_symbol = 'âš«'

        win = False
        multiplier = 0

        if payload.bet_type == 'number' and payload.bet_value == result:
            win = True
            multiplier = 35
        elif payload.bet_type == 'green' and result_color == 'green':
            win = True
            multiplier = 35
        elif payload.bet_type == result_color:
            win = True
            multiplier = 2

        if win:
            win_amount = payload.bet * multiplier
            user["coins"] += win_amount
            message = f"ğŸ‰ {result_symbol} {result} - You won +{win_amount} coins! (x{multiplier})"
        else:
            user["coins"] -= payload.bet
            message = f"ğŸ˜ {result_symbol} {result} - You lost {payload.bet} coins"

        await update_user(payload.user_id, {"coins": user["coins"]})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "coins": user["coins"],
            "result_number": result,
            "result_color": result_color,
            "result_symbol": result_symbol,
            "win": win,
            "message": message
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
# ==================== TOURNAMENT ENDPOINTS ====================

@app.get("/api/tournament/leaderboard")
async def get_tournament_leaderboard():
    """Get top 5 players from Redis leaderboard"""
    try:
        players = []

        conn = await get_redis_or_none()
        if conn:
            top_players = await conn.zrevrange(
                TOURNAMENT_KEY,
                0,
                4,
                withscores=True
            )

            for idx, (user_id_str, score) in enumerate(top_players):
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    continue

                user = await get_user_cached(user_id)

                username = user.get("username") if user else None
                avatar_url = (
                    f"https://t.me/i/userpic/320/{username}.jpg"
                    if username else "/imgg/default_avatar.png"
                )

                players.append({
                    "rank": idx + 1,
                    "user_id": user_id,
                    "name": mask_username(username),
                    "avatar": avatar_url,
                    "score": int(score)
                })

        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time_left = int((tomorrow - now).total_seconds())

        return {
            "success": True,
            "players": players,
            "prize_pool": TOURNAMENT_PRIZE_POOL,
            "time_left": time_left
        }

    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@app.get("/api/tournament/player-rank/{user_id}")
async def get_player_rank(user_id: int, request: Request):
    """Get player's rank from Redis leaderboard"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            return {
                "success": True,
                "rank": 0,
                "score": 0,
                "next_rank_score": 0,
                "avatar": "/imgg/default_avatar.png",
                "name": "Player"
            }

        username = user.get("username")
        avatar_url = (
            f"https://t.me/i/userpic/320/{username}.jpg"
            if username else "/imgg/default_avatar.png"
        )

        redis_conn = await ensure_redis_available()

        score = await redis_conn.zscore(TOURNAMENT_KEY, str(user_id))
        score = int(score) if score is not None else 0

        rev_rank = await redis_conn.zrevrank(TOURNAMENT_KEY, str(user_id))
        rank = (rev_rank + 1) if rev_rank is not None else 0

        next_rank_score = 0
        if rev_rank is not None and rev_rank > 0:
            higher_player = await redis_conn.zrevrange(
                TOURNAMENT_KEY,
                rev_rank - 1,
                rev_rank - 1,
                withscores=True
            )
            if higher_player:
                _, higher_score = higher_player[0]
                next_rank_score = max(0, int(higher_score) - score)

        return {
            "success": True,
            "rank": rank,
            "score": score,
            "next_rank_score": next_rank_score,
            "avatar": avatar_url,
            "name": mask_username(username)
        }

    except Exception as e:
        logger.error(f"Error getting player rank: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== Ğ—ĞĞ”ĞĞ§Ğ˜ ====================

_task_completion_store = {}

@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        completed_tasks = await get_completed_tasks(user_id) or []
        
        tasks = [
            {"id": "daily_bonus", "title": "ğŸ“… Daily Bonus", "description": "Come back every day", 
             "reward": "25000 coins", "icon": "ğŸ“…", "completed": "daily_bonus" in completed_tasks},
            {"id": "energy_refill", "title": "âš¡ Infinite Energy", "description": "5 minutes of unlimited energy", 
             "reward": "âš¡ 5 minutes", "icon": "âš¡", "completed": "energy_refill" in completed_tasks},
            {"id": "link_click", "title": "ğŸ”— Follow Link", "description": "Click the link and get reward", 
             "reward": "25000 coins", "icon": "ğŸ”—", "completed": False},
            {"id": "invite_5_friends", "title": "ğŸ‘¥ Invite 5 Friends", "description": "Invite 5 friends", 
             "reward": "20000 coins", "icon": "ğŸ‘¥", "completed": "invite_5_friends" in completed_tasks, 
             "progress": min(user.get("referral_count", 0), 5), "total": 5}
        ]
        return tasks
    except Exception as e:
        logger.error(f"Error in get_tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/complete-task")
async def complete_task(payload: TaskCompleteRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_redis_rate_limit("complete_task", payload.user_id, *RATE_LIMITS["complete_task"])
        await require_user_action_lock("complete_task", payload.user_id, ttl=5)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        task_id = payload.task_id
        
        if task_id == "link_click":
            user["coins"] += 25000
            await update_user(payload.user_id, {"coins": user["coins"]})
            await invalidate_user_cache(payload.user_id)
            
            return {"success": True, "message": "ğŸ”— +25000 coins!", "coins": user["coins"]}
        
        completed = await get_completed_tasks(payload.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")
        
        if task_id == "daily_bonus":
            user["coins"] += 25000
            await add_completed_task(payload.user_id, task_id)
            await update_user(payload.user_id, {"coins": user["coins"]})
            await invalidate_user_cache(payload.user_id)
            return {"success": True, "message": "ğŸ +25000 coins!", "coins": user["coins"]}
        
        elif task_id == "energy_refill":
            await add_completed_task(payload.user_id, task_id)
            return {"success": True, "message": "âš¡ Energy refill activated!"}
        
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] += 20000
                await add_completed_task(payload.user_id, task_id)
                await update_user(payload.user_id, {"coins": user["coins"]})
                await invalidate_user_cache(payload.user_id)
                return {"success": True, "message": "ğŸ‘¥ +20000 coins!", "coins": user["coins"]}
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")
        
        raise HTTPException(status_code=400, detail="Unknown task")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== ĞŸĞĞ¡Ğ¡Ğ˜Ğ’ĞĞ«Ğ™ Ğ”ĞĞ¥ĞĞ” ====================

@app.post("/api/passive-income")
async def passive_income(payload: PassiveIncomeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("passive_income", payload.user_id, ttl=5)
        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        last_income = user.get('last_passive_income')
        now = datetime.utcnow()
        
        # Ğ¡Ñ‡Ğ¸Ñ‚Ğ°ĞµĞ¼ Ñ‡Ğ°ÑÑ‹ Ñ Ğ¿Ğ¾ÑĞ»ĞµĞ´Ğ½ĞµĞ³Ğ¾ ÑĞ±Ğ¾Ñ€Ğ°
        if last_income:
            hours_passed = int((now - last_income).total_seconds() / 3600)
        else:
            hours_passed = 1
        
        # ĞĞ³Ñ€Ğ°Ğ½Ğ¸Ñ‡Ğ¸Ğ²Ğ°ĞµĞ¼ Ğ¼Ğ°ĞºÑĞ¸Ğ¼ÑƒĞ¼ 24 Ñ‡Ğ°ÑĞ°, Ñ‡Ñ‚Ğ¾Ğ±Ñ‹ Ğ½Ğµ Ğ½Ğ°Ñ‡Ğ¸ÑĞ»Ğ¸Ñ‚ÑŒ ÑĞ»Ğ¸ÑˆĞºĞ¾Ğ¼ Ğ¼Ğ½Ğ¾Ğ³Ğ¾
        hours_passed = min(hours_passed, 24)
        
        if hours_passed >= 1:
            hour_value = get_hour_value(user.get("profit_level", 0))
            total_income = hour_value * hours_passed
            
            user["coins"] += total_income
            await update_user(payload.user_id, {
                "coins": user["coins"],
                "last_passive_income": now
            })
            
            
            return {
                "success": True, 
                "coins": user["coins"], 
                "income": total_income, 
                "message": f"ğŸ’° +{total_income} coins Ğ·Ğ° {hours_passed}Ñ‡"
            }
        
        return {"success": True, "coins": user["coins"], "income": 0}
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== Ğ¡ĞšĞ˜ĞĞ« ====================

@app.get("/api/skins/list")
async def get_skins():
    skins = [
        # ========== ÇÀ ÓĞÎÂÅÍÜ (7 øò) ==========
        {
            "id": "default_SP",
            "name": "Classic Spirix",
            "image": "imgg/skins/default_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.0},
            "requirement": None,
        },
        {
            "id": "skin_lvl_1",
            "name": "Êîñìè÷åñêèé ñïèğèêñ",
            "image": "imgg/skins/Techno_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.1},
            "requirement": {"type": "level", "value": 10},
        },
        {
            "id": "skin_lvl_2",
            "name": "Èñêğÿùèéñÿ ñïèğèêñ",
            "image": "imgg/skins/Coin_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.2},
            "requirement": {"type": "level", "value": 20},
        },
        {
            "id": "skin_lvl_3",
            "name": "Çëàòîãëàçûé ñïèğèêñ",
            "image": "imgg/skins/Water_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.3},
            "requirement": {"type": "level", "value": 40},
        },
        {
            "id": "skin_lvl_4",
            "name": "Ôèëèíîâûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.4},
            "requirement": {"type": "level", "value": 60},
        },
        {
            "id": "skin_lvl_5",
            "name": "Èçóìğóäíûé ñïèğèêñ",
            "image": "imgg/skins/Galaxy_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.5},
            "requirement": {"type": "level", "value": 80},
        },
        {
            "id": "skin_lvl_6",
            "name": "Ñèíãóëÿğíûé ñïèğèêñ",
            "image": "imgg/skins/Ninja_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 1.6},
            "requirement": {"type": "level", "value": 100},
        },
        {
            "id": "skin_lvl_7",
            "name": "Áîæåñòâåííûé ñïèğèêñ",
            "image": "imgg/skins/Shadow_SP.png",
            "rarity": "common",
            "bonus": {"type": "multiplier", "value": 2.0},
            "requirement": {"type": "level", "value": 150},
        },
        # ========== ÇÀ ÂÈÄÅÎ (6 øò) ==========
        {
            "id": "skin_video_1",
            "name": "Çâåçäíûé ñïèğèêñ",
            "image": "imgg/skins/Techno_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.2},
            "requirement": {"type": "ads", "count": 1},
        },
        {
            "id": "skin_video_2",
            "name": "Êîñìè÷åñêèé ñïèğèêñ",
            "image": "imgg/skins/Water_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.3},
            "requirement": {"type": "ads", "count": 5},
        },
        {
            "id": "skin_video_3",
            "name": "Ãàëàêòè÷åñêèé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.4},
            "requirement": {"type": "ads", "count": 10},
        },
        {
            "id": "skin_video_4",
            "name": "Íåáåñíûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.5},
            "requirement": {"type": "ads", "count": 20},
        },
        {
            "id": "skin_video_5",
            "name": "Áîæåñòâåííûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "legendary",
            "bonus": {"type": "multiplier", "value": 1.75},
            "requirement": {"type": "ads", "count": 50},
        },
        {
            "id": "skin_video_6",
            "name": "Âñåìîãóùèé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "legendary",
            "bonus": {"type": "multiplier", "value": 2.0},
            "requirement": {"type": "ads", "count": 100},
        },
        # ========== ÇÀ ÄĞÓÇÅÉ (6 øò) ==========
        {
            "id": "skin_friend_1",
            "name": "Äğóæíûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.1},
            "requirement": {"type": "friends", "count": 1},
        },
        {
            "id": "skin_friend_2",
            "name": "Ïîïóëÿğíûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.2},
            "requirement": {"type": "friends", "count": 3},
        },
        {
            "id": "skin_friend_3",
            "name": "Èçâåñòíûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "rare",
            "bonus": {"type": "multiplier", "value": 1.3},
            "requirement": {"type": "friends", "count": 5},
        },
        {
            "id": "skin_friend_4",
            "name": "Çâåçäíûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "legendary",
            "bonus": {"type": "multiplier", "value": 1.5},
            "requirement": {"type": "friends", "count": 10},
        },
        {
            "id": "skin_friend_5",
            "name": "Ëåãåíäàğíûé ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "legendary",
            "bonus": {"type": "multiplier", "value": 1.75},
            "requirement": {"type": "friends", "count": 20},
        },
        {
            "id": "skin_friend_6",
            "name": "Èìïåğàòîğ ñïèğèêñ",
            "image": "imgg/skins/King_SP.png",
            "rarity": "super",
            "bonus": {"type": "multiplier", "value": 2.0},
            "requirement": {"type": "friends", "count": 50},
        },
    ]
    return {"skins": skins}

@app.post("/api/select-skin")
async def select_skin(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("select_skin", payload.user_id, ttl=3)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if not isinstance(extra, dict):
            extra = {}

        owned_skins = extra.get("owned_skins", ["default_SP"])
        if payload.skin_id not in owned_skins:
            raise HTTPException(status_code=400, detail="Skin not owned")

        extra["selected_skin"] = payload.skin_id

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "selected_skin": payload.skin_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in select_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/unlock-skin")
async def unlock_skin(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("unlock_skin", payload.user_id, ttl=5)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        owned = extra.get("owned_skins", ["default_SP"])
        ads_watched = int(extra.get("ads_watched", 0))

        # ğŸ”¥ Ğ¿Ñ€Ğ¾Ğ²ĞµÑ€ĞºĞ° ÑĞºĞ¸Ğ½Ğ¾Ğ²
        SKINS_REQUIREMENTS = {
            "skin_video_1": 5,
            "skin_video_2": 10,
            "skin_video_3": 20,
            "skin_video_4": 25,
            "skin_video_5": 35,
            "skin_video_6": 50,
        }

        if payload.skin_id in owned:
            return {"success": True}

        if payload.skin_id in SKINS_REQUIREMENTS:
            required = SKINS_REQUIREMENTS[payload.skin_id]

            if ads_watched < required:
                raise HTTPException(status_code=400, detail="Not enough ads watched")

        # âœ… Ğ´Ğ¾Ğ±Ğ°Ğ²Ğ»ÑĞµĞ¼ ÑĞºĞ¸Ğ½
        owned.append(payload.skin_id)
        extra["owned_skins"] = owned

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unlock skin error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== Ğ—ĞĞŸĞ£Ğ¡Ğš ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)

