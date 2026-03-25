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
import httpx
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
    MAX_BET,
    MAX_CLICK_BATCH_SIZE,
    MAX_UPGRADE_LEVEL,
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
    resolve_max_energy,
)
from core.telegram_auth import verify_telegram_init_data
from core.stars_skins import get_stars_skin_price
from CONFIG.settings import BOT_TOKEN


REDIS_URL = os.getenv("REDIS_URL")
redis_client = None
LOCAL_LOCKS: dict[str, float] = {}
LOCAL_IDEMPOTENCY_KEYS: dict[str, float] = {}
LOCAL_RATE_LIMITS_STATE: dict[str, deque[float]] = defaultdict(deque)
ONLINE_USERS_KEY = "online:users"
ONLINE_WINDOW_SECONDS = 75
REFERRAL_SHARE_RATE = 0.05
REFERRAL_DAILY_SHARE_LIMIT = 50000
REFERRAL_SPECIAL_SKIN_ID = "referral-special.pngSP"
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
        logger.info("вњ“ Redis reconnected")
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


async def touch_online_user(user_id: int) -> int:
    conn = await get_redis_or_none()
    if conn is None:
        return 0

    now_ts = int(time.time())
    cutoff = now_ts - ONLINE_WINDOW_SECONDS
    try:
        await conn.zadd(ONLINE_USERS_KEY, {str(user_id): now_ts})
        await conn.zremrangebyscore(ONLINE_USERS_KEY, 0, cutoff)
        online_now = await conn.zcount(ONLINE_USERS_KEY, cutoff, "+inf")
        await conn.expire(ONLINE_USERS_KEY, ONLINE_WINDOW_SECONDS * 2)
        return int(online_now or 0)
    except Exception as e:
        logger.warning(f"Online heartbeat failed: {e}")
        return 0


async def get_online_users_count() -> int:
    conn = await get_redis_or_none()
    if conn is None:
        return 0

    now_ts = int(time.time())
    cutoff = now_ts - ONLINE_WINDOW_SECONDS
    try:
        await conn.zremrangebyscore(ONLINE_USERS_KEY, 0, cutoff)
        online_now = await conn.zcount(ONLINE_USERS_KEY, cutoff, "+inf")
        return int(online_now or 0)
    except Exception as e:
        logger.warning(f"Online count fetch failed: {e}")
        return 0


async def create_telegram_stars_invoice_link(*, user_id: int, skin_id: str, price: int) -> str:
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="Bot token not configured")

    payload = f"stars_skin:{user_id}:{skin_id}"
    request_body = {
        "title": f"Skin {skin_id}",
        "description": f"Unlock premium skin {skin_id}",
        "payload": payload,
        "currency": "XTR",
        "prices": [{"label": skin_id, "amount": price}],
        "provider_token": ""
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createInvoiceLink",
            json=request_body
        )

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="Telegram invoice creation failed")

    data = response.json()
    if not data.get("ok") or not data.get("result"):
        raise HTTPException(status_code=502, detail="Telegram invoice creation failed")

    return data["result"]


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




# ==================== РўРЈР РќРР РќР«Р• Р”РђРќРќР«Р• ==================

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

# ==================== Р’СЃРїРѕРјРѕРіР°С‚РµР»СЊРЅС‹Рµ С„СѓРЅРєС†РёРё Р°РЅС‚РёСЃРїР°РјР° ====================


async def invalidate_user_cache(user_id: int):
    conn = await get_redis_or_none()
    if conn:
        await conn.delete(f"{USER_CACHE_PREFIX}{user_id}")


def parse_extra_data(extra_raw) -> dict:
    if isinstance(extra_raw, dict):
        return extra_raw
    if isinstance(extra_raw, str) and extra_raw:
        try:
            return json.loads(extra_raw)
        except Exception:
            return {}
    return {}


async def grant_referral_share_bonus(referral_user: dict, source_income: int) -> int:
    if source_income <= 0:
        return 0

    referral_user_id = int(referral_user.get("user_id", 0))
    referrer_id = int(referral_user.get("referrer_id") or 0)
    if not referrer_id or referrer_id == referral_user_id:
        return 0

    referrer = await get_user_cached(referrer_id)
    if not referrer:
        return 0

    if int(referrer.get("referrer_id") or 0) == referral_user_id:
        return 0

    bonus = int(source_income * REFERRAL_SHARE_RATE)
    if bonus <= 0:
        return 0

    extra = parse_extra_data(referrer.get("extra_data"))
    today_key = datetime.utcnow().date().isoformat()
    if extra.get("referral_commission_date") != today_key:
        extra["referral_commission_date"] = today_key
        extra["referral_commission_today"] = 0

    today_amount = int(extra.get("referral_commission_today", 0))
    available = max(0, REFERRAL_DAILY_SHARE_LIMIT - today_amount)
    bonus = min(bonus, available)
    if bonus <= 0:
        return 0

    extra["referral_commission_today"] = today_amount + bonus

    await update_user(referrer_id, {
        "coins": int(referrer.get("coins", 0)) + bonus,
        "referral_earnings": int(referrer.get("referral_earnings", 0)) + bonus,
        "extra_data": extra,
    })
    await invalidate_user_cache(referrer_id)
    return bonus




async def distribute_tournament_rewards():
    """Award top players before resetting the leaderboard."""
    try:
        redis_conn = await get_redis_or_none()
        if not redis_conn:
            return

        top_players = await redis_conn.zrevrange(
            TOURNAMENT_KEY,
            0,
            2,
            withscores=True
        )
        if not top_players:
            return

        shares = [0.5, 0.3, 0.2]

        for idx, (user_id_str, score) in enumerate(top_players):
            if idx >= len(shares):
                break
            try:
                user_id = int(user_id_str)
            except ValueError:
                continue

            reward = int(TOURNAMENT_PRIZE_POOL * shares[idx])
            user = await get_user(user_id)
            if not user:
                continue

            new_coins = int(user.get("coins", 0)) + reward
            await update_user(user_id, {"coins": new_coins})
            await invalidate_user_cache(user_id)
            logger.info(f"рџЏ† Tournament reward: user {user_id} place {idx+1} +{reward} coins (score {int(score)})")
    except Exception as e:
        logger.error(f"Error distributing tournament rewards: {e}")


async def reset_tournament_loop():
    while True:
        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        sleep_seconds = max(1, int((tomorrow - now).total_seconds()))

        await asyncio.sleep(sleep_seconds)

        try:
            await distribute_tournament_rewards()
            redis_conn = await get_redis_or_none()
            if redis_conn:
                await redis_conn.delete(TOURNAMENT_KEY)
                logger.info("рџЏ† Tournament leaderboard reset after rewards")
        except Exception as e:
            logger.error(f"Error resetting tournament leaderboard: {e}")



# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client

    logger.info("рџљЂ Starting Ryoho Clicker API")

    await init_db()
    logger.info("вњ… Database initialized")

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
            logger.info("вњ… Redis connected")
        except Exception as e:
            logger.error(f"вќЊ Redis connection failed: {e}")
            redis_client = None
    else:
        logger.warning("вљ пёЏ REDIS_URL is not set")

    if redis_client:
        asyncio.create_task(reset_tournament_loop())
        asyncio.create_task(flush_click_buffer_loop())

    logger.info("вњ… Background tasks started")
    yield

    if redis_client:
        await redis_client.close()

    logger.info("рџ›‘ Shutting down")

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

# ==================== РњРћР”Р•Р›Р ====================

# ==================== Р’РЎРџРћРњРћР“РђРўР•Р›Р¬РќР«Р• Р¤РЈРќРљР¦РР ====================
async def acquire_once_lock(key: str, ttl: float = 10) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            ttl_ms = max(1, int(float(ttl) * 1000))
            result = await conn.set(key, "1", px=ttl_ms, nx=True)
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


async def require_user_action_lock(namespace: str, user_id: int, ttl: float = 5):
    lock_key = f"lock:{namespace}:{user_id}"
    locked = await acquire_once_lock(lock_key, ttl=ttl)
    if not locked:
        raise HTTPException(status_code=429, detail="Action already in progress")


async def ensure_redis_available() -> redis.Redis:
    """
    Try reconnect once and guarantee redis_client or raise 503.
    """
    return await redis_or_503()

# ==================== Р­РќР”РџРћРРќРўР« ====================

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
    True = РјРѕР¶РЅРѕ РїСЂРѕРїСѓСЃС‚РёС‚СЊ
    False = Р»РёРјРёС‚ РїСЂРµРІС‹С€РµРЅ
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
        max_energy = resolve_max_energy(user)

        if int(user.get("max_energy", max_energy)) != max_energy or int(user.get("energy", current_energy)) > max_energy:
            await update_user(user_id, {
                "max_energy": max_energy,
                "energy": min(current_energy, max_energy),
            })
            await invalidate_user_cache(user_id)


        return {
            "user_id": user["user_id"],
            "username": user.get("username"),
            "coins": user.get("coins", 0),
            "energy": current_energy,
            "max_energy": max_energy,
            "profit_per_tap": user.get("profit_per_tap", get_tap_value(user.get("multitap_level", 0))),
            "profit_per_hour": user.get("profit_per_hour", get_hour_value(user.get("profit_level", 0))),
            "multitap_level": user.get("multitap_level", 0),
            "profit_level": user.get("profit_level", 0),
            "energy_level": user.get("energy_level", 0),
            "owned_skins": (user.get("extra_data", {}) or {}).get("owned_skins", [DEFAULT_SKIN_ID]),
            "selected_skin": (user.get("extra_data", {}) or {}).get("selected_skin", DEFAULT_SKIN_ID),
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
        
        # РџСЂРѕРІРµСЂСЏРµРј, РЅРµ Р°РєС‚РёРІРµРЅ Р»Рё СѓР¶Рµ Р±СѓСЃС‚
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
        
        # РђРєС‚РёРІРёСЂСѓРµРј РЅР° 5 РјРёРЅСѓС‚
        expires_at = (now + timedelta(minutes=5)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["active_boosts"] = active_boosts
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        
        return {
            "success": True,
            "message": "рџ”ҐвљЎ MEGA BOOST activated for 5 minutes! x2 coins + infinite energy",
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
        
        # РЎРѕС…СЂР°РЅСЏРµРј СЃС‚Р°С‚РёСЃС‚РёРєСѓ
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
        await require_user_action_lock("upgrade", payload.user_id, ttl=0.35)
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

        new_level = current_level + 1
        new_multitap_level = new_level if boost_type == "multitap" else int(user.get("multitap_level", 0))
        new_profit_level = new_level if boost_type == "profit" else int(user.get("profit_level", 0))
        new_energy_level = new_level if boost_type == "energy" else int(user.get("energy_level", 0))
        new_profit_per_tap = get_tap_value(new_multitap_level)
        new_profit_per_hour = get_hour_value(new_profit_level)
        new_max_energy = get_max_energy(new_energy_level)
        new_coins = int(user.get("coins", 0)) - price

        updates = {
            "coins": new_coins,
            f"{boost_type}_level": new_level,
            "profit_per_tap": new_profit_per_tap,
            "profit_per_hour": new_profit_per_hour,
            "max_energy": new_max_energy,
        }

        if boost_type == "energy":
            updates["energy"] = new_max_energy

        await update_user(payload.user_id, updates)
        await invalidate_user_cache(payload.user_id)
        
        # РћР±РЅРѕРІР»СЏРµРј РєСЌС€
        
        return {
            "success": True,
            "coins": new_coins,
            "new_level": new_level,
            "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] 
                if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
            "profit_per_tap": new_profit_per_tap,
            "profit_per_hour": new_profit_per_hour,
            "max_energy": new_max_energy
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/upgrade-all")
async def process_upgrade_all(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("upgrade_all", payload.user_id, ttl=0.35)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        sequence = ("multitap", "profit", "energy")
        current_levels = {boost: int(user.get(f"{boost}_level", 0)) for boost in sequence}

        if any(current_levels[boost] >= MAX_UPGRADE_LEVEL for boost in sequence):
            raise HTTPException(status_code=400, detail="Max level reached")

        total_cost = sum(UPGRADE_PRICES[boost][current_levels[boost]] for boost in sequence)
        current_coins = int(user.get("coins", 0))
        if current_coins < total_cost:
            raise HTTPException(status_code=400, detail="Not enough coins")

        new_multitap_level = current_levels["multitap"] + 1
        new_profit_level = current_levels["profit"] + 1
        new_energy_level = current_levels["energy"] + 1
        new_profit_per_tap = get_tap_value(new_multitap_level)
        new_profit_per_hour = get_hour_value(new_profit_level)
        new_max_energy = get_max_energy(new_energy_level)
        new_coins = current_coins - total_cost

        updates = {
            "coins": new_coins,
            "multitap_level": new_multitap_level,
            "profit_level": new_profit_level,
            "energy_level": new_energy_level,
            "profit_per_tap": new_profit_per_tap,
            "profit_per_hour": new_profit_per_hour,
            "max_energy": new_max_energy,
            "energy": new_max_energy,
        }

        await update_user(payload.user_id, updates)
        await invalidate_user_cache(payload.user_id)

        next_prices = {
            boost: (
                UPGRADE_PRICES[boost][current_levels[boost] + 1]
                if current_levels[boost] + 1 < len(UPGRADE_PRICES[boost]) else 0
            )
            for boost in sequence
        }

        return {
            "success": True,
            "coins": new_coins,
            "levels": {
                "multitap": new_multitap_level,
                "profit": new_profit_level,
                "energy": new_energy_level,
            },
            "prices": next_prices,
            "profit_per_tap": new_profit_per_tap,
            "profit_per_hour": new_profit_per_hour,
            "max_energy": new_max_energy,
            "energy": new_max_energy,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade_all: {e}")
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
        max_energy = resolve_max_energy(user)

        await update_user(user_id, {
            "max_energy": max_energy,
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
    """РЎС‚Р°СЂС‹Р№ СЌРЅРґРїРѕРёРЅС‚ РґР»СЏ РѕР±СЂР°С‚РЅРѕР№ СЃРѕРІРјРµСЃС‚РёРјРѕСЃС‚Рё"""
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("recover_energy", payload.user_id, ttl=3)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        max_energy = resolve_max_energy(user)
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
    """РЎРµСЂРІРµСЂРЅС‹Р№ sync СЌРЅРµСЂРіРёРё Р±РµР· СЃР±СЂРѕСЃР° С‚Р°Р№РјРµСЂР° СЂРµРіРµРЅР°."""
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()

        old_energy = int(user.get("energy", 0))
        max_energy = resolve_max_energy(user)
        last_update = normalize_dt(user.get("last_energy_update"))

        current_energy = calculate_current_energy(user, now)

        update_data = {}
        if int(user.get("max_energy", max_energy)) != max_energy:
            update_data["max_energy"] = max_energy

        # РћР±РЅРѕРІР»СЏРµРј baseline С‚РѕР»СЊРєРѕ РµСЃР»Рё СЌРЅРµСЂРіРёСЏ СЂРµР°Р»СЊРЅРѕ РІС‹СЂРѕСЃР»Р°
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

        # Р•СЃР»Рё СЌРЅРµСЂРіРёСЏ СѓР¶Рµ РїРѕР»РЅР°СЏ, РґРµСЂР¶РёРј baseline РєРѕРЅСЃРёСЃС‚РµРЅС‚РЅС‹Рј
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

DEFAULT_SKIN_ID = "default.pngSP"

SKIN_MULTIPLIERS = {
    DEFAULT_SKIN_ID: 1.0,
    REFERRAL_SPECIAL_SKIN_ID: 1.8,
    "10lvl.pngSP": 1.2,
    "25lvl.pngSP": 1.2,
    "50lvl.pngSP": 1.2,
    "75lvl.pngSP": 1.2,
    "100lvl.pngSP": 1.2,
    "video.pngSP": 1.5,
    "video2.pngSP": 1.5,
    "video3.pngSP": 1.5,
    "video4.pngSP": 1.5,
    "video5.pngSP": 1.5,
    "video6.pngSP": 1.5,
    "video7.pngSP": 1.5,
    "video8.pngSP": 1.5,
    "stars1.pngSP": 2.0,
    "stars2.pngSP": 2.0,
    "stars3.pngSP": 2.0,
    "stars4.pngSP": 2.0,
    "stars5.pngSP": 2.0,
    "stars6.pngSP": 2.0,
    "stars7.pngSP": 2.0,
    "stars8.pngSP": 2.0,
}


def get_selected_skin_multiplier(user: dict) -> float:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    selected_skin = extra.get("selected_skin", DEFAULT_SKIN_ID)
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
    "10lvl.pngSP": {"type": "level", "value": 10},
    "25lvl.pngSP": {"type": "level", "value": 25},
    "50lvl.pngSP": {"type": "level", "value": 50},
    "75lvl.pngSP": {"type": "level", "value": 75},
    "100lvl.pngSP": {"type": "level", "value": 100},
    "video.pngSP": {"type": "ads", "count": 10},
    "video2.pngSP": {"type": "ads", "count": 10},
    "video3.pngSP": {"type": "ads", "count": 10},
    "video4.pngSP": {"type": "ads", "count": 10},
    "video5.pngSP": {"type": "ads", "count": 10},
    "video6.pngSP": {"type": "ads", "count": 10},
    "video7.pngSP": {"type": "ads", "count": 10},
    "video8.pngSP": {"type": "ads", "count": 10},
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

        max_energy = resolve_max_energy(user)
        current_energy = calculate_current_energy(user, now)

        multitap_level = int(user.get("multitap_level", 0))
        tap_value = get_tap_value(multitap_level)

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        selected_skin = extra.get("selected_skin", DEFAULT_SKIN_ID)
        skin_multiplier = float(SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = is_mega_boost_active(user)

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2

        # Р·Р°С‰РёС‚Р°
        safe_requested_clicks = min(payload.clicks, MAX_CLICK_BATCH_SIZE)
        allowed_clicks = get_allowed_clicks(user, now, safe_requested_clicks)

        effective_clicks = allowed_clicks if mega_boost_active else min(allowed_clicks, current_energy)
        gained = effective_clicks * coin_per_tap

        # РЅРѕРІС‹Рµ Р·РЅР°С‡РµРЅРёСЏ
        new_energy = current_energy if mega_boost_active else max(0, current_energy - effective_clicks)
        new_coins = int(user.get("coins", 0)) + gained

        update_data = {
            "coins": new_coins,
            "max_energy": max_energy,
        }

        if mega_boost_active:
            stored_energy = int(user.get("energy", 0))
            last_update = normalize_dt(user.get("last_energy_update"))

            if stored_energy != current_energy:
                update_data["energy"] = current_energy

            if last_update:
                seconds_passed = max(0, int((now - last_update).total_seconds()))
                gained_energy = seconds_passed // ENERGY_REGEN_SECONDS
                if gained_energy > 0:
                    update_data["last_energy_update"] = last_update + timedelta(
                        seconds=gained_energy * ENERGY_REGEN_SECONDS
                    )
            elif "energy" in update_data:
                update_data["last_energy_update"] = now
        else:
            update_data["energy"] = new_energy
            update_data["last_energy_update"] = now

        # РЎРѕС…СЂР°РЅСЏРµРј СЌРЅРµСЂРіРёСЋ Рё Р±Р°Р»Р°РЅСЃ РѕРґРЅРёРј server-side update РЅР° Р±Р°С‚С‡ РєР»РёРєРѕРІ.
        await update_user(payload.user_id, update_data)

        conn = await get_redis_or_none()
        if conn and gained > 0:
            # РўСѓСЂРЅРёСЂ РѕСЃС‚Р°РІР»СЏРµРј РІ Redis РєР°Рє Р±С‹СЃС‚СЂС‹Р№ leaderboard СЃР»РѕР№.
            await conn.zincrby(
                TOURNAMENT_KEY,
                gained,
                str(payload.user_id)
            )

        # вњ… РёРЅРІР°Р»РёРґРёСЂСѓРµРј РєСЌС€
        await invalidate_user_cache(payload.user_id)
        referral_bonus = await grant_referral_share_bonus(user, gained)

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
            "mega_boost_active": mega_boost_active,
            "referral_bonus_paid": referral_bonus
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

        valid_referrer_id = None
        requested_referrer_id = int(payload.referrer_id or 0)
        if requested_referrer_id and requested_referrer_id != payload.user_id:
            referrer = await get_user_cached(requested_referrer_id)
            if referrer and int(referrer.get("referrer_id") or 0) != payload.user_id:
                valid_referrer_id = requested_referrer_id

        await create_user(
            user_id=payload.user_id,
            username=telegram_user.get("username") or payload.username,
            referrer_id=valid_referrer_id
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

# ==================== РњРРќР-РР“Р Р« ====================

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
            message = f"You won +{payload.bet} coins!"
        else:
            user["coins"] -= payload.bet
            message = f"You lost {payload.bet} coins"

        await update_user(payload.user_id, {"coins": user["coins"]})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "coins": user["coins"], "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in coinflip: {e}")
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

        symbols = ["CH", "LM", "OR", "77", "DM", "ST"]
        slots = [random.choice(symbols) for _ in range(3)]
        win = len(set(slots)) == 1
        multiplier = 10 if "77" in slots and win else 5 if "DM" in slots and win else 3

        if win:
            win_amount = payload.bet * multiplier
            user["coins"] += win_amount
            message = f"JACKPOT! +{win_amount} coins!"
        else:
            user["coins"] -= payload.bet
            message = f"You lost {payload.bet} coins"

        await update_user(payload.user_id, {"coins": user["coins"]})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "coins": user["coins"], "slots": slots, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in slots: {e}")
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
            message = f"You won +{win_amount} coins!"
        else:
            user["coins"] -= payload.bet
            message = f"You lost {payload.bet} coins"

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
            result_color = "green"
            result_symbol = "GREEN"
        elif result in red_numbers:
            result_color = "red"
            result_symbol = "RED"
        else:
            result_color = "black"
            result_symbol = "BLACK"

        win = False
        multiplier = 0

        if payload.bet_type == "number" and payload.bet_value == result:
            win = True
            multiplier = 35
        elif payload.bet_type == "green" and result_color == "green":
            win = True
            multiplier = 35
        elif payload.bet_type == result_color:
            win = True
            multiplier = 2

        if win:
            win_amount = payload.bet * multiplier
            user["coins"] += win_amount
            message = f"{result_symbol} {result} - You won +{win_amount} coins! (x{multiplier})"
        else:
            user["coins"] -= payload.bet
            message = f"{result_symbol} {result} - You lost {payload.bet} coins"

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
        logger.error(f"Error in roulette: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
# ==================== TOURNAMENT ENDPOINTS ====================

@app.post("/api/online/heartbeat")
async def online_heartbeat(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        online_now = await touch_online_user(payload.user_id)
        return {"success": True, "online_now": online_now}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in online heartbeat: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/online/count")
async def get_online_count():
    try:
        online_now = await get_online_users_count()
        return {"success": True, "online_now": online_now}
    except Exception as e:
        logger.error(f"Error getting online count: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/skins/stars-invoice")
async def create_skin_stars_invoice(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("stars_skin_invoice", payload.user_id, ttl=3)

        price = get_stars_skin_price(payload.skin_id)
        if price is None:
            raise HTTPException(status_code=400, detail="Skin is not sold for Stars")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        owned_skins = extra.get("owned_skins", [DEFAULT_SKIN_ID])
        if payload.skin_id in owned_skins:
            raise HTTPException(status_code=400, detail="Skin already owned")

        invoice_link = await create_telegram_stars_invoice_link(
            user_id=payload.user_id,
            skin_id=payload.skin_id,
            price=price
        )

        return {
            "success": True,
            "invoice_link": invoice_link,
            "price": price,
            "skin_id": payload.skin_id
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Stars invoice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

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
            "time_left": time_left,
            "online_now": await get_online_users_count()
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

# ==================== Р—РђР”РђР§Р ====================

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
            {"id": "daily_bonus", "title": "рџ“… Daily Bonus", "description": "Come back every day", 
             "reward": "25000 coins", "icon": "рџ“…", "completed": "daily_bonus" in completed_tasks},
            {"id": "energy_refill", "title": "вљЎ Infinite Energy", "description": "5 minutes of unlimited energy", 
             "reward": "вљЎ 5 minutes", "icon": "вљЎ", "completed": "energy_refill" in completed_tasks},
            {"id": "link_click", "title": "рџ”— Follow Link", "description": "Click the link and get reward", 
             "reward": "25000 coins", "icon": "рџ”—", "completed": False},
            {"id": "invite_5_friends", "title": "рџ‘Ґ Invite 5 Friends", "description": "Invite 5 friends", 
             "reward": "20000 coins", "icon": "рџ‘Ґ", "completed": "invite_5_friends" in completed_tasks, 
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
            
            return {"success": True, "message": "рџ”— +25000 coins!", "coins": user["coins"]}
        
        completed = await get_completed_tasks(payload.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")
        
        if task_id == "daily_bonus":
            user["coins"] += 25000
            await add_completed_task(payload.user_id, task_id)
            await update_user(payload.user_id, {"coins": user["coins"]})
            await invalidate_user_cache(payload.user_id)
            return {"success": True, "message": "рџЋЃ +25000 coins!", "coins": user["coins"]}
        
        elif task_id == "energy_refill":
            await add_completed_task(payload.user_id, task_id)
            return {"success": True, "message": "вљЎ Energy refill activated!"}
        
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] += 20000
                await add_completed_task(payload.user_id, task_id)
                await update_user(payload.user_id, {"coins": user["coins"]})
                await invalidate_user_cache(payload.user_id)
                return {"success": True, "message": "рџ‘Ґ +20000 coins!", "coins": user["coins"]}
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")
        
        raise HTTPException(status_code=400, detail="Unknown task")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== РџРђРЎРЎРР’РќР«Р™ Р”РћРҐРћР” ====================

@app.post("/api/passive-income")
async def passive_income(payload: PassiveIncomeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("passive_income", payload.user_id, ttl=5)
        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        last_income = normalize_dt(user.get("last_passive_income"))
        now = datetime.utcnow()

        if not last_income:
            await update_user(payload.user_id, {"last_passive_income": now})
            await invalidate_user_cache(payload.user_id)
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        elapsed_seconds = max(0.0, (now - last_income).total_seconds())
        elapsed_seconds = min(elapsed_seconds, 24 * 3600)

        hour_value = int(user.get("profit_per_hour", get_hour_value(user.get("profit_level", 0))))
        if hour_value <= 0 or elapsed_seconds <= 0:
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        total_income = int((hour_value * elapsed_seconds) // 3600)
        if total_income <= 0:
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        consumed_seconds = (total_income * 3600) / hour_value
        new_last_income = min(now, last_income + timedelta(seconds=consumed_seconds))
        new_coins = int(user.get("coins", 0)) + total_income

        await update_user(payload.user_id, {
            "coins": new_coins,
            "last_passive_income": new_last_income
        })
        await invalidate_user_cache(payload.user_id)
        referral_bonus = await grant_referral_share_bonus(user, total_income)

        return {
            "success": True,
            "coins": new_coins,
            "income": total_income,
            "referral_bonus_paid": referral_bonus,
            "message": f"+{total_income} passive income"
        }
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== РЎРљРРќР« ====================



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

        owned_skins = extra.get("owned_skins", [DEFAULT_SKIN_ID])
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

        owned = extra.get("owned_skins", [DEFAULT_SKIN_ID])
        ads_watched = int(extra.get("ads_watched", 0))

        if payload.skin_id in owned:
            return {"success": True}

        if payload.skin_id in SKIN_REQUIREMENTS and SKIN_REQUIREMENTS[payload.skin_id]["type"] == "ads":
            required = int(SKIN_REQUIREMENTS[payload.skin_id]["count"])

            if ads_watched < required:
                raise HTTPException(status_code=400, detail="Not enough ads watched")

        # вњ… РґРѕР±Р°РІР»СЏРµРј СЃРєРёРЅ
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

# ==================== Р—РђРџРЈРЎРљ ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)


