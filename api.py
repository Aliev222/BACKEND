from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
# Sync marker for VS Code source control
from fastapi.responses import JSONResponse, Response
import asyncio
import base64
import uvicorn
import random
import time
import json
import os
import logging
import httpx
import hmac
import hashlib
import secrets
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from sqlalchemy import select, func, update
from DATABASE.base import User, AsyncSessionLocal, WeeklyTournamentEntry, WeeklyTournamentWinner, RewardedAdClaim
from collections import defaultdict, deque
from dataclasses import dataclass
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from DATABASE.base import (
    get_user, add_user as create_user, update_user, add_referral_bonus,
    init_db, get_completed_tasks, add_completed_task,
    add_weekly_tournament_score, get_weekly_tournament_leaderboard,
    get_weekly_tournament_player_entry, get_weekly_tournament_season_key,
    get_weekly_tournament_season_window, get_weekly_tournament_league,
    list_weekly_tournament_seasons, get_weekly_tournament_winners,
    finalize_weekly_tournament_season, ensure_weekly_tournament_season,
    get_rewarded_ads_admin_summary, get_stars_skin_sales_admin_summary,
    get_admin_fraud_reviews, upsert_admin_fraud_review, record_rewarded_ad_claim,
)
from schemas import (
    AdActionClaimRequest,
    AdActionStartRequest,
    ClicksBatchRequest,
    CrashGameCashoutRequest,
    CrashGameStartRequest,
    EnergySyncRequest,
    GameRequest,
    LuckyBoxRequest,
    PassiveIncomeRequest,
    RegisterRequest,
    RewardVideoClaimRequest,
    RewardVideoStartRequest,
    SkinRequest,
    TaskCompleteRequest,
    TournamentData,
    UpgradeRequest,
    UserIdRequest,
    VideoTaskClaimRequest,
    AdminFraudUpdateRequest,
    AdminWinnerStarsUpdateRequest,
    WeeklyTournamentFundRequest,
)
from core.game_config import (
    BASE_MAX_ENERGY,
    CLICK_BURST_ALLOWANCE,
    CLICK_BUFFER_KEY,
    CLICK_FLUSH_INTERVAL,
    CLICK_SUSPICIOUS_OVERSHOOT,
    CLICK_SUSPICION_SOFT_LIMIT,
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
    GLOBAL_UPGRADE_PRICES,
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
APP_ENV = (os.getenv("APP_ENV", "production") or "production").strip().lower()
ONLINE_USERS_KEY = "online:users"
ONLINE_WINDOW_SECONDS = 75
REFERRAL_SHARE_RATE = 0.05
REFERRAL_DAILY_SHARE_LIMIT = 50000
REFERRAL_SPECIAL_SKIN_ID = "refferal.pngSP"
TELEGRAM_VERIFY_CHANNEL = os.getenv("TELEGRAM_VERIFY_CHANNEL", "@Spirit_cliker")
TELEGRAM_MEMBER_STATUSES = {"member", "administrator", "creator", "restricted"}
ADMIN_DASHBOARD_TOKEN = (os.getenv("ADMIN_DASHBOARD_TOKEN", "") or "").strip()
ADMIN_TELEGRAM_IDS = {
    int(item.strip())
    for item in (os.getenv("ADMIN_TELEGRAM_IDS", "1507124181") or "1507124181").split(",")
    if item.strip().isdigit()
}
MONETAG_POSTBACK_SECRET = (os.getenv("MONETAG_POSTBACK_SECRET", "") or "").strip()
MONETAG_POSTBACK_ENFORCED = (os.getenv("MONETAG_POSTBACK_ENFORCED", "1" if MONETAG_POSTBACK_SECRET else "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
SESSION_TOKEN_SECRET = (os.getenv("SESSION_TOKEN_SECRET", "") or "").strip() or BOT_TOKEN
SESSION_TOKEN_TTL_SECONDS = max(900, int((os.getenv("SESSION_TOKEN_TTL_SECONDS", "3600") or "3600").strip()))
DAILY_REWARD_MAX_DAYS = 30
DAILY_REWARD_BASE_COINS = 500
DAILY_REWARD_INFINITE_ENERGY_MINUTES = 10
DAILY_REWARD_SKIN_ID = "retro.pngSP"
MEGA_BOOST_MINUTES = 1
MEGA_BOOST_COOLDOWN_MIN_MINUTES = 10
MEGA_BOOST_COOLDOWN_MAX_MINUTES = 10
GHOST_BOOST_MULTIPLIER = 5
GHOST_BOOST_MINUTES = 1
AUTOCLICKER_COOLDOWN_MINUTES = 10
SKIN_AD_COOLDOWN_MINUTES = 10
ENERGY_REFILL_COOLDOWN_MINUTES = 10
AD_ACTION_SESSION_TTL_SECONDS = 180
AD_SESSION_MIN_WAIT_SECONDS = 8
AD_ACTIONS_ALLOWED = {"energy_refill_max", "mega_boost", "ghost_boost", "ads_increment", "video_task", "autoclicker"}
CRASH_GHOST_SESSION_TTL_SECONDS = 90
CRASH_GHOST_MULTIPLIER_SPEED = 0.68
MONETAG_POSTBACK_ID_KEYS = (
    "ad_session_id", "subid", "sub_id", "click_id", "clickid", "cid",
    "transaction_id", "txid", "tid", "session_id", "s1", "s2", "s3",
    "ymid", "request_var"
)
MONETAG_POSTBACK_SECRET_KEYS = ("token", "secret", "key")
MONETAG_POSTBACK_NEGATIVE_VALUES = {"0", "false", "failed", "cancelled", "canceled", "rejected", "deny", "denied"}
VIDEO_TASK_DEFINITIONS = {
    "tap_surge": {
        "type": "tap_boost",
        "cooldown_minutes": 75,
        "duration_minutes": 5,
        "multiplier": 2,
    },
    "passive_hour": {
        "type": "passive_boost",
        "cooldown_minutes": 240,
        "duration_minutes": 60,
        "multiplier": 2,
    },
    "coin_drop": {
        "type": "coin_drop",
        "cooldown_minutes": 60,
    },
}

VIDEO_SKIN_IDS = {
    "video.pngSP",
    "video2.pngSP",
    "video3.pngSP",
    "video4.pngSP",
    "video5.pngSP",
    "video6.pngSP",
    "video7.pngSP",
    "video8.pngSP",
}
WEEKLY_LEAGUE_ORDER = ("diamond", "gold", "silver", "bronze")
WEEKLY_LEAGUE_LEVEL_RANGES = {
    "bronze": {"min_level": 1, "max_level": 32},
    "silver": {"min_level": 33, "max_level": 65},
    "gold": {"min_level": 66, "max_level": 99},
    "diamond": {"min_level": 100, "max_level": None},
}
WEEKLY_LEAGUE_FUND_SPLITS = {
    "diamond": 0.50,
    "gold": 0.30,
    "silver": 0.15,
    "bronze": 0.05,
}
WEEKLY_TOP3_PAYOUT_SPLITS = {
    1: 0.30,
    2: 0.20,
    3: 0.15,
}
WEEKLY_RANGE_PAYOUT_SPLITS = [
    {"start": 4, "end": 10, "share": 0.20},
    {"start": 11, "end": 20, "share": 0.10},
    {"start": 21, "end": 50, "share": 0.05},
]


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _parse_csv_env(name: str, default: list[str]) -> list[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


PROD_CORS_ORIGINS = [
    "https://spirix.vercel.app",
    "https://web.telegram.org",
    "https://telegram.org",
]
DEV_CORS_ORIGINS = PROD_CORS_ORIGINS + [
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
]
ALLOWED_CORS_ORIGINS = _parse_csv_env(
    "CORS_ALLOWED_ORIGINS",
    DEV_CORS_ORIGINS if APP_ENV != "production" else PROD_CORS_ORIGINS,
)
ALLOW_NULL_ORIGIN = _parse_bool_env("CORS_ALLOW_NULL_ORIGIN", APP_ENV != "production")
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


async def verify_telegram_channel_subscription(user_id: int) -> bool:
    if not BOT_TOKEN or not TELEGRAM_VERIFY_CHANNEL:
        logger.warning("Telegram subscription verification is not configured")
        return False

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
                params={
                    "chat_id": TELEGRAM_VERIFY_CHANNEL,
                    "user_id": user_id,
                },
            )
    except Exception as exc:
        logger.warning("Telegram subscription verification request failed for %s: %s", user_id, exc)
        return False

    if response.status_code != 200:
        logger.warning(
            "Telegram subscription verification HTTP error for %s: %s",
            user_id,
            response.status_code,
        )
        return False

    try:
        payload = response.json()
    except Exception:
        logger.warning("Telegram subscription verification returned invalid JSON for %s", user_id)
        return False

    if not payload.get("ok"):
        logger.warning("Telegram subscription verification failed for %s: %s", user_id, payload)
        return False

    status = ((payload.get("result") or {}).get("status") or "").lower()
    return status in TELEGRAM_MEMBER_STATUSES


async def create_ad_action_session(user_id: int, action: str) -> str:
    if action not in AD_ACTIONS_ALLOWED:
        raise HTTPException(status_code=400, detail="Unknown ad action")

    redis_conn = await ensure_redis_available()
    ad_session_id = f"{action}:{user_id}:{int(time.time())}:{random.randint(100000, 999999)}"
    session_key = f"adsession:action:{ad_session_id}"

    await redis_conn.setex(
        session_key,
        AD_ACTION_SESSION_TTL_SECONDS,
        json.dumps({
            "user_id": user_id,
            "action": action,
            "claimed": False,
            "verified": False,
            "verified_at": None,
            "created_at": time.time(),
        })
    )
    return ad_session_id


def extract_first_value(source: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = source.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


async def mark_ad_action_session_verified(ad_session_id: str, postback_payload: dict) -> bool:
    redis_conn = await ensure_redis_available()
    session_key = f"adsession:action:{ad_session_id}"
    raw = await redis_conn.get(session_key)
    if not raw:
        return False

    try:
        session = json.loads(raw)
    except Exception:
        return False

    session["verified"] = True
    session["verified_at"] = time.time()
    session["postback_payload"] = postback_payload

    ttl = await redis_conn.ttl(session_key)
    ttl = max(int(ttl or 0), 300)
    await redis_conn.setex(session_key, ttl, json.dumps(session))
    return True


async def consume_ad_action_session(user_id: int, ad_session_id: str, expected_action: str) -> dict:
    redis_conn = await ensure_redis_available()
    session_key = f"adsession:action:{ad_session_id}"
    raw = await redis_conn.get(session_key)
    if not raw:
        raise HTTPException(status_code=400, detail="Invalid or expired ad session")

    try:
        session = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ad session payload")

    if int(session.get("user_id", 0)) != int(user_id):
        raise HTTPException(status_code=400, detail="Ad session does not belong to user")

    if session.get("action") != expected_action:
        raise HTTPException(status_code=400, detail="Ad session action mismatch")

    if session.get("claimed") is True:
        raise HTTPException(status_code=409, detail="Reward already claimed")

    if MONETAG_POSTBACK_ENFORCED:
        if session.get("verified") is not True:
            raise HTTPException(status_code=400, detail="Ad completion was not confirmed yet")
    else:
        created_at = float(session.get("created_at") or 0)
        if created_at <= 0 or (time.time() - created_at) < AD_SESSION_MIN_WAIT_SECONDS:
            raise HTTPException(status_code=400, detail="Ad watch is not completed yet")

    session["claimed"] = True
    await redis_conn.setex(session_key, 60, json.dumps(session))
    return session


def build_crash_ghost_session(bet: int, user_id: int) -> dict:
    crash_at = round(1.85 + random.random() * 4.15, 2)
    if random.random() < 0.1:
        crash_at = round(5.8 + random.random() * 2.6, 2)

    now_ts = time.time()
    crash_after_seconds = max(0.85, (crash_at - 1.0) / CRASH_GHOST_MULTIPLIER_SPEED)

    return {
        "user_id": user_id,
        "bet": bet,
        "started_at": now_ts,
        "crash_at": crash_at,
        "crash_after_seconds": crash_after_seconds,
        "claimed": False,
    }


def get_crash_ghost_runtime(session: dict, now_ts: float | None = None) -> dict:
    now_ts = now_ts or time.time()
    started_at = float(session.get("started_at") or now_ts)
    crash_after_seconds = float(session.get("crash_after_seconds") or 0)
    crash_at = float(session.get("crash_at") or 1.0)
    elapsed = max(0.0, now_ts - started_at)
    crashed = elapsed >= crash_after_seconds
    multiplier = crash_at if crashed else round(1.0 + elapsed * CRASH_GHOST_MULTIPLIER_SPEED, 2)
    multiplier = max(1.0, min(multiplier, crash_at))

    return {
        "elapsed_seconds": elapsed,
        "crashed": crashed,
        "multiplier": multiplier,
        "crash_at": crash_at,
    }


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


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def issue_session_token(telegram_user: dict) -> tuple[str, int]:
    now_ts = int(time.time())
    expires_at = now_ts + SESSION_TOKEN_TTL_SECONDS
    payload = {
        "uid": int(telegram_user.get("id", 0)),
        "username": telegram_user.get("username"),
        "iat": now_ts,
        "exp": expires_at,
        "jti": secrets.token_hex(8),
    }
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    payload_part = _b64url_encode(payload_json)
    signature = hmac.new(
        SESSION_TOKEN_SECRET.encode("utf-8"),
        payload_part.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_part}.{signature}", expires_at


def verify_session_token(token: str) -> dict:
    if not token or "." not in token:
        raise HTTPException(status_code=401, detail="Invalid session token")

    payload_part, signature = token.rsplit(".", 1)
    expected_signature = hmac.new(
        SESSION_TOKEN_SECRET.encode("utf-8"),
        payload_part.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        raise HTTPException(status_code=401, detail="Invalid session signature")

    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid session payload") from exc

    now_ts = int(time.time())
    if int(payload.get("exp", 0) or 0) <= now_ts:
        raise HTTPException(status_code=401, detail="Session expired")

    user_id = int(payload.get("uid", 0) or 0)
    if user_id <= 0:
        raise HTTPException(status_code=401, detail="Invalid session user")

    return {
        "id": user_id,
        "username": payload.get("username"),
        "iat": int(payload.get("iat", 0) or 0),
        "exp": int(payload.get("exp", 0) or 0),
        "jti": payload.get("jti"),
        "auth": "session",
    }


def read_bearer_token(request: Request) -> str:
    authorization = (request.headers.get("Authorization", "") or "").strip()
    if not authorization:
        return ""
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


async def require_telegram_user(request: Request, expected_user_id: int | None = None) -> dict:
    bearer_token = read_bearer_token(request)
    if bearer_token:
        telegram_user = verify_session_token(bearer_token)
    else:
        telegram_user = verify_telegram_init_data(
            request.headers.get("X-Telegram-Init-Data", "")
        )

    if expected_user_id is not None and int(telegram_user.get("id", 0)) != int(expected_user_id):
        raise HTTPException(status_code=403, detail="Telegram user mismatch")

    return telegram_user


async def require_admin_access(request: Request) -> dict:
    admin_token = (request.headers.get("X-Admin-Token", "") or "").strip()
    if ADMIN_DASHBOARD_TOKEN and admin_token == ADMIN_DASHBOARD_TOKEN:
        return {"auth": "token"}

    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    telegram_user_id = int(telegram_user.get("id", 0))
    if telegram_user_id not in ADMIN_TELEGRAM_IDS:
        raise HTTPException(status_code=403, detail="Admin access required")
    return telegram_user


def format_int(value: int) -> str:
    return f"{int(value or 0):,}".replace(",", " ")


async def get_rewarded_ad_user_counts(user_ids: list[int], *, hours: int) -> dict[int, int]:
    if not user_ids:
        return {}
    since = datetime.utcnow() - timedelta(hours=max(1, int(hours or 1)))
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(RewardedAdClaim.user_id, func.count(RewardedAdClaim.id))
            .where(
                RewardedAdClaim.user_id.in_(user_ids),
                RewardedAdClaim.created_at >= since,
            )
            .group_by(RewardedAdClaim.user_id)
        )
        return {int(user_id): int(count or 0) for user_id, count in result.all()}


async def build_admin_fraud_overview(season_key: str) -> list[dict]:
    now = datetime.utcnow()
    async with AsyncSessionLocal() as session:
        entries_result = await session.execute(
            select(WeeklyTournamentEntry)
            .where(WeeklyTournamentEntry.season_key == season_key)
            .order_by(WeeklyTournamentEntry.fraud_flag.desc(), WeeklyTournamentEntry.score.desc())
            .limit(200)
        )
        entries = entries_result.scalars().all()

        user_ids = [int(entry.user_id) for entry in entries]
        if not user_ids:
            return []

        users_result = await session.execute(select(User).where(User.user_id.in_(user_ids)))
        users_map = {int(user.user_id): user for user in users_result.scalars().all()}

        referrer_ids = sorted({
            int(user.referrer_id)
            for user in users_map.values()
            if getattr(user, "referrer_id", None)
        })
        referrer_cluster_counts: dict[int, int] = {}
        if referrer_ids:
            cluster_result = await session.execute(
                select(User.referrer_id, func.count(User.id))
                .where(User.referrer_id.in_(referrer_ids))
                .group_by(User.referrer_id)
            )
            referrer_cluster_counts = {
                int(referrer_id): int(count or 0)
                for referrer_id, count in cluster_result.all()
                if referrer_id is not None
            }

    reviews_map = await get_admin_fraud_reviews(user_ids)
    recent_ads_1h = await get_rewarded_ad_user_counts(user_ids, hours=1)
    recent_ads_24h = await get_rewarded_ad_user_counts(user_ids, hours=24)

    suspicious_rows = []
    for entry in entries:
        user = users_map.get(int(entry.user_id))
        if user is None:
            continue

        account_age_hours = max(0.0, (now - (user.created_at or now)).total_seconds() / 3600)
        review = reviews_map.get(int(entry.user_id), {})
        reasons: list[str] = []
        extra = parse_extra_data(getattr(user, "extra_data", {}))
        click_guard = get_click_guard_state(extra)

        if review.get("status") == "fraud":
            reasons.append(review.get("reason") or "Manual fraud review")

        if entry.display_level >= 100 and account_age_hours < 72:
            reasons.append("Too fast level growth to Diamond")
        elif entry.display_level >= 66 and account_age_hours < 24:
            reasons.append("Too fast level growth to Gold")
        elif entry.display_level >= 33 and account_age_hours < 8:
            reasons.append("Too fast level growth to Silver")

        ads_1h = int(recent_ads_1h.get(int(entry.user_id), 0))
        ads_24h = int(recent_ads_24h.get(int(entry.user_id), 0))
        if ads_1h >= 25 or ads_24h >= 120:
            reasons.append(f"Too many rewarded ads in a short period ({ads_1h}/1h, {ads_24h}/24h)")

        score_per_hour = int((entry.score or 0) / max(account_age_hours, 1))
        if score_per_hour >= 500000:
            reasons.append(f"Unusually fast click income velocity ({format_int(score_per_hour)} per hour)")

        click_suspicion_score = int(click_guard.get("suspicion_score", 0) or 0)
        hard_rejections = int(click_guard.get("hard_rejections", 0) or 0)
        if click_suspicion_score >= CLICK_SUSPICION_SOFT_LIMIT:
            reasons.append(f"Suspicious click batches detected (score {click_suspicion_score})")
        if hard_rejections > 0:
            reasons.append(f"Server rejected suspicious click bursts ({hard_rejections})")

        referrer_id = getattr(user, "referrer_id", None)
        if referrer_id and referrer_cluster_counts.get(int(referrer_id), 0) >= 5 and account_age_hours <= 72:
            reasons.append("Possible multi-account referral cluster")

        is_flagged = bool(entry.fraud_flag) or review.get("status") == "fraud" or bool(reasons)
        if not is_flagged:
            continue

        suspicious_rows.append({
            "user_id": int(entry.user_id),
            "username": entry.username or getattr(user, "username", None),
            "display_level": int(entry.display_level or 1),
            "league": entry.league,
            "score": int(entry.score or 0),
            "eligible_for_payout": bool(entry.eligible_for_payout),
            "fraud_flag": bool(entry.fraud_flag) or review.get("status") == "fraud",
            "manual_status": review.get("status", "ok"),
            "manual_reason": review.get("reason"),
            "disqualify_from_payout": bool(review.get("disqualify_from_payout")),
            "account_age_hours": round(account_age_hours, 1),
            "rewarded_ads_1h": ads_1h,
            "rewarded_ads_24h": ads_24h,
            "reasons": reasons,
        })

    suspicious_rows.sort(key=lambda item: (not item["fraud_flag"], not item["disqualify_from_payout"], -item["score"]))
    return suspicious_rows




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


def parse_iso_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def get_click_guard_state(extra: dict) -> dict:
    click_guard = extra.get("click_guard", {})
    if not isinstance(click_guard, dict):
        click_guard = {}
    return click_guard


def write_click_guard_state(extra: dict, click_guard: dict) -> dict:
    extra["click_guard"] = click_guard
    return extra


def get_skin_ad_progress(extra: dict) -> dict:
    value = extra.get("skin_ad_progress", {})
    return value if isinstance(value, dict) else {}


def get_skin_ad_last_watch(extra: dict) -> dict:
    value = extra.get("skin_ad_last_watch", {})
    return value if isinstance(value, dict) else {}


def serialize_db_field(field: str, value):
    if field == "extra_data" and value is not None and not isinstance(value, str):
        return json.dumps(value)
    return value


async def update_user_if_matches(user_id: int, expected: dict, data: dict):
    allowed_fields = {
        "username", "coins", "profit_per_hour", "profit_per_tap", "energy",
        "max_energy", "level", "multitap_level", "profit_level", "energy_level",
        "boost_level", "last_passive_income", "last_energy_update", "referrer_id",
        "referral_count", "referral_earnings", "extra_data", "luck_level"
    }
    unknown_fields = (set(expected) | set(data)) - allowed_fields
    if unknown_fields:
        raise ValueError(f"Unsupported atomic update fields: {sorted(unknown_fields)}")

    where_clauses = [User.user_id == user_id]
    for field, raw_value in expected.items():
        value = serialize_db_field(field, raw_value)
        column = getattr(User, field)
        if value is None:
            where_clauses.append(column.is_(None))
        else:
            where_clauses.append(column == value)

    values = {
        field: serialize_db_field(field, raw_value)
        for field, raw_value in data.items()
    }

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            update(User)
            .where(*where_clauses)
            .values(**values)
        )
        if result.rowcount != 1:
            await session.rollback()
            return None
        await session.commit()

    return await get_user(user_id)


def resolve_video_task_coin_drop() -> int:
    roll = random.random()
    if roll < 0.55:
        return random.randint(200, 1000)
    if roll < 0.80:
        return random.randint(1000, 5000)
    if roll < 0.92:
        return random.randint(5000, 12000)
    if roll < 0.98:
        return random.randint(12000, 20000)
    return random.randint(20000, 30000)


def get_video_task_last_claims(extra: dict) -> dict:
    claims = extra.get("video_task_last_claims", {})
    return claims if isinstance(claims, dict) else {}


def get_video_task_boosts(extra: dict) -> dict:
    boosts = extra.get("video_task_boosts", {})
    return boosts if isinstance(boosts, dict) else {}


def get_active_video_task_boost(extra: dict, boost_key: str) -> tuple[bool, str | None, int]:
    boosts = get_video_task_boosts(extra)
    boost = boosts.get(boost_key)
    if not isinstance(boost, dict):
        return False, None, 1

    expires_at = parse_iso_datetime(boost.get("expires_at"))
    if not expires_at or expires_at <= datetime.utcnow():
        return False, None, 1

    return True, expires_at.isoformat(), int(boost.get("multiplier", 1) or 1)


async def touch_user_activity(user_id: int, user: dict | None = None) -> None:
    user_data = user or await get_user_cached(user_id)
    if not user_data:
        return

    extra = parse_extra_data(user_data.get("extra_data"))
    previous_activity = extra.get("last_activity_at")
    previous_stage = int(extra.get("push_idle_stage", 0) or 0)
    now = datetime.utcnow()

    if previous_activity:
        try:
            prev_dt = datetime.fromisoformat(previous_activity)
            if (now - prev_dt).total_seconds() < 120 and previous_stage == 0:
                return
        except Exception:
            pass
    now_iso = now.isoformat()

    extra["last_activity_at"] = now_iso
    extra["push_idle_stage"] = 0
    extra["last_push_reason"] = None

    await update_user(user_id, {"extra_data": extra})
    await invalidate_user_cache(user_id)


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


async def weekly_tournament_rollover_loop():
    while True:
        now = datetime.utcnow()
        current_start, current_end = get_weekly_tournament_season_window(now)
        sleep_seconds = max(1, int((current_end - now).total_seconds()))

        await asyncio.sleep(sleep_seconds)

        try:
            previous_season_key = current_start.strftime("%Y-%m-%d")
            finalized = await finalize_weekly_tournament_season(previous_season_key)
            if finalized:
                logger.info("Weekly tournament season finalized: %s", previous_season_key)
        except Exception as e:
            logger.error(f"Error finalizing weekly tournament season: {e}")



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
        asyncio.create_task(weekly_tournament_rollover_loop())

    logger.info("вњ… Background tasks started")
    yield

    if redis_client:
        await redis_client.close()

    logger.info("рџ›‘ Shutting down")

# ==================== CORS ====================
app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_CORS_ORIGINS + (["null"] if ALLOW_NULL_ORIGIN else []),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/auth/session")
async def create_api_session(request: Request):
    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    token, expires_at = issue_session_token(telegram_user)
    return {
        "success": True,
        "token": token,
        "token_type": "Bearer",
        "expires_in": SESSION_TOKEN_TTL_SECONDS,
        "expires_at": expires_at,
        "user_id": int(telegram_user.get("id", 0)),
    }


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    path = request.url.path
    method = request.method
    status_code = 500
    try:
        if path.startswith("/api/") and path not in {"/api/ads/monetag/postback"}:
            request_ip = get_request_ip(request)
            ip_allowed = await redis_rate_limit(f"rl:global_api:ip:{request_ip}", 240, 60)
            if not ip_allowed:
                RATE_LIMIT_REJECTS.labels(namespace="global_api_ip").inc()
                status_code = 429
                return JSONResponse(status_code=429, content={"detail": "Too many requests from this IP"})

        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; "
            "frame-ancestors 'none'; "
            "base-uri 'none'; "
            "form-action 'none'; "
            "object-src 'none'"
        )
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


def get_request_ip(request: Request) -> str:
    forwarded_ip = (request.headers.get("cf-connecting-ip") or "").strip()
    if forwarded_ip:
        return forwarded_ip

    x_forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()

    return (request.client.host if request.client else "") or "unknown"


async def require_ip_rate_limit(namespace: str, request: Request, limit: int, window_seconds: int):
    request_ip = get_request_ip(request)
    allowed = await redis_rate_limit(f"rl:{namespace}:ip:{request_ip}", limit, window_seconds)
    if not allowed:
        RATE_LIMIT_REJECTS.labels(namespace=f"{namespace}_ip").inc()
        raise HTTPException(status_code=429, detail="Too many requests from this IP")


async def require_dual_rate_limit(
    namespace: str,
    request: Request,
    user_id: int,
    user_limit: int,
    window_seconds: int,
    *,
    ip_limit: int | None = None,
):
    await require_redis_rate_limit(namespace, user_id, user_limit, window_seconds)
    await require_ip_rate_limit(namespace, request, ip_limit or user_limit, window_seconds)


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
        await touch_user_activity(user_id, user)

        now = datetime.utcnow()
        current_energy = calculate_current_energy(user, now)
        max_energy = resolve_max_energy(user)

        if int(user.get("max_energy", max_energy)) != max_energy or int(user.get("energy", current_energy)) > max_energy:
            await update_user(user_id, {
                "max_energy": max_energy,
                "energy": min(current_energy, max_energy),
            })
            await invalidate_user_cache(user_id)


        extra = parse_extra_data(user.get("extra_data"))

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins)
        ghost_boost_active, ghost_boost_expires_at = get_ghost_boost_status(user)
        daily_infinite_energy_active, daily_infinite_energy_expires_at = is_daily_infinite_energy_active(user)
        task_tap_boost_active, task_tap_boost_expires_at, task_tap_boost_multiplier = get_active_video_task_boost(extra, "tap_boost")
        task_passive_boost_active, task_passive_boost_expires_at, task_passive_boost_multiplier = get_active_video_task_boost(extra, "passive_boost")
        multitap_level = int(user.get("multitap_level", 0))
        profit_level = int(user.get("profit_level", 0))
        energy_level = int(user.get("energy_level", 0))
        profit_per_tap = get_tap_value(multitap_level)
        profit_per_hour = get_hour_value(profit_level)

        if owned_skins != extra.get("owned_skins") or selected_skin != extra.get("selected_skin", DEFAULT_SKIN_ID):
            extra["owned_skins"] = owned_skins
            extra["selected_skin"] = selected_skin
            await update_user(user_id, {"extra_data": extra})
            await invalidate_user_cache(user_id)

        return {
            "user_id": user["user_id"],
            "username": user.get("username"),
            "coins": user.get("coins", 0),
            "energy": current_energy,
            "max_energy": max_energy,
            "profit_per_tap": profit_per_tap,
            "profit_per_hour": profit_per_hour,
            "multitap_level": multitap_level,
            "profit_level": profit_level,
            "energy_level": energy_level,
            "owned_skins": owned_skins,
            "selected_skin": selected_skin,
            "ads_watched": extra.get("ads_watched", 0),
            "ghost_boost_active": ghost_boost_active,
            "ghost_boost_expires_at": ghost_boost_expires_at,
            "task_tap_boost_active": task_tap_boost_active,
            "task_tap_boost_expires_at": task_tap_boost_expires_at,
            "task_tap_boost_multiplier": task_tap_boost_multiplier,
            "task_passive_boost_active": task_passive_boost_active,
            "task_passive_boost_expires_at": task_passive_boost_expires_at,
            "task_passive_boost_multiplier": task_passive_boost_multiplier,
            "daily_infinite_energy_active": daily_infinite_energy_active,
            "daily_infinite_energy_expires_at": daily_infinite_energy_expires_at,
            "skin_ad_progress": get_skin_ad_progress(extra),
            "skin_ad_last_watch": get_skin_ad_last_watch(extra),
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
                    cooldown_until = parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
                    if cooldown_until and cooldown_until > now:
                        return {
                            "active": False,
                            "cooldown_active": True,
                            "cooldown_until": cooldown_until.isoformat(),
                            "cooldown_remaining_seconds": int((cooldown_until - now).total_seconds())
                        }
                    return {"active": False, "cooldown_active": False}
                else:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "active": True, 
                        "expires_at": active_boosts["mega_boost"]["expires_at"], 
                        "remaining_seconds": remaining
                    }
            except:
                pass

        cooldown_until = parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
        if cooldown_until and cooldown_until > now:
            return {
                "active": False,
                "cooldown_active": True,
                "cooldown_until": cooldown_until.isoformat(),
                "cooldown_remaining_seconds": int((cooldown_until - now).total_seconds())
            }
        if cooldown_until and cooldown_until <= now:
            extra.pop("mega_boost_cooldown_until", None)
            await update_user(user_id, {"extra_data": extra})
            await invalidate_user_cache(user_id)

        return {"active": False, "cooldown_active": False}
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/activate-mega-boost")
async def activate_mega_boost(payload: AdActionClaimRequest, request: Request):
    """Activate mega boost (x2 coins + infinite energy for 1 minute)"""
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("activate_mega_boost", request, payload.user_id, 10, 60, ip_limit=20)
        await consume_ad_action_session(payload.user_id, payload.ad_session_id, "mega_boost")
        user = await get_user_cached(payload.user_id)
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

        cooldown_until = parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(status_code=429, detail=f"Mega boost cooldown {remaining // 60}:{remaining % 60:02d}")
        if cooldown_until and now >= cooldown_until:
            extra.pop("mega_boost_cooldown_until", None)
        
        expires_at = (now + timedelta(minutes=MEGA_BOOST_MINUTES)).isoformat()
        cooldown_minutes = MEGA_BOOST_COOLDOWN_MAX_MINUTES
        cooldown_until_value = (now + timedelta(minutes=cooldown_minutes)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["mega_boost_cooldown_until"] = cooldown_until_value
        extra["active_boosts"] = active_boosts
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(payload.user_id, "boost", {"source_action": "mega_boost"})
        
        return {
            "success": True,
            "message": "рџ”ҐвљЎ MEGA BOOST activated for 1 minute! x2 coins + infinite energy",
            "expires_at": expires_at,
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": cooldown_minutes
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/ghost-boost-status/{user_id}")
async def get_ghost_boost_status_endpoint(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        active, expires_at = get_ghost_boost_status(user)
        if not active or not expires_at:
            return {"active": False}

        remaining = max(0, int((datetime.fromisoformat(expires_at) - datetime.utcnow()).total_seconds()))
        return {
            "active": True,
            "expires_at": expires_at,
            "remaining_seconds": remaining,
            "multiplier": GHOST_BOOST_MULTIPLIER,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ghost_boost_status_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/activate-ghost-boost")
async def activate_ghost_boost(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("activate_ghost_boost", request, payload.user_id, 10, 60, ip_limit=20)
        await consume_ad_action_session(payload.user_id, payload.ad_session_id, "ghost_boost")
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        active_boosts = extra.get("active_boosts", {})
        if not isinstance(active_boosts, dict):
            active_boosts = {}

        now = datetime.utcnow()
        ghost_boost = active_boosts.get("ghost_boost")
        if ghost_boost and ghost_boost.get("expires_at"):
            try:
                expires = datetime.fromisoformat(ghost_boost["expires_at"])
                if now < expires:
                    remaining = max(0, int((expires - now).total_seconds()))
                    return {
                        "success": False,
                        "already_active": True,
                        "expires_at": ghost_boost["expires_at"],
                        "remaining_seconds": remaining,
                        "multiplier": GHOST_BOOST_MULTIPLIER,
                    }
            except Exception:
                pass

        expires_at = (now + timedelta(minutes=GHOST_BOOST_MINUTES)).isoformat()
        active_boosts["ghost_boost"] = {
            "active": True,
            "expires_at": expires_at,
            "multiplier": GHOST_BOOST_MULTIPLIER,
        }
        extra["active_boosts"] = active_boosts

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(payload.user_id, "ghost", {"source_action": "ghost_boost"})

        return {
            "success": True,
            "expires_at": expires_at,
            "remaining_seconds": GHOST_BOOST_MINUTES * 60,
            "multiplier": GHOST_BOOST_MULTIPLIER,
            "message": "Ghost boost activated",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_ghost_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video")
async def reward_video(payload: RewardVideoClaimRequest, request: Request):
    raise HTTPException(status_code=410, detail="Deprecated reward flow")

@app.post("/api/reward-video/start")
async def reward_video_start(payload: RewardVideoStartRequest, request: Request):
    raise HTTPException(status_code=410, detail="Deprecated reward flow")


@app.post("/api/ad-action/start")
async def ad_action_start(payload: AdActionStartRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("ad_action_start", request, payload.user_id, 20, 60, ip_limit=40)

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        ad_session_id = await create_ad_action_session(payload.user_id, payload.action)
        return {
            "success": True,
            "ad_session_id": ad_session_id,
            "action": payload.action,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in ad_action_start: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.api_route("/api/ads/monetag/postback", methods=["GET", "POST"])
async def monetag_postback(request: Request):
    try:
        params = {str(k): str(v) for k, v in request.query_params.items()}

        if request.method == "POST":
            content_type = (request.headers.get("content-type") or "").lower()
            if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
                form = await request.form()
                for key, value in form.items():
                    params[str(key)] = str(value)
            elif "application/json" in content_type:
                payload = await request.json()
                if isinstance(payload, dict):
                    for key, value in payload.items():
                        params[str(key)] = str(value)

        if MONETAG_POSTBACK_SECRET:
            provided_secret = extract_first_value(params, MONETAG_POSTBACK_SECRET_KEYS)
            if provided_secret != MONETAG_POSTBACK_SECRET:
                logger.warning("Monetag postback rejected: invalid secret")
                raise HTTPException(status_code=403, detail="Invalid postback secret")

        status_hints = [
            params.get("status"),
            params.get("state"),
            params.get("event"),
            params.get("result"),
            params.get("rewarded"),
            params.get("completed"),
        ]
        negative_status = any(
            hint is not None and str(hint).strip().lower() in MONETAG_POSTBACK_NEGATIVE_VALUES
            for hint in status_hints
        )
        if negative_status:
            logger.info("Monetag postback ignored as incomplete: %s", params)
            return Response(content="IGNORED", media_type="text/plain", status_code=200)

        ad_session_id = extract_first_value(params, MONETAG_POSTBACK_ID_KEYS)
        if not ad_session_id:
            logger.debug("Monetag postback without session id ignored: %s", params)
            return Response(content="IGNORED", media_type="text/plain", status_code=200)

        verified = await mark_ad_action_session_verified(ad_session_id, params)
        if not verified:
            logger.warning("Monetag postback could not find ad session %s", ad_session_id)
            return Response(content="SESSION_NOT_FOUND", media_type="text/plain", status_code=404)

        return Response(content="OK", media_type="text/plain", status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in monetag_postback: {e}")
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
async def increment_ads_watched(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("ads_increment", request, payload.user_id, 20, 60, ip_limit=40)
        await consume_ad_action_session(payload.user_id, payload.ad_session_id, "ads_increment")
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

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

        skin_id = LEGACY_SKIN_ID_MAP.get(payload.skin_id, payload.skin_id) if payload.skin_id else None
        current_count = 0
        required_count = 0
        cooldown_remaining_seconds = 0
        ready_to_unlock = False

        if skin_id:
            if skin_id not in VIDEO_SKIN_IDS:
                raise HTTPException(status_code=400, detail="Unknown ad skin")

            progress = get_skin_ad_progress(extra)
            last_watch = get_skin_ad_last_watch(extra)
            required_count = int(SKIN_REQUIREMENTS.get(skin_id, {}).get("count", 1))
            current_count = int(progress.get(skin_id, 0) or 0)

            if current_count >= required_count:
                ready_to_unlock = True
            else:
                last_watch_at = parse_iso_datetime(last_watch.get(skin_id))
                now = datetime.utcnow()
                if last_watch_at:
                    next_allowed = last_watch_at + timedelta(minutes=SKIN_AD_COOLDOWN_MINUTES)
                    if next_allowed > now:
                        cooldown_remaining_seconds = int((next_allowed - now).total_seconds())
                        raise HTTPException(
                            status_code=429,
                            detail=f"Skin ad cooldown {cooldown_remaining_seconds // 60}:{cooldown_remaining_seconds % 60:02d}"
                        )

                current_count = min(required_count, current_count + 1)
                progress[skin_id] = current_count
                last_watch[skin_id] = now.isoformat()
                extra["skin_ad_progress"] = progress
                extra["skin_ad_last_watch"] = last_watch
                ready_to_unlock = current_count >= required_count

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(payload.user_id, "skins", {"source_action": "ads_increment", "skin_id": skin_id})

        return {
            "success": True,
            "ads_watched": ads_watched,
            "skin_id": skin_id,
            "current_count": current_count,
            "required_count": required_count,
            "ready_to_unlock": ready_to_unlock,
            "cooldown_minutes": SKIN_AD_COOLDOWN_MINUTES,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in increment_ads_watched: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def get_global_upgrade_level(user: dict) -> int:
    return max(
        int(user.get("multitap_level", 0)),
        int(user.get("profit_level", 0)),
        int(user.get("energy_level", 0)),
    )


async def apply_global_upgrade_for_user(user_id: int, user: dict) -> dict:
    current_level = get_global_upgrade_level(user)
    if current_level >= MAX_UPGRADE_LEVEL:
        raise HTTPException(status_code=400, detail="Max level reached")

    price = GLOBAL_UPGRADE_PRICES[current_level]
    current_coins = int(user.get("coins", 0))
    if current_coins < price:
        raise HTTPException(status_code=400, detail="Not enough coins")

    new_level = current_level + 1
    new_profit_per_tap = get_tap_value(new_level)
    new_profit_per_hour = get_hour_value(new_level)
    new_max_energy = get_max_energy(new_level)
    new_coins = current_coins - price

    updates = {
        "coins": new_coins,
        "multitap_level": new_level,
        "profit_level": new_level,
        "energy_level": new_level,
        "profit_per_tap": new_profit_per_tap,
        "profit_per_hour": new_profit_per_hour,
        "max_energy": new_max_energy,
        "energy": new_max_energy,
    }

    await update_user(user_id, updates)
    await invalidate_user_cache(user_id)

    next_cost = GLOBAL_UPGRADE_PRICES[new_level] if new_level < len(GLOBAL_UPGRADE_PRICES) else 0
    return {
        "success": True,
        "coins": new_coins,
        "new_level": new_level,
        "levels": {
            "multitap": new_level,
            "profit": new_level,
            "energy": new_level,
        },
        "prices": {
            "global": next_cost,
        },
        "next_cost": next_cost,
        "profit_per_tap": new_profit_per_tap,
        "profit_per_hour": new_profit_per_hour,
        "max_energy": new_max_energy,
        "energy": new_max_energy,
    }


@app.post("/api/upgrade")
async def process_upgrade(payload: UpgradeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("upgrade", request, payload.user_id, 25, 60, ip_limit=50)
        await require_user_action_lock("upgrade", payload.user_id, ttl=0.35)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return await apply_global_upgrade_for_user(payload.user_id, user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/upgrade-all")
async def process_upgrade_all(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("upgrade_all", request, payload.user_id, 25, 60, ip_limit=50)
        await require_user_action_lock("upgrade_all", payload.user_id, ttl=0.35)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return await apply_global_upgrade_for_user(payload.user_id, user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade_all: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/update-energy")
async def update_energy(payload: AdActionClaimRequest, request: Request):
    try:
        user_id = payload.user_id
        await require_telegram_user(request, user_id)
        await require_dual_rate_limit("update_energy", request, user_id, 10, 60, ip_limit=20)
        await consume_ad_action_session(user_id, payload.ad_session_id, "energy_refill_max")
        await require_user_action_lock("update_energy", user_id, ttl=3)
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id required")
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        max_energy = resolve_max_energy(user)
        extra = parse_extra_data(user.get("extra_data"))
        cooldown_until = parse_iso_datetime(extra.get("energy_refill_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(status_code=429, detail=f"Energy refill cooldown active. Try again in {format_duration(remaining)}")
        if cooldown_until and now >= cooldown_until:
            extra.pop("energy_refill_cooldown_until", None)

        cooldown_until_value = (now + timedelta(minutes=ENERGY_REFILL_COOLDOWN_MINUTES)).isoformat()
        extra["energy_refill_cooldown_until"] = cooldown_until_value

        await update_user(user_id, {
            "max_energy": max_energy,
            "energy": max_energy,
            "last_energy_update": now,
            "extra_data": extra
        })
        await invalidate_user_cache(user_id)
        await record_rewarded_ad_claim(user_id, "energy_restore", {"source_action": "energy_refill_max"})

        return {
            "success": True,
            "energy": max_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": ENERGY_REFILL_COOLDOWN_MINUTES
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/autoclicker/activate")
async def activate_autoclicker(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("activate_autoclicker", request, payload.user_id, 10, 60, ip_limit=20)
        await consume_ad_action_session(payload.user_id, payload.ad_session_id, "autoclicker")
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        now = datetime.utcnow()
        cooldown_until = parse_iso_datetime(extra.get("autoclicker_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(status_code=429, detail=f"Autoclicker cooldown {remaining // 60}:{remaining % 60:02d}")
        if cooldown_until and now >= cooldown_until:
            extra.pop("autoclicker_cooldown_until", None)

        cooldown_until_value = (now + timedelta(minutes=AUTOCLICKER_COOLDOWN_MINUTES)).isoformat()
        extra["autoclicker_cooldown_until"] = cooldown_until_value
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(payload.user_id, "autoclicker", {"source_action": "autoclicker"})
        return {
            "success": True,
            "duration_seconds": 60,
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": AUTOCLICKER_COOLDOWN_MINUTES,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_autoclicker: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/recover-energy")
async def recover_energy_legacy(payload: UserIdRequest, request: Request):
    raise HTTPException(status_code=410, detail="Legacy endpoint disabled")

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
    DAILY_REWARD_SKIN_ID: 1.8,
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
    "telega.pngSP": 1.5,
    "tiktok.pngSP": 1.5,
    "insta.pngSP": 1.5,
}

LEGACY_SKIN_ID_MAP = {
    "referral-special.pngSP": REFERRAL_SPECIAL_SKIN_ID,
    "daily30.pngSP": DAILY_REWARD_SKIN_ID,
    "telegram-social.pngSP": "telega.pngSP",
    "tiktok-social.pngSP": "tiktok.pngSP",
    "instagram-social.pngSP": "insta.pngSP",
}

SOCIAL_SUB_TASK_SKINS = {
    "telegram_sub": "telega.pngSP",
    "tiktok_sub": "tiktok.pngSP",
    "instagram_sub": "insta.pngSP",
}

VALID_SKIN_IDS = set(SKIN_MULTIPLIERS.keys())


def normalize_owned_skins(raw_owned) -> list[str]:
    if isinstance(raw_owned, list):
        owned = [str(item) for item in raw_owned]
    else:
        owned = []

    normalized = []
    seen = set()
    for skin_id in owned:
        skin_id = LEGACY_SKIN_ID_MAP.get(skin_id, skin_id)
        if skin_id in VALID_SKIN_IDS and skin_id not in seen:
            seen.add(skin_id)
            normalized.append(skin_id)

    if DEFAULT_SKIN_ID not in seen:
        normalized.insert(0, DEFAULT_SKIN_ID)

    return normalized


def normalize_selected_skin(selected_skin: str | None, owned_skins: list[str]) -> str:
    selected_skin = LEGACY_SKIN_ID_MAP.get(selected_skin, selected_skin)
    if selected_skin in owned_skins:
        return selected_skin
    return DEFAULT_SKIN_ID


def get_selected_skin_multiplier(user: dict) -> float:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
    selected_skin = normalize_selected_skin(extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins)
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


def get_ghost_boost_status(user: dict) -> tuple[bool, str | None]:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("ghost_boost")
    if not boost:
        return False, None

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, None

    if datetime.utcnow() >= expires_dt:
        return False, None

    return True, expires_at


def get_daily_reward_progress(extra: dict) -> tuple[int, str | None]:
    claimed_days = int(extra.get("daily_reward_claimed_days", 0) or 0)
    claimed_days = max(0, min(DAILY_REWARD_MAX_DAYS, claimed_days))
    last_claim_date = extra.get("daily_reward_last_claim_date")
    if not isinstance(last_claim_date, str):
        last_claim_date = None
    return claimed_days, last_claim_date


def is_daily_infinite_energy_active(user: dict) -> tuple[bool, str | None]:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("daily_infinite_energy")
    if not boost:
        return False, None

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, None

    if datetime.utcnow() >= expires_dt:
        return False, None

    return True, expires_at

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
        display_level = int(user.get("multitap_level", 0)) + 1
        return display_level >= int(req["value"])

    if req["type"] == "ads":
        progress = get_skin_ad_progress(extra)
        current = int(progress.get(skin_id, 0) or 0)
        return current >= int(req["count"])

    if req["type"] == "friends":
        referral_count = int(user.get("referral_count", 0))
        return referral_count >= int(req["count"])

    return False

@app.post("/api/clicks")
async def process_clicks_batch(payload: ClicksBatchRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("clicks", request, payload.user_id, 90, 60, ip_limit=180)
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
        await touch_user_activity(payload.user_id, user)

        now = datetime.utcnow()

        max_energy = resolve_max_energy(user)
        current_energy = calculate_current_energy(user, now)

        multitap_level = int(user.get("multitap_level", 0))
        tap_value = get_tap_value(multitap_level)

        extra = parse_extra_data(user.get("extra_data"))
        click_guard = get_click_guard_state(extra)
        last_click_at = parse_iso_datetime(click_guard.get("last_click_at"))

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins)
        skin_multiplier = float(SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = is_mega_boost_active(user)
        ghost_boost_active, ghost_boost_expires_at = get_ghost_boost_status(user)
        task_tap_boost_active, _, task_tap_boost_multiplier = get_active_video_task_boost(extra, "tap_boost")
        daily_infinite_energy_active, _ = is_daily_infinite_energy_active(user)
        free_energy_clicks = mega_boost_active or daily_infinite_energy_active or ghost_boost_active

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2
        if ghost_boost_active:
            coin_per_tap *= GHOST_BOOST_MULTIPLIER
        if task_tap_boost_active:
            coin_per_tap *= max(1, task_tap_boost_multiplier)

        # Р·Р°С‰РёС‚Р°: режем накопление "законных" кликов и опираемся на server-side last_click_at.
        safe_requested_clicks = min(payload.clicks, MAX_CLICK_BATCH_SIZE)
        allowed_clicks = get_allowed_clicks(
            user,
            now,
            safe_requested_clicks,
            last_click_at=last_click_at,
        )

        severe_overshoot = (
            safe_requested_clicks > allowed_clicks + CLICK_SUSPICIOUS_OVERSHOOT
            and safe_requested_clicks > max(allowed_clicks * 2, CLICK_BURST_ALLOWANCE * 2)
        )
        if severe_overshoot:
            click_guard["hard_rejections"] = int(click_guard.get("hard_rejections", 0)) + 1
            click_guard["last_rejection_at"] = now.isoformat()
            click_guard["last_reason"] = (
                f"Click batch overshoot: requested={safe_requested_clicks}, allowed={allowed_clicks}"
            )
            write_click_guard_state(extra, click_guard)
            await update_user(payload.user_id, {"extra_data": extra})
            await invalidate_user_cache(payload.user_id)
            logger.warning(
                "Rejected suspicious click batch user=%s ip=%s requested=%s allowed=%s",
                payload.user_id,
                get_request_ip(request),
                safe_requested_clicks,
                allowed_clicks,
            )
            raise HTTPException(status_code=429, detail="Click rate too high")

        effective_clicks = allowed_clicks if free_energy_clicks else min(allowed_clicks, current_energy)
        gained = effective_clicks * coin_per_tap

        # РЅРѕРІС‹Рµ Р·РЅР°С‡РµРЅРёСЏ
        new_energy = current_energy if free_energy_clicks else max(0, current_energy - effective_clicks)
        new_coins = int(user.get("coins", 0)) + gained

        update_data = {
            "coins": new_coins,
            "max_energy": max_energy,
        }

        suspicion_score = int(click_guard.get("suspicion_score", 0))
        if safe_requested_clicks > allowed_clicks:
            suspicion_score += 1
            click_guard["last_reason"] = (
                f"Requested {safe_requested_clicks} clicks while server allowed {allowed_clicks}"
            )
        elif suspicion_score > 0:
            suspicion_score -= 1
            click_guard.pop("last_reason", None)

        click_guard["suspicion_score"] = min(12, max(0, suspicion_score))
        click_guard["last_click_at"] = now.isoformat()
        click_guard["last_requested_clicks"] = safe_requested_clicks
        click_guard["last_allowed_clicks"] = allowed_clicks
        click_guard["last_effective_clicks"] = effective_clicks
        click_guard["updated_at"] = now.isoformat()
        if click_guard["suspicion_score"] >= CLICK_SUSPICION_SOFT_LIMIT:
            click_guard["flagged_at"] = now.isoformat()

        write_click_guard_state(extra, click_guard)
        update_data["extra_data"] = extra

        if free_energy_clicks:
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

        # РЎРѕС…СЂР°РЅСЏРµРј СЌРЅРµСЂРіРёСЋ Рё Р±Р°Р»Р°РЅСЃ РѕРґРЅРёРј atomic update, С‡С‚РѕР±С‹ РіРѕРЅРєРё
        # РјРµР¶РґСѓ РєР»РёРєР°РјРё/РїР°СЃСЃРёРІРєРѕР№/СЌРЅРµСЂРіРёРµР№ РЅРµ РїРµСЂРµС‚РёСЂР°Р»Рё СЃРѕСЃС‚РѕСЏРЅРёРµ.
        updated_user = await update_user_if_matches(
            payload.user_id,
            {
                "coins": int(user.get("coins", 0)),
                "energy": int(user.get("energy", 0)),
                "last_energy_update": normalize_dt(user.get("last_energy_update")),
            },
            update_data,
        )
        if not updated_user:
            logger.warning("Atomic click update conflict for user=%s", payload.user_id)
            raise HTTPException(status_code=409, detail="Click state changed, retry")

        conn = await get_redis_or_none()
        if conn and gained > 0:
            # РўСѓСЂРЅРёСЂ РѕСЃС‚Р°РІР»СЏРµРј РІ Redis РєР°Рє Р±С‹СЃС‚СЂС‹Р№ leaderboard СЃР»РѕР№.
            await conn.zincrby(
                TOURNAMENT_KEY,
                gained,
                str(payload.user_id)
            )
        if gained > 0:
            current_display_level = max(
                int(user.get("multitap_level", 0)),
                int(user.get("profit_level", 0)),
                int(user.get("energy_level", 0)),
            ) + 1
            await add_weekly_tournament_score(
                payload.user_id,
                user.get("username"),
                current_display_level,
                gained,
            )

        # вњ… РёРЅРІР°Р»РёРґРёСЂСѓРµРј РєСЌС€
        await invalidate_user_cache(payload.user_id)
        referral_bonus = await grant_referral_share_bonus(updated_user, gained)

        return {
            "success": True,
            "coins": int(updated_user.get("coins", new_coins)),
            "energy": int(updated_user.get("energy", new_energy)),
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "gained": gained,
            "effective_clicks": effective_clicks,
            "coin_per_tap": coin_per_tap,
            "profit_per_tap": tap_value,
            "profit_per_hour": get_hour_value(int(user.get("profit_level", 0))),
            "mega_boost_active": mega_boost_active,
            "ghost_boost_active": ghost_boost_active,
            "ghost_boost_expires_at": ghost_boost_expires_at,
            "daily_infinite_energy_active": daily_infinite_energy_active,
            "click_guard_suspicion_score": click_guard["suspicion_score"],
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

        global_level = get_global_upgrade_level(user)
        global_price = GLOBAL_UPGRADE_PRICES[global_level] if global_level < len(GLOBAL_UPGRADE_PRICES) else 0

        return {
            "global": global_price,
            "multitap": global_price,
            "profit": global_price,
            "energy": global_price,
        }
    except Exception as e:
        logger.error(f"Error in get_upgrade_prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/register")
async def register_user(payload: RegisterRequest, request: Request):
    try:
        telegram_user = await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("register", request, payload.user_id, 10, 60, ip_limit=20)
        await require_user_action_lock("register", payload.user_id, ttl=5)
        existing = await get_user(payload.user_id)
        valid_referrer_id = None
        requested_referrer_id = int(payload.referrer_id or 0)
        if requested_referrer_id and requested_referrer_id != payload.user_id:
            referrer = await get_user_cached(requested_referrer_id)
            if referrer and int(referrer.get("referrer_id") or 0) != payload.user_id:
                valid_referrer_id = requested_referrer_id

        if existing:
            username = telegram_user.get("username") or payload.username
            updates = {}
            if username and username != existing.get("username"):
                updates["username"] = username

            # Allow a one-time referral attachment for a fresh account that was
            # created before the WebApp received the referral param.
            can_attach_referrer = (
                valid_referrer_id
                and not existing.get("referrer_id")
                and int(existing.get("coins", 0) or 0) == 0
                and int(existing.get("level", 0) or 0) == 0
                and int(existing.get("referral_count", 0) or 0) == 0
                and int(existing.get("referral_earnings", 0) or 0) == 0
            )
            if can_attach_referrer:
                updates["referrer_id"] = valid_referrer_id

            if updates:
                await update_user(payload.user_id, updates)
                await invalidate_user_cache(payload.user_id)
                if can_attach_referrer:
                    await add_referral_bonus(valid_referrer_id, payload.user_id)
                    await invalidate_user_cache(valid_referrer_id)

            refreshed = await get_user_cached(payload.user_id)
            return {"status": "exists", "user": refreshed or existing}

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
        await require_user_action_lock("game:coinflip", payload.user_id, ttl=0.75)
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
        await require_user_action_lock("game:slots", payload.user_id, ttl=0.75)
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
        await require_user_action_lock("game:dice", payload.user_id, ttl=0.75)
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
        await require_user_action_lock("game:roulette", payload.user_id, ttl=0.75)
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


@app.post("/api/game/luckybox")
async def play_luckybox(payload: LuckyBoxRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:luckybox", payload.user_id, ttl=0.75)
        await require_redis_rate_limit("game_action", payload.user_id, 30, 60)

        user = await get_user_cached(payload.user_id)
        current_coins = int(user.get("coins", 0)) if user else 0
        if not user or current_coins < payload.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")

        outcomes = [0.0, 0.8, 1.6, 3.5]
        random.shuffle(outcomes)
        multiplier = float(outcomes[payload.box_index])
        payout = max(0, int(payload.bet * multiplier))
        new_coins = current_coins - payload.bet + payout

        if multiplier > 1:
            message = f"Lucky hit! x{multiplier:g} +{payout - payload.bet}"
            outcome = "win"
        elif multiplier == 0.8:
            message = f"Soft save. You kept {payout} coins."
            outcome = "refund"
        else:
            message = f"Bust. You lost {payload.bet} coins."
            outcome = "lose"

        await update_user(payload.user_id, {"coins": new_coins})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "coins": new_coins,
            "message": message,
            "outcome": outcome,
            "multiplier": multiplier,
            "payout": payout,
            "profit": payout - payload.bet,
            "outcomes": outcomes,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in luckybox: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/game/crash/start")
async def start_crash_ghost_round(payload: CrashGameStartRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:crash_start", payload.user_id, ttl=0.75)
        await require_redis_rate_limit("game_action", payload.user_id, 30, 60)

        redis_conn = await ensure_redis_available()
        user = await get_user_cached(payload.user_id)
        if not user or int(user.get("coins", 0)) < payload.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")

        session_key = f"game:crash:{payload.user_id}"
        raw_session = await redis_conn.get(session_key)
        if raw_session:
            try:
                active_session = json.loads(raw_session)
                runtime = get_crash_ghost_runtime(active_session)
                if not runtime["crashed"] and not active_session.get("claimed"):
                    raise HTTPException(status_code=409, detail="Crash round already active")
            except HTTPException:
                raise
            except Exception:
                pass

        new_coins = int(user.get("coins", 0)) - payload.bet
        session = build_crash_ghost_session(payload.bet, payload.user_id)
        session["session_id"] = f"{payload.user_id}:{int(session['started_at'] * 1000)}:{random.randint(100000, 999999)}"

        await update_user(payload.user_id, {"coins": new_coins})
        await invalidate_user_cache(payload.user_id)
        await redis_conn.setex(session_key, CRASH_GHOST_SESSION_TTL_SECONDS, json.dumps(session))

        return {
            "success": True,
            "session_id": session["session_id"],
            "coins": new_coins,
            "multiplier": 1.0,
            "server_started_at": datetime.utcfromtimestamp(session["started_at"]).isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in start_crash_ghost_round: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/game/crash/status/{user_id}/{session_id}")
async def get_crash_ghost_status(user_id: int, session_id: str, request: Request):
    try:
        await require_telegram_user(request, user_id)
        redis_conn = await ensure_redis_available()
        session_key = f"game:crash:{user_id}"
        raw_session = await redis_conn.get(session_key)
        if not raw_session:
            return {"active": False}

        session = json.loads(raw_session)
        if session.get("session_id") != session_id:
            raise HTTPException(status_code=404, detail="Crash round not found")

        runtime = get_crash_ghost_runtime(session)
        if runtime["crashed"]:
            await redis_conn.delete(session_key)
            return {
                "active": False,
                "crashed": True,
                "multiplier": runtime["crash_at"],
                "crash_at": runtime["crash_at"],
            }

        return {
            "active": True,
            "crashed": False,
            "multiplier": runtime["multiplier"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_crash_ghost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/game/crash/cashout")
async def cashout_crash_ghost_round(payload: CrashGameCashoutRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("game:crash_cashout", payload.user_id, ttl=0.35)
        await require_redis_rate_limit("game_action", payload.user_id, 40, 60)

        redis_conn = await ensure_redis_available()
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        session_key = f"game:crash:{payload.user_id}"
        raw_session = await redis_conn.get(session_key)
        if not raw_session:
            raise HTTPException(status_code=404, detail="Crash round not found")

        session = json.loads(raw_session)
        if session.get("session_id") != payload.session_id:
            raise HTTPException(status_code=404, detail="Crash round not found")

        runtime = get_crash_ghost_runtime(session)
        if runtime["crashed"]:
            await redis_conn.delete(session_key)
            return {
                "success": False,
                "crashed": True,
                "coins": int(user.get("coins", 0)),
                "crash_at": runtime["crash_at"],
                "message": f"Ghost crashed. You lost {session.get('bet', 0)} coins.",
            }

        bet = int(session.get("bet", 0))
        payout = max(0, int(bet * runtime["multiplier"]))
        new_coins = int(user.get("coins", 0)) + payout

        await update_user(payload.user_id, {"coins": new_coins})
        await invalidate_user_cache(payload.user_id)
        await redis_conn.delete(session_key)

        return {
            "success": True,
            "crashed": False,
            "coins": new_coins,
            "payout": payout,
            "profit": payout - bet,
            "multiplier": runtime["multiplier"],
            "message": f"Ghost paid {payout} coins. Profit: +{payout - bet}",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in cashout_crash_ghost_round: {e}")
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


@app.get("/api/weekly-tournament/overview/{user_id}")
async def get_weekly_tournament_overview(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        now = datetime.utcnow()
        starts_at, ends_at = get_weekly_tournament_season_window(now)
        season_key = get_weekly_tournament_season_key(now)
        player = await get_weekly_tournament_player_entry(user_id, season_key)

        return {
            "success": True,
            "season_key": season_key,
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "time_left_seconds": max(0, int((ends_at - now).total_seconds())),
            "leagues": WEEKLY_LEAGUE_LEVEL_RANGES,
            "fund_splits": WEEKLY_LEAGUE_FUND_SPLITS,
            "top3_splits": WEEKLY_TOP3_PAYOUT_SPLITS,
            "rest_split": max(0.0, 1.0 - sum(WEEKLY_TOP3_PAYOUT_SPLITS.values())),
            "payout_splits": {
                "top": WEEKLY_TOP3_PAYOUT_SPLITS,
                "ranges": WEEKLY_RANGE_PAYOUT_SPLITS,
            },
            "player": player,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/weekly-tournament/leaderboard/{league}")
async def get_weekly_tournament_leaderboard_endpoint(league: str, season_key: str | None = None, limit: int = 50):
    try:
        league = (league or "").strip().lower()
        if league not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")

        effective_season_key = season_key or get_weekly_tournament_season_key()
        rows = await get_weekly_tournament_leaderboard(
            season_key=effective_season_key,
            league=league,
            limit=limit,
        )
        return {
            "success": True,
            "season_key": effective_season_key,
            "league": league,
            "players": rows,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_leaderboard_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/weekly-tournament/player/{user_id}")
async def get_weekly_tournament_player_endpoint(user_id: int, request: Request, season_key: str | None = None):
    try:
        await require_telegram_user(request, user_id)
        effective_season_key = season_key or get_weekly_tournament_season_key()
        player = await get_weekly_tournament_player_entry(user_id, effective_season_key)
        return {
            "success": True,
            "season_key": effective_season_key,
            "player": player,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_player_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/weekly-tournament/results/{league}")
async def get_weekly_tournament_results_endpoint(league: str, season_key: str | None = None, limit: int = 50):
    try:
        league = (league or "").strip().lower()
        if league not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")

        season_rows = await list_weekly_tournament_seasons(limit=52)
        if season_key:
            season = next((item for item in season_rows if item["season_key"] == season_key and item["status"] == "finalized"), None)
        else:
            season = next((item for item in season_rows if item["status"] == "finalized"), None)

        if not season:
            return {
                "success": True,
                "league": league,
                "season": None,
                "players": [],
            }

        winners = await get_weekly_tournament_winners(season["season_key"], league=league)
        return {
            "success": True,
            "league": league,
            "season": season,
            "players": winners[:max(1, min(50, int(limit or 50)))],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_results_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/admin/weekly-tournament/seasons")
async def admin_weekly_tournament_seasons(request: Request, limit: int = 12):
    try:
        await require_admin_access(request)
        seasons = await list_weekly_tournament_seasons(limit=limit)
        return {
            "success": True,
            "seasons": seasons,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_weekly_tournament_seasons: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/admin/overview")
async def admin_overview(request: Request):
    try:
        await require_admin_access(request)
        now = datetime.utcnow()
        starts_at, ends_at = get_weekly_tournament_season_window(now)
        season_key = get_weekly_tournament_season_key(now)
        online_now = await get_online_users_count()
        season_rows = await list_weekly_tournament_seasons(limit=12)
        active_season = next((item for item in season_rows if item["season_key"] == season_key), None)

        async with AsyncSessionLocal() as session:
            total_users_result = await session.execute(select(func.count(User.id)))
            total_users = int(total_users_result.scalar() or 0)

            league_counts_result = await session.execute(
                select(
                    WeeklyTournamentEntry.league,
                    func.count(WeeklyTournamentEntry.id)
                ).where(
                    WeeklyTournamentEntry.season_key == season_key
                ).group_by(WeeklyTournamentEntry.league)
            )
            league_counts = {league: 0 for league in WEEKLY_LEAGUE_ORDER}
            for league, count in league_counts_result.all():
                if league in league_counts:
                    league_counts[league] = int(count or 0)

        top_preview = {}
        for league in WEEKLY_LEAGUE_ORDER:
            players = await get_weekly_tournament_leaderboard(season_key=season_key, league=league, limit=3)
            top_preview[league] = players

        return {
            "success": True,
            "generated_at": now.isoformat(),
            "online_now": online_now,
            "total_users": total_users,
            "season_key": season_key,
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "time_left_seconds": max(0, int((ends_at - now).total_seconds())),
            "active_season": active_season,
            "league_counts": league_counts,
            "top_preview": top_preview,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/admin/rewarded-ads/summary")
async def admin_rewarded_ads_summary(request: Request, hours: int = 24):
    try:
        await require_admin_access(request)
        summary = await get_rewarded_ads_admin_summary(hours=hours)
        tracked_actions = ("boost", "autoclicker", "tasks", "ghost", "energy_restore", "skins")
        actions = {
            action: {
                "total": int(summary["actions_total"].get(action, 0)),
                "recent": int(summary["actions_recent"].get(action, 0)),
            }
            for action in tracked_actions
        }
        return {
            "success": True,
            "hours_window": summary["hours_window"],
            "total_claims": summary["total_claims"],
            "recent_claims": summary["recent_claims"],
            "actions": actions,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_rewarded_ads_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/admin/stars-skins/summary")
async def admin_stars_skins_summary(request: Request, limit: int = 20):
    try:
        await require_admin_access(request)
        summary = await get_stars_skin_sales_admin_summary(limit=limit)
        return {"success": True, **summary}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_stars_skins_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/admin/fraud/overview")
async def admin_fraud_overview(request: Request, season_key: str | None = None):
    try:
        await require_admin_access(request)
        effective_season_key = season_key or get_weekly_tournament_season_key()
        players = await build_admin_fraud_overview(effective_season_key)
        return {
            "success": True,
            "season_key": effective_season_key,
            "players": players,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_fraud_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/admin/fraud/user/{user_id}")
async def admin_update_fraud_status(user_id: int, payload: AdminFraudUpdateRequest, request: Request):
    try:
        await require_admin_access(request)
        status = (payload.status or "").strip().lower()
        if status not in {"fraud", "ok"}:
            raise HTTPException(status_code=400, detail="status must be fraud or ok")

        effective_season_key = payload.season_key or get_weekly_tournament_season_key()
        disqualify = bool(payload.disqualify_from_payout)
        await upsert_admin_fraud_review(user_id, status, payload.reason, disqualify)

        async with AsyncSessionLocal() as session:
            entry_result = await session.execute(
                select(WeeklyTournamentEntry).where(
                    WeeklyTournamentEntry.season_key == effective_season_key,
                    WeeklyTournamentEntry.user_id == user_id,
                )
            )
            entry = entry_result.scalar_one_or_none()
            if entry:
                entry.fraud_flag = status == "fraud"
                entry.eligible_for_payout = not disqualify

            winner_result = await session.execute(
                select(WeeklyTournamentWinner).where(
                    WeeklyTournamentWinner.season_key == effective_season_key,
                    WeeklyTournamentWinner.user_id == user_id,
                )
            )
            winner = winner_result.scalar_one_or_none()
            if winner:
                winner.fraud_flag = status == "fraud"
                winner.eligible_for_payout = not disqualify
                if disqualify:
                    winner.payout_cents = 0
                    winner.stars_reward = 0

            await session.commit()

        return {
            "success": True,
            "user_id": user_id,
            "season_key": effective_season_key,
            "status": status,
            "disqualify_from_payout": disqualify,
            "reason": payload.reason,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_fraud_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/admin/weekly-tournament/season/{season_key}")
async def admin_weekly_tournament_season_detail(season_key: str, request: Request):
    try:
        await require_admin_access(request)
        season_rows = await list_weekly_tournament_seasons(limit=52)
        season = next((item for item in season_rows if item["season_key"] == season_key), None)
        winners = await get_weekly_tournament_winners(season_key)

        leagues = {}
        for league in WEEKLY_LEAGUE_ORDER:
            leagues[league] = {
                "range": WEEKLY_LEAGUE_LEVEL_RANGES[league],
                "fund_split": WEEKLY_LEAGUE_FUND_SPLITS[league],
                "top50": await get_weekly_tournament_leaderboard(season_key=season_key, league=league, limit=50),
                "winners": [winner for winner in winners if winner["league"] == league],
            }

        return {
            "success": True,
            "season": season,
            "season_key": season_key,
            "leagues": leagues,
            "top3_splits": WEEKLY_TOP3_PAYOUT_SPLITS,
            "rest_split": max(0.0, 1.0 - sum(WEEKLY_TOP3_PAYOUT_SPLITS.values())),
            "payout_splits": {
                "top": WEEKLY_TOP3_PAYOUT_SPLITS,
                "ranges": WEEKLY_RANGE_PAYOUT_SPLITS,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_weekly_tournament_season_detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/admin/weekly-tournament/season/{season_key}/fund")
async def admin_set_weekly_tournament_fund(
    season_key: str,
    payload: WeeklyTournamentFundRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        starts_at = datetime.strptime(season_key, "%Y-%m-%d")
        ends_at = starts_at + timedelta(days=7)

        async with AsyncSessionLocal() as session:
            season = await ensure_weekly_tournament_season(session, season_key, starts_at, ends_at)
            season.gross_ad_revenue_cents = int(payload.gross_ad_revenue_cents or 0)
            season.payout_fund_cents = int(payload.payout_fund_cents or 0)
            await session.commit()

        return {
            "success": True,
            "season_key": season_key,
            "gross_ad_revenue_cents": int(payload.gross_ad_revenue_cents or 0),
            "payout_fund_cents": int(payload.payout_fund_cents or 0),
            "league_splits": WEEKLY_LEAGUE_FUND_SPLITS,
            "rank_splits": {
                "top1": WEEKLY_TOP3_PAYOUT_SPLITS[1],
                "top2": WEEKLY_TOP3_PAYOUT_SPLITS[2],
                "top3": WEEKLY_TOP3_PAYOUT_SPLITS[3],
                "ranks_4_50": max(0.0, 1.0 - sum(WEEKLY_TOP3_PAYOUT_SPLITS.values())),
            },
        }
    except ValueError:
        raise HTTPException(status_code=400, detail="season_key must use YYYY-MM-DD format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_set_weekly_tournament_fund: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/admin/weekly-tournament/season/{season_key}/winner-stars")
async def admin_set_weekly_tournament_winner_stars(
    season_key: str,
    payload: AdminWinnerStarsUpdateRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            winner_result = await session.execute(
                select(WeeklyTournamentWinner).where(
                    WeeklyTournamentWinner.season_key == season_key,
                    WeeklyTournamentWinner.user_id == payload.user_id,
                )
            )
            winner = winner_result.scalar_one_or_none()
            if not winner:
                raise HTTPException(status_code=404, detail="Winner not found for this season")

            winner.stars_reward = int(payload.stars_reward or 0)
            await session.commit()

            return {
                "success": True,
                "season_key": season_key,
                "user_id": payload.user_id,
                "stars_reward": int(winner.stars_reward or 0),
                "league": winner.league,
                "rank": int(winner.rank or 0),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_set_weekly_tournament_winner_stars: {e}")
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
    """Get top 3 players from Redis leaderboard"""
    try:
        players = []

        conn = await get_redis_or_none()
        if conn:
            top_players = await conn.zrevrange(
                TOURNAMENT_KEY,
                0,
                2,
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
            {"id": "telegram_sub", "title": "Telegram Channel", "description": "Subscribe to Telegram channel",
             "reward": "20000 coins + skin", "icon": "📣", "completed": "telegram_sub" in completed_tasks},
            {"id": "tiktok_sub", "title": "TikTok", "description": "Subscribe to TikTok",
             "reward": "20000 coins + skin", "icon": "🎵", "completed": "tiktok_sub" in completed_tasks},
            {"id": "instagram_sub", "title": "Instagram", "description": "Subscribe to Instagram",
             "reward": "20000 coins + skin", "icon": "📸", "completed": "instagram_sub" in completed_tasks},
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
        await require_dual_rate_limit("complete_task", request, payload.user_id, RATE_LIMITS["complete_task"][0], RATE_LIMITS["complete_task"][1], ip_limit=RATE_LIMITS["complete_task"][0] * 2)
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

        elif task_id in SOCIAL_SUB_TASK_SKINS:
            extra = user.get("extra_data", {}) or {}
            if isinstance(extra, str):
                try:
                    extra = json.loads(extra)
                except Exception:
                    extra = {}

            owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
            social_skin_id = SOCIAL_SUB_TASK_SKINS[task_id]

            if task_id == "telegram_sub":
                is_verified = await verify_telegram_channel_subscription(payload.user_id)
                if not is_verified:
                    raise HTTPException(
                        status_code=400,
                        detail="Telegram subscription was not verified yet"
                    )

            if social_skin_id not in owned_skins:
                owned_skins.append(social_skin_id)

            extra["owned_skins"] = normalize_owned_skins(owned_skins)
            user["coins"] += 20000

            await add_completed_task(payload.user_id, task_id)
            await update_user(payload.user_id, {
                "coins": user["coins"],
                "extra_data": extra,
            })
            await invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "✅ +20000 coins + skin!",
                "coins": user["coins"],
                "skin_id": social_skin_id,
                "verified": task_id == "telegram_sub",
            }
        
        raise HTTPException(status_code=400, detail="Unknown task")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/video-tasks/status/{user_id}")
async def get_video_tasks_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        last_claims = get_video_task_last_claims(extra)
        now = datetime.utcnow()
        tasks = []

        for task_id, config in VIDEO_TASK_DEFINITIONS.items():
            claimed_at = parse_iso_datetime(last_claims.get(task_id))
            cooldown_seconds = int(config["cooldown_minutes"] * 60)
            remaining_seconds = 0
            available = True

            if claimed_at:
                elapsed = (now - claimed_at).total_seconds()
                remaining_seconds = max(0, cooldown_seconds - int(elapsed))
                available = remaining_seconds <= 0

            tasks.append({
                "task_id": task_id,
                "available": available,
                "remaining_seconds": remaining_seconds,
                "cooldown_minutes": config["cooldown_minutes"],
            })

        return {"success": True, "tasks": tasks}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_video_tasks_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/video-tasks/claim")
async def claim_video_task(payload: VideoTaskClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("video_task_claim", request, payload.user_id, 20, 60, ip_limit=40)
        await consume_ad_action_session(payload.user_id, payload.ad_session_id, "video_task")
        await require_user_action_lock(f"video_task:{payload.task_id}", payload.user_id, ttl=3)

        config = VIDEO_TASK_DEFINITIONS.get(payload.task_id)
        if not config:
            raise HTTPException(status_code=400, detail="Unknown video task")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        last_claims = get_video_task_last_claims(extra)
        boosts = get_video_task_boosts(extra)
        now = datetime.utcnow()
        claimed_at = parse_iso_datetime(last_claims.get(payload.task_id))
        cooldown_seconds = int(config["cooldown_minutes"] * 60)

        if claimed_at and (now - claimed_at).total_seconds() < cooldown_seconds:
            remaining = cooldown_seconds - int((now - claimed_at).total_seconds())
            raise HTTPException(status_code=429, detail=f"Task cooldown {remaining // 60}:{remaining % 60:02d}")

        response = {
            "success": True,
            "task_id": payload.task_id,
            "coins": int(user.get("coins", 0)),
        }
        updates = {}

        if config["type"] == "coin_drop":
            reward = resolve_video_task_coin_drop()
            response["coins_reward"] = reward
            response["coins"] = int(user.get("coins", 0)) + reward
            response["message"] = f"+{reward} coins"
            updates["coins"] = response["coins"]
        elif config["type"] == "tap_boost":
            expires_at = (now + timedelta(minutes=config["duration_minutes"])).isoformat()
            boosts["tap_boost"] = {
                "expires_at": expires_at,
                "multiplier": int(config["multiplier"]),
            }
            response["message"] = f"x{config['multiplier']} tap boost for {config['duration_minutes']} min"
            response["task_tap_boost_active"] = True
            response["task_tap_boost_expires_at"] = expires_at
            response["task_tap_boost_multiplier"] = int(config["multiplier"])
        elif config["type"] == "passive_boost":
            expires_at = (now + timedelta(minutes=config["duration_minutes"])).isoformat()
            boosts["passive_boost"] = {
                "expires_at": expires_at,
                "multiplier": int(config["multiplier"]),
            }
            response["message"] = f"x{config['multiplier']} passive income for {config['duration_minutes']} min"
            response["task_passive_boost_active"] = True
            response["task_passive_boost_expires_at"] = expires_at
            response["task_passive_boost_multiplier"] = int(config["multiplier"])

        last_claims[payload.task_id] = now.isoformat()
        extra["video_task_last_claims"] = last_claims
        extra["video_task_boosts"] = boosts
        updates["extra_data"] = extra

        await update_user(payload.user_id, updates)
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(payload.user_id, "tasks", {"task_id": payload.task_id})
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in claim_video_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== РџРђРЎРЎРР’РќР«Р™ Р”РћРҐРћР” ====================

@app.post("/api/passive-income")
async def passive_income(payload: PassiveIncomeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit("passive_income", request, payload.user_id, 20, 60, ip_limit=40)
        await require_user_action_lock("passive_income", payload.user_id, ttl=5)
        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        last_income = normalize_dt(user.get("last_passive_income"))
        now = datetime.utcnow()

        if not last_income:
            initialized_user = await update_user_if_matches(
                payload.user_id,
                {"last_passive_income": None},
                {"last_passive_income": now},
            )
            if initialized_user is None:
                raise HTTPException(status_code=409, detail="Passive income baseline changed, retry")
            await invalidate_user_cache(payload.user_id)
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        elapsed_seconds = max(0.0, (now - last_income).total_seconds())
        elapsed_seconds = min(elapsed_seconds, 24 * 3600)

        extra = parse_extra_data(user.get("extra_data"))

        passive_boost_active, _, passive_boost_multiplier = get_active_video_task_boost(extra, "passive_boost")
        base_hour_value = int(user.get("profit_per_hour", get_hour_value(user.get("profit_level", 0))))
        hour_value = base_hour_value * max(1, passive_boost_multiplier) if passive_boost_active else base_hour_value
        if hour_value <= 0 or elapsed_seconds <= 0:
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        total_income = int((hour_value * elapsed_seconds) // 3600)
        if total_income <= 0:
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        consumed_seconds = (total_income * 3600) / hour_value
        new_last_income = min(now, last_income + timedelta(seconds=consumed_seconds))
        new_coins = int(user.get("coins", 0)) + total_income

        updated_user = await update_user_if_matches(
            payload.user_id,
            {
                "coins": int(user.get("coins", 0)),
                "last_passive_income": last_income,
            },
            {
                "coins": new_coins,
                "last_passive_income": new_last_income,
            },
        )
        if not updated_user:
            logger.warning("Atomic passive-income update conflict for user=%s", payload.user_id)
            raise HTTPException(status_code=409, detail="Passive income state changed, retry")

        await invalidate_user_cache(payload.user_id)
        referral_bonus = await grant_referral_share_bonus(updated_user, total_income)

        return {
            "success": True,
            "coins": int(updated_user.get("coins", new_coins)),
            "income": total_income,
            "referral_bonus_paid": referral_bonus,
            "message": f"+{total_income} passive income"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== DAILY REWARDS ====================

@app.get("/api/daily-reward/status/{user_id}")
async def get_daily_reward_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        claimed_days, last_claim_date = get_daily_reward_progress(extra)
        today = datetime.utcnow().date().isoformat()
        claim_available = claimed_days < DAILY_REWARD_MAX_DAYS and last_claim_date != today
        next_day = min(claimed_days + 1, DAILY_REWARD_MAX_DAYS)
        infinite_energy_active, infinite_energy_expires_at = is_daily_infinite_energy_active(user)

        return {
            "success": True,
            "claimed_days": claimed_days,
            "claim_available": claim_available,
            "next_day": next_day,
            "infinite_energy_active": infinite_energy_active,
            "infinite_energy_expires_at": infinite_energy_expires_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_daily_reward_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/daily-reward/claim")
async def claim_daily_reward(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_redis_rate_limit("claim_daily_reward", payload.user_id, 10, 60)
        await require_user_action_lock("claim_daily_reward", payload.user_id, ttl=5)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        claimed_days, last_claim_date = get_daily_reward_progress(extra)
        today = datetime.utcnow().date().isoformat()

        if claimed_days >= DAILY_REWARD_MAX_DAYS:
            raise HTTPException(status_code=400, detail="Daily rewards completed")

        if last_claim_date == today:
            raise HTTPException(status_code=400, detail="Reward already claimed today")

        day = claimed_days + 1
        coins_reward = day * DAILY_REWARD_BASE_COINS
        new_coins = int(user.get("coins", 0)) + coins_reward

        extra["daily_reward_claimed_days"] = day
        extra["daily_reward_last_claim_date"] = today

        response_payload = {
            "success": True,
            "day": day,
            "coins_reward": coins_reward,
            "coins": new_coins,
            "claim_available": False,
        }

        if day % 7 == 0 and day < DAILY_REWARD_MAX_DAYS:
            active_boosts = extra.get("active_boosts", {})
            if not isinstance(active_boosts, dict):
                active_boosts = {}
            expires_at = (datetime.utcnow() + timedelta(minutes=DAILY_REWARD_INFINITE_ENERGY_MINUTES)).isoformat()
            active_boosts["daily_infinite_energy"] = {
                "active": True,
                "expires_at": expires_at
            }
            extra["active_boosts"] = active_boosts
            response_payload["infinite_energy_expires_at"] = expires_at

        if day == DAILY_REWARD_MAX_DAYS:
            owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
            if DAILY_REWARD_SKIN_ID not in owned_skins:
                owned_skins.append(DAILY_REWARD_SKIN_ID)
            extra["owned_skins"] = normalize_owned_skins(owned_skins)
            response_payload["skin_id"] = DAILY_REWARD_SKIN_ID

        await update_user(payload.user_id, {
            "coins": new_coins,
            "extra_data": extra,
        })
        await invalidate_user_cache(payload.user_id)

        return response_payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in claim_daily_reward: {e}")
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
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}
        if not isinstance(extra, dict):
            extra = {}

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(payload.skin_id, owned_skins)
        if selected_skin not in owned_skins:
            raise HTTPException(status_code=400, detail="Skin not owned")

        extra["owned_skins"] = owned_skins
        extra["selected_skin"] = selected_skin

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "selected_skin": selected_skin}

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

        skin_id = LEGACY_SKIN_ID_MAP.get(payload.skin_id, payload.skin_id)
        if skin_id not in VALID_SKIN_IDS:
            raise HTTPException(status_code=400, detail="Unknown skin")

        owned = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        skin_ad_progress = get_skin_ad_progress(extra)

        if skin_id in owned:
            return {"success": True}

        if skin_id in SKIN_REQUIREMENTS and SKIN_REQUIREMENTS[skin_id]["type"] == "ads":
            required = int(SKIN_REQUIREMENTS[skin_id]["count"])
            current_progress = int(skin_ad_progress.get(skin_id, 0) or 0)
            if current_progress < required:
                raise HTTPException(status_code=400, detail="Not enough ads watched")

        # вњ… РґРѕР±Р°РІР»СЏРµРј СЃРєРёРЅ
        owned.append(skin_id)
        extra["owned_skins"] = normalize_owned_skins(owned)

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


