"""
Online status routes.

Two sets of endpoints:
- /api/v2/online/* — new API (existing)
- /api/online/*    — legacy paths (moved from legacy.py, Patch 7.2)
"""

import time
import logging
from fastapi import APIRouter, Request, HTTPException
import redis.asyncio as redis

from infrastructure.redis import get_redis, get_redis_or_none

router = APIRouter(prefix="/api/v2", tags=["online"])
router_legacy = APIRouter(tags=["online-legacy"])
logger = logging.getLogger(__name__)

ONLINE_USERS_KEY = "online:users"
ONLINE_TTL_SECONDS = 180


async def touch_online_user(redis_conn: redis.Redis, user_id: int) -> int:
    now = time.time()
    cutoff = now - ONLINE_TTL_SECONDS
    async with redis_conn.pipeline() as pipe:
        await pipe.zadd(ONLINE_USERS_KEY, {str(user_id): now})
        await pipe.zremrangebyscore(ONLINE_USERS_KEY, 0, cutoff)
        await pipe.zcount(ONLINE_USERS_KEY, cutoff, "+inf")
        results = await pipe.execute()
    return int(results[2] or 0)


@router.post("/online/heartbeat")
async def online_heartbeat(request: Request):
    telegram_user_raw = request.headers.get("X-Telegram-Init-Data", "")
    telegram_user = {}

    if telegram_user_raw:
        from core.telegram_auth import verify_telegram_init_data

        try:
            telegram_user = verify_telegram_init_data(telegram_user_raw)
        except Exception:
            pass

    if not telegram_user:
        bearer = request.headers.get("Authorization", "")
        if bearer:
            from routers.auth import verify_session_token

            try:
                telegram_user = verify_session_token(bearer.split(" ", 1)[1])
            except Exception:
                pass

    user_id = int(telegram_user.get("id", 0))
    if user_id <= 0:
        raise HTTPException(status_code=401, detail="Invalid user")

    redis_conn = await get_redis()
    online_now = await touch_online_user(redis_conn, user_id)

    return {"success": True, "online_now": online_now}


@router.get("/online/count")
async def online_count(request: Request):
    redis_conn = await get_redis()
    now = time.time()
    cutoff = now - ONLINE_TTL_SECONDS
    count = await redis_conn.zcount(ONLINE_USERS_KEY, cutoff, "+inf")
    return {"success": True, "online_now": int(count or 0)}


# ─── Legacy paths (moved from legacy.py, Patch 7.2) ─────────────────────────


@router_legacy.post("/api/online/heartbeat")
async def online_heartbeat_legacy(payload: dict, request: Request):
    """Legacy online heartbeat — same behavior as before extraction."""
    from routers.legacy import require_telegram_user, touch_online_user as legacy_touch

    user_id = payload.get("user_id")
    if user_id is None:
        raise HTTPException(status_code=400, detail="user_id required")
    await require_telegram_user(request, user_id)
    online_now = await legacy_touch(int(user_id))
    return {"success": True, "online_now": online_now}


@router_legacy.get("/api/online/count")
async def get_online_count_legacy():
    """Legacy online count — same behavior as before extraction."""
    from routers.legacy import get_online_users_count

    online_now = await get_online_users_count()
    return {"success": True, "online_now": online_now}
