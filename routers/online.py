import time
import logging
from fastapi import APIRouter, Request, HTTPException
import redis.asyncio as redis

from infrastructure.redis import get_redis

router = APIRouter(prefix="/api/v2", tags=["online"])
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
    telegram_user = request.headers.get("X-Telegram-Init-Data", "")
    if not telegram_user:
        bearer = request.headers.get("Authorization", "")
        if bearer:
            from routers.auth import verify_session_token

            try:
                telegram_user = verify_session_token(bearer.split(" ", 1)[1])
            except Exception:
                telegram_user = {}
        else:
            telegram_user = {}

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
