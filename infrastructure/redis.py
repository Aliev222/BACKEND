import os
import logging
import redis.asyncio as redis
from fastapi import HTTPException

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL")
REDIS_DB = int((os.getenv("REDIS_DB", "0") or "0").strip())
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "100"))
redis_client: redis.Redis | None = None


async def init_redis() -> redis.Redis | None:
    global redis_client
    if not REDIS_URL:
        logger.warning("REDIS_URL is not set, Redis disabled")
        return None

    try:
        redis_client = redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            db=REDIS_DB,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry_on_timeout=True,
            max_connections=REDIS_MAX_CONNECTIONS,
        )
        await redis_client.ping()
        logger.info("Redis connected (max_connections=%d)", REDIS_MAX_CONNECTIONS)
        return redis_client
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        redis_client = None
        return None


async def close_redis():
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None


async def get_redis() -> redis.Redis:
    global redis_client
    if redis_client is None:
        await init_redis()
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return redis_client


async def get_redis_or_none() -> redis.Redis | None:
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                db=REDIS_DB,
                socket_timeout=2,
                socket_connect_timeout=2,
                retry_on_timeout=True,
                max_connections=REDIS_MAX_CONNECTIONS,
            )
            await redis_client.ping()
        except Exception:
            redis_client = None
    return redis_client
