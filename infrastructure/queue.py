import os
import time
import json
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)

CLICK_BUFFER_TTL = 300
CLICK_FLUSH_INTERVAL = int(os.getenv("CLICK_FLUSH_INTERVAL", "10"))
ENERGY_FLUSH_INTERVAL = int(os.getenv("ENERGY_FLUSH_INTERVAL", "30"))
ACTIVITY_FLUSH_INTERVAL = int(os.getenv("ACTIVITY_FLUSH_INTERVAL", "300"))


async def buffer_clicks(redis_conn: redis.Redis, user_id: int, coins: int, clicks: int):
    key = f"click_buffer:{user_id}"
    async with redis_conn.pipeline() as pipe:
        await pipe.hincrby(key, "coins", coins)
        await pipe.hincrby(key, "clicks", clicks)
        await pipe.expire(key, CLICK_BUFFER_TTL)
        await pipe.execute()


async def buffer_energy(
    redis_conn: redis.Redis, user_id: int, energy: int, updated_at: float
):
    key = f"energy:{user_id}"
    await redis_conn.hset(
        key,
        mapping={
            "value": str(energy),
            "updated_at": str(updated_at),
        },
    )
    await redis_conn.expire(key, CLICK_BUFFER_TTL)


async def buffer_activity(redis_conn: redis.Redis, user_id: int, timestamp: str):
    key = f"activity:{user_id}"
    await redis_conn.setex(key, CLICK_BUFFER_TTL, timestamp)


async def get_click_buffer(redis_conn: redis.Redis, user_id: int) -> dict:
    key = f"click_buffer:{user_id}"
    data = await redis_conn.hgetall(key)
    if not data:
        return {"coins": 0, "clicks": 0}
    return {
        "coins": int(data.get("coins", 0)),
        "clicks": int(data.get("clicks", 0)),
    }


async def get_all_click_buffers(redis_conn: redis.Redis) -> dict[int, dict]:
    pattern = "click_buffer:*"
    result = {}
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(cursor, match=pattern, count=500)
        for key in keys:
            user_id = int(key.split(":")[-1])
            data = await redis_conn.hgetall(key)
            result[user_id] = {
                "coins": int(data.get("coins", 0)),
                "clicks": int(data.get("clicks", 0)),
            }
        if cursor == 0:
            break
    return result


async def delete_click_buffer(redis_conn: redis.Redis, user_id: int):
    await redis_conn.delete(f"click_buffer:{user_id}")


async def get_energy(redis_conn: redis.Redis, user_id: int) -> dict | None:
    key = f"energy:{user_id}"
    data = await redis_conn.hgetall(key)
    if not data:
        return None
    return {
        "value": int(data.get("value", 0)),
        "updated_at": float(data.get("updated_at", 0)),
    }


async def get_all_energy_buffers(redis_conn: redis.Redis) -> dict[int, dict]:
    pattern = "energy:*"
    result = {}
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(cursor, match=pattern, count=500)
        for key in keys:
            if key.startswith("energy:"):
                user_id = int(key.split(":")[-1])
                data = await redis_conn.hgetall(key)
                result[user_id] = {
                    "value": int(data.get("value", 0)),
                    "updated_at": float(data.get("updated_at", 0)),
                }
        if cursor == 0:
            break
    return result


async def delete_energy_buffer(redis_conn: redis.Redis, user_id: int):
    await redis_conn.delete(f"energy:{user_id}")
