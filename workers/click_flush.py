import asyncio
import logging
from sqlalchemy import update
from infrastructure.database import AsyncSessionLocal
from infrastructure.redis import get_redis_or_none, init_redis, close_redis
from infrastructure.queue import get_all_click_buffers, delete_click_buffer
from DATABASE.base import User

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 10


async def flush_clicks_to_db():
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        logger.warning("No Redis connection, skipping click flush")
        return

    buffers = await get_all_click_buffers(redis_conn)
    if not buffers:
        return

    logger.info(f"Flushing click buffers for {len(buffers)} users")

    async with AsyncSessionLocal() as session:
        for user_id, data in buffers.items():
            coins = data["coins"]
            if coins <= 0:
                await delete_click_buffer(redis_conn, user_id)
                continue

            await session.execute(
                update(User)
                .where(User.user_id == user_id)
                .values(coins=User.coins + coins)
            )
            await delete_click_buffer(redis_conn, user_id)

        await session.commit()

    logger.info(f"Flushed {len(buffers)} users from click buffer")


async def click_flush_loop():
    logger.info("Click flush worker started")
    while True:
        try:
            await flush_clicks_to_db()
        except Exception as e:
            logger.error(f"Click flush error: {e}")
        await asyncio.sleep(FLUSH_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(click_flush_loop())
