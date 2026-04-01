import asyncio
import logging
import time
from sqlalchemy import update
from infrastructure.database import AsyncSessionLocal
from infrastructure.redis import get_redis_or_none, init_redis, close_redis
from infrastructure.queue import get_all_click_buffers, delete_click_buffer
from DATABASE.base import User
from workers.worker_health import (
    worker_heartbeat,
    worker_heartbeat_init,
    worker_heartbeat_stop,
    log_worker_start,
    log_worker_stop,
    log_worker_loop,
    log_worker_error,
)

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 10
WORKER_NAME = "click_flush"


async def flush_clicks_to_db(redis_conn):
    buffers = await get_all_click_buffers(redis_conn)
    if not buffers:
        return 0

    logger.info("Flushing click buffers for %d users", len(buffers))

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

    logger.info("Flushed %d users from click buffer", len(buffers))
    return len(buffers)


async def click_flush_loop():
    log_worker_start(WORKER_NAME, FLUSH_INTERVAL)

    redis_conn = None
    try:
        redis_conn = await init_redis()
        if not redis_conn:
            log_worker_error(WORKER_NAME, "Redis unavailable at startup", fatal=True)
            return

        await worker_heartbeat_init(redis_conn, WORKER_NAME)

        while True:
            loop_start = time.monotonic()
            error = None
            flushed = 0

            try:
                flushed = await flush_clicks_to_db(redis_conn)
            except Exception as e:
                error = str(e)
                log_worker_error(WORKER_NAME, error)

            loop_ms = (time.monotonic() - loop_start) * 1000

            log_worker_loop(
                WORKER_NAME,
                duration_ms=loop_ms,
                flushed=flushed,
            )

            await worker_heartbeat(
                redis_conn,
                WORKER_NAME,
                loop_duration_ms=loop_ms,
                flushed=flushed,
                error=error,
            )

            await asyncio.sleep(FLUSH_INTERVAL)

    except asyncio.CancelledError:
        if redis_conn:
            await worker_heartbeat_stop(redis_conn, WORKER_NAME)
        log_worker_stop(WORKER_NAME, reason="cancelled")
        raise
    except Exception as e:
        if redis_conn:
            await worker_heartbeat_stop(redis_conn, WORKER_NAME)
        log_worker_error(WORKER_NAME, str(e), fatal=True)
        raise
    finally:
        if redis_conn:
            await close_redis()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(click_flush_loop())
