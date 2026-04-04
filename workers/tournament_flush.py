"""
Tournament Flush Worker
Periodically snapshots Redis leaderboard to PostgreSQL.

Safety:
- Upsert: INSERT ... ON CONFLICT DO UPDATE — idempotent
- If worker crashes — next run repeats snapshot
- Redis ZINCRBY always increases, snapshot only reads
- Data loss: maximum FLUSH_INTERVAL seconds of leaderboard data
"""

import asyncio
import logging
import time
import redis.asyncio as redis

from DATABASE.base import (
    AsyncSessionLocal,
    WeeklyTournamentEntry,
    get_weekly_tournament_season_key,
    get_weekly_tournament_season_window,
    ensure_weekly_tournament_season,
)
from core.game_config import TOURNAMENT_KEY
from infrastructure.redis import init_redis, close_redis
from observability.metrics import observe_worker_loop
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime
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

FLUSH_INTERVAL = 60  # seconds
LEADERBOARD_LIMIT = 1000
WORKER_NAME = "tournament_flush"


async def flush_tournament_to_db(redis_conn: redis.Redis) -> int:
    """
    Snapshot Redis ZSET leaderboard to PostgreSQL.
    Returns number of entries flushed.
    """
    flush_started_at = time.perf_counter()
    entries = await redis_conn.zrevrange(
        TOURNAMENT_KEY, 0, LEADERBOARD_LIMIT - 1, withscores=True
    )

    if not entries:
        return 0

    starts_at, ends_at = get_weekly_tournament_season_window()
    season_key = get_weekly_tournament_season_key(starts_at)
    now = datetime.utcnow()

    async with AsyncSessionLocal() as session:
        await ensure_weekly_tournament_season(session, season_key, starts_at, ends_at)

        flushed = 0
        for entry_data in entries:
            if isinstance(entry_data, tuple):
                user_id_str, score = entry_data
            else:
                continue

            user_id = int(user_id_str)
            score = int(score)

            if score <= 0:
                continue

            insert_stmt = pg_insert(WeeklyTournamentEntry).values(
                season_key=season_key,
                user_id=user_id,
                username=None,
                display_level=1,
                league="bronze",
                score=score,
                eligible_for_payout=True,
                fraud_flag=False,
                created_at=now,
                updated_at=now,
            )
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    WeeklyTournamentEntry.__table__.c.season_key,
                    WeeklyTournamentEntry.__table__.c.user_id,
                ],
                set_={
                    "score": score,
                    "updated_at": now,
                },
            )
            await session.execute(upsert_stmt)
            flushed += 1

        await session.commit()

    observe_worker_loop(
        WORKER_NAME,
        "flush",
        time.perf_counter() - flush_started_at,
        flushed=flushed,
    )
    return flushed


async def tournament_flush_loop():
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
                flushed = await flush_tournament_to_db(redis_conn)
                if flushed > 0:
                    logger.info("Flushed %d tournament entries to DB", flushed)
            except Exception as e:
                error = str(e)
                observe_worker_loop(
                    WORKER_NAME,
                    "flush",
                    0.0,
                    error=e,
                )
                log_worker_error(WORKER_NAME, error)

            loop_ms = (time.monotonic() - loop_start) * 1000
            observe_worker_loop(
                WORKER_NAME,
                "loop",
                loop_ms / 1000.0,
                error=error,
                flushed=flushed,
            )

            # Leaderboard size for lag visibility
            pending_count = 0
            try:
                pending_count = await redis_conn.zcard(TOURNAMENT_KEY)
            except Exception:
                pass

            log_worker_loop(
                WORKER_NAME,
                duration_ms=loop_ms,
                flushed=flushed,
                pending_count=pending_count,
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
    asyncio.run(tournament_flush_loop())
