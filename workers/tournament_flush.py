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
import os
import time
import uuid
import redis.asyncio as redis

from DATABASE.base import (
    AsyncSessionLocal,
    WeeklyTournamentEntry,
    User,
    get_weekly_tournament_season_key,
    get_weekly_tournament_season_window,
    get_weekly_tournament_league,
    ensure_weekly_tournament_season,
)
from core.game_config import TOURNAMENT_KEY
from core.game_logic import resolve_progression_level
from infrastructure.redis import init_redis, close_redis
from observability.metrics import observe_worker_loop
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text, select
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

FLUSH_INTERVAL = int(os.getenv("TOURNAMENT_FLUSH_INTERVAL_SECONDS", "180"))
LEADERBOARD_LIMIT = 1000
WORKER_NAME = "tournament_flush"
FLUSH_TIMEOUT_SECONDS = 50
LOCK_TTL_SECONDS = int(
    os.getenv(
        "TOURNAMENT_FLUSH_LOCK_TTL_SECONDS", str(max(FLUSH_TIMEOUT_SECONDS + 10, 65))
    )
)
IDLE_BACKOFF_MAX_INTERVAL = int(
    os.getenv("TOURNAMENT_FLUSH_IDLE_MAX_INTERVAL_SECONDS", "600")
)


async def _ensure_flush_log_table():
    """Ensure tournament_flush_log table exists (idempotent)."""
    async with AsyncSessionLocal() as session:
        await session.execute(
            text("""
            CREATE TABLE IF NOT EXISTS tournament_flush_log (
                batch_id TEXT PRIMARY KEY,
                season_key TEXT NOT NULL,
                flushed_count INT NOT NULL,
                flushed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        )
        await session.commit()


async def flush_tournament_to_db(redis_conn: redis.Redis) -> int:
    """
    Snapshot Redis ZSET leaderboard to PostgreSQL with deterministic idempotency.

    Idempotency model:
    - batch_id is deterministic: season_key + flush_window (minute-level)
    - Same logical snapshot = same batch_id
    - UPSERT ensures score is absolute (not incremental)
    - Crash recovery: restart sees same batch_id, skips if already processed

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

    # Deterministic batch_id: season + minute-level window
    # Same minute = same batch_id = idempotent
    flush_window = now.strftime("%Y%m%d%H%M")
    batch_id = f"{season_key}:flush:{flush_window}"

    async with AsyncSessionLocal() as session:
        await ensure_weekly_tournament_season(session, season_key, starts_at, ends_at)

        # Phase 1: INSERT idempotency log first (atomic claim)
        # If this batch_id already exists, skip entire flush
        log_result = await session.execute(
            text("""
                INSERT INTO tournament_flush_log (batch_id, season_key, flushed_count)
                VALUES (:bid, :sk, :count)
                ON CONFLICT (batch_id) DO NOTHING
                RETURNING batch_id
            """),
            {"bid": batch_id, "sk": season_key, "count": len(entries)},
        )
        inserted = log_result.scalar()

        if not inserted:
            # Batch already processed (crash recovery or duplicate run)
            logger.info(
                "Tournament flush batch %s already processed, skipping (crash recovery or duplicate)",
                batch_id,
            )
            return 0

        user_ids = []
        for entry_data in entries:
            if isinstance(entry_data, tuple):
                user_id_str, _ = entry_data
                try:
                    user_ids.append(int(user_id_str))
                except Exception:
                    continue

        users_by_id = {}
        if user_ids:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            for user_row in users_result.scalars().all():
                users_by_id[int(user_row.user_id)] = user_row

        # Phase 2: Upsert entries (only if log insert succeeded)
        # UPSERT with absolute score ensures repeated processing is safe
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

            user_row = users_by_id.get(user_id)
            if user_row is not None:
                user_payload = {
                    "level": int(getattr(user_row, "level", 0) or 0),
                    "multitap_level": int(
                        getattr(user_row, "multitap_level", 0) or 0
                    ),
                    "profit_level": int(getattr(user_row, "profit_level", 0) or 0),
                    "energy_level": int(getattr(user_row, "energy_level", 0) or 0),
                }
                display_level = max(1, int(resolve_progression_level(user_payload)) + 1)
                league = get_weekly_tournament_league(display_level)
                username = getattr(user_row, "username", None)
            else:
                display_level = 1
                league = "bronze"
                username = None

            insert_stmt = pg_insert(WeeklyTournamentEntry).values(
                season_key=season_key,
                user_id=user_id,
                username=username,
                display_level=display_level,
                league=league,
                score=score,
                eligible_for_payout=True,
                fraud_flag=False,
                created_at=now,
                updated_at=now,
            )
            # CRITICAL: UPSERT with absolute score (not increment)
            # This makes repeated processing safe (idempotent)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    WeeklyTournamentEntry.__table__.c.season_key,
                    WeeklyTournamentEntry.__table__.c.user_id,
                ],
                set_={
                    "score": score,  # Absolute value, not += delta
                    "username": insert_stmt.excluded.username,
                    "display_level": insert_stmt.excluded.display_level,
                    "league": insert_stmt.excluded.league,
                    "updated_at": now,
                },
            )
            await session.execute(upsert_stmt)
            flushed += 1

        # Phase 3: Commit (log + entries are atomic)
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
    await _ensure_flush_log_table()

    redis_conn = None
    try:
        redis_conn = await init_redis()
        if not redis_conn:
            log_worker_error(WORKER_NAME, "Redis unavailable at startup", fatal=True)
            return

        await worker_heartbeat_init(redis_conn, WORKER_NAME)

        instance_id = uuid.uuid4().hex[:8]
        empty_streak = 0
        while True:
            loop_start = time.monotonic()
            error = None
            flushed = 0
            pending_count = 0
            sleep_seconds = FLUSH_INTERVAL
            lock_key = f"worker:lock:{WORKER_NAME}"
            got_lock = False

            try:
                got_lock = bool(
                    await redis_conn.set(
                        lock_key, instance_id, ex=LOCK_TTL_SECONDS, nx=True
                    )
                )
                if not got_lock:
                    empty_streak = min(empty_streak + 1, 6)
                    sleep_seconds = min(
                        IDLE_BACKOFF_MAX_INTERVAL,
                        FLUSH_INTERVAL * (2 ** min(empty_streak, 4)),
                    )
                    await asyncio.sleep(sleep_seconds)
                    continue

                flushed = await asyncio.wait_for(
                    flush_tournament_to_db(redis_conn), timeout=FLUSH_TIMEOUT_SECONDS
                )
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

            if flushed > 0 or pending_count > 0:
                empty_streak = 0
                sleep_seconds = FLUSH_INTERVAL
            else:
                empty_streak = min(empty_streak + 1, 6)
                sleep_seconds = min(
                    IDLE_BACKOFF_MAX_INTERVAL,
                    FLUSH_INTERVAL * (2 ** min(empty_streak, 4)),
                )

            if got_lock:
                try:
                    await redis_conn.eval(
                        """
                        if redis.call('GET', KEYS[1]) == ARGV[1] then
                            return redis.call('DEL', KEYS[1])
                        end
                        return 0
                        """,
                        1,
                        lock_key,
                        instance_id,
                    )
                except Exception:
                    pass

            await asyncio.sleep(sleep_seconds)

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
