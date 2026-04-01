"""
Coins Flush Worker
Redis-first coins: click path writes to Redis, worker flushes to PostgreSQL.

Safety model (two-phase with idempotency log):
1. MOVE: RENAME coins_pending → coins_flushing:{user_id}:{batch_id}
2. LOG: INSERT INTO coins_flush_log ON CONFLICT DO NOTHING RETURNING
3. PROCESS: UPDATE users SET coins = coins + delta (only if log inserted)
4. CLEANUP: DEL coins_flushing key

Recovery:
- Scan coins_flushing:* on startup/retry
- Re-process if not in log
- No data loss, no double-apply

Crash scenarios:
- Crash before MOVE: data stays in pending, next run processes
- Crash after MOVE, before LOG: flushing key exists, recovery re-processes
- Crash after LOG, before UPDATE: log exists, recovery skips (safe: no coins credited)
- Crash after UPDATE, before DEL: flushing key exists, recovery skips (log prevents double-apply)
"""

import asyncio
import logging
import time
import uuid
import redis.asyncio as redis

from DATABASE.base import AsyncSessionLocal, User
from infrastructure.redis import init_redis, close_redis
from sqlalchemy import update, text
from workers.worker_health import (
    worker_heartbeat,
    worker_heartbeat_init,
    worker_heartbeat_stop,
    detect_stuck_keys,
    log_worker_start,
    log_worker_stop,
    log_worker_loop,
    log_worker_error,
    log_worker_recovery,
)

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 30  # seconds
WORKER_NAME = "coins_flush"


async def _ensure_flush_log_table():
    """Ensure coins_flush_log table exists (idempotent)."""
    async with AsyncSessionLocal() as session:
        await session.execute(
            text("""
            CREATE TABLE IF NOT EXISTS coins_flush_log (
                batch_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                delta BIGINT NOT NULL,
                flushed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        )
        await session.commit()


async def flush_coins_to_db(redis_conn: redis.Redis) -> int:
    """
    Flush accumulated coins from Redis to PostgreSQL.
    Returns number of users flushed.
    """
    # Find all pending keys
    pending_keys = []
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(cursor, match="coins_pending:*", count=500)
        pending_keys.extend(keys)
        if cursor == 0:
            break

    if not pending_keys:
        return 0

    now = int(time.time())
    moved_data = []  # (flushing_key, user_id, delta, batch_id)

    # Phase 1: MOVE pending → flushing (atomic RENAME with batch_id)
    for key in pending_keys:
        user_id = int(key.split(":")[-1])
        batch_id = f"{user_id}:{now}:{uuid.uuid4().hex[:8]}"
        flushing_key = f"coins_flushing:{user_id}:{batch_id}"

        try:
            delta = await redis_conn.get(key)
            if not delta or int(delta) <= 0:
                await redis_conn.delete(key)
                continue

            # Atomic move: set flushing value, delete pending
            await redis_conn.set(flushing_key, delta)
            await redis_conn.delete(key)
            moved_data.append((flushing_key, user_id, int(delta), batch_id))
        except Exception as e:
            logger.warning("Failed to move coins key %s: %s", key, e)

    if not moved_data:
        return 0

    # Phase 2: Recovery scan — process any leftover flushing keys
    flushing_keys = []
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(
            cursor, match="coins_flushing:*", count=500
        )
        for k in keys:
            if k not in [md[0] for md in moved_data]:
                flushing_keys.append(k)
        if cursor == 0:
            break

    for flush_key in flushing_keys:
        parts = flush_key.split(":")
        user_id = int(parts[2])
        batch_id = ":".join(parts[3:]) if len(parts) > 3 else f"{user_id}:recovery"
        delta = await redis_conn.get(flush_key)
        if delta and int(delta) > 0:
            moved_data.append((flush_key, user_id, int(delta), batch_id))

    if flushing_keys:
        log_worker_recovery(WORKER_NAME, len(flushing_keys))

    # Phase 3: PROCESS (DB) — each batch in its own transaction
    flushed = 0
    for flush_key, user_id, delta, batch_id in moved_data:
        async with AsyncSessionLocal() as session:
            # Step 1: INSERT log first (atomic claim)
            log_result = await session.execute(
                text("""
                    INSERT INTO coins_flush_log (batch_id, user_id, delta)
                    VALUES (:bid, :uid, :delta)
                    ON CONFLICT (batch_id) DO NOTHING
                    RETURNING batch_id
                """),
                {"bid": batch_id, "uid": user_id, "delta": delta},
            )
            inserted = log_result.scalar()

            if not inserted:
                # Batch already processed (double-apply protection)
                try:
                    await redis_conn.delete(flush_key)
                except Exception:
                    pass
                continue

            # Step 2: UPDATE users (only if log insert succeeded)
            result = await session.execute(
                update(User)
                .where(User.user_id == user_id)
                .values(coins=User.coins + delta)
            )

            if result.rowcount == 0:
                # User not found. DO NOT commit.
                # Session rollback removes the log INSERT automatically.
                # Flushing key stays in Redis for retry/recovery.
                logger.warning(
                    "Coins flush: user %s not found, batch %s kept for retry",
                    user_id,
                    batch_id,
                )
                continue

            # Step 3: Commit (log + balance update are atomic)
            await session.commit()

        # Step 4: Cleanup flushing key ONLY after successful commit
        try:
            await redis_conn.delete(flush_key)
        except Exception as e:
            logger.warning(
                "Failed to cleanup flushing key %s: %s (data flushed, safe)",
                flush_key,
                e,
            )
        flushed += 1

    return flushed


async def coins_flush_loop():
    log_worker_start(WORKER_NAME, FLUSH_INTERVAL)
    await _ensure_flush_log_table()

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
                flushed = await flush_coins_to_db(redis_conn)
                if flushed > 0:
                    logger.info("Flushed coins for %d users", flushed)
            except Exception as e:
                error = str(e)
                log_worker_error(WORKER_NAME, error)

            # Stuck key detection
            stuck = await detect_stuck_keys(
                redis_conn,
                "coins_flushing:*",
                WORKER_NAME,
                max_age_seconds=FLUSH_INTERVAL * 3,
            )

            loop_ms = (time.monotonic() - loop_start) * 1000

            # Count pending keys for lag visibility
            pending_count = 0
            try:
                cursor = 0
                while True:
                    cursor, keys = await redis_conn.scan(
                        cursor, match="coins_pending:*", count=500
                    )
                    pending_count += len(keys)
                    if cursor == 0:
                        break
            except Exception:
                pass

            log_worker_loop(
                WORKER_NAME,
                duration_ms=loop_ms,
                flushed=flushed,
                pending_count=pending_count,
                stuck_count=len(stuck),
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
    asyncio.run(coins_flush_loop())
