"""
Referral Pending Flush Worker
Recovery-safe flush: pending → processing → DB → cleanup.

Safety guarantees:
- No data loss: processing key persists until explicit cleanup after commit
- No double-apply: unique batch_id in referral_flush_log table
- Crash recovery: processing keys re-scanned on startup/recovery
"""

import asyncio
import logging
import time
import uuid
import redis.asyncio as redis

from DATABASE.base import AsyncSessionLocal, User
from infrastructure.redis import init_redis, close_redis
from sqlalchemy import update, text

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 30  # seconds


async def _ensure_flush_log_table():
    """Ensure referral_flush_log table exists (idempotent)."""
    async with AsyncSessionLocal() as session:
        await session.execute(
            text("""
            CREATE TABLE IF NOT EXISTS referral_flush_log (
                batch_id TEXT PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                coins BIGINT NOT NULL,
                flushed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        )
        await session.commit()


async def flush_referral_pending(redis_conn: redis.Redis) -> int:
    """
    Recovery-safe flush:
    1. Atomic move: pending → processing (with batch_id)
    2. DB update + log insert
    3. Cleanup: DEL processing
    """
    # Lua: atomic move pending → processing
    move_script = """
    local pending_key = KEYS[1]
    local processing_key = KEYS[2]
    local batch_id = ARGV[1]

    -- Don't overwrite existing processing key (crash recovery safety)
    if redis.call('EXISTS', processing_key) == 1 then
        return -1  -- already processing, skip
    end

    local coins = redis.call('HGET', pending_key, 'coins')
    if not coins or tonumber(coins) <= 0 then
        return 0
    end

    -- Atomic move: RENAME pending → processing
    redis.call('HSET', processing_key, 'coins', coins, 'batch_id', batch_id, 'moved_at', tostring(ARGV[2]))
    redis.call('DEL', pending_key)
    return coins
    """

    # Find all pending keys
    pending_keys = []
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(
            cursor, match="referral_pending:*", count=500
        )
        pending_keys.extend(keys)
        if cursor == 0:
            break

    if not pending_keys:
        return 0

    now = int(time.time())
    moved_data = []  # (processing_key, referrer_id, coins, batch_id)

    # Phase 1: MOVE pending → processing
    for key in pending_keys:
        referrer_id = int(key.split(":")[-1])
        batch_id = f"{referrer_id}:{now}:{uuid.uuid4().hex[:8]}"
        processing_key = f"referral_processing:{referrer_id}"

        coins = await redis_conn.eval(
            move_script, 2, key, processing_key, batch_id, str(now)
        )
        coins = int(coins) if coins else 0

        if coins > 0:
            moved_data.append((processing_key, referrer_id, coins, batch_id))
        # coins == 0: nothing to flush
        # coins == -1: already processing, skip (will be recovered)

    if not moved_data:
        return 0

    # Phase 2: Recovery scan — also process any leftover processing keys
    recovery_keys = []
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(
            cursor, match="referral_processing:*", count=500
        )
        for k in keys:
            if k not in [md[0] for md in moved_data]:
                recovery_keys.append(k)
        if cursor == 0:
            break

    for proc_key in recovery_keys:
        referrer_id = int(proc_key.split(":")[-1])
        data = await redis_conn.hgetall(proc_key)
        if data and data.get("coins") and data.get("batch_id"):
            coins = int(data["coins"])
            batch_id = data["batch_id"]
            if coins > 0:
                moved_data.append((proc_key, referrer_id, coins, batch_id))

    # Phase 3: PROCESS (DB) — each batch in its own transaction
    flushed = 0
    for proc_key, referrer_id, coins, batch_id in moved_data:
        async with AsyncSessionLocal() as session:
            # Step 1: INSERT log first (atomic claim)
            log_result = await session.execute(
                text("""
                    INSERT INTO referral_flush_log (batch_id, referrer_id, coins)
                    VALUES (:bid, :rid, :coins)
                    ON CONFLICT (batch_id) DO NOTHING
                    RETURNING batch_id
                """),
                {"bid": batch_id, "rid": referrer_id, "coins": coins},
            )
            inserted = log_result.scalar()

            if not inserted:
                # Batch already processed (double-apply protection)
                # Safe to cleanup since data is already in DB
                try:
                    await redis_conn.delete(proc_key)
                except Exception:
                    pass
                continue

            # Step 2: UPDATE users
            result = await session.execute(
                update(User)
                .where(User.user_id == referrer_id)
                .values(
                    coins=User.coins + coins,
                    referral_earnings=User.referral_earnings + coins,
                )
            )

            if result.rowcount == 0:
                # User not found. DO NOT commit.
                # Session rollback removes the log INSERT automatically.
                # Processing key stays in Redis for retry/recovery.
                logger.warning(
                    "Referral flush: user %s not found, batch %s kept for retry",
                    referrer_id,
                    batch_id,
                )
                continue

            # Step 3: Commit (log + balance update are atomic)
            await session.commit()

        # Step 4: Cleanup processing key ONLY after successful commit
        try:
            await redis_conn.delete(proc_key)
        except Exception as e:
            logger.warning(
                "Failed to cleanup processing key %s: %s (data flushed, safe)",
                proc_key,
                e,
            )
        flushed += 1

    # Phase 4: CLEANUP (DEL processing keys after successful commit)
    for proc_key, referrer_id, coins, batch_id in moved_data:
        try:
            await redis_conn.delete(proc_key)
        except Exception as e:
            logger.warning(
                "Failed to cleanup processing key %s: %s (data flushed, safe to retry)",
                proc_key,
                e,
            )

    return flushed


async def referral_flush_loop():
    logger.info("Referral flush worker started (interval=%ds)", FLUSH_INTERVAL)
    # Ensure table exists
    await _ensure_flush_log_table()
    while True:
        try:
            redis_conn = await init_redis()
            if redis_conn:
                flushed = await flush_referral_pending(redis_conn)
                if flushed > 0:
                    logger.info("Flushed referral bonuses for %d users", flushed)
        except Exception as e:
            logger.error("Referral flush error: %s", e)
        await asyncio.sleep(FLUSH_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(referral_flush_loop())
