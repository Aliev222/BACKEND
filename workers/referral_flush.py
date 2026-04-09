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
import os
import time
import uuid
import redis.asyncio as redis

from DATABASE.base import AsyncSessionLocal, User
from infrastructure.redis import init_redis, close_redis
from infrastructure.coins_hot_sync import sync_hot_after_db_increment
from observability.metrics import observe_worker_loop
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

FLUSH_INTERVAL = int(os.getenv("REFERRAL_FLUSH_INTERVAL_SECONDS", "60"))
WORKER_NAME = "referral_flush"
FLUSH_TIMEOUT_SECONDS = 25
LOCK_TTL_SECONDS = int(
    os.getenv(
        "REFERRAL_FLUSH_LOCK_TTL_SECONDS", str(max(FLUSH_TIMEOUT_SECONDS + 10, 35))
    )
)
IDLE_BACKOFF_MAX_INTERVAL = int(
    os.getenv("REFERRAL_FLUSH_IDLE_MAX_INTERVAL_SECONDS", "300")
)
MAX_KEYS_PER_FLUSH = 1000  # Prevent OOM if too many pending keys
RECOVERY_SCAN_INTERVAL = 60  # seconds

_last_recovery_scan_ts = 0.0


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
    Recovery-safe flush with ZSET queue for O(log N) performance.
    1. Get user_ids from ZSET queue
    2. Atomic move: pending → processing (with batch_id)
    3. DB update + log insert
    4. Cleanup: DEL processing + ZREM from queue
    """
    flush_started_at = time.perf_counter()

    # Phase 1: Get pending referrer_ids from ZSET queue (O(log N))
    pending_referrer_ids = await redis_conn.zrange(
        "referral_pending_queue", 0, MAX_KEYS_PER_FLUSH - 1
    )

    if not pending_referrer_ids:
        return 0

    if len(pending_referrer_ids) > MAX_KEYS_PER_FLUSH:
        logger.warning(
            "Limiting flush batch to %d keys (found %d in queue)",
            MAX_KEYS_PER_FLUSH,
            len(pending_referrer_ids),
        )
        pending_referrer_ids = pending_referrer_ids[:MAX_KEYS_PER_FLUSH]

    # Lua: atomic move pending → processing
    move_script = """
    local pending_key = KEYS[1]
    local processing_key = KEYS[2]
    local queue_key = KEYS[3]
    local referrer_id = ARGV[1]
    local batch_id = ARGV[2]
    local moved_at = ARGV[3]

    -- Don't overwrite existing processing key (crash recovery safety)
    if redis.call('EXISTS', processing_key) == 1 then
        return -1  -- already processing, skip
    end

    local coins = redis.call('HGET', pending_key, 'coins')
    if not coins or tonumber(coins) <= 0 then
        redis.call('ZREM', queue_key, referrer_id)
        return 0
    end

    -- Atomic move: RENAME pending → processing
    redis.call('HSET', processing_key, 'coins', coins, 'batch_id', batch_id, 'moved_at', moved_at)
    redis.call('DEL', pending_key)
    return coins
    """

    now = int(time.time())
    moved_data = []  # (processing_key, referrer_id, coins, batch_id)

    # Phase 2: MOVE pending → processing
    for referrer_id_str in pending_referrer_ids:
        referrer_id = int(referrer_id_str)
        batch_id = f"{referrer_id}:{now}:{uuid.uuid4().hex[:8]}"
        processing_key = f"referral_processing:{referrer_id}"
        pending_key = f"referral_pending:{referrer_id}"

        coins = await redis_conn.eval(
            move_script,
            3,
            pending_key,
            processing_key,
            "referral_pending_queue",
            str(referrer_id),
            batch_id,
            str(now),
        )
        coins = int(coins) if coins else 0

        if coins > 0:
            moved_data.append((processing_key, referrer_id, coins, batch_id))
        # coins == 0: nothing to flush
        # coins == -1: already processing, skip (will be recovered)

    if not moved_data:
        return 0

    # Phase 3: Recovery scan — process leftover processing keys, throttled.
    global _last_recovery_scan_ts
    now_ts = time.time()
    if (now_ts - _last_recovery_scan_ts) >= RECOVERY_SCAN_INTERVAL:
        _last_recovery_scan_ts = now_ts
        recovery_keys = []
        cursor = 0
        moved_key_set = {md[0] for md in moved_data}
        while True:
            cursor, keys = await redis_conn.scan(
                cursor, match="referral_processing:*", count=500
            )
            for k in keys:
                if k not in moved_key_set:
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

        if recovery_keys:
            log_worker_recovery(WORKER_NAME, len(recovery_keys))

    # Phase 4: PROCESS (DB) — each batch in its own transaction
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
                    await redis_conn.zrem("referral_pending_queue", str(referrer_id))
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
                .returning(User.coins)
            )
            new_coins = result.scalar_one_or_none()
            if new_coins is None:
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
            await sync_hot_after_db_increment(referrer_id, coins, int(new_coins))

        # Step 4: Cleanup processing key AND remove from queue ONLY after successful commit
        try:
            await redis_conn.delete(proc_key)
            await redis_conn.zrem("referral_pending_queue", str(referrer_id))
        except Exception as e:
            logger.warning(
                "Failed to cleanup processing key %s: %s (data flushed, safe)",
                proc_key,
                e,
            )
        flushed += 1

    observe_worker_loop(
        WORKER_NAME,
        "flush",
        time.perf_counter() - flush_started_at,
        flushed=flushed,
    )
    return flushed


async def referral_flush_loop():
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
                    flush_referral_pending(redis_conn), timeout=FLUSH_TIMEOUT_SECONDS
                )
                if flushed > 0:
                    logger.info("Flushed referral bonuses for %d users", flushed)
            except Exception as e:
                error = str(e)
                observe_worker_loop(
                    WORKER_NAME,
                    "flush",
                    0.0,
                    error=e,
                )
                log_worker_error(WORKER_NAME, error)

            # Stuck key detection
            stuck = await detect_stuck_keys(
                redis_conn,
                "referral_processing:*",
                WORKER_NAME,
                max_age_seconds=FLUSH_INTERVAL * 3,
            )

            loop_ms = (time.monotonic() - loop_start) * 1000
            observe_worker_loop(
                WORKER_NAME,
                "loop",
                loop_ms / 1000.0,
                error=error,
                flushed=flushed,
            )

            # Count pending users in queue for lag visibility (cheap O(1))
            try:
                pending_count = int(await redis_conn.zcard("referral_pending_queue") or 0)
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
    asyncio.run(referral_flush_loop())
