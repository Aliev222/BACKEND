"""
Worker Health & Observability Utilities

Provides:
- Heartbeat tracking for background workers (Redis-backed)
- Stuck key detection for in-flight flush batches
- Structured logging helpers
- Flush lag metrics

Health data lives in Redis under:
  worker:health:{worker_name}  — hash with status fields

Stuck key detection scans:
  coins_flushing:*       — should not exist longer than FLUSH_INTERVAL * 3
  referral_processing:*  — should not exist longer than FLUSH_INTERVAL * 3
"""

import json
import logging
import time
from typing import Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)

HEALTH_KEY_PREFIX = "worker:health:"
STUCK_THRESHOLD_MULTIPLIER = 3  # keys older than interval * this are "stuck"


# ─── Heartbeat ───────────────────────────────────────────────────────────────


async def worker_heartbeat(
    redis_conn: redis.Redis,
    worker_name: str,
    loop_duration_ms: Optional[float] = None,
    flushed: int = 0,
    error: Optional[str] = None,
) -> None:
    """
    Update the health hash for a worker after each loop iteration.

    Fields written:
      - last_loop_ts:    Unix timestamp of last completed loop
      - last_flush_ts:   Unix timestamp of last successful flush (if flushed > 0)
      - last_flushed:    Number of items flushed in last loop
      - loop_duration_ms: How long the last loop took
      - status:          "ok" | "error"
      - last_error:      Last error message (cleared on success)
      - error_count:     Total errors since worker start (counter in this process)
      - uptime_ts:       Worker start timestamp (set once)
    """
    health_key = f"{HEALTH_KEY_PREFIX}{worker_name}"
    now = time.time()

    mapping = {
        "last_loop_ts": str(now),
        "last_flushed": str(flushed),
    }
    if loop_duration_ms is not None:
        mapping["loop_duration_ms"] = f"{loop_duration_ms:.1f}"
    if flushed > 0:
        mapping["last_flush_ts"] = str(now)
    if error:
        mapping["status"] = "error"
        mapping["last_error"] = error[:500]
    else:
        mapping["status"] = "ok"
        mapping["last_error"] = ""

    try:
        await redis_conn.hset(health_key, mapping=mapping)
        await redis_conn.expire(health_key, 300)
    except Exception as e:
        logger.warning("Failed to write worker heartbeat for %s: %s", worker_name, e)


async def worker_heartbeat_init(redis_conn: redis.Redis, worker_name: str) -> None:
    """Set the initial heartbeat when a worker starts."""
    health_key = f"{HEALTH_KEY_PREFIX}{worker_name}"
    try:
        await redis_conn.hset(
            health_key,
            mapping={
                "uptime_ts": str(time.time()),
                "status": "starting",
                "error_count": "0",
            },
        )
    except Exception:
        pass


async def worker_heartbeat_stop(redis_conn: redis.Redis, worker_name: str) -> None:
    """Mark a worker as stopped."""
    health_key = f"{HEALTH_KEY_PREFIX}{worker_name}"
    try:
        await redis_conn.hset(
            health_key,
            mapping={
                "status": "stopped",
                "stop_ts": str(time.time()),
            },
        )
    except Exception:
        pass


# ─── Stuck key detection ─────────────────────────────────────────────────────


async def detect_stuck_keys(
    redis_conn: redis.Redis,
    pattern: str,
    worker_name: str,
    max_age_seconds: int,
) -> list[dict]:
    """
    Scan for keys matching `pattern` that are older than `max_age_seconds`.

    Returns a list of dicts with:
      - key:       Redis key name
      - age_sec:   How old the key is (estimated from value metadata)
      - age_unknown: True if key exists but age cannot be determined
      - value:     Truncated key value (for debugging)

    Does NOT delete stuck keys — only detects and logs them.
    """
    stuck = []
    age_unknown = []
    now = time.time()

    try:
        cursor = 0
        while True:
            cursor, keys = await redis_conn.scan(cursor, match=pattern, count=200)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                age_sec = None
                value_preview = ""

                try:
                    key_type = await redis_conn.type(key_str)
                    if key_type == "hash":
                        data = await redis_conn.hgetall(key_str)
                        moved_at = data.get("moved_at")
                        batch_id = data.get("batch_id", "")
                        if moved_at:
                            age_sec = now - float(moved_at)
                        value_preview = (
                            f"coins={data.get('coins', '?')}, batch={batch_id[:40]}"
                        )
                    elif key_type == "string":
                        val = await redis_conn.get(key_str)
                        value_preview = str(val)[:100] if val else ""
                        # String keys have no embedded timestamp — age unknown
                        age_unknown.append(key_str)
                except Exception:
                    pass

                if age_sec is not None and age_sec > max_age_seconds:
                    stuck.append(
                        {
                            "key": key_str,
                            "age_sec": round(age_sec, 1),
                            "value": value_preview,
                        }
                    )

            if cursor == 0:
                break
    except Exception as e:
        logger.warning("Stuck key scan failed for %s: %s", pattern, e)

    if stuck:
        logger.warning(
            "[worker=%s] STUCK KEYS DETECTED: pattern=%s count=%d keys=%s",
            worker_name,
            pattern,
            len(stuck),
            json.dumps(stuck[:10]),
        )

    if age_unknown:
        logger.warning(
            "[worker=%s] KEYS WITH UNKNOWN AGE: pattern=%s count=%d keys=%s",
            worker_name,
            pattern,
            len(age_unknown),
            json.dumps(age_unknown[:10]),
        )

    return stuck + [{"key": k, "age_unknown": True} for k in age_unknown]


# ─── Flush lag metrics ───────────────────────────────────────────────────────


async def get_flush_lag(redis_conn: redis.Redis) -> dict:
    """
    Return a snapshot of flush pipeline state for observability.

    Returns:
      {
        "coins_pending": N,
        "coins_flushing": N,
        "referral_pending": N,
        "referral_processing": N,
        "tournament_entries": N,
      }
    """
    result = {}
    patterns = [
        ("coins_pending:*", "coins_pending"),
        ("coins_flushing:*", "coins_flushing"),
        ("referral_pending:*", "referral_pending"),
        ("referral_processing:*", "referral_processing"),
    ]

    for pattern, label in patterns:
        count = 0
        try:
            cursor = 0
            while True:
                cursor, keys = await redis_conn.scan(cursor, match=pattern, count=500)
                count += len(keys)
                if cursor == 0:
                    break
        except Exception:
            pass
        result[label] = count

    # Tournament leaderboard size
    try:
        from core.game_config import TOURNAMENT_KEY

        result["tournament_entries"] = await redis_conn.zcard(TOURNAMENT_KEY)
    except Exception:
        result["tournament_entries"] = -1

    return result


# ─── Structured logging ──────────────────────────────────────────────────────


def log_worker_start(worker_name: str, interval: int) -> None:
    logger.info(
        "WORKER_START name=%s interval=%ds",
        worker_name,
        interval,
    )


def log_worker_stop(worker_name: str, reason: str = "shutdown") -> None:
    logger.info(
        "WORKER_STOP name=%s reason=%s",
        worker_name,
        reason,
    )


def log_worker_loop(
    worker_name: str,
    duration_ms: float,
    flushed: int,
    pending_count: int = 0,
    stuck_count: int = 0,
) -> None:
    logger.info(
        "WORKER_LOOP name=%s duration_ms=%.0f flushed=%d pending=%d stuck=%d",
        worker_name,
        duration_ms,
        flushed,
        pending_count,
        stuck_count,
    )


def log_worker_error(worker_name: str, error: str, fatal: bool = False) -> None:
    level = logging.ERROR if fatal else logging.WARNING
    logger.log(
        level,
        "WORKER_ERROR name=%s fatal=%s error=%s",
        worker_name,
        fatal,
        error[:500],
    )


def log_worker_recovery(worker_name: str, recovered: int) -> None:
    if recovered > 0:
        logger.info(
            "WORKER_RECOVERY name=%s recovered=%d",
            worker_name,
            recovered,
        )
