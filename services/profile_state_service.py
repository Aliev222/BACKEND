import json
import os
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import HTTPException
from observability.metrics import observe_storage_error, observe_storage_timing

PROFILE_STATE_CACHE_PREFIX = "user:state:rt:"
PROFILE_STATE_CACHE_TTL_MS = max(
    0,
    int(float(os.getenv("PROFILE_STATE_CACHE_TTL_SECONDS", "1.5")) * 1000),
)


@dataclass(frozen=True)
class ProfileStateServiceDeps:
    require_telegram_user: Callable[..., Awaitable[Any]]
    get_user_cached: Callable[[int], Awaitable[dict | None]]
    touch_user_activity: Callable[[int, dict], Awaitable[Any]]
    get_redis_or_none: Callable[[], Awaitable[Any]]
    get_user: Callable[[int], Awaitable[dict | None]]
    build_realtime_player_state: Callable[[int], Awaitable[dict | None]]
    logger: Any


async def ensure_coins_hot_initialized_service(
    user_id: int, db_coins: int, redis_conn
) -> None:
    """
    Ensure coins_hot:{user_id} exists in Redis, initializing it if missing.

    Called from profile/auth endpoints (NOT from the hot click path) to
    safely bootstrap the hot balance key outside the increment pipeline.

    baseline = DB_coins + coins_pending + SUM(coins_flushing:{user_id}:*)

    This does NOT double-count because:
    - flushing keys are deleted ONLY after DB commit succeeds
    - once deleted, the amount is in DB_coins for future boots
    - SET is conditional on EXISTS, so we never overwrite an existing key

    Race-safe: the Lua script atomically checks EXISTS and SETs baseline.
    If flush worker is running concurrently, worst case is a tiny stale
    read (pending already moved to flushing), which self-corrects on next
    profile load after flush commits to DB.
    """
    coins_hot_key = f"coins_hot:{user_id}"

    try:
        exists = await redis_conn.exists(coins_hot_key)
        if exists:
            return

        baseline = db_coins

        try:
            pending = await redis_conn.get(f"coins_pending:{user_id}")
            if pending:
                baseline += int(pending)
        except Exception:
            pass

        try:
            cursor = 0
            while True:
                cursor, keys = await redis_conn.scan(
                    cursor, match=f"coins_flushing:{user_id}:*", count=100
                )
                for key in keys:
                    val = await redis_conn.get(key)
                    if val:
                        baseline += int(val)
                if cursor == 0:
                    break
        except Exception:
            pass

        init_lua = """
        local hot_key = KEYS[1]
        local baseline = ARGV[1]

        if redis.call('EXISTS', hot_key) == 0 then
            redis.call('SET', hot_key, baseline)
            return 1
        end
        return 0
        """
        await redis_conn.eval(init_lua, 1, coins_hot_key, str(baseline))
    except Exception:
        pass


async def get_user_data_service(
    user_id: int,
    request: Any,
    deps: ProfileStateServiceDeps,
    ensure_coins_hot_initialized: Callable[[int, int, Any], Awaitable[None]],
):
    try:
        await deps.require_telegram_user(request, user_id)
        redis_conn = await deps.get_redis_or_none()
        state_cache_key = f"{PROFILE_STATE_CACHE_PREFIX}{user_id}"

        if redis_conn and PROFILE_STATE_CACHE_TTL_MS > 0:
            try:
                t = time.perf_counter()
                cached_state = await redis_conn.get(state_cache_key)
                observe_storage_timing(
                    "redis",
                    "profile_state_cache_get",
                    "profile",
                    time.perf_counter() - t,
                    outcome="hit" if cached_state else "miss",
                )
                if cached_state:
                    decoded_state = json.loads(cached_state)
                    if isinstance(decoded_state, dict):
                        return decoded_state
            except Exception:
                # Cache is best-effort: profile assembly must continue on any failure.
                pass

        t = time.perf_counter()
        user = await deps.get_user_cached(user_id)
        observe_storage_timing("db", "get_user_cached", "profile", time.perf_counter() - t)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        await deps.touch_user_activity(user_id, user)

        if redis_conn:
            t = time.perf_counter()
            hot_exists = await redis_conn.exists(f"coins_hot:{user_id}")
            observe_storage_timing("redis", "coins_hot_exists", "profile", time.perf_counter() - t)
            if not hot_exists:
                t = time.perf_counter()
                db_user = await deps.get_user(user_id)
                observe_storage_timing("db", "get_user", "profile", time.perf_counter() - t)
                db_coins = int((db_user or {}).get("coins", 0))
                t = time.perf_counter()
                await ensure_coins_hot_initialized(user_id, db_coins, redis_conn)
                observe_storage_timing(
                    "redis", "ensure_coins_hot_initialized", "profile", time.perf_counter() - t
                )

        state = await deps.build_realtime_player_state(user_id)
        if state is None:
            raise HTTPException(status_code=404, detail="User not found")

        if redis_conn and PROFILE_STATE_CACHE_TTL_MS > 0:
            try:
                t = time.perf_counter()
                await redis_conn.psetex(
                    state_cache_key,
                    PROFILE_STATE_CACHE_TTL_MS,
                    json.dumps(state),
                )
                observe_storage_timing(
                    "redis",
                    "profile_state_cache_set",
                    "profile",
                    time.perf_counter() - t,
                )
            except Exception:
                pass

        return state

    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in get_user_data: {e}")
        observe_storage_error("app", "get_user_data", "profile")
        raise HTTPException(status_code=500, detail="Internal server error")
