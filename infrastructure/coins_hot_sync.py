import logging

from infrastructure.redis import get_redis_or_none

logger = logging.getLogger(__name__)


async def sync_hot_after_db_increment(
    user_id: int, delta: int, db_coins: int | None = None
) -> int | None:
    """
    Mirror DB coin increments into Redis hot-state.

    This is for DB-first reward/income paths (passive, daily, tasks, referral).
    It updates coins_hot only (NOT coins_pending) to avoid double-flush into DB.
    """
    if int(delta or 0) <= 0:
        return None

    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return None

    hot_key = f"coins_hot:{int(user_id)}"

    # new_hot = max(hot + delta, db_coins) when hot exists
    # new_hot = db_coins (or delta fallback) when hot is missing
    sync_lua = """
    local hot_key = KEYS[1]
    local delta = tonumber(ARGV[1])
    local db_coins_raw = ARGV[2]

    local has_db = db_coins_raw ~= ''
    local db_coins = nil
    if has_db then
        db_coins = tonumber(db_coins_raw)
    end

    if redis.call('EXISTS', hot_key) == 0 then
        local baseline = db_coins
        if baseline == nil then
            baseline = delta
        end
        redis.call('SET', hot_key, tostring(baseline))
        return baseline
    end

    local current = tonumber(redis.call('GET', hot_key))
    local candidate = current + delta
    if db_coins ~= nil and candidate < db_coins then
        candidate = db_coins
    end
    redis.call('SET', hot_key, tostring(candidate))
    return candidate
    """

    try:
        return int(
            await redis_conn.eval(
                sync_lua,
                1,
                hot_key,
                str(int(delta)),
                "" if db_coins is None else str(int(db_coins)),
            )
        )
    except Exception as exc:
        logger.warning(
            "coins_hot sync failed user=%s delta=%s db=%s err=%s",
            user_id,
            delta,
            db_coins,
            exc,
        )
        return None
