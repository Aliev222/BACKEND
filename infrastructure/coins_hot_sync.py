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
    user_hot_key = f"user_hot:{int(user_id)}"

    # new_hot = max(hot + delta, db_coins) when hot exists
    # new_hot = db_coins (or delta fallback) when hot is missing
    # mirror user_hot.coins when present.
    sync_lua = """
    local hot_key = KEYS[1]
    local user_hot_key = KEYS[2]
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
        if redis.call('EXISTS', user_hot_key) == 1 then
            redis.call('HSET', user_hot_key, 'coins', tostring(baseline))
        end
        return baseline
    end

    local current = tonumber(redis.call('GET', hot_key) or '0')
    local candidate = current + delta
    if db_coins ~= nil and candidate < db_coins then
        candidate = db_coins
    end
    redis.call('SET', hot_key, tostring(candidate))
    if redis.call('EXISTS', user_hot_key) == 1 then
        redis.call('HSET', user_hot_key, 'coins', tostring(candidate))
    end
    return candidate
    """

    try:
        return int(
            await redis_conn.eval(
                sync_lua,
                2,
                hot_key,
                user_hot_key,
                str(int(delta)),
                "" if db_coins is None else str(int(db_coins)),
            )
        )
    except Exception as exc:
        logger.warning(
            "coins_hot increment sync failed user=%s delta=%s db=%s err=%s",
            user_id,
            delta,
            db_coins,
            exc,
        )
        return None


async def sync_hot_after_db_decrement(
    user_id: int, spent: int, db_coins: int | None = None
) -> int | None:
    """
    Mirror DB coin decrements into Redis hot-state.

    This is for DB-first spend paths (upgrade, rebirth-like costs, bets).
    It updates coins_hot only (NOT coins_pending) to preserve Redis-first click flow.
    """
    if int(spent or 0) <= 0:
        return None

    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return None

    hot_key = f"coins_hot:{int(user_id)}"
    user_hot_key = f"user_hot:{int(user_id)}"

    # new_hot = max(db_coins, hot - spent) when hot exists
    # new_hot = db_coins when hot is missing
    # mirror user_hot.coins when present.
    sync_lua = """
    local hot_key = KEYS[1]
    local user_hot_key = KEYS[2]
    local spent = tonumber(ARGV[1])
    local db_coins_raw = ARGV[2]

    local db_coins = nil
    if db_coins_raw ~= '' then
        db_coins = tonumber(db_coins_raw)
    end

    local new_hot = 0
    if redis.call('EXISTS', hot_key) == 0 then
        new_hot = db_coins or 0
        redis.call('SET', hot_key, tostring(new_hot))
    else
        local current = tonumber(redis.call('GET', hot_key) or '0')
        new_hot = current - spent
        if db_coins ~= nil and new_hot < db_coins then
            new_hot = db_coins
        end
        if new_hot < 0 then
            new_hot = 0
        end
        redis.call('SET', hot_key, tostring(new_hot))
    end

    if redis.call('EXISTS', user_hot_key) == 1 then
        redis.call('HSET', user_hot_key, 'coins', tostring(new_hot))
    end

    return new_hot
    """

    try:
        return int(
            await redis_conn.eval(
                sync_lua,
                2,
                hot_key,
                user_hot_key,
                str(int(spent)),
                "" if db_coins is None else str(int(db_coins)),
            )
        )
    except Exception as exc:
        logger.warning(
            "coins_hot decrement sync failed user=%s spent=%s db=%s err=%s",
            user_id,
            spent,
            db_coins,
            exc,
        )
        return None


async def get_hot_authoritative_coins(user_id: int, db_fallback: int = 0) -> int:
    """
    Get authoritative hot coins for non-click response contract.

    Returns coins_hot:{user_id} if exists, otherwise db_fallback.
    This is the single source of truth for non-click coin mutation responses.
    """
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return int(db_fallback)

    try:
        hot_coins = await redis_conn.get(f"coins_hot:{int(user_id)}")
        if hot_coins is not None:
            return int(hot_coins)
        return int(db_fallback)
    except Exception as exc:
        logger.warning(
            "get_hot_authoritative_coins failed user=%s err=%s",
            user_id,
            exc,
        )
        return int(db_fallback)
