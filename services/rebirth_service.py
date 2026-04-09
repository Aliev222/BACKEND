from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from core.game_logic import (
    get_tap,
    get_profit_per_hour,
    get_max_energy,
    resolve_progression_level,
)

REBIRTH_MIN_LEVEL = 100
REBIRTH_COST_COINS = 1_000_000


@dataclass(frozen=True)
class RebirthServiceDeps:
    require_telegram_user: Callable[..., Awaitable[Any]]
    require_dual_rate_limit: Callable[..., Awaitable[Any]]
    require_user_action_lock: Callable[..., Awaitable[Any]]
    get_user: Callable[[int], Awaitable[dict | None]]
    update_user_if_matches: Callable[[int, dict, dict], Awaitable[dict | None]]
    invalidate_user_cache: Callable[[int], Awaitable[Any]]
    get_redis_or_none: Callable[[], Awaitable[Any]]
    logger: Any
    ENERGY_REGEN_SECONDS: int


def _resolve_global_level(user: dict) -> int:
    return resolve_progression_level(user)


async def _sync_rebirth_hot_state(
    deps: RebirthServiceDeps,
    *,
    user_id: int,
    rebirth_count: int,
    db_coins_after: int,
    tap_power: int,
    level: int,
    profit_per_hour: int,
    max_energy: int,
    energy: int,
) -> int:
    redis_conn = await deps.get_redis_or_none()
    if not redis_conn:
        return db_coins_after

    sync_lua = """
    local coins_hot_key = KEYS[1]
    local user_hot_key = KEYS[2]
    local energy_key = KEYS[3]

    local db_coins_after = tonumber(ARGV[1])
    local rebirth_count = tonumber(ARGV[2])
    local tap_power = tonumber(ARGV[3])
    local level = tonumber(ARGV[4])
    local profit_per_hour = tonumber(ARGV[5])
    local max_energy = tonumber(ARGV[6])
    local energy = tonumber(ARGV[7])

    local hot_coins = db_coins_after
    if redis.call('EXISTS', coins_hot_key) == 1 then
        local current_hot = tonumber(redis.call('GET', coins_hot_key) or tostring(db_coins_after))
        hot_coins = current_hot
        if hot_coins < db_coins_after then
            hot_coins = db_coins_after
        end
    end
    redis.call('SET', coins_hot_key, tostring(hot_coins))

    local version = tonumber(redis.call('HGET', user_hot_key, 'version') or '1')
    if version < 1 then
        version = 1
    end

    redis.call('HSET', user_hot_key,
        'coins', tostring(hot_coins),
        'rebirth_count', tostring(rebirth_count),
        'level', tostring(level),
        'tap_power', tostring(tap_power),
        'max_energy', tostring(max_energy),
        'energy', tostring(energy),
        'profit_per_hour', tostring(profit_per_hour),
        'version', tostring(version)
    )
    redis.call('HDEL', user_hot_key, 'multitap_level', 'profit_level', 'energy_level')

    return hot_coins
    """
    try:
        return int(
            await redis_conn.eval(
                sync_lua,
                3,
                f"coins_hot:{user_id}",
                f"user_hot:{user_id}",
                f"energy:v2:{user_id}",
                str(db_coins_after),
                str(rebirth_count),
                str(tap_power),
                str(level),
                str(profit_per_hour),
                str(max_energy),
                str(energy),
            )
        )
    except Exception as exc:
        deps.logger.warning(
            "rebirth hot-state sync failed user=%s err=%s",
            user_id,
            exc,
        )
        return db_coins_after


async def process_rebirth_service(payload: Any, request: Any, deps: RebirthServiceDeps):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "rebirth", request, payload.user_id, 8, 60, ip_limit=16
        )
        await deps.require_user_action_lock("rebirth", payload.user_id, ttl=1.5)

        user = await deps.get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        current_level = _resolve_global_level(user)
        if current_level < REBIRTH_MIN_LEVEL:
            raise HTTPException(
                status_code=400, detail="Rebirth requires level 100"
            )

        current_coins = int(user.get("coins", 0) or 0)
        if current_coins < REBIRTH_COST_COINS:
            raise HTTPException(
                status_code=400, detail="Not enough coins for rebirth"
            )

        current_rebirth_count = max(0, int(user.get("rebirth_count", 0) or 0))
        next_rebirth_count = current_rebirth_count + 1
        next_coins = current_coins - REBIRTH_COST_COINS

        next_level = 0
        next_tap_power = get_tap(next_level, next_rebirth_count)
        current_profit_per_hour = int(user.get("profit_per_hour", 0) or 0)
        base_next_profit_per_hour = get_profit_per_hour(next_level)
        # Keep passive income on rebirth; never downgrade hourly value.
        next_profit_per_hour = max(current_profit_per_hour, base_next_profit_per_hour)
        next_max_energy = get_max_energy(next_level)
        next_energy = min(int(user.get("energy", 0) or 0), next_max_energy)

        expected = {
            "coins": current_coins,
            "level": int(user.get("level", 0) or 0),
            "rebirth_count": current_rebirth_count,
        }
        updates = {
            "coins": next_coins,
            "rebirth_count": next_rebirth_count,
            "level": next_level,
            # Deprecated mirrors kept for compatibility.
            "multitap_level": next_level,
            "profit_level": next_level,
            "energy_level": next_level,
            "profit_per_tap": next_tap_power,
            "profit_per_hour": next_profit_per_hour,
            "max_energy": next_max_energy,
            "energy": next_energy,
        }

        updated_user = await deps.update_user_if_matches(payload.user_id, expected, updates)
        if not updated_user:
            raise HTTPException(
                status_code=409, detail="Rebirth state changed, retry"
            )

        hot_coins = await _sync_rebirth_hot_state(
            deps,
            user_id=payload.user_id,
            rebirth_count=next_rebirth_count,
            db_coins_after=next_coins,
            tap_power=next_tap_power,
            level=next_level,
            profit_per_hour=next_profit_per_hour,
            max_energy=next_max_energy,
            energy=next_energy,
        )
        await deps.invalidate_user_cache(payload.user_id)

        state_updated_at = int(time.time() * 1000)
        return {
            "success": True,
            "coins": hot_coins,
            "rebirth_count": next_rebirth_count,
            "level": next_level,
            "multitap_level": next_level,
            "profit_level": next_level,
            "energy_level": next_level,
            "profit_per_tap": next_tap_power,
            "profit_per_hour": next_profit_per_hour,
            "max_energy": next_max_energy,
            "energy": next_energy,
            "state_updated_at": state_updated_at,
            "state_version": state_updated_at,
            "server_time": datetime.utcnow().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        deps.logger.error("Error in process_rebirth_service: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")
