from dataclasses import dataclass
from datetime import datetime
import time
from typing import Any, Awaitable, Callable

from fastapi import HTTPException
from observability.metrics import observe_storage_error, observe_storage_timing


@dataclass(frozen=True)
class UpgradesServiceDeps:
    require_telegram_user: Callable[..., Awaitable[Any]]
    require_dual_rate_limit: Callable[..., Awaitable[Any]]
    require_user_action_lock: Callable[..., Awaitable[Any]]
    get_user: Callable[[int], Awaitable[dict | None]]
    update_user_if_matches: Callable[[int, dict, dict], Awaitable[dict | None]]
    get_redis_or_none: Callable[[], Awaitable[Any]]
    get_tap_value: Callable[[int], int]
    get_hour_value: Callable[[int], int]
    get_max_energy: Callable[[int], int]
    logger: Any
    GLOBAL_UPGRADE_PRICES: list[int]
    MAX_UPGRADE_LEVEL: int


def get_global_upgrade_level_service(user: dict) -> int:
    return max(
        int(user.get("multitap_level", 0)),
        int(user.get("profit_level", 0)),
        int(user.get("energy_level", 0)),
    )


async def apply_global_upgrade_for_user_service(
    user_id: int, user: dict, deps: UpgradesServiceDeps
) -> dict:
    current_level = get_global_upgrade_level_service(user)
    if current_level >= deps.MAX_UPGRADE_LEVEL:
        raise HTTPException(status_code=400, detail="Max level reached")

    price = deps.GLOBAL_UPGRADE_PRICES[current_level]
    current_coins = int(user.get("coins", 0))
    if current_coins < price:
        raise HTTPException(status_code=400, detail="Not enough coins")

    new_level = current_level + 1
    new_profit_per_tap = deps.get_tap_value(new_level)
    new_profit_per_hour = deps.get_hour_value(new_level)
    new_max_energy = deps.get_max_energy(new_level)
    new_coins = current_coins - price

    ml = int(user.get("multitap_level", 0))
    pl = int(user.get("profit_level", 0))
    el = int(user.get("energy_level", 0))

    expected = {
        "coins": current_coins,
        "multitap_level": ml,
        "profit_level": pl,
        "energy_level": el,
    }
    updates = {
        "coins": new_coins,
        "multitap_level": new_level,
        "profit_level": new_level,
        "energy_level": new_level,
        "profit_per_tap": new_profit_per_tap,
        "profit_per_hour": new_profit_per_hour,
        "max_energy": new_max_energy,
        "energy": new_max_energy,
    }

    t = time.perf_counter()
    updated_user = await deps.update_user_if_matches(user_id, expected, updates)
    observe_storage_timing(
        "db", "update_user_if_matches", "upgrades", time.perf_counter() - t
    )
    if not updated_user:
        raise HTTPException(status_code=409, detail="Upgrade state changed, retry")

    # Keep energy:v2 in sync because upgrade resets energy to full max.
    try:
        redis_conn = await deps.get_redis_or_none()
        if redis_conn:
            t = time.perf_counter()
            await redis_conn.hset(
                f"energy:v2:{user_id}",
                mapping={
                    "value": str(new_max_energy),
                    "updated_at": str(datetime.utcnow().timestamp()),
                    "max_energy": str(new_max_energy),
                },
            )
            observe_storage_timing("redis", "energy_v2_hset", "upgrades", time.perf_counter() - t)
    except Exception:
        observe_storage_error("redis", "energy_v2_hset", "upgrades")
        pass

    next_cost = (
        deps.GLOBAL_UPGRADE_PRICES[new_level]
        if new_level < len(deps.GLOBAL_UPGRADE_PRICES)
        else 0
    )
    return {
        "success": True,
        "coins": int(updated_user.get("coins", new_coins)),
        "new_level": new_level,
        "levels": {
            "multitap": new_level,
            "profit": new_level,
            "energy": new_level,
        },
        "prices": {
            "global": next_cost,
        },
        "next_cost": next_cost,
        "profit_per_tap": new_profit_per_tap,
        "profit_per_hour": new_profit_per_hour,
        "max_energy": new_max_energy,
        "energy": new_max_energy,
        "server_time": datetime.utcnow().isoformat(),
    }


async def process_upgrade_service(payload: Any, request: Any, deps: UpgradesServiceDeps):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "upgrade", request, payload.user_id, 25, 60, ip_limit=50
        )
        await deps.require_user_action_lock("upgrade", payload.user_id, ttl=0.35)
        # Authoritative spend checks must use DB user state (coins are hot-state).
        t = time.perf_counter()
        user = await deps.get_user(payload.user_id)
        observe_storage_timing("db", "get_user", "upgrades", time.perf_counter() - t)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return await apply_global_upgrade_for_user_service(payload.user_id, user, deps)
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in process_upgrade: {e}")
        observe_storage_error("app", "process_upgrade", "upgrades")
        raise HTTPException(status_code=500, detail="Internal server error")


async def process_upgrade_all_service(
    payload: Any, request: Any, deps: UpgradesServiceDeps
):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "upgrade_all", request, payload.user_id, 25, 60, ip_limit=50
        )
        await deps.require_user_action_lock("upgrade_all", payload.user_id, ttl=0.35)
        # Upgrade-all already requires authoritative DB state for coins.
        t = time.perf_counter()
        user = await deps.get_user(payload.user_id)
        observe_storage_timing("db", "get_user", "upgrades", time.perf_counter() - t)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return await apply_global_upgrade_for_user_service(payload.user_id, user, deps)
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in process_upgrade_all: {e}")
        observe_storage_error("app", "process_upgrade_all", "upgrades")
        raise HTTPException(status_code=500, detail="Internal server error")
