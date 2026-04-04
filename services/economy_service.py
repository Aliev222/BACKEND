import logging
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from repositories.user_repo import (
    get_user_by_id,
    spend_coins_if_enough,
    update_user_atomic,
)
from core.game_logic import get_tap_value_with_rebirth, get_hour_value, get_max_energy
from core.game_config import GLOBAL_UPGRADE_PRICES, MAX_UPGRADE_LEVEL

logger = logging.getLogger(__name__)


async def apply_global_upgrade(session: AsyncSession, user_id: int) -> dict:
    user = await get_user_by_id(session, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    multitap_level = int(user.get("multitap_level", 0))
    profit_level = int(user.get("profit_level", 0))
    energy_level = int(user.get("energy_level", 0))
    rebirth_count = max(0, int(user.get("rebirth_count", 0) or 0))

    global_level = max(multitap_level, profit_level, energy_level)
    if global_level >= MAX_UPGRADE_LEVEL:
        raise HTTPException(status_code=400, detail="Max upgrade level reached")

    price = GLOBAL_UPGRADE_PRICES[global_level]
    user_coins = int(user.get("coins", 0))

    if user_coins < price:
        raise HTTPException(status_code=400, detail="Not enough coins")

    spent = await spend_coins_if_enough(session, user_id, price)
    if not spent:
        raise HTTPException(status_code=409, detail="Concurrent modification, retry")

    new_level = global_level + 1
    new_tap_value = get_tap_value_with_rebirth(new_level, rebirth_count)
    new_hour_value = get_hour_value(new_level)
    new_max_energy = get_max_energy(new_level)

    updated = await update_user_atomic(
        session,
        user_id,
        expected_coins=user_coins - price,
        multitap_level=new_level,
        profit_level=new_level,
        energy_level=new_level,
        level=new_level,
        profit_per_tap=new_tap_value,
        profit_per_hour=new_hour_value,
        max_energy=new_max_energy,
        energy=new_max_energy,
    )

    if not updated:
        raise HTTPException(status_code=409, detail="Upgrade failed, state changed")

    return {
        "success": True,
        "new_level": new_level,
        "price": price,
        "profit_per_tap": new_tap_value,
        "profit_per_hour": new_hour_value,
        "max_energy": new_max_energy,
        "next_price": GLOBAL_UPGRADE_PRICES[new_level]
        if new_level < MAX_UPGRADE_LEVEL
        else None,
    }
