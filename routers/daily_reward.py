"""
Daily reward routes extracted from legacy.py (Patch 7.3).
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Request, HTTPException

from routers.legacy import (
    require_telegram_user,
    get_user_cached,
    parse_extra_data,
    update_user,
    invalidate_user_cache,
    UserIdRequest,
    DAILY_REWARD_MAX_DAYS,
    get_daily_reward_progress,
    is_daily_infinite_energy_active,
)

router = APIRouter(tags=["daily-reward"])
logger = logging.getLogger(__name__)

# Daily reward coin amounts by day (1-indexed)
_DAILY_REWARD_COINS = [
    500,
    1000,
    2500,
    5000,
    10000,
    15000,
    25000,
    30000,
    40000,
    50000,
    75000,
    100000,
    150000,
    200000,
    250000,
    300000,
    400000,
    500000,
    750000,
    1000000,
    1500000,
    2000000,
    2500000,
    3000000,
    5000000,
    7500000,
    10000000,
    15000000,
    20000000,
    50000000,
]


def _daily_reward_coins_for_day(day: int) -> int:
    """Return coin reward for the given day (1-indexed)."""
    idx = max(0, min(day - 1, len(_DAILY_REWARD_COINS) - 1))
    return _DAILY_REWARD_COINS[idx]


router = APIRouter(tags=["daily-reward"])
logger = logging.getLogger(__name__)


@router.get("/api/daily-reward/status/{user_id}")
async def get_daily_reward_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        claimed_days, last_claim_date = get_daily_reward_progress(extra)
        today = datetime.utcnow().date().isoformat()
        claim_available = (
            claimed_days < DAILY_REWARD_MAX_DAYS and last_claim_date != today
        )
        next_day = min(claimed_days + 1, DAILY_REWARD_MAX_DAYS)
        infinite_energy_active, infinite_energy_expires_at = (
            is_daily_infinite_energy_active(user)
        )

        return {
            "success": True,
            "claimed_days": claimed_days,
            "claim_available": claim_available,
            "next_day": next_day,
            "infinite_energy_active": infinite_energy_active,
            "infinite_energy_expires_at": infinite_energy_expires_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_daily_reward_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/daily-reward/claim")
async def claim_daily_reward(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        claimed_days, last_claim_date = get_daily_reward_progress(extra)
        today = datetime.utcnow().date().isoformat()

        if claimed_days >= DAILY_REWARD_MAX_DAYS:
            raise HTTPException(status_code=400, detail="All daily rewards claimed")
        if last_claim_date == today:
            raise HTTPException(status_code=400, detail="Already claimed today")

        next_day = min(claimed_days + 1, DAILY_REWARD_MAX_DAYS)
        coins_reward = _daily_reward_coins_for_day(next_day)

        claimed_days += 1
        extra["daily_reward_claimed_days"] = claimed_days
        extra["daily_reward_last_claim_date"] = today

        updates = {
            "coins": int(user.get("coins", 0)) + coins_reward,
            "extra_data": extra,
        }

        if next_day == DAILY_REWARD_MAX_DAYS:
            from datetime import timedelta

            infinite_energy_expires = (
                datetime.utcnow() + timedelta(days=1)
            ).isoformat()
            extra["daily_infinite_energy_expires_at"] = infinite_energy_expires
            updates["extra_data"] = extra

        await update_user(payload.user_id, updates)
        await invalidate_user_cache(payload.user_id)
        refreshed_user = await get_user_cached(payload.user_id)

        return {
            "success": True,
            "day": next_day,
            "coins_reward": coins_reward,
            "coins": int((refreshed_user or {}).get("coins", 0)),
            "claimed_days": claimed_days,
            "all_claimed": claimed_days >= DAILY_REWARD_MAX_DAYS,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in claim_daily_reward: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
