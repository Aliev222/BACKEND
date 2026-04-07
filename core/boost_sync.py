"""
Canonical boost state builder for user_hot synchronization.

This module provides a single source of truth for building normalized
boost state that is used across:
- boost activation endpoints
- click path initialization
- user_hot sync after DB updates
"""

import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def build_normalized_user_hot_boosts(
    extra: dict, ghost_boost_multiplier: int = 5
) -> dict:
    """
    Build canonical normalized boosts payload for user_hot:boosts.

    This is the ONLY function that should build boosts for user_hot.
    All boost activation endpoints and click path initialization must use this.

    Args:
        extra: User extra_data dict from DB
        ghost_boost_multiplier: Default ghost boost multiplier

    Returns:
        Normalized boosts dict with all required fields for Lua click path
    """
    from core.realtime_state import _check_boost_active

    mega_active, _, _ = _check_boost_active(extra, ["active_boosts", "mega_boost"])
    ghost_active, _, ghost_mult = _check_boost_active(
        extra, ["active_boosts", "ghost_boost"]
    )
    daily_active, _, _ = _check_boost_active(
        extra, ["active_boosts", "daily_infinite_energy"]
    )
    tap_active, _, tap_mult = _check_boost_active(
        extra, ["video_task_boosts", "tap_boost"]
    )

    # Ensure ghost multiplier has valid default
    if ghost_mult < 1:
        ghost_mult = ghost_boost_multiplier

    # Ensure tap multiplier has valid default
    if tap_mult < 1:
        tap_mult = 1

    return {
        "mega_boost_active": mega_active,
        "ghost_boost_active": ghost_active,
        "ghost_boost_multiplier": ghost_mult,
        "daily_infinite_energy_active": daily_active,
        "task_tap_boost_active": tap_active,
        "task_tap_boost_multiplier": tap_mult,
    }


async def sync_boosts_to_user_hot(
    user_id: int, extra: dict, ghost_boost_multiplier: int = 5, redis_conn: Any = None
) -> bool:
    """
    Sync canonical boost state to user_hot:boosts.

    This should be called immediately after any boost activation/deactivation
    to ensure click path sees fresh boost state.

    Args:
        user_id: User ID
        extra: Fresh extra_data from DB
        ghost_boost_multiplier: Default ghost boost multiplier
        redis_conn: Redis connection (if None, will get from pool)

    Returns:
        True if sync succeeded, False otherwise
    """
    if redis_conn is None:
        from infrastructure.redis import get_redis_or_none

        redis_conn = await get_redis_or_none()

    if not redis_conn:
        logger.warning(
            "Cannot sync boosts to user_hot: Redis unavailable for user %s", user_id
        )
        return False

    try:
        boosts = build_normalized_user_hot_boosts(extra, ghost_boost_multiplier)
        user_hot_key = f"user_hot:{user_id}"

        await redis_conn.hset(user_hot_key, "boosts", json.dumps(boosts))

        logger.info("Synced boosts to user_hot for user %s: %s", user_id, boosts)
        return True
    except Exception as e:
        logger.error("Failed to sync boosts to user_hot for user %s: %s", user_id, e)
        return False
