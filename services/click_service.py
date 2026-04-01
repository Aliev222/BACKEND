import time
import logging
from datetime import datetime
from fastapi import HTTPException
import redis.asyncio as redis

from core.game_logic import (
    calculate_current_energy,
    get_allowed_clicks,
    get_tap_value,
    resolve_max_energy,
)
from core.game_config import (
    CLICK_SUSPICION_SOFT_LIMIT,
    CLICK_SUSPICIOUS_OVERSHOOT,
    MAX_CLICK_BATCH_SIZE,
    MAX_REAL_CLICKS_PER_SECOND,
)
from infrastructure.queue import buffer_clicks, buffer_energy

logger = logging.getLogger(__name__)

SKIN_MULTIPLIERS = {
    "default.pngSP": 1.0,
    "10lvl.pngSP": 1.2,
    "25lvl.pngSP": 1.2,
    "50lvl.pngSP": 1.2,
    "75lvl.pngSP": 1.2,
    "100lvl.pngSP": 1.2,
    "video.pngSP": 1.5,
    "video2.pngSP": 1.5,
    "video3.pngSP": 1.5,
    "video4.pngSP": 1.5,
    "video5.pngSP": 1.5,
    "video6.pngSP": 1.5,
    "video7.pngSP": 1.5,
    "video8.pngSP": 1.5,
    "refferal.pngSP": 1.8,
    "retro.pngSP": 1.7,
    "insta.pngSP": 1.5,
    "tiktok.pngSP": 1.5,
    "telega.pngSP": 1.5,
    "stars1.pngSP": 2.0,
    "stars2.pngSP": 2.0,
    "stars3.pngSP": 2.0,
    "stars4.pngSP": 2.0,
    "stars5.pngSP": 2.0,
    "stars6.pngSP": 2.0,
    "stars7.pngSP": 2.0,
    "stars8.pngSP": 2.0,
}


def get_skin_multiplier(selected_skin: str) -> float:
    return SKIN_MULTIPLIERS.get(selected_skin, 1.0)


def is_ghost_boost_active(extra: dict, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    active_boosts = extra.get("active_boosts", {})
    if not isinstance(active_boosts, dict):
        return False
    ghost = active_boosts.get("ghost_boost")
    if not isinstance(ghost, dict):
        return False
    expires_at = ghost.get("expires_at")
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > now
    except (ValueError, TypeError):
        return False


def is_mega_boost_active(extra: dict, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    active_boosts = extra.get("active_boosts", {})
    if not isinstance(active_boosts, dict):
        return False
    mega = active_boosts.get("mega_boost")
    if not isinstance(mega, dict):
        return False
    expires_at = mega.get("expires_at")
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > now
    except (ValueError, TypeError):
        return False


def is_daily_infinite_energy_active(extra: dict, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    boosts = extra.get("active_boosts", {})
    if not isinstance(boosts, dict):
        return False
    daily = boosts.get("daily_infinite_energy")
    if not isinstance(daily, dict):
        return False
    expires_at = daily.get("expires_at")
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > now
    except (ValueError, TypeError):
        return False


def has_free_energy(extra: dict, now: datetime | None = None) -> bool:
    return (
        is_ghost_boost_active(extra, now)
        or is_mega_boost_active(extra, now)
        or is_daily_infinite_energy_active(extra, now)
    )


async def process_clicks(
    redis_conn: redis.Redis,
    user_id: int,
    user: dict,
    requested_clicks: int,
    batch_id: str,
    now: datetime | None = None,
) -> dict:
    now = now or datetime.utcnow()

    multitap_level = int(user.get("multitap_level", 0))
    energy_level = int(user.get("energy_level", 0))
    extra = user.get("extra_data", {})
    if isinstance(extra, str):
        import json

        extra = json.loads(extra)

    selected_skin = extra.get("selected_skin", "default.pngSP")
    skin_multiplier = get_skin_multiplier(selected_skin)

    tap_value = get_tap_value(multitap_level)
    coin_per_tap = int(tap_value * skin_multiplier)

    ghost_active = is_ghost_boost_active(extra, now)
    mega_active = is_mega_boost_active(extra, now)
    free_energy = has_free_energy(extra, now)

    if ghost_active:
        from core.game_config import GHOST_BOOST_MULTIPLIER

        coin_per_tap *= GHOST_BOOST_MULTIPLIER

    if mega_active:
        coin_per_tap *= 2

    max_energy = resolve_max_energy(user)
    current_energy = calculate_current_energy(user, now)

    if not free_energy:
        effective_clicks = min(requested_clicks, current_energy)
    else:
        effective_clicks = requested_clicks

    allowed = get_allowed_clicks(user, now, requested_clicks)
    effective_clicks = min(effective_clicks, allowed)

    if effective_clicks <= 0:
        return {
            "accepted": 0,
            "coins_earned": 0,
            "energy_remaining": current_energy,
            "message": "No energy or clicks allowed",
        }

    coins_earned = effective_clicks * coin_per_tap
    energy_spent = effective_clicks if not free_energy else 0
    new_energy = current_energy - energy_spent

    await buffer_clicks(redis_conn, user_id, coins_earned, effective_clicks)

    if not free_energy:
        await buffer_energy(redis_conn, user_id, new_energy, now.timestamp())

    return {
        "accepted": effective_clicks,
        "coins_earned": coins_earned,
        "energy_remaining": new_energy,
        "coin_per_tap": coin_per_tap,
    }
