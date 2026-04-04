"""
Realtime Player State Assembler

Single authoritative source for assembling frontend-facing player state.
Reads from the correct hot-state Redis keys, not from stale user:cache.

Design:
- energy from energy:v2:{user_id} (NOT user:cache)
- coins from coins_hot:{user_id}
- boosts derived from expires_at (NOT active: flags)
- static profile data from user:cache (username, levels, skins, etc.)
- state_version = monotonic server timestamp for frontend ordering
"""

import json
import logging
import time
from datetime import datetime
from typing import Any

from core.game_config import (
    ENERGY_REGEN_SECONDS,
    USER_CACHE_PREFIX,
    USER_CACHE_TTL,
)
from core.game_logic import get_hour_value, get_tap_value_with_rebirth, get_max_energy
from core.ton_utils import get_ton_wallet_from_user
from core.skins import normalize_owned_skins, normalize_selected_skin, DEFAULT_SKIN_ID
from core.tasks import get_active_video_task_boost
from infrastructure.redis import get_redis_or_none
from DATABASE.base import get_user

logger = logging.getLogger(__name__)


# ─── Energy source of truth ──────────────────────────────────────────────────


async def read_energy_v2(user_id: int, max_energy: int) -> dict:
    """
    Read authoritative energy from energy:v2:{user_id}.

    Returns:
      {
        "energy": int,
        "max_energy": int,
        "updated_at": float (timestamp),
        "from_cache": bool,
      }

    If energy:v2 is missing, falls back to DB energy with explicit warning
    and initializes energy:v2 for future reads.
    """
    redis_conn = await get_redis_or_none()
    energy_key = f"energy:v2:{user_id}"
    now = time.time()

    if redis_conn:
        cached = await redis_conn.hgetall(energy_key)
        if cached:
            cached_max = int(cached.get("max_energy", max_energy))
            cached_value = int(cached.get("value", 0))
            cached_updated = float(cached.get("updated_at", now))
            elapsed = now - cached_updated
            regen = int(elapsed // ENERGY_REGEN_SECONDS)
            current = min(cached_max, cached_value + regen)
            return {
                "energy": current,
                "max_energy": cached_max,
                "updated_at": cached_updated,
                "from_cache": True,
            }

    # Fallback: compute from DB and initialize energy:v2
    user = await get_user(user_id)
    if user:
        stored = int(user.get("energy", 0))
        last_update = user.get("last_energy_update")
        if last_update:
            try:
                from core.game_logic import normalize_dt

                last_dt = normalize_dt(last_update)
                if last_dt:
                    elapsed_db = max(0, (datetime.utcnow() - last_dt).total_seconds())
                    regen_db = int(elapsed_db // ENERGY_REGEN_SECONDS)
                    stored = min(max_energy, stored + regen_db)
            except Exception:
                pass
        stored = min(stored, max_energy)

        # Initialize energy:v2 for future reads
        if redis_conn:
            try:
                await redis_conn.hset(
                    energy_key,
                    mapping={
                        "value": str(stored),
                        "updated_at": str(now),
                        "max_energy": str(max_energy),
                    },
                )
                # NOTE: No TTL — energy:v2 is persistent hot-state like coins_hot.
            except Exception:
                pass

        return {
            "energy": stored,
            "max_energy": max_energy,
            "updated_at": now,
            "from_cache": False,
        }

    return {
        "energy": 0,
        "max_energy": max_energy,
        "updated_at": now,
        "from_cache": False,
    }


async def write_energy_v2(user_id: int, value: int, max_energy: int) -> None:
    """Write authoritative energy to energy:v2:{user_id}."""
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return
    energy_key = f"energy:v2:{user_id}"
    now = time.time()
    try:
        await redis_conn.hset(
            energy_key,
            mapping={
                "value": str(value),
                "updated_at": str(now),
                "max_energy": str(max_energy),
            },
        )
        # NOTE: No TTL — energy:v2 is persistent hot-state like coins_hot.
    except Exception as e:
        logger.warning("Failed to write energy:v2 for user %s: %s", user_id, e)


# ─── Boost truth model ───────────────────────────────────────────────────────


def _check_boost_active(
    extra: dict, boost_path: list[str]
) -> tuple[bool, str | None, int]:
    """
    Check if a boost is active by deriving from expires_at only.
    The `active: true` flag is NOT trusted as source of truth.

    Returns: (is_active, expires_at_iso_or_None, multiplier)
    """
    current = extra
    for key in boost_path:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return False, None, 1
    if not isinstance(current, dict):
        return False, None, 1

    expires_at_str = current.get("expires_at")
    multiplier = int(current.get("multiplier", 1) or 1)

    if not expires_at_str:
        return False, None, multiplier

    try:
        expires_dt = datetime.fromisoformat(str(expires_at_str))
        is_active = datetime.utcnow() < expires_dt
        return is_active, str(expires_at_str) if is_active else None, multiplier
    except Exception:
        return False, None, multiplier


def get_all_boost_states(extra: dict) -> dict:
    """
    Derive ALL boost states from expires_at timestamps.
    Returns a dict with canonical boost state.
    """
    mega_active, mega_expires, _ = _check_boost_active(
        extra, ["active_boosts", "mega_boost"]
    )
    ghost_active, ghost_expires, ghost_mult = _check_boost_active(
        extra, ["active_boosts", "ghost_boost"]
    )
    daily_active, daily_expires, _ = _check_boost_active(
        extra, ["active_boosts", "daily_infinite_energy"]
    )
    tap_active, tap_expires, tap_mult = _check_boost_active(
        extra, ["video_task_boosts", "tap_boost"]
    )
    passive_active, passive_expires, passive_mult = _check_boost_active(
        extra, ["video_task_boosts", "passive_boost"]
    )

    return {
        "mega_boost_active": mega_active,
        "mega_boost_expires_at": mega_expires,
        "ghost_boost_active": ghost_active,
        "ghost_boost_expires_at": ghost_expires,
        "ghost_boost_multiplier": ghost_mult,
        "daily_infinite_energy_active": daily_active,
        "daily_infinite_energy_expires_at": daily_expires,
        "task_tap_boost_active": tap_active,
        "task_tap_boost_expires_at": tap_expires,
        "task_tap_boost_multiplier": tap_mult,
        "task_passive_boost_active": passive_active,
        "task_passive_boost_expires_at": passive_expires,
        "task_passive_boost_multiplier": passive_mult,
    }


# ─── Realtime state assembler ────────────────────────────────────────────────


async def build_realtime_player_state(user_id: int) -> dict | None:
    """
    Assemble authoritative realtime player state from correct sources.

    Source of truth map:
    - energy:         energy:v2:{user_id} (Redis)
    - coins:          coins_hot:{user_id} (Redis) with explicit DB fallback
    - boosts/cooldowns: extra_data from DB (NOT user:cache, to avoid stale boost state)
    - static profile: user:cache:{user_id} (Redis) with DB fallback for username, levels, etc.

    Returns None if user does not exist.
    """
    redis_conn = await get_redis_or_none()

    # 1. Get static profile from cache
    profile = None
    if redis_conn:
        cached = await redis_conn.get(f"{USER_CACHE_PREFIX}{user_id}")
        if cached:
            try:
                profile = json.loads(cached)
            except Exception:
                pass

    # 2. Fallback to DB if cache miss
    if profile is None:
        profile = await get_user(user_id)
        if profile is None:
            return None

    # 3. Compute levels and max_energy BEFORE reading energy
    # Main frontend progression level follows tap progression.
    # Keep it aligned with multitap_level to avoid UI/tap desync.
    level = int(profile.get("multitap_level", 0))
    energy_level = int(profile.get("energy_level", 0))
    multitap_level = int(profile.get("multitap_level", 0))
    profit_level = int(profile.get("profit_level", 0))
    rebirth_count = int(profile.get("rebirth_count", 0))
    max_energy = get_max_energy(energy_level)

    # 4. Read authoritative energy from energy:v2
    energy_state = await read_energy_v2(user_id, max_energy)

    # 5. Check if we need DB fallback for coins or fresh extra_data
    hot_coins = None
    if redis_conn:
        hot_coins = await redis_conn.get(f"coins_hot:{user_id}")

    # Fetch DB user AT MOST ONCE for fallback/freshness
    needs_db = hot_coins is None
    db_user = await get_user(user_id) if needs_db else None

    # 6. Determine coins (Redis hot -> DB -> cache fallback)
    if hot_coins is not None:
        coins = int(hot_coins)
    elif db_user:
        coins = int(db_user.get("coins", 0))
    else:
        coins = int(profile.get("coins", 0))

    # 7. Determine extra_data (prefer DB for freshness, fallback to cache)
    extra = {}
    raw_extra = None
    if db_user and db_user.get("extra_data"):
        raw_extra = db_user.get("extra_data")
    elif profile.get("extra_data"):
        raw_extra = profile.get("extra_data")

    if raw_extra:
        if isinstance(raw_extra, str):
            try:
                extra = json.loads(raw_extra)
            except Exception:
                extra = {}
        elif isinstance(raw_extra, dict):
            extra = raw_extra

    owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
    selected_skin = normalize_selected_skin(
        extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
    )

    # 8. Derive ALL boost states from expires_at (NOT from active: flags)
    boosts = get_all_boost_states(extra)

    # 9. TON wallet
    ton_wallet = get_ton_wallet_from_user({"extra_data": extra})

    # 10. Compute derived values
    tap_value = get_tap_value_with_rebirth(multitap_level, rebirth_count)
    profit_per_hour = get_hour_value(profit_level)

    # 11. State ordering fields (timestamp-based)
    state_updated_at = int(time.time() * 1000)  # milliseconds

    return {
        # Static profile
        "user_id": int(profile.get("user_id", user_id)),
        "username": profile.get("username"),
        "level": level,
        "multitap_level": multitap_level,
        "profit_level": profit_level,
        "energy_level": energy_level,
        "rebirth_count": rebirth_count,
        "referral_count": int(profile.get("referral_count", 0)),
        "referral_earnings": int(profile.get("referral_earnings", 0)),
        # Authoritative hot state
        "coins": coins,
        "energy": energy_state["energy"],
        "max_energy": energy_state["max_energy"],
        "regen_seconds": ENERGY_REGEN_SECONDS,
        # Derived values
        "profit_per_tap": tap_value,
        "profit_per_hour": profit_per_hour,
        # Boost states (derived from expires_at, read from DB extra_data)
        **boosts,
        # Skins
        "owned_skins": owned_skins,
        "selected_skin": selected_skin,
        # TON wallet
        "ton_wallet": ton_wallet,
        # Skin ad progress
        "skin_ad_progress": extra.get("skin_ad_progress", 0),
        "skin_ad_last_watch": extra.get("skin_ad_last_watch"),
        "ads_watched": extra.get("ads_watched", 0),
        # Cooldowns
        "mega_boost_cooldown_until": extra.get("mega_boost_cooldown_until"),
        "autoclicker_cooldown_until": extra.get("autoclicker_cooldown_until"),
        "energy_refill_cooldown_until": extra.get("energy_refill_cooldown_until"),
        # Ordering (both for compatibility)
        "state_updated_at": state_updated_at,
        "state_version": state_updated_at,
        "server_time": datetime.utcnow().isoformat(),
    }


async def build_click_response_state(
    user_id: int,
    coins_after: int,
    energy_after: int,
    max_energy: int,
    gained: int,
    effective_clicks: int,
    coin_per_tap: int,
    tap_value: int,
    profit_per_hour: int,
    boosts: dict,
    suspicion_score: int,
    referral_bonus: int,
) -> dict:
    """
    Build authoritative click response from post-click hot state.

    This is the response sent back to the frontend after processing clicks.
    All values come from authoritative sources, not stale cache.
    """
    state_updated_at = int(time.time() * 1000)

    return {
        "success": True,
        "coins": coins_after,
        "energy": energy_after,
        "max_energy": max_energy,
        "regen_seconds": ENERGY_REGEN_SECONDS,
        "server_time": datetime.utcnow().isoformat(),
        "gained": gained,
        "effective_clicks": effective_clicks,
        "coin_per_tap": coin_per_tap,
        "profit_per_tap": tap_value,
        "profit_per_hour": profit_per_hour,
        # Ordering (both for compatibility)
        "state_updated_at": state_updated_at,
        "state_version": state_updated_at,
        **boosts,
        "click_guard_suspicion_score": suspicion_score,
        "referral_bonus_paid": referral_bonus,
    }
