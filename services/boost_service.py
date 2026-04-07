import logging
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import HTTPException

from DATABASE.base import User
from repositories.user_repo import get_user_by_id, update_user_atomic
from core.game_logic import calculate_current_energy, resolve_max_energy

logger = logging.getLogger(__name__)

MEGA_BOOST_MINUTES = 1
MEGA_BOOST_COOLDOWN_MINUTES = 10
GHOST_BOOST_MULTIPLIER = 5
GHOST_BOOST_MINUTES = 1
AUTOCLICKER_COOLDOWN_MINUTES = 10
ENERGY_REFILL_COOLDOWN_MINUTES = 10


def _get_extra(user: dict) -> dict:
    extra = user.get("extra_data", {})
    if isinstance(extra, str):
        import json

        try:
            return json.loads(extra)
        except Exception:
            return {}
    return extra if isinstance(extra, dict) else {}


def _is_boost_active(extra: dict, key: str, now: datetime) -> bool:
    boosts = extra.get("active_boosts", {})
    if not isinstance(boosts, dict):
        return False
    boost = boosts.get(key)
    if not isinstance(boost, dict):
        return False
    expires_at = boost.get("expires_at")
    if not expires_at:
        return False
    try:
        return datetime.fromisoformat(expires_at) > now
    except (ValueError, TypeError):
        return False


def _get_cooldown(extra: dict, key: str, now: datetime) -> datetime | None:
    value = extra.get(key)
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt > now else None
    except (ValueError, TypeError):
        return None


async def activate_mega_boost(
    session: AsyncSession,
    user_id: int,
    user: dict,
) -> dict:
    now = datetime.utcnow()
    extra = _get_extra(user)

    active_boosts = extra.get("active_boosts", {})
    if not isinstance(active_boosts, dict):
        active_boosts = {}

    if _is_boost_active(extra, "mega_boost", now):
        expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
        remaining = int((expires - now).total_seconds())
        return {
            "success": False,
            "already_active": True,
            "remaining_seconds": remaining,
        }

    cooldown_until = _get_cooldown(extra, "mega_boost_cooldown_until", now)
    if cooldown_until:
        raise HTTPException(status_code=429, detail="Mega boost cooldown active")

    expires_at = (now + timedelta(minutes=MEGA_BOOST_MINUTES)).isoformat()
    cooldown_until_value = (
        now + timedelta(minutes=MEGA_BOOST_COOLDOWN_MINUTES)
    ).isoformat()

    # Use nested path atomic JSONB updates to prevent boost merge race
    from sqlalchemy import text
    import json

    await session.execute(
        text("""
            UPDATE users 
            SET extra_data = jsonb_set(
                jsonb_set(
                    COALESCE(extra_data::jsonb, '{}'::jsonb),
                    '{active_boosts,mega_boost}',
                    CAST(:boost AS jsonb),
                    true
                ),
                '{mega_boost_cooldown_until}',
                CAST(:cooldown AS jsonb),
                true
            )::text
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "boost": json.dumps({"active": True, "expires_at": expires_at}),
            "cooldown": json.dumps(cooldown_until_value),
        },
    )
    await session.commit()

    # Cache invalidation
    from infrastructure.cache_invalidation import invalidate_user_cache

    await invalidate_user_cache(user_id)

    return {
        "success": True,
        "expires_at": expires_at,
        "cooldown_until": cooldown_until_value,
    }


async def activate_ghost_boost(
    session: AsyncSession,
    user_id: int,
    user: dict,
) -> dict:
    now = datetime.utcnow()
    extra = _get_extra(user)

    active_boosts = extra.get("active_boosts", {})
    if not isinstance(active_boosts, dict):
        active_boosts = {}

    if _is_boost_active(extra, "ghost_boost", now):
        expires = datetime.fromisoformat(active_boosts["ghost_boost"]["expires_at"])
        remaining = int((expires - now).total_seconds())
        return {
            "success": False,
            "already_active": True,
            "remaining_seconds": remaining,
        }

    expires_at = (now + timedelta(minutes=GHOST_BOOST_MINUTES)).isoformat()

    # Use nested path atomic JSONB updates to prevent boost merge race
    from sqlalchemy import text
    import json

    await session.execute(
        text("""
            UPDATE users 
            SET extra_data = jsonb_set(
                COALESCE(extra_data::jsonb, '{}'::jsonb),
                '{active_boosts,ghost_boost}',
                CAST(:boost AS jsonb),
                true
            )::text
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "boost": json.dumps(
                {
                    "active": True,
                    "expires_at": expires_at,
                    "multiplier": GHOST_BOOST_MULTIPLIER,
                }
            ),
        },
    )
    await session.commit()

    # Cache invalidation
    from infrastructure.cache_invalidation import invalidate_user_cache

    await invalidate_user_cache(user_id)

    return {
        "success": True,
        "expires_at": expires_at,
        "remaining_seconds": GHOST_BOOST_MINUTES * 60,
        "multiplier": GHOST_BOOST_MULTIPLIER,
    }


async def activate_autoclicker(
    session: AsyncSession,
    user_id: int,
    user: dict,
) -> dict:
    now = datetime.utcnow()
    extra = _get_extra(user)

    cooldown_until = _get_cooldown(extra, "autoclicker_cooldown_until", now)
    if cooldown_until:
        remaining = int((cooldown_until - now).total_seconds())
        raise HTTPException(
            status_code=429, detail=f"Autoclicker cooldown: {remaining}s"
        )

    cooldown_until_value = (
        now + timedelta(minutes=AUTOCLICKER_COOLDOWN_MINUTES)
    ).isoformat()

    # Use atomic JSONB update
    from infrastructure.jsonb_helpers import jsonb_set_field

    await jsonb_set_field(
        session, user_id, "autoclicker_cooldown_until", cooldown_until_value
    )
    await session.commit()

    # Cache invalidation
    from infrastructure.cache_invalidation import invalidate_user_cache

    await invalidate_user_cache(user_id)

    return {
        "success": True,
        "cooldown_until": cooldown_until_value,
    }


async def refill_energy(
    session: AsyncSession,
    user_id: int,
    user: dict,
) -> dict:
    now = datetime.utcnow()
    extra = _get_extra(user)

    cooldown_until = _get_cooldown(extra, "energy_refill_cooldown_until", now)
    if cooldown_until:
        remaining = int((cooldown_until - now).total_seconds())
        raise HTTPException(
            status_code=429, detail=f"Energy refill cooldown: {remaining}s"
        )

    max_energy = resolve_max_energy(user)
    cooldown_until_value = (
        now + timedelta(minutes=ENERGY_REFILL_COOLDOWN_MINUTES)
    ).isoformat()

    # Use atomic JSONB update + regular field updates
    from infrastructure.jsonb_helpers import jsonb_set_field
    from sqlalchemy import text

    await jsonb_set_field(
        session, user_id, "energy_refill_cooldown_until", cooldown_until_value
    )
    await session.execute(
        text("""
            UPDATE users 
            SET energy = :energy,
                last_energy_update = :last_update
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "energy": max_energy,
            "last_update": now,
        },
    )
    await session.commit()

    # Cache invalidation
    from infrastructure.cache_invalidation import invalidate_user_cache

    await invalidate_user_cache(user_id)

    return {
        "success": True,
        "energy": max_energy,
        "cooldown_until": cooldown_until_value,
    }
