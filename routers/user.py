import json
import logging
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException

from infrastructure.database import AsyncSessionLocal
from infrastructure.redis import get_redis
from repositories.user_repo import get_user_by_id, create_user
from routers.auth import require_telegram_user
from core.game_logic import (
    calculate_current_energy,
    resolve_max_energy,
    get_tap_value_with_rebirth,
    get_hour_value,
)
from core.game_config import ENERGY_REGEN_SECONDS

DEFAULT_SKIN_ID = "default.pngSP"

router = APIRouter(prefix="/api/v2", tags=["user"])
logger = logging.getLogger(__name__)


def _parse_extra(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _get_owned_skins(extra: dict) -> list:
    owned = extra.get("owned_skins", [DEFAULT_SKIN_ID])
    if not isinstance(owned, list):
        owned = [DEFAULT_SKIN_ID]
    return owned


def _get_selected_skin(extra: dict, owned: list) -> str:
    selected = extra.get("selected_skin", DEFAULT_SKIN_ID)
    if selected not in owned:
        selected = DEFAULT_SKIN_ID
    return selected


def _get_boost_status(extra: dict, key: str):
    boosts = extra.get("active_boosts", {})
    if not isinstance(boosts, dict):
        return False, None
    boost = boosts.get(key)
    if not isinstance(boost, dict):
        return False, None
    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None
    try:
        active = datetime.fromisoformat(expires_at) > datetime.utcnow()
        return active, expires_at
    except (ValueError, TypeError):
        return False, None


def _get_video_task_boost(extra: dict, boost_type: str):
    boosts = extra.get("active_boosts", {})
    if not isinstance(boosts, dict):
        return False, None, 1.0
    boost = boosts.get(boost_type)
    if not isinstance(boost, dict):
        return False, None, 1.0
    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None, 1.0
    try:
        active = datetime.fromisoformat(expires_at) > datetime.utcnow()
        multiplier = float(boost.get("multiplier", 1.0))
        return active, expires_at, multiplier
    except (ValueError, TypeError):
        return False, None, 1.0


def _get_ton_wallet(extra: dict) -> dict:
    wallet = extra.get("ton_wallet")
    if not wallet:
        return {}
    return {
        "address": wallet,
        "provider": extra.get("ton_wallet_provider", ""),
        "verified": extra.get("ton_wallet_verified", False),
        "connected_at": extra.get("ton_wallet_connected_at"),
    }


def _get_skin_ad_progress(extra: dict) -> dict:
    return extra.get("skin_ad_progress", {})


def _get_skin_ad_last_watch(extra: dict) -> str | None:
    return extra.get("skin_ad_last_watch")


@router.get("/user")
async def get_user_data(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    now = datetime.utcnow()
    current_energy = calculate_current_energy(user, now)
    max_energy = resolve_max_energy(user)

    extra = _parse_extra(user.get("extra_data", {}))
    owned_skins = _get_owned_skins(extra)
    selected_skin = _get_selected_skin(extra, owned_skins)

    ghost_active, ghost_expires = _get_boost_status(extra, "ghost_boost")
    daily_active, daily_expires = _get_boost_status(extra, "daily_infinite_energy")
    task_tap_active, task_tap_expires, task_tap_mult = _get_video_task_boost(
        extra, "tap_boost"
    )
    task_passive_active, task_passive_expires, task_passive_mult = (
        _get_video_task_boost(extra, "passive_boost")
    )

    multitap_level = int(user.get("multitap_level", 0))
    profit_level = int(user.get("profit_level", 0))
    energy_level = int(user.get("energy_level", 0))
    rebirth_count = max(0, int(user.get("rebirth_count", 0) or 0))

    return {
        "user_id": user["user_id"],
        "username": user.get("username"),
        "coins": user.get("coins", 0),
        "energy": current_energy,
        "max_energy": max_energy,
        "profit_per_tap": get_tap_value_with_rebirth(multitap_level, rebirth_count),
        "profit_per_hour": get_hour_value(profit_level),
        "multitap_level": multitap_level,
        "profit_level": profit_level,
        "energy_level": energy_level,
        "level": user.get("level", 0),
        "rebirth_count": rebirth_count,
        "owned_skins": owned_skins,
        "selected_skin": selected_skin,
        "ads_watched": extra.get("ads_watched", 0),
        "ghost_boost_active": ghost_active,
        "ghost_boost_expires_at": ghost_expires,
        "task_tap_boost_active": task_tap_active,
        "task_tap_boost_expires_at": task_tap_expires,
        "task_tap_boost_multiplier": task_tap_mult,
        "task_passive_boost_active": task_passive_active,
        "task_passive_boost_expires_at": task_passive_expires,
        "task_passive_boost_multiplier": task_passive_mult,
        "daily_infinite_energy_active": daily_active,
        "daily_infinite_energy_expires_at": daily_expires,
        "skin_ad_progress": _get_skin_ad_progress(extra),
        "skin_ad_last_watch": _get_skin_ad_last_watch(extra),
        "ton_wallet": _get_ton_wallet(extra),
        "regen_seconds": ENERGY_REGEN_SECONDS,
        "server_time": now.isoformat(),
    }


@router.post("/user")
async def register_user(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))
    username = telegram_user.get("username")

    body = await request.json()
    referrer_id = body.get("referrer_id")

    async with AsyncSessionLocal() as session:
        existing = await get_user_by_id(session, user_id)
        if existing:
            return {
                "user_id": existing["user_id"],
                "coins": existing.get("coins", 0),
                "energy": existing.get("energy", 500),
                "max_energy": existing.get("max_energy", 500),
                "profit_per_tap": existing.get("profit_per_tap", 1),
                "profit_per_hour": existing.get("profit_per_hour", 100),
                "multitap_level": existing.get("multitap_level", 0),
                "profit_level": existing.get("profit_level", 0),
                "energy_level": existing.get("energy_level", 0),
                "level": existing.get("level", 0),
                "rebirth_count": existing.get("rebirth_count", 0),
                "owned_skins": _get_owned_skins(
                    _parse_extra(existing.get("extra_data", {}))
                ),
                "selected_skin": _get_selected_skin(
                    _parse_extra(existing.get("extra_data", {})),
                    _get_owned_skins(_parse_extra(existing.get("extra_data", {}))),
                ),
                "ads_watched": _parse_extra(existing.get("extra_data", {})).get(
                    "ads_watched", 0
                ),
                "skin_ad_progress": _get_skin_ad_progress(
                    _parse_extra(existing.get("extra_data", {}))
                ),
                "ton_wallet": _get_ton_wallet(
                    _parse_extra(existing.get("extra_data", {}))
                ),
                "referrer_id": existing.get("referrer_id"),
                "referral_count": existing.get("referral_count", 0),
                "referral_earnings": existing.get("referral_earnings", 0),
            }

        user = await create_user(session, user_id, username, referrer_id)
        await session.commit()

        return {
            "user_id": user.user_id,
            "coins": user.coins,
            "energy": user.energy,
            "max_energy": user.max_energy,
            "profit_per_tap": user.profit_per_tap,
            "profit_per_hour": user.profit_per_hour,
            "multitap_level": user.multitap_level,
            "profit_level": user.profit_level,
            "energy_level": user.energy_level,
            "level": user.level,
            "rebirth_count": user.rebirth_count,
            "owned_skins": ["default.pngSP"],
            "selected_skin": "default.pngSP",
            "ads_watched": 0,
            "skin_ad_progress": {},
            "ton_wallet": {},
            "referrer_id": user.referrer_id,
            "referral_count": 0,
            "referral_earnings": 0,
        }
