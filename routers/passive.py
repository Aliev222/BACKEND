"""
Passive income and autoclicker routes extracted from legacy.py (Patch 7.3).
"""
import time
import json
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Request, HTTPException

from core.config import AUTOCLICKER_COOLDOWN_MINUTES
from core.utils import parse_extra_data
from core.game_logic import normalize_dt, get_profit_per_hour, resolve_progression_level
from core.tasks import get_active_video_task_boost
from schemas import PassiveIncomeRequest, AdActionClaimRequest
from routers.legacy import (
    require_telegram_user,
    require_dual_rate_limit,
    require_user_action_lock,
    get_user,
    get_user_cached,
    update_user,
    update_user_if_matches,
    invalidate_user_cache,
    consume_ad_action_session,
    record_rewarded_ad_claim,
)
from infrastructure.redis import get_redis_or_none

router = APIRouter(tags=["passive"])
logger = logging.getLogger(__name__)


@router.post("/api/passive-income")
async def passive_income(payload: PassiveIncomeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "passive_income", request, payload.user_id, 20, 60, ip_limit=40
        )
        await require_user_action_lock("passive_income", payload.user_id, ttl=5)

        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        last_income = normalize_dt(user.get("last_passive_income"))
        now = datetime.utcnow()

        if not last_income:
            initialized_user = await update_user_if_matches(
                payload.user_id,
                {"last_passive_income": None},
                {"last_passive_income": now},
            )
            if initialized_user is None:
                raise HTTPException(
                    status_code=409, detail="Passive income baseline changed, retry"
                )

            await invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "coins": int(user.get("coins", 0)),
                "income": 0,
                "message": "",
                "state_updated_at": int(time.time() * 1000),
            }

        elapsed_seconds = max(0.0, (now - last_income).total_seconds())
        elapsed_seconds = min(elapsed_seconds, 24 * 3600)

        extra = parse_extra_data(user.get("extra_data"))

        passive_boost_active, _, passive_boost_multiplier = get_active_video_task_boost(
            extra, "passive_boost"
        )

        # Use stored hourly value as source of truth (rebirth keeps passive income).
        # Fallback to computed value only if DB value is empty/invalid.
        base_hour_value = int(user.get("profit_per_hour", 0) or 0)
        if base_hour_value <= 0:
            base_hour_value = int(get_profit_per_hour(resolve_progression_level(user)))

        hour_value = (
            base_hour_value * max(1, passive_boost_multiplier)
            if passive_boost_active
            else base_hour_value
        )

        if hour_value <= 0 or elapsed_seconds <= 0:
            return {
                "success": True,
                "coins": int(user.get("coins", 0)),
                "income": 0,
                "message": "",
                "state_updated_at": int(time.time() * 1000),
            }

        total_income = int((hour_value * elapsed_seconds) // 3600)
        if total_income <= 0:
            return {
                "success": True,
                "coins": int(user.get("coins", 0)),
                "income": 0,
                "message": "",
                "state_updated_at": int(time.time() * 1000),
            }

        consumed_seconds = (total_income * 3600) / hour_value
        new_last_income = min(now, last_income + timedelta(seconds=consumed_seconds))
        current_coins = int(user.get("coins", 0))
        new_coins = current_coins + total_income

        updated_user = await update_user_if_matches(
            payload.user_id,
            {
                "coins": current_coins,
                "last_passive_income": last_income,
            },
            {
                "coins": new_coins,
                "last_passive_income": new_last_income,
            },
        )
        if not updated_user:
            logger.warning(
                "Atomic passive-income update conflict for user=%s",
                payload.user_id,
            )
            raise HTTPException(
                status_code=409, detail="Passive income state changed, retry"
            )

        # CRITICAL: sync hot balance after DB increment
        from infrastructure.coins_hot_sync import (
            get_hot_authoritative_coins,
            sync_hot_after_db_increment,
        )

        await sync_hot_after_db_increment(payload.user_id, total_income, new_coins)

        # Referral bonus (5% of income, buffered in Redis)
        referral_bonus = 0
        referrer_id = updated_user.get("referrer_id")
        if referrer_id:
            referral_bonus = max(1, int(total_income * 0.05))
            redis_conn = await get_redis_or_none()
            if redis_conn:
                await redis_conn.hincrby(
                    f"referral_pending:{referrer_id}", "coins", referral_bonus
                )
                await redis_conn.expire(f"referral_pending:{referrer_id}", 300)
                await redis_conn.zadd(
                    "referral_pending_queue",
                    {str(referrer_id): int(time.time())},
                )

        await invalidate_user_cache(payload.user_id)

        # Return hot authoritative coins, not stale DB coins
        hot_coins = await get_hot_authoritative_coins(payload.user_id, new_coins)

        return {
            "success": True,
            "coins": hot_coins,
            "income": total_income,
            "referral_bonus_paid": referral_bonus,
            "message": f"+{total_income} passive income",
            "state_updated_at": int(time.time() * 1000),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/autoclicker/activate")
async def activate_autoclicker(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "activate_autoclicker", request, payload.user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "autoclicker"
        )
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        now = datetime.utcnow()
        cooldown_until = parse_extra_data(extra).get("autoclicker_cooldown_until")
        from routers.legacy import parse_iso_datetime

        cooldown_until = parse_iso_datetime(extra.get("autoclicker_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Autoclicker cooldown {remaining // 60}:{remaining % 60:02d}",
            )

        cooldown_until_value = (
            now + timedelta(minutes=AUTOCLICKER_COOLDOWN_MINUTES)
        ).isoformat()

        # Use atomic JSONB update
        from infrastructure.jsonb_helpers import jsonb_set_field
        from DATABASE.base import AsyncSessionLocal

        async with AsyncSessionLocal() as session:
            await jsonb_set_field(
                session,
                payload.user_id,
                "autoclicker_cooldown_until",
                cooldown_until_value,
            )
            await session.commit()
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(
            payload.user_id, "autoclicker", {"source_action": "autoclicker"}
        )
        return {
            "success": True,
            "duration_seconds": 60,
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": AUTOCLICKER_COOLDOWN_MINUTES,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_autoclicker: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
