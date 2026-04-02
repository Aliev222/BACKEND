import logging
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from DATABASE.base import User

router = APIRouter(prefix="/api/v2", tags=["referrals"])
logger = logging.getLogger(__name__)

REFERRAL_BONUS_COINS = 25000
REFERRAL_SHARE_PERCENT = 0.05


@router.get("/referrals")
async def get_referrals(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user_result = await session.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        referrals_result = await session.execute(
            select(User.user_id, User.username, User.level, User.created_at)
            .where(User.referrer_id == user_id)
            .order_by(User.created_at.desc())
            .limit(50)
        )
        referrals = referrals_result.fetchall()

    referral_list = [
        {
            "user_id": r.user_id,
            "username": r.username,
            "level": r.level,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in referrals
    ]

    return {
        "success": True,
        "count": user.referral_count or 0,
        "earnings": user.referral_earnings or 0,
        "referrals": referral_list,
        "bonus_per_referral": REFERRAL_BONUS_COINS,
        "share_percent": REFERRAL_SHARE_PERCENT * 100,
    }


@router.get("/referrals/stats")
async def get_referral_stats(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user_result = await session.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        count_result = await session.execute(
            select(func.count()).select_from(User).where(User.referrer_id == user_id)
        )
        count = count_result.scalar() or 0

    return {
        "success": True,
        "total_referrals": count,
        "total_earnings": user.referral_earnings or 0,
        "bonus_per_referral": REFERRAL_BONUS_COINS,
        "share_percent": REFERRAL_SHARE_PERCENT * 100,
    }


# ─── Legacy referral route (moved from legacy.py, Patch 7.4) ─────────────────

router_legacy = APIRouter(tags=["referrals-legacy"])
logger_legacy = logging.getLogger(__name__)

from routers.legacy import (
    require_telegram_user as _require_telegram_user,
    get_user_cached as _get_user_cached,
)


@router_legacy.get("/api/referral-data/{user_id}")
async def get_referral_data_legacy(user_id: int, request: Request):
    """Get referral statistics"""
    try:
        await _require_telegram_user(request, user_id)
        user = await _get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "count": user.get("referral_count", 0),
            "earnings": user.get("referral_earnings", 0),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger_legacy.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
