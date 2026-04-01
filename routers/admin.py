import logging
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select, update, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from DATABASE.base import User, WeeklyTournamentWinner, WeeklyTournamentTonPayout

router = APIRouter(prefix="/api/v2/admin", tags=["admin"])
logger = logging.getLogger(__name__)


def _check_admin_token(request: Request):
    from core.config import ADMIN_DASHBOARD_TOKEN, ADMIN_TELEGRAM_IDS

    token = request.headers.get("X-Admin-Token", "")
    if ADMIN_DASHBOARD_TOKEN and token != ADMIN_DASHBOARD_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid admin token")

    telegram_user = request.headers.get("X-Telegram-Init-Data", "")
    if telegram_user:
        from core.telegram_auth import verify_telegram_init_data

        try:
            user = verify_telegram_init_data(telegram_user)
            uid = int(user.get("id", 0))
            if uid in ADMIN_TELEGRAM_IDS:
                return uid
        except Exception:
            pass

    if not ADMIN_DASHBOARD_TOKEN:
        raise HTTPException(status_code=403, detail="Admin not configured")
    return 0


@router.get("/dashboard")
async def admin_dashboard(request: Request):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        total_users = await session.execute(select(func.count()).select_from(User))
        total_coins = await session.execute(
            select(func.sum(User.coins)).select_from(User)
        )
        total_referrals = await session.execute(
            select(func.count()).select_from(User).where(User.referrer_id.isnot(None))
        )

    return {
        "success": True,
        "total_users": total_users.scalar() or 0,
        "total_coins": total_coins.scalar() or 0,
        "total_referrals": total_referrals.scalar() or 0,
    }


@router.get("/players")
async def admin_players(request: Request, limit: int = 50, offset: int = 0):
    _check_admin_token(request)
    limit = min(limit, 200)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(
                User.user_id,
                User.username,
                User.coins,
                User.level,
                User.referral_count,
                User.created_at,
            )
            .order_by(desc(User.coins))
            .limit(limit)
            .offset(offset)
        )
        players = result.fetchall()

    return {
        "success": True,
        "players": [
            {
                "user_id": p.user_id,
                "username": p.username,
                "coins": p.coins,
                "level": p.level,
                "referrals": p.referral_count,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in players
        ],
        "limit": limit,
        "offset": offset,
    }


@router.get("/players/{user_id}")
async def admin_player_detail(user_id: int, request: Request):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        user_result = await session.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        referrals_result = await session.execute(
            select(func.count()).select_from(User).where(User.referrer_id == user_id)
        )

    extra = {}
    if user.extra_data:
        try:
            import json

            extra = json.loads(user.extra_data)
        except Exception:
            extra = {}

    return {
        "success": True,
        "user": {
            "user_id": user.user_id,
            "username": user.username,
            "coins": user.coins,
            "level": user.level,
            "multitap_level": user.multitap_level,
            "profit_level": user.profit_level,
            "energy_level": user.energy_level,
            "profit_per_hour": user.profit_per_hour,
            "profit_per_tap": user.profit_per_tap,
            "energy": user.energy,
            "max_energy": user.max_energy,
            "referrer_id": user.referrer_id,
            "referral_count": user.referral_count,
            "referral_earnings": user.referral_earnings,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "extra_data": extra,
        },
        "direct_referrals": referrals_result.scalar() or 0,
    }


@router.post("/players/{user_id}/ban")
async def admin_ban_player(user_id: int, request: Request):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(User).where(User.user_id == user_id).values(fraud_flag=True)
        )
        await session.commit()

    return {"success": True, "user_id": user_id, "action": "banned"}


@router.post("/players/{user_id}/unban")
async def admin_unban_player(user_id: int, request: Request):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(User).where(User.user_id == user_id).values(fraud_flag=False)
        )
        await session.commit()

    return {"success": True, "user_id": user_id, "action": "unbanned"}


@router.get("/tournaments")
async def admin_tournaments(request: Request):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        from DATABASE.base import WeeklyTournamentSeason

        result = await session.execute(
            select(WeeklyTournamentSeason)
            .order_by(desc(WeeklyTournamentSeason.starts_at))
            .limit(10)
        )
        seasons = result.scalars().all()

    return {
        "success": True,
        "seasons": [
            {
                "season_key": s.season_key,
                "status": s.status,
                "starts_at": s.starts_at.isoformat() if s.starts_at else None,
                "ends_at": s.ends_at.isoformat() if s.ends_at else None,
                "payout_fund_cents": s.payout_fund_cents,
                "settled_at": s.settled_at.isoformat() if s.settled_at else None,
            }
            for s in seasons
        ],
    }


@router.get("/tournaments/{season_key}/winners")
async def admin_tournament_winners(season_key: str, request: Request):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WeeklyTournamentWinner)
            .where(WeeklyTournamentWinner.season_key == season_key)
            .order_by(WeeklyTournamentWinner.league, WeeklyTournamentWinner.rank)
        )
        winners = result.scalars().all()

    return {
        "success": True,
        "season_key": season_key,
        "winners": [
            {
                "user_id": w.user_id,
                "username": w.username,
                "league": w.league,
                "rank": w.rank,
                "score": w.score,
                "payout_cents": w.payout_cents,
                "eligible": w.eligible_for_payout,
                "fraud": w.fraud_flag,
            }
            for w in winners
        ],
    }


@router.get("/ton/payouts")
async def admin_ton_payouts(request: Request, season_key: str = ""):
    _check_admin_token(request)

    async with AsyncSessionLocal() as session:
        query = select(WeeklyTournamentTonPayout)
        if season_key:
            query = query.where(WeeklyTournamentTonPayout.season_key == season_key)
        result = await session.execute(
            query.order_by(desc(WeeklyTournamentTonPayout.created_at)).limit(100)
        )
        payouts = result.scalars().all()

    return {
        "success": True,
        "payouts": [
            {
                "user_id": p.user_id,
                "username": p.username,
                "league": p.league,
                "rank": p.rank,
                "wallet": p.wallet_address,
                "payout_cents": p.payout_cents,
                "ton_amount_nano": p.ton_amount_nano,
                "status": p.status,
                "tx_hash": p.tx_hash,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in payouts
        ],
    }
