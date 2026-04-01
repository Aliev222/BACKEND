import logging
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from services.tournament_service import (
    upsert_tournament_score,
    get_weekly_leaderboard,
    get_player_tournament_entry,
)
from DATABASE.base import User

router = APIRouter(prefix="/api/v2", tags=["tournament"])
logger = logging.getLogger(__name__)


@router.get("/tournament/weekly")
async def get_weekly_tournament(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        player_entry = await get_player_tournament_entry(session, user_id)
        leaderboard = await get_weekly_leaderboard(session, limit=50)

    return {
        "success": True,
        "player": player_entry,
        "leaderboard": leaderboard,
    }


@router.get("/tournament/weekly/league/{league}")
async def get_league_leaderboard(league: str, request: Request):
    await require_telegram_user(request)

    if league not in ("diamond", "gold", "silver", "bronze"):
        raise HTTPException(status_code=400, detail="Invalid league")

    async with AsyncSessionLocal() as session:
        leaderboard = await get_weekly_leaderboard(session, league=league, limit=50)

    return {"success": True, "league": league, "leaderboard": leaderboard}


@router.post("/tournament/score")
async def submit_tournament_score(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    gained = int(body.get("gained", 0))

    if gained <= 0:
        return {"success": True, "score": 0}

    async with AsyncSessionLocal() as session:
        user_result = await session.execute(select(User).where(User.user_id == user_id))
        user_row = user_result.scalar_one_or_none()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        display_level = int(user_row.level or 0)
        username = user_row.username

        result = await upsert_tournament_score(
            session, user_id, username, display_level, gained
        )
        await session.commit()

    return {"success": True, "score": result["score"] if result else 0}
