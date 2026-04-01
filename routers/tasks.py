import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from DATABASE.base import User, UserTask
from repositories.user_repo import get_user_by_id, update_user_atomic

router = APIRouter(prefix="/api/v2", tags=["tasks"])
logger = logging.getLogger(__name__)

DAILY_REWARD_MAX_DAYS = 30
DAILY_REWARD_BASE_COINS = 500
DAILY_REWARD_INFINITE_ENERGY_MINUTES = 10
DAILY_REWARD_SKIN_ID = "retro.pngSP"

TASK_DEFINITIONS = [
    {
        "id": "join_channel",
        "title": "Join Telegram Channel",
        "reward": 5000,
        "type": "social",
    },
    {
        "id": "invite_3_friends",
        "title": "Invite 3 Friends",
        "reward": 15000,
        "type": "referral",
    },
    {
        "id": "reach_level_10",
        "title": "Reach Level 10",
        "reward": 10000,
        "type": "progress",
    },
]


@router.get("/tasks")
async def get_tasks(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserTask.task_id).where(UserTask.user_id == user_id)
        )
        completed = set(result.scalars().all())

    tasks = []
    for task in TASK_DEFINITIONS:
        tasks.append(
            {
                "id": task["id"],
                "title": task["title"],
                "reward": task["reward"],
                "type": task["type"],
                "completed": task["id"] in completed,
            }
        )

    return {"success": True, "tasks": tasks}


@router.post("/tasks/complete")
async def complete_task(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    task_id = body.get("task_id", "")

    task_def = next((t for t in TASK_DEFINITIONS if t["id"] == task_id), None)
    if not task_def:
        raise HTTPException(status_code=400, detail="Unknown task")

    async with AsyncSessionLocal() as session:
        existing = await session.execute(
            select(UserTask).where(
                UserTask.user_id == user_id, UserTask.task_id == task_id
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Task already completed")

        session.add(UserTask(user_id=user_id, task_id=task_id))

        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        new_coins = int(user.get("coins", 0)) + task_def["reward"]
        await session.execute(
            select(User).where(User.user_id == user_id).with_for_update()
        )
        await session.execute(
            User.__table__.update()
            .where(User.user_id == user_id)
            .values(coins=new_coins)
        )
        await session.commit()

    return {"success": True, "coins": new_coins, "reward": task_def["reward"]}


@router.get("/daily-reward")
async def get_daily_reward_status(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    extra = user.get("extra_data", {})
    if isinstance(extra, str):
        import json

        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    claimed_days = extra.get("daily_reward_claimed_days", [])
    last_claim = extra.get("daily_reward_last_claim_date")

    today = datetime.utcnow().date().isoformat()
    already_claimed_today = last_claim == today

    next_day = len(claimed_days) + 1 if not already_claimed_today else len(claimed_days)
    next_day = min(next_day, DAILY_REWARD_MAX_DAYS)

    return {
        "success": True,
        "claimed_days": claimed_days,
        "last_claim_date": last_claim,
        "already_claimed_today": already_claimed_today,
        "next_day": next_day,
        "next_reward": next_day * DAILY_REWARD_BASE_COINS,
        "max_days": DAILY_REWARD_MAX_DAYS,
    }


@router.post("/daily-reward/claim")
async def claim_daily_reward(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user_row = await session.execute(
            select(User).where(User.user_id == user_id).with_for_update()
        )
        user_db = user_row.scalar_one_or_none()
        if not user_db:
            raise HTTPException(status_code=404, detail="User not found")

        import json

        extra = {}
        if user_db.extra_data:
            try:
                extra = json.loads(user_db.extra_data)
            except Exception:
                extra = {}

        claimed_days = extra.get("daily_reward_claimed_days", [])
        last_claim = extra.get("daily_reward_last_claim_date")
        today = datetime.utcnow().date().isoformat()

        if last_claim == today:
            raise HTTPException(status_code=400, detail="Already claimed today")

        if len(claimed_days) >= DAILY_REWARD_MAX_DAYS:
            raise HTTPException(status_code=400, detail="All daily rewards claimed")

        day_number = len(claimed_days) + 1
        reward = day_number * DAILY_REWARD_BASE_COINS

        claimed_days.append(day_number)
        extra["daily_reward_claimed_days"] = claimed_days
        extra["daily_reward_last_claim_date"] = today

        if day_number % 7 == 0:
            expires_at = (
                datetime.utcnow()
                + timedelta(minutes=DAILY_REWARD_INFINITE_ENERGY_MINUTES)
            ).isoformat()
            active_boosts = extra.get("active_boosts", {})
            if not isinstance(active_boosts, dict):
                active_boosts = {}
            active_boosts["daily_infinite_energy"] = {
                "active": True,
                "expires_at": expires_at,
            }
            extra["active_boosts"] = active_boosts

        if day_number == DAILY_REWARD_MAX_DAYS:
            owned_skins = extra.get("owned_skins", ["default.pngSP"])
            if not isinstance(owned_skins, list):
                owned_skins = ["default.pngSP"]
            if DAILY_REWARD_SKIN_ID not in owned_skins:
                owned_skins.append(DAILY_REWARD_SKIN_ID)
            extra["owned_skins"] = owned_skins

        user_db.coins += reward
        user_db.extra_data = json.dumps(extra)
        await session.commit()

    return {
        "success": True,
        "day": day_number,
        "reward": reward,
        "coins": user_db.coins,
        "infinite_energy": day_number % 7 == 0,
        "skin_unlocked": day_number == DAILY_REWARD_MAX_DAYS,
    }
