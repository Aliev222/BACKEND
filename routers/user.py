import logging
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException

from infrastructure.database import AsyncSessionLocal
from infrastructure.redis import get_redis
from repositories.user_repo import get_user_by_id
from routers.auth import require_telegram_user
from core.game_logic import (
    calculate_current_energy,
    resolve_max_energy,
    get_tap_value,
    get_hour_value,
)

router = APIRouter(prefix="/api/v2", tags=["user"])
logger = logging.getLogger(__name__)


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
    multitap_level = int(user.get("multitap_level", 0))
    profit_level = int(user.get("profit_level", 0))
    energy_level = int(user.get("energy_level", 0))

    return {
        "user_id": user["user_id"],
        "username": user.get("username"),
        "coins": user.get("coins", 0),
        "energy": current_energy,
        "max_energy": max_energy,
        "profit_per_tap": get_tap_value(multitap_level),
        "profit_per_hour": get_hour_value(profit_level),
        "multitap_level": multitap_level,
        "profit_level": profit_level,
        "energy_level": energy_level,
        "level": user.get("level", 0),
        "server_time": now.isoformat(),
    }
