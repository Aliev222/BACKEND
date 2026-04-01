import logging
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from services.economy_service import apply_global_upgrade
from core.game_config import GLOBAL_UPGRADE_PRICES, MAX_UPGRADE_LEVEL

router = APIRouter(prefix="/api/v2", tags=["economy"])
logger = logging.getLogger(__name__)


@router.post("/upgrade")
async def upgrade(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        result = await apply_global_upgrade(session, user_id)
        await session.commit()

    return result


@router.get("/upgrade-prices")
async def get_upgrade_prices(request: Request):
    await require_telegram_user(request)
    return {
        "global_prices": GLOBAL_UPGRADE_PRICES,
        "max_level": MAX_UPGRADE_LEVEL,
    }
