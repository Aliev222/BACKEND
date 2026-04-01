import logging
from fastapi import APIRouter, Request, HTTPException

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from repositories.user_repo import get_user_by_id
from services.skin_service import (
    get_owned_skins,
    select_skin,
    create_stars_invoice,
    unlock_skin_by_level,
    SKIN_MULTIPLIERS,
)

router = APIRouter(prefix="/api/v2", tags=["skins"])
logger = logging.getLogger(__name__)


@router.get("/skins")
async def get_skins(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        owned = await get_owned_skins(user)
        selected = user.get("extra_data", {}).get("selected_skin", "default.pngSP")
        if isinstance(user.get("extra_data"), str):
            import json

            try:
                extra = json.loads(user["extra_data"])
                selected = extra.get("selected_skin", "default.pngSP")
            except Exception:
                selected = "default.pngSP"

    skins = []
    for skin_id, multiplier in SKIN_MULTIPLIERS.items():
        skins.append(
            {
                "skin_id": skin_id,
                "multiplier": multiplier,
                "owned": skin_id in owned,
                "selected": skin_id == selected,
            }
        )

    return {"success": True, "skins": skins, "selected": selected}


@router.post("/skins/select")
async def select_skin_endpoint(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    skin_id = body.get("skin_id", "")

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        result = await select_skin(session, user_id, user, skin_id)
        await session.commit()

    return result


@router.post("/skins/stars-invoice")
async def create_invoice(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    skin_id = body.get("skin_id", "")

    invoice_url = await create_stars_invoice(user_id, skin_id)
    return {"success": True, "invoice_url": invoice_url}


@router.post("/skins/unlock-level")
async def unlock_level_skins(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        result = await unlock_skin_by_level(
            session, user_id, user, user.get("level", 0)
        )
        await session.commit()

    return result
