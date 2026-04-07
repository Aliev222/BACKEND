import logging
import httpx
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from DATABASE.base import User
from repositories.user_repo import get_user_by_id, update_user_atomic
from core.stars_skins import get_stars_skin_price
from core.config import BOT_TOKEN

logger = logging.getLogger(__name__)

SKIN_MULTIPLIERS = {
    "default.pngSP": 1.0,
    "10lvl.pngSP": 1.2,
    "25lvl.pngSP": 1.2,
    "50lvl.pngSP": 1.2,
    "75lvl.pngSP": 1.2,
    "100lvl.pngSP": 1.2,
    "video.pngSP": 1.5,
    "video2.pngSP": 1.5,
    "video3.pngSP": 1.5,
    "video4.pngSP": 1.5,
    "video5.pngSP": 1.5,
    "video6.pngSP": 1.5,
    "video7.pngSP": 1.5,
    "video8.pngSP": 1.5,
    "refferal.pngSP": 1.8,
    "retro.pngSP": 1.7,
    "insta.pngSP": 1.5,
    "tiktok.pngSP": 1.5,
    "telega.pngSP": 1.5,
    "stars1.pngSP": 2.0,
    "stars2.pngSP": 2.0,
    "stars3.pngSP": 2.0,
    "stars4.pngSP": 2.0,
    "stars5.pngSP": 2.0,
    "stars6.pngSP": 2.0,
    "stars7.pngSP": 2.0,
    "stars8.pngSP": 2.0,
}

LEVEL_SKINS = {
    10: "10lvl.pngSP",
    25: "25lvl.pngSP",
    50: "50lvl.pngSP",
    75: "75lvl.pngSP",
    100: "100lvl.pngSP",
}


def _get_extra(user: dict) -> dict:
    extra = user.get("extra_data", {})
    if isinstance(extra, str):
        import json

        try:
            return json.loads(extra)
        except Exception:
            return {}
    return extra if isinstance(extra, dict) else {}


async def get_owned_skins(user: dict) -> list[str]:
    extra = _get_extra(user)
    owned = extra.get("owned_skins", ["default.pngSP"])
    if not isinstance(owned, list):
        owned = ["default.pngSP"]
    return owned


async def select_skin(
    session: AsyncSession,
    user_id: int,
    user: dict,
    skin_id: str,
) -> dict:
    owned = await get_owned_skins(user)

    if skin_id not in owned:
        raise HTTPException(status_code=400, detail="Skin not owned")

    # Use atomic JSONB update (no optimistic locking)
    from sqlalchemy import text
    import json

    await session.execute(
        text("""
            UPDATE users 
            SET extra_data = jsonb_set(
                COALESCE(extra_data, '{}'::jsonb),
                '{selected_skin}',
                :skin::jsonb,
                true
            )
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "skin": json.dumps(skin_id),
        },
    )
    await session.commit()

    # Cache invalidation
    from infrastructure.cache_invalidation import invalidate_user_cache

    await invalidate_user_cache(user_id)

    return {
        "success": True,
        "selected_skin": skin_id,
        "multiplier": SKIN_MULTIPLIERS.get(skin_id, 1.0),
    }


async def create_stars_invoice(
    user_id: int,
    skin_id: str,
) -> str:
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="Bot token not configured")

    price = get_stars_skin_price(skin_id)
    payload = f"stars_skin:{user_id}:{skin_id}"

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createInvoiceLink",
            json={
                "title": f"Skin {skin_id}",
                "description": f"Unlock premium skin {skin_id}",
                "payload": payload,
                "currency": "XTR",
                "prices": [{"label": skin_id, "amount": price}],
                "provider_token": "",
            },
        )

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="Invoice creation failed")

    data = response.json()
    if not data.get("ok") or not data.get("result"):
        raise HTTPException(status_code=502, detail="Invoice creation failed")

    return data["result"]


async def unlock_skin_by_level(
    session: AsyncSession,
    user_id: int,
    user: dict,
    level: int,
) -> dict:
    extra = _get_extra(user)
    owned = await get_owned_skins(user)

    unlocked = []
    for req_level, skin_id in LEVEL_SKINS.items():
        if level >= req_level and skin_id not in owned:
            owned.append(skin_id)
            unlocked.append(skin_id)

    if not unlocked:
        return {"success": True, "unlocked": []}

    # Use atomic JSONB update
    from infrastructure.jsonb_helpers import jsonb_set_field

    await jsonb_set_field(session, user_id, "owned_skins", owned)
    await session.commit()

    # Cache invalidation
    from infrastructure.cache_invalidation import invalidate_user_cache

    await invalidate_user_cache(user_id)

    return {"success": True, "unlocked": unlocked}
