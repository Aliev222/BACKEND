import time
import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from infrastructure.redis import get_redis, get_redis_or_none
from repositories.user_repo import get_user_by_id
from services.click_service import process_clicks
from core.telegram_auth import verify_telegram_init_data
from core.game_config import MAX_CLICK_BATCH_SIZE

router = APIRouter(prefix="/api/v2", tags=["clicks"])
logger = logging.getLogger(__name__)


@router.post("/clicks")
async def submit_clicks(request: Request):
    redis_conn = await get_redis()

    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    user_id = int(telegram_user.get("id", 0))
    if user_id <= 0:
        raise HTTPException(status_code=401, detail="Invalid user")

    body = await request.json()
    clicks = int(body.get("clicks", 0))
    batch_id = body.get("batch_id", "")

    if clicks <= 0 or clicks > MAX_CLICK_BATCH_SIZE:
        raise HTTPException(
            status_code=400, detail=f"Clicks must be 1-{MAX_CLICK_BATCH_SIZE}"
        )

    if not batch_id:
        raise HTTPException(status_code=400, detail="batch_id is required")

    idempotency_key = f"idemp:click:{batch_id}"
    acquired = await redis_conn.set(idempotency_key, "1", ex=120, nx=True)
    if not acquired:
        raise HTTPException(status_code=409, detail="Batch already processed")

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    result = await process_clicks(
        redis_conn=redis_conn,
        user_id=user_id,
        user=user,
        requested_clicks=clicks,
        batch_id=batch_id,
    )

    return {
        "success": True,
        "accepted": result["accepted"],
        "coins_earned": result["coins_earned"],
        "energy_remaining": result["energy_remaining"],
        "coin_per_tap": result["coin_per_tap"],
    }


@router.post("/energy/sync")
async def sync_energy(request: Request):
    redis_conn = await get_redis()

    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    user_id = int(telegram_user.get("id", 0))
    if user_id <= 0:
        raise HTTPException(status_code=401, detail="Invalid user")

    from core.game_logic import calculate_current_energy, resolve_max_energy

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    now = datetime.utcnow()
    current_energy = calculate_current_energy(user, now)
    max_energy = resolve_max_energy(user)

    from infrastructure.queue import buffer_energy

    await buffer_energy(redis_conn, user_id, current_energy, now.timestamp())

    return {
        "success": True,
        "energy": current_energy,
        "max_energy": max_energy,
        "server_time": now.isoformat(),
    }
