import logging
from fastapi import APIRouter, Request, HTTPException

from infrastructure.database import AsyncSessionLocal
from infrastructure.redis import get_redis
from routers.auth import require_telegram_user
from repositories.user_repo import get_user_by_id
from services.ad_service import create_ad_session, consume_ad_session
from services.boost_service import (
    activate_mega_boost,
    activate_ghost_boost,
    activate_autoclicker,
    refill_energy,
)
from core.config import MONETAG_POSTBACK_SECRET, ADSGRAM_REWARD_SECRET

router = APIRouter(prefix="/api/v2", tags=["ads"])
logger = logging.getLogger(__name__)


@router.post("/ads/start")
async def ad_start(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    action = body.get("action", "")

    redis_conn = await get_redis()
    session_id = await create_ad_session(redis_conn, user_id, action)

    return {"success": True, "ad_session_id": session_id, "action": action}


@router.post("/ads/complete")
async def ad_complete(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    ad_session_id = body.get("ad_session_id", "")
    action = body.get("action", "")

    redis_conn = await get_redis()
    try:
        await consume_ad_session(redis_conn, user_id, ad_session_id, action)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {"success": True, "verified": True}


@router.post("/boost/mega")
async def mega_boost(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    ad_session_id = body.get("ad_session_id", "")

    redis_conn = await get_redis()
    try:
        await consume_ad_session(redis_conn, user_id, ad_session_id, "mega_boost")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        result = await activate_mega_boost(session, user_id, user)
        await session.commit()

    return result


@router.post("/boost/ghost")
async def ghost_boost(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    ad_session_id = body.get("ad_session_id", "")

    redis_conn = await get_redis()
    try:
        await consume_ad_session(redis_conn, user_id, ad_session_id, "ghost_boost")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        result = await activate_ghost_boost(session, user_id, user)
        await session.commit()

    return result


@router.post("/boost/autoclicker")
async def autoclicker_activate(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    ad_session_id = body.get("ad_session_id", "")

    redis_conn = await get_redis()
    try:
        await consume_ad_session(redis_conn, user_id, ad_session_id, "autoclicker")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        result = await activate_autoclicker(session, user_id, user)
        await session.commit()

    return result


@router.post("/energy/refill")
async def energy_refill(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    ad_session_id = body.get("ad_session_id", "")

    redis_conn = await get_redis()
    try:
        await consume_ad_session(
            redis_conn, user_id, ad_session_id, "energy_refill_max"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        result = await refill_energy(session, user_id, user)
        await session.commit()

    return result


@router.post("/ads/monetag/postback")
async def monetag_postback(request: Request):
    params = dict(request.query_params)

    if MONETAG_POSTBACK_SECRET:
        provided = params.get("token") or params.get("secret") or params.get("key")
        if provided != MONETAG_POSTBACK_SECRET:
            logger.warning("Monetag postback rejected: invalid secret")
            raise HTTPException(status_code=403, detail="Invalid secret")

    ad_session_id = (
        params.get("ad_session_id") or params.get("subid") or params.get("click_id")
    )
    if not ad_session_id:
        return {"status": "ignored"}

    redis_conn = await get_redis()
    try:
        await consume_ad_session(
            redis_conn, 0, ad_session_id, "", enforce_verification=False
        )
    except ValueError:
        pass

    return {"status": "ok"}


@router.post("/ads/adsgram/reward")
async def adsgram_reward(request: Request):
    params = dict(request.query_params)

    if ADSGRAM_REWARD_SECRET:
        provided = params.get("token") or params.get("secret") or params.get("key")
        if provided != ADSGRAM_REWARD_SECRET:
            raise HTTPException(status_code=403, detail="Invalid secret")

    ad_session_id = (
        params.get("ad_session_id")
        or params.get("session_id")
        or params.get("click_id")
    )
    if ad_session_id:
        redis_conn = await get_redis()
        try:
            await consume_ad_session(
                redis_conn, 0, ad_session_id, "", enforce_verification=False
            )
        except ValueError:
            pass
        return {"status": "ok"}

    return {"status": "session_not_found"}
