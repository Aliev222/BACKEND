import logging
from fastapi import APIRouter, Request, HTTPException

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from services.ton_service import (
    generate_proof_payload,
    verify_ton_proof,
    connect_wallet,
    disconnect_wallet,
)

router = APIRouter(prefix="/api/v2", tags=["ton"])
logger = logging.getLogger(__name__)


@router.get("/ton/proof")
async def get_ton_proof(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))
    proof = await generate_proof_payload(user_id)
    return {"success": True, "proof": proof}


@router.post("/ton/connect")
async def connect_ton_wallet(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    wallet_address = body.get("wallet_address", "")
    wallet_provider = body.get("wallet_provider")
    wallet_public_key = body.get("wallet_public_key")
    wallet_state_init = body.get("wallet_state_init")

    ton_proof = body.get("ton_proof")
    proof_verified = False

    if ton_proof:
        proof_verified = await verify_ton_proof(
            wallet_address=wallet_address,
            wallet_public_key=wallet_public_key or "",
            proof_payload=ton_proof.get("payload", ""),
            proof_timestamp=ton_proof.get("timestamp", 0),
            proof_domain=(ton_proof.get("domain") or {}).get("value", ""),
            proof_signature=ton_proof.get("signature", ""),
            user_id=user_id,
        )

    async with AsyncSessionLocal() as session:
        result = await connect_wallet(
            session,
            user_id,
            wallet_address,
            wallet_provider,
            wallet_public_key,
            wallet_state_init,
            proof_verified,
        )
        await session.commit()

    return result


@router.post("/ton/disconnect")
async def disconnect_ton_wallet(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        result = await disconnect_wallet(session, user_id)
        await session.commit()

    return result


@router.get("/ton/wallet")
async def get_ton_wallet(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user_result = await session.execute(select(User).where(User.user_id == user_id))
        user = user_result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    extra = {}
    if user.extra_data:
        try:
            import json

            extra = json.loads(user.extra_data)
        except Exception:
            extra = {}

    wallet = extra.get("ton_wallet")
    if not wallet:
        return {"success": True, "connected": False}

    return {
        "success": True,
        "connected": True,
        "wallet_address": wallet,
        "provider": extra.get("ton_wallet_provider"),
        "verified": extra.get("ton_wallet_verified", False),
        "connected_at": extra.get("ton_wallet_connected_at"),
    }
