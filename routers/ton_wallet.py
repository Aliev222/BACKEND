"""
TON wallet routes extracted from legacy.py (Patch 7.4).
"""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, Request, HTTPException

from core.utils import parse_extra_data
from core.ton_utils import (
    get_ton_wallet_from_user,
    is_valid_ton_wallet_address,
    ton_addresses_match,
    ton_proof_allowed_domains,
)
from schemas import TonWalletConnectRequest, TonWalletDisconnectRequest
from routers.legacy import (
    require_telegram_user,
    get_user_cached,
    get_user,
    update_user,
    invalidate_user_cache,
    issue_ton_proof_payload,
    verify_ton_wallet_proof,
)

router = APIRouter(tags=["ton-wallet"])
logger = logging.getLogger(__name__)


@router.get("/api/ton/wallet/{user_id}")
async def get_ton_wallet_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {
            "success": True,
            "user_id": user_id,
            "wallet": get_ton_wallet_from_user(user),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ton_wallet_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/ton/wallet/proof-payload/{user_id}")
async def get_ton_wallet_proof_payload(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        payload, expires_at = await issue_ton_proof_payload(user_id)
        return {
            "success": True,
            "user_id": user_id,
            "payload": payload,
            "expires_at": expires_at,
            "expires_in_seconds": max(0, expires_at - int(time.time())),
            "allowed_domains": sorted(ton_proof_allowed_domains(request)),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ton_wallet_proof_payload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ton/wallet/connect")
async def connect_ton_wallet(payload: TonWalletConnectRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        wallet_address = (payload.wallet_address or "").strip()
        if not is_valid_ton_wallet_address(wallet_address):
            raise HTTPException(status_code=400, detail="Invalid TON wallet address")

        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        existing_wallet = (
            extra.get("ton_wallet") if isinstance(extra.get("ton_wallet"), dict) else {}
        )

        wallet_verified = False
        verification_error = None
        verified_at = None
        if payload.ton_proof:
            wallet_verified, verification_error = await verify_ton_wallet_proof(
                payload.user_id,
                wallet_address,
                payload.ton_proof,
                request,
                payload.wallet_public_key,
                payload.wallet_state_init,
            )
            if not wallet_verified:
                logger.warning(
                    "TON wallet proof verification failed for user %s: %s (domain=%s, address=%s)",
                    payload.user_id,
                    verification_error or "unknown error",
                    getattr(getattr(payload.ton_proof, "domain", None), "value", None),
                    wallet_address,
                )
        elif bool(existing_wallet.get("verified")) and ton_addresses_match(
            existing_wallet.get("address"), wallet_address
        ):
            wallet_verified = True
            verification_error = None
            verified_at = existing_wallet.get("verified_at")

        extra["ton_wallet"] = {
            "address": wallet_address,
            "provider": (payload.wallet_provider or "").strip(),
            "app_name": (payload.wallet_app_name or "").strip(),
            "network": (payload.wallet_network or "").strip(),
            "connected_at": datetime.utcnow().isoformat(),
            "verified": wallet_verified,
            "verified_at": verified_at
            or (datetime.utcnow().isoformat() if wallet_verified else None),
            "verification_error": verification_error or None,
        }
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "user_id": payload.user_id,
            "wallet": get_ton_wallet_from_user({"extra_data": extra}),
            "verification_error": verification_error,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in connect_ton_wallet: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ton/wallet/disconnect")
async def disconnect_ton_wallet(payload: TonWalletDisconnectRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        extra.pop("ton_wallet", None)
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "user_id": payload.user_id,
            "wallet": get_ton_wallet_from_user({"extra_data": extra}),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in disconnect_ton_wallet: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
