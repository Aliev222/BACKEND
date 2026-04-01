import os
import time
import json
import hmac
import hashlib
import base64
import secrets
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from fastapi import HTTPException

from DATABASE.base import User
from core.config import TON_VERIFIER_API_BASE, TON_VERIFIER_API_KEY

logger = logging.getLogger(__name__)

TON_PROOF_TTL_SECONDS = 300
TON_PROOF_PAYLOAD_LENGTH = 32

LOCAL_TON_PROOF_PAYLOADS: dict[str, dict] = {}


def _generate_proof_payload() -> dict:
    payload = secrets.token_hex(TON_PROOF_PAYLOAD_LENGTH)
    domain = os.getenv("TON_PROOF_DOMAIN", "spirix.vercel.app")
    timestamp = int(time.time())
    return {
        "payload": payload,
        "domain": domain,
        "timestamp": timestamp,
        "expires_at": timestamp + TON_PROOF_TTL_SECONDS,
    }


def _verify_ton_proof_signature(
    signature_b64: str,
    message: bytes,
    public_key_hex: str,
) -> bool:
    try:
        import nacl.signing

        public_key = bytes.fromhex(public_key_hex)
        signature = base64.b64decode(signature_b64)
        verify_key = nacl.signing.VerifyKey(public_key)
        verify_key.verify(message, signature)
        return True
    except Exception:
        return False


def _build_ton_proof_message(
    domain: str,
    timestamp: int,
    payload: str,
    wallet_address: str,
) -> bytes:
    prefix = "\xff\xffton-proof-item-v2/"
    message = prefix + wallet_address + domain + str(timestamp) + payload
    return hashlib.sha256(message.encode("utf-8")).digest()


async def generate_proof_payload(user_id: int) -> dict:
    proof_data = _generate_proof_payload()
    LOCAL_TON_PROOF_PAYLOADS[str(user_id)] = proof_data
    return {
        "payload": proof_data["payload"],
        "domain": proof_data["domain"],
        "timestamp": proof_data["timestamp"],
        "expires_at": proof_data["expires_at"],
    }


async def verify_ton_proof(
    wallet_address: str,
    wallet_public_key: str,
    proof_payload: str,
    proof_timestamp: int,
    proof_domain: str,
    proof_signature: str,
    user_id: int,
) -> bool:
    stored = LOCAL_TON_PROOF_PAYLOADS.get(str(user_id))
    if not stored:
        return False

    if stored["payload"] != proof_payload:
        return False

    now = int(time.time())
    if now > stored["expires_at"]:
        LOCAL_TON_PROOF_PAYLOADS.pop(str(user_id), None)
        return False

    message = _build_ton_proof_message(
        proof_domain, proof_timestamp, proof_payload, wallet_address
    )

    if not _verify_ton_proof_signature(proof_signature, message, wallet_public_key):
        return False

    if TON_VERIFIER_API_KEY and TON_VERIFIER_API_BASE:
        try:
            import httpx

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{TON_VERIFIER_API_BASE}/wallet/verifyProof",
                    json={
                        "address": wallet_address,
                        "proof": {
                            "timestamp": proof_timestamp,
                            "domain": {"lengthBytes": 1, "value": proof_domain},
                            "signature": proof_signature,
                            "payload": proof_payload,
                            "stateInit": "",
                        },
                    },
                    headers={"X-API-Key": TON_VERIFIER_API_KEY},
                )
                if response.status_code == 200:
                    data = response.json()
                    if not data.get("ok"):
                        return False
        except Exception as e:
            logger.warning(f"TON verifier API error: {e}")

    LOCAL_TON_PROOF_PAYLOADS.pop(str(user_id), None)
    return True


async def connect_wallet(
    session: AsyncSession,
    user_id: int,
    wallet_address: str,
    wallet_provider: str | None = None,
    wallet_public_key: str | None = None,
    wallet_state_init: str | None = None,
    proof_verified: bool = False,
) -> dict:
    user_result = await session.execute(select(User).where(User.user_id == user_id))
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    extra = {}
    if user.extra_data:
        try:
            extra = json.loads(user.extra_data)
        except Exception:
            extra = {}

    extra["ton_wallet"] = wallet_address
    extra["ton_wallet_provider"] = wallet_provider
    extra["ton_wallet_public_key"] = wallet_public_key
    extra["ton_wallet_state_init"] = wallet_state_init
    extra["ton_wallet_verified"] = proof_verified
    extra["ton_wallet_connected_at"] = datetime.utcnow().isoformat()

    await session.execute(
        update(User).where(User.user_id == user_id).values(extra_data=json.dumps(extra))
    )

    return {
        "success": True,
        "wallet_address": wallet_address,
        "verified": proof_verified,
    }


async def disconnect_wallet(
    session: AsyncSession,
    user_id: int,
) -> dict:
    user_result = await session.execute(select(User).where(User.user_id == user_id))
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    extra = {}
    if user.extra_data:
        try:
            extra = json.loads(user.extra_data)
        except Exception:
            extra = {}

    for key in [
        "ton_wallet",
        "ton_wallet_provider",
        "ton_wallet_public_key",
        "ton_wallet_state_init",
        "ton_wallet_verified",
        "ton_wallet_connected_at",
    ]:
        extra.pop(key, None)

    await session.execute(
        update(User).where(User.user_id == user_id).values(extra_data=json.dumps(extra))
    )

    return {"success": True}
