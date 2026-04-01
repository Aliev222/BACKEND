import json
import time
import hmac
import hashlib
import base64
import secrets
import logging
from fastapi import APIRouter, Request, HTTPException

from core.telegram_auth import verify_telegram_init_data
from core.config import SESSION_TOKEN_SECRET, SESSION_TOKEN_TTL_SECONDS

router = APIRouter(prefix="/api/v2", tags=["auth"])
logger = logging.getLogger(__name__)


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def issue_session_token(telegram_user: dict) -> tuple[str, int]:
    now_ts = int(time.time())
    expires_at = now_ts + SESSION_TOKEN_TTL_SECONDS
    payload = {
        "uid": int(telegram_user.get("id", 0)),
        "username": telegram_user.get("username"),
        "iat": now_ts,
        "exp": expires_at,
        "jti": secrets.token_hex(8),
    }
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    payload_part = _b64url_encode(payload_json)
    signature = hmac.new(
        SESSION_TOKEN_SECRET.encode("utf-8"),
        payload_part.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_part}.{signature}", expires_at


def verify_session_token(token: str) -> dict:
    if not token or "." not in token:
        raise HTTPException(status_code=401, detail="Invalid session token")

    payload_part, signature = token.rsplit(".", 1)
    expected_signature = hmac.new(
        SESSION_TOKEN_SECRET.encode("utf-8"),
        payload_part.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        raise HTTPException(status_code=401, detail="Invalid session signature")

    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid session payload") from exc

    now_ts = int(time.time())
    if int(payload.get("exp", 0) or 0) <= now_ts:
        raise HTTPException(status_code=401, detail="Session expired")

    user_id = int(payload.get("uid", 0) or 0)
    if user_id <= 0:
        raise HTTPException(status_code=401, detail="Invalid session user")

    return {
        "id": user_id,
        "username": payload.get("username"),
        "iat": int(payload.get("iat", 0) or 0),
        "exp": int(payload.get("exp", 0) or 0),
        "jti": payload.get("jti"),
        "auth": "session",
    }


def read_bearer_token(request: Request) -> str:
    authorization = (request.headers.get("Authorization", "") or "").strip()
    if not authorization:
        return ""
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return ""
    return parts[1].strip()


async def require_telegram_user(
    request: Request, expected_user_id: int | None = None
) -> dict:
    bearer_token = read_bearer_token(request)
    if bearer_token:
        telegram_user = verify_session_token(bearer_token)
    else:
        telegram_user = verify_telegram_init_data(
            request.headers.get("X-Telegram-Init-Data", "")
        )

    if expected_user_id is not None and int(telegram_user.get("id", 0)) != int(
        expected_user_id
    ):
        raise HTTPException(status_code=403, detail="Telegram user mismatch")

    return telegram_user


@router.post("/auth/session")
async def create_session(request: Request):
    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    token, expires_at = issue_session_token(telegram_user)
    return {
        "success": True,
        "token": token,
        "token_type": "Bearer",
        "expires_in": SESSION_TOKEN_TTL_SECONDS,
        "expires_at": expires_at,
        "user_id": int(telegram_user.get("id", 0)),
    }
