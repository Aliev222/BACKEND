import hashlib
import hmac
import json
import time
from urllib.parse import parse_qsl

from fastapi import HTTPException

from CONFIG.settings import BOT_TOKEN


TELEGRAM_INIT_DATA_TTL_SECONDS = 24 * 60 * 60


def verify_telegram_init_data(init_data: str) -> dict:
    if not init_data:
        raise HTTPException(status_code=401, detail="Missing Telegram auth data")

    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = pairs.pop("hash", None)
    if not received_hash:
        raise HTTPException(status_code=401, detail="Invalid Telegram auth data")

    data_check_string = "\n".join(
        f"{key}={value}" for key, value in sorted(pairs.items())
    )
    secret_key = hmac.new(
        b"WebAppData",
        BOT_TOKEN.encode("utf-8"),
        hashlib.sha256
    ).digest()
    computed_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        raise HTTPException(status_code=401, detail="Invalid Telegram signature")

    auth_date = pairs.get("auth_date")
    if auth_date:
        try:
            auth_ts = int(auth_date)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail="Invalid Telegram auth date") from exc

        if time.time() - auth_ts > TELEGRAM_INIT_DATA_TTL_SECONDS:
            raise HTTPException(status_code=401, detail="Telegram auth data expired")

    raw_user = pairs.get("user")
    if not raw_user:
        raise HTTPException(status_code=401, detail="Telegram user data is missing")

    try:
        return json.loads(raw_user)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=401, detail="Invalid Telegram user payload") from exc
