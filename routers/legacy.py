from fastapi import APIRouter, HTTPException, Request

# Sync marker for VS Code source control
from fastapi.responses import JSONResponse, Response
import asyncio
import base64
import random
import time
import json
import os
import logging
import httpx
import hmac
import hashlib
import secrets
import re
import struct
from datetime import datetime, timedelta
from urllib.parse import urlparse
from sqlalchemy import select, func, update, or_
from sqlalchemy.ext.asyncio import AsyncSession
from DATABASE.base import (
    User,
    UserTask,
    AsyncSessionLocal,
    WeeklyTournamentEntry,
    WeeklyTournamentWinner,
    WeeklyTournamentTonPayout,
    RewardedAdClaim,
    StarsSkinPurchase,
)
from collections import defaultdict, deque
from dataclasses import dataclass
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from DATABASE.base import (
    get_user,
    add_user as create_user,
    update_user,
    add_referral_bonus,
    init_db,
    get_completed_tasks,
    add_completed_task,
    record_crash_ghost_cashout,
    add_weekly_tournament_score,
    get_weekly_tournament_leaderboard,
    get_weekly_tournament_player_entry,
    get_weekly_tournament_season_key,
    get_weekly_tournament_season_window,
    get_weekly_tournament_league,
    list_weekly_tournament_seasons,
    get_weekly_tournament_winners,
    finalize_weekly_tournament_season,
    ensure_weekly_tournament_season,
    get_rewarded_ads_admin_summary,
    get_stars_skin_sales_admin_summary,
    get_admin_fraud_reviews,
    upsert_admin_fraud_review,
    record_rewarded_ad_claim,
    get_referral_stats,
    get_referrals_list,
)
from schemas import (
    AdActionClaimRequest,
    AdActionStartRequest,
    ClicksBatchRequest,
    EnergySyncRequest,
    PassiveIncomeRequest,
    RegisterRequest,
    RewardVideoClaimRequest,
    RewardVideoStartRequest,
    SkinRequest,
    TaskCompleteRequest,
    TournamentData,
    UpgradeRequest,
    UserIdRequest,
    VideoTaskClaimRequest,
    AdminFraudUpdateRequest,
    AdminTonPayoutConfirmRequest,
    AdminTonPayoutQueueRequest,
    AdminTonPayoutBulkStatusUpdateRequest,
    AdminTonPayoutStatusUpdateRequest,
    AdminWalletReminderRequest,
    AdminWinnerStarsUpdateRequest,
    TonProofRequest,
    TonWalletConnectRequest,
    TonWalletDisconnectRequest,
    WeeklyTournamentFundRequest,
)
from core.game_config import (
    BASE_MAX_ENERGY,
    CLICK_BURST_ALLOWANCE,
    CLICK_BUFFER_KEY,
    CLICK_FLUSH_INTERVAL,
    CLICK_TIME_ACCUMULATION_CAP_SECONDS,
    CLICK_SUSPICIOUS_OVERSHOOT,
    CLICK_SUSPICION_SOFT_LIMIT,
    ENERGY_REGEN_SECONDS,
    INITIAL_CLICK_BATCH_ALLOWANCE,
    MAX_BET,
    MAX_CLICK_BATCH_SIZE,
    MAX_UPGRADE_LEVEL,
    MAX_REAL_CLICKS_PER_SECOND,
    MAX_REWARD_PER_VIDEO,
    MIN_BET,
    RATE_LIMITS,
    TOURNAMENT_KEY,
    TOURNAMENT_PRIZE_POOL,
    GLOBAL_UPGRADE_PRICES,
    UPGRADE_PRICES,
    USER_CACHE_PREFIX,
    USER_CACHE_TTL,
)
from core.game_logic import (
    build_energy_payload,
    calculate_current_energy,
    get_hour_value,
    get_max_energy,
    get_tap_value,
    mask_username,
    normalize_dt,
    resolve_max_energy,
    resolve_progression_level,
)
from core.config import (
    DIAGNOSTICS_DURATION_WINDOW,
    DAILY_REWARD_MAX_DAYS,
    AUTOCLICKER_COOLDOWN_MINUTES,
    VIDEO_TASK_DEFINITIONS,
    WEEKLY_LEAGUE_ORDER,
    WEEKLY_LEAGUE_LEVEL_RANGES,
    WEEKLY_LEAGUE_FUND_SPLITS,
    WEEKLY_TOP3_PAYOUT_SPLITS,
    WEEKLY_RANGE_PAYOUT_SPLITS,
    TON_NANO,
    TON_VERIFIER_API_BASE,
    TON_VERIFIER_API_KEY,
    TON_VERIFIER_TIMEOUT_SECONDS,
)
from core.skins import DEFAULT_SKIN_ID, SOCIAL_SUB_TASK_SKINS
from core.realtime_state import (
    build_realtime_player_state,
    build_click_response_state,
    get_all_boost_states,
)
from core.telegram_auth import verify_telegram_init_data
from core.stars_skins import get_stars_skin_price
from CONFIG.settings import BOT_TOKEN
from infrastructure.coins_hot_sync import (
    sync_hot_after_db_increment,
    sync_hot_after_db_decrement,
)
from services.clicks_service import (
    ClicksServiceDeps,
    process_clicks_batch_service,
    sync_energy_service,
)
from services.ads_boosts_service import (
    AdsBoostsServiceDeps,
    activate_ghost_boost_service,
    activate_mega_boost_service,
    ad_action_start_service,
    adsgram_complete_locally_service,
    consume_ad_action_session_service,
    increment_ads_watched_service,
    mark_ad_action_session_verified_service,
    mark_latest_ad_action_session_verified_for_user_service,
    update_energy_service as update_energy_reward_service,
)
from services.profile_state_service import (
    ProfileStateServiceDeps,
    ensure_coins_hot_initialized_service,
    get_user_data_service,
)
from services.tasks_rewards_service import (
    TasksRewardsServiceDeps,
    complete_task_reward_atomically_service,
    get_active_video_task_boost_service,
    get_daily_reward_progress_service,
    get_video_task_boosts_service,
    get_video_task_last_claims_service,
    resolve_video_task_coin_drop_service,
    verify_telegram_channel_subscription_service,
)
from services.tournament_service import (
    TournamentReadServiceDeps,
    get_weekly_tournament_leaderboard_service,
    get_weekly_tournament_overview_service,
    get_weekly_tournament_results_service,
)
from services.upgrades_service import (
    UpgradesServiceDeps,
    get_global_upgrade_level_service,
    process_upgrade_all_service,
    process_upgrade_service,
)
from services.rebirth_service import (
    RebirthServiceDeps,
    process_rebirth_service,
)


router = APIRouter()


REDIS_URL = os.getenv("REDIS_URL")
REDIS_DB = int((os.getenv("REDIS_DB", "0") or "0").strip())
redis_client = None
LOCAL_LOCKS: dict[str, float] = {}
LOCAL_IDEMPOTENCY_KEYS: dict[str, float] = {}
LOCAL_RATE_LIMITS_STATE: dict[str, deque[float]] = defaultdict(deque)
LOCAL_TON_PROOF_PAYLOADS: dict[str, dict] = {}
ENDPOINT_DIAGNOSTICS: dict[tuple[str, str], dict] = {}
RECENT_DIAGNOSTIC_ERRORS: deque[dict] = deque(maxlen=120)
APP_ENV = (os.getenv("APP_ENV", "production") or "production").strip().lower()
ONLINE_USERS_KEY = "online:users"
ONLINE_WINDOW_SECONDS = 75
REFERRAL_SHARE_RATE = 0.05

REFERRAL_DAILY_SHARE_LIMIT = 50000
REFERRAL_SPECIAL_SKIN_ID = "refferal.pngSP"
TELEGRAM_VERIFY_CHANNEL = os.getenv("TELEGRAM_VERIFY_CHANNEL", "@Spirit_cliker")
TELEGRAM_MEMBER_STATUSES = {"member", "administrator", "creator", "restricted"}
TELEGRAM_BOT_USERNAME = (
    (os.getenv("TELEGRAM_BOT_USERNAME", "Ryoho_bot") or "Ryoho_bot").strip().lstrip("@")
)
GAME_WEBAPP_URL = (
    os.getenv("GAME_WEBAPP_URL", "https://spirix.vercel.app")
    or "https://spirix.vercel.app"
).strip()
ADMIN_DASHBOARD_TOKEN = (os.getenv("ADMIN_DASHBOARD_TOKEN", "") or "").strip()
ADMIN_TELEGRAM_IDS = {
    int(item.strip())
    for item in (os.getenv("ADMIN_TELEGRAM_IDS", "") or "").split(",")
    if item.strip().isdigit()
}
MONETAG_POSTBACK_SECRET = (os.getenv("MONETAG_POSTBACK_SECRET", "") or "").strip()
MONETAG_POSTBACK_ENFORCED = (
    os.getenv("MONETAG_POSTBACK_ENFORCED", "1" if MONETAG_POSTBACK_SECRET else "0")
    or "0"
).strip().lower() in {"1", "true", "yes", "on"}
ADSGRAM_REWARD_SECRET = (os.getenv("ADSGRAM_REWARD_SECRET", "") or "").strip()
ADSGRAM_REWARD_ENFORCED = (
    os.getenv("ADSGRAM_REWARD_ENFORCED", "1" if ADSGRAM_REWARD_SECRET else "0") or "0"
).strip().lower() in {"1", "true", "yes", "on"}
SESSION_TOKEN_SECRET = (
    os.getenv("SESSION_TOKEN_SECRET", "") or ""
).strip() or hashlib.sha256(f"{BOT_TOKEN}:session-token".encode("utf-8")).hexdigest()
SESSION_TOKEN_TTL_SECONDS = max(
    900, int((os.getenv("SESSION_TOKEN_TTL_SECONDS", "3600") or "3600").strip())
)
ENABLE_K6_FRAUD_HEURISTICS = (
    os.getenv("ENABLE_K6_FRAUD_HEURISTICS", "0") or "0"
).strip().lower() in {"1", "true", "yes", "on"}
DAILY_REWARD_BASE_COINS = 500
DAILY_REWARD_INFINITE_ENERGY_MINUTES = 10
DAILY_REWARD_SKIN_ID = "retro.pngSP"
MEGA_BOOST_MINUTES = 1
MEGA_BOOST_COOLDOWN_MIN_MINUTES = 10
MEGA_BOOST_COOLDOWN_MAX_MINUTES = 10
GHOST_BOOST_MULTIPLIER = 5
GHOST_BOOST_MINUTES = 1
SKIN_AD_COOLDOWN_MINUTES = 10
ENERGY_REFILL_COOLDOWN_MINUTES = 10
AD_ACTION_SESSION_TTL_SECONDS = 180
AD_SESSION_MIN_WAIT_SECONDS = 8
AD_ACTIONS_ALLOWED = {
    "energy_refill_max",
    "mega_boost",
    "ghost_boost",
    "ads_increment",
    "video_task",
    "autoclicker",
}
CRASH_GHOST_SESSION_TTL_SECONDS = 90
CRASH_GHOST_MULTIPLIER_SPEED = 0.68
MONETAG_POSTBACK_ID_KEYS = (
    "ad_session_id",
    "subid",
    "sub_id",
    "click_id",
    "clickid",
    "cid",
    "transaction_id",
    "txid",
    "tid",
    "session_id",
    "s1",
    "s2",
    "s3",
    "ymid",
    "request_var",
)
MONETAG_POSTBACK_SECRET_KEYS = ("token", "secret", "key")
MONETAG_POSTBACK_NEGATIVE_VALUES = {
    "0",
    "false",
    "failed",
    "cancelled",
    "canceled",
    "rejected",
    "deny",
    "denied",
}
ADSGRAM_REWARD_USER_KEYS = (
    "user_id",
    "userid",
    "userId",
    "telegram_id",
    "telegramId",
    "tg_user_id",
    "tgUserId",
)
ADSGRAM_REWARD_SESSION_KEYS = (
    "ad_session_id",
    "session_id",
    "request_var",
    "click_id",
    "cid",
    "payload",
    "custom_data",
)
ADSGRAM_REWARD_SECRET_KEYS = ("token", "secret", "key")

VIDEO_SKIN_IDS = {
    "video.pngSP",
    "video2.pngSP",
    "video3.pngSP",
    "video4.pngSP",
    "video5.pngSP",
    "video6.pngSP",
    "video7.pngSP",
    "video8.pngSP",
}
TON_WALLET_ALLOWED_CHARS = set(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-:"
)
TON_PROOF_TTL_SECONDS = max(
    120, int((os.getenv("TON_PROOF_TTL_SECONDS", "900") or "900").strip())
)
TON_PROOF_ALLOWED_DOMAINS = tuple(
    item.strip().lower()
    for item in (os.getenv("TON_PROOF_ALLOWED_DOMAINS", "") or "").split(",")
    if item.strip()
)


def is_valid_ton_wallet_address(value: str) -> bool:
    address = (value or "").strip()
    if not 32 <= len(address) <= 128:
        return False
    return all(char in TON_WALLET_ALLOWED_CHARS for char in address)


def mask_ton_wallet(address: str | None) -> str:
    raw = (address or "").strip()
    if len(raw) < 12:
        return raw
    return f"{raw[:6]}...{raw[-6:]}"


def get_ton_wallet_from_user(user: dict | None) -> dict:
    extra_data = (user or {}).get("extra_data") or {}
    wallet = extra_data.get("ton_wallet") or {}
    if not isinstance(wallet, dict):
        wallet = {}
    address = (wallet.get("address") or "").strip()
    connected = bool(address and is_valid_ton_wallet_address(address))
    return {
        "connected": connected,
        "address": address if connected else "",
        "masked_address": mask_ton_wallet(address) if connected else "",
        "provider": (wallet.get("provider") or "").strip(),
        "app_name": (wallet.get("app_name") or "").strip(),
        "connected_at": wallet.get("connected_at"),
        "verified": bool(connected and wallet.get("verified")),
        "verified_at": wallet.get("verified_at"),
        "verification_error": (wallet.get("verification_error") or "").strip(),
    }


def parse_extra_data_object(raw_extra) -> dict:
    if isinstance(raw_extra, dict):
        return raw_extra
    if isinstance(raw_extra, str):
        try:
            parsed = json.loads(raw_extra)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def parse_json_object(raw_value) -> dict:
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def get_ton_proof_storage_key(user_id: int, payload: str) -> str:
    return f"ton:proof:{int(user_id)}:{payload}"


def ton_proof_allowed_domains(request: Request | None = None) -> set[str]:
    allowed: set[str] = set(TON_PROOF_ALLOWED_DOMAINS)
    game_host = (urlparse(GAME_WEBAPP_URL).netloc or "").strip().lower()
    if game_host:
        allowed.add(game_host)
    if request:
        for header_name in ("origin", "referer"):
            header_value = (request.headers.get(header_name) or "").strip()
            if not header_value:
                continue
            header_host = (urlparse(header_value).netloc or "").strip().lower()
            if header_host:
                allowed.add(header_host)
    return {item for item in allowed if item}


def crc16_xmodem(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def parse_ton_address_parts(address: str) -> tuple[int, bytes, str]:
    value = (address or "").strip()
    if ":" in value:
        workchain_raw, account_id = value.split(":", 1)
        workchain = int(workchain_raw.strip())
        account_id = account_id.strip().lower()
        if len(account_id) != 64 or not re.fullmatch(r"[0-9a-f]{64}", account_id):
            raise ValueError("Invalid raw TON address")
        return workchain, bytes.fromhex(account_id), f"{workchain}:{account_id}"

    normalized = value.replace("-", "+").replace("_", "/")
    padding = (-len(normalized)) % 4
    if padding:
        normalized += "=" * padding
    decoded = base64.b64decode(normalized)
    if len(decoded) != 36:
        raise ValueError("Invalid friendly TON address")
    body, checksum = decoded[:34], decoded[34:]
    expected_checksum = crc16_xmodem(body).to_bytes(2, "big")
    if checksum != expected_checksum:
        raise ValueError("Invalid TON address checksum")
    workchain = struct.unpack("b", body[1:2])[0]
    account_bytes = body[2:]
    return workchain, account_bytes, f"{workchain}:{account_bytes.hex()}"


def decode_base64_any(value: str) -> bytes:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("Empty base64 value")
    normalized = raw.replace("-", "+").replace("_", "/")
    padding = (-len(normalized)) % 4
    if padding:
        normalized += "=" * padding
    return base64.b64decode(normalized)


async def issue_ton_proof_payload(user_id: int) -> tuple[str, int]:
    payload = secrets.token_urlsafe(24)
    expires_at = int(time.time()) + TON_PROOF_TTL_SECONDS
    storage_key = get_ton_proof_storage_key(user_id, payload)
    payload_data = {"user_id": int(user_id), "expires_at": expires_at}
    redis_conn = await get_redis_or_none()
    if redis_conn:
        await redis_conn.setex(
            storage_key, TON_PROOF_TTL_SECONDS, json.dumps(payload_data)
        )
    else:
        LOCAL_TON_PROOF_PAYLOADS[storage_key] = payload_data
    return payload, expires_at


async def consume_ton_proof_payload(user_id: int, payload: str) -> bool:
    storage_key = get_ton_proof_storage_key(user_id, payload)
    now_ts = int(time.time())
    redis_conn = await get_redis_or_none()
    if redis_conn:
        raw = await redis_conn.get(storage_key)
        if not raw:
            return False
        try:
            data = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            data = {}
        await redis_conn.delete(storage_key)
        return (
            int(data.get("user_id") or 0) == int(user_id)
            and int(data.get("expires_at") or 0) >= now_ts
        )

    payload_data = LOCAL_TON_PROOF_PAYLOADS.pop(storage_key, None)
    if not payload_data:
        return False
    return (
        int(payload_data.get("user_id") or 0) == int(user_id)
        and int(payload_data.get("expires_at") or 0) >= now_ts
    )


async def fetch_wallet_public_key_from_chain(raw_address: str) -> bytes | None:
    headers = {}
    if TON_VERIFIER_API_KEY:
        headers["X-API-Key"] = TON_VERIFIER_API_KEY
    payload = {
        "address": raw_address,
        "method": "get_public_key",
        "stack": [],
    }
    try:
        async with httpx.AsyncClient(timeout=TON_VERIFIER_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{TON_VERIFIER_API_BASE}/runGetMethod", json=payload, headers=headers
            )
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        logger.warning(f"TON public key lookup failed for {raw_address}: {exc}")
        return None

    stack = data.get("stack") or data.get("result", {}).get("stack") or []
    if not stack:
        return None
    value = stack[0].get("value")
    if value is None:
        return None
    try:
        key_int = int(str(value), 0)
        if key_int < 0:
            return None
        return key_int.to_bytes(32, "big")
    except Exception:
        return None


def decode_ton_wallet_public_key(value: str | None) -> bytes | None:
    raw = (value or "").strip()
    if not raw:
        return None
    normalized = raw.lower()
    if normalized.startswith("0x"):
        normalized = normalized[2:]
    if re.fullmatch(r"[0-9a-f]{64}", normalized):
        try:
            return bytes.fromhex(normalized)
        except ValueError:
            return None
    try:
        decoded = decode_base64_any(raw)
        return decoded if len(decoded) == 32 else None
    except Exception:
        return None


def ton_addresses_match(left: str | None, right: str | None) -> bool:
    left_value = (left or "").strip()
    right_value = (right or "").strip()
    if not left_value or not right_value:
        return False
    try:
        return (
            parse_ton_address_parts(left_value)[2]
            == parse_ton_address_parts(right_value)[2]
        )
    except Exception:
        return left_value == right_value


async def verify_ton_wallet_proof(
    user_id: int,
    wallet_address: str,
    ton_proof: TonProofRequest,
    request: Request,
    wallet_public_key: str | None = None,
    wallet_state_init: str | None = None,
) -> tuple[bool, str | None]:
    try:
        from nacl.exceptions import BadSignatureError
        from nacl.signing import VerifyKey
    except ImportError:
        logger.error("PyNaCl is not installed; TON proof verification is unavailable")
        raise HTTPException(
            status_code=500, detail="TON proof verification is unavailable"
        )

    payload = (ton_proof.payload or "").strip()
    if not payload:
        return False, "Missing ton proof payload"
    if not await consume_ton_proof_payload(user_id, payload):
        return False, "TON proof payload expired or invalid"

    proof_domain_raw = (ton_proof.domain.value or "").strip()
    proof_domain = proof_domain_raw.lower()
    if not proof_domain or proof_domain not in ton_proof_allowed_domains(request):
        return False, "TON proof domain is not allowed"

    domain_bytes = proof_domain_raw.encode("utf-8")
    if int(ton_proof.domain.lengthBytes) != len(domain_bytes):
        return False, "TON proof domain length mismatch"

    now_ts = int(time.time())
    proof_ts = int(ton_proof.timestamp or 0)
    if proof_ts <= 0 or abs(now_ts - proof_ts) > TON_PROOF_TTL_SECONDS:
        return False, "TON proof expired"

    try:
        workchain, account_bytes, raw_address = parse_ton_address_parts(wallet_address)
    except ValueError:
        return False, "Invalid TON wallet address"

    client_public_key = decode_ton_wallet_public_key(wallet_public_key)
    chain_public_key = await fetch_wallet_public_key_from_chain(raw_address)
    if not client_public_key and not chain_public_key:
        return False, "Unable to verify wallet public key"

    candidate_keys: list[bytes] = []
    if client_public_key:
        candidate_keys.append(client_public_key)
    if chain_public_key and all(
        existing != chain_public_key for existing in candidate_keys
    ):
        candidate_keys.append(chain_public_key)

    try:
        signature_bytes = decode_base64_any(ton_proof.signature)
    except Exception:
        return False, "Invalid TON proof signature"

    message = b"".join(
        [
            b"ton-proof-item-v2/",
            struct.pack(">i", int(workchain)),
            account_bytes,
            struct.pack("<I", len(domain_bytes)),
            domain_bytes,
            struct.pack("<Q", proof_ts),
            payload.encode("utf-8"),
        ]
    )
    message_hash = hashlib.sha256(message).digest()
    full_message = b"\xff\xff" + b"ton-connect" + message_hash
    verify_hash = hashlib.sha256(full_message).digest()

    for public_key in candidate_keys:
        try:
            VerifyKey(public_key).verify(verify_hash, signature_bytes)
            if (
                client_public_key
                and chain_public_key
                and client_public_key != chain_public_key
            ):
                logger.warning(
                    "TON wallet proof verified with client key that differs from chain key for user %s (address=%s, has_state_init=%s)",
                    user_id,
                    wallet_address,
                    bool((wallet_state_init or "").strip()),
                )
            return True, None
        except BadSignatureError:
            continue
        except Exception:
            continue

    return False, "Invalid TON proof signature"


async def get_pending_ton_wallet_notice(user_id: int) -> dict | None:
    user = await get_user_cached(user_id)
    if not user:
        return None

    extra_data = parse_extra_data(user.get("extra_data"))
    wallet = get_ton_wallet_from_user({"extra_data": extra_data})
    if wallet.get("connected") and wallet.get("verified"):
        return None

    async with AsyncSessionLocal() as session:
        winner_result = await session.execute(
            select(WeeklyTournamentWinner)
            .where(
                WeeklyTournamentWinner.user_id == user_id,
                WeeklyTournamentWinner.eligible_for_payout == True,
                WeeklyTournamentWinner.fraud_flag == False,
                WeeklyTournamentWinner.payout_cents > 0,
            )
            .order_by(WeeklyTournamentWinner.created_at.desc())
            .limit(1)
        )
        winner_row = winner_result.scalars().first()
        if not winner_row:
            return None

        payout_result = await session.execute(
            select(WeeklyTournamentTonPayout)
            .where(
                WeeklyTournamentTonPayout.season_key == winner_row.season_key,
                WeeklyTournamentTonPayout.user_id == user_id,
            )
            .limit(1)
        )
        payout_row = payout_result.scalars().first()

    if payout_row and str(getattr(payout_row, "status", "") or "").lower() in {
        "queued",
        "submitted",
        "sent",
    }:
        return None

    reminders_by_season = extra_data.get("ton_wallet_reminders") or {}
    reminder = (
        reminders_by_season.get(winner_row.season_key)
        if isinstance(reminders_by_season, dict)
        else {}
    )
    if not isinstance(reminder, dict):
        reminder = {}

    reminder_sent_at = reminder.get("sent_at")
    hours_until_deadline = int(reminder.get("hours_until_deadline") or 72)
    deadline_at = None
    parsed_sent_at = parse_iso_datetime(reminder_sent_at)
    if parsed_sent_at:
        deadline_at = (
            parsed_sent_at + timedelta(hours=hours_until_deadline)
        ).isoformat()

    return {
        "season_key": winner_row.season_key,
        "league": winner_row.league,
        "rank": int(winner_row.rank or 0),
        "payout_cents": int(winner_row.payout_cents or 0),
        "wallet_connected": False,
        "reminder_sent_at": reminder_sent_at,
        "hours_until_deadline": hours_until_deadline,
        "deadline_at": deadline_at,
    }


def ton_wallet_normalized_variants(address: str | None) -> set[str]:
    raw = (address or "").strip()
    if not raw:
        return set()
    lowered = raw.lower()
    variants = {raw, lowered}
    if raw.startswith("0:"):
        variants.add(raw[2:])
        variants.add(raw[2:].lower())
    return {item for item in variants if item}


def ton_wallets_equal(left: str | None, right: str | None) -> bool:
    left_variants = ton_wallet_normalized_variants(left)
    right_variants = ton_wallet_normalized_variants(right)
    return bool(
        left_variants and right_variants and left_variants.intersection(right_variants)
    )


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _parse_csv_env(name: str, default: list[str]) -> list[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


PROD_CORS_ORIGINS = [
    "https://spirix.vercel.app",
    "https://web.telegram.org",
    "https://telegram.org",
]
DEV_CORS_ORIGINS = PROD_CORS_ORIGINS + [
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:5500",
    "http://127.0.0.1:5500",
]
ALLOWED_CORS_ORIGINS = _parse_csv_env(
    "CORS_ALLOWED_ORIGINS",
    DEV_CORS_ORIGINS if APP_ENV != "production" else PROD_CORS_ORIGINS,
)
ALLOW_NULL_ORIGIN = _parse_bool_env("CORS_ALLOW_NULL_ORIGIN", APP_ENV != "production")
MOBILE_ONLY_ENFORCED = _parse_bool_env("MOBILE_ONLY_ENFORCED", False)
MOBILE_TELEGRAM_PLATFORMS = {"android", "ios", "ipados"}
DESKTOP_TELEGRAM_PLATFORMS = {
    "tdesktop",
    "weba",
    "webk",
    "web",
    "macos",
    "windows",
    "linux",
    "unigram",
}
MOBILE_USER_AGENT_RE = re.compile(
    r"(android|iphone|ipad|ipod|mobile|windows phone)", re.IGNORECASE
)
DESKTOP_USER_AGENT_RE = re.compile(
    r"(windows nt|macintosh|x11|cros|linux x86_64)", re.IGNORECASE
)


# Single lightweight reconnect helper to avoid code duplication
async def try_reconnect_redis() -> None:
    global redis_client
    if not REDIS_URL or redis_client is not None:
        return
    client = redis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        db=REDIS_DB,
        socket_timeout=2,
        socket_connect_timeout=2,
        retry_on_timeout=True,
    )
    try:
        await client.ping()
        redis_client = client
        logger.info("вњ“ Redis reconnected")
    except Exception as e:
        logger.warning(f"Redis reconnect failed: {e}")
        redis_client = None


async def get_redis_or_none() -> redis.Redis | None:
    """
    Best-effort Redis with single reconnect attempt. No exceptions.
    """
    if redis_client is None:
        await try_reconnect_redis()
    return redis_client


async def redis_or_503() -> redis.Redis:
    """
    Strong guarantee: return redis connection or raise 503.
    """
    conn = await get_redis_or_none()
    if conn is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return conn


async def touch_online_user(user_id: int) -> int:
    conn = await get_redis_or_none()
    if conn is None:
        return 0

    now_ts = int(time.time())
    cutoff = now_ts - ONLINE_WINDOW_SECONDS
    try:
        await conn.zadd(ONLINE_USERS_KEY, {str(user_id): now_ts})
        await conn.zremrangebyscore(ONLINE_USERS_KEY, 0, cutoff)
        online_now = await conn.zcount(ONLINE_USERS_KEY, cutoff, "+inf")
        await conn.expire(ONLINE_USERS_KEY, ONLINE_WINDOW_SECONDS * 2)
        return int(online_now or 0)
    except Exception as e:
        logger.warning(f"Online heartbeat failed: {e}")
        return 0


async def get_online_users_count() -> int:
    conn = await get_redis_or_none()
    if conn is None:
        return 0

    now_ts = int(time.time())
    cutoff = now_ts - ONLINE_WINDOW_SECONDS
    try:
        await conn.zremrangebyscore(ONLINE_USERS_KEY, 0, cutoff)
        online_now = await conn.zcount(ONLINE_USERS_KEY, cutoff, "+inf")
        return int(online_now or 0)
    except Exception as e:
        logger.warning(f"Online count fetch failed: {e}")
        return 0


async def create_telegram_stars_invoice_link(
    *, user_id: int, skin_id: str, price: int
) -> str:
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="Bot token not configured")

    payload = f"stars_skin:{user_id}:{skin_id}"
    request_body = {
        "title": f"Skin {skin_id}",
        "description": f"Unlock premium skin {skin_id}",
        "payload": payload,
        "currency": "XTR",
        "prices": [{"label": skin_id, "amount": price}],
        "provider_token": "",
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createInvoiceLink",
            json=request_body,
        )

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail="Telegram invoice creation failed")

    data = response.json()
    if not data.get("ok") or not data.get("result"):
        raise HTTPException(status_code=502, detail="Telegram invoice creation failed")

    return data["result"]


async def verify_telegram_channel_subscription(user_id: int) -> bool:
    return await verify_telegram_channel_subscription_service(
        user_id, _build_tasks_rewards_service_deps()
    )


async def send_telegram_wallet_reminder_message(
    *,
    user_id: int,
    season_key: str,
    league: str,
    hours_until_deadline: int,
) -> tuple[bool, str | None]:
    if not BOT_TOKEN:
        return False, "Bot token not configured"

    league_label = league.title()
    deadline_text = f"{int(hours_until_deadline)} часов"
    reminder_text = (
        "Ты попал в турнирные выплаты.\n\n"
        f"Лига: {league_label}\n"
        f"Сезон: {season_key}\n\n"
        f"Подключи TON-кошелёк в течение {deadline_text}, иначе выплата не будет отправлена."
    )
    reply_markup = {
        "inline_keyboard": [
            [
                {
                    "text": "Открыть игру",
                    "web_app": {"url": GAME_WEBAPP_URL},
                }
            ],
            [
                {
                    "text": "Открыть бота",
                    "url": f"https://t.me/{TELEGRAM_BOT_USERNAME}",
                }
            ],
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": int(user_id),
                    "text": reminder_text,
                    "reply_markup": reply_markup,
                },
            )
        payload = response.json() if response.content else {}
        if response.status_code != 200 or not payload.get("ok"):
            return False, str(
                (payload or {}).get("description") or f"HTTP {response.status_code}"
            )
        return True, None
    except Exception as exc:
        return False, str(exc)


def extract_first_value(source: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = source.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


async def mark_ad_action_session_verified(
    ad_session_id: str, postback_payload: dict
) -> bool:
    return await mark_ad_action_session_verified_service(
        ad_session_id, postback_payload, _build_ads_boosts_service_deps()
    )


async def mark_latest_ad_action_session_verified_for_user(
    user_id: int, postback_payload: dict
) -> str | None:
    return await mark_latest_ad_action_session_verified_for_user_service(
        user_id, postback_payload, _build_ads_boosts_service_deps()
    )


async def consume_ad_action_session(
    user_id: int, ad_session_id: str, expected_action: str
) -> dict:
    return await consume_ad_action_session_service(
        user_id, ad_session_id, expected_action, _build_ads_boosts_service_deps()
    )


def build_crash_ghost_session(bet: int, user_id: int) -> dict:
    crash_at = round(1.85 + random.random() * 4.15, 2)
    if random.random() < 0.1:
        crash_at = round(5.8 + random.random() * 2.6, 2)

    now_ts = time.time()
    crash_after_seconds = max(0.85, (crash_at - 1.0) / CRASH_GHOST_MULTIPLIER_SPEED)

    return {
        "user_id": user_id,
        "bet": bet,
        "started_at": now_ts,
        "crash_at": crash_at,
        "crash_after_seconds": crash_after_seconds,
        "claimed": False,
    }


def get_crash_ghost_runtime(session: dict, now_ts: float | None = None) -> dict:
    now_ts = now_ts or time.time()
    started_at = float(session.get("started_at") or now_ts)
    crash_after_seconds = float(session.get("crash_after_seconds") or 0)
    crash_at = float(session.get("crash_at") or 1.0)
    elapsed = max(0.0, now_ts - started_at)
    crashed = elapsed >= crash_after_seconds
    multiplier = (
        crash_at if crashed else round(1.0 + elapsed * CRASH_GHOST_MULTIPLIER_SPEED, 2)
    )
    multiplier = max(1.0, min(multiplier, crash_at))

    return {
        "elapsed_seconds": elapsed,
        "crashed": crashed,
        "multiplier": multiplier,
        "crash_at": crash_at,
    }


# ==================== METRICS ====================
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
REDIS_ERRORS = Counter("redis_errors_total", "Redis operation errors")
DB_ERRORS = Counter("db_errors_total", "Database operation errors")
RATE_LIMIT_REJECTS = Counter(
    "rate_limit_rejects_total",
    "Rate-limit rejections",
    ["namespace"],
)
# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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


def is_mobile_game_client_request(request: Request) -> bool:
    platform = (request.headers.get("X-Telegram-Platform", "") or "").strip().lower()
    if platform in DESKTOP_TELEGRAM_PLATFORMS:
        return False
    if platform in MOBILE_TELEGRAM_PLATFORMS:
        return True

    sec_mobile = (request.headers.get("sec-ch-ua-mobile", "") or "").strip().lower()
    if sec_mobile == "?1":
        return True

    client_mobile_header = (
        (request.headers.get("X-Client-Mobile", "") or "").strip().lower()
    )
    client_mobile = client_mobile_header in {"1", "true", "yes", "on"}

    user_agent = request.headers.get("user-agent", "") or ""
    ua_is_mobile = bool(MOBILE_USER_AGENT_RE.search(user_agent))
    ua_is_desktop = bool(DESKTOP_USER_AGENT_RE.search(user_agent))

    if client_mobile and not ua_is_desktop:
        return True

    return ua_is_mobile and not ua_is_desktop


def ensure_mobile_only_game_access(request: Request) -> None:
    if not MOBILE_ONLY_ENFORCED:
        return
    if is_mobile_game_client_request(request):
        return
    raise HTTPException(
        status_code=403,
        detail="This game is available only on mobile devices inside Telegram",
    )


async def require_telegram_user(
    request: Request, expected_user_id: int | None = None
) -> dict:
    ensure_mobile_only_game_access(request)
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


async def require_admin_access(request: Request) -> dict:
    admin_token = (request.headers.get("X-Admin-Token", "") or "").strip()
    if ADMIN_DASHBOARD_TOKEN and admin_token == ADMIN_DASHBOARD_TOKEN:
        return {"auth": "token"}

    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    telegram_user_id = int(telegram_user.get("id", 0))
    if telegram_user_id not in ADMIN_TELEGRAM_IDS:
        raise HTTPException(status_code=403, detail="Admin access required")
    return telegram_user


def format_int(value: int) -> str:
    return f"{int(value or 0):,}".replace(",", " ")


def format_duration(seconds: int) -> str:
    total = max(0, int(seconds or 0))
    minutes, secs = divmod(total, 60)
    return f"{minutes}:{secs:02d}"


async def get_rewarded_ad_user_counts(
    user_ids: list[int], *, hours: int
) -> dict[int, int]:
    if not user_ids:
        return {}
    since = datetime.utcnow() - timedelta(hours=max(1, int(hours or 1)))
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(RewardedAdClaim.user_id, func.count(RewardedAdClaim.id))
            .where(
                RewardedAdClaim.user_id.in_(user_ids),
                RewardedAdClaim.created_at >= since,
            )
            .group_by(RewardedAdClaim.user_id)
        )
        return {int(user_id): int(count or 0) for user_id, count in result.all()}


async def build_admin_fraud_overview(season_key: str) -> list[dict]:
    now = datetime.utcnow()
    async with AsyncSessionLocal() as session:
        entries_result = await session.execute(
            select(WeeklyTournamentEntry)
            .where(WeeklyTournamentEntry.season_key == season_key)
            .order_by(
                WeeklyTournamentEntry.fraud_flag.desc(),
                WeeklyTournamentEntry.score.desc(),
            )
            .limit(200)
        )
        entries = entries_result.scalars().all()

        user_ids = [int(entry.user_id) for entry in entries]
        if not user_ids:
            return []

        users_result = await session.execute(
            select(User).where(User.user_id.in_(user_ids))
        )
        users_map = {int(user.user_id): user for user in users_result.scalars().all()}

        referrer_ids = sorted(
            {
                int(user.referrer_id)
                for user in users_map.values()
                if getattr(user, "referrer_id", None)
            }
        )
        referrer_cluster_counts: dict[int, int] = {}
        if referrer_ids:
            cluster_result = await session.execute(
                select(User.referrer_id, func.count(User.id))
                .where(User.referrer_id.in_(referrer_ids))
                .group_by(User.referrer_id)
            )
            referrer_cluster_counts = {
                int(referrer_id): int(count or 0)
                for referrer_id, count in cluster_result.all()
                if referrer_id is not None
            }

    reviews_map = await get_admin_fraud_reviews(user_ids)
    recent_ads_1h = await get_rewarded_ad_user_counts(user_ids, hours=1)
    recent_ads_24h = await get_rewarded_ad_user_counts(user_ids, hours=24)

    suspicious_rows = []
    for entry in entries:
        user = users_map.get(int(entry.user_id))
        if user is None:
            continue

        account_age_hours = max(
            0.0, (now - (user.created_at or now)).total_seconds() / 3600
        )
        review = reviews_map.get(int(entry.user_id), {})
        reasons: list[str] = []
        extra = parse_extra_data(getattr(user, "extra_data", {}))
        click_guard = get_click_guard_state(extra)

        if review.get("status") == "fraud":
            reasons.append(review.get("reason") or "Manual fraud review")

        if entry.display_level >= 100 and account_age_hours < 72:
            reasons.append("Too fast level growth to Diamond")
        elif entry.display_level >= 66 and account_age_hours < 24:
            reasons.append("Too fast level growth to Gold")
        elif entry.display_level >= 33 and account_age_hours < 8:
            reasons.append("Too fast level growth to Silver")

        ads_1h = int(recent_ads_1h.get(int(entry.user_id), 0))
        ads_24h = int(recent_ads_24h.get(int(entry.user_id), 0))
        if ads_1h >= 25 or ads_24h >= 120:
            reasons.append(
                f"Too many rewarded ads in a short period ({ads_1h}/1h, {ads_24h}/24h)"
            )

        score_per_hour = int((entry.score or 0) / max(account_age_hours, 1))
        if score_per_hour >= 500000:
            reasons.append(
                f"Unusually fast click income velocity ({format_int(score_per_hour)} per hour)"
            )

        click_suspicion_score = int(click_guard.get("suspicion_score", 0) or 0)
        hard_rejections = int(click_guard.get("hard_rejections", 0) or 0)
        if click_suspicion_score >= CLICK_SUSPICION_SOFT_LIMIT:
            reasons.append(
                f"Suspicious click batches detected (score {click_suspicion_score})"
            )
        if hard_rejections > 0:
            reasons.append(
                f"Server rejected suspicious click bursts ({hard_rejections})"
            )

        referrer_id = getattr(user, "referrer_id", None)
        if (
            referrer_id
            and referrer_cluster_counts.get(int(referrer_id), 0) >= 5
            and account_age_hours <= 72
        ):
            reasons.append("Possible multi-account referral cluster")

        is_flagged = (
            bool(entry.fraud_flag) or review.get("status") == "fraud" or bool(reasons)
        )
        if not is_flagged:
            continue

        suspicious_rows.append(
            {
                "user_id": int(entry.user_id),
                "username": entry.username or getattr(user, "username", None),
                "display_level": int(entry.display_level or 1),
                "league": entry.league,
                "score": int(entry.score or 0),
                "eligible_for_payout": bool(entry.eligible_for_payout),
                "fraud_flag": bool(entry.fraud_flag) or review.get("status") == "fraud",
                "manual_status": review.get("status", "ok"),
                "manual_reason": review.get("reason"),
                "disqualify_from_payout": bool(review.get("disqualify_from_payout")),
                "account_age_hours": round(account_age_hours, 1),
                "rewarded_ads_1h": ads_1h,
                "rewarded_ads_24h": ads_24h,
                "reasons": reasons,
            }
        )

    suspicious_rows.sort(
        key=lambda item: (
            not item["fraud_flag"],
            not item["disqualify_from_payout"],
            -item["score"],
        )
    )
    return suspicious_rows


# ==================== РўРЈР РќРР РќР«Р• Р”РђРќРќР«Р• ==================


# Hot-state fields that must NOT be stored in user:cache.
# These are authoritative from Redis hot keys (energy:v2, coins_hot, etc.)
_USER_CACHE_EXCLUDE_FIELDS = frozenset(
    {
        "energy",
        "max_energy",
        "coins",
        "last_energy_update",
        "last_passive_income",
        "profit_per_hour",
        "profit_per_tap",
    }
)


async def get_user_cached(user_id: int) -> dict | None:
    conn = await get_redis_or_none()
    if conn:
        try:
            cached = await conn.get(f"{USER_CACHE_PREFIX}{user_id}")
            if cached:
                try:
                    return json.loads(cached)
                except Exception:
                    pass
        except Exception as e:
            logger.warning("User cache read failed for %s: %s", user_id, e)

    user = await get_user(user_id)
    if not user:
        return None

    conn = await get_redis_or_none()
    if conn:
        try:
            # Filter out hot-state fields before caching
            cache_data = {
                k: v for k, v in user.items() if k not in _USER_CACHE_EXCLUDE_FIELDS
            }
            await conn.setex(
                f"{USER_CACHE_PREFIX}{user_id}",
                USER_CACHE_TTL,
                json.dumps(cache_data, default=str),
            )
        except Exception as e:
            logger.warning("User cache write failed for %s: %s", user_id, e)

    return user


# ==================== Р’СЃРїРѕРјРѕРіР°С‚РµР»СЊРЅС‹Рµ С„СѓРЅРєС†РёРё Р°РЅС‚РёСЃРїР°РјР° ====================


async def invalidate_user_cache(user_id: int):
    conn = await get_redis_or_none()
    if conn:
        await conn.delete(f"{USER_CACHE_PREFIX}{user_id}")


def parse_extra_data(extra_raw) -> dict:
    if isinstance(extra_raw, dict):
        return extra_raw
    if isinstance(extra_raw, str) and extra_raw:
        try:
            return json.loads(extra_raw)
        except Exception:
            return {}
    return {}


def parse_iso_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def get_click_guard_state(extra: dict) -> dict:
    click_guard = extra.get("click_guard", {})
    if not isinstance(click_guard, dict):
        click_guard = {}
    return click_guard


def write_click_guard_state(extra: dict, click_guard: dict) -> dict:
    extra["click_guard"] = click_guard
    return extra


def get_skin_ad_progress(extra: dict) -> dict:
    value = extra.get("skin_ad_progress", {})
    return value if isinstance(value, dict) else {}


def get_skin_ad_last_watch(extra: dict) -> dict:
    value = extra.get("skin_ad_last_watch", {})
    return value if isinstance(value, dict) else {}


def serialize_db_field(field: str, value):
    if field == "extra_data" and value is not None and not isinstance(value, str):
        return json.dumps(value)
    return value


def get_ad_action_active_session_key(user_id: int) -> str:
    return f"adsession:user:active:{int(user_id)}"


async def update_user_if_matches(user_id: int, expected: dict, data: dict):
    allowed_fields = {
        "username",
        "coins",
        "profit_per_hour",
        "profit_per_tap",
        "energy",
        "max_energy",
        "level",
        "multitap_level",
        "profit_level",
        "energy_level",
        "boost_level",
        "rebirth_count",
        "last_passive_income",
        "last_energy_update",
        "referrer_id",
        "referral_count",
        "referral_earnings",
        "extra_data",
        "luck_level",
    }
    unknown_fields = (set(expected) | set(data)) - allowed_fields
    if unknown_fields:
        raise ValueError(f"Unsupported atomic update fields: {sorted(unknown_fields)}")

    where_clauses = [User.user_id == user_id]
    for field, raw_value in expected.items():
        value = serialize_db_field(field, raw_value)
        column = getattr(User, field)
        if value is None:
            where_clauses.append(column.is_(None))
        else:
            where_clauses.append(column == value)

    values = {
        field: serialize_db_field(field, raw_value) for field, raw_value in data.items()
    }
    expected_coins = expected.get("coins")
    new_coins = data.get("coins")
    coin_delta = None
    if expected_coins is not None and new_coins is not None:
        try:
            coin_delta = int(new_coins) - int(expected_coins)
        except Exception:
            coin_delta = None

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            update(User).where(*where_clauses).values(**values)
        )
        if result.rowcount != 1:
            await session.rollback()
            return None
        await session.commit()
    if coin_delta and coin_delta > 0:
        await sync_hot_after_db_increment(user_id, int(coin_delta), int(new_coins))
    elif coin_delta and coin_delta < 0:
        await sync_hot_after_db_decrement(user_id, int(-coin_delta), int(new_coins))

    return await get_user(user_id)


async def apply_atomic_user_updates(
    user_id: int,
    current_user: dict,
    updates: dict,
    *,
    expected_fields: tuple[str, ...] | None = None,
    conflict_detail: str = "User state changed, retry",
):
    fields = expected_fields or tuple(
        field
        for field in (
            "coins",
            "energy",
            "last_energy_update",
            "extra_data",
            "last_passive_income",
            "referral_earnings",
        )
        if field in updates
    )
    expected = {field: current_user.get(field) for field in fields}
    updated_user = await update_user_if_matches(user_id, expected, updates)
    if not updated_user:
        raise HTTPException(status_code=409, detail=conflict_detail)
    await invalidate_user_cache(user_id)
    return updated_user


async def complete_task_reward_atomically(
    user_id: int, task_id: str, user_updates: dict | None = None
) -> dict:
    return await complete_task_reward_atomically_service(
        user_id, task_id, user_updates, _build_tasks_rewards_service_deps()
    )


def resolve_video_task_coin_drop() -> int:
    return resolve_video_task_coin_drop_service()


def get_video_task_last_claims(extra: dict) -> dict:
    return get_video_task_last_claims_service(extra)


def get_video_task_boosts(extra: dict) -> dict:
    return get_video_task_boosts_service(extra)


def get_active_video_task_boost(
    extra: dict, boost_key: str
) -> tuple[bool, str | None, int]:
    return get_active_video_task_boost_service(
        extra, boost_key, _build_tasks_rewards_service_deps()
    )


async def touch_user_activity(user_id: int, user: dict | None = None) -> None:
    user_data = user or await get_user_cached(user_id)
    if not user_data:
        return

    extra = parse_extra_data(user_data.get("extra_data"))
    previous_activity = extra.get("last_activity_at")
    previous_stage = int(extra.get("push_idle_stage", 0) or 0)
    now = datetime.utcnow()

    if previous_activity:
        try:
            prev_dt = datetime.fromisoformat(previous_activity)
            if (now - prev_dt).total_seconds() < 120 and previous_stage == 0:
                return
        except Exception:
            pass
    now_iso = now.isoformat()

    extra["last_activity_at"] = now_iso
    extra["push_idle_stage"] = 0
    extra["last_push_reason"] = None

    await update_user(user_id, {"extra_data": extra})


# ==================== МОДЕЛИ ====================


# ==================== РњРћР”Р•Р›Р ====================


# ==================== Р’РЎРџРћРњРћР“РђРўР•Р›Р¬РќР«Р• Р¤РЈРќРљР¦РР ====================
def normalize_diagnostics_path(path: str) -> str:
    normalized = str(path or "/")
    normalized = re.sub(r"/\d{4}-\d{2}-\d{2}(?=/|$)", "/{season_key}", normalized)
    normalized = re.sub(r"/-?\d+(?=/|$)", "/{id}", normalized)
    return normalized


def record_endpoint_diagnostic(
    method: str, path: str, status_code: int, duration_seconds: float
) -> None:
    if path in {"/metrics", "/health"} or path.startswith("/api/admin/diagnostics"):
        return

    normalized_path = normalize_diagnostics_path(path)
    key = (str(method or "GET").upper(), normalized_path)
    stats = ENDPOINT_DIAGNOSTICS.get(key)
    if stats is None:
        stats = {
            "method": key[0],
            "path": normalized_path,
            "requests": 0,
            "errors": 0,
            "status_counts": defaultdict(int),
            "durations_ms": deque(maxlen=DIAGNOSTICS_DURATION_WINDOW),
            "last_error": None,
            "last_error_at": None,
            "last_status": None,
            "last_duration_ms": None,
        }
        ENDPOINT_DIAGNOSTICS[key] = stats

    duration_ms = max(0.0, float(duration_seconds or 0.0) * 1000.0)
    stats["requests"] += 1
    stats["status_counts"][int(status_code or 0)] += 1
    stats["durations_ms"].append(duration_ms)
    stats["last_status"] = int(status_code or 0)
    stats["last_duration_ms"] = round(duration_ms, 2)

    if int(status_code or 0) >= 400:
        stats["errors"] += 1
        stats["last_error"] = f"HTTP {int(status_code or 0)}"
        stats["last_error_at"] = datetime.utcnow().isoformat()
        RECENT_DIAGNOSTIC_ERRORS.appendleft(
            {
                "method": stats["method"],
                "path": normalized_path,
                "status": int(status_code or 0),
                "at": stats["last_error_at"],
                "duration_ms": round(duration_ms, 2),
            }
        )


def percentile_from_sorted(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    rank = max(0, min(len(values) - 1, int(round((len(values) - 1) * percentile))))
    return float(values[rank])


def serialize_endpoint_diagnostic(stats: dict) -> dict:
    durations = sorted(float(item) for item in stats.get("durations_ms", []))
    status_counts = stats.get("status_counts", {}) or {}
    return {
        "method": stats.get("method"),
        "path": stats.get("path"),
        "requests": int(stats.get("requests", 0)),
        "errors": int(stats.get("errors", 0)),
        "status_2xx": int(
            sum(
                count for code, count in status_counts.items() if 200 <= int(code) < 300
            )
        ),
        "status_4xx": int(
            sum(
                count for code, count in status_counts.items() if 400 <= int(code) < 500
            )
        ),
        "status_429": int(status_counts.get(429, 0)),
        "status_5xx": int(
            sum(
                count for code, count in status_counts.items() if 500 <= int(code) < 600
            )
        ),
        "avg_ms": round(sum(durations) / len(durations), 2) if durations else 0.0,
        "p95_ms": round(percentile_from_sorted(durations, 0.95), 2)
        if durations
        else 0.0,
        "p99_ms": round(percentile_from_sorted(durations, 0.99), 2)
        if durations
        else 0.0,
        "last_status": stats.get("last_status"),
        "last_duration_ms": stats.get("last_duration_ms"),
        "last_error": stats.get("last_error"),
        "last_error_at": stats.get("last_error_at"),
    }


async def acquire_once_lock(key: str, ttl: float = 10) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            ttl_ms = max(1, int(float(ttl) * 1000))
            result = await conn.set(key, "1", px=ttl_ms, nx=True)
            return bool(result)
        except Exception as e:
            logger.warning(f"Redis acquire_once_lock failed, fallback to local: {e}")

    now = time.monotonic()
    expires_at = LOCAL_LOCKS.get(key)
    if expires_at and expires_at > now:
        return False

    LOCAL_LOCKS[key] = now + ttl
    return True


async def acquire_idempotency_key(key: str, ttl: int = 60) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            result = await conn.set(key, "1", ex=ttl, nx=True)
            return bool(result)
        except Exception as e:
            logger.warning(
                f"Redis acquire_idempotency_key failed, fallback to local: {e}"
            )

    now = time.monotonic()
    expires_at = LOCAL_IDEMPOTENCY_KEYS.get(key)
    if expires_at and expires_at > now:
        return False

    LOCAL_IDEMPOTENCY_KEYS[key] = now + ttl
    return True


async def require_user_action_lock(namespace: str, user_id: int, ttl: float = 5):
    lock_key = f"lock:{namespace}:{user_id}"
    locked = await acquire_once_lock(lock_key, ttl=ttl)
    if not locked:
        raise HTTPException(status_code=429, detail="Action already in progress")


async def ensure_redis_available() -> redis.Redis:
    """
    Try reconnect once and guarantee redis_client or raise 503.
    """
    return await redis_or_503()


# ==================== Р­РќР”РџРћРРќРўР« ====================


def _local_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    now = time.monotonic()
    bucket = LOCAL_RATE_LIMITS_STATE[key]

    while bucket and (now - bucket[0]) >= window_seconds:
        bucket.popleft()

    if len(bucket) >= limit:
        return False

    bucket.append(now)
    return True


DUAL_RATE_LIMIT_LUA = """
local user_key = KEYS[1]
local ip_key = KEYS[2]
local user_limit = tonumber(ARGV[1])
local ip_limit = tonumber(ARGV[2])
local window_seconds = tonumber(ARGV[3])

local user_current = redis.call('INCR', user_key)
if user_current == 1 then
    redis.call('EXPIRE', user_key, window_seconds)
end
if user_current > user_limit then
    return {0, user_current, -1}
end

local ip_current = redis.call('INCR', ip_key)
if ip_current == 1 then
    redis.call('EXPIRE', ip_key, window_seconds)
end
if ip_current > ip_limit then
    return {2, user_current, ip_current}
end

return {1, user_current, ip_current}
"""


async def redis_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """
    True = РјРѕР¶РЅРѕ РїСЂРѕРїСѓСЃС‚РёС‚СЊ
    False = Р»РёРјРёС‚ РїСЂРµРІС‹С€РµРЅ
    """
    global redis_client

    conn = await get_redis_or_none()

    if conn is None:
        return _local_rate_limit(key, limit, window_seconds)

    try:
        current = await conn.incr(key)
        if current == 1:
            await conn.expire(key, window_seconds)
        return current <= limit
    except Exception as e:
        logger.warning(f"Redis rate_limit failed, fallback to local: {e}")
        REDIS_ERRORS.inc()
        redis_client = None
        return _local_rate_limit(key, limit, window_seconds)


async def redis_dual_rate_limit(
    user_key: str,
    ip_key: str,
    user_limit: int,
    ip_limit: int,
    window_seconds: int,
) -> tuple[bool, bool]:
    """
    Returns tuple:
      (user_allowed, ip_allowed)
    Semantics intentionally match sequential checks:
      1) user check first; if denied -> ip is not checked
      2) if user allowed -> ip check and decide
    """
    global redis_client

    conn = await get_redis_or_none()
    if conn is None:
        user_allowed = _local_rate_limit(user_key, user_limit, window_seconds)
        if not user_allowed:
            return False, True
        ip_allowed = _local_rate_limit(ip_key, ip_limit, window_seconds)
        return user_allowed, ip_allowed

    try:
        result = await conn.eval(
            DUAL_RATE_LIMIT_LUA,
            2,
            user_key,
            ip_key,
            str(user_limit),
            str(ip_limit),
            str(window_seconds),
        )
        code = int(result[0])
        if code == 1:
            return True, True
        if code == 0:
            return False, True
        return True, False
    except Exception as e:
        logger.warning(f"Redis dual_rate_limit failed, fallback to local: {e}")
        REDIS_ERRORS.inc()
        redis_client = None
        user_allowed = _local_rate_limit(user_key, user_limit, window_seconds)
        if not user_allowed:
            return False, True
        ip_allowed = _local_rate_limit(ip_key, ip_limit, window_seconds)
        return user_allowed, ip_allowed


async def require_redis_rate_limit(
    namespace: str, user_id: int, limit: int, window_seconds: int
):
    allowed = await redis_rate_limit(f"rl:{namespace}:{user_id}", limit, window_seconds)
    if not allowed:
        RATE_LIMIT_REJECTS.labels(namespace=namespace).inc()
        raise HTTPException(status_code=429, detail="Too many requests")


def get_request_ip(request: Request) -> str:
    forwarded_ip = (request.headers.get("cf-connecting-ip") or "").strip()
    if forwarded_ip:
        return forwarded_ip

    x_forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()

    return (request.client.host if request.client else "") or "unknown"


@router.post("/api/auth/session")
async def create_api_session(request: Request):
    ensure_mobile_only_game_access(request)
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


@router.post("/api/debug/session")
async def create_debug_session(payload: UserIdRequest):
    if APP_ENV != "staging":
        raise HTTPException(status_code=404, detail="Not found")

    user_id = int(payload.user_id)
    username = None
    try:
        user = await get_user(user_id)
        if user:
            username = user.get("username")
    except Exception:
        username = None

    token, _expires_at = issue_session_token({"id": user_id, "username": username})
    return {"token": token}


async def require_ip_rate_limit(
    namespace: str, request: Request, limit: int, window_seconds: int
):
    request_ip = get_request_ip(request)
    allowed = await redis_rate_limit(
        f"rl:{namespace}:ip:{request_ip}", limit, window_seconds
    )
    if not allowed:
        RATE_LIMIT_REJECTS.labels(namespace=f"{namespace}_ip").inc()
        raise HTTPException(status_code=429, detail="Too many requests from this IP")


async def require_dual_rate_limit(
    namespace: str,
    request: Request,
    user_id: int,
    user_limit: int,
    window_seconds: int,
    *,
    ip_limit: int | None = None,
):
    request_ip = get_request_ip(request)
    user_key = f"rl:{namespace}:{user_id}"
    ip_key = f"rl:{namespace}:ip:{request_ip}"
    effective_ip_limit = ip_limit or user_limit

    user_allowed, ip_allowed = await redis_dual_rate_limit(
        user_key,
        ip_key,
        user_limit,
        effective_ip_limit,
        window_seconds,
    )
    if not user_allowed:
        RATE_LIMIT_REJECTS.labels(namespace=namespace).inc()
        raise HTTPException(status_code=429, detail="Too many requests")
    if not ip_allowed:
        RATE_LIMIT_REJECTS.labels(namespace=f"{namespace}_ip").inc()
        raise HTTPException(status_code=429, detail="Too many requests from this IP")


async def flush_click_buffer_loop():
    while True:
        try:
            conn = await get_redis_or_none()
            if not conn:
                await asyncio.sleep(5)
                continue

            data = await conn.hgetall(CLICK_BUFFER_KEY)

            if not data:
                await asyncio.sleep(5)
                continue

            for user_id, coins in data.items():
                user_id = int(user_id)
                coins = int(coins)

                if coins <= 0:
                    continue

                user = await get_user(user_id)
                if not user:
                    continue

                new_coins = int(user.get("coins", 0)) + coins

                await update_user(user_id, {"coins": new_coins})
                # NOTE: Not invalidating cache — coins is a hot-state field
                # excluded from user:cache. Cache only stores static profile data.

            await conn.delete(CLICK_BUFFER_KEY)

            logger.info(f"Flushed {len(data)} users from Redis buffer")

        except Exception as e:
            logger.error(f"Flush error: {e}")

        await asyncio.sleep(5)


@router.get("/api/user/{user_id}")
async def get_user_data(user_id: int, request: Request):
    return await get_user_data_service(
        user_id,
        request,
        _build_profile_state_service_deps(),
        ensure_coins_hot_initialized=ensure_coins_hot_initialized,
    )


@router.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int, request: Request):
    """Get mega boost status"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()

        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(
                    active_boosts["mega_boost"]["expires_at"]
                )
                if now > expires:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
                    await invalidate_user_cache(user_id)
                    # NOTE: Not invalidating cache — boost state is derived from
                    # expires_at in extra_data, read from DB in realtime assembler.
                    cooldown_until = parse_iso_datetime(
                        extra.get("mega_boost_cooldown_until")
                    )
                    if cooldown_until and cooldown_until > now:
                        return {
                            "active": False,
                            "cooldown_active": True,
                            "cooldown_until": cooldown_until.isoformat(),
                            "cooldown_remaining_seconds": int(
                                (cooldown_until - now).total_seconds()
                            ),
                        }
                    return {"active": False, "cooldown_active": False}
                else:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"],
                        "remaining_seconds": remaining,
                    }
            except:
                pass

        cooldown_until = parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
        if cooldown_until and cooldown_until > now:
            return {
                "active": False,
                "cooldown_active": True,
                "cooldown_until": cooldown_until.isoformat(),
                "cooldown_remaining_seconds": int(
                    (cooldown_until - now).total_seconds()
                ),
            }
        if cooldown_until and cooldown_until <= now:
            extra.pop("mega_boost_cooldown_until", None)
            await update_user(user_id, {"extra_data": extra})
            await invalidate_user_cache(user_id)
            # NOTE: Not invalidating cache — boost state is derived from
            # expires_at in extra_data, read from DB in realtime assembler.

        return {"active": False, "cooldown_active": False}
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/activate-mega-boost")
async def activate_mega_boost(payload: AdActionClaimRequest, request: Request):
    return await activate_mega_boost_service(
        payload, request, _build_ads_boosts_service_deps()
    )


@router.get("/api/ghost-boost-status/{user_id}")
async def get_ghost_boost_status_endpoint(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        active, expires_at = get_ghost_boost_status(user)
        if not active or not expires_at:
            return {"active": False}

        remaining = max(
            0,
            int(
                (datetime.fromisoformat(expires_at) - datetime.utcnow()).total_seconds()
            ),
        )
        return {
            "active": True,
            "expires_at": expires_at,
            "remaining_seconds": remaining,
            "multiplier": GHOST_BOOST_MULTIPLIER,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ghost_boost_status_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/activate-ghost-boost")
async def activate_ghost_boost(payload: AdActionClaimRequest, request: Request):
    return await activate_ghost_boost_service(
        payload, request, _build_ads_boosts_service_deps()
    )


@router.post("/api/ad-action/start")
async def ad_action_start(payload: AdActionStartRequest, request: Request):
    return await ad_action_start_service(
        payload, request, _build_ads_boosts_service_deps()
    )


@router.post("/api/ads/adsgram/complete")
async def adsgram_complete_locally(payload: AdActionClaimRequest, request: Request):
    return await adsgram_complete_locally_service(
        payload, request, _build_ads_boosts_service_deps()
    )


@router.api_route("/api/ads/monetag/postback", methods=["GET", "POST"])
async def monetag_postback(request: Request):
    try:
        params = {str(k): str(v) for k, v in request.query_params.items()}

        if request.method == "POST":
            content_type = (request.headers.get("content-type") or "").lower()
            if (
                "application/x-www-form-urlencoded" in content_type
                or "multipart/form-data" in content_type
            ):
                form = await request.form()
                for key, value in form.items():
                    params[str(key)] = str(value)
            elif "application/json" in content_type:
                payload = await request.json()
                if isinstance(payload, dict):
                    for key, value in payload.items():
                        params[str(key)] = str(value)

        if MONETAG_POSTBACK_SECRET:
            provided_secret = extract_first_value(params, MONETAG_POSTBACK_SECRET_KEYS)
            if provided_secret != MONETAG_POSTBACK_SECRET:
                logger.warning("Monetag postback rejected: invalid secret")
                raise HTTPException(status_code=403, detail="Invalid postback secret")

        status_hints = [
            params.get("status"),
            params.get("state"),
            params.get("event"),
            params.get("result"),
            params.get("rewarded"),
            params.get("completed"),
        ]
        negative_status = any(
            hint is not None
            and str(hint).strip().lower() in MONETAG_POSTBACK_NEGATIVE_VALUES
            for hint in status_hints
        )
        if negative_status:
            logger.info("Monetag postback ignored as incomplete: %s", params)
            return Response(content="IGNORED", media_type="text/plain", status_code=200)

        ad_session_id = extract_first_value(params, MONETAG_POSTBACK_ID_KEYS)
        if not ad_session_id:
            logger.debug("Monetag postback without session id ignored: %s", params)
            return Response(content="IGNORED", media_type="text/plain", status_code=200)

        verified = await mark_ad_action_session_verified(ad_session_id, params)
        if not verified:
            logger.warning(
                "Monetag postback could not find ad session %s", ad_session_id
            )
            return Response(
                content="SESSION_NOT_FOUND", media_type="text/plain", status_code=404
            )

        return Response(content="OK", media_type="text/plain", status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in monetag_postback: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.api_route("/api/ads/adsgram/reward", methods=["GET", "POST"])
async def adsgram_reward_callback(request: Request):
    try:
        params = {str(k): str(v) for k, v in request.query_params.items()}

        if request.method == "POST":
            content_type = (request.headers.get("content-type") or "").lower()
            if (
                "application/x-www-form-urlencoded" in content_type
                or "multipart/form-data" in content_type
            ):
                form = await request.form()
                for key, value in form.items():
                    params[str(key)] = str(value)
            elif "application/json" in content_type:
                payload = await request.json()
                if isinstance(payload, dict):
                    for key, value in payload.items():
                        params[str(key)] = str(value)

        if ADSGRAM_REWARD_ENFORCED:
            provided_secret = extract_first_value(params, ADSGRAM_REWARD_SECRET_KEYS)
            if provided_secret != ADSGRAM_REWARD_SECRET:
                logger.warning("AdsGram reward callback rejected: invalid secret")
                raise HTTPException(
                    status_code=403, detail="Invalid AdsGram reward secret"
                )

        ad_session_id = extract_first_value(params, ADSGRAM_REWARD_SESSION_KEYS)
        if ad_session_id:
            verified = await mark_ad_action_session_verified(ad_session_id, params)
            if not verified:
                logger.warning(
                    "AdsGram callback could not find ad session %s", ad_session_id
                )
                return Response(
                    content="SESSION_NOT_FOUND",
                    media_type="text/plain",
                    status_code=404,
                )
            return Response(content="OK", media_type="text/plain", status_code=200)

        user_id_raw = extract_first_value(params, ADSGRAM_REWARD_USER_KEYS)
        if not user_id_raw or not str(user_id_raw).strip().isdigit():
            logger.warning("AdsGram reward callback missing valid user id: %s", params)
            raise HTTPException(status_code=400, detail="Missing user id")

        user_id = int(str(user_id_raw).strip())
        matched_session_id = await mark_latest_ad_action_session_verified_for_user(
            user_id, params
        )
        if not matched_session_id:
            logger.warning(
                "AdsGram callback could not match an active ad session for user %s",
                user_id,
            )
            return Response(
                content="SESSION_NOT_FOUND", media_type="text/plain", status_code=404
            )

        return Response(content="OK", media_type="text/plain", status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in adsgram_reward_callback: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ad-watched")
async def ad_watched(payload: dict, request: Request):
    """Track ad watch statistics"""
    try:
        user_id = payload.get("user_id")
        reward_type = payload.get("reward_type")
        await require_telegram_user(request, user_id)
        await require_user_action_lock("ad_watched", user_id, ttl=3)

        user = await get_user_cached(user_id)
        if not user:
            return {"success": False}

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        # РЎРѕС…СЂР°РЅСЏРµРј СЃС‚Р°С‚РёСЃС‚РёРєСѓ
        ads_history = extra.get("ads_history", [])
        ads_history.append(
            {"type": reward_type, "timestamp": datetime.utcnow().isoformat()}
        )
        extra["ads_history"] = ads_history[-50:]

        await update_user(user_id, {"extra_data": extra})
        # NOTE: Not invalidating cache — ads_history is not a cached field.

        return {"success": True}

    except Exception as e:
        logger.error(f"Error in ad_watched: {e}")
        return {"success": False}


@router.post("/api/ads/increment")
async def increment_ads_watched(payload: AdActionClaimRequest, request: Request):
    return await increment_ads_watched_service(
        payload,
        request,
        _build_ads_boosts_service_deps(),
        acquire_once_lock=acquire_once_lock,
    )



@router.post("/api/upgrade")
async def process_upgrade(payload: UpgradeRequest, request: Request):
    return await process_upgrade_service(payload, request, _build_upgrades_service_deps())


@router.post("/api/upgrade-all")
async def process_upgrade_all(payload: UserIdRequest, request: Request):
    return await process_upgrade_all_service(
        payload, request, _build_upgrades_service_deps()
    )


@router.post("/api/rebirth")
async def process_rebirth(payload: UserIdRequest, request: Request):
    return await process_rebirth_service(
        payload, request, _build_rebirth_service_deps()
    )


@router.post("/api/update-energy")
async def update_energy(payload: AdActionClaimRequest, request: Request):
    return await update_energy_reward_service(
        payload, request, _build_ads_boosts_service_deps()
    )


@router.post("/api/sync-energy")
async def sync_energy(payload: EnergySyncRequest, request: Request):
    """
    Read-only energy sync. Returns current energy from energy:v2.
    Does NOT write to energy:v2 - only the click path writes energy.
    This prevents sync-energy from racing with active click updates.
    """
    return await sync_energy_service(payload, request, _build_clicks_service_deps())


SKIN_MULTIPLIERS = {
    DEFAULT_SKIN_ID: 1.0,
    REFERRAL_SPECIAL_SKIN_ID: 1.8,
    DAILY_REWARD_SKIN_ID: 1.8,
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
    "stars1.pngSP": 2.0,
    "stars2.pngSP": 2.0,
    "stars3.pngSP": 2.0,
    "stars4.pngSP": 2.0,
    "stars5.pngSP": 2.0,
    "stars6.pngSP": 2.0,
    "stars7.pngSP": 2.0,
    "stars8.pngSP": 2.0,
    "telega.pngSP": 1.5,
    "tiktok.pngSP": 1.5,
    "insta.pngSP": 1.5,
}

LEGACY_SKIN_ID_MAP = {
    "referral-special.pngSP": REFERRAL_SPECIAL_SKIN_ID,
    "daily30.pngSP": DAILY_REWARD_SKIN_ID,
    "telegram-social.pngSP": "telega.pngSP",
    "tiktok-social.pngSP": "tiktok.pngSP",
    "instagram-social.pngSP": "insta.pngSP",
}

VALID_SKIN_IDS = set(SKIN_MULTIPLIERS.keys())


def normalize_owned_skins(raw_owned) -> list[str]:
    if isinstance(raw_owned, list):
        owned = [str(item) for item in raw_owned]
    else:
        owned = []

    normalized = []
    seen = set()
    for skin_id in owned:
        skin_id = LEGACY_SKIN_ID_MAP.get(skin_id, skin_id)
        if skin_id in VALID_SKIN_IDS and skin_id not in seen:
            seen.add(skin_id)
            normalized.append(skin_id)

    if DEFAULT_SKIN_ID not in seen:
        normalized.insert(0, DEFAULT_SKIN_ID)

    return normalized


def normalize_selected_skin(selected_skin: str | None, owned_skins: list[str]) -> str:
    selected_skin = LEGACY_SKIN_ID_MAP.get(selected_skin, selected_skin)
    if selected_skin in owned_skins:
        return selected_skin
    return DEFAULT_SKIN_ID


def get_selected_skin_multiplier(user: dict) -> float:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
    selected_skin = normalize_selected_skin(
        extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
    )
    return SKIN_MULTIPLIERS.get(selected_skin, 1.0)


def is_mega_boost_active(user: dict) -> bool:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("mega_boost")
    if not boost:
        return False

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False

    try:
        expires_dt = datetime.fromisoformat(expires_at)
        return datetime.utcnow() < expires_dt
    except Exception:
        return False


def get_ghost_boost_status(user: dict) -> tuple[bool, str | None]:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("ghost_boost")
    if not boost:
        return False, None

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, None

    if datetime.utcnow() >= expires_dt:
        return False, None

    return True, expires_at


def get_daily_reward_progress(extra: dict) -> tuple[int, str | None]:
    return get_daily_reward_progress_service(extra, _build_tasks_rewards_service_deps())


def is_daily_infinite_energy_active(user: dict) -> tuple[bool, str | None]:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("daily_infinite_energy")
    if not boost:
        return False, None

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, None

    if datetime.utcnow() >= expires_dt:
        return False, None

    return True, expires_at


SKIN_REQUIREMENTS = {
    "10lvl.pngSP": {"type": "level", "value": 10},
    "25lvl.pngSP": {"type": "level", "value": 25},
    "50lvl.pngSP": {"type": "level", "value": 50},
    "75lvl.pngSP": {"type": "level", "value": 75},
    "100lvl.pngSP": {"type": "level", "value": 100},
    "video.pngSP": {"type": "ads", "count": 10},
    "video2.pngSP": {"type": "ads", "count": 10},
    "video3.pngSP": {"type": "ads", "count": 10},
    "video4.pngSP": {"type": "ads", "count": 10},
    "video5.pngSP": {"type": "ads", "count": 10},
    "video6.pngSP": {"type": "ads", "count": 10},
    "video7.pngSP": {"type": "ads", "count": 10},
    "video8.pngSP": {"type": "ads", "count": 10},
}


async def can_unlock_skin(user: dict, skin_id: str) -> bool:
    req = SKIN_REQUIREMENTS.get(skin_id)
    if not req:
        return False

    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    if req["type"] == "level":
        display_level = int(resolve_progression_level(user)) + 1
        return display_level >= int(req["value"])

    if req["type"] == "ads":
        progress = get_skin_ad_progress(extra)
        current = int(progress.get(skin_id, 0) or 0)
        return current >= int(req["count"])

    if req["type"] == "friends":
        referral_count = int(user.get("referral_count", 0))
        return referral_count >= int(req["count"])

    return False


async def ensure_coins_hot_initialized(user_id: int, db_coins: int, redis_conn) -> None:
    await ensure_coins_hot_initialized_service(user_id, db_coins, redis_conn)


def _build_profile_state_service_deps() -> ProfileStateServiceDeps:
    return ProfileStateServiceDeps(
        require_telegram_user=require_telegram_user,
        get_user_cached=get_user_cached,
        touch_user_activity=touch_user_activity,
        get_redis_or_none=get_redis_or_none,
        get_user=get_user,
        build_realtime_player_state=build_realtime_player_state,
        logger=logger,
    )


def _build_tasks_rewards_service_deps() -> TasksRewardsServiceDeps:
    return TasksRewardsServiceDeps(
        logger=logger,
        BOT_TOKEN=BOT_TOKEN,
        TELEGRAM_VERIFY_CHANNEL=TELEGRAM_VERIFY_CHANNEL,
        TELEGRAM_MEMBER_STATUSES=TELEGRAM_MEMBER_STATUSES,
        DAILY_REWARD_MAX_DAYS=DAILY_REWARD_MAX_DAYS,
        parse_iso_datetime=parse_iso_datetime,
        AsyncSessionLocal=AsyncSessionLocal,
        User=User,
        UserTask=UserTask,
        get_user=get_user,
        sync_hot_after_db_increment=sync_hot_after_db_increment,
    )


def _build_tournament_service_deps() -> TournamentReadServiceDeps:
    return TournamentReadServiceDeps(
        require_telegram_user=require_telegram_user,
        get_weekly_tournament_season_window=get_weekly_tournament_season_window,
        get_weekly_tournament_season_key=get_weekly_tournament_season_key,
        list_weekly_tournament_seasons=list_weekly_tournament_seasons,
        get_weekly_tournament_player_entry=get_weekly_tournament_player_entry,
        get_weekly_tournament_leaderboard=get_weekly_tournament_leaderboard,
        get_weekly_tournament_winners=get_weekly_tournament_winners,
        AsyncSessionLocal=AsyncSessionLocal,
        WeeklyTournamentTonPayout=WeeklyTournamentTonPayout,
        WEEKLY_LEAGUE_ORDER=WEEKLY_LEAGUE_ORDER,
        WEEKLY_LEAGUE_LEVEL_RANGES=WEEKLY_LEAGUE_LEVEL_RANGES,
        WEEKLY_LEAGUE_FUND_SPLITS=WEEKLY_LEAGUE_FUND_SPLITS,
        WEEKLY_TOP3_PAYOUT_SPLITS=WEEKLY_TOP3_PAYOUT_SPLITS,
        WEEKLY_RANGE_PAYOUT_SPLITS=WEEKLY_RANGE_PAYOUT_SPLITS,
        logger=logger,
    )


def _build_ads_boosts_service_deps() -> AdsBoostsServiceDeps:
    return AdsBoostsServiceDeps(
        ensure_redis_available=ensure_redis_available,
        get_ad_action_active_session_key=get_ad_action_active_session_key,
        require_telegram_user=require_telegram_user,
        require_dual_rate_limit=require_dual_rate_limit,
        require_user_action_lock=require_user_action_lock,
        get_user_cached=get_user_cached,
        update_user=update_user,
        invalidate_user_cache=invalidate_user_cache,
        record_rewarded_ad_claim=record_rewarded_ad_claim,
        parse_extra_data=parse_extra_data,
        parse_iso_datetime=parse_iso_datetime,
        get_redis_or_none=get_redis_or_none,
        resolve_max_energy=resolve_max_energy,
        format_duration=format_duration,
        logger=logger,
        AD_ACTIONS_ALLOWED=AD_ACTIONS_ALLOWED,
        AD_ACTION_SESSION_TTL_SECONDS=AD_ACTION_SESSION_TTL_SECONDS,
        AD_SESSION_MIN_WAIT_SECONDS=AD_SESSION_MIN_WAIT_SECONDS,
        MONETAG_POSTBACK_ENFORCED=MONETAG_POSTBACK_ENFORCED,
        ADSGRAM_REWARD_ENFORCED=ADSGRAM_REWARD_ENFORCED,
        MEGA_BOOST_MINUTES=MEGA_BOOST_MINUTES,
        MEGA_BOOST_COOLDOWN_MAX_MINUTES=MEGA_BOOST_COOLDOWN_MAX_MINUTES,
        GHOST_BOOST_MULTIPLIER=GHOST_BOOST_MULTIPLIER,
        GHOST_BOOST_MINUTES=GHOST_BOOST_MINUTES,
        SKIN_AD_COOLDOWN_MINUTES=SKIN_AD_COOLDOWN_MINUTES,
        VIDEO_SKIN_IDS=VIDEO_SKIN_IDS,
        SKIN_REQUIREMENTS=SKIN_REQUIREMENTS,
        LEGACY_SKIN_ID_MAP=LEGACY_SKIN_ID_MAP,
        ENERGY_REFILL_COOLDOWN_MINUTES=ENERGY_REFILL_COOLDOWN_MINUTES,
        ENERGY_REGEN_SECONDS=ENERGY_REGEN_SECONDS,
    )


def _build_upgrades_service_deps() -> UpgradesServiceDeps:
    return UpgradesServiceDeps(
        require_telegram_user=require_telegram_user,
        require_dual_rate_limit=require_dual_rate_limit,
        require_user_action_lock=require_user_action_lock,
        get_user=get_user,
        update_user_if_matches=update_user_if_matches,
        get_redis_or_none=get_redis_or_none,
        logger=logger,
        GLOBAL_UPGRADE_PRICES=GLOBAL_UPGRADE_PRICES,
        MAX_UPGRADE_LEVEL=MAX_UPGRADE_LEVEL,
    )


def _build_rebirth_service_deps() -> RebirthServiceDeps:
    return RebirthServiceDeps(
        require_telegram_user=require_telegram_user,
        require_dual_rate_limit=require_dual_rate_limit,
        require_user_action_lock=require_user_action_lock,
        get_user=get_user,
        update_user_if_matches=update_user_if_matches,
        invalidate_user_cache=invalidate_user_cache,
        get_redis_or_none=get_redis_or_none,
        logger=logger,
        ENERGY_REGEN_SECONDS=ENERGY_REGEN_SECONDS,
    )


def _build_clicks_service_deps() -> ClicksServiceDeps:
    return ClicksServiceDeps(
        require_telegram_user=require_telegram_user,
        require_dual_rate_limit=require_dual_rate_limit,
        get_user=get_user,
        acquire_idempotency_key=acquire_idempotency_key,
        get_request_ip=get_request_ip,
        get_redis_or_none=get_redis_or_none,
        parse_extra_data=parse_extra_data,
        get_click_guard_state=get_click_guard_state,
        parse_iso_datetime=parse_iso_datetime,
        normalize_dt=normalize_dt,
        calculate_current_energy=calculate_current_energy,
        resolve_max_energy=resolve_max_energy,
        get_tap_value=get_tap_value,
        normalize_owned_skins=normalize_owned_skins,
        normalize_selected_skin=normalize_selected_skin,
        is_mega_boost_active=is_mega_boost_active,
        get_ghost_boost_status=get_ghost_boost_status,
        get_active_video_task_boost=get_active_video_task_boost,
        is_daily_infinite_energy_active=is_daily_infinite_energy_active,
        ensure_coins_hot_initialized=ensure_coins_hot_initialized,
        update_user=update_user,
        write_click_guard_state=write_click_guard_state,
        get_all_boost_states=get_all_boost_states,
        get_hour_value=get_hour_value,
        build_click_response_state=build_click_response_state,
        get_max_energy=get_max_energy,
        logger=logger,
        MAX_CLICK_BATCH_SIZE=MAX_CLICK_BATCH_SIZE,
        ENERGY_REGEN_SECONDS=ENERGY_REGEN_SECONDS,
        MAX_REAL_CLICKS_PER_SECOND=MAX_REAL_CLICKS_PER_SECOND,
        CLICK_BURST_ALLOWANCE=CLICK_BURST_ALLOWANCE,
        CLICK_TIME_ACCUMULATION_CAP_SECONDS=CLICK_TIME_ACCUMULATION_CAP_SECONDS,
        INITIAL_CLICK_BATCH_ALLOWANCE=INITIAL_CLICK_BATCH_ALLOWANCE,
        CLICK_SUSPICIOUS_OVERSHOOT=CLICK_SUSPICIOUS_OVERSHOOT,
        CLICK_SUSPICION_SOFT_LIMIT=CLICK_SUSPICION_SOFT_LIMIT,
        TOURNAMENT_KEY=TOURNAMENT_KEY,
        GHOST_BOOST_MULTIPLIER=GHOST_BOOST_MULTIPLIER,
        SKIN_MULTIPLIERS=SKIN_MULTIPLIERS,
        DEFAULT_SKIN_ID=DEFAULT_SKIN_ID,
        ENABLE_K6_FRAUD_HEURISTICS=ENABLE_K6_FRAUD_HEURISTICS,
    )


@router.post("/api/clicks")
async def process_clicks_batch(payload: ClicksBatchRequest, request: Request):
    return await process_clicks_batch_service(
        payload, request, _build_clicks_service_deps()
    )


@router.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        global_level = get_global_upgrade_level_service(user)
        global_price = (
            GLOBAL_UPGRADE_PRICES[global_level]
            if global_level < len(GLOBAL_UPGRADE_PRICES)
            else 0
        )

        return {
            "global": global_price,
            "multitap": global_price,
            "profit": global_price,
            "energy": global_price,
        }
    except Exception as e:
        logger.error(f"Error in get_upgrade_prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/register")
async def register_user(payload: RegisterRequest, request: Request):
    try:
        telegram_user = await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "register", request, payload.user_id, 10, 60, ip_limit=20
        )
        await require_user_action_lock("register", payload.user_id, ttl=5)
        existing = await get_user(payload.user_id)
        valid_referrer_id = None
        requested_referrer_id = int(payload.referrer_id or 0)
        if requested_referrer_id and requested_referrer_id != payload.user_id:
            referrer = await get_user_cached(requested_referrer_id)
            if referrer and int(referrer.get("referrer_id") or 0) != payload.user_id:
                valid_referrer_id = requested_referrer_id

        if existing:
            username = telegram_user.get("username") or payload.username
            updates = {}
            if username and username != existing.get("username"):
                updates["username"] = username

            # Allow a one-time referral attachment for a fresh account that was
            # created before the WebApp received the referral param.
            can_attach_referrer = (
                valid_referrer_id
                and not existing.get("referrer_id")
                and int(existing.get("coins", 0) or 0) == 0
                and int(existing.get("level", 0) or 0) == 0
                and int(existing.get("referral_count", 0) or 0) == 0
                and int(existing.get("referral_earnings", 0) or 0) == 0
            )
            if can_attach_referrer:
                updates["referrer_id"] = valid_referrer_id

            if updates:
                await update_user(payload.user_id, updates)
                await invalidate_user_cache(payload.user_id)
                if can_attach_referrer:
                    await add_referral_bonus(valid_referrer_id, payload.user_id)
                    await invalidate_user_cache(valid_referrer_id)

            refreshed = await get_user_cached(payload.user_id)
            return {"status": "exists", "user": refreshed or existing}

        await create_user(
            user_id=payload.user_id,
            username=telegram_user.get("username") or payload.username,
            referrer_id=valid_referrer_id,
        )

        created_user = await get_user_cached(payload.user_id)
        return {"status": "created", "user": created_user}
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== REFERRALS ====================


@router.get("/api/weekly-tournament/overview/{user_id}")
async def get_weekly_tournament_overview(user_id: int, request: Request):
    return await get_weekly_tournament_overview_service(
        user_id,
        request,
        _build_tournament_service_deps(),
        get_pending_ton_wallet_notice=get_pending_ton_wallet_notice,
    )


# ==================== REFERRALS ====================


@router.get("/api/weekly-tournament/leaderboard/{league}")
async def get_weekly_tournament_leaderboard_endpoint(
    league: str, season_key: str | None = None, limit: int = 50
):
    return await get_weekly_tournament_leaderboard_service(
        league, season_key, limit, _build_tournament_service_deps()
    )


@router.get("/api/weekly-tournament/player/{user_id}")
async def get_weekly_tournament_player_endpoint(
    user_id: int, request: Request, season_key: str | None = None
):
    try:
        await require_telegram_user(request, user_id)
        effective_season_key = season_key or get_weekly_tournament_season_key()
        player = await get_weekly_tournament_player_entry(user_id, effective_season_key)
        return {
            "success": True,
            "season_key": effective_season_key,
            "player": player,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_player_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/weekly-tournament/results/{league}")
async def get_weekly_tournament_results_endpoint(
    league: str, season_key: str | None = None, limit: int = 50
):
    return await get_weekly_tournament_results_service(
        league, season_key, limit, _build_tournament_service_deps()
    )


@router.post("/api/skins/stars-invoice")
async def create_skin_stars_invoice(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("stars_skin_invoice", payload.user_id, ttl=3)

        price = get_stars_skin_price(payload.skin_id)
        if price is None:
            raise HTTPException(status_code=400, detail="Skin is not sold for Stars")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        owned_skins = extra.get("owned_skins", [DEFAULT_SKIN_ID])
        if payload.skin_id in owned_skins:
            raise HTTPException(status_code=400, detail="Skin already owned")

        invoice_link = await create_telegram_stars_invoice_link(
            user_id=payload.user_id, skin_id=payload.skin_id, price=price
        )

        return {
            "success": True,
            "invoice_link": invoice_link,
            "price": price,
            "skin_id": payload.skin_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Stars invoice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/tournament/leaderboard")
async def get_tournament_leaderboard():
    """Get top 3 players from Redis leaderboard"""
    try:
        players = []

        conn = await get_redis_or_none()
        if conn:
            top_players = await conn.zrevrange(TOURNAMENT_KEY, 0, 2, withscores=True)

            for idx, (user_id_str, score) in enumerate(top_players):
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    continue

                user = await get_user_cached(user_id)

                username = user.get("username") if user else None
                avatar_url = (
                    f"https://t.me/i/userpic/320/{username}.jpg"
                    if username
                    else "/imgg/default_avatar.png"
                )

                players.append(
                    {
                        "rank": idx + 1,
                        "user_id": user_id,
                        "name": mask_username(username),
                        "avatar": avatar_url,
                        "score": int(score),
                    }
                )

        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time_left = int((tomorrow - now).total_seconds())

        return {
            "success": True,
            "players": players,
            "prize_pool": TOURNAMENT_PRIZE_POOL,
            "time_left": time_left,
            "online_now": await get_online_users_count(),
        }

    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/tournament/player-rank/{user_id}")
async def get_player_rank(user_id: int, request: Request):
    """Get player's rank from Redis leaderboard"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            return {
                "success": True,
                "rank": 0,
                "score": 0,
                "next_rank_score": 0,
                "avatar": "/imgg/default_avatar.png",
                "name": "Player",
            }

        username = user.get("username")
        avatar_url = (
            f"https://t.me/i/userpic/320/{username}.jpg"
            if username
            else "/imgg/default_avatar.png"
        )

        redis_conn = await ensure_redis_available()

        score = await redis_conn.zscore(TOURNAMENT_KEY, str(user_id))
        score = int(score) if score is not None else 0

        rev_rank = await redis_conn.zrevrank(TOURNAMENT_KEY, str(user_id))
        rank = (rev_rank + 1) if rev_rank is not None else 0

        next_rank_score = 0
        if rev_rank is not None and rev_rank > 0:
            higher_player = await redis_conn.zrevrange(
                TOURNAMENT_KEY, rev_rank - 1, rev_rank - 1, withscores=True
            )
            if higher_player:
                _, higher_score = higher_player[0]
                next_rank_score = max(0, int(higher_score) - score)

        return {
            "success": True,
            "rank": rank,
            "score": score,
            "next_rank_score": next_rank_score,
            "avatar": avatar_url,
            "name": mask_username(username),
        }

    except Exception as e:
        logger.error(f"Error getting player rank: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== Р—РђР”РђР§Р ====================

_task_completion_store = {}


# ==================== РџРђРЎРЎРР’РќР«Р™ Р”РћРҐРћР” ====================


# ==================== DAILY REWARDS ====================


# ==================== РЎРљРРќР« ====================


@router.post("/api/select-skin")
async def select_skin(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("select_skin", payload.user_id, ttl=3)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}
        if not isinstance(extra, dict):
            extra = {}

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(payload.skin_id, owned_skins)
        if selected_skin not in owned_skins:
            raise HTTPException(status_code=400, detail="Skin not owned")

        extra["owned_skins"] = owned_skins
        extra["selected_skin"] = selected_skin

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "selected_skin": selected_skin}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in select_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/unlock-skin")
async def unlock_skin(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("unlock_skin", payload.user_id, ttl=5)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        skin_id = LEGACY_SKIN_ID_MAP.get(payload.skin_id, payload.skin_id)
        if skin_id not in VALID_SKIN_IDS:
            raise HTTPException(status_code=400, detail="Unknown skin")

        owned = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        skin_ad_progress = get_skin_ad_progress(extra)

        if skin_id in owned:
            return {"success": True}

        if skin_id in SKIN_REQUIREMENTS and SKIN_REQUIREMENTS[skin_id]["type"] == "ads":
            required = int(SKIN_REQUIREMENTS[skin_id]["count"])
            current_progress = int(skin_ad_progress.get(skin_id, 0) or 0)
            if current_progress < required:
                raise HTTPException(status_code=400, detail="Not enough ads watched")

        # вњ… РґРѕР±Р°РІР»СЏРµРј СЃРєРёРЅ
        owned.append(skin_id)
        extra["owned_skins"] = normalize_owned_skins(owned)

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unlock skin error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== RECONCILIATION / DEBUG ====================


@router.get("/api/admin/reconcile/coins")
async def admin_reconcile_coins(request: Request, limit: int = 100):
    """
    Read-only reconciliation: compare DB coins vs Redis hot/pending/flushing.
    Returns summary + any mismatches found.
    Does NOT auto-fix balances.
    """
    await require_admin_access(request)
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return {"error": "Redis unavailable", "status": "error"}

    from workers.reconcile_coins import reconcile_all_users, print_report

    results = await reconcile_all_users(redis_conn, limit=limit)
    mismatches = [r for r in results if r.mismatch_categories]

    return {
        "status": "ok",
        "total_checked": len(results),
        "mismatches_found": len(mismatches),
        "categories": {
            "hot_below_db": len(
                [r for r in mismatches if "hot_below_db" in r.mismatch_categories]
            ),
            "hot_negative": len(
                [r for r in mismatches if "hot_negative" in r.mismatch_categories]
            ),
            "pending_zero_or_negative": len(
                [
                    r
                    for r in mismatches
                    if "pending_zero_or_negative" in r.mismatch_categories
                ]
            ),
            "excessive_flushing": len(
                [
                    r
                    for r in mismatches
                    if "excessive_flushing_batches" in r.mismatch_categories
                ]
            ),
        },
        "details": [
            {
                "user_id": r.user_id,
                "db_coins": r.db_coins,
                "hot_coins": r.hot_coins,
                "pending_coins": r.pending_coins,
                "flushing_coins": r.flushing_coins,
                "flushing_batches": r.flushing_batches,
                "issues": r.mismatch_categories,
            }
            for r in mismatches[:50]
        ],
    }


@router.get("/api/admin/reconcile/coins/{user_id}")
async def admin_reconcile_coins_user(user_id: int, request: Request):
    """Reconcile a single user's coins across DB and Redis."""
    await require_admin_access(request)
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return {"error": "Redis unavailable", "status": "error"}

    from workers.reconcile_coins import reconcile_user

    rec = await reconcile_user(user_id, redis_conn)
    return {
        "status": "ok",
        "user_id": rec.user_id,
        "db_coins": rec.db_coins,
        "hot_coins": rec.hot_coins,
        "hot_exists": rec.hot_exists,
        "pending_coins": rec.pending_coins,
        "pending_exists": rec.pending_exists,
        "flushing_coins": rec.flushing_coins,
        "flushing_batches": rec.flushing_batches,
        "flushing_keys": rec.flushing_keys[:20],
        "total_redis": rec.total_redis,
        "expected_db_after_flush": rec.expected_db_coins,
        "mismatch_categories": rec.mismatch_categories,
    }


async def _repair_user_hot_if_below_db(user_id: int, redis_conn) -> dict:
    """
    One-time repair for drifted balances where DB coins > Redis coins_hot.
    Safe by design:
    - updates only coins_hot
    - does not touch coins_pending / coins_flushing
    - no DB writes
    """
    from workers.reconcile_coins import reconcile_user

    before = await reconcile_user(user_id, redis_conn)
    drift_confirmed = "hot_below_db" in before.mismatch_categories

    if not drift_confirmed:
        return {
            "user_id": before.user_id,
            "repaired": False,
            "reason": "no_confirmed_drift",
            "before": {
                "db_coins": before.db_coins,
                "hot_coins": before.hot_coins,
                "pending_coins": before.pending_coins,
                "flushing_coins": before.flushing_coins,
                "mismatch_categories": before.mismatch_categories,
            },
        }

    repair_lua = """
    local hot_key = KEYS[1]
    local db_coins = tonumber(ARGV[1])

    local current_raw = redis.call('GET', hot_key)
    local current = 0
    if current_raw then
        current = tonumber(current_raw)
    end

    if current < db_coins then
        redis.call('SET', hot_key, tostring(db_coins))
        return db_coins
    end

    return current
    """
    repaired_hot = int(
        await redis_conn.eval(repair_lua, 1, f"coins_hot:{int(user_id)}", before.db_coins)
    )
    after = await reconcile_user(user_id, redis_conn)

    return {
        "user_id": before.user_id,
        "repaired": True,
        "before": {
            "db_coins": before.db_coins,
            "hot_coins": before.hot_coins,
            "pending_coins": before.pending_coins,
            "flushing_coins": before.flushing_coins,
            "mismatch_categories": before.mismatch_categories,
        },
        "after": {
            "db_coins": after.db_coins,
            "hot_coins": after.hot_coins,
            "pending_coins": after.pending_coins,
            "flushing_coins": after.flushing_coins,
            "mismatch_categories": after.mismatch_categories,
        },
        "repaired_hot": repaired_hot,
    }


@router.post("/api/admin/reconcile/coins/{user_id}/repair-hot")
async def admin_reconcile_repair_hot_user(user_id: int, request: Request):
    """Repair one user if and only if confirmed drift exists (DB > coins_hot)."""
    await require_admin_access(request)
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return {"error": "Redis unavailable", "status": "error"}

    result = await _repair_user_hot_if_below_db(user_id, redis_conn)
    return {"status": "ok", **result}


@router.post("/api/admin/reconcile/coins/repair-hot")
async def admin_reconcile_repair_hot_batch(
    request: Request, limit: int = 200, apply: bool = True
):
    """
    One-time batch repair:
    - scans users
    - selects only confirmed hot_below_db drifts
    - sets coins_hot to DB coins for those users
    """
    await require_admin_access(request)
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return {"error": "Redis unavailable", "status": "error"}

    from workers.reconcile_coins import reconcile_all_users

    recs = await reconcile_all_users(redis_conn, limit=max(1, min(int(limit), 5000)))
    drifted = [r for r in recs if "hot_below_db" in r.mismatch_categories]

    if not apply:
        return {
            "status": "ok",
            "dry_run": True,
            "checked": len(recs),
            "drifted_count": len(drifted),
            "drifted_users": [
                {
                    "user_id": r.user_id,
                    "db_coins": r.db_coins,
                    "hot_coins": r.hot_coins,
                    "pending_coins": r.pending_coins,
                    "flushing_coins": r.flushing_coins,
                }
                for r in drifted[:200]
            ],
        }

    repaired = []
    skipped = []
    for r in drifted:
        outcome = await _repair_user_hot_if_below_db(r.user_id, redis_conn)
        if outcome.get("repaired"):
            repaired.append(outcome)
        else:
            skipped.append(outcome)

    return {
        "status": "ok",
        "dry_run": False,
        "checked": len(recs),
        "drifted_count": len(drifted),
        "repaired_count": len(repaired),
        "skipped_count": len(skipped),
        "repaired_users": repaired[:200],
        "skipped_users": skipped[:200],
    }


@router.get("/api/admin/reconcile/flush-lag")
async def admin_flush_lag(request: Request):
    """Current flush pipeline state — pending/flushing/processing key counts."""
    await require_admin_access(request)
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return {"error": "Redis unavailable"}

    from workers.worker_health import get_flush_lag

    return await get_flush_lag(redis_conn)


# ==================== Р—РђРџРЈРЎРљ ====================

