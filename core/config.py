import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is required")

REDIS_URL = os.getenv("REDIS_URL")

SESSION_TOKEN_SECRET = os.getenv("SESSION_TOKEN_SECRET", "")
if not SESSION_TOKEN_SECRET:
    import hashlib

    SESSION_TOKEN_SECRET = hashlib.sha256(
        f"{BOT_TOKEN}:session-token".encode("utf-8")
    ).hexdigest()

SESSION_TOKEN_TTL_SECONDS = max(
    900, int(os.getenv("SESSION_TOKEN_TTL_SECONDS", "3600"))
)

APP_ENV = os.getenv("APP_ENV", "production").strip().lower()

MOBILE_ONLY_ENFORCED = os.getenv("MOBILE_ONLY_ENFORCED", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

ADMIN_DASHBOARD_TOKEN = os.getenv("ADMIN_DASHBOARD_TOKEN", "").strip()
ADMIN_TELEGRAM_IDS = {
    int(item.strip())
    for item in (os.getenv("ADMIN_TELEGRAM_IDS", "") or "").split(",")
    if item.strip().isdigit()
}

TELEGRAM_VERIFY_CHANNEL = os.getenv("TELEGRAM_VERIFY_CHANNEL", "@Spirit_cliker")
TELEGRAM_BOT_USERNAME = (
    os.getenv("TELEGRAM_BOT_USERNAME", "Ryoho_bot").strip().lstrip("@")
)
GAME_WEBAPP_URL = os.getenv("GAME_WEBAPP_URL", "https://spirix.vercel.app").strip()

MONETAG_POSTBACK_SECRET = os.getenv("MONETAG_POSTBACK_SECRET", "").strip()
ADSGRAM_REWARD_SECRET = os.getenv("ADSGRAM_REWARD_SECRET", "").strip()

TON_VERIFIER_API_BASE = (
    os.getenv("TON_VERIFIER_API_BASE", "https://toncenter.com/api/v3")
    .strip()
    .rstrip("/")
)
TON_VERIFIER_API_KEY = os.getenv("TON_VERIFIER_API_KEY", "").strip()
TON_VERIFIER_TIMEOUT_SECONDS = max(
    5, int(os.getenv("TON_VERIFIER_TIMEOUT_SECONDS", "30"))
)
TON_NANO = 1_000_000_000
TON_PAYOUT_SENDER_URL = os.getenv("TON_PAYOUT_SENDER_URL", "").strip()
TON_PAYOUT_SENDER_TOKEN = os.getenv("TON_PAYOUT_SENDER_TOKEN", "").strip()
TON_PAYOUT_SENDER_TIMEOUT_SECONDS = max(
    5, int(os.getenv("TON_PAYOUT_SENDER_TIMEOUT_SECONDS", "45"))
)

# Tournament
WEEKLY_LEAGUE_ORDER = ("diamond", "gold", "silver", "bronze")
WEEKLY_LEAGUE_LEVEL_RANGES = {
    "bronze": {"min_level": 1, "max_level": 32},
    "silver": {"min_level": 33, "max_level": 65},
    "gold": {"min_level": 66, "max_level": 99},
    "diamond": {"min_level": 100, "max_level": None},
}
WEEKLY_LEAGUE_FUND_SPLITS = {
    "diamond": 0.50,
    "gold": 0.30,
    "silver": 0.15,
    "bronze": 0.05,
}
WEEKLY_TOP3_PAYOUT_SPLITS = {
    1: 0.30,
    2: 0.20,
    3: 0.13,
}
WEEKLY_RANGE_PAYOUT_SPLITS = [
    {"start": 4, "end": 10, "share": 0.22},
    {"start": 11, "end": 20, "share": 0.10},
    {"start": 21, "end": 50, "share": 0.05},
]

# Daily rewards
DAILY_REWARD_MAX_DAYS = 30

# Autoclicker
AUTOCLICKER_COOLDOWN_MINUTES = 10

# Video tasks — matches legacy.py VIDEO_TASK_DEFINITIONS exactly
VIDEO_TASK_DEFINITIONS = {
    "tap_surge": {
        "type": "tap_boost",
        "cooldown_minutes": 75,
        "duration_minutes": 5,
        "multiplier": 2,
    },
    "passive_hour": {
        "type": "passive_boost",
        "cooldown_minutes": 240,
        "duration_minutes": 60,
        "multiplier": 2,
    },
    "coin_drop": {
        "type": "coin_drop",
        "cooldown_minutes": 60,
    },
}

# Diagnostics
DIAGNOSTICS_DURATION_WINDOW = 240

CORS_ORIGINS = [
    "https://spirix.vercel.app",
    "https://web.telegram.org",
    "https://telegram.org",
]
if APP_ENV != "production":
    CORS_ORIGINS += [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5500",
    ]
