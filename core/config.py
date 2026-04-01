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

MOBILE_ONLY_ENFORCED = os.getenv("MOBILE_ONLY_ENFORCED", "1").strip().lower() in {
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
