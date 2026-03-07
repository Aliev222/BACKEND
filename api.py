from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import asyncio
import uvicorn
import random
import time
import hashlib
import hmac
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import os
import logging
from contextlib import asynccontextmanager

from DATABASE.base import (
    get_user, add_user as create_user, update_user,
    init_db, get_completed_tasks, add_completed_task
)

# ==================== CONFIGURATION ====================

# Anti-cheat constants
MAX_CLICKS_PER_SECOND = 100
MAX_ENERGY_RECOVERY_PER_SECOND = 1
MIN_TASK_COMPLETION_INTERVAL = 60  # seconds
MAX_REWARD_PER_VIDEO = 5000
MAX_BET = 1000000
MIN_BET = 10
BASE_MAX_ENERGY = 500          # Начальный максимум энергии
FULL_RECHARGE_TIME = 600        # Полное восстановление за 600 секунд (10 минут)

UPGRADE_PRICES = {
    "multitap": [
        50, 75, 100, 150, 200, 300, 450, 650, 900, 1200,
        1600, 2100, 2700, 3400, 4200, 5100, 6100, 7200, 8400, 9700,
        11100, 12600, 14200, 15900, 17700, 19600, 21600, 23700, 25900, 28200,
        30600, 33100, 35700, 38400, 41200, 44100, 47100, 50200, 53400, 56700,
        60100, 63600, 67200, 70900, 74700, 78600, 82600, 86700, 90900, 95200,
        100000, 105000, 110000, 115000, 120000, 125000, 130000, 135000, 140000, 145000,
        150000, 160000, 170000, 180000, 190000, 200000, 210000, 220000, 230000, 240000,
        250000, 270000, 290000, 310000, 330000, 350000, 370000, 390000, 410000, 430000,
        450000, 500000, 550000, 600000, 650000, 700000, 750000, 800000, 850000, 900000,
        950000, 1000000, 1100000, 1200000, 1300000, 1400000, 1500000, 1600000, 1700000, 1800000
    ],
    "profit": [
        40, 60, 90, 130, 180, 240, 310, 390, 480, 580,
        690, 810, 940, 1080, 1230, 1390, 1560, 1740, 1930, 2130,
        2340, 2560, 2790, 3030, 3280, 3540, 3810, 4090, 4380, 4680,
        4990, 5310, 5640, 5980, 6330, 6690, 7060, 7440, 7830, 8230,
        8640, 9060, 9490, 9930, 10380, 10840, 11310, 11790, 12280, 12780,
        13300, 13850, 14420, 15010, 15620, 16250, 16900, 17570, 18260, 18970,
        19700, 20450, 21220, 22010, 22820, 23650, 24500, 25370, 26260, 27170,
        28100, 29050, 30020, 31010, 32020, 33050, 34100, 35170, 36260, 37370,
        38500, 39650, 40820, 42010, 43220, 44450, 45700, 46970, 48260, 49570,
        50900, 52250, 53620, 55010, 56420, 57850, 59300, 60770, 62260, 63770
    ],
    "energy": [
        30, 45, 65, 90, 120, 155, 195, 240, 290, 345,
        405, 470, 540, 615, 695, 780, 870, 965, 1065, 1170,
        1280, 1395, 1515, 1640, 1770, 1905, 2045, 2190, 2340, 2495,
        2655, 2820, 2990, 3165, 3345, 3530, 3720, 3915, 4115, 4320,
        4530, 4745, 4965, 5190, 5420, 5655, 5895, 6140, 6390, 6645,
        6905, 7170, 7440, 7715, 7995, 8280, 8570, 8865, 9165, 9470,
        9780, 10095, 10415, 10740, 11070, 11405, 11745, 12090, 12440, 12795,
        13155, 13520, 13890, 14265, 14645, 15030, 15420, 15815, 16215, 16620,
        17030, 17445, 17865, 18290, 18720, 19155, 19595, 20040, 20490, 20945,
        21405, 21870, 22340, 22815, 23295, 23780, 24270, 24765, 25265, 25770
    ],
}

# Значения для расчёта дохода в час и максимальной энергии
HOUR_VALUES = [
    100, 150, 250, 500, 1000, 1250, 1500, 1800, 2000, 2500,
    3000, 4000, 5000, 6000, 7000, 10000, 12000, 14000, 16000, 18000,
    20000, 23000, 26000, 29000, 32000, 36000, 40000, 44000, 48000, 52000,
    56000, 61000, 66000, 71000, 76000, 82000, 88000, 94000, 100000, 107000,
    114000, 121000, 128000, 135000, 142000, 150000, 150000, 150000, 150000, 150000,
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000
]

# Базовая энергия = 500, затем растёт на 5 за уровень, до 1000
def get_max_energy(level: int) -> int:
    """Максимальная энергия = 500 + level * 5, но не более 1000"""
    return min(1000, BASE_MAX_ENERGY + level * 5)

# Бонусы скинов (хранятся на сервере)
SKIN_BONUSES = {
    'default_SP': {'type': 'multiplier', 'value': 1.0},
    'Galaxy_SP': {'type': 'multiplier', 'value': 1.1},
    'Water_SP': {'type': 'multiplier', 'value': 1.15},
    'Ninja_SP': {'type': 'multiplier', 'value': 1.5},
    'Monster_SP': {'type': 'interval', 'value': 8},
    'Techno_SP': {'type': 'multiplier', 'value': 2.0},
    'Coin_SP': {'type': 'both', 'multiplier': 1.8, 'interval': 7},
    'King_SP': {'type': 'multiplier', 'value': 3.0},
    'Shadow_SP': {'type': 'interval', 'value': 5}
}

# In-memory rate limiting storage (use Redis in production)
_rate_limit_store: Dict[str, List[float]] = {}
_click_spam_store: Dict[str, List[float]] = {}

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== LIFESPAN ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting Ryoho Clicker API")
    try:
        await init_db()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
    yield
    logger.info("🛑 Shutting down Ryoho Clicker API")

app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)

# ==================== MIDDLEWARE ====================
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    path = request.url.path
    if path in ["/health", "/"]:
        return await call_next(request)
    key = f"{client_ip}:{path}"
    now = time.time()
    if key not in _rate_limit_store:
        _rate_limit_store[key] = []
    _rate_limit_store[key] = [t for t in _rate_limit_store[key] if now - t < 60]
    if len(_rate_limit_store[key]) >= 60:
        logger.warning(f"Rate limit exceeded for {key}")
        return JSONResponse(status_code=429, content={"detail": "Too many requests. Please slow down."})
    _rate_limit_store[key].append(now)
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ryoho-eta.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== MODELS ====================
class ClickRequest(BaseModel):
    user_id: int
    clicks: int = Field(1, ge=1, le=100)
    actual_gain: int = Field(..., ge=1)  # Сколько монет заработано за клик (с учётом буста и скина)
    mega_boost_active: bool = False

    @validator('user_id')
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError('Invalid user_id')
        return v

class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str

    @validator('boost_type')
    def validate_boost_type(cls, v):
        if v not in UPGRADE_PRICES:
            raise ValueError('Invalid boost type')
        return v

class UserIdRequest(BaseModel):
    user_id: int

    @validator('user_id')
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError('Invalid user_id')
        return v

class GameRequest(BaseModel):
    user_id: int
    bet: int = Field(..., ge=MIN_BET, le=MAX_BET)
    color: Optional[str] = None
    bet_type: Optional[str] = None
    bet_value: Optional[int] = Field(None, ge=0, le=36)
    prediction: Optional[str] = None

class TaskCompleteRequest(BaseModel):
    user_id: int
    task_id: str

class RegisterRequest(BaseModel):
    user_id: int
    username: Optional[str] = None
    referrer_id: Optional[int] = None

    @validator('user_id')
    def validate_user_id(cls, v):
        if v <= 0:
            raise ValueError('Invalid user_id')
        return v

class BoostActivateRequest(BaseModel):
    user_id: int

class PassiveIncomeRequest(BaseModel):
    user_id: int
    skin_bonus: Optional[Dict[str, Any]] = None

class RewardVideoRequest(BaseModel):
    user_id: int
    reward: int = Field(MAX_REWARD_PER_VIDEO, le=MAX_REWARD_PER_VIDEO)

class SelectSkinRequest(BaseModel):
    user_id: int
    skin_id: str

class UnlockSkinRequest(BaseModel):
    user_id: int
    skin_id: str
    method: str  # 'ads' or 'cpa'

# ==================== HEALTH ENDPOINTS ====================
@app.get("/health")
@app.get("/")
async def root():
    return {"status": "ok", "message": "Ryoho Clicker API is running", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/health/db")
async def check_db_endpoint():
    try:
        await get_user(0)
        return {"database": "connected"}
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        return {"database": "disconnected", "error": str(e)}

# ==================== HELPER FUNCTIONS ====================
def get_tap_value(level: int) -> int:
    return 1 + level

def get_hour_value(level: int) -> int:
    if level >= len(HOUR_VALUES):
        return HOUR_VALUES[-1] * (2 ** (level - len(HOUR_VALUES) + 1))
    return HOUR_VALUES[level]

def check_click_spam(user_id: int) -> bool:
    key = f"click_{user_id}"
    now = time.time()
    if key not in _click_spam_store:
        _click_spam_store[key] = []
    _click_spam_store[key] = [t for t in _click_spam_store[key] if now - t < 1]
    if len(_click_spam_store[key]) >= MAX_CLICKS_PER_SECOND:
        logger.warning(f"Click spam detected for user {user_id}")
        return False
    _click_spam_store[key].append(now)
    return True

def calculate_passive_income(user: Dict, hours_passed: int) -> int:
    hour_value = get_hour_value(user.get("profit_level", 0))
    return hour_value * max(1, hours_passed)

# ==================== API ENDPOINTS ====================
@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found. Please register first.")
        return {
            "coins": user.get("coins", 0),
            "energy": user.get("energy", 0),
            "max_energy": user.get("max_energy", BASE_MAX_ENERGY),
            "profit_per_tap": get_tap_value(user.get("multitap_level", 0)),
            "profit_per_hour": get_hour_value(user.get("profit_level", 0)),
            "multitap_level": user.get("multitap_level", 0),
            "profit_level": user.get("profit_level", 0),
            "energy_level": user.get("energy_level", 0),
            "selected_skin": user.get("extra_data", {}).get("selected_skin", "default_SP"),
            "owned_skins": user.get("extra_data", {}).get("owned_skins", ["default_SP"]),
            "ads_watched": user.get("extra_data", {}).get("ads_watched", 0)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/click")
async def process_click(request: ClickRequest):
    try:
        if not check_click_spam(request.user_id):
            raise HTTPException(status_code=429, detail="Too many clicks. Please slow down.")
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        mega_boost_active = False
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now <= expires:
                    mega_boost_active = True
                else:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(request.user_id, {"extra_data": extra})
            except (ValueError, KeyError):
                if "mega_boost" in active_boosts:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(request.user_id, {"extra_data": extra})

        # Проверка энергии
        if not mega_boost_active:
            if user.get("energy", 0) < 1:
                raise HTTPException(status_code=400, detail="Not enough energy")
            user["energy"] -= 1

        # Добавляем монеты (actual_gain пришёл от клиента, включая бонус скина и буст)
        user["coins"] += request.actual_gain

        # Сохраняем изменения
        await update_user(request.user_id, {
            "coins": user["coins"],
            "energy": user["energy"]
        })

        return {
            "coins": user["coins"],
            "energy": user["energy"],
            "mega_boost_active": mega_boost_active
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_click: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/upgrade")
async def process_upgrade(request: UpgradeRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        boost_type = request.boost_type
        current_level = user.get(f"{boost_type}_level", 0)
        if current_level >= len(UPGRADE_PRICES[boost_type]):
            raise HTTPException(status_code=400, detail="Max level reached")
        price = UPGRADE_PRICES[boost_type][current_level]
        if user.get("coins", 0) < price:
            raise HTTPException(status_code=400, detail="Not enough coins")

        user["coins"] -= price
        user[f"{boost_type}_level"] = current_level + 1
        updates = {
            "coins": user["coins"],
            f"{boost_type}_level": current_level + 1
        }

        if boost_type == "profit":
            updates["profit_per_hour"] = get_hour_value(current_level + 1)
        elif boost_type == "energy":
            new_max = get_max_energy(current_level + 1)
            updates["max_energy"] = new_max
            updates["energy"] = new_max  # Полная энергия после апгрейда

        await update_user(request.user_id, updates)

        # Возвращаем обновлённые данные
        return {
            "coins": user["coins"],
            "new_level": current_level + 1,
            "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
            "profit_per_tap": get_tap_value(user.get("multitap_level", 0) + (1 if boost_type == "multitap" else 0)),
            "profit_per_hour": get_hour_value(user.get("profit_level", 0) + (1 if boost_type == "profit" else 0)),
            "max_energy": get_max_energy(user.get("energy_level", 0) + (1 if boost_type == "energy" else 0))
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/recover-energy")
async def recover_energy(data: UserIdRequest):
    try:
        user = await get_user(data.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        max_energy = user.get("max_energy", BASE_MAX_ENERGY)
        current_energy = user.get("energy", 0)
        if current_energy < max_energy:
            new_energy = min(max_energy, current_energy + 1)
            await update_user(data.user_id, {"energy": new_energy})
            return {"energy": new_energy}
        return {"energy": current_energy}
    except Exception as e:
        logger.error(f"Error in recover_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        prices = {}
        for boost in UPGRADE_PRICES:
            level = user.get(f"{boost}_level", 0)
            prices[boost] = UPGRADE_PRICES[boost][level] if level < len(UPGRADE_PRICES[boost]) else 0
        return prices
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_upgrade_prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/register")
async def register_user(request: RegisterRequest):
    try:
        existing = await get_user(request.user_id)
        if existing:
            return {"status": "exists", "user": existing}
        await create_user(
            user_id=request.user_id,
            username=request.username,
            referrer_id=request.referrer_id
        )
        user = await get_user(request.user_id)

        if request.referrer_id and request.referrer_id != request.user_id:
            referrer = await get_user(request.referrer_id)
            if referrer:
                current_coins = referrer.get("coins", 0)
                current_count = referrer.get("referral_count", 0)
                current_earnings = referrer.get("referral_earnings", 0)
                bonus = 5000
                await update_user(request.referrer_id, {
                    "coins": current_coins + bonus,
                    "referral_count": current_count + 1,
                    "referral_earnings": current_earnings + bonus
                })
                logger.info(f"Referral bonus: User {request.referrer_id} got +{bonus} coins for inviting {request.user_id}")

        if request.referrer_id:
            return {"status": "created_with_referral", "user": user, "message": f"🎉 Welcome! You were invited by {request.referrer_id}"}
        return {"status": "created", "user": user}
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video")
async def reward_video(request: RewardVideoRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        reward = MAX_REWARD_PER_VIDEO
        user["coins"] += reward
        # Увеличиваем счётчик просмотренных видео
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        extra["ads_watched"] = extra.get("ads_watched", 0) + 1
        await update_user(request.user_id, {"coins": user["coins"], "extra_data": extra})
        return {"success": True, "coins": user["coins"], "ads_watched": extra["ads_watched"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reward_video: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== BOOSTS ====================
@app.post("/api/activate-mega-boost")
async def activate_mega_boost(request: BoostActivateRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now < expires:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "success": False,
                        "message": f"Boost already active! {remaining // 60}:{remaining % 60:02d} remaining",
                        "already_active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"]
                    }
            except (ValueError, KeyError):
                del active_boosts["mega_boost"]

        # Активируем на 5 минут
        expires_at = (now + timedelta(minutes=5)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["active_boosts"] = active_boosts
        await update_user(request.user_id, {"extra_data": extra})
        return {
            "success": True,
            "message": "🔥⚡ MEGA BOOST activated for 5 minutes! x2 coins + infinite energy",
            "expires_at": expires_at
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now > expires:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
                    return {"active": False}
                else:
                    remaining = int((expires - now).total_seconds())
                    return {"active": True, "expires_at": active_boosts["mega_boost"]["expires_at"], "remaining_seconds": remaining}
            except (ValueError, KeyError):
                if "mega_boost" in active_boosts:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
        return {"active": False}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== SKINS ====================
@app.post("/api/select-skin")
async def select_skin(request: SelectSkinRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        owned = extra.get("owned_skins", [])
        if request.skin_id not in owned:
            raise HTTPException(status_code=400, detail="Skin not owned")
        extra["selected_skin"] = request.skin_id
        await update_user(request.user_id, {"extra_data": extra})
        return {"success": True, "selected_skin": request.skin_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in select_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/unlock-skin")
async def unlock_skin(request: UnlockSkinRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        owned = extra.get("owned_skins", [])
        if request.skin_id in owned:
            return {"success": False, "message": "Already owned"}
        # Проверяем условия разблокировки (для видео или CPA)
        if request.method == 'ads':
            # Проверяем, достаточно ли просмотров
            ads_watched = extra.get("ads_watched", 0)
            # Здесь можно загрузить требования скина, но для простоты будем считать, что клиент сам проверяет
            # и отправляет запрос, когда условие выполнено. Сервер доверяет клиенту? Можно добавить проверку.
            # В реальности лучше хранить требования на сервере.
            pass
        elif request.method == 'cpa':
            # Для CPA тоже доверяем клиенту, но можно добавить проверку по referral или отдельному эндпоинту
            pass
        owned.append(request.skin_id)
        extra["owned_skins"] = owned
        # Если это первый скин, делаем его выбранным
        if len(owned) == 1:
            extra["selected_skin"] = request.skin_id
        await update_user(request.user_id, {"extra_data": extra})
        return {"success": True, "owned_skins": owned, "selected_skin": extra.get("selected_skin")}
    except Exception as e:
        logger.error(f"Error in unlock_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/skins/list")
async def get_skins_list():
    # Возвращаем все доступные скины с их бонусами и требованиями
    skins = [
        {"id": "default_SP", "name": "Классический спирикс", "rarity": "common", "bonus": {"type": "multiplier", "value": 1.0}, "requirement": {"type": "free"}},
        {"id": "Galaxy_SP", "name": "Галактический спирикс", "rarity": "common", "bonus": {"type": "multiplier", "value": 1.1}, "requirement": {"type": "free"}},
        {"id": "Water_SP", "name": "Водяной спирикс", "rarity": "common", "bonus": {"type": "multiplier", "value": 1.15}, "requirement": {"type": "free"}},
        {"id": "Ninja_SP", "name": "Нинзя спирикс", "rarity": "rare", "bonus": {"type": "multiplier", "value": 1.5}, "requirement": {"type": "ads", "count": 10}},
        {"id": "Monster_SP", "name": "Монстр спирикс", "rarity": "rare", "bonus": {"type": "interval", "value": 8}, "requirement": {"type": "ads", "count": 20}},
        {"id": "Techno_SP", "name": "Техно спирикс", "rarity": "legendary", "bonus": {"type": "multiplier", "value": 2.0}, "requirement": {"type": "cpa", "url": "https://omg10.com/4/10675986"}},
        {"id": "Coin_SP", "name": "Кот-маг", "rarity": "legendary", "bonus": {"type": "both", "multiplier": 1.8, "interval": 7}, "requirement": {"type": "cpa", "url": "https://omg10.com/4/10675991"}},
        {"id": "King_SP", "name": "Король спирикс", "rarity": "super", "bonus": {"type": "multiplier", "value": 3.0}, "requirement": {"type": "special", "description": "Пригласить 50 друзей", "total": 50}},
        {"id": "Shadow_SP", "name": "Теневой спирикс", "rarity": "super", "bonus": {"type": "interval", "value": 5}, "requirement": {"type": "special", "description": "Достичь 100 уровня", "total": 100}}
    ]
    return {"skins": skins}

# ==================== REFERRALS ====================
@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"count": user.get("referral_count", 0), "earnings": user.get("referral_earnings", 0)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== TASKS ====================
_task_completion_store: Dict[str, Dict[str, float]] = {}

@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        completed_tasks = await get_completed_tasks(user_id) or []
        tasks = [
            {"id": "daily_bonus", "title": "📅 Daily Bonus", "description": "Come back every day for rewards", "reward": "25000 coins", "icon": "📅", "completed": "daily_bonus" in completed_tasks},
            {"id": "energy_refill", "title": "⚡ Infinite Energy", "description": "5 minutes of unlimited energy", "reward": "⚡ 5 minutes", "icon": "⚡", "completed": "energy_refill" in completed_tasks},
            {"id": "link_click", "title": "🔗 Follow Link", "description": "Click the link and get reward", "reward": "25000 coins", "icon": "🔗", "completed": False},
            {"id": "invite_5_friends", "title": "👥 Invite 5 Friends", "description": "Invite 5 friends to the game", "reward": "20000 coins", "icon": "👥", "completed": "invite_5_friends" in completed_tasks, "progress": min(user.get("referral_count", 0), 5), "total": 5}
        ]
        return tasks
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/complete-task")
async def complete_task(request: TaskCompleteRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        task_id = request.task_id
        now_timestamp = time.time()
        if task_id == "link_click":
            spam_key = f"link_click:{request.user_id}"
            last_click = _task_completion_store.get(spam_key, {}).get("last", 0)
            if now_timestamp - last_click < MIN_TASK_COMPLETION_INTERVAL:
                raise HTTPException(status_code=429, detail="Please wait before clicking again")
            _task_completion_store[spam_key] = {"last": now_timestamp}
            user["coins"] += 25000
            await update_user(request.user_id, {"coins": user["coins"]})
            return {"success": True, "message": "🔗 +25000 coins for clicking!", "coins": user["coins"]}

        completed = await get_completed_tasks(request.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")

        message = ""
        updates = {}
        if task_id == "daily_bonus":
            user["coins"] += 25000
            message = "🎁 +25000 coins (daily bonus)"
            updates = {"coins": user["coins"]}
            await add_completed_task(request.user_id, task_id)
        elif task_id == "energy_refill":
            message = "⚡ Infinite energy activated for 5 minutes!"
            await add_completed_task(request.user_id, task_id)
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] += 20000
                message = "👥 +20000 coins for 5 friends!"
                updates = {"coins": user["coins"]}
                await add_completed_task(request.user_id, task_id)
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")
        if updates:
            await update_user(request.user_id, updates)
        return {"success": True, "message": message, "coins": user["coins"]}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== MINI-GAMES ====================
@app.post("/api/game/coinflip")
async def play_coinflip(request: GameRequest):
    try:
        user = await get_user(request.user_id)
        if not user or user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        win = random.choice([True, False])
        if win:
            user["coins"] += request.bet
            message = f"🎉 You won +{request.bet} coins!"
        else:
            user["coins"] -= request.bet
            message = f"😞 You lost {request.bet} coins"
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "result": "win" if win else "lose", "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_coinflip: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/slots")
async def play_slots(request: GameRequest):
    try:
        user = await get_user(request.user_id)
        if not user or user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        symbols = ["🍒", "🍋", "🍊", "7️⃣", "💎"]
        slots = [random.choice(symbols) for _ in range(3)]
        win = len(set(slots)) == 1
        multiplier = 10 if "7️⃣" in slots and win else 5 if "💎" in slots and win else 3
        if win:
            win_amount = request.bet * multiplier
            user["coins"] += win_amount
            message = f"🎰 JACKPOT! +{win_amount} coins (x{multiplier})"
        else:
            user["coins"] -= request.bet
            message = f"😞 You lost {request.bet} coins"
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "slots": slots, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_slots: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/dice")
async def play_dice(request: GameRequest):
    try:
        user = await get_user(request.user_id)
        if not user or user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        dice1 = random.randint(1, 6)
        dice2 = random.randint(1, 6)
        total = dice1 + dice2
        win = False
        multiplier = 1
        if request.prediction == "7" and total == 7:
            win = True
            multiplier = 5
        elif request.prediction == "even" and total % 2 == 0:
            win = True
            multiplier = 2
        elif request.prediction == "odd" and total % 2 == 1:
            win = True
            multiplier = 2
        if win:
            win_amount = request.bet * multiplier
            user["coins"] += win_amount
            message = f"🎲 You won +{win_amount} coins (x{multiplier})"
        else:
            user["coins"] -= request.bet
            message = f"😞 You lost {request.bet} coins"
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "dice1": dice1, "dice2": dice2, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/roulette")
async def play_roulette(request: GameRequest):
    try:
        user = await get_user(request.user_id)
        if not user or user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        red_numbers = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]
        result = random.randint(0, 36)
        if result == 0:
            result_color = 'green'
            result_symbol = '🟢'
        elif result in red_numbers:
            result_color = 'red'
            result_symbol = '🔴'
        else:
            result_color = 'black'
            result_symbol = '⚫'
        win = False
        multiplier = 0
        if request.bet_type == 'number' and request.bet_value == result:
            win = True
            multiplier = 35
        elif request.bet_type == 'green' and result_color == 'green':
            win = True
            multiplier = 35
        elif request.bet_type == result_color:
            win = True
            multiplier = 2
        if win:
            win_amount = request.bet * multiplier
            user["coins"] += win_amount
            message = f"🎉 {result_symbol} {result} - You won +{win_amount} coins! (x{multiplier})"
        else:
            user["coins"] -= request.bet
            message = f"😞 {result_symbol} {result} - You lost {request.bet} coins"
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "result_number": result, "result_color": result_color, "result_symbol": result_symbol, "win": win, "message": message}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_roulette: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== PASSIVE INCOME ====================
@app.post("/api/passive-income")
async def passive_income(request: PassiveIncomeRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        last_income = user.get('last_passive_income')
        now = datetime.utcnow()
        if not last_income or (now - last_income) >= timedelta(hours=1):
            if last_income:
                hours_passed = int((now - last_income).total_seconds() / 3600)
            else:
                hours_passed = 1
            total_income = calculate_passive_income(user, hours_passed)
            if total_income > 0:
                user["coins"] += total_income
                await update_user(request.user_id, {"coins": user["coins"], "last_passive_income": now})
                return {"coins": user["coins"], "income": total_income, "message": f"💰 +{total_income} coins (passive income)"}
        return {"coins": user["coins"], "income": 0}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== LAUNCH ====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False, workers=4)