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

UPGRADE_PRICES = {
    "multitap": [
        50, 75, 100, 150, 200, 300, 450, 650, 900, 1200,  # 1-10
        1600, 2100, 2700, 3400, 4200, 5100, 6100, 7200, 8400, 9700,  # 11-20
        11100, 12600, 14200, 15900, 17700, 19600, 21600, 23700, 25900, 28200,  # 21-30
        30600, 33100, 35700, 38400, 41200, 44100, 47100, 50200, 53400, 56700,  # 31-40
        60100, 63600, 67200, 70900, 74700, 78600, 82600, 86700, 90900, 95200,  # 41-50
        100000, 105000, 110000, 115000, 120000, 125000, 130000, 135000, 140000, 145000,  # 51-60
        150000, 160000, 170000, 180000, 190000, 200000, 210000, 220000, 230000, 240000,  # 61-70
        250000, 270000, 290000, 310000, 330000, 350000, 370000, 390000, 410000, 430000,  # 71-80
        450000, 500000, 550000, 600000, 650000, 700000, 750000, 800000, 850000, 900000,  # 81-90
        950000, 1000000, 1100000, 1200000, 1300000, 1400000, 1500000, 1600000, 1700000, 1800000],
    "profit": [
        40, 60, 90, 130, 180, 240, 310, 390, 480, 580,  # 1-10
        690, 810, 940, 1080, 1230, 1390, 1560, 1740, 1930, 2130,  # 11-20
        2340, 2560, 2790, 3030, 3280, 3540, 3810, 4090, 4380, 4680,  # 21-30
        4990, 5310, 5640, 5980, 6330, 6690, 7060, 7440, 7830, 8230,  # 31-40
        8640, 9060, 9490, 9930, 10380, 10840, 11310, 11790, 12280, 12780,  # 41-50
        13300, 13850, 14420, 15010, 15620, 16250, 16900, 17570, 18260, 18970,  # 51-60
        19700, 20450, 21220, 22010, 22820, 23650, 24500, 25370, 26260, 27170,  # 61-70
        28100, 29050, 30020, 31010, 32020, 33050, 34100, 35170, 36260, 37370,  # 71-80
        38500, 39650, 40820, 42010, 43220, 44450, 45700, 46970, 48260, 49570,  # 81-90
        50900, 52250, 53620, 55010, 56420, 57850, 59300, 60770, 62260, 63770   # 91-100
    ],
     "energy": [
        30, 45, 65, 90, 120, 155, 195, 240, 290, 345,  # 1-10
        405, 470, 540, 615, 695, 780, 870, 965, 1065, 1170,  # 11-20
        1280, 1395, 1515, 1640, 1770, 1905, 2045, 2190, 2340, 2495,  # 21-30
        2655, 2820, 2990, 3165, 3345, 3530, 3720, 3915, 4115, 4320,  # 31-40
        4530, 4745, 4965, 5190, 5420, 5655, 5895, 6140, 6390, 6645,  # 41-50
        6905, 7170, 7440, 7715, 7995, 8280, 8570, 8865, 9165, 9470,  # 51-60
        9780, 10095, 10415, 10740, 11070, 11405, 11745, 12090, 12440, 12795,  # 61-70
        13155, 13520, 13890, 14265, 14645, 15030, 15420, 15815, 16215, 16620,  # 71-80
        17030, 17445, 17865, 18290, 18720, 19155, 19595, 20040, 20490, 20945,  # 81-90
        21405, 21870, 22340, 22815, 23295, 23780, 24270, 24765, 25265, 25770   # 91-100
    ],
}


HOUR_VALUES = [
    # 1-10
    100, 150, 250, 500, 1000, 1250, 1500, 1800, 2000, 2500,
    # 11-20
    3000, 4000, 5000, 6000, 7000, 10000, 12000, 14000, 16000, 18000,
    # 21-30
    20000, 23000, 26000, 29000, 32000, 36000, 40000, 44000, 48000, 52000,
    # 31-40
    56000, 61000, 66000, 71000, 76000, 82000, 88000, 94000, 100000, 107000,
    # 41-50
    114000, 121000, 128000, 135000, 142000, 150000, 150000, 150000, 150000, 150000,
    # 51-60
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    # 61-70
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    # 71-80
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    # 81-90
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000,
    # 91-100
    150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000, 150000]
ENERGY_VALUES = [
    # 1-10
    300, 330, 370, 420, 480, 550, 630, 720, 820, 930,
    # 11-20
    1050, 1180, 1320, 1470, 1630, 1800, 1980, 2170, 2370, 2580,
    # 21-30
    2800, 3030, 3270, 3520, 3780, 4050, 4330, 4620, 4920, 5230,
    # 31-40
    5550, 5880, 6220, 6570, 6930, 7300, 7680, 8070, 8470, 8880,
    # 41-50
    9300, 9730, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000,
    # 51-60
    10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000,
    # 61-70
    10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000,
    # 71-80
    10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000,
    # 81-90
    10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000,
    # 91-100
    10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000]

SKIN_BONUSES = {
    'default_SP': {'type': 'multiplier', 'value': 1.0},
    'Galaxy_SP': {'type': 'multiplier', 'value': 1.3},
    'Water_SP': {'type': 'multiplier', 'value': 1.4},
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
    """Startup and shutdown events"""
    logger.info("🚀 Starting Ryoho Clicker API")
    try:
        await init_db()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
    yield
    logger.info("🛑 Shutting down Ryoho Clicker API")

# ==================== INITIALIZATION ====================

app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)

# ==================== MIDDLEWARE ====================
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Basic rate limiting middleware"""
    client_ip = request.client.host if request.client else "unknown"
    path = request.url.path
    
    # Skip rate limiting for health checks
    if path in ["/health", "/"]:
        return await call_next(request)
    
    # Rate limit by IP and endpoint
    key = f"{client_ip}:{path}"
    now = time.time()
    
    if key not in _rate_limit_store:
        _rate_limit_store[key] = []
    
    # Clean old requests (older than 60 seconds)
    _rate_limit_store[key] = [t for t in _rate_limit_store[key] if now - t < 60]
    
    # Check rate (max 60 requests per minute per endpoint)
    if len(_rate_limit_store[key]) >= 60:
        logger.warning(f"Rate limit exceeded for {key}")
        return JSONResponse(
            status_code=429,
            content={"detail": "Too many requests. Please slow down."}
        )
    
    _rate_limit_store[key].append(now)
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )

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
    energy_cost: int = Field(1, ge=1, le=10)
    
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

# ==================== HEALTH ENDPOINTS ====================

@app.get("/health")
@app.get("/")
async def root():
    return {
        "status": "ok", 
        "message": "Ryoho Clicker API is running",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/health/db")
async def check_db_endpoint():
    """Separate endpoint for DB health check"""
    try:
        await get_user(0)
        return {"database": "connected"}
    except Exception as e:
        logger.error(f"DB health check failed: {e}")
        return {"database": "disconnected", "error": str(e)}

# ==================== HELPER FUNCTIONS ====================

def get_tap_value(level: int) -> int:
    """Calculate tap value based on level"""
    return 1 + level

def get_hour_value(level: int) -> int:
    """Calculate hourly profit based on level"""
    if level >= len(HOUR_VALUES):
        return HOUR_VALUES[-1] * (2 ** (level - len(HOUR_VALUES) + 1))
    return HOUR_VALUES[level]

def get_max_energy(level: int) -> int:
    """Calculate max energy based on level"""
    if level >= len(ENERGY_VALUES):
        return ENERGY_VALUES[-1] * (1.5 ** (level - len(ENERGY_VALUES) + 1))
    return ENERGY_VALUES[level]

def check_click_spam(user_id: int) -> bool:
    """Check if user is clicking too fast"""
    key = f"click_{user_id}"
    now = time.time()
    
    if key not in _click_spam_store:
        _click_spam_store[key] = []
    
    # Clean old clicks (older than 1 second)
    _click_spam_store[key] = [t for t in _click_spam_store[key] if now - t < 1]
    
    if len(_click_spam_store[key]) >= MAX_CLICKS_PER_SECOND:
        logger.warning(f"Click spam detected for user {user_id}")
        return False
    
    _click_spam_store[key].append(now)
    return True

def calculate_passive_income(user: Dict, hours_passed: int) -> int:
    """Calculate passive income based on hours passed"""
    hour_value = get_hour_value(user.get("profit_level", 0))
    # Просто умножаем часовой доход на количество прошедших часов
    return hour_value * max(1, hours_passed)

# ==================== API ENDPOINTS ====================

@app.get("/api/check-referral/{user_id}")
async def check_referral(user_id: int):
    """Check if user has a referrer"""
    try:
        user = await get_user(user_id)
        if not user:
            return {"has_referrer": False}
        
        return {
            "has_referrer": user.get("referrer_id") is not None,
            "referrer_id": user.get("referrer_id")
        }
    except Exception as e:
        logger.error(f"Error in check_referral: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    """Get user data"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found. Please register first.")
        
        return {
            "coins": user.get("coins", 0),
            "energy": user.get("energy", 0),
            "max_energy": user.get("max_energy", 1000),
            "profit_per_tap": get_tap_value(user.get("multitap_level", 0)),
            "profit_per_hour": get_hour_value(user.get("profit_level", 0)),
            "multitap_level": user.get("multitap_level", 0),
            "profit_level": user.get("profit_level", 0),
            "energy_level": user.get("energy_level", 0),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/click")
async def process_click(request: ClickRequest):
    """Process a click with anti-cheat protection"""
    try:
        # Anti-spam check
        if not check_click_spam(request.user_id):
            raise HTTPException(status_code=429, detail="Too many clicks. Please slow down.")
        
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Check mega boost
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
                # Invalid boost data, remove it
                del active_boosts["mega_boost"]
                extra["active_boosts"] = active_boosts
                await update_user(request.user_id, {"extra_data": extra})
        
        # Calculate gain
        base_tap = get_tap_value(user.get("multitap_level", 0))
        actual_gain = base_tap * (2 if mega_boost_active else 1)
        
        # Validate energy
        if not mega_boost_active:
            if user.get("energy", 0) < 1:
                raise HTTPException(status_code=400, detail="Not enough energy")
            user["energy"] = user.get("energy", 0) - 1
        
        # Update coins
        user["coins"] = user.get("coins", 0) + actual_gain
        
        # Save to DB
        await update_user(request.user_id, {
            "coins": user["coins"],
            "energy": user["energy"]
        })
        
        return {
            "coins": user["coins"],
            "energy": user["energy"],
            "tap_value": base_tap,
            "multiplier": 2 if mega_boost_active else 1,
            "actual_gain": actual_gain,
            "mega_boost_active": mega_boost_active
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_click: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/upgrade")
async def process_upgrade(request: UpgradeRequest):
    """Process an upgrade with validation"""
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
        
        # Atomic update simulation (use DB transaction in production)
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
            updates["energy"] = new_max
        
        await update_user(request.user_id, updates)
        
        # Get fresh user data
        updated_user = await get_user(request.user_id)
        
        return {
            "coins": updated_user["coins"],
            "new_level": updated_user[f"{boost_type}_level"],
            "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
            "profit_per_tap": get_tap_value(updated_user["multitap_level"]),
            "profit_per_hour": get_hour_value(updated_user["profit_level"]),
            "max_energy": updated_user["max_energy"],
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
        
        # Получаем реальный max_energy из БД
        max_energy = user.get("max_energy", 1000)
        current_energy = user.get("energy", 0)
        
        if current_energy >= max_energy:
            return {"energy": current_energy}
        
        # +1 за вызов (каждые 5 секунд)
        new_energy = min(max_energy, current_energy + 1)
        await update_user(data.user_id, {"energy": new_energy})
        
        return {"energy": new_energy}
        
    except Exception as e:
        logger.error(f"Error in recover_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    """Get current upgrade prices for user"""
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
    """Register new user with referral"""
    try:
        # Check if user exists
        existing = await get_user(request.user_id)
        if existing:
            return {"status": "exists", "user": existing}
        
        # Create new user
        await create_user(
            user_id=request.user_id, 
            username=request.username,
            referrer_id=request.referrer_id
        )
        
        user = await get_user(request.user_id)

        # Обработка реферала
        if request.referrer_id and request.referrer_id != request.user_id:  # Защита от само-реферала
            referrer = await get_user(request.referrer_id)
            if referrer:
                # Получаем текущие значения
                current_coins = referrer.get("coins", 0)
                current_count = referrer.get("referral_count", 0)
                current_earnings = referrer.get("referral_earnings", 0)
                
                # Бонус за реферала (увеличил до 5000 как в вашем коде)
                bonus = 5000
                
                # Обновляем данные реферера
                await update_user(request.referrer_id, {
                    "coins": current_coins + bonus,
                    "referral_count": current_count + 1,
                    "referral_earnings": current_earnings + bonus
                })
                
                # Логируем для отладки
                logger.info(f"Referral bonus: User {request.referrer_id} got +{bonus} coins for inviting {request.user_id}")
        
        # Возвращаем ответ
        if request.referrer_id:
            return {
                "status": "created_with_referral",
                "user": user,
                "message": f"🎉 Welcome! You were invited by {request.referrer_id}"
            }
        
        return {"status": "created", "user": user}
        
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video")
async def reward_video(request: RewardVideoRequest):
    """Process video reward with fixed amount"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Fixed reward amount (not from client)
        reward = MAX_REWARD_PER_VIDEO
        
        user["coins"] = user.get("coins", 0) + reward
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {"success": True, "coins": user["coins"]}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reward_video: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
    @app.post("/api/verify-cpa")
    async def verify_cpa(request: dict):
        user_id = request.get("user_id")
        skin_id = request.get("skin_id")
        
        # Здесь должна быть проверка с CPA-сетью
        # Например, проверка уникального идентификатора перехода
        
        # Если подтверждено - разблокируем скин
        user = await get_user(user_id)
        if user:
            # Логика разблокировки скина
            return {"success": True}
        return {"success": False}

# ==================== BOOSTS ====================

@app.post("/api/activate-boost")
@app.post("/api/activate-mega-boost")
async def activate_mega_boost(request: BoostActivateRequest):
    """Activate mega boost (x2 coins + infinite energy for 2 minutes)"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        # Check if boost already active
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
                # Invalid data, remove it
                del active_boosts["mega_boost"]
        
        # Activate boost for 2 minutes
        expires_at = (now + timedelta(minutes=2)).isoformat()
        active_boosts["mega_boost"] = {
            "active": True,
            "expires_at": expires_at
        }
        
        extra["active_boosts"] = active_boosts
        await update_user(request.user_id, {"extra_data": extra})
        
        return {
            "success": True,
            "message": "🔥⚡ MEGA BOOST activated for 2 minutes! x2 coins + infinite energy",
            "expires_at": expires_at
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/boosts/{user_id}")
@app.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int):
    """Get mega boost status"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        # Check and clean expired boost
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
                    return {
                        "active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"],
                        "remaining_seconds": remaining
                    }
            except (ValueError, KeyError):
                # Invalid data, remove it
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

@app.post("/api/verify-cpa")
async def verify_cpa(request: dict):
    """Verify CPA completion for skins"""
    try:
        user_id = request.get("user_id")
        skin_id = request.get("skin_id")
        
        user = await get_user(user_id)
        if not user:
            return {"success": False, "error": "User not found"}
        
        # Здесь должна быть проверка с CPA-сетью
        # Например, проверка в базе данных переходов
        
        # Для теста можно просто разблокировать через 5 секунд
        # Но в реальности нужно проверять с CPA-сетью
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        # Отмечаем, что CPA выполнен
        completed_cpa = extra.get("completed_cpa", [])
        if skin_id not in completed_cpa:
            completed_cpa.append(skin_id)
            extra["completed_cpa"] = completed_cpa
            
            await update_user(user_id, {"extra_data": extra})
            
            return {"success": True, "message": "CPA verified"}
        
        return {"success": False, "message": "Already verified"}
        
    except Exception as e:
        logger.error(f"Error in verify_cpa: {e}")
        return {"success": False, "error": str(e)}

# ==================== REFERRALS ====================

@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int):
    """Get referral statistics"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "count": user.get("referral_count", 0),
            "earnings": user.get("referral_earnings", 0)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/migrate-referrals")
async def migrate_referrals():
    """Migration endpoint (safe to call multiple times)"""
    try:
        from sqlalchemy import create_engine, inspect, text
        
        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        sync_engine = create_engine(db_url.replace("+asyncpg", ""))
        
        with sync_engine.connect() as conn:
            inspector = inspect(sync_engine)
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            added = []
            if 'referrer_id' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN referrer_id BIGINT"))
                added.append('referrer_id')
            if 'referral_count' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN referral_count INTEGER DEFAULT 0"))
                added.append('referral_count')
            if 'referral_earnings' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN referral_earnings BIGINT DEFAULT 0"))
                added.append('referral_earnings')
            if 'created_at' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
                added.append('created_at')
            
            conn.commit()
            
            return {
                "status": "success",
                "message": f"Columns added: {added}",
                "columns": columns + added
            }
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return {"status": "error", "error": str(e)}

# ==================== TASKS ====================

# In-memory task completion tracking (use Redis in production)
_task_completion_store: Dict[str, Dict[str, float]] = {}

@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int):
    """Get available tasks"""
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        completed_tasks = await get_completed_tasks(user_id) or []
        
        tasks = [
            {
                "id": "daily_bonus",
                "title": "📅 Daily Bonus",
                "description": "Come back every day for rewards",
                "reward": "25000 coins",
                "icon": "📅",
                "completed": "daily_bonus" in completed_tasks,
                "progress": 0,
                "total": 1
            },
            {
                "id": "energy_refill",
                "title": "⚡ Infinite Energy",
                "description": "5 minutes of unlimited energy",
                "reward": "⚡ 5 minutes",
                "icon": "⚡",
                "completed": "energy_refill" in completed_tasks,
                "progress": 0,
                "total": 1
            },
            {
                "id": "link_click",
                "title": "🔗 Follow Link",
                "description": "Click the link and get reward",
                "reward": "25000 coins",
                "icon": "🔗",
                "completed": False,  # Always available
                "progress": 0,
                "total": 1
            },
            {
                "id": "invite_5_friends",
                "title": "👥 Invite 5 Friends",
                "description": "Invite 5 friends to the game",
                "reward": "20000 coins",
                "icon": "👥",
                "completed": "invite_5_friends" in completed_tasks,
                "progress": min(user.get("referral_count", 0), 5),
                "total": 5
            }
        ]
        
        return tasks
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/complete-task")
async def complete_task(request: TaskCompleteRequest):
    """Complete a task with anti-cheat protection"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        task_id = request.task_id
        now = datetime.utcnow()
        now_timestamp = time.time()
        
        # Anti-spam for link_click
        if task_id == "link_click":
            spam_key = f"link_click:{request.user_id}"
            last_click = _task_completion_store.get(spam_key, {}).get("last", 0)
            
            if now_timestamp - last_click < MIN_TASK_COMPLETION_INTERVAL:
                logger.warning(f"Link click spam detected for user {request.user_id}")
                raise HTTPException(status_code=429, detail="Please wait before clicking again")
            
            _task_completion_store[spam_key] = {"last": now_timestamp}
            
            # Award coins
            user["coins"] = user.get("coins", 0) + 25000
            await update_user(request.user_id, {"coins": user["coins"]})
            
            return {
                "success": True, 
                "message": "🔗 +25000 coins for clicking!",
                "coins": user["coins"]
            }
        
        # One-time tasks
        completed = await get_completed_tasks(request.user_id) or []
        
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")
        
        message = ""
        updates = {}
        
        if task_id == "daily_bonus":
            user["coins"] = user.get("coins", 0) + 25000
            message = "🎁 +25000 coins (daily bonus)"
            updates = {"coins": user["coins"]}
            await add_completed_task(request.user_id, task_id)
            
        elif task_id == "energy_refill":
            message = "⚡ Infinite energy activated for 5 minutes!"
            await add_completed_task(request.user_id, task_id)
            
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] = user.get("coins", 0) + 20000
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
    """Play coinflip game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        win = random.choice([True, False])
        
        if win:
            user["coins"] = user.get("coins", 0) + request.bet
            message = f"🎉 You won +{request.bet} coins!"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "result": "win" if win else "lose",
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_coinflip: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/slots")
async def play_slots(request: GameRequest):
    """Play slots game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
            raise HTTPException(status_code=400, detail="Not enough coins")
        
        symbols = ["🍒", "🍋", "🍊", "7️⃣", "💎"]
        slots = [random.choice(symbols) for _ in range(3)]
        win = len(set(slots)) == 1
        multiplier = 10 if "7️⃣" in slots and win else 5 if "💎" in slots and win else 3
        
        if win:
            win_amount = request.bet * multiplier
            user["coins"] = user.get("coins", 0) + win_amount
            message = f"🎰 JACKPOT! +{win_amount} coins (x{multiplier})"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "slots": slots,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_slots: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/dice")
async def play_dice(request: GameRequest):
    """Play dice game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
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
            user["coins"] = user.get("coins", 0) + win_amount
            message = f"🎲 You won +{win_amount} coins (x{multiplier})"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "dice1": dice1,
            "dice2": dice2,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/roulette")
async def play_roulette(request: GameRequest):
    """Play roulette game"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if user.get("coins", 0) < request.bet:
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
            user["coins"] = user.get("coins", 0) + win_amount
            message = f"🎉 {result_symbol} {result} - You won +{win_amount} coins! (x{multiplier})"
        else:
            user["coins"] = user.get("coins", 0) - request.bet
            message = f"😞 {result_symbol} {result} - You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        
        return {
            "coins": user["coins"],
            "result_number": result,
            "result_color": result_color,
            "result_symbol": result_symbol,
            "win": win,
            "message": message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in play_roulette: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== PASSIVE INCOME ====================

@app.post("/api/passive-income")
async def passive_income(request: PassiveIncomeRequest):
    """Collect passive income (per hour) - NO SKIN BONUS"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        last_income = user.get('last_passive_income')
        now = datetime.utcnow()
        
        # Проверяем, прошёл ли хотя бы час
        if not last_income or (now - last_income) >= timedelta(hours=1):
            # Сколько часов прошло?
            if last_income:
                hours_passed = int((now - last_income).total_seconds() / 3600)
            else:
                hours_passed = 1  # Первый сбор - за 1 час
            
            # Рассчитываем доход (БЕЗ МНОЖИТЕЛЯ СКИНОВ)
            total_income = calculate_passive_income(user, hours_passed)
            
            if total_income > 0:
                user["coins"] = user.get("coins", 0) + total_income
                await update_user(request.user_id, {
                    "coins": user["coins"],
                    "last_passive_income": now
                })
                
                return {
                    "coins": user["coins"],
                    "income": total_income,
                    "message": f"💰 +{total_income} coins (passive income)"
                }
        
        return {"coins": user["coins"], "income": 0}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== LAUNCH ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "api:app", 
        host="0.0.0.0", 
        port=port, 
        reload=False,  # Disable reload in production
        workers=4      # Multiple workers for concurrency
    )