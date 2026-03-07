from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
import asyncio
import uvicorn
import random
import time
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

MAX_REWARD_PER_VIDEO = 5000
MAX_BET = 1000000
MIN_BET = 10
BASE_MAX_ENERGY = 500

UPGRADE_PRICES = {
    "multitap": [
        50, 75, 100, 150, 200, 300, 450, 650, 900, 1200,
        1600, 2100, 2700, 3400, 4200, 5100, 6100, 7200, 8400, 9700,
        11100, 12600, 14200, 15900, 17700, 19600, 21600, 23700, 25900, 28200,
        30600, 33100, 35700, 38400, 41200, 44100, 47100, 50200, 53400, 56700,
        60100, 63600, 67200, 70900, 74700, 78600, 82600, 86700, 90900, 95200,
        100000, 105000, 110000, 115000, 120000, 125000, 130000, 135000, 140000, 145000,
    20000, 160000, 170000, 180000, 190000, 200000, 210000, 220000, 230000, 240000,
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
}

HOUR_VALUES = [
    100, 150, 250, 500, 1000, 1250, 1500, 1800, 2000, 2500,
    3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000,
    13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000, 20000, 20000,
    20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000, 20000,
    20000, 20000, 20000, 20000, 20000,20000,20000,20000,20000,20000,
    20000,20000,20000,20000,20000,20000,20000,20000,20000,20000,
    20000,20000,20000,20000,20000,20000,20000,20000,20000,20000,
    20000,20000,20000,20000,20000,20000,20000,20000,20000,20000,
    20000,20000,20000,20000,20000,20000,20000,20000,20000,20000,
    20000,20000,20000,20000,20000,20000,20000,20000,20000,20000
]

# ==================== HELPER FUNCTIONS ====================

def get_tap_value(level: int) -> int:
    return 1 + level

def get_hour_value(level: int) -> int:
    if level >= len(HOUR_VALUES):
        return HOUR_VALUES[-1] * (2 ** (level - len(HOUR_VALUES) + 1))
    return HOUR_VALUES[level]

def get_max_energy(level: int) -> int:
    return min(1000, BASE_MAX_ENERGY + level * 5)

def calculate_passive_income(user: Dict, hours_passed: int) -> int:
    hour_value = get_hour_value(user.get("profit_level", 0))
    return hour_value * max(1, hours_passed)

# ==================== LOGGING ====================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://ryoho-eta.vercel.app",  # Ваш продакшн фронтенд
        "http://localhost:3000",          # Локальная разработка
        "https://ryoho.onrender.com",     # Сам бэкенд
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    expose_headers=["Content-Length", "X-Request-ID"],
    max_age=600,  # Кэшировать preflight запросы на 10 минут
)

# ==================== MODELS ====================

class ClickRequest(BaseModel):
    user_id: int
    clicks: int = Field(1, ge=1, le=100)
    actual_gain: int = Field(..., ge=1)
    mega_boost_active: bool = False

class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str

class UserIdRequest(BaseModel):
    user_id: int

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

class BoostActivateRequest(BaseModel):
    user_id: int

class PassiveIncomeRequest(BaseModel):
    user_id: int
    skin_bonus: Optional[Dict[str, Any]] = None

class RewardVideoRequest(BaseModel):
    user_id: int

class SelectSkinRequest(BaseModel):
    user_id: int
    skin_id: str

class UnlockSkinRequest(BaseModel):
    user_id: int
    skin_id: str
    method: str

# ==================== HEALTH ENDPOINTS ====================

@app.get("/health")
@app.get("/")
async def root():
    return {"status": "ok", "message": "Ryoho Clicker API is running"}

@app.get("/api/health/db")
async def check_db_endpoint():
    try:
        await get_user(0)
        return {"database": "connected"}
    except:
        return {"database": "disconnected"}

# ==================== MAIN ENDPOINTS ====================

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
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
    """Мгновенная обработка кликов без ограничений"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Проверяем Mega Boost
        extra = user.get("extra_data", {})
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        mega_boost_active = False
        
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now <= expires:
                    mega_boost_active = True
            except:
                pass

        # Обновляем энергию (если не буст)
        if not mega_boost_active:
            user["energy"] = max(0, user.get("energy", 0) - request.clicks)
        
        # Добавляем монеты
        user["coins"] += request.actual_gain

        # Сохраняем
        await update_user(request.user_id, {
            "coins": user["coins"],
            "energy": user["energy"]
        })

        return {
            "success": True,
            "coins": user["coins"],
            "energy": user["energy"]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_click: {e}")
        return {"success": False, "error": str(e)}

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
        updates = {"coins": user["coins"], f"{boost_type}_level": current_level + 1}

        if boost_type == "profit":
            updates["profit_per_hour"] = get_hour_value(current_level + 1)
        elif boost_type == "energy":
            new_max = get_max_energy(current_level + 1)
            updates["max_energy"] = new_max
            updates["energy"] = new_max

        await update_user(request.user_id, updates)

        return {
            "success": True,
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

        if request.referrer_id and request.referrer_id != request.user_id:
            referrer = await get_user(request.referrer_id)
            if referrer:
                await update_user(request.referrer_id, {
                    "coins": referrer.get("coins", 0) + 5000,
                    "referral_count": referrer.get("referral_count", 0) + 1,
                    "referral_earnings": referrer.get("referral_earnings", 0) + 5000
                })

        user = await get_user(request.user_id)
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
        
        user["coins"] += MAX_REWARD_PER_VIDEO
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        extra["ads_watched"] = extra.get("ads_watched", 0) + 1
        
        await update_user(request.user_id, {
            "coins": user["coins"],
            "extra_data": extra
        })
        
        return {"success": True, "coins": user["coins"], "ads_watched": extra["ads_watched"]}
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
        
        expires_at = (now + timedelta(minutes=5)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["active_boosts"] = active_boosts
        
        await update_user(request.user_id, {"extra_data": extra})
        
        return {
            "success": True,
            "message": "🔥 MEGA BOOST activated for 5 minutes!",
            "expires_at": expires_at
        }
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
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
                if now <= expires:
                    remaining = int((expires - now).total_seconds())
                    return {"active": True, "expires_at": active_boosts["mega_boost"]["expires_at"], "remaining_seconds": remaining}
            except:
                pass
        
        return {"active": False}
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== SKINS ====================

@app.get("/api/skins/list")
async def get_skins_list():
    skins = [
        {"id": "default_SP", "name": "Классический спирикс", "description": "Классический спирикс", "image": "imgg/skins/default_SP.png", "rarity": "common", "bonus": {"type": "multiplier", "value": 1.0}, "requirement": {"type": "free"}},
        {"id": "Galaxy_SP", "name": "Галактический спирикс", "description": "Галактический спирикс", "image": "imgg/skins/Galaxy_SP.png", "rarity": "common", "bonus": {"type": "multiplier", "value": 1.1}, "requirement": {"type": "free"}},
        {"id": "Water_SP", "name": "Водяной спирикс", "description": "Водяной спирикс", "image": "imgg/skins/Water_SP.png", "rarity": "common", "bonus": {"type": "multiplier", "value": 1.15}, "requirement": {"type": "free"}},
        {"id": "Ninja_SP", "name": "Нинзя спирикс", "description": "Ловкий нинзя спирикс", "image": "imgg/skins/Ninja_SP.png", "rarity": "rare", "bonus": {"type": "multiplier", "value": 1.5}, "requirement": {"type": "ads", "count": 10, "description": "Просмотреть 10 видео"}},
        {"id": "Monster_SP", "name": "Монстр спирикс", "description": "Монстр спирикс", "image": "imgg/skins/Monster_SP.png", "rarity": "rare", "bonus": {"type": "interval", "value": 8}, "requirement": {"type": "ads", "count": 20, "description": "Просмотреть 20 видео"}},
        {"id": "Techno_SP", "name": "Техно спирикс", "description": "Техно спирикс", "image": "imgg/skins/Techno_SP.png", "rarity": "legendary", "bonus": {"type": "multiplier", "value": 2.0}, "requirement": {"type": "cpa", "url": "https://omg10.com/4/10675986", "description": "Получить скин"}},
        {"id": "Coin_SP", "name": "Кот-маг", "description": "Волшебный кот", "image": "imgg/skins/Coin_SP.png", "rarity": "legendary", "bonus": {"type": "both", "multiplier": 1.8, "interval": 7}, "requirement": {"type": "cpa", "url": "https://omg10.com/4/10675991", "description": "Получить скин"}},
        {"id": "King_SP", "name": "Король спирикс", "description": "Король всех королей", "image": "imgg/skins/King_SP.png", "rarity": "super", "bonus": {"type": "multiplier", "value": 3.0}, "requirement": {"type": "special", "description": "Пригласить 50 друзей", "total": 50}},
        {"id": "Shadow_SP", "name": "Теневой спирикс", "description": "Сама тьма", "image": "imgg/skins/Shadow_SP.png", "rarity": "super", "bonus": {"type": "interval", "value": 5}, "requirement": {"type": "special", "description": "Достичь 100 уровня", "total": 100}}
    ]
    return {"skins": skins}

@app.post("/api/select-skin")
async def select_skin(request: SelectSkinRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        extra["selected_skin"] = request.skin_id
        await update_user(request.user_id, {"extra_data": extra})
        
        return {"success": True, "selected_skin": request.skin_id}
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
        
        owned = extra.get("owned_skins", ["default_SP"])
        if request.skin_id not in owned:
            owned.append(request.skin_id)
            extra["owned_skins"] = owned
        
        await update_user(request.user_id, {"extra_data": extra})
        
        return {"success": True, "owned_skins": owned}
    except Exception as e:
        logger.error(f"Error in unlock_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== REFERRALS ====================

@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"count": user.get("referral_count", 0), "earnings": user.get("referral_earnings", 0)}
    except Exception as e:
        logger.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== TASKS ====================

_task_completion_store = {}

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
        
        if task_id == "link_click":
            user["coins"] += 25000
            await update_user(request.user_id, {"coins": user["coins"]})
            return {"success": True, "message": "🔗 +25000 coins!", "coins": user["coins"]}
        
        completed = await get_completed_tasks(request.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")
        
        if task_id == "daily_bonus":
            user["coins"] += 25000
            await add_completed_task(request.user_id, task_id)
            await update_user(request.user_id, {"coins": user["coins"]})
            return {"success": True, "message": "🎁 +25000 coins!", "coins": user["coins"]}
        
        elif task_id == "energy_refill":
            await add_completed_task(request.user_id, task_id)
            return {"success": True, "message": "⚡ Energy refill activated!"}
        
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] += 20000
                await add_completed_task(request.user_id, task_id)
                await update_user(request.user_id, {"coins": user["coins"]})
                return {"success": True, "message": "👥 +20000 coins!", "coins": user["coins"]}
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")
        
        raise HTTPException(status_code=400, detail="Unknown task")
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
        return {"coins": user["coins"], "message": message}
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
            message = f"🎰 JACKPOT! +{win_amount} coins!"
        else:
            user["coins"] -= request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "slots": slots, "message": message}
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
            message = f"🎲 You won +{win_amount} coins!"
        else:
            user["coins"] -= request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "dice1": dice1, "dice2": dice2, "message": message}
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
        elif result in red_numbers:
            result_color = 'red'
        else:
            result_color = 'black'
        
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
            message = f"🎉 You won +{win_amount} coins!"
        else:
            user["coins"] -= request.bet
            message = f"😞 You lost {request.bet} coins"
        
        await update_user(request.user_id, {"coins": user["coins"]})
        return {"coins": user["coins"], "result_number": result, "message": message}
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
                await update_user(request.user_id, {
                    "coins": user["coins"],
                    "last_passive_income": now
                })
                return {"coins": user["coins"], "income": total_income, "message": f"💰 +{total_income} coins"}
        
        return {"coins": user["coins"], "income": 0}
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== LAUNCH ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)