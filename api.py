from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import asyncio
import uvicorn
import random
import time
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from collections import defaultdict
from sqlalchemy import select
from DATABASE.base import User, AsyncSessionLocal
from datetime import datetime, timedelta
from fastapi import HTTPException
from pydantic import BaseModel, Field
from collections import defaultdict, deque
from dataclasses import dataclass
import redis.asyncio as redis

from DATABASE.base import (
    get_user, add_user as create_user, update_user,
    init_db, get_completed_tasks, add_completed_task
)



# ==================== КОНФИГУРАЦИЯ ====================

MAX_REWARD_PER_VIDEO = 5000
MAX_BET = 1000000
MIN_BET = 10
BASE_MAX_ENERGY = 500
ENERGY_REGEN_SECONDS = 2  # 1 энергия каждые 5 секунд

# ==================== АНТИСПАМ / АНТИЧИТ ====================

MAX_REAL_CLICKS_PER_SECOND = 25   # честный быстрый таппер
CLICK_BURST_ALLOWANCE = 15        # небольшой запас на батч
MAX_CLICK_BATCH_SIZE = 200        # жёсткий серверный потолок на один батч

RATE_LIMITS = {
    "reward_video": (5, 60),         # 5 запросов в минуту
    "activate_mega_boost": (10, 60), # 10 в минуту
    "update_energy": (10, 60),       # 10 в минуту
    "complete_task": (20, 60),       # 20 в минуту
    "cpa_status": (60, 60),          # 60 в минуту
    "game_action": (30, 60),         # 30 в минуту на мини-игры
}

rate_limit_store = defaultdict(deque)

REDIS_URL = os.getenv("REDIS_URL")
redis_client = None
# ==================== ЦЕНЫ АПГРЕЙДОВ ====================

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

HOUR_VALUES = [
    10, 15, 22, 32, 45, 62, 83, 108, 138, 173,
    215, 265, 324, 393, 473, 565, 670, 789, 923, 1073,
    1240, 1425, 1629, 1853, 2098, 2365, 2655, 2969, 3308, 3673,
    4065, 4485, 4934, 5413, 5923, 6465, 7040, 7649, 8293, 8973,
    9690, 10445, 11239, 12073, 12948, 13865, 14825, 15829, 16878, 17973,
    19115, 20305, 21544, 22833, 24173, 25565, 27010, 28509, 30063, 31673,
    33340, 35065, 36849, 38693, 40598, 42565, 44595, 46689, 48848, 51073,
    51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073,
    51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073,
    51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073, 51073
]



# ==================== LOGGING ====================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



user_cache = {}
# ==================== ТУРНИРНЫЕ ДАННЫЕ ==================

def mask_username(username):
    """Оставляет первые 2 и последние 2 символа, остальное звездочки"""
    if not username:
        return "Player"
    
    username = str(username)
    if len(username) <= 4:
        return username  # Короткие ники не шифруем
    
    # Берем первые 2 и последние 2 символа
    first_two = username[:2]
    last_two = username[-2:]
    middle_len = len(username) - 4
    
    # Добавляем звездочки в середину
    masked = f"{first_two}{'*' * min(middle_len, 3)}{last_two}"
    return masked


# ==================== Вспомогательные функции антиспама ====================


def check_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """
    Простая in-memory защита от спама.
    True = запрос можно пропустить
    False = лимит превышен
    """
    now = time.time()
    bucket = rate_limit_store[key]

    # Удаляем старые записи за пределами окна
    while bucket and bucket[0] <= now - window_seconds:
        bucket.popleft()

    if len(bucket) >= limit:
        return False

    bucket.append(now)
    return True


def require_rate_limit(namespace: str, user_id: int, limit: int, window_seconds: int):
    """
    Бросает 429, если пользователь превысил лимит.
    """
    key = f"{namespace}:{user_id}"
    if not check_rate_limit(key, limit, window_seconds):
        raise HTTPException(
            status_code=429,
            detail="Too many requests"
        )


def get_allowed_clicks(user: dict, now: datetime, requested_clicks: int) -> int:
    """
    Ограничиваем число кликов в батче по времени с прошлого серверного апдейта.
    Не тормозит UX, но режет нереалистичный спам.
    """
    last_update = _normalize_dt(user.get("last_energy_update"))

    # На первом батче даём разумный стартовый лимит
    if not last_update:
        return min(requested_clicks, 60, MAX_CLICK_BATCH_SIZE)

    elapsed = max(0.0, (now - last_update).total_seconds())

    # Сколько честно могло накопиться кликов за это время
    allowed_by_time = int(elapsed * MAX_REAL_CLICKS_PER_SECOND) + CLICK_BURST_ALLOWANCE

    # Минимальный запас, чтобы честный игрок не упирался в ноль
    allowed = max(1, min(allowed_by_time, MAX_CLICK_BATCH_SIZE))

    return min(requested_clicks, allowed)



# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client

    logger.info("🚀 Starting Ryoho Clicker API")

    await init_db()
    logger.info("✅ Database initialized")

    if REDIS_URL:
        redis_client = redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry_on_timeout=True,
        )
        try:
            await redis_client.ping()
            logger.info("✅ Redis connected")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            redis_client = None
    else:
        logger.warning("⚠️ REDIS_URL is not set")

    logger.info("✅ Background tasks started")
    yield

    if redis_client:
        await redis_client.close()

    logger.info("🛑 Shutting down")

# ==================== CORS ====================
app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://spirix.vercel.app",
        "https://web.telegram.org",
        "https://telegram.org",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== МОДЕЛИ ====================

class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str

class UserIdRequest(BaseModel):
    user_id: int

class RegisterRequest(BaseModel):
    user_id: int
    username: Optional[str] = None
    referrer_id: Optional[int] = None

class SkinRequest(BaseModel):
    user_id: int
    skin_id: str

class GameRequest(BaseModel):
    user_id: int
    bet: int = Field(..., ge=10, le=1000000)
    prediction: Optional[str] = None
    bet_type: Optional[str] = None
    bet_value: Optional[int] = None

class TaskCompleteRequest(BaseModel):
    user_id: int
    task_id: str

class PassiveIncomeRequest(BaseModel):
    user_id: int

class UserIdRequest(BaseModel):
    user_id: int


class BoostActivateRequest(BaseModel):
    user_id: int

class EnergySyncRequest(BaseModel):
    user_id: int

class ClicksBatchRequest(BaseModel):
    user_id: int
    clicks: int = Field(..., ge=1, le=500)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def get_tap_value(level: int) -> int:
    return 1 + level

def get_hour_value(level: int) -> int:
    return HOUR_VALUES[min(level, len(HOUR_VALUES)-1)]

def get_max_energy(level: int) -> int:
    return min(1000, BASE_MAX_ENERGY + level * 5)

# ==================== ЭНДПОИНТЫ ====================

async def redis_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """
    True = можно пропустить
    False = лимит превышен
    """
    if redis_client is None:
        return True  # fallback, чтобы игра не падала

    current = await redis_client.incr(key)
    if current == 1:
        await redis_client.expire(key, window_seconds)

    return current <= limit


async def require_redis_rate_limit(namespace: str, user_id: int, limit: int, window_seconds: int):
    allowed = await redis_rate_limit(f"rl:{namespace}:{user_id}", limit, window_seconds)
    if not allowed:
        raise HTTPException(status_code=429, detail="Too many requests")


def _normalize_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def calculate_current_energy(user: dict, now: datetime | None = None) -> int:
    """Считает актуальную энергию на сервере по stored energy + времени."""
    now = now or datetime.utcnow()

    stored_energy = int(user.get("energy", 0))
    max_energy = int(user.get("max_energy", 500))
    last_update = _normalize_dt(user.get("last_energy_update"))

    if stored_energy >= max_energy:
        return max_energy

    if not last_update:
        return min(stored_energy, max_energy)

    seconds_passed = max(0, int((now - last_update).total_seconds()))
    gained = seconds_passed // ENERGY_REGEN_SECONDS

    return min(max_energy, stored_energy + gained)


def build_energy_payload(user: dict, now: datetime | None = None) -> dict:
    """Готовит серверный снимок энергии для фронта."""
    now = now or datetime.utcnow()
    max_energy = int(user.get("max_energy", 500))
    current_energy = calculate_current_energy(user, now)

    return {
        "energy": current_energy,
        "max_energy": max_energy,
        "regen_seconds": ENERGY_REGEN_SECONDS,
        "server_time": now.isoformat()
    }


@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        current_energy = calculate_current_energy(user, now)
        max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))

        # сохраняем baseline, чтобы сервер и клиент смотрели на одну точку
        await update_user(user_id, {
            "energy": current_energy,
            "last_energy_update": now
        })

        return {
            "user_id": user["user_id"],
            "username": user.get("username"),
            "coins": user.get("coins", 0),
            "energy": current_energy,
            "max_energy": max_energy,
            "profit_per_tap": user.get("profit_per_tap", 1),
            "profit_per_hour": user.get("profit_per_hour", 100),
            "multitap_level": user.get("multitap_level", 0),
            "profit_level": user.get("profit_level", 0),
            "energy_level": user.get("energy_level", 0),
            "owned_skins": (user.get("extra_data", {}) or {}).get("owned_skins", ["default_SP"]),
            "selected_skin": (user.get("extra_data", {}) or {}).get("selected_skin", "default_SP"),
            "ads_watched": (user.get("extra_data", {}) or {}).get("ads_watched", 0),
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int):
    """Get mega boost status"""
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
            except:
                pass
        
        return {"active": False}
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/activate-mega-boost")
async def activate_mega_boost(request: BoostActivateRequest):
    """Activate mega boost (x2 coins + infinite energy for 5 minutes)"""
    try:
        user = await get_user(request.user_id)
        await require_redis_rate_limit("activate_mega_boost", request.user_id, 10, 60)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        require_rate_limit("activate_mega_boost", request.user_id, *RATE_LIMITS["activate_mega_boost"])

        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()
        
        # Проверяем, не активен ли уже буст
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
            except:
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
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/reward-video")
async def reward_video(request: dict):
    """Handle rewarded video watch"""
    try:
        user_id = request.get("user_id")
        
        await require_redis_rate_limit("reward_video", user_id, 5, 60)

        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        require_rate_limit("reward_video", user_id, *RATE_LIMITS["reward_video"])

        # Начисляем награду
        reward = 5000
        user["coins"] += reward
        
        # Обновляем счетчик просмотренных видео
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        extra["ads_watched"] = extra.get("ads_watched", 0) + 1
        
        await update_user(user_id, {
            "coins": user["coins"],
            "extra_data": extra
        })
        
        # Обновляем кэш
        
        
        return {
            "success": True,
            "coins": user["coins"],
            "ads_watched": extra["ads_watched"]
        }
        
    except Exception as e:
        logger.error(f"Error in reward_video: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/ad-watched")
async def ad_watched(request: dict):
    """Track ad watch statistics"""
    try:
        user_id = request.get("user_id")
        reward_type = request.get("reward_type")
        
        user = await get_user(user_id)
        if not user:
            return {"success": False}
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        # Сохраняем статистику
        ads_history = extra.get("ads_history", [])
        ads_history.append({
            "type": reward_type,
            "timestamp": datetime.utcnow().isoformat()
        })
        extra["ads_history"] = ads_history
        
        await update_user(user_id, {"extra_data": extra})
        
        return {"success": True}
        
    except Exception as e:
        logger.error(f"Error in ad_watched: {e}")
        return {"success": False}

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

        if boost_type == "energy":
            new_max = get_max_energy(current_level + 1)
            updates["max_energy"] = new_max
            updates["energy"] = new_max

        await update_user(request.user_id, updates)
        
        # Обновляем кэш
        
        return {
            "success": True,
            "coins": user["coins"],
            "new_level": current_level + 1,
            "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] 
                if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
            "profit_per_tap": get_tap_value(user.get("multitap_level", 0) + 
                (1 if boost_type == "multitap" else 0)),
            "profit_per_hour": get_hour_value(user.get("profit_level", 0) + 
                (1 if boost_type == "profit" else 0)),
            "max_energy": get_max_energy(user.get("energy_level", 0) + 
                (1 if boost_type == "energy" else 0))
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/update-energy")
async def update_energy(request: dict):
    try:
        user_id = request.get("user_id")
        await require_redis_rate_limit("update_energy", user_id, 10, 60)
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id required")

        require_rate_limit("update_energy", user_id, *RATE_LIMITS["update_energy"])

        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        max_energy = int(user.get("max_energy", 500))

        

        return {
            "success": True,
            "energy": max_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/recover-energy")
async def recover_energy_legacy(request: UserIdRequest):
    """Старый эндпоинт для обратной совместимости"""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        max_energy = user.get("max_energy", BASE_MAX_ENERGY)
        current_energy = user.get("energy", 0)
        
       
        
        if current_energy < max_energy:
            new_energy = min(max_energy, current_energy + 3)
            
            await update_user(request.user_id, {
                "energy": new_energy,
                "last_energy_update": datetime.utcnow()
            })
            
            
            
            
            return {"energy": new_energy}
        
        return {"energy": current_energy}
    except Exception as e:
       
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/sync-energy")
async def sync_energy(request: EnergySyncRequest):
    """Серверный sync энергии без сброса таймера регена."""
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()

        old_energy = int(user.get("energy", 0))
        max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))
        last_update = _normalize_dt(user.get("last_energy_update"))

        current_energy = calculate_current_energy(user, now)

        update_data = {}

        # Обновляем baseline только если энергия реально выросла
        if current_energy != old_energy:
            update_data["energy"] = current_energy

            if last_update:
                seconds_passed = max(0, int((now - last_update).total_seconds()))
                gained = seconds_passed // ENERGY_REGEN_SECONDS

                if gained > 0:
                    update_data["last_energy_update"] = last_update + timedelta(
                        seconds=gained * ENERGY_REGEN_SECONDS
                    )
            else:
                update_data["last_energy_update"] = now

        # Если энергия уже полная, держим baseline консистентным
        if current_energy >= max_energy and not update_data.get("last_energy_update"):
            update_data["last_energy_update"] = now
            update_data["energy"] = max_energy

        if update_data:
            await update_user(request.user_id, update_data)

            

        return {
            "success": True,
            "energy": current_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in sync_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

SKIN_MULTIPLIERS = {
    "default_SP": 1.0,

    "skin_lvl_1": 1.1,
    "skin_lvl_2": 1.2,
    "skin_lvl_3": 1.3,
    "skin_lvl_4": 1.4,
    "skin_lvl_5": 1.5,
    "skin_lvl_6": 1.6,
    "skin_lvl_7": 2.0,

    "skin_video_1": 1.2,
    "skin_video_2": 1.3,
    "skin_video_3": 1.4,
    "skin_video_4": 1.5,
    "skin_video_5": 1.75,
    "skin_video_6": 2.0,

    "skin_friend_1": 1.1,
    "skin_friend_2": 1.2,
    "skin_friend_3": 1.3,
    "skin_friend_4": 1.5,
    "skin_friend_5": 1.75,
    "skin_friend_6": 2.0,

    "skin_cpa_1": 2.5,
}


def get_selected_skin_multiplier(user: dict) -> float:
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        return 1.0

    selected_skin = extra.get("selected_skin", "default_SP")
    return SKIN_MULTIPLIERS.get(selected_skin, 1.0)


def is_mega_boost_active(user: dict) -> bool:
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        return False

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


@app.post("/api/clicks")
async def process_clicks_batch(request: ClicksBatchRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()

        max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))
        current_energy = calculate_current_energy(user, now)

        multitap_level = int(user.get("multitap_level", 0))
        tap_value = get_tap_value(multitap_level)

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        selected_skin = extra.get("selected_skin", "default_SP")
        skin_multiplier = float(SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = is_mega_boost_active(user)

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2

        # Серверная защита от нереалистичного количества кликов
        safe_requested_clicks = min(request.clicks, MAX_CLICK_BATCH_SIZE)
        allowed_clicks = get_allowed_clicks(user, now, safe_requested_clicks)

        effective_clicks = min(allowed_clicks, current_energy)
        gained = effective_clicks * coin_per_tap

        new_energy = max(0, current_energy - effective_clicks)
        new_coins = int(user.get("coins", 0)) + gained

        await update_user(request.user_id, {
            "coins": new_coins,
            "energy": new_energy,
            "last_energy_update": now
        })

        return {
            "success": True,
            "coins": new_coins,
            "energy": new_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "gained": gained,
            "effective_clicks": effective_clicks,
            "coin_per_tap": coin_per_tap,
            "profit_per_tap": tap_value,
            "profit_per_hour": get_hour_value(int(user.get("profit_level", 0))),
            "mega_boost_active": mega_boost_active
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_clicks_batch: {e}")
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
                new_coins = referrer.get("coins", 0) + 5000
                new_count = referrer.get("referral_count", 0) + 1
                new_earnings = referrer.get("referral_earnings", 0) + 5000
                
                await update_user(request.referrer_id, {
                    "coins": new_coins,
                    "referral_count": new_count,
                    "referral_earnings": new_earnings
                })
                
                # Обновляем кэш
                
                
                logger.info(f"✅ Referral bonus: {request.referrer_id} got +5000 for {request.user_id}")

        return {"status": "created", "user": await get_user(request.user_id)}
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")




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

# ==================== CPA ENDPOINTS ====================

_cpa_store = {}

@app.post("/api/cpa-status")
async def cpa_status(request: dict):
    """Проверка статуса CPA-задания"""
    try:
        user_id = request.get("user_id")
        if user_id:
            require_rate_limit("cpa_status", user_id, *RATE_LIMITS["cpa_status"])
        offer_id = request.get("offer_id")
        check_only = request.get("check_only", False)
        
        cpa_key = f"cpa_{user_id}_{offer_id}"
        
        if check_only:
            return {"completed": cpa_key in _cpa_store and _cpa_store[cpa_key].get("completed", False)}
        
        if cpa_key not in _cpa_store:
            _cpa_store[cpa_key] = {
                "start_time": time.time(),
                "completed": False
            }
            return {"completed": False}
        
        elapsed = time.time() - _cpa_store[cpa_key]["start_time"]
        
        if elapsed > 30 and not _cpa_store[cpa_key]["completed"]:
            _cpa_store[cpa_key]["completed"] = True
            
            user = await get_user(user_id)
            if user:
                rewards = {
                    "cpa_1": 50000,
                    "cpa_2": 100000,
                    "cpa_3": 25000
                }
                reward = rewards.get(offer_id, 50000)
                
                user["coins"] += reward
                await update_user(user_id, {"coins": user["coins"]})
                
                
                
                logger.info(f"CPA completed: user {user_id}, offer {offer_id}, reward {reward}")
            
            return {"completed": True}
        
        return {"completed": False}
        
    except Exception as e:
        logger.error(f"CPA status error: {e}")
        return {"completed": False}

# ==================== МИНИ-ИГРЫ ====================

@app.post("/api/game/coinflip")
async def play_coinflip(request: GameRequest):
    try:
        await require_redis_rate_limit("game_action", request.user_id, 30, 60)

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

        return {"success": True, "coins": user["coins"], "message": message}
    except Exception as e:
        logger.error(f"Error in coinflip: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/slots")
async def play_slots(request: GameRequest):
    try:
        await require_redis_rate_limit("game_action", request.user_id, 30, 60)

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

        return {"success": True, "coins": user["coins"], "slots": slots, "message": message}
    except Exception as e:
        logger.error(f"Error in slots: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/dice")
async def play_dice(request: GameRequest):
    try:
        await require_redis_rate_limit("game_action", request.user_id, 30, 60)

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

        return {
            "success": True,
            "coins": user["coins"],
            "dice1": dice1,
            "dice2": dice2,
            "message": message
        }
    except Exception as e:
        logger.error(f"Error in dice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/game/roulette")
async def play_roulette(request: GameRequest):
    try:
        await require_redis_rate_limit("game_action", request.user_id, 30, 60)

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

        return {
            "success": True,
            "coins": user["coins"],
            "result_number": result,
            "result_color": result_color,
            "result_symbol": result_symbol,
            "win": win,
            "message": message
        }
    except Exception as e:
        logger.error(f"Error in play_roulette: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
# ==================== TOURNAMENT ENDPOINTS ====================

class TournamentData(BaseModel):
    user_id: int
    score: int

@app.get("/api/tournament/leaderboard")
async def get_tournament_leaderboard():
    """Get top 5 players with avatars and masked names"""
    try:
        async with AsyncSessionLocal() as session:
            # Получаем топ-5 игроков по монетам
            result = await session.execute(
                select(User)
                .order_by(User.coins.desc())
                .limit(5)
            )
            top_players = result.scalars().all()
            
            players = []
            for idx, user in enumerate(top_players):
                # Маскируем ник
                masked_name = mask_username(user.username)
                
                # Формируем URL аватарки
                avatar_url = None
                if user.username:
                    avatar_url = f"https://t.me/i/userpic/320/{user.username}.jpg"
                else:
                    avatar_url = "/imgg/default_avatar.png"
                
                players.append({
                    "rank": idx + 1,
                    "user_id": user.user_id,
                    "name": masked_name,
                    "avatar": avatar_url,
                    "score": user.coins
                })
        
        # ✅ ПРАВИЛЬНЫЙ РАСЧЕТ ТАЙМЕРА (до конца дня)
        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time_left = int((tomorrow - now).total_seconds())
        
        return {
            "success": True,
            "players": players,
            "prize_pool": 100000,
            "time_left": time_left  # ← ТЕПЕРЬ ПРАВИЛЬНОЕ ВРЕМЯ
        }
        
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

        
    except Exception as e:
        logger.error(f"Error updating tournament score for {user_id}: {e}")
@app.get("/api/tournament/player-rank/{user_id}")
async def get_player_rank(user_id: int):
    """Get player's rank and coins"""
    try:
        async with AsyncSessionLocal() as session:
            # Получаем текущего пользователя
            user_result = await session.execute(
                select(User).where(User.user_id == user_id)
            )
            user = user_result.scalar_one_or_none()
            
            if not user:
                return {
                    "success": True,
                    "rank": 0,
                    "score": 0,
                    "next_rank_score": 0,
                    "avatar": "/imgg/default_avatar.png"
                }
            
            # Считаем ранг
            rank_result = await session.execute(
                select(User).where(User.coins > user.coins)
            )
            higher_players = rank_result.scalars().all()
            rank = len(higher_players) + 1
            
            # Следующий ранг
            next_rank_score = 0
            if rank > 1:
                next_user_result = await session.execute(
                    select(User)
                    .where(User.coins > user.coins)
                    .order_by(User.coins.asc())
                    .limit(1)
                )
                next_user = next_user_result.scalar_one_or_none()
                if next_user:
                    next_rank_score = next_user.coins - user.coins
            
            # Аватарка
            avatar_url = None
            if user.username:
                avatar_url = f"https://t.me/i/userpic/320/{user.username}.jpg"
            else:
                avatar_url = "/imgg/default_avatar.png"
        
        return {
            "success": True,
            "rank": rank,
            "score": user.coins,
            "next_rank_score": next_rank_score,
            "avatar": avatar_url,
            "name": mask_username(user.username)
        }
        
    except Exception as e:
        logger.error(f"Error getting player rank: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
# ==================== ЗАДАЧИ ====================

_task_completion_store = {}

@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int):
    try:
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        completed_tasks = await get_completed_tasks(user_id) or []
        
        tasks = [
            {"id": "daily_bonus", "title": "📅 Daily Bonus", "description": "Come back every day", 
             "reward": "25000 coins", "icon": "📅", "completed": "daily_bonus" in completed_tasks},
            {"id": "energy_refill", "title": "⚡ Infinite Energy", "description": "5 minutes of unlimited energy", 
             "reward": "⚡ 5 minutes", "icon": "⚡", "completed": "energy_refill" in completed_tasks},
            {"id": "link_click", "title": "🔗 Follow Link", "description": "Click the link and get reward", 
             "reward": "25000 coins", "icon": "🔗", "completed": False},
            {"id": "invite_5_friends", "title": "👥 Invite 5 Friends", "description": "Invite 5 friends", 
             "reward": "20000 coins", "icon": "👥", "completed": "invite_5_friends" in completed_tasks, 
             "progress": min(user.get("referral_count", 0), 5), "total": 5}
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
        require_rate_limit("complete_task", request.user_id, *RATE_LIMITS["complete_task"])

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

# ==================== ПАССИВНЫЙ ДОХОД ====================

@app.post("/api/passive-income")
async def passive_income(request: PassiveIncomeRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        last_income = user.get('last_passive_income')
        now = datetime.utcnow()
        
        # Считаем часы с последнего сбора
        if last_income:
            hours_passed = int((now - last_income).total_seconds() / 3600)
        else:
            hours_passed = 1
        
        # Ограничиваем максимум 24 часа, чтобы не начислить слишком много
        hours_passed = min(hours_passed, 24)
        
        if hours_passed >= 1:
            hour_value = get_hour_value(user.get("profit_level", 0))
            total_income = hour_value * hours_passed
            
            user["coins"] += total_income
            await update_user(request.user_id, {
                "coins": user["coins"],
                "last_passive_income": now
            })
            
            
            return {
                "success": True, 
                "coins": user["coins"], 
                "income": total_income, 
                "message": f"💰 +{total_income} coins за {hours_passed}ч"
            }
        
        return {"success": True, "coins": user["coins"], "income": 0}
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== СКИНЫ ====================

@app.get("/api/skins/list")
async def get_skins_list():
    skins = [
        # ========== ОБЫЧНЫЕ (7 шт) - за уровень ==========
        {
            "id": "skin_lvl_1", 
            "name": "Начинающий спирикс", 
            "image": "imgg/skins/default_SP.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 1.1}, 
            "requirement": {"type": "level", "value": 1}
        },
        {
            "id": "skin_lvl_2", 
            "name": "Опытный спирикс", 
            "image": "imgg/skins/icon.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 1.2}, 
            "requirement": {"type": "level", "value": 10}
        },
        {
            "id": "skin_lvl_3", 
            "name": "Мастер спирикс", 
            "image": "imgg/skins/Galaxy_SP.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 1.3}, 
            "requirement": {"type": "level", "value": 25}
        },
        {
            "id": "skin_lvl_4", 
            "name": "Элитный спирикс", 
            "image": "imgg/skins/Coin_SP.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 1.4}, 
            "requirement": {"type": "level", "value": 50}
        },
        {
            "id": "skin_lvl_5", 
            "name": "Легендарный спирикс", 
            "image": "imgg/skins/Monster_SP.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 1.5}, 
            "requirement": {"type": "level", "value": 75}
        },
        {
            "id": "skin_lvl_6", 
            "name": "Мифический спирикс", 
            "image": "imgg/skins/Ninja_SP.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 1.6}, 
            "requirement": {"type": "level", "value": 100}
        },
        {
            "id": "skin_lvl_7", 
            "name": "Божественный спирикс", 
            "image": "imgg/skins/Shadow_SP.png", 
            "rarity": "common", 
            "bonus": {"type": "multiplier", "value": 2.0}, 
            "requirement": {"type": "level", "value": 150}
        },
        
        # ========== ЗА ВИДЕО (6 шт) ==========
        {
            "id": "skin_video_1", 
            "name": "Звездный спирикс", 
            "image": "imgg/skins/Techno_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.2}, 
            "requirement": {"type": "ads", "count": 1}
        },
        {
            "id": "skin_video_2", 
            "name": "Космический спирикс", 
            "image": "imgg/skins/Water_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.3}, 
            "requirement": {"type": "ads", "count": 5}
        },
        {
            "id": "skin_video_3", 
            "name": "Галактический спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.4}, 
            "requirement": {"type": "ads", "count": 10}
        },
        {
            "id": "skin_video_4", 
            "name": "Небесный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.5}, 
            "requirement": {"type": "ads", "count": 20}
        },
        {
            "id": "skin_video_5", 
            "name": "Божественный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "legendary", 
            "bonus": {"type": "multiplier", "value": 1.75}, 
            "requirement": {"type": "ads", "count": 50}
        },
        {
            "id": "skin_video_6", 
            "name": "Всемогущий спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "legendary", 
            "bonus": {"type": "multiplier", "value": 2.0}, 
            "requirement": {"type": "ads", "count": 100}
        },
        
        # ========== ЗА ДРУЗЕЙ (6 шт) ==========
        {
            "id": "skin_friend_1", 
            "name": "Дружный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.1}, 
            "requirement": {"type": "friends", "count": 1}
        },
        {
            "id": "skin_friend_2", 
            "name": "Популярный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.2}, 
            "requirement": {"type": "friends", "count": 3}
        },
        {
            "id": "skin_friend_3", 
            "name": "Известный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "rare", 
            "bonus": {"type": "multiplier", "value": 1.3}, 
            "requirement": {"type": "friends", "count": 5}
        },
        {
            "id": "skin_friend_4", 
            "name": "Звездный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "legendary", 
            "bonus": {"type": "multiplier", "value": 1.5}, 
            "requirement": {"type": "friends", "count": 10}
        },
        {
            "id": "skin_friend_5", 
            "name": "Легендарный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "legendary", 
            "bonus": {"type": "multiplier", "value": 1.75}, 
            "requirement": {"type": "friends", "count": 20}
        },
        {
            "id": "skin_friend_6", 
            "name": "Император спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "super", 
            "bonus": {"type": "multiplier", "value": 2.0}, 
            "requirement": {"type": "friends", "count": 50}
        },
        
        # ========== ЗА ССЫЛКУ (1 шт) ==========
        {
            "id": "skin_cpa_1", 
            "name": "Тайный спирикс", 
            "image": "imgg/skins/King_SP.png", 
            "rarity": "super", 
            "bonus": {"type": "multiplier", "value": 2.5}, 
            "requirement": {"type": "cpa", "url": "https://example.com"}
        }
    ]
    return {"skins": skins}

@app.post("/api/select-skin")
async def select_skin(request: SkinRequest):
    try:
        user = await get_user(request.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        extra["selected_skin"] = request.skin_id
        await update_user(request.user_id, {"extra_data": extra})
        
        
        return {"success": True, "selected_skin": request.skin_id}
    except Exception as e:
        logger.error(f"Error in select_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/unlock-skin")
async def unlock_skin(request: dict):
    """Unlock skin for user"""
    try:
        user_id = request.get("user_id")
        skin_id = request.get("skin_id")
        method = request.get("method", "ads")
        
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        extra = user.get("extra_data", {})
        if not isinstance(extra, dict):
            extra = {}
        
        owned_skins = extra.get("owned_skins", ["default_SP"])
        
        if skin_id not in owned_skins:
            owned_skins.append(skin_id)
            extra["owned_skins"] = owned_skins
            
            # Если это первый скин, делаем его выбранным
            if len(owned_skins) == 1:
                extra["selected_skin"] = skin_id
            
            await update_user(user_id, {"extra_data": extra})
            
            # Обновляем кэш
           
            
            logger.info(f"✅ Skin {skin_id} unlocked for user {user_id}")
            
            return {
                "success": True,
                "owned_skins": owned_skins,
                "selected_skin": extra.get("selected_skin", skin_id)
            }
        
        return {
            "success": False,
            "message": "Skin already owned",
            "owned_skins": owned_skins
        }
        
    except Exception as e:
        logger.error(f"Error in unlock_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/admin/fix-db")
async def fix_db():
    try:
        from sqlalchemy import text
        from DATABASE.base import AsyncSessionLocal

        async with AsyncSessionLocal() as session:
            await session.execute(text("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS last_energy_update TIMESTAMP
            """))

            await session.execute(text("""
                UPDATE users
                SET last_energy_update = NOW()
                WHERE last_energy_update IS NULL
            """))

            await session.commit()

        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}

# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)