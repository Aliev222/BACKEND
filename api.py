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
import aioredis
from dotenv import load_dotenv

from DATABASE.base import (
    get_user, add_user as create_user, update_user,
    init_db, get_completed_tasks, add_completed_task
)

load_dotenv()

# ==================== КОНФИГУРАЦИЯ ====================

MAX_REWARD_PER_VIDEO = 5000
MAX_BET = 1000000
MIN_BET = 10
BASE_MAX_ENERGY = 500

# ==================== ЦЕНЫ АПГРЕЙДОВ ====================

UPGRADE_PRICES = {
    "multitap": [50, 75, 100, 150, 200, 300, 450, 650, 900, 1200],
    "profit": [40, 60, 90, 130, 180, 240, 310, 390, 480, 580],
    "energy": [30, 45, 65, 90, 120, 155, 195, 240, 290, 345]
}

HOUR_VALUES = [100, 150, 250, 500, 1000, 1250, 1500, 1800, 2000, 2500]

# ==================== КЭШ (REDIS) ====================

redis_client = None
click_queue = asyncio.Queue()
user_cache = {}  # Локальный кэш пользователей

async def init_redis():
    global redis_client
    try:
        redis_client = await aioredis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379"),
            encoding="utf-8",
            decode_responses=True
        )
        print("✅ Redis connected")
    except:
        print("⚠️ Redis not available, using memory cache only")
        redis_client = None

# ==================== ФОНОВЫЕ ЗАДАЧИ ====================

async def click_processor():
    """Обработка кликов пачками (раз в 3 секунды)"""
    while True:
        try:
            # Собираем все клики за последние 3 секунды
            batch = []
            for _ in range(1000):  # Максимум 1000 кликов за раз
                try:
                    click = await asyncio.wait_for(click_queue.get(), timeout=0.01)
                    batch.append(click)
                except asyncio.TimeoutError:
                    break
            
            if batch:
                # Группируем по пользователям
                user_data = defaultdict(lambda: {'clicks': 0, 'gain': 0})
                for click in batch:
                    uid = click['user_id']
                    user_data[uid]['clicks'] += 1
                    user_data[uid]['gain'] += click['gain']
                
                # Сохраняем в кэш и БД
                for uid, data in user_data.items():
                    # Обновляем кэш
                    if uid in user_cache:
                        user_cache[uid]['coins'] += data['gain']
                        if not click.get('mega_boost'):
                            user_cache[uid]['energy'] = max(0, user_cache[uid]['energy'] - data['clicks'])
                    
                    # Асинхронно обновляем БД
                    asyncio.create_task(update_user_db(uid, data))
                
                print(f"✅ Processed {len(batch)} clicks for {len(user_data)} users")
        
        except Exception as e:
            print(f"❌ Click processor error: {e}")
        
        await asyncio.sleep(3)  # Сохраняем раз в 3 секунды

async def update_user_db(user_id: int, data: dict):
    """Обновление пользователя в БД"""
    try:
        user = await get_user(user_id)
        if user:
            await update_user(user_id, {
                "coins": user.get("coins", 0) + data['gain'],
                "energy": max(0, user.get("energy", 0) - data['clicks'])
            })
    except Exception as e:
        print(f"❌ DB update error for user {user_id}: {e}")

# ==================== LOGGING ====================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== LIFESPAN ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Запуск и остановка сервера"""
    logger.info("🚀 Starting Ryoho Clicker API")
    
    # Инициализация
    await init_db()
    await init_redis()
    
    # Запуск фоновых задач
    asyncio.create_task(click_processor())
    logger.info("✅ Background tasks started")
    
    yield
    
    # Очистка при остановке
    if redis_client:
        await redis_client.close()
    logger.info("🛑 Shutting down")

app = FastAPI(title="Ryoho Clicker API", lifespan=lifespan)

# ==================== CORS ====================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ryoho-eta.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== МОДЕЛИ ====================

class ClickRequest(BaseModel):
    user_id: int
    clicks: int = 1
    gain: int
    mega_boost: bool = False

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

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def get_tap_value(level: int) -> int:
    return 1 + level

def get_hour_value(level: int) -> int:
    return HOUR_VALUES[min(level, len(HOUR_VALUES)-1)]

def get_max_energy(level: int) -> int:
    return min(1000, BASE_MAX_ENERGY + level * 5)

# ==================== ЭНДПОИНТЫ ====================

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    """Быстрое получение данных пользователя (из кэша или БД)"""
    try:
        # Сначала проверяем кэш
        if user_id in user_cache:
            return user_cache[user_id]
        
        # Если нет в кэше - грузим из БД
        user = await get_user(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Формируем ответ
        user_data = {
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
        
        # Сохраняем в кэш
        user_cache[user_id] = user_data
        
        return user_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/click")
async def process_click(request: ClickRequest):
    """СУПЕР-БЫСТРЫЙ клик (просто кладем в очередь)"""
    try:
        # Мгновенно кладем в очередь
        await click_queue.put({
            'user_id': request.user_id,
            'gain': request.gain,
            'clicks': request.clicks,
            'mega_boost': request.mega_boost
        })
        
        # Если есть кэш - обновляем его сразу для UI
        if request.user_id in user_cache:
            user_cache[request.user_id]['coins'] += request.gain
            if not request.mega_boost:
                user_cache[request.user_id]['energy'] = max(0, 
                    user_cache[request.user_id]['energy'] - request.clicks)
        
        # Мгновенный ответ!
        return {
            "success": True,
            "queued": True,
            "cached": request.user_id in user_cache
        }
        
    except Exception as e:
        logger.error(f"Error queueing click: {e}")
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

        if boost_type == "energy":
            new_max = get_max_energy(current_level + 1)
            updates["max_energy"] = new_max
            updates["energy"] = new_max

        await update_user(request.user_id, updates)
        
        # Обновляем кэш
        if request.user_id in user_cache:
            user_cache[request.user_id]['coins'] = user["coins"]
            if boost_type == "energy":
                user_cache[request.user_id]['max_energy'] = updates["max_energy"]
                user_cache[request.user_id]['energy'] = updates["energy"]

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

        return {"status": "created", "user": await get_user(request.user_id)}
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== МИНИ-ИГРЫ ====================

@app.post("/api/game/coinflip")
async def play_coinflip(request: GameRequest):
    """Игра в орлянку"""
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
        
        # Обновляем кэш
        if request.user_id in user_cache:
            user_cache[request.user_id]['coins'] = user["coins"]
        
        return {"success": True, "coins": user["coins"], "message": message}
    except Exception as e:
        logger.error(f"Error in coinflip: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/slots")
async def play_slots(request: GameRequest):
    """Игровой автомат"""
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
        
        if request.user_id in user_cache:
            user_cache[request.user_id]['coins'] = user["coins"]
        
        return {"success": True, "coins": user["coins"], "slots": slots, "message": message}
    except Exception as e:
        logger.error(f"Error in slots: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/api/game/dice")
async def play_dice(request: GameRequest):
    """Игра в кости"""
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
        
        if request.user_id in user_cache:
            user_cache[request.user_id]['coins'] = user["coins"]
        
        return {"success": True, "coins": user["coins"], "dice1": dice1, "dice2": dice2, "message": message}
    except Exception as e:
        logger.error(f"Error in dice: {e}")
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
        
        task_id = request.task_id
        
        if task_id == "link_click":
            user["coins"] += 25000
            await update_user(request.user_id, {"coins": user["coins"]})
            if request.user_id in user_cache:
                user_cache[request.user_id]['coins'] = user["coins"]
            return {"success": True, "message": "🔗 +25000 coins!", "coins": user["coins"]}
        
        completed = await get_completed_tasks(request.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")
        
        if task_id == "daily_bonus":
            user["coins"] += 25000
            await add_completed_task(request.user_id, task_id)
            await update_user(request.user_id, {"coins": user["coins"]})
            if request.user_id in user_cache:
                user_cache[request.user_id]['coins'] = user["coins"]
            return {"success": True, "message": "🎁 +25000 coins!", "coins": user["coins"]}
        
        elif task_id == "energy_refill":
            await add_completed_task(request.user_id, task_id)
            return {"success": True, "message": "⚡ Energy refill activated!"}
        
        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                user["coins"] += 20000
                await add_completed_task(request.user_id, task_id)
                await update_user(request.user_id, {"coins": user["coins"]})
                if request.user_id in user_cache:
                    user_cache[request.user_id]['coins'] = user["coins"]
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
        
        if not last_income or (now - last_income) >= timedelta(hours=1):
            if last_income:
                hours_passed = int((now - last_income).total_seconds() / 3600)
            else:
                hours_passed = 1
            
            hour_value = get_hour_value(user.get("profit_level", 0))
            total_income = hour_value * max(1, hours_passed)
            
            if total_income > 0:
                user["coins"] += total_income
                await update_user(request.user_id, {
                    "coins": user["coins"],
                    "last_passive_income": now
                })
                
                if request.user_id in user_cache:
                    user_cache[request.user_id]['coins'] = user["coins"]
                
                return {"success": True, "coins": user["coins"], "income": total_income, 
                        "message": f"💰 +{total_income} coins"}
        
        return {"success": True, "coins": user["coins"], "income": 0}
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== СКИНЫ ====================

@app.get("/api/skins/list")
async def get_skins_list():
    skins = [
        {"id": "default_SP", "name": "Классический спирикс", "image": "imgg/skins/default_SP.png", 
         "rarity": "common", "bonus": {"type": "multiplier", "value": 1.0}, "requirement": {"type": "free"}},
        {"id": "Galaxy_SP", "name": "Галактический спирикс", "image": "imgg/skins/Galaxy_SP.png", 
         "rarity": "common", "bonus": {"type": "multiplier", "value": 1.1}, "requirement": {"type": "free"}},
        {"id": "Ninja_SP", "name": "Нинзя спирикс", "image": "imgg/skins/Ninja_SP.png", 
         "rarity": "rare", "bonus": {"type": "multiplier", "value": 1.5}, 
         "requirement": {"type": "ads", "count": 10}}
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
        
        # Обновляем кэш
        if request.user_id in user_cache:
            user_cache[request.user_id]['selected_skin'] = request.skin_id
        
        return {"success": True, "selected_skin": request.skin_id}
    except Exception as e:
        logger.error(f"Error in select_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)