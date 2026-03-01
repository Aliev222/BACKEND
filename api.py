from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import uvicorn
import random
from datetime import datetime, timedelta
import os
from typing import Optional

from DATABASE.base import get_user, add_user as create_user, update_user, init_db, get_completed_tasks, add_completed_task

# ==================== КОНФИГУРАЦИЯ ====================

UPGRADE_PRICES = {
    "multitap": [50, 200, 500, 2000, 8000, 32000, 128000, 512000, 2048000, 8192000],
    "profit":   [100, 400, 1000, 4000, 16000, 64000, 256000, 1024000, 4096000, 16384000],
    "energy":   [80, 300, 800, 3000, 12000, 48000, 192000, 768000, 3072000, 12288000],
    "luck":     [500, 2000, 5000, 20000, 50000, 200000, 500000, 2000000, 5000000, 20000000],
}

TAP_VALUES = [1, 2, 5, 10, 20, 40, 80, 160, 320, 640, 1280]
HOUR_VALUES = [100, 150, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000]
ENERGY_VALUES = [1000, 1100, 1250, 1500, 2000, 3000, 5000, 8000, 13000, 21000, 34000]

# ==================== ИНИЦИАЛИЗАЦИЯ ====================

app = FastAPI(title="Ryoho Clicker API")

@app.get("/health")
@app.get("/")
async def root():
    # Простой ответ без проверки БД
    return {
        "status": "ok", 
        "message": "Ryoho Clicker API is running",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/api/health/db")
async def check_db_endpoint():
    """Отдельный эндпоинт для проверки БД"""
    try:
        await get_user(0)
        return {"database": "connected"}
    except Exception as e:
        return {"database": "disconnected", "error": str(e)}


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
    clicks: int
    energy_cost: int = 1
    
class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str

class UserIdRequest(BaseModel):
    user_id: int

class GameRequest(BaseModel):
    user_id: int
    bet: int
    color: Optional[str] = None
    bet_type: Optional[str] = None
    bet_value: Optional[int] = None
    prediction: Optional[str] = None

class TaskCompleteRequest(BaseModel):
    user_id: int
    task_id: str

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def get_tap_value(level: int) -> int:
    
    return 1 + level

def get_hour_value(level: int) -> int:
    if level >= len(HOUR_VALUES):
        return HOUR_VALUES[-1] * (2 ** (level - len(HOUR_VALUES) + 1))
    return HOUR_VALUES[level]

def get_max_energy(level: int) -> int:
    if level >= len(ENERGY_VALUES):
        return ENERGY_VALUES[-1] * (1.5 ** (level - len(ENERGY_VALUES) + 1))
    return ENERGY_VALUES[level]

def get_luck_chances(luck_level: int) -> dict:
    if luck_level >= 10:
        return {"x2": 25, "x3": 8, "x5": 2}
    elif luck_level >= 7:
        return {"x2": 18, "x3": 5, "x5": 1}
    elif luck_level >= 5:
        return {"x2": 15, "x3": 3, "x5": 0.5}
    elif luck_level >= 3:
        return {"x2": 12, "x3": 2, "x5": 0}
    elif luck_level >= 1:
        return {"x2": 5 + luck_level * 2, "x3": 0, "x5": 0}
    return {"x2": 0, "x3": 0, "x5": 0}

def get_luck_multiplier(luck_level: int) -> tuple[int, int]:
    chances = get_luck_chances(luck_level)
    rand = random.random() * 100
    if rand < chances["x5"]:
        return 5, 5
    elif rand < chances["x5"] + chances["x3"]:
        return 3, 3
    elif rand < chances["x5"] + chances["x3"] + chances["x2"]:
        return 2, 2
    return 1, 0
SKIN_BONUSES = {
    'default_cat': {'type': 'multiplier', 'value': 1.0},
    'black_cat': {'type': 'multiplier', 'value': 1.1},
    'white_cat': {'type': 'multiplier', 'value': 1.15},
    'gold_cat': {'type': 'multiplier', 'value': 1.5},
    'space_cat': {'type': 'interval', 'value': 8},
    'ninja_cat': {'type': 'multiplier', 'value': 2.0},
    'wizard_cat': {'type': 'both', 'multiplier': 1.8, 'interval': 7},
    'rainbow_cat': {'type': 'multiplier', 'value': 3.0},
    'alien_cat': {'type': 'interval', 'value': 5}
}

async def get_skin_bonus(skin_id: str):
    """Возвращает бонус скина для пассивного дохода"""
    return SKIN_BONUSES.get(skin_id, {'type': 'multiplier', 'value': 1.0})
# ==================== API ЭНДПОИНТЫ ====================

@app.get("/api/check-referral/{user_id}")
async def check_referral(user_id: int):
    """Проверить, есть ли у пользователя реферер"""
    user = await get_user(user_id)
    if not user:
        return {"has_referrer": False}
    
    return {
        "has_referrer": user.get("referrer_id") is not None,
        "referrer_id": user.get("referrer_id")
    }


@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    user = await get_user(user_id)
    if not user:
        # Вместо создания - возвращаем ошибку
        raise HTTPException(status_code=404, detail="User not found. Please register first.")
    
    luck_chances = get_luck_chances(user.get("luck_level", 0))

    return {
        "coins": user["coins"],
        "energy": user["energy"],
        "max_energy": user["max_energy"],
        "profit_per_tap": get_tap_value(user["multitap_level"]),
        "profit_per_hour": get_hour_value(user["profit_level"]),
        "multitap_level": user["multitap_level"],
        "profit_level": user["profit_level"],
        "energy_level": user["energy_level"],
        "luck_level": user.get("luck_level", 0),
        "luck_chances": luck_chances
    }



@app.post("/api/click")
async def process_click(request: ClickRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Проверяем активный МЕГА-БУСТ
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        extra = {}
    
    active_boosts = extra.get("active_boosts", {})
    now = datetime.utcnow()
    
    mega_boost_active = False
    if "mega_boost" in active_boosts:
        expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
        if now <= expires:
            mega_boost_active = True  # Дает и x2, и бесконечную энергию
        else:
            del active_boosts["mega_boost"]
            extra["active_boosts"] = active_boosts
            await update_user(request.user_id, {"extra_data": extra})
    
    base_tap = get_tap_value(user["multitap_level"])
    
    # Удача (криты)
    multiplier, crit_type = get_luck_multiplier(user.get("luck_level", 0))
    
    # Применяем x2 если буст активен
    if mega_boost_active:
        multiplier *= 2
    
    actual_gain = base_tap * multiplier
    
    # Обновляем баланс
    user["coins"] += actual_gain
    
    # Тратим энергию ТОЛЬКО если буст НЕ активен
    if not mega_boost_active:
        if user["energy"] < 1:
            raise HTTPException(status_code=400, detail="Not enough energy")
        user["energy"] -= 1
    
    # Сохраняем в БД
    await update_user(request.user_id, {
        "coins": user["coins"],
        "energy": user["energy"]
    })
    
    return {
        "coins": user["coins"],
        "energy": user["energy"],
        "tap_value": base_tap,
        "multiplier": multiplier,
        "actual_gain": actual_gain,
        "crit": crit_type if multiplier > 1 and not mega_boost_active else 0,
        "mega_boost_active": mega_boost_active
    }


@app.post("/api/upgrade")
async def process_upgrade(request: UpgradeRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    boost_type = request.boost_type
    current_level = user.get(f"{boost_type}_level", 0)

    if current_level >= len(UPGRADE_PRICES[boost_type]):
        raise HTTPException(status_code=400, detail="Max level reached")

    price = UPGRADE_PRICES[boost_type][current_level]
    if user["coins"] < price:
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
        updates["energy"] = new_max

    await update_user(request.user_id, updates)
    updated_user = await get_user(request.user_id)

    luck_chances = get_luck_chances(updated_user.get("luck_level", 0))

    return {
        "coins": updated_user["coins"],
        "new_level": updated_user[f"{boost_type}_level"],
        "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
        "profit_per_tap": get_tap_value(updated_user["multitap_level"]),
        "profit_per_hour": get_hour_value(updated_user["profit_level"]),
        "max_energy": updated_user["max_energy"],
        "luck_chances": luck_chances
    }

@app.post("/api/recover-energy")
async def recover_energy(data: UserIdRequest):
    user = await get_user(data.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["energy"] >= user["max_energy"]:
        return {"energy": user["energy"]}
    
    # ✅ ИСПРАВЛЕНО: всегда +1 в секунду
    recovery = 1  # вместо 2% от максимума
    
    new_energy = min(user["max_energy"], user["energy"] + recovery)
    await update_user(data.user_id, {"energy": new_energy})
    return {"energy": new_energy}


@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    prices = {}
    for boost in UPGRADE_PRICES:
        level = user.get(f"{boost}_level", 0)
        prices[boost] = UPGRADE_PRICES[boost][level] if level < len(UPGRADE_PRICES[boost]) else 0
    return prices

@app.get("/api/migrate-referrals")
async def migrate_referrals():
    """Добавляет реферальные колонки в таблицу users"""
    try:
        from sqlalchemy import create_engine, inspect, text
        import os

        db_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://...")
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
                "message": f"Колонки добавлены: {added}",
                "columns": columns + added
            }
    except Exception as e:
        return {"status": "error", "error": str(e)}
    

@app.post("/api/reward-video")
async def reward_video(data: dict):
    user_id = data.get('user_id')
    reward = data.get('reward', 5000)
    
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user['coins'] += reward
    await update_user(user_id, {"coins": user['coins']})
    
    return {"success": True, "coins": user['coins']}

# ==================== БУСТЫ ====================

class BoostActivateRequest(BaseModel):
    user_id: int
    # Один тип буста, который включает всё

@app.post("/api/activate-boost")
async def activate_boost(request: BoostActivateRequest):
    """Активирует МЕГА-БУСТ: x2 монет + бесконечная энергия на 2 минуты"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Получаем текущие бусты из extra_data
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        extra = {}
    
    active_boosts = extra.get("active_boosts", {})
    now = datetime.utcnow()
    
    # Проверяем, не активен ли уже буст
    if "mega_boost" in active_boosts:
        expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
        if now < expires:
            raise HTTPException(status_code=400, detail="Буст уже активен!")
    
    # Активируем МЕГА-БУСТ на 2 минуты
    active_boosts["mega_boost"] = {
        "active": True,
        "expires_at": (now + timedelta(minutes=2)).isoformat()
    }
    
    # Сохраняем в extra_data
    extra["active_boosts"] = active_boosts
    await update_user(request.user_id, {"extra_data": extra})
    
    return {
        "success": True,
        "message": "🔥⚡ МЕГА-БУСТ активирован на 2 минуты! x2 монет + бесконечная энергия",
        "expires_at": active_boosts["mega_boost"]["expires_at"]
    }


@app.get("/api/boosts/{user_id}")
async def get_boosts(user_id: int):
    """Получить статус активного буста"""
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        extra = {}
    
    active_boosts = extra.get("active_boosts", {})
    now = datetime.utcnow()
    
    # Проверяем и очищаем просроченный буст
    changed = False
    if "mega_boost" in active_boosts:
        expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
        if now > expires:
            del active_boosts["mega_boost"]
            changed = True
    
    if changed:
        extra["active_boosts"] = active_boosts
        await update_user(user_id, {"extra_data": extra})
    
    return {
        "mega_boost": active_boosts.get("mega_boost")
    }

# ==================== РЕФЕРАЛЫ ====================
@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int):
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {
        "count": user.get("referral_count", 0),
        "earnings": user.get("referral_earnings", 0)
    }

# ==================== ЗАДАНИЯ ====================
@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int):
    """Получить список доступных заданий"""
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Здесь должна быть логика получения статуса заданий из БД
    # Для простоты пока возвращаем статичный список
    
    tasks = [
        {
            "id": "daily_bonus",
            "title": "📅 Ежедневный бонус",
            "description": "Заходи каждый день и получай награду",
            "reward": "25000 монет",
            "icon": "📅",
            "completed": False,
            "progress": 0,
            "total": 1
        },
        {
            "id": "energy_refill",
            "title": "⚡ Бесконечная энергия",
            "description": "5 минут без лимита энергии",
            "reward": "⚡ 5 минут",
            "icon": "⚡",
            "completed": False,
            "progress": 0,
            "total": 1
        },
        {
            "id": "link_click",
            "title": "🔗 Переход по ссылке",
            "description": "Кликни по ссылке и получи награду",
            "reward": "25000 монет",
            "icon": "🔗",
            "completed": False,
            "progress": 0,
            "total": 1
        },
        {
            "id": "invite_5_friends",
            "title": "👥 Пригласи 5 друзей",
            "description": "Приведи 5 друзей в игру",
            "reward": "20000 монет",
            "icon": "👥",
            "completed": user.get("referral_count", 0) >= 5,
            "progress": min(user.get("referral_count", 0), 5),
            "total": 5
        }
    ]
    
    return tasks

@app.post("/api/complete-task")
async def complete_task(request: TaskCompleteRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    task_id = request.task_id
    message = ""
    updates = {}
    
    if task_id == "daily_bonus":
        # Ежедневный бонус (раз в 24 часа)
        # Здесь проверка по времени
        user["coins"] += 25000
        message = "🎁 +25000 монет (ежедневный бонус)"
        updates = {"coins": user["coins"]}
        
    elif task_id == "energy_refill":
        # Бесконечная энергия (ОДИН РАЗ)
        completed = await get_completed_tasks(request.user_id)
        if "energy_refill" in completed:
            raise HTTPException(status_code=400, detail="Уже активировано")
        
        message = "⚡ Бесконечная энергия активирована на 5 минут!"
        await add_completed_task(request.user_id, task_id)
        
    elif task_id == "link_click":
        # 👇 ПЕРЕХОД ПО ССЫЛКЕ - БЕЗ ОГРАНИЧЕНИЙ!
        user["coins"] += 25000
        message = "🔗 +25000 монет за переход!"
        updates = {"coins": user["coins"]}
        # НЕ добавляем в completed_tasks!
        
    elif task_id == "invite_5_friends":
        # Пригласить 5 друзей (ОДИН РАЗ)
        completed = await get_completed_tasks(request.user_id)
        if "invite_5_friends" in completed:
            raise HTTPException(status_code=400, detail="Уже выполнено")
        
        if user.get("referral_count", 0) >= 5:
            user["coins"] += 20000
            message = "👥 +20000 монет за 5 друзей!"
            updates = {"coins": user["coins"]}
            await add_completed_task(request.user_id, task_id)
        else:
            raise HTTPException(status_code=400, detail="Недостаточно друзей")
    
    if updates:
        await update_user(request.user_id, updates)
    
    return {"success": True, "message": message, "coins": user["coins"]}

# ==================== МИНИ-ИГРЫ ====================
@app.post("/api/game/coinflip")
async def play_coinflip(request: GameRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["coins"] < request.bet:
        raise HTTPException(status_code=400, detail="Not enough coins")
    if request.bet < 10:
        raise HTTPException(status_code=400, detail="Minimum bet 10")
    
    win = random.choice([True, False])
    if win:
        user["coins"] += request.bet
        message = f"🎉 Вы выиграли +{request.bet} монет!"
    else:
        user["coins"] -= request.bet
        message = f"😞 Вы проиграли {request.bet} монет"
    
    await update_user(request.user_id, {"coins": user["coins"]})
    
    return {
        "coins": user["coins"],
        "result": "win" if win else "lose",
        "message": message
    }

@app.post("/api/game/slots")
async def play_slots(request: GameRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["coins"] < request.bet:
        raise HTTPException(status_code=400, detail="Not enough coins")
    if request.bet < 10:
        raise HTTPException(status_code=400, detail="Minimum bet 10")
    symbols = ["🍒", "🍋", "🍊", "7️⃣", "💎"]
    slots = [random.choice(symbols) for _ in range(3)]
    win = len(set(slots)) == 1
    multiplier = 10 if "7️⃣" in slots and win else 5 if "💎" in slots and win else 3
    if win:
        win_amount = request.bet * multiplier
        user["coins"] += win_amount
        message = f"🎰 Джекпот! +{win_amount} монет (x{multiplier})"
    else:
        user["coins"] -= request.bet
        message = f"😞 Вы проиграли {request.bet} монет"
    await update_user(request.user_id, {"coins": user["coins"]})
    return {"coins": user["coins"], "slots": slots, "message": message}

@app.post("/api/game/dice")
async def play_dice(request: GameRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["coins"] < request.bet:
        raise HTTPException(status_code=400, detail="Not enough coins")
    if request.bet < 10:
        raise HTTPException(status_code=400, detail="Minimum bet 10")
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
        message = f"🎲 Вы выиграли +{win_amount} монет (x{multiplier})"
    else:
        user["coins"] -= request.bet
        message = f"😞 Вы проиграли {request.bet} монет"
    await update_user(request.user_id, {"coins": user["coins"]})
    return {"coins": user["coins"], "dice1": dice1, "dice2": dice2, "message": message}

@app.post("/api/game/roulette")
async def play_roulette(request: GameRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["coins"] < request.bet:
        raise HTTPException(status_code=400, detail="Not enough coins")
    if request.bet < 10:
        raise HTTPException(status_code=400, detail="Minimum bet 10")
    
    red_numbers = [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]
    black_numbers = [2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35]
    
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
        message = f"🎉 {result_symbol} {result} - Вы выиграли +{win_amount} монет! (x{multiplier})"
    else:
        user["coins"] -= request.bet
        message = f"😞 {result_symbol} {result} - Вы проиграли {request.bet} монет"
    
    await update_user(request.user_id, {"coins": user["coins"]})
    
    return {
        "coins": user["coins"],
        "result_number": result,
        "result_color": result_color,
        "result_symbol": result_symbol,
        "win": win,
        "message": message
    }

class RegisterRequest(BaseModel):
    user_id: int
    username: Optional[str] = None
    referrer_id: Optional[int] = None

@app.post("/api/register")
async def register_user(request: RegisterRequest):
    """Регистрация нового пользователя с рефералом"""
    # Проверяем, есть ли уже такой пользователь
    existing = await get_user(request.user_id)
    if existing:
        # Пользователь уже есть, просто возвращаем данные
        return {"status": "exists", "user": existing}
    
    # Создаем нового пользователя с referrer_id
    await create_user(
        user_id=request.user_id, 
        username=request.username,
        referrer_id=request.referrer_id
    )
    
    user = await get_user(request.user_id)
    
    # Если был реферер, бонус начислится автоматически в add_user
    if request.referrer_id:
        return {
            "status": "created_with_referral",
            "user": user,
            "message": f"Добро пожаловать! Вас пригласил {request.referrer_id}"
        }
    
    return {"status": "created", "user": user}


class MegaBoostActivateRequest(BaseModel):
    user_id: int

@app.post("/api/activate-mega-boost")
async def activate_mega_boost(request: MegaBoostActivateRequest):
    """Активирует МЕГА-БУСТ: x2 монет + бесконечная энергия на 2 минуты"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Получаем текущие бусты из extra_data
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        extra = {}
    
    active_boosts = extra.get("active_boosts", {})
    now = datetime.utcnow()
    
    # Проверяем, не активен ли уже буст
    if "mega_boost" in active_boosts:
        expires = datetime.fromisoformat(active_boosts["mega_boost"]["expires_at"])
        if now < expires:
            # Возвращаем оставшееся время
            remaining = int((expires - now).total_seconds())
            return {
                "success": False,
                "message": f"Буст уже активен! Осталось {remaining // 60}:{remaining % 60:02d}",
                "already_active": True,
                "expires_at": active_boosts["mega_boost"]["expires_at"]
            }
    
    # Активируем МЕГА-БУСТ на 2 минуты
    expires_at = (now + timedelta(minutes=2)).isoformat()
    active_boosts["mega_boost"] = {
        "active": True,
        "expires_at": expires_at
    }
    
    # Сохраняем в extra_data
    extra["active_boosts"] = active_boosts
    await update_user(request.user_id, {"extra_data": extra})
    
    return {
        "success": True,
        "message": "🔥⚡ МЕГА-БУСТ активирован на 2 минуты! x2 монет + бесконечная энергия",
        "expires_at": expires_at
    }


@app.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int):
    """Получить статус МЕГА-БУСТА"""
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    extra = user.get("extra_data", {})
    if not isinstance(extra, dict):
        extra = {}
    
    active_boosts = extra.get("active_boosts", {})
    now = datetime.utcnow()
    
    # Проверяем и очищаем просроченный буст
    if "mega_boost" in active_boosts:
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
    
    return {"active": False}

class PassiveIncomeRequest(BaseModel):
    user_id: int
    skin_bonus: Optional[dict] = None  # бонус приходит с клиента

@app.post("/api/passive-income")
async def passive_income(request: PassiveIncomeRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    last_income = user.get('last_passive_income')
    now = datetime.utcnow()
    
    # Базовый интервал 10 минут
    base_interval = timedelta(minutes=10)
    
    # Используем бонус с клиента
    multiplier = 1.0
    interval = base_interval
    
    if request.skin_bonus:
        bonus_type = request.skin_bonus.get('type')
        
        if bonus_type == 'multiplier':
            multiplier = request.skin_bonus.get('value', 1.0)
        elif bonus_type == 'interval':
            interval = timedelta(minutes=request.skin_bonus.get('value', 10))
        elif bonus_type == 'both':
            multiplier = request.skin_bonus.get('multiplier', 1.0)
            interval = timedelta(minutes=request.skin_bonus.get('interval', 10))
    
    # Расчет дохода
    if not last_income or (now - last_income) >= interval:
        minutes_passed = (now - last_income).total_seconds() / 60 if last_income else 0
        cycles = max(1, int(minutes_passed // 10)) if last_income else 1
        
        hour_value = get_hour_value(user["profit_level"])
        base_income_per_10min = hour_value // 6
        total_income = int(base_income_per_10min * cycles * multiplier)
        
        if total_income > 0:
            user["coins"] += total_income
            await update_user(request.user_id, {
                "coins": user["coins"],
                "last_passive_income": now
            })
            
            return {
                "coins": user["coins"],
                "income": total_income,
                "message": f"💰 +{total_income} монет (бонус от скина x{multiplier})"
            }
    
    return {"coins": user["coins"], "income": 0}

# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    asyncio.run(init_db())
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=True)  # добавили reload=True