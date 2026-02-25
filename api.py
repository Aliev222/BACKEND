from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import uvicorn
import random
from datetime import datetime, timedelta
import os

from DATABASE.base import get_user, add_user as create_user, update_user, init_db

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

class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str

class UserIdRequest(BaseModel):
    user_id: int

class GameRequest(BaseModel):
    user_id: int
    bet: int
    prediction: str = None

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def get_tap_value(level: int) -> int:
    if level >= len(TAP_VALUES):
        return TAP_VALUES[-1] * (2 ** (level - len(TAP_VALUES) + 1))
    return TAP_VALUES[level]

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

# ==================== API ЭНДПОИНТЫ ====================

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    user = await get_user(user_id)
    if not user:
        await create_user(user_id)
        user = await get_user(user_id)

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

    base_tap = get_tap_value(user["multitap_level"])
    if user["energy"] < base_tap:
        raise HTTPException(status_code=400, detail="Not enough energy")

    multiplier, crit_type = get_luck_multiplier(user.get("luck_level", 0))
    actual_gain = base_tap * multiplier

    user["coins"] += actual_gain
    user["energy"] -= base_tap

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
        "crit": crit_type
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
    recovery = max(1, int(user["max_energy"] * 0.02))
    new_energy = min(user["max_energy"], user["energy"] + recovery)
    await update_user(data.user_id, {"energy": new_energy})
    return {"energy": new_energy}

@app.post("/api/passive-income")
async def passive_income(request: UserIdRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    last_income = user.get('last_passive_income')
    now = datetime.utcnow()
    if not last_income or (now - last_income) >= timedelta(minutes=10):
        hour_value = get_hour_value(user["profit_level"])
        income = hour_value // 6
        if income > 0:
            user["coins"] += income
            await update_user(request.user_id, {
                "coins": user["coins"],
                "last_passive_income": now
            })
            return {
                "coins": user["coins"],
                "income": income,
                "message": f"💰 +{income} монет (10 мин)"
            }
    return {"coins": user["coins"], "income": 0}

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

# ==================== РЕФЕРАЛЫ ====================
@app.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int):
    """Получить реферальную статистику пользователя"""
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Здесь должна быть логика подсчёта из базы
    # Вместо заглушки возвращаем реальные данные
    
    return {
        "count": user.get("referral_count", 0),
        "earnings": user.get("referral_earnings", 0)
    }

# ==================== ЗАДАНИЯ ====================
@app.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int, category: str = "all"):
    # Здесь должна быть логика получения заданий
    return []

@app.post("/api/task/start/{task_id}")
async def start_task(task_id: str, data: dict):
    # Здесь должна быть логика старта задания
    return {"redirect": "https://example.com"}

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
        message = f"🪙 Вы выиграли +{request.bet} монет!"
    else:
        user["coins"] -= request.bet
        message = f"😞 Вы проиграли {request.bet} монет"
    await update_user(request.user_id, {"coins": user["coins"]})
    return {"coins": user["coins"], "result": "win" if win else "lose", "message": message}

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

@app.post("/api/game/wheel")
async def play_wheel(request: GameRequest):
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["coins"] < request.bet:
        raise HTTPException(status_code=400, detail="Not enough coins")
    if request.bet < 10:
        raise HTTPException(status_code=400, detail="Minimum bet 10")
    sectors = ["lose", "lose", "lose", "lose", "small", "small", "medium", "big"]
    result = random.choice(sectors)
    if result == "big":
        multiplier = 5
    elif result == "medium":
        multiplier = 3
    elif result == "small":
        multiplier = 2
    else:
        multiplier = 0
    if multiplier > 0:
        win_amount = request.bet * multiplier
        user["coins"] += win_amount
        message = f"🎡 Вы выиграли +{win_amount} монет (x{multiplier})"
    else:
        user["coins"] -= request.bet
        message = f"😞 Вы проиграли {request.bet} монет"
    await update_user(request.user_id, {"coins": user["coins"]})
    return {"coins": user["coins"], "sector": result, "message": message}

@app.get("/api/migrate-referrals")
async def migrate_referrals():
    """Добавляет реферальные колонки в таблицу users"""
    try:
        from sqlalchemy import create_engine, inspect, text
        import os

        db_url = os.getenv("DATABASE_URL", "postgresql+asyncpg://...")
        # Создаём синхронный движок для миграции
        sync_engine = create_engine(db_url.replace("+asyncpg", ""))

        with sync_engine.connect() as conn:
            # Проверяем, какие колонки уже есть
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

# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    asyncio.run(init_db())
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)