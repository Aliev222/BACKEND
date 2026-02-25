from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import uvicorn
import random
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine, inspect, text


from DATABASE.base import get_user, add_user as create_user, update_user, init_db

# ==================== КОНФИГУРАЦИЯ ====================

# Настройки улучшений (цены за уровень)
UPGRADE_PRICES = {
    "multitap": [50, 200, 500, 2000, 8000, 32000, 128000, 512000, 2048000, 8192000],
    "profit":   [100, 400, 1000, 4000, 16000, 64000, 256000, 1024000, 4096000, 16384000],
    "energy":   [80, 300, 800, 3000, 12000, 48000, 192000, 768000, 3072000, 12288000],
    "luck":     [500, 2000, 5000, 20000, 50000, 200000, 500000, 2000000, 5000000, 20000000],
}

# Значения за уровень
TAP_VALUES = [1, 2, 5, 10, 20, 40, 80, 160, 320, 640, 1280]           # Мультитап
HOUR_VALUES = [100, 150, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000]  # Прибыль в час
ENERGY_VALUES = [1000, 1100, 1250, 1500, 2000, 3000, 5000, 8000, 13000, 21000, 34000]  # Макс энергия

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
def ensure_luck_column():
    """Автоматически добавляет колонку luck_level при запуске"""
    try:
        db_url = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///database_new.db")
        # Создаём синхронный движок
        sync_engine = create_engine(db_url.replace("+aiosqlite", ""))
        
        with sync_engine.connect() as conn:
            inspector = inspect(sync_engine)
            columns = [col['name'] for col in inspector.get_columns('users')]
            
            if 'luck_level' not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN luck_level INTEGER DEFAULT 0"))
                conn.commit()
                print("✅ Колонка luck_level автоматически добавлена!")
            else:
                print("✅ Колонка luck_level уже существует")
    except Exception as e:
        print(f"⚠️ Ошибка при проверке/добавлении колонки: {e}")

# Вызываем функцию при старте
ensure_luck_column()
# ===== КОНЕЦ АВТООБНОВЛЕНИЯ =====


app = FastAPI(title="Ryoho Clicker API")

# Разрешаем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ryoho-eta.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== МОДЕЛИ ДАННЫХ ====================

class ClickRequest(BaseModel):
    user_id: int
    clicks: int

class UpgradeRequest(BaseModel):
    user_id: int
    boost_type: str  # "multitap", "profit", "energy", "luck"

class UserIdRequest(BaseModel):
    user_id: int

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def get_tap_value(level: int) -> int:
    """Получить количество монет за клик по уровню мультитапа"""
    if level >= len(TAP_VALUES):
        return TAP_VALUES[-1] * (2 ** (level - len(TAP_VALUES) + 1))
    return TAP_VALUES[level]

def get_hour_value(level: int) -> int:
    """Получить пассивный доход в час по уровню прибыли"""
    if level >= len(HOUR_VALUES):
        return HOUR_VALUES[-1] * (2 ** (level - len(HOUR_VALUES) + 1))
    return HOUR_VALUES[level]

def get_max_energy(level: int) -> int:
    """Получить максимальную энергию по уровню энергии"""
    if level >= len(ENERGY_VALUES):
        return ENERGY_VALUES[-1] * (1.5 ** (level - len(ENERGY_VALUES) + 1))
    return ENERGY_VALUES[level]

def get_luck_multiplier(luck_level: int) -> int:
    """
    Рассчитать множитель удачи при клике
    Шансы растут с уровнем
    """
    rand = random.random() * 100  # случайное число от 0 до 100
    
    # Максимальные шансы на 10 уровне
    if luck_level >= 10:
        if rand < 2: return 5      # 2% на x5
        if rand < 8: return 3      # 6% на x3
        if rand < 25: return 2     # 17% на x2
    elif luck_level >= 7:
        if rand < 1: return 5      # 1% на x5
        if rand < 5: return 3      # 4% на x3
        if rand < 18: return 2     # 13% на x2
    elif luck_level >= 5:
        if rand < 0.5: return 5    # 0.5% на x5
        if rand < 3: return 3      # 2.5% на x3
        if rand < 15: return 2     # 12% на x2
    elif luck_level >= 3:
        if rand < 2: return 3      # 2% на x3
        if rand < 12: return 2     # 10% на x2
    elif luck_level >= 1:
        if rand < 5 + luck_level * 2: return 2  # 7%, 9%, 11% на x2
    
    return 1  # обычный клик

# ==================== API ЭНДПОИНТЫ ====================

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    """Получить данные пользователя"""
    user = await get_user(user_id)

    if not user:
        await create_user(user_id)
        user = await get_user(user_id)

    return {
        "coins": user["coins"],
        "energy": user["energy"],
        "max_energy": user["max_energy"],
        "profit_per_tap": get_tap_value(user["multitap_level"]),
        "profit_per_hour": get_hour_value(user["profit_level"]),
        "multitap_level": user["multitap_level"],
        "profit_level": user["profit_level"],
        "energy_level": user["energy_level"],
        "luck_level": user.get("luck_level", 0)
    }


@app.post("/api/click")
async def process_click(request: ClickRequest):
    """Обработать клик"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Базовая стоимость клика
    base_tap_value = get_tap_value(user["multitap_level"])
    
    if user["energy"] < base_tap_value:
        raise HTTPException(status_code=400, detail="Not enough energy")

    # Применяем удачу
    luck_level = user.get("luck_level", 0)
    multiplier = get_luck_multiplier(luck_level)
    actual_gain = base_tap_value * multiplier

    # Обновляем данные
    user["coins"] += actual_gain
    user["energy"] -= base_tap_value  # тратится базовая энергия, не умноженная!

    # Сохраняем
    await update_user(request.user_id, {
        "coins": user["coins"],
        "energy": user["energy"]
    })

    return {
        "coins": user["coins"],
        "energy": user["energy"],
        "tap_value": base_tap_value,
        "multiplier": multiplier,
        "actual_gain": actual_gain
    }


@app.post("/api/upgrade")
async def process_upgrade(request: UpgradeRequest):
    """Улучшить буст"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    boost_type = request.boost_type
    current_level = user.get(f"{boost_type}_level", 0)

    # Проверяем, есть ли цена для этого уровня
    if current_level >= len(UPGRADE_PRICES[boost_type]):
        raise HTTPException(status_code=400, detail="Max level reached")

    price = UPGRADE_PRICES[boost_type][current_level]

    if user["coins"] < price:
        raise HTTPException(status_code=400, detail="Not enough coins")

    # Обновляем данные
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
    elif boost_type == "luck":
        # Удача не меняет другие параметры, просто уровень растёт
        pass

    await update_user(request.user_id, updates)
    updated_user = await get_user(request.user_id)

    return {
        "coins": updated_user["coins"],
        "new_level": updated_user[f"{boost_type}_level"],
        "next_cost": UPGRADE_PRICES[boost_type][current_level + 1] if current_level + 1 < len(UPGRADE_PRICES[boost_type]) else 0,
        "profit_per_tap": get_tap_value(updated_user["multitap_level"]),
        "profit_per_hour": get_hour_value(updated_user["profit_level"]),
        "max_energy": updated_user["max_energy"]
    }


@app.post("/api/recover-energy")
async def recover_energy(data: UserIdRequest):
    """Восстановить энергию"""
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
    """Начислить пассивный доход"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    last_income = user.get('last_passive_income')
    now = datetime.utcnow()

    if not last_income or (now - last_income) >= timedelta(minutes=1):
        hour_value = get_hour_value(user["profit_level"])
        income = hour_value // 60

        if income > 0:
            user["coins"] += income
            await update_user(request.user_id, {
                "coins": user["coins"],
                "last_passive_income": now
            })
            return {
                "coins": user["coins"],
                "income": income,
                "message": f"💰 +{income} монет"
            }

    return {"coins": user["coins"], "income": 0}


@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    """Получить цены улучшений"""
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    prices = {}
    for boost in UPGRADE_PRICES:
        level = user.get(f"{boost}_level", 0)
        if level < len(UPGRADE_PRICES[boost]):
            prices[boost] = UPGRADE_PRICES[boost][level]
        else:
            prices[boost] = 0
    return prices

# ==================== ЗАПУСК ====================

if __name__ == "__main__":
    import os
    asyncio.run(init_db())
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)