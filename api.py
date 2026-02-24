from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import uvicorn

from DATABASE.base import get_user, create_user, update_user, init_db

# ==================== КОНФИГУРАЦИЯ ====================

# Настройки улучшений (цены за уровень)
UPGRADE_PRICES = {
    "multitap": [50, 200, 500, 2000, 8000, 32000, 128000, 512000, 2048000, 8192000],
    "profit":   [100, 400, 1000, 4000, 16000, 64000, 256000, 1024000, 4096000, 16384000],
    "energy":   [80, 300, 800, 3000, 12000, 48000, 192000, 768000, 3072000, 12288000],
}

# Значения за уровень
TAP_VALUES = [1, 2, 5, 10, 20, 40, 80, 160, 320, 640, 1280]           # Мультитап
HOUR_VALUES = [100, 150, 250, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000]  # Прибыль в час
ENERGY_VALUES = [1000, 1100, 1250, 1500, 2000, 3000, 5000, 8000, 13000, 21000, 34000]  # Макс энергия

# ==================== ИНИЦИАЛИЗАЦИЯ ====================

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
    boost_type: str  # "multitap", "profit", "energy"

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

# ==================== API ЭНДПОИНТЫ ====================

@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    """Получить данные пользователя"""
    user = await get_user(user_id)

    if not user:
        # Создаем нового пользователя
        await create_user(user_id)
        user = await get_user(user_id)

    # Добавляем вычисляемые значения
    user["max_tap_value"] = get_tap_value(user["multitap_level"])
    user["max_hour_value"] = get_hour_value(user["profit_level"])
    user["max_energy_value"] = get_max_energy(user["energy_level"])

    return user


@app.post("/api/click")
async def process_click(request: ClickRequest):
    """Обработать клик"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Проверяем энергию
    tap_value = get_tap_value(user["multitap_level"])
    if user["energy"] < tap_value:
        raise HTTPException(status_code=400, detail="Not enough energy")

    # Обновляем данные
    user["coins"] += tap_value
    user["energy"] -= tap_value

    # Сохраняем
    await update_user(request.user_id, {
        "coins": user["coins"],
        "energy": user["energy"]
    })

    return {
        "coins": user["coins"],
        "energy": user["energy"],
        "tap_value": tap_value
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

    # Проверяем монеты
    if user["coins"] < price:
        raise HTTPException(status_code=400, detail="Not enough coins")

    # Обновляем данные
    user["coins"] -= price
    user[f"{boost_type}_level"] = current_level + 1

    # Применяем эффект улучшения
    updates = {
        "coins": user["coins"],
        f"{boost_type}_level": current_level + 1
    }

    if boost_type == "multitap":
        # Мультитап уже влияет через get_tap_value
        pass
    elif boost_type == "profit":
        # Обновляем profit_per_hour для совместимости
        updates["profit_per_hour"] = get_hour_value(current_level + 1)
    elif boost_type == "energy":
        # Увеличиваем максимальную энергию
        new_max = get_max_energy(current_level + 1)
        updates["max_energy"] = new_max
        updates["energy"] = new_max  # Восстанавливаем энергию

    await update_user(request.user_id, updates)

    # Получаем обновленные данные
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
    """Восстановить энергию (пропорционально)"""
    user = await get_user(data.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user["energy"] >= user["max_energy"]:
        return {"energy": user["energy"]}
    
    # ПРОПОРЦИОНАЛЬНОЕ ВОССТАНОВЛЕНИЕ
    max_energy = user["max_energy"]
    
    # Восстанавливаем 2% от максимума (но минимум 1)
    recovery = max(1, int(max_energy * 0.02))  # 2% за раз
    
    # Можно регулировать процент:
    # 1% = max_energy * 0.01
    # 3% = max_energy * 0.03
    # 5% = max_energy * 0.05
    
    new_energy = min(max_energy, user["energy"] + recovery)
    
    await update_user(data.user_id, {"energy": new_energy})
    
    return {"energy": new_energy}

@app.post("/api/passive-income")
async def passive_income(request: UserIdRequest):
    """Начислить пассивный доход"""
    user = await get_user(request.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Доход в секунду (profit_per_hour / 3600)
    hour_value = get_hour_value(user["profit_level"])
    second_income = hour_value // 3600

    if second_income > 0:
        user["coins"] += second_income
        await update_user(request.user_id, {"coins": user["coins"]})

    return {"coins": user["coins"]}


@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    """Получить цены улучшений для пользователя"""
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
    # Создаем таблицы
    asyncio.run(init_db())
    # Запускаем сервер
    uvicorn.run(app, host="0.0.0.0", port=8000)