from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from DATABASE.base import get_user, update_user, add_user

app = FastAPI()

# Разрешаем запросы с твоего фронтенда
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ryoho-eta.vercel.app", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Модели данных
class ClickData(BaseModel):
    user_id: int
    clicks: int

class UpgradeData(BaseModel):
    user_id: int
    boost_type: str

class EnergyData(BaseModel):
    user_id: int

# API эндпоинты
@app.get("/api/user/{user_id}")
async def get_user_data(user_id: int):
    """Получить данные пользователя"""
    user = await get_user(user_id)
    if not user:
        # Создаем нового пользователя, если не существует
        await add_user(user_id, f"user_{user_id}")
        user = await get_user(user_id)
    
    return {
        "coins": user["coins"],
        "profit_per_hour": user.get("profit_per_hour", 3200),
        "profit_per_tap": user.get("profit_per_tap", 1),
        "energy": user.get("energy", 1000),
        "max_energy": user.get("max_energy", 1000),
        "level": user.get("level", 0),
        "multitap_level": user.get("multitap_level", 0),
        "profit_level": user.get("profit_level", 0),
        "energy_level": user.get("energy_level", 0)
    }

@app.post("/api/click")
async def process_click(data: ClickData):
    """Обработать клик"""
    user = await get_user(data.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Проверяем энергию
    if user["energy"] < data.clicks:
        raise HTTPException(status_code=400, detail="Not enough energy")
    
    # Обновляем данные
    user["coins"] += data.clicks
    user["energy"] -= data.clicks
    
    # Сохраняем в БД
    await update_user(data.user_id, user)
    
    return {
        "coins": user["coins"],
        "energy": user["energy"]
    }

@app.post("/api/upgrade")
async def process_upgrade(data: UpgradeData):
    """Улучшение буста"""
    user = await get_user(data.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Цены улучшений
    prices = {
        'multitap': [500, 2000, 5000, 20000, 50000],
        'profit': [1000, 5000, 15000, 50000, 200000],
        'energy': [1000, 3000, 10000, 30000, 100000],
        'boost': [500, 2000, 8000, 30000, 100000]
    }
    
    current_level = user.get(f"{data.boost_type}_level", 0)
    if current_level >= len(prices[data.boost_type]):
        raise HTTPException(status_code=400, detail="Max level reached")
    
    price = prices[data.boost_type][current_level]
    
    if user["coins"] < price:
        raise HTTPException(status_code=400, detail="Not enough coins")
    
    # Применяем улучшение
    user["coins"] -= price
    user[f"{data.boost_type}_level"] = current_level + 1
    
    # Обновляем параметры
    if data.boost_type == 'multitap':
        user["profit_per_tap"] = 1 + 62 * user["multitap_level"]
    elif data.boost_type == 'profit':
        user["profit_per_hour"] = 3200 + user["profit_level"] * 100
    elif data.boost_type == 'energy':
        user["max_energy"] = 1000 + user["energy_level"] * 100
        user["energy"] = user["max_energy"]
    
    await update_user(data.user_id, user)
    
    next_level = user[f"{data.boost_type}_level"]
    next_price = prices[data.boost_type][next_level] if next_level < len(prices[data.boost_type]) else 0
    
    return {
        "coins": user["coins"],
        "new_level": user[f"{data.boost_type}_level"],
        "next_cost": next_price,
        "profit_per_tap": user.get("profit_per_tap", 1),
        "profit_per_hour": user.get("profit_per_hour", 100),
        "max_energy": user.get("max_energy", 1000)
    }

@app.post("/api/recover-energy")
async def recover_energy(data: EnergyData):
    """Восстановить энергию"""
    user = await get_user(data.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Восстанавливаем 1 энергии в секунду (вызывается раз в 3 секунды)
    max_energy = user.get("max_energy", 1000)
    if user["energy"] < max_energy:
        user["energy"] = min(max_energy, user["energy"] + 10)  # +3 за 3 секунды
        await update_user(data.user_id, user)
    
    return {"energy": user["energy"]}

@app.post("/api/passive-income")
async def passive_income(data: EnergyData):
    """Пассивный доход"""
    user = await get_user(data.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Начисляем пассивный доход - ОКРУГЛЯЕМ!
    profit_per_second = user.get("profit_per_hour", 3200) / 3600
    # Добавляем целое число, отбрасывая дробную часть
    user["coins"] += int(profit_per_second)  # ← ИЗМЕНЕНО!
    
    await update_user(data.user_id, user)
    
    return {"coins": user["coins"]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

@app.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int):
    """Получить актуальные цены улучшений для пользователя"""
    user = await get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Базовые цены для каждого уровня (как в твоей функции process_upgrade)
    base_prices = {
        'multitap': [500, 2000, 5000, 20000, 50000],
        'profit': [1000, 5000, 15000, 50000, 200000],
        'energy': [800, 3000, 10000, 30000, 100000],
        'boost': [500, 2000, 8000, 30000, 100000]
    }

    # Текущие уровни пользователя
    current_levels = {
        'multitap': user.get('multitap_level', 0),
        'profit': user.get('profit_level', 0),
        'energy': user.get('energy_level', 0),
        'boost': user.get('boost_level', 0) 
    }

    prices = {}
    for boost, levels in base_prices.items():
        level = current_levels[boost]
        # Если уровень меньше максимального, возвращаем цену, иначе 0
        if level < len(levels):
            prices[boost] = levels[level]
        else:
            prices[boost] = 0  # Достигнут максимум

    return prices