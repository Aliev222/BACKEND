import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from CONFIG.settings import BOT_TOKEN
from DATABASE.base import init_db, add_user, get_user

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Проверяем, есть ли пользователь в базе
    user_data = await get_user(user_id)
    
    if user_data:
        print(f"👋 Пользователь {username} уже существует в базе")
        # Используем словарь
        user_coins = user_data.get('coins', 0)
        user_energy = user_data.get('energy', 1000)
        user_max_energy = user_data.get('max_energy', 1000)
    else:
        # Добавляем нового пользователя
        await add_user(user_id, username)
        # Получаем данные словарем
        user_data = await get_user(user_id)
        user_coins = user_data.get('coins', 0) if user_data else 0
        user_energy = user_data.get('energy', 1000) if user_data else 1000
        user_max_energy = user_data.get('max_energy', 1000) if user_data else 1000
        print(f"✅ Новый пользователь {username} добавлен в базу")
    
    # Создаём кнопку для Mini App
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="🎮 Играть", 
                web_app=WebAppInfo(url="https://spirix.vercel.app/")
            )]
        ]
    )
    
    await message.answer(
        f"👋 Привет, {username}!\n\n"
        f"💰 Монет: {user_coins}\n"
        f"Нажми кнопку ниже, чтобы играть:",
        reply_markup=keyboard
    )

# Запуск бота
async def main():
    await init_db()
    print("🚀 Бот запущен...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

