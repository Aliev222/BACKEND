import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from CONFIG.settings import BOT_TOKEN
from DATABASE.base import init_db, add_user, get_user

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчераы
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Добавляем пользователя в БД
    await add_user(user_id, username)
    
    # Создаем кнопку для открытия Mini App
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Играть", web_app=types.WebAppInfo(url="https://ваш-сайт.com"))]
        ]
    )
    
    await message.answer(
        "👻 Добро пожаловать в RYAOHO!\n\n"
        "Тапай по хомяку, зарабатывай монеты и улучшай свои навыки!",
        reply_markup=keyboard
    )

# Обработка данных из Mini App
@dp.message()
async def handle_webapp_data(message: types.Message):
    if message.web_app_data:
        data = message.web_app_data.data
        # Обработка данных от клиента
        await message.answer(f"Получены данные: {data}")

# Запуск бота
async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())