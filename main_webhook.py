import asyncio
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from CONFIG.settings import BOT_TOKEN
from DATABASE.base import init_db, get_user, add_user

# Настройка логирования
import logging
logging.basicConfig(level=logging.INFO)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    user_data = await get_user(user_id)
    
    if user_data:
        user_coins = user_data.get('coins', 0)
        user_energy = user_data.get('energy', 1000)
        user_max_energy = user_data.get('max_energy', 1000)
    else:
        await add_user(user_id, username)
        user_coins = 0
        user_energy = 1000
        user_max_energy = 1000
    
    # URL твоего API (который уже работает)
    GAME_URL = "https://ryoho-eta.vercel.app"
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="🎮 Играть", 
                web_app=WebAppInfo(url=GAME_URL)
            )]
        ]
    )
    
    await message.answer(
        f"👋 Привет, {username}!\n\n"
        f"💰 Монет: {user_coins}\n"
        f"⚡ Энергия: {user_energy}/{user_max_energy}\n\n"
        f"Нажми кнопку ниже, чтобы играть:",
        reply_markup=keyboard
    )

# Настройка вебхука
async def on_startup(bot: Bot):
    await bot.set_webhook(f"https://ryoho-bot.onrender.com/webhook")

def main():
    # Создаем aiohttp приложение
    app = web.Application()
    
    # Настраиваем вебхук
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_requests_handler.register(app, path="/webhook")
    
    # Запускаем
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8001)))

if __name__ == "__main__":
    main()