import asyncio
import os
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiohttp import web

from CONFIG.settings import BOT_TOKEN
from DATABASE.base import init_db, get_user, add_user

# Настройка логирования
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
    WEBHOOK_URL = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}/webhook"
    await bot.set_webhook(WEBHOOK_URL)
    logging.info(f"Webhook set to {WEBHOOK_URL}")

async def on_shutdown(bot: Bot):
    await bot.delete_webhook()

async def handle_webhook(request):
    update = types.Update(**await request.json())
    await dp.feed_update(bot, update)
    return web.Response()

def main():
    # Создаем aiohttp приложение
    app = web.Application()
    
    # Регистрируем вебхук
    app.router.add_post('/webhook', handle_webhook)
    
    # Запускаем
    port = int(os.environ.get("PORT", 8001))
    logging.info(f"Starting bot on port {port}")
    
    # Устанавливаем вебхук при старте
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(on_startup(bot))
    
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()