import os
import logging
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from CONFIG.settings import BOT_TOKEN
from DATABASE.base import get_user, add_user

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
    
    try:
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
    except Exception as e:
        logging.error(f"Ошибка в /start: {e}")
        await message.answer("Произошла ошибка. Попробуй позже.")

# Функция, которая выполняется при старте
async def on_startup(bot: Bot):
    # Устанавливаем вебхук
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}/webhook"
    await bot.set_webhook(webhook_url)
    logging.info(f"✅ Webhook установлен на {webhook_url}")

# Функция для остановки
async def on_shutdown(bot: Bot):
    await bot.delete_webhook()
    logging.info("🔴 Webhook удален")

# Главная функция
def main():
    # Создаем aiohttp приложение
    app = web.Application()
    
    # Создаем обработчик вебхуков (правильный способ для aiogram 3.x)
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    
    # Регистрируем обработчик
    webhook_requests_handler.register(app, path="/webhook")
    
    # Регистрируем функции жизненного цикла
    app.on_startup.append(lambda _: on_startup(bot))
    app.on_shutdown.append(lambda _: on_shutdown(bot))
    
    # Получаем порт из переменной окружения
    port = int(os.environ.get("PORT", 8001))
    logging.info(f"🚀 Запуск бота на порту {port}")
    
    # Запускаем приложение
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()