import os
import logging
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

from DATABASE.base import init_db, get_user, add_user
from CONFIG.settings import BOT_TOKEN

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# ===== ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ (перед всем остальным) =====
async def create_tables():
    """Создаёт таблицы в PostgreSQL, если их нет"""
    try:
        logging.info("🔄 [Бот] Проверка и создание таблиц...")
        await init_db()
        logging.info("✅ [Бот] Таблицы успешно созданы или уже существуют.")
    except Exception as e:
        logging.error(f"❌ [Бот] Ошибка при создании таблиц: {e}")
        logging.error("Бот не сможет работать без таблиц. Проверьте подключение к БД.")

# Запускаем создание таблиц синхронно перед запуском бота
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(create_tables())
# ===== КОНЕЦ ИНИЦИАЛИЗАЦИИ =====

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    try:
        logging.info(f"▶️ Начало обработки /start для user_id={user_id}, username={username}")
        
        # Получаем пользователя из базы
        user_data = await get_user(user_id)
        
        if user_data:
            user_coins = user_data.get('coins', 0)
            user_energy = user_data.get('energy', 1000)
            user_max_energy = user_data.get('max_energy', 1000)
        else:
            # Создаём нового пользователя
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
        logging.error(f"❌ Ошибка в /start: {e}")
        import traceback
        logging.error(traceback.format_exc())
        await message.answer("Произошла ошибка. Попробуй позже.")

# Функция при старте вебхука
async def on_startup(bot: Bot):
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}/webhook"
    await bot.set_webhook(webhook_url)
    logging.info(f"✅ Webhook установлен на {webhook_url}")

# Функция при остановке
async def on_shutdown(bot: Bot):
    await bot.delete_webhook()
    logging.info("🔴 Webhook удален")

# Главная функция
def main():
    app = web.Application()
    
    # Регистрируем обработчик вебхуков
    webhook_requests_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    webhook_requests_handler.register(app, path="/webhook")
    
    # Регистрируем функции жизненного цикла
    app.on_startup.append(lambda _: on_startup(bot))
    app.on_shutdown.append(lambda _: on_shutdown(bot))
    
    # Получаем порт
    port = int(os.environ.get("PORT", 8001))
    logging.info(f"🚀 Запуск бота на порту {port}")
    
    web.run_app(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()