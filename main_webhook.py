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

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ===== ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ =====
async def create_tables():
    """Создаёт таблицы в PostgreSQL, если их нет"""
    try:
        logging.info("🔄 [Бот] Проверка и создание таблиц...")
        await init_db()
        logging.info("✅ [Бот] Таблицы успешно созданы или уже существуют.")
    except Exception as e:
        logging.error(f"❌ [Бот] Ошибка при создании таблиц: {e}")
        raise

# ===== КОМАНДА /start =====
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Получаем реферальный параметр
    args = message.text.split()
    referrer_id = None
    if len(args) > 1 and args[1].startswith('ref_'):
        try:
            referrer_id = int(args[1].replace('ref_', ''))
            logging.info(f"👥 Реферальный переход: {user_id} от {referrer_id}")
        except ValueError:
            pass
    
    try:
        # Получаем или создаём пользователя
        user_data = await get_user(user_id)
        
        if user_data:
            user_coins = user_data.get('coins', 0)
            user_energy = user_data.get('energy', 1000)
            user_max_energy = user_data.get('max_energy', 1000)
            logging.info(f"👋 Пользователь {user_id} найден: монет={user_coins}")
        else:
            # Создаём нового пользователя с рефералом
            await add_user(user_id, username, referrer_id)
            user_coins = 0
            user_energy = 1000
            user_max_energy = 1000
            logging.info(f"🆕 Создан новый пользователь {user_id}, реферал: {referrer_id}")
        
        # Кнопка для игры
        GAME_URL = "https://ryoho-eta.vercel.app"
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text="🎮 Играть", 
                    web_app=WebAppInfo(url=GAME_URL)
                )]
            ]
        )
        
        # Отправляем ответ
        await message.answer(
            f"👋 Привет, {username}!\n\n"
            f"💰 Монет: {user_coins}\n"
            f"⚡ Энергия: {user_energy}/{user_max_energy}\n\n"
            f"Нажми кнопку ниже, чтобы играть:",
            reply_markup=keyboard
        )
        logging.info(f"✅ Ответ отправлен пользователю {user_id}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка в /start: {e}", exc_info=True)
        await message.answer("Произошла ошибка. Попробуй позже.")

# ===== ФУНКЦИИ ВЕБХУКА =====
async def on_startup(bot: Bot):
    """Выполняется при запуске приложения"""
    try:
        # Сначала создаём таблицы
        await create_tables()
        
        # Удаляем старый вебхук и все ожидающие обновления
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Старый вебхук удалён")
        
        # Получаем URL сервиса
        render_url = os.environ.get('RENDER_EXTERNAL_HOSTNAME')
        if not render_url:
            logging.error("❌ RENDER_EXTERNAL_HOSTNAME не задан!")
            return
        
        webhook_url = f"https://{render_url}/webhook"
        await bot.set_webhook(webhook_url)
        logging.info(f"✅ Вебхук установлен на {webhook_url}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка при установке вебхука: {e}", exc_info=True)

async def on_shutdown(bot: Bot):
    """Выполняется при остановке"""
    try:
        # Не удаляем вебхук при каждом ответе!
        # Просто логируем остановку
        logging.info("🔄 Бот останавливается...")
    except Exception as e:
        logging.error(f"❌ Ошибка при остановке: {e}")

# ===== ГЛАВНАЯ ФУНКЦИЯ =====
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