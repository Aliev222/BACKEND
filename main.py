import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from CONFIG.settings import BOT_TOKEN
from DATABASE.base import init_db, add_user, get_user

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Команда /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    
    # Проверяем реферальный параметр
    referrer_id = None
    args = message.text.split()
    if len(args) > 1 and args[1].startswith('ref_'):
        try:
            referrer_id = int(args[1].replace('ref_', ''))
            logger.info(f"👥 Реферальный переход: {user_id} приглашен {referrer_id}")
        except:
            pass
    
    # Проверяем, есть ли пользователь в базе
    user_data = await get_user(user_id)
    
    if user_data:
        logger.info(f"👋 Пользователь {username} уже существует")
        user_coins = user_data.get('coins', 0)
    else:
        # Добавляем нового пользователя с рефералом
        await add_user(user_id, username, referrer_id)
        user_coins = 0
        logger.info(f"✅ Новый пользователь {username} добавлен в базу")
    
    # Создаём кнопку для Mini App
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="🎮 Играть", 
                web_app=WebAppInfo(url="https://spirix.vercel.app")
            )]
        ]
    )
    
    await message.answer(
        f"👋 Привет, {username}!\n\n"
        f"💰 Монет: {user_coins}\n"
        f"👇 Нажми кнопку ниже, чтобы начать играть:",
        reply_markup=keyboard
    )

# Запуск бота
async def main():
    await init_db()
    logger.info("🚀 Бот запущен...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())