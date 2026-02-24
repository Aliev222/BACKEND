import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from CONFIG.settings import BOT_TOKEN
from DATABASE.base import init_db, create_user, get_user

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
    existing_user = await get_user(user_id)
    
    if existing_user:
        print(f"👋 Пользователь {username} уже существует в базе")
    else:
        # Добавляем только если нет
        await create_user(user_id, username)
        print(f"✅ Новый пользователь {username} добавлен в базу")
    
    # Создаём кнопку для Mini App
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="🎮 Играть", 
                web_app=WebAppInfo(url="https://ryoho-eta.vercel.app")
            )]
        ]
    )
    
    await message.answer(
        f"👋 Привет, {username}!\n\n"
        f"💰 Твой баланс: {existing_user['coins'] if existing_user else 0} монет\n"
        f"⚡ Уровень: {existing_user['level'] if existing_user else 1}\n\n"
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

