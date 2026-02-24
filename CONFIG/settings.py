import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# Берем токен из переменных окружения
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Настройки базы данных
DATABASE_URL = os.getenv('DATABASE_URL', "sqlite+aiosqlite:///database.db")

# Проверка, что токен загрузился
if not BOT_TOKEN:
    raise ValueError("❌ Токен не найден! Создай файл .env с BOT_TOKEN=твой_токен")