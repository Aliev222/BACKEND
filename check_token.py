import os
from dotenv import load_dotenv
from pathlib import Path

print("Текущая директория:", os.getcwd())

# Проверяем наличие .env файла
env_path = Path('.env')
if env_path.exists():
    print("✅ .env файл найден")
    print("Содержимое .env:")
    with open('.env', 'r') as f:
        print(f.read())
else:
    print("❌ .env файл НЕ найден!")
    print("Файлы в текущей папке:")
    for file in os.listdir('.'):
        print(f"  - {file}")

# Загружаем переменные
load_dotenv()
token = os.getenv('BOT_TOKEN')

if token:
    print(f"\n✅ Токен загружен: {token[:10]}...{token[-5:]}")
    print(f"Длина токена: {len(token)}")
else:
    print("\n❌ Токен НЕ загружен!")