from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, BigInteger, select
import json
import os

# Конфигурация базы данных
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///database.db")

# Создаем движок и сессию
engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class User(Base):
    """Модель пользователя в базе данных"""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, index=True, nullable=False)
    username = Column(String, nullable=True)

    # Основные ресурсы
    coins = Column(BigInteger, default=0)
    energy = Column(Integer, default=1000)
    max_energy = Column(Integer, default=1000)

    # Доходы
    profit_per_tap = Column(Integer, default=1)      # Монет за клик
    profit_per_hour = Column(Integer, default=100)   # Пассивный доход в час

    # Уровни улучшений (0-10)
    multitap_level = Column(Integer, default=0)      # Уровень мультитапа
    profit_level = Column(Integer, default=0)        # Уровень прибыли
    energy_level = Column(Integer, default=0)         # Уровень энергии

    # Дополнительные данные
    extra_data = Column(String, default="{}")


async def init_db():
    """Создание таблиц в базе данных"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_user(user_id: int):
    """Получить пользователя по ID"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None

        return {
            "user_id": user.user_id,
            "username": user.username,
            "coins": user.coins,
            "energy": user.energy,
            "max_energy": user.max_energy,
            "profit_per_tap": user.profit_per_tap,
            "profit_per_hour": user.profit_per_hour,
            "multitap_level": user.multitap_level,
            "profit_level": user.profit_level,
            "energy_level": user.energy_level,
            "extra_data": json.loads(user.extra_data)
        }


async def create_user(user_id: int, username: str = None):
    """Создать нового пользователя"""
    async with AsyncSessionLocal() as session:
        # Проверяем, существует ли уже
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        if result.scalar_one_or_none():
            return None

        # Создаем с базовыми значениями
        new_user = User(
            user_id=user_id,
            username=username or f"user_{user_id}",
            coins=0,
            energy=1000,
            max_energy=1000,
            profit_per_tap=1,
            profit_per_hour=100,
            multitap_level=0,
            profit_level=0,
            energy_level=0
        )

        session.add(new_user)
        await session.commit()
        return new_user


async def update_user(user_id: int, data: dict):
    """Обновить данные пользователя"""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None

        # Обновляем только переданные поля
        for key, value in data.items():
            if hasattr(user, key):
                setattr(user, key, value)

        await session.commit()
        return await get_user(user_id)