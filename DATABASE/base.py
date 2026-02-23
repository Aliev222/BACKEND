from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, BigInteger, select, update
import json

# Используем SQLite для простоты
DATABASE_URL = "sqlite+aiosqlite:///database.db"

engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# Модель пользователя
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, index=True)
    username = Column(String, nullable=True)
    coins = Column(BigInteger, default=0)
    
    # Новые поля для кликера
    profit_per_hour = Column(BigInteger, default=3200)
    profit_per_tap = Column(Integer, default=1)
    energy = Column(Integer, default=500)
    max_energy = Column(Integer, default=1000)
    level = Column(Integer, default=3)
    
    # Уровни улучшений
    multitap_level = Column(Integer, default=0)
    profit_level = Column(Integer, default=0)
    energy_level = Column(Integer, default=0)
    boost_level = Column(Integer, default=0)
    
    # Дополнительные данные в JSON
    extra_data = Column(String, default="{}")

# Создание таблиц
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Получить пользователя
async def get_user(user_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if user:
            return {
                "user_id": user.user_id,
                "username": user.username,
                "coins": user.coins,
                "profit_per_hour": user.profit_per_hour,
                "profit_per_tap": user.profit_per_tap,
                "energy": user.energy,
                "max_energy": user.max_energy,
                "level": user.level,
                "multitap_level": user.multitap_level,
                "profit_level": user.profit_level,
                "energy_level": user.energy_level,
                "extra_data": json.loads(user.extra_data)
            }
        return None

# Добавить нового пользователя
async def add_user(user_id: int, username: str = None):
    async with AsyncSessionLocal() as session:
        # Проверяем, есть ли уже
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        existing = result.scalar_one_or_none()
        
        if existing:
            return existing
        
        # Создаем нового
        new_user = User(
            user_id=user_id,
            username=username or f"user_{user_id}",
            coins=0,
            profit_per_hour=100,
            profit_per_tap=1,
            energy=1000,
            max_energy=1000,
            level=1,
            multitap_level=1,
            profit_level=1,
            energy_level=1,
        )
        session.add(new_user)
        await session.commit()
        return new_user

# 🔥 ВОТ ЭТА НОВАЯ ФУНКЦИЯ - ОБНОВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ
async def update_user(user_id: int, data: dict):
    """Обновляет данные пользователя"""
    async with AsyncSessionLocal() as session:
        # Получаем пользователя
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return None
        
        # Обновляем поля из словаря
        if 'coins' in data:
            user.coins = data['coins']
        if 'energy' in data:
            user.energy = data['energy']
        if 'profit_per_hour' in data:
            user.profit_per_hour = data['profit_per_hour']
        if 'profit_per_tap' in data:
            user.profit_per_tap = data['profit_per_tap']
        if 'max_energy' in data:
            user.max_energy = data['max_energy']
        if 'level' in data:
            user.level = data['level']
        if 'multitap_level' in data:
            user.multitap_level = data['multitap_level']
        if 'profit_level' in data:
            user.profit_level = data['profit_level']
        if 'energy_level' in data:
            user.energy_level = data['energy_level']
        if 'extra_data' in data:
            user.extra_data = json.dumps(data['extra_data'])
        
        await session.commit()
        
        # Возвращаем обновленные данные
        return await get_user(user_id)