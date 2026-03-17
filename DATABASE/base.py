import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, BigInteger, select, update, DateTime
import json
from datetime import datetime
import logging

# Используем переменную окружения или значение по умолчанию
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://...")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


# Модель пользователя
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, unique=True, index=True)
    username = Column(String, nullable=True)
    coins = Column(BigInteger, default=0)

    profit_per_hour = Column(BigInteger, default=100)
    profit_per_tap = Column(Integer, default=1)
    energy = Column(Integer, default=1000)
    max_energy = Column(Integer, default=1000)
    level = Column(Integer, default=0)

    multitap_level = Column(Integer, default=0)
    profit_level = Column(Integer, default=0)
    energy_level = Column(Integer, default=0)
    boost_level = Column(Integer, default=0)

    last_passive_income = Column(DateTime, default=datetime.utcnow)
    last_energy_update = Column(DateTime, default=datetime.utcnow)

    referrer_id = Column(BigInteger, nullable=True)
    referral_count = Column(Integer, default=0)
    referral_earnings = Column(BigInteger, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow)

    extra_data = Column(String, default="{}")
    
    luck_level = Column(Integer, default=0)


# ==================== МОДЕЛЬ ЗАДАНИЙ ====================
class UserTask(Base):
    __tablename__ = 'user_tasks'
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, index=True)
    task_id = Column(String)
    completed_at = Column(DateTime, default=datetime.utcnow)


# ==================== ФУНКЦИИ ====================

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


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
                "energy": user.energy,
                "max_energy": user.max_energy,
                "level": user.level,
                "multitap_level": user.multitap_level,
                "profit_level": user.profit_level,
                "energy_level": user.energy_level,
                "last_passive_income": user.last_passive_income,
                "last_energy_update": user.last_energy_update,
                "luck_level": user.luck_level,
                "referral_count": user.referral_count,
                "referral_earnings": user.referral_earnings,
                "extra_data": json.loads(user.extra_data)
            }
        return None


# ==================== РЕФЕРАЛЬНАЯ СИСТЕМА ====================

async def add_referral_bonus(referrer_id: int, referral_id: int):
    """Начисление бонуса рефереру за нового реферала"""
    try:
        async with AsyncSessionLocal() as session:
            # Получаем реферера
            result = await session.execute(
                select(User).where(User.user_id == referrer_id)
            )
            referrer = result.scalar_one_or_none()
            
            if not referrer:
                logging.error(f"❌ Реферер {referrer_id} не найден при попытке начисления бонуса за реферала {referral_id}")
                return False
            
            # Обновляем поля реферера
            referrer.coins += 5000  # Бонус за реферала
            referrer.referral_count += 1
            referrer.referral_earnings += 5000
            
            await session.commit()
            logging.info(f"✅ Реферер {referrer_id} получил +5000 монет за реферала {referral_id}")
            logging.info(f"📊 Теперь у реферера {referrer_id}: coins={referrer.coins}, count={referrer.referral_count}")
            
            return True
            
    except Exception as e:
        logging.error(f"❌ Ошибка начисления бонуса рефереру {referrer_id} за реферала {referral_id}: {e}")
        return False


async def get_referral_stats(user_id: int):
    """Получение реферальной статистики пользователя"""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(User).where(User.user_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                return {"count": 0, "earnings": 0}
            
            return {
                "count": user.referral_count or 0,
                "earnings": user.referral_earnings or 0
            }
            
    except Exception as e:
        logging.error(f"❌ Ошибка получения реферальной статистики для {user_id}: {e}")
        return {"count": 0, "earnings": 0}


async def get_referrals_list(user_id: int):
    """Получение списка рефералов пользователя"""
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(User).where(User.referrer_id == user_id)
            )
            referrals = result.scalars().all()
            
            return [
                {
                    "user_id": ref.user_id,
                    "username": ref.username,
                    "joined_at": ref.created_at.isoformat() if ref.created_at else None,
                    "earned": 5000  # Фиксированный бонус за регистрацию
                }
                for ref in referrals
            ]
            
    except Exception as e:
        logging.error(f"❌ Ошибка получения списка рефералов для {user_id}: {e}")
        return []


async def get_completed_tasks(user_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserTask).where(UserTask.user_id == user_id)
        )
        tasks = result.scalars().all()
        return [task.task_id for task in tasks]


async def add_completed_task(user_id: int, task_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserTask).where(
                UserTask.user_id == user_id,
                UserTask.task_id == task_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            return False
        
        new_task = UserTask(
            user_id=user_id,
            task_id=task_id
        )
        session.add(new_task)
        await session.commit()
        return True


async def add_user(user_id: int, username: str = None, referrer_id: int = None):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing
        
        new_user = User(
            user_id=user_id,
            username=username or f"user_{user_id}",
            coins=0,
            profit_per_hour=100,
            profit_per_tap=1,
            energy=1000,
            max_energy=1000,
            level=0,
            multitap_level=0,
            profit_level=0,
            energy_level=0,
            luck_level=0,
            last_passive_income=datetime.utcnow(),
            last_energy_update=datetime.utcnow(),
            referrer_id=referrer_id,
            referral_count=0,
            referral_earnings=0,
            extra_data=json.dumps({"owned_skins": ["default_SP"], "ads_watched": 0})  # ✅ ИСПРАВЛЕНО
        )
        
        session.add(new_user)
        await session.commit()
        logging.info(f"✅ Пользователь {user_id} создан, referrer_id={referrer_id}")
        
        if referrer_id:
            logging.info(f"🎯 Попытка начисления бонуса: реферер {referrer_id} за реферала {user_id}")
            await add_referral_bonus(referrer_id, user_id)
        
        return new_user


async def update_user(user_id: int, data: dict):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.user_id == user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None

        if 'coins' in data:
            user.coins = data['coins']
        if 'energy' in data:
            user.energy = data['energy']
        if 'profit_per_hour' in data:
            user.profit_per_hour = data['profit_per_hour']
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
        if 'last_passive_income' in data:
            user.last_passive_income = data['last_passive_income']
        if 'last_energy_update' in data:
            user.last_energy_update = data['last_energy_update']
        if 'extra_data' in data:
            user.extra_data = json.dumps(data['extra_data'])

        await session.commit()

        return await get_user(user_id)