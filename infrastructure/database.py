import os
from dotenv import load_dotenv
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is required")

if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "50"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "100"))
POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_SSL = os.getenv("DB_SSL", "1").strip().lower() in {"1", "true", "yes", "on", "require"}

connect_args = {"ssl": True} if DB_SSL else {}

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_recycle=POOL_RECYCLE,
    pool_timeout=POOL_TIMEOUT,
    pool_pre_ping=True,
    connect_args=connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def healthcheck_db() -> bool:
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(select(1))
        return True
    except Exception:
        return False
