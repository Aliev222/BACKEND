import asyncio
from sqlalchemy import create_engine, text

async def migrate():
    # Используй свой DATABASE_URL
    DATABASE_URL = "postgresql+asyncpg://..."
    engine = create_engine(DATABASE_URL.replace("+asyncpg", ""))
    
    with engine.connect() as conn:
        # Добавляем новые колонки
        conn.execute(text("""
            ALTER TABLE users 
            ADD COLUMN IF NOT EXISTS referrer_id BIGINT,
            ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS referral_earnings BIGINT DEFAULT 0,
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """))
        conn.commit()
        print("✅ Миграция выполнена")

if __name__ == "__main__":
    asyncio.run(migrate())