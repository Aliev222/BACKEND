import os

from sqlalchemy import create_engine, text


def _get_sync_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    return (
        database_url
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("sqlite+aiosqlite:///", "sqlite:///")
    )


def migrate() -> None:
    engine = create_engine(_get_sync_database_url())

    statements = [
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS referrer_id BIGINT
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS referral_earnings BIGINT DEFAULT 0
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS last_energy_update TIMESTAMP
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS boost_level INTEGER DEFAULT 0
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS luck_level INTEGER DEFAULT 0
        """,
        """
        UPDATE users
        SET last_energy_update = CURRENT_TIMESTAMP
        WHERE last_energy_update IS NULL
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_users_referrer_id
        ON users (referrer_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_users_created_at
        ON users (created_at)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_user_tasks_user_id_completed_at
        ON user_tasks (user_id, completed_at)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_user_tasks_user_id_task_id
        ON user_tasks (user_id, task_id)
        """,
        """
        CREATE TABLE IF NOT EXISTS crash_ghost_cashouts (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            session_id VARCHAR(255) NOT NULL,
            bet BIGINT NOT NULL,
            payout BIGINT NOT NULL,
            multiplier VARCHAR(16) NOT NULL,
            profit BIGINT NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_crash_ghost_cashouts_session_id
        ON crash_ghost_cashouts (session_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_crash_ghost_cashouts_user_id
        ON crash_ghost_cashouts (user_id)
        """,
    ]

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    print("Database migration completed")


if __name__ == "__main__":
    migrate()
