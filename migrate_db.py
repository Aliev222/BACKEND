import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import create_engine, text


def _get_sync_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    sync_url = database_url.replace("postgresql+asyncpg://", "postgresql://").replace(
        "sqlite+aiosqlite:///", "sqlite:///"
    )
    if not sync_url.startswith(("postgresql://", "postgres://")):
        return sync_url

    # psycopg2 doesn't accept `ssl=` query option; normalize to `sslmode=...`.
    parts = urlsplit(sync_url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    ssl_value = params.pop("ssl", None)
    if ssl_value is not None and "sslmode" not in params:
        normalized = str(ssl_value).strip().lower()
        if normalized in {"1", "true", "yes", "on", "require", "required"}:
            params["sslmode"] = "require"
        elif normalized in {"0", "false", "no", "off", "disable", "disabled"}:
            params["sslmode"] = "disable"

    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment)
    )


def migrate() -> None:
    engine = create_engine(_get_sync_database_url())
    dialect_name = engine.dialect.name.lower()

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
        ADD COLUMN IF NOT EXISTS rebirth_count INTEGER DEFAULT 0
        """,
        """
        ALTER TABLE users
        ADD COLUMN IF NOT EXISTS level INTEGER DEFAULT 0
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
    ]

    if dialect_name == "postgresql":
        statements.append(
            """
            UPDATE users
            SET level = GREATEST(
                COALESCE(level, 0),
                COALESCE(multitap_level, 0),
                COALESCE(profit_level, 0),
                COALESCE(energy_level, 0)
            )
            """
        )
    else:
        statements.append(
            """
            UPDATE users
            SET level = MAX(
                COALESCE(level, 0),
                COALESCE(multitap_level, 0),
                COALESCE(profit_level, 0),
                COALESCE(energy_level, 0)
            )
            """
        )

    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))

    print("Database migration completed")


if __name__ == "__main__":
    migrate()
