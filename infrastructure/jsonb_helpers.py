"""
JSONB Helpers for Atomic Partial Updates

Prevents race conditions in extra_data updates by using PostgreSQL JSONB operators
instead of read-modify-write pattern.

NOTE: These functions use PostgreSQL-specific JSONB syntax and will NOT work with SQLite.
For SQLite testing, use the atomic update functions in DATABASE.base instead.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import os


def _is_postgresql() -> bool:
    """Check if we're using PostgreSQL (vs SQLite for testing)"""
    db_url = os.getenv("DATABASE_URL", "")
    return db_url.startswith("postgresql")


async def jsonb_set_field(
    session: AsyncSession,
    user_id: int,
    field_path: str,
    value: any,
) -> None:
    """
    Atomically set a field in extra_data JSONB.

    Args:
        session: DB session
        user_id: User ID
        field_path: JSON path (e.g., "daily_reward_claimed_days")
        value: Value to set (will be JSON-encoded)

    Example:
        await jsonb_set_field(session, user_id, "owned_skins", ["skin1", "skin2"])
    """
    import json

    if _is_postgresql():
        # extra_data is stored as String/TEXT, need to convert to jsonb for operations
        # Inject both path and value to avoid :: parameter issues with asyncpg
        path_literal = f"{{{field_path}}}"
        value_json = json.dumps(value)

        await session.execute(
            text(f"""
                UPDATE users 
                SET extra_data = jsonb_set(
                    COALESCE(extra_data::jsonb, '{{}}'::jsonb),
                    '{path_literal}'::text[],
                    '{value_json}'::jsonb,
                    true
                )::text
                WHERE user_id = :uid
            """),
            {"uid": user_id},
        )
    else:
        # SQLite fallback: read-modify-write (not atomic, for testing only)
        result = await session.execute(
            text("SELECT extra_data FROM users WHERE user_id = :uid"), {"uid": user_id}
        )
        extra_data = result.scalar()
        extra = json.loads(extra_data) if extra_data else {}
        extra[field_path] = value
        await session.execute(
            text("UPDATE users SET extra_data = :data WHERE user_id = :uid"),
            {"uid": user_id, "data": json.dumps(extra)},
        )


async def jsonb_append_to_array(
    session: AsyncSession,
    user_id: int,
    field_path: str,
    value: any,
) -> None:
    """
    Atomically append to an array in extra_data JSONB.

    Args:
        session: DB session
        user_id: User ID
        field_path: JSON path (e.g., "owned_skins")
        value: Value to append (single item, will be wrapped in array)

    Example:
        await jsonb_append_to_array(session, user_id, "owned_skins", "new_skin.png")
    """
    import json

    await session.execute(
        text("""
            UPDATE users 
            SET extra_data = jsonb_set(
                COALESCE(extra_data, '{}'::jsonb),
                :path,
                COALESCE(extra_data->:field, '[]'::jsonb) || CAST(:value AS jsonb),
                true
            )
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "path": f"{{{field_path}}}",
            "field": field_path,
            "value": json.dumps([value]),
        },
    )


async def jsonb_set_nested_field(
    session: AsyncSession,
    user_id: int,
    parent_path: str,
    child_key: str,
    value: any,
) -> None:
    """
    Atomically set a nested field in extra_data JSONB.

    Args:
        session: DB session
        user_id: User ID
        parent_path: Parent JSON path (e.g., "active_boosts")
        child_key: Child key (e.g., "daily_infinite_energy")
        value: Value to set (will be JSON-encoded)

    Example:
        await jsonb_set_nested_field(
            session, user_id, "active_boosts", "daily_infinite_energy",
            {"active": True, "expires_at": "2026-04-07T12:00:00"}
        )
    """
    import json

    await session.execute(
        text("""
            UPDATE users 
            SET extra_data = jsonb_set(
                COALESCE(extra_data, '{}'::jsonb),
                :path,
                CAST(:value AS jsonb),
                true
            )
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "path": f"{{{parent_path},{child_key}}}",
            "value": json.dumps(value),
        },
    )


async def jsonb_update_multiple_fields(
    session: AsyncSession,
    user_id: int,
    updates: dict,
) -> None:
    """
    Atomically update multiple top-level fields in extra_data JSONB.

    Args:
        session: DB session
        user_id: User ID
        updates: Dict of field_path -> value

    Example:
        await jsonb_update_multiple_fields(session, user_id, {
            "daily_reward_claimed_days": [1, 2, 3],
            "daily_reward_last_claim_date": "2026-04-07",
        })
    """
    import json

    # Build JSONB merge operation
    merge_obj = json.dumps(updates)

    await session.execute(
        text("""
            UPDATE users 
            SET extra_data = COALESCE(extra_data, '{}'::jsonb) || CAST(:merge AS jsonb)
            WHERE user_id = :uid
        """),
        {
            "uid": user_id,
            "merge": merge_obj,
        },
    )
