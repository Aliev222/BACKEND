import json
import logging
from datetime import datetime
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from DATABASE.base import User, UserTask

logger = logging.getLogger(__name__)


async def get_user_by_id(session: AsyncSession, user_id: int) -> dict | None:
    result = await session.execute(select(User).where(User.user_id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return None
    return _serialize_user(user)


async def get_users_by_ids(
    session: AsyncSession, user_ids: list[int]
) -> dict[int, dict]:
    result = await session.execute(select(User).where(User.user_id.in_(user_ids)))
    return {u.user_id: _serialize_user(u) for u in result.scalars().all()}


async def create_user(
    session: AsyncSession,
    user_id: int,
    username: str | None,
    referrer_id: int | None = None,
) -> User:
    from core.game_logic import get_hour_value, get_max_energy, get_tap_value

    user = User(
        user_id=user_id,
        username=username or f"user_{user_id}",
        coins=0,
        profit_per_hour=get_hour_value(0),
        profit_per_tap=get_tap_value(0),
        energy=get_max_energy(0),
        max_energy=get_max_energy(0),
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
        extra_data=json.dumps(
            {
                "owned_skins": ["default.pngSP"],
                "selected_skin": "default.pngSP",
                "ads_watched": 0,
            }
        ),
    )
    session.add(user)
    await session.flush()
    return user


async def add_coins_atomic(session: AsyncSession, user_id: int, amount: int) -> bool:
    result = await session.execute(
        update(User).where(User.user_id == user_id).values(coins=User.coins + amount)
    )
    return result.rowcount == 1


async def spend_coins_if_enough(
    session: AsyncSession, user_id: int, amount: int
) -> bool:
    result = await session.execute(
        update(User)
        .where(User.user_id == user_id)
        .where(User.coins >= amount)
        .values(coins=User.coins - amount)
    )
    return result.rowcount == 1


async def update_user_atomic(
    session: AsyncSession,
    user_id: int,
    expected_coins: int | None = None,
    expected_energy: int | None = None,
    **updates,
) -> bool:
    conditions = [User.user_id == user_id]
    if expected_coins is not None:
        conditions.append(User.coins == expected_coins)
    if expected_energy is not None:
        conditions.append(User.energy == expected_energy)

    # Serialize extra_data dict to JSON string (DB column is VARCHAR)
    if "extra_data" in updates and isinstance(updates["extra_data"], dict):
        updates["extra_data"] = json.dumps(updates["extra_data"])

    result = await session.execute(update(User).where(*conditions).values(**updates))
    return result.rowcount == 1


async def batch_update_coins(session: AsyncSession, user_coins: dict[int, int]):
    if not user_coins:
        return
    for user_id, coins in user_coins.items():
        await session.execute(
            update(User).where(User.user_id == user_id).values(coins=User.coins + coins)
        )


async def get_user_for_update(session: AsyncSession, user_id: int) -> User | None:
    result = await session.execute(
        select(User).where(User.user_id == user_id).with_for_update()
    )
    return result.scalar_one_or_none()


def _serialize_user(user: User) -> dict:
    extra_data = {}
    if user.extra_data:
        try:
            extra_data = json.loads(user.extra_data)
        except json.JSONDecodeError:
            logger.error("Failed to decode extra_data for user_id=%s", user.user_id)

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
        "boost_level": user.boost_level,
        "last_passive_income": user.last_passive_income,
        "last_energy_update": user.last_energy_update,
        "luck_level": user.luck_level,
        "referrer_id": user.referrer_id,
        "referral_count": user.referral_count,
        "referral_earnings": user.referral_earnings,
        "created_at": user.created_at,
        "extra_data": extra_data,
    }
