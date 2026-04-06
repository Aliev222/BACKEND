import json
import random
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

import httpx
from fastapi import HTTPException
from sqlalchemy import select


@dataclass(frozen=True)
class TasksRewardsServiceDeps:
    logger: Any
    BOT_TOKEN: str
    TELEGRAM_VERIFY_CHANNEL: str
    TELEGRAM_MEMBER_STATUSES: set[str]
    DAILY_REWARD_MAX_DAYS: int
    parse_iso_datetime: Callable[[Any], datetime | None]
    AsyncSessionLocal: Any
    User: Any
    UserTask: Any
    get_user: Callable[[int], Awaitable[dict | None]]
    sync_hot_after_db_increment: Callable[[int, int, int], Awaitable[Any]]


async def complete_task_reward_atomically_service(
    user_id: int,
    task_id: str,
    user_updates: dict | None,
    deps: TasksRewardsServiceDeps,
) -> dict:
    from DATABASE.base import update_extra_data_atomic

    user_updates = user_updates or {}
    sync_delta = 0
    sync_new_coins = None

    async with deps.AsyncSessionLocal() as session:
        user_result = await session.execute(
            select(deps.User).where(deps.User.user_id == user_id).with_for_update()
        )
        user_row = user_result.scalar_one_or_none()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")
        old_coins = int(user_row.coins or 0)

        task_result = await session.execute(
            select(deps.UserTask).where(
                deps.UserTask.user_id == user_id,
                deps.UserTask.task_id == task_id,
            )
        )
        if task_result.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Task already completed")

        session.add(deps.UserTask(user_id=user_id, task_id=task_id))

        # Apply updates except extra_data (handled separately)
        for field, value in user_updates.items():
            if field != "extra_data":
                setattr(user_row, field, value)

        await session.commit()
        if "coins" in user_updates:
            sync_new_coins = int(user_row.coins or 0)
            sync_delta = sync_new_coins - old_coins

    # Update extra_data.completed_tasks atomically (strict mode)
    if "extra_data" in user_updates:
        result = await update_extra_data_atomic(
            user_id,
            "completed_tasks",
            "append_unique",
            task_id,
            allow_lossy_fallback=False,
        )
        if result is None:
            deps.logger.warning(
                f"Failed to update extra_data.completed_tasks for user {user_id}, "
                f"task {task_id} (strict mode conflict). UserTask table is authoritative."
            )

    if sync_delta > 0 and sync_new_coins is not None:
        await deps.sync_hot_after_db_increment(user_id, sync_delta, sync_new_coins)

    return await deps.get_user(user_id)


def resolve_video_task_coin_drop_service() -> int:
    roll = random.random()
    if roll < 0.55:
        return random.randint(200, 1000)
    if roll < 0.80:
        return random.randint(1000, 5000)
    if roll < 0.92:
        return random.randint(5000, 12000)
    if roll < 0.98:
        return random.randint(12000, 20000)
    return random.randint(20000, 30000)


def get_video_task_last_claims_service(extra: dict) -> dict:
    claims = extra.get("video_task_last_claims", {})
    return claims if isinstance(claims, dict) else {}


def get_video_task_boosts_service(extra: dict) -> dict:
    boosts = extra.get("video_task_boosts", {})
    return boosts if isinstance(boosts, dict) else {}


def get_active_video_task_boost_service(
    extra: dict,
    boost_key: str,
    deps: TasksRewardsServiceDeps,
) -> tuple[bool, str | None, int]:
    boosts = get_video_task_boosts_service(extra)
    boost = boosts.get(boost_key)
    if not isinstance(boost, dict):
        return False, None, 1

    expires_at = deps.parse_iso_datetime(boost.get("expires_at"))
    if not expires_at or expires_at <= datetime.utcnow():
        return False, None, 1

    return True, expires_at.isoformat(), int(boost.get("multiplier", 1) or 1)


def get_daily_reward_progress_service(
    extra: dict, deps: TasksRewardsServiceDeps
) -> tuple[int, str | None]:
    claimed_days = int(extra.get("daily_reward_claimed_days", 0) or 0)
    claimed_days = max(0, min(deps.DAILY_REWARD_MAX_DAYS, claimed_days))
    last_claim_date = extra.get("daily_reward_last_claim_date")
    if not isinstance(last_claim_date, str):
        last_claim_date = None
    return claimed_days, last_claim_date


async def verify_telegram_channel_subscription_service(
    user_id: int, deps: TasksRewardsServiceDeps
) -> bool:
    if not deps.BOT_TOKEN or not deps.TELEGRAM_VERIFY_CHANNEL:
        deps.logger.warning("Telegram subscription verification is not configured")
        return False

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.get(
                f"https://api.telegram.org/bot{deps.BOT_TOKEN}/getChatMember",
                params={
                    "chat_id": deps.TELEGRAM_VERIFY_CHANNEL,
                    "user_id": user_id,
                },
            )
    except Exception as exc:
        deps.logger.warning(
            "Telegram subscription verification request failed for %s: %s", user_id, exc
        )
        return False

    if response.status_code != 200:
        deps.logger.warning(
            "Telegram subscription verification HTTP error for %s: %s",
            user_id,
            response.status_code,
        )
        return False

    try:
        payload = response.json()
    except Exception:
        deps.logger.warning(
            "Telegram subscription verification returned invalid JSON for %s", user_id
        )
        return False

    if not payload.get("ok"):
        deps.logger.warning(
            "Telegram subscription verification failed for %s: %s", user_id, payload
        )
        return False

    status = ((payload.get("result") or {}).get("status") or "").lower()
    return status in deps.TELEGRAM_MEMBER_STATUSES
