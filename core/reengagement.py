import asyncio
import json
import logging
import os
import random
from datetime import datetime

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from sqlalchemy import select

from DATABASE.base import AsyncSessionLocal, User
from core.game_logic import calculate_current_energy, resolve_max_energy

logger = logging.getLogger(__name__)

WEBAPP_URL = os.getenv("WEBAPP_URL", "https://spirix.vercel.app")
REENGAGEMENT_CHECK_INTERVAL_SECONDS = 600
REENGAGEMENT_STAGE_HOURS = 3


def _parse_extra(extra_raw) -> dict:
    if isinstance(extra_raw, dict):
        return extra_raw
    if isinstance(extra_raw, str) and extra_raw:
        try:
            return json.loads(extra_raw)
        except Exception:
            return {}
    return {}


def _idle_stage(last_activity_at: str | None) -> int:
    if not last_activity_at:
        return 0
    try:
        last_dt = datetime.fromisoformat(last_activity_at)
    except Exception:
        return 0
    idle_hours = max(0.0, (datetime.utcnow() - last_dt).total_seconds() / 3600)
    return int(idle_hours // REENGAGEMENT_STAGE_HOURS)


def _build_reason_and_text(user_row: User, extra: dict, stage: int) -> tuple[str, str]:
    current_energy = calculate_current_energy({
        "energy": user_row.energy,
        "max_energy": user_row.max_energy,
        "last_energy_update": user_row.last_energy_update,
        "energy_level": user_row.energy_level,
    }, datetime.utcnow())
    max_energy = resolve_max_energy({
        "max_energy": user_row.max_energy,
        "energy_level": user_row.energy_level,
    })

    today = datetime.utcnow().date().isoformat()
    daily_claimed_today = extra.get("daily_reward_last_claim_date") == today
    daily_days = int(extra.get("daily_reward_claimed_days", 0) or 0)
    profit_per_hour = int(user_row.profit_per_hour or 0)

    if not daily_claimed_today:
        day = min(daily_days + 1, 30)
        return (
            "daily_reward",
            f"🎁 Day {day} is waiting. Miss the streak now and tomorrow feels worse. Jump back in."
        )

    if current_energy >= max_energy:
        return (
            "full_energy",
            f"⚡ Your energy is full again. One minute in Spirit Clicker and the run wakes up fast."
        )

    if profit_per_hour > 0:
        return (
            "passive_income",
            f"💰 Your spirit kept working. You're sitting on roughly {profit_per_hour:,}/h and it’s being wasted offline."
        )

    variants = [
        "👻 Something weird is moving inside the arena. Tap back in before the ghost bonus finds someone else.",
        "🏁 The board does not stay kind for long. Come back and push your score before the gap grows.",
        "🔥 Your combo cooled off. A few taps now will bring the whole run back to life.",
    ]
    return ("general", variants[(stage - 1) % len(variants)])


async def run_reengagement_once(bot: Bot) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Open Spirit Clicker", web_app=WebAppInfo(url=WEBAPP_URL))]
            ]
        )

        for user in users:
            if not user.user_id:
                continue

            extra = _parse_extra(user.extra_data)
            stage = _idle_stage(extra.get("last_activity_at"))
            sent_stage = int(extra.get("push_idle_stage", 0) or 0)

            if stage < 1 or stage <= sent_stage:
                continue

            reason, text = _build_reason_and_text(user, extra, stage)

            try:
                await bot.send_message(
                    chat_id=user.user_id,
                    text=text,
                    reply_markup=keyboard,
                )
            except Exception as exc:
                logger.warning("Re-engagement push failed for %s: %s", user.user_id, exc)
                continue

            extra["push_idle_stage"] = stage
            extra["last_push_at"] = datetime.utcnow().isoformat()
            extra["last_push_reason"] = reason
            user.extra_data = json.dumps(extra)

        await session.commit()


async def reengagement_loop(bot: Bot) -> None:
    while True:
        try:
            await run_reengagement_once(bot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Re-engagement loop error: %s", exc)
        await asyncio.sleep(REENGAGEMENT_CHECK_INTERVAL_SECONDS)
