import asyncio
import logging
import os

import redis.asyncio as redis
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from CONFIG.settings import BOT_TOKEN
from DATABASE.base import (
    add_user,
    get_user,
    init_db,
    update_user,
)
from core.game_config import USER_CACHE_PREFIX
from infrastructure.redis import get_redis_or_none

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
REDIS_URL = os.getenv("REDIS_URL")
REENGAGEMENT_RUNTIME = (
    (os.getenv("REENGAGEMENT_RUNTIME", "webhook") or "webhook").strip().lower()
)
START_PHOTO_URL = (os.getenv("START_PHOTO_URL") or "").strip()


async def invalidate_user_cache(user_id: int) -> None:
    if not REDIS_URL:
        return
    try:
        client = redis.from_url(REDIS_URL, decode_responses=True)
        await client.delete(f"{USER_CACHE_PREFIX}{user_id}")
        await client.aclose()
    except Exception as exc:
        logger.warning("Failed to invalidate cache for %s: %s", user_id, exc)


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"

    referrer_id = None
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].replace("ref_", ""))
        except ValueError:
            referrer_id = None

    user_data = await get_user(user_id)
    if user_data:
        redis_conn = await get_redis_or_none()
        coins_hot = await redis_conn.get(f"coins_hot:{user_id}") if redis_conn else None
        if coins_hot is not None:
            user_coins = int(coins_hot)
        else:
            user_coins = int(user_data.get("coins", 0))

    else:
        await add_user(user_id, username, referrer_id)
        user_coins = 0

    webapp_url = "https://spirix.vercel.app"
    if referrer_id:
        webapp_url = f"{webapp_url}?ref={referrer_id}"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Play",
                    web_app=WebAppInfo(url=webapp_url),
                )
            ]
        ]
    )

    caption = (
        f"👋 Welcome, {username}!\n\n"
        f"🪙 Your click coins: {user_coins}\n"
        f"🚀 Tap the button below to open the game."
    )

    if START_PHOTO_URL:
        try:
            await message.answer_photo(
                photo=START_PHOTO_URL,
                caption=caption,
                reply_markup=keyboard,
            )
            return
        except Exception as exc:
            logger.warning("Failed to send /start photo, fallback to text: %s", exc)

    await message.answer(caption, reply_markup=keyboard)


@dp.pre_checkout_query()
async def handle_pre_checkout_query(pre_checkout_query: types.PreCheckoutQuery) -> None:
    # Legacy in-app payments were removed; keep graceful rejection for old clients.
    await bot.answer_pre_checkout_query(
        pre_checkout_query.id,
        ok=False,
        error_message="In-app payments are disabled. Use TON purchase in app.",
    )


async def main() -> None:
    await init_db()
    reengagement_task = None
    if REENGAGEMENT_RUNTIME == "polling":
        from core.reengagement import reengagement_loop

        reengagement_task = asyncio.create_task(reengagement_loop(bot))
        logger.info("Re-engagement loop started in polling runtime")
    logger.info("Starting bot polling")
    try:
        await dp.start_polling(bot)
    finally:
        if reengagement_task:
            reengagement_task.cancel()
            try:
                await reengagement_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
