import asyncio
import json
import logging
import os

import redis.asyncio as redis
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from CONFIG.settings import BOT_TOKEN
from DATABASE.base import add_user, get_user, init_db, record_stars_skin_purchase, update_user
from core.game_config import USER_CACHE_PREFIX
from core.stars_skins import get_stars_skin_price

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
REDIS_URL = os.getenv("REDIS_URL")
REENGAGEMENT_RUNTIME = (os.getenv("REENGAGEMENT_RUNTIME", "webhook") or "webhook").strip().lower()


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
        user_coins = user_data.get("coins", 0)
    else:
        await add_user(user_id, username, referrer_id)
        user_coins = 0

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Play",
                    web_app=WebAppInfo(url="https://spirix.vercel.app"),
                )
            ]
        ]
    )

    await message.answer(
        f"Welcome, {username}!\n\n"
        f"Coins: {user_coins}\n"
        f"Tap the button below to open the game:",
        reply_markup=keyboard,
    )


@dp.pre_checkout_query()
async def handle_pre_checkout_query(pre_checkout_query: types.PreCheckoutQuery) -> None:
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(F.successful_payment)
async def handle_successful_payment(message: types.Message) -> None:
    payment = message.successful_payment
    payload = payment.invoice_payload or ""
    parts = payload.split(":", 2)

    if len(parts) != 3 or parts[0] != "stars_skin":
        logger.warning("Unknown payment payload: %s", payload)
        return

    try:
        target_user_id = int(parts[1])
    except ValueError:
        logger.warning("Invalid Stars payload user id: %s", payload)
        return

    skin_id = parts[2]
    expected_price = get_stars_skin_price(skin_id)
    if expected_price is None:
        logger.warning("Payment received for unknown Stars skin: %s", skin_id)
        return

    if payment.currency != "XTR" or payment.total_amount != expected_price:
        logger.warning(
            "Unexpected payment params for %s: currency=%s amount=%s expected=%s",
            skin_id,
            payment.currency,
            payment.total_amount,
            expected_price,
        )
        return

    if message.from_user.id != target_user_id:
        logger.warning(
            "Payment user mismatch: message=%s payload=%s skin=%s",
            message.from_user.id,
            target_user_id,
            skin_id,
        )
        return

    user = await get_user(target_user_id)
    if not user:
        await add_user(target_user_id, message.from_user.username or f"user_{target_user_id}")
        user = await get_user(target_user_id)
    if not user:
        logger.error("Failed to load user after successful payment: %s", target_user_id)
        return

    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    owned_skins = list(extra.get("owned_skins", ["default.pngSP"]))
    if skin_id not in owned_skins:
        owned_skins.append(skin_id)
        extra["owned_skins"] = owned_skins
        await update_user(target_user_id, {"extra_data": extra})
        await invalidate_user_cache(target_user_id)

    await record_stars_skin_purchase(
        user_id=target_user_id,
        username=message.from_user.username,
        skin_id=skin_id,
        stars_amount=payment.total_amount,
        currency=payment.currency,
        telegram_charge_id=getattr(payment, "telegram_payment_charge_id", None),
    )

    await message.answer(f"Skin {skin_id} unlocked.")


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
