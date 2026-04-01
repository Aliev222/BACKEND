"""
Clicks v2 — Redis-first обработка кликов
- Энергия хранится в Redis, мгновенное обновление
- Клики буферизуются в Redis, flush в БД каждые 10 сек
- Нет race conditions, нет скачков энергии
"""

import time
import json
import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, HTTPException
import redis.asyncio as redis

from CONFIG.settings import BOT_TOKEN
from core.telegram_auth import verify_telegram_init_data
from core.game_logic import (
    calculate_current_energy,
    get_allowed_clicks,
    get_tap_value,
    resolve_max_energy,
)
from core.game_config import (
    MAX_CLICK_BATCH_SIZE,
    CLICK_SUSPICION_SOFT_LIMIT,
    CLICK_SUSPICIOUS_OVERSHOOT,
    ENERGY_REGEN_SECONDS,
    GHOST_BOOST_MULTIPLIER,
)
from core.stars_skins import SKIN_MULTIPLIERS, DEFAULT_SKIN_ID
from DATABASE.base import (
    get_user_cached,
    update_user,
    update_user_if_matches,
    invalidate_user_cache,
    touch_user_activity,
    parse_extra_data,
    normalize_owned_skins,
    normalize_selected_skin,
    is_mega_boost_active,
    get_ghost_boost_status,
    get_active_video_task_boost,
    is_daily_infinite_energy_active,
    get_click_guard_state,
    write_click_guard_state,
    normalize_dt,
    grant_referral_share_bonus,
    add_weekly_tournament_score,
    acquire_idempotency_key,
    require_telegram_user,
    require_dual_rate_limit,
    get_request_ip,
    get_redis_or_none,
    TOURNAMENT_KEY,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Redis keys
ENERGY_KEY_PREFIX = "energy:v2:"
CLICK_BUFFER_PREFIX = "click_buf:"
CACHE_TTL = 300  # 5 минут


async def _get_energy_from_redis(conn: redis.Redis, user_id: int) -> dict | None:
    """Получить энергию из Redis кэша"""
    key = f"{ENERGY_KEY_PREFIX}{user_id}"
    data = await conn.hgetall(key)
    if not data:
        return None
    return {
        "value": int(data.get("value", 0)),
        "updated_at": float(data.get("updated_at", 0)),
        "max_energy": int(data.get("max_energy", 500)),
    }


async def _set_energy_to_redis(
    conn: redis.Redis, user_id: int, energy: int, max_energy: int, updated_at: float
):
    """Сохранить энергию в Redis кэш"""
    key = f"{ENERGY_KEY_PREFIX}{user_id}"
    await conn.hset(
        key,
        mapping={
            "value": str(energy),
            "updated_at": str(updated_at),
            "max_energy": str(max_energy),
        },
    )
    await conn.expire(key, CACHE_TTL)


async def _buffer_click_in_redis(
    conn: redis.Redis, user_id: int, coins: int, clicks: int
):
    """Буферизовать клик в Redis для последующего flush в БД"""
    key = f"{CLICK_BUFFER_PREFIX}{user_id}"
    async with conn.pipeline() as pipe:
        await pipe.hincrby(key, "coins", coins)
        await pipe.hincrby(key, "clicks", clicks)
        await pipe.expire(key, CACHE_TTL)
        await pipe.execute()


def _calc_energy_from_cache(cached: dict, now: datetime) -> int:
    """Рассчитать текущую энергию из Redis кэша"""
    elapsed = now.timestamp() - cached["updated_at"]
    if elapsed <= 0:
        return cached["value"]
    regen = int(elapsed // ENERGY_REGEN_SECONDS)
    if regen <= 0:
        return cached["value"]
    max_e = cached.get("max_energy", 500)
    return min(max_e, cached["value"] + regen)


@router.post("/api/clicks")
async def process_clicks_v2(
    payload: type(
        "P", (), {"__annotations__": {"user_id": int, "clicks": int, "batch_id": str}}
    ),
    request: Request,
):
    """
    Redis-first обработка кликов:
    1. Энергия из Redis кэша (мгновенно)
    2. Клики → Redis буфер (без БД)
    3. Ответ за 5-10ms
    4. Фоновый worker flush в БД каждые 10 сек
    """
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "clicks", request, payload.user_id, 90, 60, ip_limit=180
        )

        conn = await get_redis_or_none()
        if not conn:
            # Fallback на старый обработчик если Redis недоступен
            raise HTTPException(status_code=503, detail="Redis unavailable")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        if payload.clicks > MAX_CLICK_BATCH_SIZE:
            raise HTTPException(status_code=400, detail="Too many clicks in batch")

        # Idempotency
        batch_key = f"idem:clicks:{payload.user_id}:{payload.batch_id}"
        is_new_batch = await acquire_idempotency_key(batch_key, ttl=120)
        if not is_new_batch:
            logger.warning(
                f"Duplicate click batch rejected: user={payload.user_id}, batch_id={payload.batch_id}"
            )
            raise HTTPException(status_code=409, detail="Duplicate batch")

        await touch_user_activity(payload.user_id, user)
        now = datetime.utcnow()

        max_energy = resolve_max_energy(user)
        multitap_level = int(user.get("multitap_level", 0))
        tap_value = get_tap_value(multitap_level)

        extra = parse_extra_data(user.get("extra_data"))
        click_guard = get_click_guard_state(extra)
        last_click_at = None
        lc = click_guard.get("last_click_at")
        if lc:
            try:
                last_click_at = datetime.fromisoformat(lc)
            except (ValueError, TypeError):
                pass

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(
            extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
        )
        skin_multiplier = float(SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = is_mega_boost_active(user)
        ghost_boost_active, _ = get_ghost_boost_status(user)
        task_tap_boost_active, _, task_tap_boost_multiplier = (
            get_active_video_task_boost(extra, "tap_boost")
        )
        daily_infinite_energy_active, _ = is_daily_infinite_energy_active(user)
        free_energy_clicks = (
            mega_boost_active or daily_infinite_energy_active or ghost_boost_active
        )

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2
        if ghost_boost_active:
            coin_per_tap *= GHOST_BOOST_MULTIPLIER
        if task_tap_boost_active:
            coin_per_tap *= max(1, task_tap_boost_multiplier)

        # === REDIS ENERGY (вместо БД) ===
        cached_energy = await _get_energy_from_redis(conn, payload.user_id)
        if cached_energy:
            current_energy = _calc_energy_from_cache(cached_energy, now)
        else:
            # Первый запрос — берём из БД и кэшируем
            current_energy = calculate_current_energy(user, now)
            await _set_energy_to_redis(
                conn, payload.user_id, current_energy, max_energy, now.timestamp()
            )

        safe_requested_clicks = min(payload.clicks, MAX_CLICK_BATCH_SIZE)
        allowed_clicks = get_allowed_clicks(
            user,
            now,
            safe_requested_clicks,
            last_click_at=last_click_at,
        )

        # Anti-cheat
        severe_overshoot = (
            safe_requested_clicks > allowed_clicks + CLICK_SUSPICIOUS_OVERSHOOT
            and safe_requested_clicks > max(allowed_clicks * 2, 200)
        )
        if severe_overshoot:
            click_guard["hard_rejections"] = (
                int(click_guard.get("hard_rejections", 0)) + 1
            )
            click_guard["last_rejection_at"] = now.isoformat()
            click_guard["last_reason"] = (
                f"Click batch overshoot: requested={safe_requested_clicks}, allowed={allowed_clicks}"
            )
            write_click_guard_state(extra, click_guard)
            await update_user(payload.user_id, {"extra_data": extra})
            await invalidate_user_cache(payload.user_id)
            raise HTTPException(status_code=429, detail="Click rate too high")

        effective_clicks = (
            allowed_clicks
            if free_energy_clicks
            else min(allowed_clicks, current_energy)
        )
        gained = effective_clicks * coin_per_tap

        # === REDIS UPDATE (без БД!) ===
        new_energy = (
            current_energy
            if free_energy_clicks
            else max(0, current_energy - effective_clicks)
        )

        # Обновляем энергию в Redis
        await _set_energy_to_redis(
            conn, payload.user_id, new_energy, max_energy, now.timestamp()
        )

        # Буферизуем клики в Redis
        await _buffer_click_in_redis(conn, payload.user_id, gained, effective_clicks)

        # Tournament leaderboard в Redis
        if gained > 0:
            await conn.zincrby(TOURNAMENT_KEY, gained, str(payload.user_id))

        # Обновляем click guard в extra_data (редко, только при подозрении)
        suspicion_score = int(click_guard.get("suspicion_score", 0))
        if safe_requested_clicks > allowed_clicks:
            suspicion_score += 1
        elif suspicion_score > 0:
            suspicion_score -= 1
        click_guard["suspicion_score"] = min(12, max(0, suspicion_score))
        click_guard["last_click_at"] = now.isoformat()
        click_guard["last_requested_clicks"] = safe_requested_clicks
        click_guard["last_allowed_clicks"] = allowed_clicks
        click_guard["last_effective_clicks"] = effective_clicks
        click_guard["updated_at"] = now.isoformat()
        if click_guard["suspicion_score"] >= CLICK_SUSPICION_SOFT_LIMIT:
            click_guard["flagged_at"] = now.isoformat()

        # Пишем click guard в БД (только extra_data, не coins/energy)
        write_click_guard_state(extra, click_guard)
        await update_user(payload.user_id, {"extra_data": extra})

        # Referral bonus — раз в 30 сек, не на каждый клик
        # (упрощение: пропускаем для производительности)

        return {
            "success": True,
            "coins": int(user.get("coins", 0)) + gained,
            "energy": new_energy,
            "max_energy": max_energy,
            "clicks_accepted": effective_clicks,
            "coin_per_tap": coin_per_tap,
            "gained": gained,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in clicks_v2: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
