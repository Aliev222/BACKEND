import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from fastapi import HTTPException


def _is_trace_enabled() -> bool:
    return (os.getenv("CLICK_TRACE_TIMING", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


CLICK_TRACE_TIMING = _is_trace_enabled()


def _ms_since(start_ts: float) -> float:
    return round((time.perf_counter() - start_ts) * 1000, 2)


ATOMIC_CLICK_MUTATION_LUA = """
local energy_key = KEYS[1]
local hot_key = KEYS[2]
local pending_key = KEYS[3]

local now_ts = tonumber(ARGV[1])
local regen_seconds = tonumber(ARGV[2])
local requested_clicks = tonumber(ARGV[3])
local max_click_batch_size = tonumber(ARGV[4])
local max_real_cps = tonumber(ARGV[5])
local click_burst_allowance = tonumber(ARGV[6])
local click_time_cap = tonumber(ARGV[7])
local initial_click_allowance = tonumber(ARGV[8])
local free_energy_clicks = tonumber(ARGV[9])
local coin_per_tap = tonumber(ARGV[10])
local baseline_click_ts = tonumber(ARGV[11])
local max_energy = tonumber(ARGV[12])
local init_energy = tonumber(ARGV[13])
local suspicious_overshoot = tonumber(ARGV[14])

if redis.call('EXISTS', energy_key) == 0 then
    redis.call('HSET', energy_key,
        'value', tostring(init_energy),
        'updated_at', tostring(now_ts),
        'max_energy', tostring(max_energy),
        'click_updated_at', tostring(baseline_click_ts)
    )
end

local stored_value = tonumber(redis.call('HGET', energy_key, 'value') or '0')
local stored_updated = tonumber(redis.call('HGET', energy_key, 'updated_at') or tostring(now_ts))
local stored_max = tonumber(redis.call('HGET', energy_key, 'max_energy') or tostring(max_energy))
local click_updated = tonumber(redis.call('HGET', energy_key, 'click_updated_at') or tostring(baseline_click_ts))

if stored_max ~= max_energy then
    stored_max = max_energy
    if stored_value > stored_max then
        stored_value = stored_max
    end
end

local elapsed = now_ts - stored_updated
if elapsed < 0 then
    elapsed = 0
end
local regen = math.floor(elapsed / regen_seconds)
local current_energy = stored_value
if regen > 0 then
    current_energy = math.min(stored_max, stored_value + regen)
end

local allowed_clicks = 0
if click_updated and click_updated > 0 then
    local elapsed_click = now_ts - click_updated
    if elapsed_click < 0 then
        elapsed_click = 0
    end
    elapsed_click = math.min(elapsed_click, click_time_cap)
    local allowed_by_time = math.floor(elapsed_click * max_real_cps) + click_burst_allowance
    allowed_clicks = math.max(1, math.min(allowed_by_time, max_click_batch_size))
    allowed_clicks = math.min(requested_clicks, allowed_clicks)
else
    allowed_clicks = math.min(requested_clicks, initial_click_allowance, max_click_batch_size)
end

if requested_clicks > (allowed_clicks + suspicious_overshoot)
   and requested_clicks > math.max(allowed_clicks * 2, click_burst_allowance * 2) then
    return {-2, -1, current_energy, 0, 0, allowed_clicks}
end

local effective_clicks = allowed_clicks
if free_energy_clicks ~= 1 then
    effective_clicks = math.min(allowed_clicks, current_energy)
end

local gained = effective_clicks * coin_per_tap
local new_energy = current_energy
if free_energy_clicks ~= 1 then
    new_energy = math.max(0, current_energy - effective_clicks)
end

if redis.call('EXISTS', hot_key) == 0 then
    return {-1, -1, current_energy, effective_clicks, gained, allowed_clicks}
end

local new_coins = redis.call('INCRBY', hot_key, gained)
redis.call('INCRBY', pending_key, gained)
redis.call('HSET', energy_key,
    'value', tostring(new_energy),
    'updated_at', tostring(now_ts),
    'max_energy', tostring(max_energy),
    'click_updated_at', tostring(now_ts)
)

return {0, new_coins, new_energy, effective_clicks, gained, allowed_clicks}
"""


@dataclass(frozen=True)
class ClicksServiceDeps:
    require_telegram_user: Callable[..., Awaitable[Any]]
    require_dual_rate_limit: Callable[..., Awaitable[Any]]
    get_user_cached: Callable[[int], Awaitable[dict | None]]
    acquire_idempotency_key: Callable[[str, int], Awaitable[bool]]
    get_request_ip: Callable[[Any], str]
    get_redis_or_none: Callable[[], Awaitable[Any]]
    parse_extra_data: Callable[[Any], dict]
    get_click_guard_state: Callable[[dict], dict]
    parse_iso_datetime: Callable[[Any], Any]
    normalize_dt: Callable[[Any], Any]
    calculate_current_energy: Callable[[dict, datetime], int]
    resolve_max_energy: Callable[[dict], int]
    get_tap_value: Callable[[int], int]
    normalize_owned_skins: Callable[[Any], list[str]]
    normalize_selected_skin: Callable[[str | None, list[str]], str]
    is_mega_boost_active: Callable[[dict], bool]
    get_ghost_boost_status: Callable[[dict], tuple[bool, Any]]
    get_active_video_task_boost: Callable[[dict, str], tuple[bool, Any, int]]
    is_daily_infinite_energy_active: Callable[[dict], tuple[bool, Any]]
    ensure_coins_hot_initialized: Callable[[int, int, Any], Awaitable[None]]
    update_user: Callable[[int, dict], Awaitable[Any]]
    write_click_guard_state: Callable[[dict, dict], None]
    get_all_boost_states: Callable[[dict], dict]
    get_hour_value: Callable[[int], int]
    build_click_response_state: Callable[..., Awaitable[dict]]
    get_max_energy: Callable[[int], int]
    logger: Any
    MAX_CLICK_BATCH_SIZE: int
    ENERGY_REGEN_SECONDS: int
    MAX_REAL_CLICKS_PER_SECOND: int
    CLICK_BURST_ALLOWANCE: int
    CLICK_TIME_ACCUMULATION_CAP_SECONDS: int
    INITIAL_CLICK_BATCH_ALLOWANCE: int
    CLICK_SUSPICIOUS_OVERSHOOT: int
    CLICK_SUSPICION_SOFT_LIMIT: int
    TOURNAMENT_KEY: str
    GHOST_BOOST_MULTIPLIER: int
    SKIN_MULTIPLIERS: dict
    DEFAULT_SKIN_ID: str
    ENABLE_K6_FRAUD_HEURISTICS: bool


async def process_clicks_batch_service(payload: Any, request: Any, deps: ClicksServiceDeps):
    request_started_at = time.perf_counter()
    timings: dict[str, float] = {}

    def mark(name: str, started_at: float) -> None:
        timings[name] = _ms_since(started_at)

    def flush_trace(status: str) -> None:
        if not CLICK_TRACE_TIMING:
            return
        deps.logger.info(
            "CLICK_TIMING user=%s status=%s timings_ms=%s total_ms=%.2f",
            payload.user_id,
            status,
            timings,
            _ms_since(request_started_at),
        )

    try:
        t = time.perf_counter()
        await deps.require_telegram_user(request, payload.user_id)
        mark("require_telegram_user", t)

        t = time.perf_counter()
        await deps.require_dual_rate_limit(
            "clicks", request, payload.user_id, 90, 60, ip_limit=180
        )
        mark("require_dual_rate_limit", t)
        timings["require_user_action_lock"] = 0.0  # Click path currently does not use this lock.

        t = time.perf_counter()
        user = await deps.get_user_cached(payload.user_id)
        mark("user_profile_hot_state_load", t)

        if payload.clicks > deps.MAX_CLICK_BATCH_SIZE:
            flush_trace("too_many_clicks")
            raise HTTPException(status_code=400, detail="Too many clicks in batch")

        batch_key = f"idem:clicks:{payload.user_id}:{payload.batch_id}"
        t = time.perf_counter()
        is_new_batch = await deps.acquire_idempotency_key(batch_key, ttl=86400)
        mark("idempotency_check", t)
        if not is_new_batch:
            deps.logger.warning(
                "FRAUD_SUSPECT duplicate_batch user=%s batch_id=%s ip=%s",
                payload.user_id,
                payload.batch_id,
                deps.get_request_ip(request),
            )
            flush_trace("duplicate_batch")
            raise HTTPException(status_code=409, detail="Duplicate batch")

        t = time.perf_counter()
        if deps.ENABLE_K6_FRAUD_HEURISTICS:
            batch_parts = str(payload.batch_id).split("-")
            if len(batch_parts) >= 2:
                try:
                    batch_vu = int(batch_parts[0])
                    batch_iter = int(batch_parts[1])
                    if batch_iter > 500 and batch_vu <= 5:
                        deps.logger.warning(
                            "FRAUD_SUSPECT high_iter_same_vu user=%s batch_id=%s vu=%s iter=%s",
                            payload.user_id,
                            payload.batch_id,
                            batch_vu,
                            batch_iter,
                        )
                except (ValueError, IndexError):
                    pass
        mark("anti_fraud_precheck", t)

        if not user:
            flush_trace("user_not_found")
            raise HTTPException(status_code=404, detail="User not found")

        t = time.perf_counter()
        now = datetime.utcnow()
        max_energy = deps.resolve_max_energy(user)

        redis_conn = await deps.get_redis_or_none()
        mark("precompute_and_redis_load", t)
        if not redis_conn:
            flush_trace("redis_unavailable")
            raise HTTPException(
                status_code=503,
                detail="Redis unavailable: click processing temporarily disabled",
            )
        energy_key = f"energy:v2:{payload.user_id}"

        multitap_level = int(user.get("multitap_level", 0))
        tap_value = deps.get_tap_value(multitap_level)

        extra = deps.parse_extra_data(user.get("extra_data"))
        click_guard = deps.get_click_guard_state(extra)
        last_click_at = deps.parse_iso_datetime(click_guard.get("last_click_at"))

        owned_skins = deps.normalize_owned_skins(
            extra.get("owned_skins", [deps.DEFAULT_SKIN_ID])
        )
        selected_skin = deps.normalize_selected_skin(
            extra.get("selected_skin", deps.DEFAULT_SKIN_ID), owned_skins
        )
        skin_multiplier = float(deps.SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = deps.is_mega_boost_active(user)
        ghost_boost_active, _ghost_boost_expires_at = deps.get_ghost_boost_status(user)
        task_tap_boost_active, _, task_tap_boost_multiplier = (
            deps.get_active_video_task_boost(extra, "tap_boost")
        )
        daily_infinite_energy_active, _ = deps.is_daily_infinite_energy_active(user)
        free_energy_clicks = (
            mega_boost_active or daily_infinite_energy_active or ghost_boost_active
        )

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2
        if ghost_boost_active:
            coin_per_tap *= deps.GHOST_BOOST_MULTIPLIER
        if task_tap_boost_active:
            coin_per_tap *= max(1, task_tap_boost_multiplier)

        safe_requested_clicks = min(payload.clicks, deps.MAX_CLICK_BATCH_SIZE)
        coins_hot_key = f"coins_hot:{payload.user_id}"
        coins_pending_key = f"coins_pending:{payload.user_id}"
        baseline_click_dt = last_click_at or deps.normalize_dt(user.get("last_energy_update"))
        baseline_click_ts = baseline_click_dt.timestamp() if baseline_click_dt else 0.0
        init_energy = deps.calculate_current_energy(user, now)

        t = time.perf_counter()
        atomic_result = await redis_conn.eval(
            ATOMIC_CLICK_MUTATION_LUA,
            3,
            energy_key,
            coins_hot_key,
            coins_pending_key,
            str(now.timestamp()),
            str(deps.ENERGY_REGEN_SECONDS),
            str(safe_requested_clicks),
            str(deps.MAX_CLICK_BATCH_SIZE),
            str(deps.MAX_REAL_CLICKS_PER_SECOND),
            str(deps.CLICK_BURST_ALLOWANCE),
            str(deps.CLICK_TIME_ACCUMULATION_CAP_SECONDS),
            str(deps.INITIAL_CLICK_BATCH_ALLOWANCE),
            "1" if free_energy_clicks else "0",
            str(coin_per_tap),
            str(baseline_click_ts),
            str(max_energy),
            str(init_energy),
            str(deps.CLICK_SUSPICIOUS_OVERSHOOT),
        )
        mark("atomic_redis_click_mutation", t)

        status = int(atomic_result[0])
        if status == -1:
            t = time.perf_counter()
            await deps.ensure_coins_hot_initialized(
                payload.user_id, int(user.get("coins", 0)), redis_conn
            )
            atomic_result = await redis_conn.eval(
                ATOMIC_CLICK_MUTATION_LUA,
                3,
                energy_key,
                coins_hot_key,
                coins_pending_key,
                str(now.timestamp()),
                str(deps.ENERGY_REGEN_SECONDS),
                str(safe_requested_clicks),
                str(deps.MAX_CLICK_BATCH_SIZE),
                str(deps.MAX_REAL_CLICKS_PER_SECOND),
                str(deps.CLICK_BURST_ALLOWANCE),
                str(deps.CLICK_TIME_ACCUMULATION_CAP_SECONDS),
                str(deps.INITIAL_CLICK_BATCH_ALLOWANCE),
                "1" if free_energy_clicks else "0",
                str(coin_per_tap),
                str(baseline_click_ts),
                str(max_energy),
                str(init_energy),
                str(deps.CLICK_SUSPICIOUS_OVERSHOOT),
            )
            mark("atomic_redis_click_mutation_reinit", t)
            status = int(atomic_result[0])

        if status == -2:
            allowed_clicks = int(atomic_result[5])
            click_guard["hard_rejections"] = (
                int(click_guard.get("hard_rejections", 0)) + 1
            )
            click_guard["last_rejection_at"] = now.isoformat()
            click_guard["last_reason"] = (
                f"Click batch overshoot: requested={safe_requested_clicks}, allowed={allowed_clicks}"
            )
            deps.write_click_guard_state(extra, click_guard)
            t = time.perf_counter()
            await deps.update_user(payload.user_id, {"extra_data": extra})
            mark("anti_fraud_db_update", t)
            deps.logger.warning(
                "Rejected suspicious click batch user=%s ip=%s requested=%s allowed=%s",
                payload.user_id,
                deps.get_request_ip(request),
                safe_requested_clicks,
                allowed_clicks,
            )
            flush_trace("rate_limited_overshoot")
            raise HTTPException(status_code=429, detail="Click rate too high")

        if status != 0:
            deps.logger.error(
                "Atomic click mutation failed user=%s status=%s result=%s",
                payload.user_id,
                status,
                atomic_result,
            )
            flush_trace("atomic_mutation_failed")
            raise HTTPException(status_code=500, detail="Atomic click mutation failed")

        new_coins = int(atomic_result[1])
        new_energy = int(atomic_result[2])
        effective_clicks = int(atomic_result[3])
        gained = int(atomic_result[4])
        allowed_clicks = int(atomic_result[5])

        suspicion_score = int(click_guard.get("suspicion_score", 0))
        if safe_requested_clicks > allowed_clicks:
            suspicion_score += 1
            click_guard["last_reason"] = (
                f"Requested {safe_requested_clicks} clicks while server allowed {allowed_clicks}"
            )
        elif suspicion_score > 0:
            suspicion_score -= 1
            click_guard.pop("last_reason", None)

        click_guard["suspicion_score"] = min(12, max(0, suspicion_score))
        click_guard["last_click_at"] = now.isoformat()
        click_guard["last_requested_clicks"] = safe_requested_clicks
        click_guard["last_allowed_clicks"] = allowed_clicks
        click_guard["last_effective_clicks"] = effective_clicks
        click_guard["updated_at"] = now.isoformat()
        if click_guard["suspicion_score"] >= deps.CLICK_SUSPICION_SOFT_LIMIT:
            click_guard["flagged_at"] = now.isoformat()

        t = time.perf_counter()
        deps.write_click_guard_state(extra, click_guard)
        mark("anti_fraud_guard_update", t)

        t = time.perf_counter()
        try:
            async with redis_conn.pipeline(transaction=False) as pipe:
                pipe.setex(f"activity:{payload.user_id}", 300, now.isoformat())
                pipe.set(
                    f"click_guard:{payload.user_id}",
                    json.dumps(click_guard),
                    ex=300,
                )
                await pipe.execute()
        except Exception as e:
            deps.logger.warning("Redis activity/click_guard write failed (non-critical): %s", e)
        mark("post_mutation_redis_activity_and_guard", t)

        t = time.perf_counter()
        if gained > 0:
            click_buf_key = f"click_buf:{payload.user_id}"
            async with redis_conn.pipeline(transaction=False) as pipe:
                pipe.zincrby(deps.TOURNAMENT_KEY, gained, str(payload.user_id))
                pipe.hincrby(click_buf_key, "coins", gained)
                pipe.hincrby(click_buf_key, "clicks", effective_clicks)
                pipe.expire(click_buf_key, 300)

                referral_bonus = 0
                referrer_id = user.get("referrer_id")
                if referrer_id:
                    referral_bonus = max(1, int(gained * 0.05))
                    referral_pending_key = f"referral_pending:{referrer_id}"
                    pipe.hincrby(referral_pending_key, "coins", referral_bonus)
                    pipe.hincrby(referral_pending_key, "clicks", 1)
                    pipe.expire(referral_pending_key, 300)

                await pipe.execute()
        else:
            referral_bonus = 0
        mark("post_mutation_redis_side_effects", t)

        t = time.perf_counter()
        boosts = deps.get_all_boost_states(deps.parse_extra_data(user.get("extra_data")))
        tap_value = deps.get_tap_value(int(user.get("multitap_level", 0)))
        profit_per_hour = deps.get_hour_value(int(user.get("profit_level", 0)))
        mark("response_assembly_prepare", t)

        t = time.perf_counter()
        response_payload = await deps.build_click_response_state(
            user_id=payload.user_id,
            coins_after=new_coins,
            energy_after=int(new_energy),
            max_energy=max_energy,
            gained=gained,
            effective_clicks=effective_clicks,
            coin_per_tap=coin_per_tap,
            tap_value=tap_value,
            profit_per_hour=profit_per_hour,
            boosts=boosts,
            suspicion_score=click_guard["suspicion_score"],
            referral_bonus=referral_bonus,
        )
        mark("response_assembly", t)
        flush_trace("ok")
        return response_payload
    except HTTPException:
        flush_trace("http_exception")
        raise
    except Exception as e:
        deps.logger.error(f"Error in process_clicks_batch: {e}")
        flush_trace("unexpected_exception")
        raise HTTPException(status_code=500, detail="Internal server error")


async def sync_energy_service(payload: Any, request: Any, deps: ClicksServiceDeps):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        user = await deps.get_user_cached(payload.user_id)

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        energy_level = int(user.get("energy_level", 0))
        max_energy = deps.get_max_energy(energy_level)

        redis_conn = await deps.get_redis_or_none()
        energy_key = f"energy:v2:{payload.user_id}"
        current_energy = max_energy
        energy_updated_at = now.timestamp()

        if redis_conn:
            cached = await redis_conn.hgetall(energy_key)
            if cached:
                cached_max = int(cached.get("max_energy", max_energy))
                cached_value = int(cached.get("value", 0))
                energy_updated_at = float(cached.get("updated_at", now.timestamp()))
                elapsed = now.timestamp() - energy_updated_at
                regen = int(elapsed // deps.ENERGY_REGEN_SECONDS)
                current_energy = min(cached_max, cached_value + regen)
                max_energy = cached_max
                deps.logger.debug(
                    "SYNC-ENERGY user=%s energy=%d max=%d source=energy:v2 updated_at=%.0f",
                    payload.user_id,
                    current_energy,
                    max_energy,
                    energy_updated_at,
                )
            else:
                db_energy = int(user.get("energy", 0))
                last_update = deps.normalize_dt(user.get("last_energy_update"))
                if last_update:
                    seconds_passed = max(0, int((now - last_update).total_seconds()))
                    gained = seconds_passed // deps.ENERGY_REGEN_SECONDS
                    db_energy = min(max_energy, db_energy + gained)
                    energy_updated_at = last_update.timestamp()
                else:
                    energy_updated_at = 0
                current_energy = min(db_energy, max_energy)
                deps.logger.info(
                    "SYNC-ENERGY user=%s energy=%d max=%d source=DB (energy:v2 missing) updated_at=%.0f",
                    payload.user_id,
                    current_energy,
                    max_energy,
                    energy_updated_at,
                )
        else:
            current_energy = deps.calculate_current_energy(user, now)
            energy_updated_at = now.timestamp()

        update_data = {}
        if int(user.get("max_energy", max_energy)) != max_energy:
            update_data["max_energy"] = max_energy
        if update_data:
            await deps.update_user(payload.user_id, update_data)

        state_updated_at = int(energy_updated_at * 1000)

        return {
            "success": True,
            "energy": current_energy,
            "max_energy": max_energy,
            "regen_seconds": deps.ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "state_updated_at": state_updated_at,
            "state_version": state_updated_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in sync_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
