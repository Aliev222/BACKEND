import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from fastapi import HTTPException
from observability.metrics import observe_storage_error, observe_storage_timing


@dataclass(frozen=True)
class AdsBoostsServiceDeps:
    ensure_redis_available: Callable[[], Awaitable[Any]]
    get_ad_action_active_session_key: Callable[[int], str]
    require_telegram_user: Callable[..., Awaitable[Any]]
    require_dual_rate_limit: Callable[..., Awaitable[Any]]
    require_user_action_lock: Callable[..., Awaitable[Any]]
    get_user_cached: Callable[[int], Awaitable[dict | None]]
    update_user: Callable[[int, dict], Awaitable[Any]]
    invalidate_user_cache: Callable[[int], Awaitable[Any]]
    record_rewarded_ad_claim: Callable[..., Awaitable[Any]]
    parse_extra_data: Callable[[Any], dict]
    parse_iso_datetime: Callable[[Any], datetime | None]
    get_redis_or_none: Callable[[], Awaitable[Any]]
    resolve_max_energy: Callable[[dict], int]
    format_duration: Callable[[int], str]
    logger: Any
    AD_ACTIONS_ALLOWED: set[str]
    AD_ACTION_SESSION_TTL_SECONDS: int
    AD_SESSION_MIN_WAIT_SECONDS: int
    MONETAG_POSTBACK_ENFORCED: bool
    ADSGRAM_REWARD_ENFORCED: bool
    MEGA_BOOST_MINUTES: int
    MEGA_BOOST_COOLDOWN_MAX_MINUTES: int
    GHOST_BOOST_MULTIPLIER: int
    GHOST_BOOST_MINUTES: int
    SKIN_AD_COOLDOWN_MINUTES: int
    VIDEO_SKIN_IDS: set[str]
    SKIN_REQUIREMENTS: dict
    LEGACY_SKIN_ID_MAP: dict
    ENERGY_REFILL_COOLDOWN_MINUTES: int
    ENERGY_REGEN_SECONDS: int


async def create_ad_action_session_service(
    user_id: int, action: str, deps: AdsBoostsServiceDeps
) -> str:
    if action not in deps.AD_ACTIONS_ALLOWED:
        raise HTTPException(status_code=400, detail="Unknown ad action")

    ad_session_id = (
        f"{action}:{user_id}:{int(time.time())}:{random.randint(100000, 999999)}"
    )
    session_key = f"adsession:action:{ad_session_id}"
    session_payload = json.dumps(
        {
            "user_id": user_id,
            "action": action,
            "claimed": False,
            "verified": False,
            "verified_at": None,
            "created_at": time.time(),
        }
    )

    async def _write_session_once(redis_conn):
        started_at = time.perf_counter()
        await redis_conn.setex(
            session_key,
            deps.AD_ACTION_SESSION_TTL_SECONDS,
            session_payload,
        )
        user_index_key = f"adsession:user:{user_id}"
        active_session_key = deps.get_ad_action_active_session_key(user_id)
        try:
            await redis_conn.zadd(user_index_key, {ad_session_id: time.time()})
            await redis_conn.expire(
                user_index_key, max(deps.AD_ACTION_SESSION_TTL_SECONDS, 600)
            )
            await redis_conn.setex(
                active_session_key, deps.AD_ACTION_SESSION_TTL_SECONDS, ad_session_id
            )
        except Exception:
            pass
        observe_storage_timing(
            "redis",
            "ad_action_session_create",
            "ads_boosts",
            time.perf_counter() - started_at,
        )

    redis_conn = await deps.ensure_redis_available()
    try:
        await _write_session_once(redis_conn)
    except Exception:
        observe_storage_error("redis", "ad_action_session_create", "ads_boosts")
        # Transient Redis connection can drop mid-operation; retry once.
        redis_conn = await deps.ensure_redis_available()
        try:
            await _write_session_once(redis_conn)
        except Exception:
            observe_storage_error("redis", "ad_action_session_create", "ads_boosts")
            raise HTTPException(
                status_code=503, detail="Ad session temporarily unavailable"
            )
    return ad_session_id


async def mark_ad_action_session_verified_service(
    ad_session_id: str, postback_payload: dict, deps: AdsBoostsServiceDeps
) -> bool:
    redis_conn = await deps.ensure_redis_available()
    session_key = f"adsession:action:{ad_session_id}"
    t = time.perf_counter()
    raw = await redis_conn.get(session_key)
    observe_storage_timing(
        "redis", "ad_session_get_for_verify", "ads_boosts", time.perf_counter() - t
    )
    if not raw:
        return False

    try:
        session = json.loads(raw)
    except Exception:
        return False

    session["verified"] = True
    session["verified_at"] = time.time()
    session["postback_payload"] = postback_payload

    t = time.perf_counter()
    ttl = await redis_conn.ttl(session_key)
    ttl = max(int(ttl or 0), 300)
    await redis_conn.setex(session_key, ttl, json.dumps(session))
    observe_storage_timing(
        "redis", "ad_session_verify_write", "ads_boosts", time.perf_counter() - t
    )
    return True


async def mark_ad_action_session_verified_for_user_service(
    user_id: int,
    ad_session_id: str,
    verification_payload: dict | None,
    deps: AdsBoostsServiceDeps,
    *,
    enforce_min_wait: bool = True,
) -> bool:
    redis_conn = await deps.ensure_redis_available()
    session_key = f"adsession:action:{ad_session_id}"
    t = time.perf_counter()
    raw = await redis_conn.get(session_key)
    observe_storage_timing(
        "redis", "ad_session_get_for_consume", "ads_boosts", time.perf_counter() - t
    )
    if not raw:
        return False

    try:
        session = json.loads(raw)
    except Exception:
        return False

    if int(session.get("user_id", 0)) != int(user_id):
        return False

    if session.get("claimed") is True:
        return False

    if enforce_min_wait:
        created_at = float(session.get("created_at") or 0)
        if (
            created_at <= 0
            or (time.time() - created_at) < deps.AD_SESSION_MIN_WAIT_SECONDS
        ):
            return False

    return await mark_ad_action_session_verified_service(
        ad_session_id, verification_payload or {}, deps
    )


async def find_latest_ad_action_session_for_user_service(
    user_id: int, deps: AdsBoostsServiceDeps
) -> str | None:
    redis_conn = await deps.ensure_redis_available()
    user_index_key = f"adsession:user:{user_id}"
    active_session_key = deps.get_ad_action_active_session_key(user_id)
    active_session_id = await redis_conn.get(active_session_key)
    if active_session_id:
        session_key = f"adsession:action:{active_session_id}"
        raw = await redis_conn.get(session_key)
        if raw:
            try:
                session = json.loads(raw)
                if (
                    int(session.get("user_id", 0)) == int(user_id)
                    and session.get("claimed") is not True
                ):
                    return active_session_id
            except Exception:
                pass
        try:
            await redis_conn.delete(active_session_key)
        except Exception:
            pass
    session_ids = await redis_conn.zrevrange(user_index_key, 0, 24)
    stale_ids: list[str] = []

    for session_id in session_ids:
        session_key = f"adsession:action:{session_id}"
        raw = await redis_conn.get(session_key)
        if not raw:
            stale_ids.append(session_id)
            continue

        try:
            session = json.loads(raw)
        except Exception:
            stale_ids.append(session_id)
            continue

        if int(session.get("user_id", 0)) != int(user_id):
            stale_ids.append(session_id)
            continue

        if session.get("claimed") is True:
            continue

        return session_id

    if stale_ids:
        try:
            await redis_conn.zrem(user_index_key, *stale_ids)
        except Exception:
            pass
    return None


async def mark_latest_ad_action_session_verified_for_user_service(
    user_id: int, postback_payload: dict, deps: AdsBoostsServiceDeps
) -> str | None:
    ad_session_id = await find_latest_ad_action_session_for_user_service(user_id, deps)
    if not ad_session_id:
        return None
    verified = await mark_ad_action_session_verified_service(
        ad_session_id, postback_payload, deps
    )
    if not verified:
        return None
    return ad_session_id


async def consume_ad_action_session_service(
    user_id: int, ad_session_id: str, expected_action: str, deps: AdsBoostsServiceDeps
) -> dict:
    redis_conn = await deps.ensure_redis_available()
    session_key = f"adsession:action:{ad_session_id}"
    active_session_key = deps.get_ad_action_active_session_key(user_id)
    raw = await redis_conn.get(session_key)
    if not raw:
        raise HTTPException(status_code=400, detail="Invalid or expired ad session")

    try:
        session = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ad session payload")

    if int(session.get("user_id", 0)) != int(user_id):
        raise HTTPException(
            status_code=400, detail="Ad session does not belong to user"
        )

    if session.get("action") != expected_action:
        raise HTTPException(status_code=400, detail="Ad session action mismatch")

    if session.get("claimed") is True:
        raise HTTPException(status_code=409, detail="Reward already claimed")

    if deps.MONETAG_POSTBACK_ENFORCED or deps.ADSGRAM_REWARD_ENFORCED:
        if session.get("verified") is not True:
            raise HTTPException(
                status_code=400, detail="Ad completion was not confirmed yet"
            )
    else:
        if session.get("verified") is not True:
            created_at = float(session.get("created_at") or 0)
            if (
                created_at <= 0
                or (time.time() - created_at) < deps.AD_SESSION_MIN_WAIT_SECONDS
            ):
                raise HTTPException(
                    status_code=400, detail="Ad watch is not completed yet"
                )

    session["claimed"] = True
    t = time.perf_counter()
    await redis_conn.setex(session_key, 60, json.dumps(session))
    observe_storage_timing(
        "redis", "ad_session_consume_write", "ads_boosts", time.perf_counter() - t
    )
    try:
        t = time.perf_counter()
        active_session_id = await redis_conn.get(active_session_key)
        if active_session_id == ad_session_id:
            await redis_conn.delete(active_session_key)
        observe_storage_timing(
            "redis", "ad_session_active_cleanup", "ads_boosts", time.perf_counter() - t
        )
    except Exception:
        observe_storage_error("redis", "ad_session_active_cleanup", "ads_boosts")
        pass
    return session


async def ad_action_start_service(
    payload: Any, request: Any, deps: AdsBoostsServiceDeps
):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "ad_action_start", request, payload.user_id, 20, 60, ip_limit=40
        )

        t = time.perf_counter()
        user = await deps.get_user_cached(payload.user_id)
        observe_storage_timing(
            "db", "get_user_cached", "ads_boosts", time.perf_counter() - t
        )
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        ad_session_id = await create_ad_action_session_service(
            payload.user_id, payload.action, deps
        )
        return {
            "success": True,
            "ad_session_id": ad_session_id,
            "action": payload.action,
        }
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.exception("Error in ad_action_start")
        observe_storage_error("app", "ad_action_start", "ads_boosts")
        raise HTTPException(status_code=500, detail="Internal server error")


async def adsgram_complete_locally_service(
    payload: Any, request: Any, deps: AdsBoostsServiceDeps
):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "adsgram_complete", request, payload.user_id, 20, 60, ip_limit=40
        )
        await deps.require_user_action_lock("adsgram_complete", payload.user_id, ttl=2)

        verified = await mark_ad_action_session_verified_for_user_service(
            payload.user_id,
            payload.ad_session_id,
            {"source": "adsgram_sdk", "confirmed_at": datetime.utcnow().isoformat()},
            deps,
            enforce_min_wait=False,
        )
        if not verified:
            raise HTTPException(
                status_code=400, detail="Ad completion was not confirmed yet"
            )

        return {"success": True, "verified": True}
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in adsgram_complete_locally: {e}")
        observe_storage_error("app", "adsgram_complete", "ads_boosts")
        raise HTTPException(status_code=500, detail="Internal server error")


async def activate_mega_boost_service(
    payload: Any, request: Any, deps: AdsBoostsServiceDeps
):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "activate_mega_boost", request, payload.user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session_service(
            payload.user_id, payload.ad_session_id, "mega_boost", deps
        )
        t = time.perf_counter()
        user = await deps.get_user_cached(payload.user_id)
        observe_storage_timing(
            "db", "get_user_cached", "ads_boosts", time.perf_counter() - t
        )
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()

        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(
                    active_boosts["mega_boost"]["expires_at"]
                )
                if now < expires:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "success": False,
                        "message": f"Boost already active! {remaining // 60}:{remaining % 60:02d} remaining",
                        "already_active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"],
                    }
            except Exception:
                del active_boosts["mega_boost"]

        cooldown_until = deps.parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Mega boost cooldown {remaining // 60}:{remaining % 60:02d}",
            )
        if cooldown_until and now >= cooldown_until:
            extra.pop("mega_boost_cooldown_until", None)

        expires_at = (now + timedelta(minutes=deps.MEGA_BOOST_MINUTES)).isoformat()
        cooldown_minutes = deps.MEGA_BOOST_COOLDOWN_MAX_MINUTES
        cooldown_until_value = (now + timedelta(minutes=cooldown_minutes)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["mega_boost_cooldown_until"] = cooldown_until_value
        extra["active_boosts"] = active_boosts
        t = time.perf_counter()
        await deps.update_user(payload.user_id, {"extra_data": extra})
        observe_storage_timing(
            "db", "update_user", "ads_boosts", time.perf_counter() - t
        )
        await deps.invalidate_user_cache(payload.user_id)
        await deps.record_rewarded_ad_claim(
            payload.user_id, "boost", {"source_action": "mega_boost"}
        )

        return {
            "success": True,
            "message": "рџ”ҐвљЎ MEGA BOOST activated for 1 minute! x2 coins + infinite energy",
            "expires_at": expires_at,
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": cooldown_minutes,
        }
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in activate_mega_boost: {e}")
        observe_storage_error("app", "activate_mega_boost", "ads_boosts")
        raise HTTPException(status_code=500, detail="Internal server error")


async def activate_ghost_boost_service(
    payload: Any, request: Any, deps: AdsBoostsServiceDeps
):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "activate_ghost_boost", request, payload.user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session_service(
            payload.user_id, payload.ad_session_id, "ghost_boost", deps
        )
        t = time.perf_counter()
        user = await deps.get_user_cached(payload.user_id)
        observe_storage_timing(
            "db", "get_user_cached", "ads_boosts", time.perf_counter() - t
        )
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = deps.parse_extra_data(user.get("extra_data"))

        active_boosts = extra.get("active_boosts", {})
        if not isinstance(active_boosts, dict):
            active_boosts = {}

        now = datetime.utcnow()
        ghost_boost = active_boosts.get("ghost_boost")
        if ghost_boost and ghost_boost.get("expires_at"):
            try:
                expires = datetime.fromisoformat(ghost_boost["expires_at"])
                if now < expires:
                    remaining = max(0, int((expires - now).total_seconds()))
                    return {
                        "success": False,
                        "already_active": True,
                        "expires_at": ghost_boost["expires_at"],
                        "remaining_seconds": remaining,
                        "multiplier": deps.GHOST_BOOST_MULTIPLIER,
                    }
            except Exception:
                pass

        expires_at = (now + timedelta(minutes=deps.GHOST_BOOST_MINUTES)).isoformat()
        active_boosts["ghost_boost"] = {
            "active": True,
            "expires_at": expires_at,
            "multiplier": deps.GHOST_BOOST_MULTIPLIER,
        }
        extra["active_boosts"] = active_boosts

        t = time.perf_counter()
        await deps.update_user(payload.user_id, {"extra_data": extra})
        observe_storage_timing(
            "db", "update_user", "ads_boosts", time.perf_counter() - t
        )
        await deps.invalidate_user_cache(payload.user_id)
        await deps.record_rewarded_ad_claim(
            payload.user_id, "ghost", {"source_action": "ghost_boost"}
        )

        return {
            "success": True,
            "expires_at": expires_at,
            "remaining_seconds": deps.GHOST_BOOST_MINUTES * 60,
            "multiplier": deps.GHOST_BOOST_MULTIPLIER,
            "message": "Ghost boost activated",
        }
    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in activate_ghost_boost: {e}")
        observe_storage_error("app", "activate_ghost_boost", "ads_boosts")
        raise HTTPException(status_code=500, detail="Internal server error")


async def increment_ads_watched_service(
    payload: Any,
    request: Any,
    deps: AdsBoostsServiceDeps,
    acquire_once_lock: Callable[..., Awaitable[bool]],
):
    try:
        await deps.require_telegram_user(request, payload.user_id)
        await deps.require_dual_rate_limit(
            "ads_increment", request, payload.user_id, 20, 60, ip_limit=40
        )
        await consume_ad_action_session_service(
            payload.user_id, payload.ad_session_id, "ads_increment", deps
        )
        t = time.perf_counter()
        user = await deps.get_user_cached(payload.user_id)
        observe_storage_timing(
            "db", "get_user_cached", "ads_boosts", time.perf_counter() - t
        )
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        lock_key = f"lock:ads_increment:{payload.user_id}"
        locked = await acquire_once_lock(lock_key, ttl=5)
        if not locked:
            raise HTTPException(status_code=429, detail="Ad already being processed")

        # Import update_extra_data_atomic
        from DATABASE.base import update_extra_data_atomic

        # Increment ads_watched atomically (lossy mode - counter is non-critical)
        t = time.perf_counter()
        ads_watched = await update_extra_data_atomic(
            payload.user_id, "ads_watched", "increment", 1, allow_lossy_fallback=True
        )
        observe_storage_timing(
            "db", "update_extra_data_atomic", "ads_boosts", time.perf_counter() - t
        )

        if ads_watched is None:
            ads_watched = 1  # Fallback if user not found

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        skin_id = (
            deps.LEGACY_SKIN_ID_MAP.get(payload.skin_id, payload.skin_id)
            if payload.skin_id
            else None
        )
        current_count = 0
        required_count = 0
        cooldown_remaining_seconds = 0
        ready_to_unlock = False

        if skin_id:
            if skin_id not in deps.VIDEO_SKIN_IDS:
                raise HTTPException(status_code=400, detail="Unknown ad skin")

            progress = get_skin_ad_progress(extra)
            last_watch = get_skin_ad_last_watch(extra)
            required_count = int(
                deps.SKIN_REQUIREMENTS.get(skin_id, {}).get("count", 1)
            )
            current_count = int(progress.get(skin_id, 0) or 0)

            if current_count >= required_count:
                ready_to_unlock = True
            else:
                last_watch_at = deps.parse_iso_datetime(last_watch.get(skin_id))
                now = datetime.utcnow()
                if last_watch_at:
                    next_allowed = last_watch_at + timedelta(
                        minutes=deps.SKIN_AD_COOLDOWN_MINUTES
                    )
                    if next_allowed > now:
                        cooldown_remaining_seconds = int(
                            (next_allowed - now).total_seconds()
                        )
                        raise HTTPException(
                            status_code=429,
                            detail=f"Skin ad cooldown {cooldown_remaining_seconds // 60}:{cooldown_remaining_seconds % 60:02d}",
                        )

                current_count = min(required_count, current_count + 1)
                progress[skin_id] = current_count
                last_watch[skin_id] = now.isoformat()
                extra["skin_ad_progress"] = progress
                extra["skin_ad_last_watch"] = last_watch
                ready_to_unlock = current_count >= required_count

        t = time.perf_counter()
        await deps.update_user(payload.user_id, {"extra_data": extra})
        observe_storage_timing(
            "db", "update_user", "ads_boosts", time.perf_counter() - t
        )
        await deps.record_rewarded_ad_claim(
            payload.user_id,
            "skins",
            {"source_action": "ads_increment", "skin_id": skin_id},
        )

        return {
            "success": True,
            "ads_watched": ads_watched,
            "skin_id": skin_id,
            "current_count": current_count,
            "required_count": required_count,
            "ready_to_unlock": ready_to_unlock,
            "cooldown_minutes": deps.SKIN_AD_COOLDOWN_MINUTES,
        }

    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in increment_ads_watched: {e}")
        observe_storage_error("app", "increment_ads_watched", "ads_boosts")
        raise HTTPException(status_code=500, detail="Internal server error")


async def update_energy_service(payload: Any, request: Any, deps: AdsBoostsServiceDeps):
    try:
        user_id = payload.user_id
        await deps.require_telegram_user(request, user_id)
        await deps.require_dual_rate_limit(
            "update_energy", request, user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session_service(
            user_id, payload.ad_session_id, "energy_refill_max", deps
        )
        await deps.require_user_action_lock("update_energy", user_id, ttl=3)
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id required")
        t = time.perf_counter()
        user = await deps.get_user_cached(user_id)
        observe_storage_timing(
            "db", "get_user_cached", "ads_boosts", time.perf_counter() - t
        )
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        max_energy = deps.resolve_max_energy(user)
        extra = deps.parse_extra_data(user.get("extra_data"))
        cooldown_until = deps.parse_iso_datetime(
            extra.get("energy_refill_cooldown_until")
        )
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Energy refill cooldown active. Try again in {deps.format_duration(remaining)}",
            )
        if cooldown_until and now >= cooldown_until:
            extra.pop("energy_refill_cooldown_until", None)

        cooldown_until_value = (
            now + timedelta(minutes=deps.ENERGY_REFILL_COOLDOWN_MINUTES)
        ).isoformat()
        extra["energy_refill_cooldown_until"] = cooldown_until_value

        t = time.perf_counter()
        await deps.update_user(
            user_id,
            {
                "max_energy": max_energy,
                "energy": max_energy,
                "last_energy_update": now,
                "extra_data": extra,
            },
        )
        observe_storage_timing(
            "db", "update_user", "ads_boosts", time.perf_counter() - t
        )
        try:
            redis_conn = await deps.get_redis_or_none()
            if redis_conn:
                t = time.perf_counter()
                await redis_conn.hset(
                    f"energy:v2:{user_id}",
                    mapping={
                        "value": str(max_energy),
                        "updated_at": str(now.timestamp()),
                        "max_energy": str(max_energy),
                    },
                )
                observe_storage_timing(
                    "redis", "energy_v2_hset", "ads_boosts", time.perf_counter() - t
                )
        except Exception:
            observe_storage_error("redis", "energy_v2_hset", "ads_boosts")
            pass

        await deps.record_rewarded_ad_claim(
            user_id, "energy_restore", {"source_action": "energy_refill_max"}
        )

        return {
            "success": True,
            "energy": max_energy,
            "max_energy": max_energy,
            "regen_seconds": deps.ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": deps.ENERGY_REFILL_COOLDOWN_MINUTES,
        }

    except HTTPException:
        raise
    except Exception as e:
        deps.logger.error(f"Error in update_energy: {e}")
        observe_storage_error("app", "update_energy", "ads_boosts")
        raise HTTPException(status_code=500, detail="Internal server error")


def get_skin_ad_progress(extra: dict) -> dict:
    progress = extra.get("skin_ad_progress", {})
    if isinstance(progress, dict):
        return progress
    return {}


def get_skin_ad_last_watch(extra: dict) -> dict:
    last_watch = extra.get("skin_ad_last_watch", {})
    if isinstance(last_watch, dict):
        return last_watch
    return {}
