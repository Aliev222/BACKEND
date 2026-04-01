import time
import json
import logging
import redis.asyncio as redis

logger = logging.getLogger(__name__)

AD_ACTION_SESSION_TTL_SECONDS = 180
AD_SESSION_MIN_WAIT_SECONDS = 8


def get_ad_action_active_session_key(user_id: int) -> str:
    return f"adsession:user:active:{int(user_id)}"


async def create_ad_session(redis_conn: redis.Redis, user_id: int, action: str) -> str:
    allowed = {
        "energy_refill_max",
        "mega_boost",
        "ghost_boost",
        "ads_increment",
        "video_task",
        "autoclicker",
    }
    if action not in allowed:
        raise ValueError(f"Unknown ad action: {action}")

    ad_session_id = (
        f"{action}:{user_id}:{int(time.time())}:{time.monotonic_ns() % 1000000}"
    )
    session_key = f"adsession:action:{ad_session_id}"

    await redis_conn.setex(
        session_key,
        AD_ACTION_SESSION_TTL_SECONDS,
        json.dumps(
            {
                "user_id": user_id,
                "action": action,
                "claimed": False,
                "verified": False,
                "verified_at": None,
                "created_at": time.time(),
            }
        ),
    )

    user_index_key = f"adsession:user:{user_id}"
    active_session_key = get_ad_action_active_session_key(user_id)
    try:
        await redis_conn.zadd(user_index_key, {ad_session_id: time.time()})
        await redis_conn.expire(user_index_key, max(AD_ACTION_SESSION_TTL_SECONDS, 600))
        await redis_conn.setex(
            active_session_key, AD_ACTION_SESSION_TTL_SECONDS, ad_session_id
        )
    except Exception:
        pass

    return ad_session_id


async def mark_session_verified(
    redis_conn: redis.Redis, ad_session_id: str, payload: dict
) -> bool:
    session_key = f"adsession:action:{ad_session_id}"
    raw = await redis_conn.get(session_key)
    if not raw:
        return False

    try:
        session = json.loads(raw)
    except Exception:
        return False

    session["verified"] = True
    session["verified_at"] = time.time()
    session["postback_payload"] = payload

    ttl = await redis_conn.ttl(session_key)
    ttl = max(int(ttl or 0), 300)
    await redis_conn.setex(session_key, ttl, json.dumps(session))
    return True


async def consume_ad_session(
    redis_conn: redis.Redis,
    user_id: int,
    ad_session_id: str,
    expected_action: str,
    enforce_verification: bool = False,
) -> dict:
    session_key = f"adsession:action:{ad_session_id}"
    raw = await redis_conn.get(session_key)
    if not raw:
        raise ValueError("Invalid or expired ad session")

    try:
        session = json.loads(raw)
    except Exception:
        raise ValueError("Invalid ad session payload")

    if int(session.get("user_id", 0)) != int(user_id):
        raise ValueError("Ad session does not belong to user")

    if session.get("action") != expected_action:
        raise ValueError("Ad session action mismatch")

    if session.get("claimed") is True:
        raise ValueError("Reward already claimed")

    if enforce_verification:
        if session.get("verified") is not True:
            raise ValueError("Ad completion was not confirmed yet")
    else:
        created_at = float(session.get("created_at") or 0)
        if created_at <= 0 or (time.time() - created_at) < AD_SESSION_MIN_WAIT_SECONDS:
            raise ValueError("Ad watch is not completed yet")

    session["claimed"] = True
    await redis_conn.setex(session_key, 60, json.dumps(session))

    active_session_key = get_ad_action_active_session_key(user_id)
    try:
        active_session_id = await redis_conn.get(active_session_key)
        if active_session_id == ad_session_id:
            await redis_conn.delete(active_session_key)
    except Exception:
        pass

    return session


async def find_latest_ad_session_for_user(
    redis_conn: redis.Redis, user_id: int
) -> str | None:
    user_index_key = f"adsession:user:{user_id}"
    active_session_key = get_ad_action_active_session_key(user_id)

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
    for session_id in session_ids:
        session_key = f"adsession:action:{session_id}"
        raw = await redis_conn.get(session_key)
        if not raw:
            continue

        try:
            session = json.loads(raw)
        except Exception:
            continue

        if int(session.get("user_id", 0)) != int(user_id):
            continue
        if session.get("claimed") is True:
            continue

        return session_id

    return None
