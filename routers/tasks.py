import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.database import AsyncSessionLocal
from routers.auth import require_telegram_user
from DATABASE.base import User, UserTask
from repositories.user_repo import get_user_by_id, update_user_atomic

router = APIRouter(prefix="/api/v2", tags=["tasks"])
logger = logging.getLogger(__name__)

DAILY_REWARD_MAX_DAYS = 30
DAILY_REWARD_BASE_COINS = 500
DAILY_REWARD_INFINITE_ENERGY_MINUTES = 10
DAILY_REWARD_SKIN_ID = "retro.pngSP"

TASK_DEFINITIONS = [
    {
        "id": "join_channel",
        "title": "Join Telegram Channel",
        "reward": 5000,
        "type": "social",
    },
    {
        "id": "invite_3_friends",
        "title": "Invite 3 Friends",
        "reward": 15000,
        "type": "referral",
    },
    {
        "id": "reach_level_10",
        "title": "Reach Level 10",
        "reward": 10000,
        "type": "progress",
    },
]


@router.get("/tasks")
async def get_tasks(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UserTask.task_id).where(UserTask.user_id == user_id)
        )
        completed = set(result.scalars().all())

    tasks = []
    for task in TASK_DEFINITIONS:
        tasks.append(
            {
                "id": task["id"],
                "title": task["title"],
                "reward": task["reward"],
                "type": task["type"],
                "completed": task["id"] in completed,
            }
        )

    return {"success": True, "tasks": tasks}


@router.post("/tasks/complete")
async def complete_task(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    body = await request.json()
    task_id = body.get("task_id", "")

    task_def = next((t for t in TASK_DEFINITIONS if t["id"] == task_id), None)
    if not task_def:
        raise HTTPException(status_code=400, detail="Unknown task")

    async with AsyncSessionLocal() as session:
        existing = await session.execute(
            select(UserTask).where(
                UserTask.user_id == user_id, UserTask.task_id == task_id
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Task already completed")

        session.add(UserTask(user_id=user_id, task_id=task_id))

        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        new_coins = int(user.get("coins", 0)) + task_def["reward"]
        await session.execute(
            select(User).where(User.user_id == user_id).with_for_update()
        )
        await session.execute(
            User.__table__.update()
            .where(User.user_id == user_id)
            .values(coins=new_coins)
        )
        await session.commit()

    return {"success": True, "coins": new_coins, "reward": task_def["reward"]}


@router.get("/daily-reward")
async def get_daily_reward_status(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user = await get_user_by_id(session, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

    extra = user.get("extra_data", {})
    if isinstance(extra, str):
        import json

        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    claimed_days = extra.get("daily_reward_claimed_days", [])
    last_claim = extra.get("daily_reward_last_claim_date")

    today = datetime.utcnow().date().isoformat()
    already_claimed_today = last_claim == today

    next_day = len(claimed_days) + 1 if not already_claimed_today else len(claimed_days)
    next_day = min(next_day, DAILY_REWARD_MAX_DAYS)

    return {
        "success": True,
        "claimed_days": claimed_days,
        "last_claim_date": last_claim,
        "already_claimed_today": already_claimed_today,
        "next_day": next_day,
        "next_reward": next_day * DAILY_REWARD_BASE_COINS,
        "max_days": DAILY_REWARD_MAX_DAYS,
    }


@router.post("/daily-reward/claim")
async def claim_daily_reward(request: Request):
    telegram_user = await require_telegram_user(request)
    user_id = int(telegram_user.get("id", 0))

    async with AsyncSessionLocal() as session:
        user_row = await session.execute(
            select(User).where(User.user_id == user_id).with_for_update()
        )
        user_db = user_row.scalar_one_or_none()
        if not user_db:
            raise HTTPException(status_code=404, detail="User not found")

        import json

        extra = {}
        if user_db.extra_data:
            try:
                extra = json.loads(user_db.extra_data)
            except Exception:
                extra = {}

        claimed_days = extra.get("daily_reward_claimed_days", [])
        last_claim = extra.get("daily_reward_last_claim_date")
        today = datetime.utcnow().date().isoformat()

        if last_claim == today:
            raise HTTPException(status_code=400, detail="Already claimed today")

        if len(claimed_days) >= DAILY_REWARD_MAX_DAYS:
            raise HTTPException(status_code=400, detail="All daily rewards claimed")

        day_number = len(claimed_days) + 1
        reward = day_number * DAILY_REWARD_BASE_COINS

        claimed_days.append(day_number)
        extra["daily_reward_claimed_days"] = claimed_days
        extra["daily_reward_last_claim_date"] = today

        if day_number % 7 == 0:
            expires_at = (
                datetime.utcnow()
                + timedelta(minutes=DAILY_REWARD_INFINITE_ENERGY_MINUTES)
            ).isoformat()
            active_boosts = extra.get("active_boosts", {})
            if not isinstance(active_boosts, dict):
                active_boosts = {}
            active_boosts["daily_infinite_energy"] = {
                "active": True,
                "expires_at": expires_at,
            }
            extra["active_boosts"] = active_boosts

        if day_number == DAILY_REWARD_MAX_DAYS:
            owned_skins = extra.get("owned_skins", ["default.pngSP"])
            if not isinstance(owned_skins, list):
                owned_skins = ["default.pngSP"]
            if DAILY_REWARD_SKIN_ID not in owned_skins:
                owned_skins.append(DAILY_REWARD_SKIN_ID)
            extra["owned_skins"] = owned_skins

        user_db.coins += reward
        user_db.extra_data = json.dumps(extra)
        await session.commit()

    return {
        "success": True,
        "day": day_number,
        "reward": reward,
        "coins": user_db.coins,
        "infinite_energy": day_number % 7 == 0,
        "skin_unlocked": day_number == DAILY_REWARD_MAX_DAYS,
    }


# ─── Legacy task routes (moved from legacy.py, Patch 7.3) ────────────────────

import json as _json
from fastapi import APIRouter as _APIRouter

router_legacy = _APIRouter(tags=["tasks-legacy"])
logger_legacy = logging.getLogger(__name__)

from core.config import VIDEO_TASK_DEFINITIONS as _VIDEO_TASK_DEFINITIONS
from core.game_config import RATE_LIMITS as _RATE_LIMITS
from core.utils import (
    parse_extra_data as _parse_extra_data,
    parse_iso_datetime as _parse_iso_datetime,
)
from core.skins import (
    normalize_owned_skins as _normalize_owned_skins,
    DEFAULT_SKIN_ID as _DEFAULT_SKIN_ID,
    SOCIAL_SUB_TASK_SKINS as _SOCIAL_SUB_TASK_SKINS,
)
from core.game_logic import normalize_dt
from core.tasks import (
    resolve_video_task_coin_drop as _resolve_video_task_coin_drop,
    get_video_task_last_claims as _get_video_task_last_claims,
    get_video_task_boosts as _get_video_task_boosts,
    get_active_video_task_boost as _get_active_video_task_boost,
)
from routers.legacy import (
    require_telegram_user as _require_telegram_user,
    require_dual_rate_limit as _require_dual_rate_limit,
    require_user_action_lock as _require_user_action_lock,
    get_user_cached as _get_user_cached,
    update_user as _update_user,
    invalidate_user_cache as _invalidate_user_cache,
    get_completed_tasks as _get_completed_tasks,
    complete_task_reward_atomically as _complete_task_reward_atomically,
    consume_ad_action_session as _consume_ad_action_session,
    verify_telegram_channel_subscription as _verify_telegram_channel_subscription,
    resolve_video_task_coin_drop as _resolve_video_task_coin_drop,
    get_video_task_last_claims as _get_video_task_last_claims,
    get_video_task_boosts as _get_video_task_boosts,
    get_active_video_task_boost as _get_active_video_task_boost,
    record_rewarded_ad_claim as _record_rewarded_ad_claim,
    TaskCompleteRequest as _TaskCompleteRequest,
    VideoTaskClaimRequest as _VideoTaskClaimRequest,
)


@router_legacy.get("/api/tasks/{user_id}")
async def get_tasks_legacy(user_id: int, request: Request):
    try:
        await _require_telegram_user(request, user_id)
        user = await _get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        completed_tasks = await _get_completed_tasks(user_id) or []

        tasks = [
            {
                "id": "daily_bonus",
                "title": "📅 Daily Bonus",
                "description": "Come back every day",
                "reward": "25000 coins",
                "icon": "📅",
                "completed": "daily_bonus" in completed_tasks,
            },
            {
                "id": "energy_refill",
                "title": "⚡ Infinite Energy",
                "description": "5 minutes of unlimited energy",
                "reward": "⚡ 5 minutes",
                "icon": "⚡",
                "completed": "energy_refill" in completed_tasks,
            },
            {
                "id": "link_click",
                "title": "🔗 Follow Link",
                "description": "Click the link and get reward",
                "reward": "25000 coins",
                "icon": "🔗",
                "completed": "link_click" in completed_tasks,
            },
            {
                "id": "telegram_sub",
                "title": "Telegram Channel",
                "description": "Subscribe to Telegram channel",
                "reward": "20000 coins + skin",
                "icon": "📣",
                "completed": "telegram_sub" in completed_tasks,
            },
            {
                "id": "tiktok_sub",
                "title": "TikTok",
                "description": "Subscribe to TikTok",
                "reward": "20000 coins + skin",
                "icon": "🎵",
                "completed": "tiktok_sub" in completed_tasks,
            },
            {
                "id": "instagram_sub",
                "title": "Instagram",
                "description": "Subscribe to Instagram",
                "reward": "20000 coins + skin",
                "icon": "📸",
                "completed": "instagram_sub" in completed_tasks,
            },
            {
                "id": "invite_5_friends",
                "title": "👥 Invite 5 Friends",
                "description": "Invite 5 friends",
                "reward": "20000 coins",
                "icon": "👥",
                "completed": "invite_5_friends" in completed_tasks,
                "progress": min(user.get("referral_count", 0), 5),
                "total": 5,
            },
        ]
        return tasks
    except Exception as e:
        logger_legacy.error(f"Error in get_tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router_legacy.post("/api/complete-task")
async def complete_task_legacy(payload: _TaskCompleteRequest, request: Request):
    try:
        await _require_telegram_user(request, payload.user_id)
        await _require_dual_rate_limit(
            "complete_task",
            request,
            payload.user_id,
            _RATE_LIMITS["complete_task"][0],
            _RATE_LIMITS["complete_task"][1],
            ip_limit=_RATE_LIMITS["complete_task"][0] * 2,
        )
        await _require_user_action_lock("complete_task", payload.user_id, ttl=5)
        user = await _get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        task_id = payload.task_id
        completed = await _get_completed_tasks(payload.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")

        if task_id == "link_click":
            updated_user = await _complete_task_reward_atomically(
                payload.user_id,
                task_id,
                {"coins": int(user.get("coins", 0)) + 25000},
            )
            await _invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "🔗 +25000 coins!",
                "coins": int(updated_user.get("coins", 0)),
            }

        if task_id == "daily_bonus":
            updated_user = await _complete_task_reward_atomically(
                payload.user_id,
                task_id,
                {"coins": int(user.get("coins", 0)) + 25000},
            )
            await _invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "🎁 +25000 coins!",
                "coins": int(updated_user.get("coins", 0)),
            }

        elif task_id == "energy_refill":
            await _complete_task_reward_atomically(payload.user_id, task_id)
            await _invalidate_user_cache(payload.user_id)
            return {"success": True, "message": "⚡ Energy refill activated!"}

        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                updated_user = await _complete_task_reward_atomically(
                    payload.user_id,
                    task_id,
                    {"coins": int(user.get("coins", 0)) + 20000},
                )
                await _invalidate_user_cache(payload.user_id)
                return {
                    "success": True,
                    "message": "👥 +20000 coins!",
                    "coins": int(updated_user.get("coins", 0)),
                }
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")

        elif task_id in _SOCIAL_SUB_TASK_SKINS:
            extra = user.get("extra_data", {}) or {}
            if isinstance(extra, str):
                try:
                    extra = _json.loads(extra)
                except Exception:
                    extra = {}
            owned_skins = _normalize_owned_skins(
                extra.get("owned_skins", [_DEFAULT_SKIN_ID])
            )
            social_skin_id = _SOCIAL_SUB_TASK_SKINS[task_id]

            if task_id == "telegram_sub":
                is_verified = await _verify_telegram_channel_subscription(
                    payload.user_id
                )
                if not is_verified:
                    raise HTTPException(
                        status_code=400,
                        detail="Telegram subscription was not verified yet",
                    )
            else:
                raise HTTPException(
                    status_code=400, detail="Task verification is not available yet"
                )

            if social_skin_id not in owned_skins:
                owned_skins.append(social_skin_id)
            extra["owned_skins"] = _normalize_owned_skins(owned_skins)
            updated_user = await _complete_task_reward_atomically(
                payload.user_id,
                task_id,
                {"coins": int(user.get("coins", 0)) + 20000, "extra_data": extra},
            )
            await _invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "✅ +20000 coins + skin!",
                "coins": int(updated_user.get("coins", 0)),
                "skin_id": social_skin_id,
                "verified": task_id == "telegram_sub",
            }

        raise HTTPException(status_code=400, detail="Unknown task")
    except HTTPException:
        raise
    except Exception as e:
        logger_legacy.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router_legacy.get("/api/video-tasks/status/{user_id}")
async def get_video_tasks_status_legacy(user_id: int, request: Request):
    try:
        await _require_telegram_user(request, user_id)
        user = await _get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = _parse_extra_data(user.get("extra_data"))
        last_claims = _get_video_task_last_claims(extra)
        now = datetime.utcnow()
        tasks = []

        for task_id, config in _VIDEO_TASK_DEFINITIONS.items():
            claimed_at = _parse_iso_datetime(last_claims.get(task_id))
            cooldown_seconds = int(config["cooldown_minutes"] * 60)
            remaining_seconds = 0
            available = True
            if claimed_at:
                elapsed = (now - claimed_at).total_seconds()
                remaining_seconds = max(0, cooldown_seconds - int(elapsed))
                available = remaining_seconds <= 0
            tasks.append(
                {
                    "task_id": task_id,
                    "available": available,
                    "remaining_seconds": remaining_seconds,
                    "cooldown_minutes": config["cooldown_minutes"],
                }
            )

        return {"success": True, "tasks": tasks}
    except HTTPException:
        raise
    except Exception as e:
        logger_legacy.error(f"Error in get_video_tasks_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router_legacy.post("/api/video-tasks/claim")
async def claim_video_task_legacy(payload: _VideoTaskClaimRequest, request: Request):
    try:
        await _require_telegram_user(request, payload.user_id)
        await _require_dual_rate_limit(
            "video_task_claim", request, payload.user_id, 20, 60, ip_limit=40
        )
        await _consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "video_task"
        )
        await _require_user_action_lock(
            f"video_task:{payload.task_id}", payload.user_id, ttl=3
        )

        config = _VIDEO_TASK_DEFINITIONS.get(payload.task_id)
        if not config:
            raise HTTPException(status_code=400, detail="Unknown video task")

        user = await _get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = _parse_extra_data(user.get("extra_data"))
        last_claims = _get_video_task_last_claims(extra)
        boosts = _get_video_task_boosts(extra)
        now = datetime.utcnow()
        claimed_at = _parse_iso_datetime(last_claims.get(payload.task_id))
        cooldown_seconds = int(config["cooldown_minutes"] * 60)

        if claimed_at and (now - claimed_at).total_seconds() < cooldown_seconds:
            remaining = cooldown_seconds - int((now - claimed_at).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Task cooldown {remaining // 60}:{remaining % 60:02d}",
            )

        response = {
            "success": True,
            "task_id": payload.task_id,
            "coins": int(user.get("coins", 0)),
        }
        updates = {}

        if config["type"] == "coin_drop":
            reward = _resolve_video_task_coin_drop()
            response["coins_reward"] = reward
            response["coins"] = int(user.get("coins", 0)) + reward
            response["message"] = f"+{reward} coins"
            updates["coins"] = response["coins"]
        elif config["type"] == "tap_boost":
            expires_at = (
                now + timedelta(minutes=config["duration_minutes"])
            ).isoformat()
            boosts["tap_boost"] = {
                "expires_at": expires_at,
                "multiplier": int(config["multiplier"]),
            }
            response["message"] = (
                f"x{config['multiplier']} tap boost for {config['duration_minutes']} min"
            )
            response["task_tap_boost_active"] = True
            response["task_tap_boost_expires_at"] = expires_at
            response["task_tap_boost_multiplier"] = int(config["multiplier"])
        elif config["type"] == "passive_boost":
            expires_at = (
                now + timedelta(minutes=config["duration_minutes"])
            ).isoformat()
            boosts["passive_boost"] = {
                "expires_at": expires_at,
                "multiplier": int(config["multiplier"]),
            }
            response["message"] = (
                f"x{config['multiplier']} passive income for {config['duration_minutes']} min"
            )
            response["task_passive_boost_active"] = True
            response["task_passive_boost_expires_at"] = expires_at
            response["task_passive_boost_multiplier"] = int(config["multiplier"])

        last_claims[payload.task_id] = now.isoformat()
        extra["video_task_last_claims"] = last_claims
        extra["video_task_boosts"] = boosts
        updates["extra_data"] = extra

        await _update_user(payload.user_id, updates)
        await _invalidate_user_cache(payload.user_id)
        refreshed_user = await _get_user_cached(payload.user_id)
        response["coins"] = int((refreshed_user or {}).get("coins", response["coins"]))
        await _record_rewarded_ad_claim(
            payload.user_id, "tasks", {"task_id": payload.task_id}
        )
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger_legacy.error(f"Error in claim_video_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
