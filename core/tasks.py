"""
Video task pure helpers extracted from routers/legacy.py (Patch 7.6).
"""

import random
from typing import Any


def resolve_video_task_coin_drop() -> int:
    """Resolve a random coin drop reward for video tasks."""
    return random.randint(5000, 25000)


def get_video_task_last_claims(extra: dict) -> dict:
    """Get last claim timestamps for video tasks."""
    claims = extra.get("video_task_last_claims")
    if isinstance(claims, dict):
        return claims
    return {}


def get_video_task_boosts(extra: dict) -> dict:
    """Get active video task boosts."""
    boosts = extra.get("video_task_boosts")
    if isinstance(boosts, dict):
        return boosts
    return {}


def get_active_video_task_boost(
    extra: dict, boost_type: str
) -> tuple[bool, str | None, int]:
    """Check if a video task boost is currently active."""
    boosts = get_video_task_boosts(extra)
    boost = boosts.get(boost_type)
    if not isinstance(boost, dict):
        return False, None, 1
    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None, 1
    from datetime import datetime

    try:
        exp_dt = datetime.fromisoformat(expires_at)
        if datetime.utcnow() < exp_dt:
            return True, expires_at, int(boost.get("multiplier", 1))
    except Exception:
        pass
    return False, expires_at, 1
