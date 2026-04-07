"""
Cache Invalidation Helpers

Ensures cache consistency after user mutations.
"""

import logging
from infrastructure.redis import get_redis_or_none

logger = logging.getLogger(__name__)


async def invalidate_user_cache(user_id: int) -> bool:
    """
    Invalidate user profile cache after mutation.

    Args:
        user_id: User ID

    Returns:
        True if cache was invalidated, False if Redis unavailable
    """
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return False

    try:
        cache_key = f"user:cache:{user_id}"
        await redis_conn.delete(cache_key)
        logger.debug("Invalidated user cache for user_id=%s", user_id)
        return True
    except Exception as e:
        logger.warning("Failed to invalidate user cache for user_id=%s: %s", user_id, e)
        return False


async def invalidate_user_caches(user_ids: list[int]) -> int:
    """
    Invalidate multiple user profile caches.

    Args:
        user_ids: List of user IDs

    Returns:
        Number of caches invalidated
    """
    redis_conn = await get_redis_or_none()
    if not redis_conn:
        return 0

    try:
        cache_keys = [f"user:cache:{uid}" for uid in user_ids]
        deleted = await redis_conn.delete(*cache_keys)
        logger.debug("Invalidated %d user caches", deleted)
        return deleted
    except Exception as e:
        logger.warning("Failed to invalidate user caches: %s", e)
        return 0
