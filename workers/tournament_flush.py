"""
Tournament Flush Worker
Периодически snapshot'ит Redis leaderboard в PostgreSQL.

Safety:
- Upsert: INSERT ... ON CONFLICT DO UPDATE — idempotent
- Если worker падает — следующий запуск повторит snapshot
- Redis ZINCRBY всегда увеличивается, snapshot только читает
- Потеря данных: максимум FLUSH_INTERVAL секунд leaderboard данных
"""

import asyncio
import logging
import redis.asyncio as redis

from DATABASE.base import (
    AsyncSessionLocal,
    WeeklyTournamentEntry,
    get_weekly_tournament_season_key,
    get_weekly_tournament_season_window,
    ensure_weekly_tournament_season,
)
from core.game_config import TOURNAMENT_KEY
from infrastructure.redis import init_redis, close_redis
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime

logger = logging.getLogger(__name__)

FLUSH_INTERVAL = 60  # seconds
LEADERBOARD_LIMIT = 1000


async def flush_tournament_to_db(redis_conn: redis.Redis) -> int:
    """
    Snapshot Redis ZSET leaderboard → PostgreSQL.
    Возвращает количество записей.
    """
    # Получить топ-N из Redis leaderboard
    entries = await redis_conn.zrevrange(
        TOURNAMENT_KEY, 0, LEADERBOARD_LIMIT - 1, withscores=True
    )

    if not entries:
        return 0

    starts_at, ends_at = get_weekly_tournament_season_window()
    season_key = get_weekly_tournament_season_key(starts_at)
    now = datetime.utcnow()

    async with AsyncSessionLocal() as session:
        await ensure_weekly_tournament_season(session, season_key, starts_at, ends_at)

        flushed = 0
        for entry_data in entries:
            # zrevrange withscores returns (member, score) tuples
            if isinstance(entry_data, tuple):
                user_id_str, score = entry_data
            else:
                continue

            user_id = int(user_id_str)
            score = int(score)

            if score <= 0:
                continue

            # Upsert: INSERT ... ON CONFLICT DO UPDATE
            insert_stmt = pg_insert(WeeklyTournamentEntry).values(
                season_key=season_key,
                user_id=user_id,
                username=None,
                display_level=1,
                league="bronze",
                score=score,
                eligible_for_payout=True,
                fraud_flag=False,
                created_at=now,
                updated_at=now,
            )
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    WeeklyTournamentEntry.__table__.c.season_key,
                    WeeklyTournamentEntry.__table__.c.user_id,
                ],
                set_={
                    "score": score,
                    "updated_at": now,
                },
            )
            await session.execute(upsert_stmt)
            flushed += 1

        await session.commit()

    return flushed


async def tournament_flush_loop():
    logger.info("Tournament flush worker started (interval=%ds)", FLUSH_INTERVAL)
    while True:
        try:
            redis_conn = await init_redis()
            if redis_conn:
                flushed = await flush_tournament_to_db(redis_conn)
                if flushed > 0:
                    logger.info("Flushed %d tournament entries to DB", flushed)
        except Exception as e:
            logger.error("Tournament flush error: %s", e)
        await asyncio.sleep(FLUSH_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(tournament_flush_loop())
