"""
Coins Reconciliation Script

Detects mismatches between:
  - PostgreSQL persisted coins (DB)
  - Redis coins_hot (visible balance)
  - Redis coins_pending (unflushed)
  - Redis coins_flushing (in-flight)

Does NOT auto-fix balances. Read-only detection and reporting.

Usage:
  python -m workers.reconcile_coins          # all users
  python -m workers.reconcile_coins --user 123  # specific user
  python -m workers.reconcile_coins --json      # machine-readable output
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

from sqlalchemy import select, func
from DATABASE.base import AsyncSessionLocal, User
from infrastructure.redis import init_redis, close_redis

logger = logging.getLogger("reconcile_coins")


@dataclass
class UserReconciliation:
    user_id: int
    db_coins: int = 0
    hot_coins: int = 0
    pending_coins: int = 0
    flushing_coins: int = 0
    flushing_batches: int = 0
    hot_exists: bool = False
    pending_exists: bool = False
    flushing_keys: list = field(default_factory=list)

    @property
    def total_redis(self) -> int:
        return self.hot_coins + self.pending_coins + self.flushing_coins

    @property
    def expected_db_coins(self) -> int:
        """What DB should be after all flushes complete."""
        return self.db_coins + self.pending_coins + self.flushing_coins

    @property
    def mismatch_categories(self) -> list[str]:
        cats = []
        if self.hot_exists and self.hot_coins < 0:
            cats.append("hot_negative")
        if self.hot_exists and self.db_coins > 0 and self.hot_coins < self.db_coins:
            cats.append("hot_below_db")
        if self.hot_exists and self.pending_coins == 0 and self.flushing_coins == 0:
            pass  # clean state
        if self.pending_exists and self.pending_coins <= 0:
            cats.append("pending_zero_or_negative")
        if self.flushing_batches > 10:
            cats.append("excessive_flushing_batches")
        return cats


async def reconcile_user(user_id: int, redis_conn) -> UserReconciliation:
    result = UserReconciliation(user_id=user_id)

    # DB coins
    async with AsyncSessionLocal() as session:
        user = await session.execute(select(User).where(User.user_id == user_id))
        user_row = user.scalar_one_or_none()
        if user_row:
            result.db_coins = int(user_row.coins or 0)

    # Redis coins_hot
    hot_key = f"coins_hot:{user_id}"
    hot_val = await redis_conn.get(hot_key)
    result.hot_exists = hot_val is not None
    if hot_val is not None:
        result.hot_coins = int(hot_val)

    # Redis coins_pending
    pending_key = f"coins_pending:{user_id}"
    pending_val = await redis_conn.get(pending_key)
    result.pending_exists = pending_val is not None
    if pending_val is not None:
        result.pending_coins = int(pending_val)

    # Redis coins_flushing
    cursor = 0
    while True:
        cursor, keys = await redis_conn.scan(
            cursor, match=f"coins_flushing:{user_id}:*", count=100
        )
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode()
            result.flushing_keys.append(key_str)
            val = await redis_conn.get(key_str)
            if val:
                result.flushing_coins += int(val)
        result.flushing_batches = len(result.flushing_keys)
        if cursor == 0:
            break

    return result


async def reconcile_all_users(
    redis_conn,
    user_ids: Optional[list[int]] = None,
    limit: int = 1000,
) -> list[UserReconciliation]:
    """
    Reconcile coins for all users (or specific user_ids).
    Returns list of results with mismatches flagged.
    """
    async with AsyncSessionLocal() as session:
        stmt = select(User.user_id, User.coins)
        if user_ids:
            stmt = stmt.where(User.user_id.in_(user_ids))
        else:
            stmt = stmt.order_by(User.user_id).limit(limit)

        rows = await session.execute(stmt)
        users = rows.all()

    results = []
    for user_id, db_coins in users:
        rec = await reconcile_user(int(user_id), redis_conn)
        rec.db_coins = int(db_coins or 0)
        if rec.mismatch_categories or rec.hot_exists:
            results.append(rec)

    return results


def print_report(results: list[UserReconciliation], as_json: bool = False):
    total = len(results)
    mismatches = [r for r in results if r.mismatch_categories]
    hot_below_db = [r for r in results if "hot_below_db" in r.mismatch_categories]
    hot_negative = [r for r in results if "hot_negative" in r.mismatch_categories]
    excessive_flushing = [
        r for r in results if "excessive_flushing_batches" in r.mismatch_categories
    ]

    if as_json:
        output = {
            "summary": {
                "total_checked": total,
                "mismatches": len(mismatches),
                "hot_below_db": len(hot_below_db),
                "hot_negative": len(hot_negative),
                "excessive_flushing": len(excessive_flushing),
            },
            "mismatches": [asdict(r) for r in mismatches],
        }
        print(json.dumps(output, indent=2))
        return

    logger.info("=" * 60)
    logger.info("COINS RECONCILIATION REPORT")
    logger.info("=" * 60)
    logger.info("Users checked:       %d", total)
    logger.info("Users with hot key:  %d", len([r for r in results if r.hot_exists]))
    logger.info("Mismatches found:    %d", len(mismatches))
    logger.info("  hot_below_db:      %d", len(hot_below_db))
    logger.info("  hot_negative:      %d", len(hot_negative))
    logger.info("  excessive_flush:   %d", len(excessive_flushing))
    logger.info("")

    if mismatches:
        logger.info("MISMATCH DETAILS:")
        logger.info("-" * 60)
        for r in mismatches:
            logger.info(
                "User %d: db=%d hot=%d pending=%d flushing=%d(%d batches) issues=%s",
                r.user_id,
                r.db_coins,
                r.hot_coins,
                r.pending_coins,
                r.flushing_coins,
                r.flushing_batches,
                r.mismatch_categories,
            )
    else:
        logger.info("No mismatches detected.")

    logger.info("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description="Coins reconciliation checker")
    parser.add_argument("--user", type=int, help="Check specific user ID")
    parser.add_argument("--users", type=str, help="Comma-separated user IDs to check")
    parser.add_argument(
        "--limit", type=int, default=1000, help="Max users to check (default: 1000)"
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )

    user_ids = None
    if args.user:
        user_ids = [args.user]
    elif args.users:
        user_ids = [int(x.strip()) for x in args.users.split(",")]

    redis_conn = await init_redis()
    if not redis_conn:
        logger.error("Cannot connect to Redis")
        sys.exit(1)

    try:
        start = time.monotonic()
        results = await reconcile_all_users(
            redis_conn, user_ids=user_ids, limit=args.limit
        )
        elapsed = (time.monotonic() - start) * 1000
        logger.info("Reconciliation completed in %.0fms", elapsed)
        print_report(results, as_json=args.json)
    finally:
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
