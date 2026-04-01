import logging
from datetime import datetime
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from DATABASE.base import (
    WeeklyTournamentEntry,
    WeeklyTournamentSeason,
    WeeklyTournamentWinner,
    get_weekly_tournament_season_key,
    get_weekly_tournament_season_window,
    get_weekly_tournament_league,
    ensure_weekly_tournament_season,
)

logger = logging.getLogger(__name__)

WEEKLY_LEAGUE_ORDER = ("diamond", "gold", "silver", "bronze")
WEEKLY_LEAGUE_FUND_SPLITS = {
    "diamond": 0.50,
    "gold": 0.30,
    "silver": 0.15,
    "bronze": 0.05,
}
WEEKLY_TOP_PAYOUT_SPLITS = {1: 0.30, 2: 0.20, 3: 0.13}
WEEKLY_RANGE_PAYOUT_SPLITS = [
    {"start": 4, "end": 10, "share": 0.22},
    {"start": 11, "end": 20, "share": 0.10},
    {"start": 21, "end": 50, "share": 0.05},
]


async def upsert_tournament_score(
    session: AsyncSession,
    user_id: int,
    username: str | None,
    display_level: int,
    gained: int,
) -> dict | None:
    if gained <= 0:
        return None

    starts_at, ends_at = get_weekly_tournament_season_window()
    season_key = get_weekly_tournament_season_key(starts_at)
    league = get_weekly_tournament_league(display_level)
    now = datetime.utcnow()

    await ensure_weekly_tournament_season(session, season_key, starts_at, ends_at)

    insert_stmt = pg_insert(WeeklyTournamentEntry).values(
        season_key=season_key,
        user_id=user_id,
        username=username,
        display_level=max(1, display_level),
        league=league,
        score=gained,
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
            "score": WeeklyTournamentEntry.__table__.c.score
            + insert_stmt.excluded.score,
            "username": insert_stmt.excluded.username,
            "display_level": insert_stmt.excluded.display_level,
            "league": insert_stmt.excluded.league,
            "updated_at": insert_stmt.excluded.updated_at,
        },
    )
    await session.execute(upsert_stmt)
    await session.flush()

    score_row = await session.execute(
        select(WeeklyTournamentEntry.score).where(
            WeeklyTournamentEntry.season_key == season_key,
            WeeklyTournamentEntry.user_id == user_id,
        )
    )
    final_score = int(score_row.scalar_one())

    return {
        "season_key": season_key,
        "league": league,
        "score": final_score,
    }


async def get_weekly_leaderboard(
    session: AsyncSession,
    season_key: str | None = None,
    league: str | None = None,
    limit: int = 50,
) -> list[dict]:
    season_key = season_key or get_weekly_tournament_season_key()
    limit = max(1, min(200, limit))

    query = select(WeeklyTournamentEntry).where(
        WeeklyTournamentEntry.season_key == season_key
    )
    if league:
        query = query.where(WeeklyTournamentEntry.league == league)
    query = query.order_by(
        desc(WeeklyTournamentEntry.score), WeeklyTournamentEntry.updated_at.asc()
    ).limit(limit)

    result = await session.execute(query)
    entries = result.scalars().all()

    return [
        {
            "rank": idx,
            "user_id": entry.user_id,
            "username": entry.username,
            "display_level": int(entry.display_level or 1),
            "league": entry.league,
            "score": int(entry.score or 0),
            "eligible_for_payout": bool(entry.eligible_for_payout),
            "fraud_flag": bool(entry.fraud_flag),
        }
        for idx, entry in enumerate(entries, start=1)
    ]


async def get_player_tournament_entry(
    session: AsyncSession,
    user_id: int,
    season_key: str | None = None,
) -> dict | None:
    season_key = season_key or get_weekly_tournament_season_key()

    result = await session.execute(
        select(WeeklyTournamentEntry).where(
            WeeklyTournamentEntry.season_key == season_key,
            WeeklyTournamentEntry.user_id == user_id,
        )
    )
    entry = result.scalar_one_or_none()
    if not entry:
        return None

    rank_query = await session.execute(
        select(
            select(WeeklyTournamentEntry)
            .where(
                WeeklyTournamentEntry.season_key == season_key,
                WeeklyTournamentEntry.league == entry.league,
                WeeklyTournamentEntry.score > entry.score,
            )
            .count()
        )
    )
    rank = int(rank_query.scalar() or 0) + 1

    return {
        "user_id": entry.user_id,
        "username": entry.username,
        "display_level": int(entry.display_level or 1),
        "league": entry.league,
        "score": int(entry.score or 0),
        "rank": rank,
        "eligible_for_payout": bool(entry.eligible_for_payout),
        "fraud_flag": bool(entry.fraud_flag),
    }


async def finalize_season(
    session: AsyncSession,
    season_key: str,
) -> bool:
    season_result = await session.execute(
        select(WeeklyTournamentSeason).where(
            WeeklyTournamentSeason.season_key == season_key,
            WeeklyTournamentSeason.status != "finalized",
        )
    )
    season = season_result.scalar_one_or_none()
    if not season:
        return False

    total_fund_cents = max(0, int(season.payout_fund_cents or 0))

    for league in WEEKLY_LEAGUE_ORDER:
        result = await session.execute(
            select(WeeklyTournamentEntry)
            .where(
                WeeklyTournamentEntry.season_key == season_key,
                WeeklyTournamentEntry.league == league,
            )
            .order_by(
                desc(WeeklyTournamentEntry.score),
                WeeklyTournamentEntry.updated_at.asc(),
            )
            .limit(50)
        )
        entries = result.scalars().all()
        league_fund_cents = int(
            total_fund_cents * WEEKLY_LEAGUE_FUND_SPLITS.get(league, 0)
        )

        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP_PAYOUT_SPLITS.items()
        }
        range_payouts = []
        for range_def in WEEKLY_RANGE_PAYOUT_SPLITS:
            pool_cents = int(league_fund_cents * range_def["share"])
            eligible = [
                e
                for idx, e in enumerate(entries, start=1)
                if range_def["start"] <= idx <= range_def["end"]
                and bool(e.eligible_for_payout)
                and not bool(e.fraud_flag)
            ]
            share_cents = pool_cents // len(eligible) if eligible else 0
            remainder = pool_cents % len(eligible) if eligible else 0
            range_payouts.append(
                {
                    "start": range_def["start"],
                    "end": range_def["end"],
                    "share_cents": share_cents,
                    "remainder_cents": remainder,
                }
            )

        for idx, entry in enumerate(entries, start=1):
            payout_cents = 0
            if bool(entry.eligible_for_payout) and not bool(entry.fraud_flag):
                if idx in top_payouts:
                    payout_cents = top_payouts[idx]
                else:
                    for rp in range_payouts:
                        if rp["start"] <= idx <= rp["end"]:
                            payout_cents = rp["share_cents"]
                            if rp["remainder_cents"] > 0:
                                payout_cents += 1
                                rp["remainder_cents"] -= 1
                            break

            winner = WeeklyTournamentWinner(
                season_key=season_key,
                user_id=entry.user_id,
                username=entry.username,
                league=league,
                rank=idx,
                display_level=int(entry.display_level or 1),
                score=int(entry.score or 0),
                payout_cents=payout_cents,
                eligible_for_payout=bool(entry.eligible_for_payout),
                fraud_flag=bool(entry.fraud_flag),
            )
            session.add(winner)

    season.status = "finalized"
    season.settled_at = datetime.utcnow()
    return True
