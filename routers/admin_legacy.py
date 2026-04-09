"""
Admin endpoints extracted from legacy.py (Patch 7.1).

All routes use /api/admin/* prefix, matching the original paths exactly.
Shared helpers (require_admin_access, build_admin_fraud_overview, etc.)
remain in legacy.py and are imported here.
"""

import json
import logging
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import select, update, func, desc, or_
from sqlalchemy.ext.asyncio import AsyncSession

from DATABASE.base import (
    AsyncSessionLocal,
    User,
    UserTask,
    WeeklyTournamentEntry,
    WeeklyTournamentWinner,
    WeeklyTournamentTonPayout,
    RewardedAdClaim,
    StarsSkinPurchase,
    get_weekly_tournament_season_key,
    get_weekly_tournament_season_window,
    list_weekly_tournament_seasons,
    get_weekly_tournament_winners,
    get_weekly_tournament_leaderboard,
    ensure_weekly_tournament_season,
    get_rewarded_ads_admin_summary,
    get_stars_skin_sales_admin_summary,
    get_admin_fraud_reviews,
    upsert_admin_fraud_review,
    get_referral_stats,
    get_referrals_list,
    WEEKLY_LEAGUE_ORDER,
    WEEKLY_LEAGUE_FUND_SPLITS,
    WEEKLY_RANGE_PAYOUT_SPLITS,
)
from core.config import (
    WEEKLY_LEAGUE_LEVEL_RANGES,
    WEEKLY_TOP3_PAYOUT_SPLITS,
    TON_NANO,
    TON_PAYOUT_SENDER_URL,
    TON_PAYOUT_SENDER_TOKEN,
    TON_PAYOUT_SENDER_TIMEOUT_SECONDS,
    TON_VERIFIER_API_BASE,
    TON_VERIFIER_API_KEY,
    TON_VERIFIER_TIMEOUT_SECONDS,
    DIAGNOSTICS_DURATION_WINDOW,
)
from core.utils import parse_extra_data_object, parse_json_object, parse_extra_data
from core.ton_utils import (
    get_ton_wallet_from_user,
    mask_ton_wallet,
    ton_wallet_normalized_variants,
    ton_wallets_equal,
    is_valid_ton_wallet_address,
)
from core.skins import normalize_owned_skins, normalize_selected_skin, DEFAULT_SKIN_ID
from routers.legacy import (
    require_admin_access,
    build_admin_fraud_overview,
    serialize_endpoint_diagnostic,
    ENDPOINT_DIAGNOSTICS,
    RECENT_DIAGNOSTIC_ERRORS,
    invalidate_user_cache,
    send_telegram_wallet_reminder_message,
)
from schemas import (
    AdminFraudUpdateRequest,
    AdminTonPayoutSendRequest,
    AdminTonPayoutQueueRequest,
    AdminTonPayoutStatusUpdateRequest,
    AdminTonPayoutBulkStatusUpdateRequest,
    AdminTonPayoutConfirmRequest,
    AdminWalletReminderRequest,
    AdminWinnerStarsUpdateRequest,
    WeeklyTournamentFundRequest,
)

router = APIRouter(tags=["admin"])
logger = logging.getLogger(__name__)


# ─── Helper functions (moved from legacy.py) ─────────────────────────────────


async def fetch_ton_transactions_for_accounts(
    accounts: list[str], start_utime: int
) -> list[dict]:
    if not accounts:
        return []
    params: list[tuple[str, str]] = [
        ("limit", "500"),
        ("sort", "desc"),
        ("start_utime", str(max(0, int(start_utime or 0)))),
    ]
    for account in accounts:
        params.append(("account", account))
    headers = {}
    if TON_VERIFIER_API_KEY:
        headers["X-API-Key"] = TON_VERIFIER_API_KEY
    async with httpx.AsyncClient(timeout=TON_VERIFIER_TIMEOUT_SECONDS) as client:
        response = await client.get(
            f"{TON_VERIFIER_API_BASE}/transactions", params=params, headers=headers
        )
        response.raise_for_status()
        payload = response.json()
    if isinstance(payload, dict):
        if isinstance(payload.get("transactions"), list):
            return payload["transactions"]
        if isinstance(payload.get("result"), list):
            return payload["result"]
    return []


def match_ton_queue_rows_to_transactions(
    rows: list[WeeklyTournamentTonPayout],
    transactions: list[dict],
    sender_wallet_address: str,
) -> tuple[list[dict], set[int]]:
    matched_rows: list[dict] = []
    matched_user_ids: set[int] = set()
    sender_variants = ton_wallet_normalized_variants(sender_wallet_address)
    for row in rows:
        recipient_variants = ton_wallet_normalized_variants(row.wallet_address)
        if not recipient_variants:
            continue
        for tx in transactions:
            tx_account = tx.get("account")
            if tx_account and not ton_wallets_equal(tx_account, row.wallet_address):
                continue
            in_msg = tx.get("in_msg") or {}
            source = in_msg.get("source") or ""
            destination = in_msg.get("destination") or tx_account or ""
            value = int(in_msg.get("value") or 0)
            tx_hash = tx.get("hash") or in_msg.get("hash") or tx.get("trace_id") or ""
            tx_now = int(tx.get("now") or 0)
            aborted = bool((tx.get("description") or {}).get("aborted"))
            if aborted:
                continue
            if sender_variants and not ton_wallet_normalized_variants(
                source
            ).intersection(sender_variants):
                continue
            if destination and not ton_wallets_equal(destination, row.wallet_address):
                continue
            if value != int(row.ton_amount_nano or 0):
                continue
            matched_rows.append(
                {
                    "user_id": int(row.user_id),
                    "tx_hash": tx_hash,
                    "confirmed_at": tx_now,
                }
            )
            matched_user_ids.add(int(row.user_id))
            break
    return matched_rows, matched_user_ids


def allocate_ton_nano_by_weights(
    candidates: list[dict], total_fund_ton: float
) -> tuple[dict[int, int], int]:
    total_nano = max(0, int(round(float(total_fund_ton or 0) * TON_NANO)))
    if total_nano <= 0 or not candidates:
        return {}, total_nano
    total_weight = sum(max(0, int(item.get("payout_cents") or 0)) for item in candidates)
    if total_weight <= 0:
        return {}, total_nano

    allocations: dict[int, int] = {}
    remainders: list[tuple[int, int, int]] = []
    allocated_sum = 0
    for item in candidates:
        user_id = int(item["user_id"])
        weight = max(0, int(item.get("payout_cents") or 0))
        numerator = total_nano * weight
        base_amount = numerator // total_weight
        remainder = numerator % total_weight
        allocations[user_id] = int(base_amount)
        allocated_sum += int(base_amount)
        remainders.append((int(remainder), int(item.get("rank") or 999999), user_id))

    left = max(0, total_nano - allocated_sum)
    if left > 0:
        remainders.sort(key=lambda x: (-x[0], x[1], x[2]))
        for idx in range(left):
            _, _, user_id = remainders[idx % len(remainders)]
            allocations[user_id] = allocations.get(user_id, 0) + 1
    return allocations, total_nano


async def send_ton_payouts_with_sender_service(
    season_key: str, payouts: list[dict], note: str | None = None
) -> dict[int, dict]:
    if not TON_PAYOUT_SENDER_URL:
        return {}
    headers = {"Content-Type": "application/json"}
    if TON_PAYOUT_SENDER_TOKEN:
        headers["Authorization"] = f"Bearer {TON_PAYOUT_SENDER_TOKEN}"
    payload = {
        "season_key": season_key,
        "note": (note or "").strip() or None,
        "payouts": [
            {
                "user_id": int(item["user_id"]),
                "wallet_address": str(item["wallet_address"]),
                "amount_nano": int(item["ton_amount_nano"]),
                "rank": int(item.get("rank") or 0),
                "league": str(item.get("league") or ""),
            }
            for item in payouts
        ],
    }
    async with httpx.AsyncClient(timeout=TON_PAYOUT_SENDER_TIMEOUT_SECONDS) as client:
        response = await client.post(
            TON_PAYOUT_SENDER_URL,
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        body = response.json()
    raw_results = body.get("results") if isinstance(body, dict) else None
    if not isinstance(raw_results, list):
        return {}
    results_by_user: dict[int, dict] = {}
    for item in raw_results:
        if not isinstance(item, dict):
            continue
        user_id = int(item.get("user_id") or 0)
        if user_id <= 0:
            continue
        results_by_user[user_id] = item
    return results_by_user


async def build_weekly_ton_payout_candidates(
    season_key: str, ton_price_usd: float
) -> tuple[dict | None, list[dict], dict]:
    season_rows = await list_weekly_tournament_seasons(limit=52)
    season = next(
        (item for item in season_rows if item["season_key"] == season_key), None
    )
    if not season:
        return None, [], {"with_wallet": 0, "without_wallet": 0, "without_payout": 0}
    total_fund_cents = max(0, int(season.get("payout_fund_cents") or 0))
    if total_fund_cents <= 0:
        return season, [], {"with_wallet": 0, "without_wallet": 0, "without_payout": 0}
    entries_by_league: dict[str, list[dict]] = {}
    for league in WEEKLY_LEAGUE_ORDER:
        entries_by_league[league] = await get_weekly_tournament_leaderboard(
            season_key=season_key, league=league, limit=50
        )
    user_ids = [
        int(entry["user_id"])
        for league_entries in entries_by_league.values()
        for entry in league_entries
    ]
    wallet_map: dict[int, dict] = {}
    if user_ids:
        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_rows = users_result.scalars().all()
            for row in user_rows:
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet
    payouts: list[dict] = []
    stats = {"with_wallet": 0, "without_wallet": 0, "without_payout": 0}
    ton_price_usd_micros = int(round(float(ton_price_usd) * 1_000_000))
    for league in WEEKLY_LEAGUE_ORDER:
        entries = entries_by_league.get(league) or []
        league_fund_cents = int(
            total_fund_cents * WEEKLY_LEAGUE_FUND_SPLITS.get(league, 0)
        )
        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP3_PAYOUT_SPLITS.items()
        }
        range_payouts = []
        for payout_range in WEEKLY_RANGE_PAYOUT_SPLITS:
            start_rank = int(payout_range["start"])
            end_rank = int(payout_range["end"])
            pool_cents = int(league_fund_cents * float(payout_range["share"]))
            eligible_entries = [
                entry
                for entry in entries
                if start_rank <= int(entry["rank"]) <= end_rank
                and bool(entry.get("eligible_for_payout", True))
                and not bool(entry.get("fraud_flag", False))
            ]
            share_cents = 0
            remainder_cents = 0
            if eligible_entries:
                share_cents = pool_cents // len(eligible_entries)
                remainder_cents = pool_cents % len(eligible_entries)
            range_payouts.append(
                {
                    "start": start_rank,
                    "end": end_rank,
                    "share_cents": share_cents,
                    "remainder_cents": remainder_cents,
                }
            )
        for entry in entries:
            rank = int(entry["rank"])
            payout_cents = 0
            if bool(entry.get("eligible_for_payout", True)) and not bool(
                entry.get("fraud_flag", False)
            ):
                if rank in top_payouts:
                    payout_cents = top_payouts[rank]
                else:
                    for payout_range in range_payouts:
                        if payout_range["start"] <= rank <= payout_range["end"]:
                            payout_cents = payout_range["share_cents"]
                            if payout_range["remainder_cents"] > 0:
                                payout_cents += 1
                                payout_range["remainder_cents"] -= 1
                            break
            if payout_cents <= 0:
                stats["without_payout"] += 1
                continue
            wallet = wallet_map.get(int(entry["user_id"]))
            if not wallet:
                stats["without_wallet"] += 1
                continue
            ton_amount_nano = max(
                0, int(round(((payout_cents / 100.0) / ton_price_usd) * TON_NANO))
            )
            if ton_amount_nano <= 0:
                stats["without_payout"] += 1
                continue
            stats["with_wallet"] += 1
            payouts.append(
                {
                    "user_id": int(entry["user_id"]),
                    "username": entry.get("username"),
                    "league": league,
                    "rank": rank,
                    "wallet_address": wallet["address"],
                    "masked_wallet": mask_ton_wallet(wallet["address"]),
                    "payout_cents": int(payout_cents),
                    "ton_amount_nano": int(ton_amount_nano),
                    "ton_price_usd": ton_price_usd_micros / 1_000_000,
                    "status": "preview"
                    if season.get("status") != "finalized"
                    else "queued",
                    "tx_hash": None,
                    "note": "preview queue"
                    if season.get("status") != "finalized"
                    else None,
                }
            )
    payouts.sort(
        key=lambda row: (WEEKLY_LEAGUE_ORDER.index(row["league"]), int(row["rank"]))
    )
    return season, payouts, stats


async def build_weekly_ton_payout_view(
    season_key: str, ton_price_usd: float, league: str | None = None
) -> tuple[dict | None, dict[str, list[dict]], dict]:
    season_rows = await list_weekly_tournament_seasons(limit=52)
    season = next(
        (item for item in season_rows if item["season_key"] == season_key), None
    )
    if not season:
        return None, {}, {}
    selected_leagues = [league] if league else list(WEEKLY_LEAGUE_ORDER)
    total_fund_cents = max(0, int(season.get("payout_fund_cents") or 0))
    entries_by_league: dict[str, list[dict]] = {}
    for league_key in selected_leagues:
        entries_by_league[league_key] = await get_weekly_tournament_leaderboard(
            season_key=season_key, league=league_key, limit=50
        )
    user_ids = [
        int(entry["user_id"])
        for league_entries in entries_by_league.values()
        for entry in league_entries
    ]
    wallet_map: dict[int, dict] = {}
    wallet_reminders_map: dict[int, dict] = {}
    existing_payouts_map: dict[int, WeeklyTournamentTonPayout] = {}
    async with AsyncSessionLocal() as session:
        if user_ids:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_rows = users_result.scalars().all()
            for row in user_rows:
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet
                reminders_by_season = extra_data.get("ton_wallet_reminders") or {}
                if isinstance(reminders_by_season, dict):
                    reminder = reminders_by_season.get(season_key) or {}
                    if isinstance(reminder, dict) and reminder.get("sent_at"):
                        wallet_reminders_map[int(row.user_id)] = reminder
        if user_ids:
            payouts_result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key,
                    WeeklyTournamentTonPayout.user_id.in_(user_ids),
                )
            )
            existing_payouts_map = {
                int(row.user_id): row for row in payouts_result.scalars().all()
            }
    winner_map_by_league: dict[str, dict[int, dict]] = {}
    if season.get("status") == "finalized":
        winners = await get_weekly_tournament_winners(season_key)
        for league_key in selected_leagues:
            winner_map_by_league[league_key] = {
                int(winner["user_id"]): winner
                for winner in winners
                if winner["league"] == league_key
            }
    leagues_payload: dict[str, list[dict]] = {}
    summary = {
        "with_wallet": 0,
        "without_wallet": 0,
        "eligible": 0,
        "without_payout": 0,
        "wallet_reminder_sent": 0,
    }
    for league_key in selected_leagues:
        entries = entries_by_league.get(league_key, [])
        league_fund_cents = int(
            total_fund_cents * WEEKLY_LEAGUE_FUND_SPLITS.get(league_key, 0)
        )
        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP3_PAYOUT_SPLITS.items()
        }
        range_payouts = []
        for payout_range in WEEKLY_RANGE_PAYOUT_SPLITS:
            start_rank = int(payout_range["start"])
            end_rank = int(payout_range["end"])
            pool_cents = int(league_fund_cents * float(payout_range["share"]))
            eligible_entries = [
                entry
                for entry in entries
                if start_rank <= int(entry["rank"]) <= end_rank
                and bool(entry.get("eligible_for_payout", True))
                and not bool(entry.get("fraud_flag", False))
            ]
            share_cents = 0
            remainder_cents = 0
            if eligible_entries:
                share_cents = pool_cents // len(eligible_entries)
                remainder_cents = pool_cents % len(eligible_entries)
            range_payouts.append(
                {
                    "start": start_rank,
                    "end": end_rank,
                    "share_cents": share_cents,
                    "remainder_cents": remainder_cents,
                }
            )
        winner_map = winner_map_by_league.get(league_key, {})
        league_rows: list[dict] = []
        for entry in entries:
            user_id = int(entry["user_id"])
            wallet = wallet_map.get(user_id)
            existing_row = existing_payouts_map.get(user_id)
            winner = winner_map.get(user_id)
            payout_cents = 0
            if season.get("status") == "finalized" and winner:
                payout_cents = int(winner.get("payout_cents") or 0)
            elif bool(entry.get("eligible_for_payout", True)) and not bool(
                entry.get("fraud_flag", False)
            ):
                rank = int(entry["rank"])
                if rank in top_payouts:
                    payout_cents = top_payouts[rank]
                else:
                    for payout_range in range_payouts:
                        if payout_range["start"] <= rank <= payout_range["end"]:
                            payout_cents = payout_range["share_cents"]
                            if payout_range["remainder_cents"] > 0:
                                payout_cents += 1
                                payout_range["remainder_cents"] -= 1
                            break
            if payout_cents > 0:
                summary["eligible"] += 1
            else:
                summary["without_payout"] += 1
            wallet_connected = bool(
                wallet and wallet.get("address") and wallet.get("verified")
            )
            if wallet_connected:
                summary["with_wallet"] += 1
            else:
                summary["without_wallet"] += 1
            wallet_reminder = wallet_reminders_map.get(user_id) or {}
            wallet_reminder_sent_at = wallet_reminder.get("sent_at")
            if wallet_reminder_sent_at:
                summary["wallet_reminder_sent"] += 1
            ton_amount_nano = 0
            if existing_row:
                ton_amount_nano = int(existing_row.ton_amount_nano or 0)
            elif wallet_connected and payout_cents > 0 and ton_price_usd > 0:
                ton_amount_nano = max(
                    0, int(round(((payout_cents / 100.0) / ton_price_usd) * TON_NANO))
                )
            derived_status = "wallet_missing"
            if payout_cents <= 0:
                derived_status = "no_payout"
            elif existing_row:
                derived_status = existing_row.status or "queued"
            elif season.get("status") == "finalized":
                derived_status = "queued" if wallet_connected else "wallet_missing"
            else:
                derived_status = (
                    "preview_ready" if wallet_connected else "wallet_missing"
                )
            league_rows.append(
                {
                    "user_id": user_id,
                    "username": entry.get("username"),
                    "league": league_key,
                    "rank": int(entry["rank"]),
                    "display_level": int(entry.get("display_level") or 1),
                    "score": int(entry.get("score") or 0),
                    "eligible_for_payout": bool(entry.get("eligible_for_payout", True)),
                    "fraud_flag": bool(entry.get("fraud_flag", False)),
                    "wallet_connected": wallet_connected,
                    "wallet_address": wallet.get("address") if wallet else None,
                    "masked_wallet": wallet.get("masked_address") if wallet else None,
                    "payout_cents": int(payout_cents),
                    "ton_amount_nano": int(ton_amount_nano),
                    "status": derived_status,
                    "tx_hash": getattr(existing_row, "tx_hash", None),
                    "note": getattr(existing_row, "note", None),
                    "wallet_reminder_sent_at": wallet_reminder_sent_at,
                    "wallet_reminder_hours_until_deadline": wallet_reminder.get(
                        "hours_until_deadline"
                    ),
                }
            )
        leagues_payload[league_key] = league_rows
    return season, leagues_payload, summary


async def recalculate_finalized_weekly_winner_payouts(
    session: AsyncSession,
    season_key: str,
    total_fund_cents: int,
) -> None:
    winners_result = await session.execute(
        select(WeeklyTournamentWinner)
        .where(WeeklyTournamentWinner.season_key == season_key)
        .order_by(
            WeeklyTournamentWinner.league.asc(),
            WeeklyTournamentWinner.rank.asc(),
            WeeklyTournamentWinner.user_id.asc(),
        )
    )
    winners = winners_result.scalars().all()
    if not winners:
        return
    winners_by_league: dict[str, list[WeeklyTournamentWinner]] = {}
    for winner in winners:
        winners_by_league.setdefault((winner.league or "bronze").lower(), []).append(
            winner
        )
    for league in WEEKLY_LEAGUE_ORDER:
        league_winners = winners_by_league.get(league, [])
        if not league_winners:
            continue
        league_fund_cents = int(
            max(0, int(total_fund_cents or 0))
            * WEEKLY_LEAGUE_FUND_SPLITS.get(league, 0)
        )
        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP3_PAYOUT_SPLITS.items()
        }
        range_payouts = []
        for payout_range in WEEKLY_RANGE_PAYOUT_SPLITS:
            start_rank = int(payout_range["start"])
            end_rank = int(payout_range["end"])
            pool_cents = int(league_fund_cents * float(payout_range["share"]))
            eligible_winners = [
                row
                for row in league_winners
                if start_rank <= int(row.rank or 0) <= end_rank
                and bool(row.eligible_for_payout)
                and not bool(row.fraud_flag)
            ]
            share_cents = 0
            remainder_cents = 0
            if eligible_winners:
                share_cents = pool_cents // len(eligible_winners)
                remainder_cents = pool_cents % len(eligible_winners)
            range_payouts.append(
                {
                    "start": start_rank,
                    "end": end_rank,
                    "share_cents": share_cents,
                    "remainder_cents": remainder_cents,
                }
            )
        for winner in league_winners:
            payout_cents = 0
            rank = int(winner.rank or 0)
            if bool(winner.eligible_for_payout) and not bool(winner.fraud_flag):
                if rank in top_payouts:
                    payout_cents = top_payouts[rank]
                else:
                    for payout_range in range_payouts:
                        if payout_range["start"] <= rank <= payout_range["end"]:
                            payout_cents = payout_range["share_cents"]
                            if payout_range["remainder_cents"] > 0:
                                payout_cents += 1
                                payout_range["remainder_cents"] -= 1
                            break
            winner.payout_cents = int(payout_cents)


# ─── Admin routes ────────────────────────────────────────────────────────────


@router.get("/api/admin/weekly-tournament/seasons")
async def admin_weekly_tournament_seasons(request: Request, limit: int = 12):
    try:
        await require_admin_access(request)
        seasons = await list_weekly_tournament_seasons(limit=limit)
        return {"success": True, "seasons": seasons}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_weekly_tournament_seasons: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/overview")
async def admin_overview(request: Request):
    try:
        await require_admin_access(request)
        now = datetime.utcnow()
        starts_at, ends_at = get_weekly_tournament_season_window(now)
        season_key = get_weekly_tournament_season_key(now)
        from routers.legacy import get_online_users_count

        online_now = await get_online_users_count()
        season_rows = await list_weekly_tournament_seasons(limit=12)
        active_season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )
        async with AsyncSessionLocal() as session:
            total_users_result = await session.execute(select(func.count(User.id)))
            total_users = int(total_users_result.scalar() or 0)
            league_counts_result = await session.execute(
                select(
                    WeeklyTournamentEntry.league, func.count(WeeklyTournamentEntry.id)
                )
                .where(WeeklyTournamentEntry.season_key == season_key)
                .group_by(WeeklyTournamentEntry.league)
            )
            league_counts = {league: 0 for league in WEEKLY_LEAGUE_ORDER}
            for league, count in league_counts_result.all():
                if league in league_counts:
                    league_counts[league] = int(count or 0)
        top_preview = {}
        for league in WEEKLY_LEAGUE_ORDER:
            players = await get_weekly_tournament_leaderboard(
                season_key=season_key, league=league, limit=3
            )
            top_preview[league] = players
        return {
            "success": True,
            "generated_at": now.isoformat(),
            "online_now": online_now,
            "total_users": total_users,
            "season_key": season_key,
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "time_left_seconds": max(0, int((ends_at - now).total_seconds())),
            "active_season": active_season,
            "league_counts": league_counts,
            "top_preview": top_preview,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/diagnostics/endpoints")
async def admin_endpoint_diagnostics(
    request: Request, limit: int = 20, sort: str = "requests"
):
    try:
        await require_admin_access(request)
        max_items = max(1, min(100, int(limit or 20)))
        sort_key = (sort or "requests").strip().lower()
        supported_sort = {
            "requests",
            "errors",
            "p95_ms",
            "avg_ms",
            "status_429",
            "status_5xx",
        }
        if sort_key not in supported_sort:
            sort_key = "requests"
        rows = [
            serialize_endpoint_diagnostic(stats)
            for stats in ENDPOINT_DIAGNOSTICS.values()
        ]
        rows.sort(key=lambda item: float(item.get(sort_key, 0) or 0), reverse=True)
        top_rows = rows[:max_items]
        summary = {
            "tracked_endpoints": len(rows),
            "total_requests": sum(int(item.get("requests", 0)) for item in rows),
            "total_errors": sum(int(item.get("errors", 0)) for item in rows),
            "total_429": sum(int(item.get("status_429", 0)) for item in rows),
            "total_5xx": sum(int(item.get("status_5xx", 0)) for item in rows),
            "window_size": DIAGNOSTICS_DURATION_WINDOW,
        }
        return {
            "success": True,
            "sort": sort_key,
            "limit": max_items,
            "summary": summary,
            "endpoints": top_rows,
            "recent_errors": list(RECENT_DIAGNOSTIC_ERRORS)[:20],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_endpoint_diagnostics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/rewarded-ads/summary")
async def admin_rewarded_ads_summary(request: Request, hours: int = 24):
    try:
        await require_admin_access(request)
        summary = await get_rewarded_ads_admin_summary(hours=hours)
        tracked_actions = (
            "boost",
            "autoclicker",
            "tasks",
            "ghost",
            "energy_restore",
            "skins",
        )
        actions = {
            action: {
                "total": int(summary["actions_total"].get(action, 0)),
                "recent": int(summary["actions_recent"].get(action, 0)),
            }
            for action in tracked_actions
        }
        return {
            "success": True,
            "hours_window": summary["hours_window"],
            "total_claims": summary["total_claims"],
            "recent_claims": summary["recent_claims"],
            "actions": actions,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_rewarded_ads_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/stars-skins/summary")
async def admin_stars_skins_summary(
    request: Request, limit: int = 20, currency: str | None = None
):
    try:
        await require_admin_access(request)
        currency_filter = (currency or "").strip().upper() or None
        summary = await get_stars_skin_sales_admin_summary(
            limit=limit, currency=currency_filter
        )
        response = {"success": True, **summary}
        if currency_filter == "TON":
            total_nano = int(summary.get("total_stars") or 0)
            response["total_ton_nano"] = total_nano
            response["total_ton"] = float(total_nano) / float(TON_NANO)
            response["by_skin"] = [
                {
                    **item,
                    "amount_nano": int(item.get("stars_amount") or 0),
                    "amount_ton": float(int(item.get("stars_amount") or 0))
                    / float(TON_NANO),
                }
                for item in (summary.get("by_skin") or [])
            ]
            response["recent"] = [
                {
                    **item,
                    "amount_nano": int(item.get("stars_amount") or 0),
                    "amount_ton": float(int(item.get("stars_amount") or 0))
                    / float(TON_NANO),
                }
                for item in (summary.get("recent") or [])
            ]
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_stars_skins_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/fraud/overview")
async def admin_fraud_overview(request: Request, season_key: str | None = None):
    try:
        await require_admin_access(request)
        effective_season_key = season_key or get_weekly_tournament_season_key()
        players = await build_admin_fraud_overview(effective_season_key)
        return {
            "success": True,
            "season_key": effective_season_key,
            "players": players,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_fraud_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/players/search")
async def admin_players_search(
    request: Request,
    query: str = "",
    season_key: str | None = None,
    limit: int = 20,
):
    try:
        await require_admin_access(request)
        effective_season_key = (
            season_key or get_weekly_tournament_season_key() or ""
        ).strip()
        search_query = (query or "").strip()
        normalized_query = search_query.lstrip("@").strip()
        search_limit = max(1, min(50, int(limit or 20)))
        async with AsyncSessionLocal() as session:
            stmt = select(User)
            if normalized_query:
                if normalized_query.isdigit():
                    stmt = stmt.where(User.user_id == int(normalized_query))
                else:
                    lowered = normalized_query.lower()
                    matching_user_ids: set[int] = set()
                    users_match_result = await session.execute(
                        select(User.user_id)
                        .where(
                            func.lower(func.coalesce(User.username, "")).like(
                                f"%{lowered}%"
                            )
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in users_match_result.all()
                        if row and row[0] is not None
                    )
                    entry_match_result = await session.execute(
                        select(WeeklyTournamentEntry.user_id)
                        .where(
                            func.lower(
                                func.coalesce(WeeklyTournamentEntry.username, "")
                            ).like(f"%{lowered}%")
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in entry_match_result.all()
                        if row and row[0] is not None
                    )
                    winner_match_result = await session.execute(
                        select(WeeklyTournamentWinner.user_id)
                        .where(
                            func.lower(
                                func.coalesce(WeeklyTournamentWinner.username, "")
                            ).like(f"%{lowered}%")
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in winner_match_result.all()
                        if row and row[0] is not None
                    )
                    payout_match_result = await session.execute(
                        select(WeeklyTournamentTonPayout.user_id)
                        .where(
                            func.lower(
                                func.coalesce(WeeklyTournamentTonPayout.username, "")
                            ).like(f"%{lowered}%")
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in payout_match_result.all()
                        if row and row[0] is not None
                    )
                    if matching_user_ids:
                        stmt = stmt.where(User.user_id.in_(sorted(matching_user_ids)))
                    else:
                        stmt = stmt.where(
                            or_(
                                func.lower(func.coalesce(User.username, "")).like(
                                    f"%{lowered}%"
                                ),
                                User.user_id == -1,
                            )
                        )
            stmt = stmt.order_by(User.created_at.desc()).limit(search_limit)
            users = (await session.execute(stmt)).scalars().all()
            user_ids = [int(user.user_id) for user in users]
            entry_map: dict[int, WeeklyTournamentEntry] = {}
            if user_ids and effective_season_key:
                entries_result = await session.execute(
                    select(WeeklyTournamentEntry).where(
                        WeeklyTournamentEntry.season_key == effective_season_key,
                        WeeklyTournamentEntry.user_id.in_(user_ids),
                    )
                )
                entry_map = {
                    int(row.user_id): row for row in entries_result.scalars().all()
                }
        reviews_map = await get_admin_fraud_reviews(user_ids) if user_ids else {}
        players = []
        for user in users:
            extra = parse_extra_data_object(user.extra_data)
            wallet = get_ton_wallet_from_user({"extra_data": extra})
            owned_skins = normalize_owned_skins(
                extra.get("owned_skins", [DEFAULT_SKIN_ID])
            )
            selected_skin = normalize_selected_skin(
                extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
            )
            entry = entry_map.get(int(user.user_id))
            review = reviews_map.get(int(user.user_id), {}) or {}
            players.append(
                {
                    "user_id": int(user.user_id),
                    "username": user.username,
                    "created_at": user.created_at.isoformat()
                    if user.created_at
                    else None,
                    "coins": int(user.coins or 0),
                    "energy": int(user.energy or 0),
                    "max_energy": int(user.max_energy or 0),
                    "level": int(user.level or 0),
                    "referral_count": int(user.referral_count or 0),
                    "owned_skins_count": int(len(owned_skins)),
                    "selected_skin": selected_skin,
                    "wallet_connected": bool(wallet.get("connected")),
                    "wallet_verified": bool(wallet.get("verified")),
                    "wallet_masked": wallet.get("masked_address"),
                    "season_key": effective_season_key,
                    "season_entry": {
                        "league": entry.league,
                        "score": int(entry.score or 0),
                        "display_level": int(entry.display_level or 1),
                        "eligible_for_payout": bool(entry.eligible_for_payout),
                        "fraud_flag": bool(entry.fraud_flag),
                    }
                    if entry
                    else None,
                    "fraud_review": {
                        "status": review.get("status") or "ok",
                        "reason": review.get("reason"),
                        "disqualify_from_payout": bool(
                            review.get("disqualify_from_payout")
                        ),
                    },
                }
            )
        return {
            "success": True,
            "query": normalized_query,
            "season_key": effective_season_key,
            "players": players,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_players_search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/players/{user_id}")
async def admin_player_detail(
    user_id: int, request: Request, season_key: str | None = None
):
    try:
        await require_admin_access(request)
        effective_season_key = (
            season_key or get_weekly_tournament_season_key() or ""
        ).strip()
        async with AsyncSessionLocal() as session:
            user_result = await session.execute(
                select(User).where(User.user_id == user_id)
            )
            user = user_result.scalar_one_or_none()
            if not user:
                raise HTTPException(status_code=404, detail="Player not found")
            extra = parse_extra_data_object(user.extra_data)
            wallet = get_ton_wallet_from_user({"extra_data": extra})
            owned_skins = normalize_owned_skins(
                extra.get("owned_skins", [DEFAULT_SKIN_ID])
            )
            selected_skin = normalize_selected_skin(
                extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
            )
            selected_entry = None
            selected_winner = None
            selected_payout = None
            selected_rank = None
            if effective_season_key:
                selected_entry_result = await session.execute(
                    select(WeeklyTournamentEntry).where(
                        WeeklyTournamentEntry.season_key == effective_season_key,
                        WeeklyTournamentEntry.user_id == user_id,
                    )
                )
                selected_entry = selected_entry_result.scalar_one_or_none()
                selected_winner_result = await session.execute(
                    select(WeeklyTournamentWinner).where(
                        WeeklyTournamentWinner.season_key == effective_season_key,
                        WeeklyTournamentWinner.user_id == user_id,
                    )
                )
                selected_winner = selected_winner_result.scalar_one_or_none()
                selected_payout_result = await session.execute(
                    select(WeeklyTournamentTonPayout).where(
                        WeeklyTournamentTonPayout.season_key == effective_season_key,
                        WeeklyTournamentTonPayout.user_id == user_id,
                    )
                )
                selected_payout = selected_payout_result.scalar_one_or_none()
                if selected_entry:
                    rank_result = await session.execute(
                        select(func.count(WeeklyTournamentEntry.id)).where(
                            WeeklyTournamentEntry.season_key == effective_season_key,
                            WeeklyTournamentEntry.league == selected_entry.league,
                            WeeklyTournamentEntry.score > selected_entry.score,
                        )
                    )
                    selected_rank = int(rank_result.scalar() or 0) + 1
            reward_rows_result = await session.execute(
                select(RewardedAdClaim)
                .where(RewardedAdClaim.user_id == user_id)
                .order_by(RewardedAdClaim.created_at.desc())
                .limit(20)
            )
            reward_rows = reward_rows_result.scalars().all()
            reward_summary_result = await session.execute(
                select(RewardedAdClaim.action, func.count(RewardedAdClaim.id))
                .where(RewardedAdClaim.user_id == user_id)
                .group_by(RewardedAdClaim.action)
            )
            reward_summary = {
                str(action): int(total or 0)
                for action, total in reward_summary_result.all()
                if action
            }
            skin_purchases_result = await session.execute(
                select(StarsSkinPurchase)
                .where(StarsSkinPurchase.user_id == user_id)
                .order_by(StarsSkinPurchase.created_at.desc())
                .limit(20)
            )
            skin_purchases = skin_purchases_result.scalars().all()
            payout_rows_result = await session.execute(
                select(WeeklyTournamentTonPayout)
                .where(WeeklyTournamentTonPayout.user_id == user_id)
                .order_by(
                    WeeklyTournamentTonPayout.updated_at.desc(),
                    WeeklyTournamentTonPayout.created_at.desc(),
                )
                .limit(20)
            )
            payout_rows = payout_rows_result.scalars().all()
            task_rows_result = await session.execute(
                select(UserTask)
                .where(UserTask.user_id == user_id)
                .order_by(UserTask.completed_at.desc())
                .limit(20)
            )
            task_rows = task_rows_result.scalars().all()
            recent_entries_result = await session.execute(
                select(WeeklyTournamentEntry)
                .where(WeeklyTournamentEntry.user_id == user_id)
                .order_by(WeeklyTournamentEntry.season_key.desc())
                .limit(8)
            )
            recent_entries = recent_entries_result.scalars().all()
            recent_winners_result = await session.execute(
                select(WeeklyTournamentWinner)
                .where(WeeklyTournamentWinner.user_id == user_id)
                .order_by(WeeklyTournamentWinner.season_key.desc())
                .limit(8)
            )
            recent_winners = recent_winners_result.scalars().all()
        referrals = (await get_referrals_list(user_id))[:20]
        referral_stats = await get_referral_stats(user_id)
        review = (await get_admin_fraud_reviews([user_id])).get(user_id, {}) or {}
        return {
            "success": True,
            "season_key": effective_season_key,
            "player": {
                "user_id": int(user.user_id),
                "username": user.username,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "profile": {
                    "coins": int(user.coins or 0),
                    "profit_per_hour": int(user.profit_per_hour or 0),
                    "profit_per_tap": int(user.profit_per_tap or 0),
                    "energy": int(user.energy or 0),
                    "max_energy": int(user.max_energy or 0),
                    "level": int(user.level or 0),
                    "referrer_id": int(user.referrer_id) if user.referrer_id else None,
                },
                "upgrades": {
                    "multitap_level": int(user.multitap_level or 0),
                    "profit_level": int(user.profit_level or 0),
                    "energy_level": int(user.energy_level or 0),
                    "boost_level": int(user.boost_level or 0),
                    "luck_level": int(user.luck_level or 0),
                },
                "skins": {
                    "selected_skin": selected_skin,
                    "owned_skins": owned_skins,
                    "owned_count": int(len(owned_skins)),
                },
                "wallet": wallet,
                "referrals": {
                    "count": int(referral_stats.get("count", 0) or 0),
                    "earnings": int(referral_stats.get("earnings", 0) or 0),
                    "recent": referrals,
                },
                "selected_season": {
                    "season_key": effective_season_key,
                    "entry": {
                        "league": selected_entry.league,
                        "score": int(selected_entry.score or 0),
                        "display_level": int(selected_entry.display_level or 1),
                        "eligible_for_payout": bool(selected_entry.eligible_for_payout),
                        "fraud_flag": bool(selected_entry.fraud_flag),
                        "rank_estimate": selected_rank,
                    }
                    if selected_entry
                    else None,
                    "winner": {
                        "league": selected_winner.league,
                        "rank": int(selected_winner.rank or 0),
                        "score": int(selected_winner.score or 0),
                        "payout_cents": int(selected_winner.payout_cents or 0),
                        "stars_reward": int(selected_winner.stars_reward or 0),
                        "eligible_for_payout": bool(
                            selected_winner.eligible_for_payout
                        ),
                        "fraud_flag": bool(selected_winner.fraud_flag),
                    }
                    if selected_winner
                    else None,
                    "ton_payout": {
                        "status": selected_payout.status,
                        "payout_cents": int(selected_payout.payout_cents or 0),
                        "ton_amount_nano": int(selected_payout.ton_amount_nano or 0),
                        "wallet_address": selected_payout.wallet_address,
                        "tx_hash": selected_payout.tx_hash,
                        "note": selected_payout.note,
                        "updated_at": selected_payout.updated_at.isoformat()
                        if selected_payout.updated_at
                        else None,
                    }
                    if selected_payout
                    else None,
                },
                "recent_tournament_entries": [
                    {
                        "season_key": row.season_key,
                        "league": row.league,
                        "score": int(row.score or 0),
                        "display_level": int(row.display_level or 1),
                        "eligible_for_payout": bool(row.eligible_for_payout),
                        "fraud_flag": bool(row.fraud_flag),
                        "updated_at": row.updated_at.isoformat()
                        if row.updated_at
                        else None,
                    }
                    for row in recent_entries
                ],
                "recent_tournament_wins": [
                    {
                        "season_key": row.season_key,
                        "league": row.league,
                        "rank": int(row.rank or 0),
                        "score": int(row.score or 0),
                        "payout_cents": int(row.payout_cents or 0),
                        "stars_reward": int(row.stars_reward or 0),
                        "eligible_for_payout": bool(row.eligible_for_payout),
                        "fraud_flag": bool(row.fraud_flag),
                        "created_at": row.created_at.isoformat()
                        if row.created_at
                        else None,
                    }
                    for row in recent_winners
                ],
                "reward_ads": {
                    "summary_by_action": reward_summary,
                    "recent": [
                        {
                            "action": row.action,
                            "created_at": row.created_at.isoformat()
                            if row.created_at
                            else None,
                            "metadata": parse_json_object(row.metadata_json),
                        }
                        for row in reward_rows
                    ],
                },
                "completed_tasks": [
                    {
                        "task_id": row.task_id,
                        "completed_at": row.completed_at.isoformat()
                        if row.completed_at
                        else None,
                    }
                    for row in task_rows
                ],
                "stars_skin_purchases": [
                    {
                        "skin_id": row.skin_id,
                        "stars_amount": int(row.stars_amount or 0),
                        "currency": row.currency,
                        "telegram_charge_id": row.telegram_charge_id,
                        "created_at": row.created_at.isoformat()
                        if row.created_at
                        else None,
                    }
                    for row in skin_purchases
                ],
                "payout_history": [
                    {
                        "season_key": row.season_key,
                        "league": row.league,
                        "rank": int(row.rank or 0),
                        "status": row.status,
                        "wallet_address": row.wallet_address,
                        "payout_cents": int(row.payout_cents or 0),
                        "ton_amount_nano": int(row.ton_amount_nano or 0),
                        "tx_hash": row.tx_hash,
                        "note": row.note,
                        "updated_at": row.updated_at.isoformat()
                        if row.updated_at
                        else None,
                    }
                    for row in payout_rows
                ],
                "fraud_review": {
                    "status": review.get("status") or "ok",
                    "reason": review.get("reason"),
                    "disqualify_from_payout": bool(
                        review.get("disqualify_from_payout")
                    ),
                    "updated_at": review.get("updated_at"),
                },
                "support": {
                    "reward_failures_available": False,
                    "duplicate_reward_attempts_available": False,
                    "moderation_timeline_available": False,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_player_detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/fraud/user/{user_id}")
async def admin_update_fraud_status(
    user_id: int, payload: AdminFraudUpdateRequest, request: Request
):
    try:
        await require_admin_access(request)
        status = (payload.status or "").strip().lower()
        if status not in {"fraud", "ok"}:
            raise HTTPException(status_code=400, detail="status must be fraud or ok")
        effective_season_key = payload.season_key or get_weekly_tournament_season_key()
        disqualify = bool(payload.disqualify_from_payout)
        await upsert_admin_fraud_review(user_id, status, payload.reason, disqualify)
        async with AsyncSessionLocal() as session:
            entry_result = await session.execute(
                select(WeeklyTournamentEntry).where(
                    WeeklyTournamentEntry.season_key == effective_season_key,
                    WeeklyTournamentEntry.user_id == user_id,
                )
            )
            entry = entry_result.scalar_one_or_none()
            if entry:
                entry.fraud_flag = status == "fraud"
                entry.eligible_for_payout = not disqualify
            winner_result = await session.execute(
                select(WeeklyTournamentWinner).where(
                    WeeklyTournamentWinner.season_key == effective_season_key,
                    WeeklyTournamentWinner.user_id == user_id,
                )
            )
            winner = winner_result.scalar_one_or_none()
            if winner:
                winner.fraud_flag = status == "fraud"
                winner.eligible_for_payout = not disqualify
                if disqualify:
                    winner.payout_cents = 0
                    winner.stars_reward = 0
            await session.commit()
        return {
            "success": True,
            "user_id": user_id,
            "season_key": effective_season_key,
            "status": status,
            "disqualify_from_payout": disqualify,
            "reason": payload.reason,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_fraud_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/weekly-tournament/season/{season_key}")
async def admin_weekly_tournament_season_detail(season_key: str, request: Request):
    try:
        await require_admin_access(request)
        season_rows = await list_weekly_tournament_seasons(limit=52)
        season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )
        winners = await get_weekly_tournament_winners(season_key)
        leagues = {}
        for league in WEEKLY_LEAGUE_ORDER:
            leagues[league] = {
                "range": WEEKLY_LEAGUE_LEVEL_RANGES[league],
                "fund_split": WEEKLY_LEAGUE_FUND_SPLITS[league],
                "top50": await get_weekly_tournament_leaderboard(
                    season_key=season_key, league=league, limit=50
                ),
                "winners": [winner for winner in winners if winner["league"] == league],
            }
        return {
            "success": True,
            "season": season,
            "season_key": season_key,
            "leagues": leagues,
            "top3_splits": WEEKLY_TOP3_PAYOUT_SPLITS,
            "rest_split": max(0.0, 1.0 - sum(WEEKLY_TOP3_PAYOUT_SPLITS.values())),
            "payout_splits": {
                "top": WEEKLY_TOP3_PAYOUT_SPLITS,
                "ranges": WEEKLY_RANGE_PAYOUT_SPLITS,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_weekly_tournament_season_detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/weekly-tournament/season/{season_key}/ton-queue")
async def admin_get_ton_payout_queue(season_key: str, request: Request):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(WeeklyTournamentTonPayout)
                .where(WeeklyTournamentTonPayout.season_key == season_key)
                .order_by(
                    WeeklyTournamentTonPayout.league.asc(),
                    WeeklyTournamentTonPayout.rank.asc(),
                )
            )
            rows = result.scalars().all()
        return {
            "success": True,
            "season_key": season_key,
            "payouts": [
                {
                    "user_id": int(row.user_id),
                    "username": row.username,
                    "league": row.league,
                    "rank": int(row.rank or 0),
                    "wallet_address": row.wallet_address,
                    "masked_wallet": mask_ton_wallet(row.wallet_address),
                    "payout_cents": int(row.payout_cents or 0),
                    "ton_amount_nano": int(row.ton_amount_nano or 0),
                    "ton_price_usd": (int(row.ton_price_usd_micros or 0) / 1_000_000)
                    if row.ton_price_usd_micros
                    else 0,
                    "status": row.status,
                    "tx_hash": row.tx_hash,
                    "note": row.note,
                }
                for row in rows
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_get_ton_payout_queue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-queue/preview")
async def admin_preview_ton_payout_queue(
    season_key: str, payload: AdminTonPayoutQueueRequest, request: Request
):
    try:
        await require_admin_access(request)
        ton_price_usd = float(payload.ton_price_usd or 0)
        if ton_price_usd <= 0:
            raise HTTPException(
                status_code=400, detail="ton_price_usd must be greater than zero"
            )
        season, payouts, stats = await build_weekly_ton_payout_candidates(
            season_key, ton_price_usd
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")
        return {
            "success": True,
            "season_key": season_key,
            "preview": True,
            "season_status": season.get("status"),
            "queued": len(payouts),
            "with_wallet": int(stats["with_wallet"]),
            "without_wallet": int(stats["without_wallet"]),
            "without_payout": int(stats["without_payout"]),
            "payouts": payouts,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_preview_ton_payout_queue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/weekly-tournament/season/{season_key}/ton-view")
async def admin_get_ton_payout_view(
    season_key: str,
    request: Request,
    ton_price_usd: float,
    league: str | None = None,
):
    try:
        await require_admin_access(request)
        if ton_price_usd <= 0:
            raise HTTPException(
                status_code=400, detail="ton_price_usd must be greater than zero"
            )
        league_key = (league or "").strip().lower() or None
        if league_key and league_key not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")
        season, leagues_payload, summary = await build_weekly_ton_payout_view(
            season_key, float(ton_price_usd), league=league_key
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")
        return {
            "success": True,
            "season_key": season_key,
            "season_status": season.get("status"),
            "preview": season.get("status") != "finalized",
            "league": league_key,
            "summary": summary,
            "leagues": leagues_payload,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_get_ton_payout_view: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/wallet-reminders")
async def admin_send_wallet_reminders(
    season_key: str,
    payload: AdminWalletReminderRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        season_rows = await list_weekly_tournament_seasons(limit=52)
        season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")
        if season.get("status") != "finalized":
            raise HTTPException(
                status_code=400,
                detail="Wallet reminders are available only after the season is finalized",
            )
        league_key = (payload.league or "").strip().lower() or None
        if league_key and league_key not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")
        _, leagues_payload, _ = await build_weekly_ton_payout_view(
            season_key, 1.0, league=league_key
        )
        rows = [
            row
            for league_rows in leagues_payload.values()
            for row in league_rows
            if int(row.get("payout_cents") or 0) > 0
            and not bool(row.get("wallet_connected"))
        ]
        if not rows:
            return {
                "success": True,
                "season_key": season_key,
                "league": league_key,
                "candidates": 0,
                "sent": 0,
                "failed": 0,
                "results": [],
            }
        user_ids = sorted({int(row["user_id"]) for row in rows})
        reminder_results: list[dict] = []
        cache_ids_to_invalidate: list[int] = []
        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_map = {int(row.user_id): row for row in users_result.scalars().all()}
            for row in rows:
                user_id = int(row["user_id"])
                user_row = user_map.get(user_id)
                if not user_row:
                    reminder_results.append(
                        {
                            "user_id": user_id,
                            "league": row.get("league"),
                            "sent": False,
                            "error": "User not found",
                        }
                    )
                    continue
                ok, error = await send_telegram_wallet_reminder_message(
                    user_id=user_id,
                    season_key=season_key,
                    league=str(row.get("league") or league_key or ""),
                    hours_until_deadline=int(payload.hours_until_deadline),
                )
                if ok:
                    from infrastructure.jsonb_helpers import jsonb_set_nested_field

                    # Atomic JSONB update instead of full overwrite
                    await jsonb_set_nested_field(
                        session,
                        user_id,
                        "ton_wallet_reminders",
                        season_key,
                        {
                            "sent_at": datetime.utcnow().isoformat(),
                            "league": str(row.get("league") or league_key or ""),
                            "hours_until_deadline": int(payload.hours_until_deadline),
                        },
                    )
                    cache_ids_to_invalidate.append(user_id)
                reminder_results.append(
                    {
                        "user_id": user_id,
                        "league": row.get("league"),
                        "sent": ok,
                        "error": error,
                    }
                )
            await session.commit()
        for user_id in cache_ids_to_invalidate:
            await invalidate_user_cache(user_id)
        sent_count = sum(1 for item in reminder_results if item["sent"])
        failed_count = len(reminder_results) - sent_count
        return {
            "success": True,
            "season_key": season_key,
            "league": league_key,
            "candidates": len(rows),
            "sent": sent_count,
            "failed": failed_count,
            "results": reminder_results,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_send_wallet_reminders: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-queue")
async def admin_build_ton_payout_queue(
    season_key: str, payload: AdminTonPayoutQueueRequest, request: Request
):
    try:
        await require_admin_access(request)
        ton_price_usd = float(payload.ton_price_usd or 0)
        if ton_price_usd <= 0:
            raise HTTPException(
                status_code=400, detail="ton_price_usd must be greater than zero"
            )
        winners = await get_weekly_tournament_winners(season_key)
        if not winners:
            return {
                "success": True,
                "season_key": season_key,
                "created": 0,
                "queued": 0,
                "skipped_without_wallet": 0,
                "skipped_without_payout": 0,
            }
        user_ids = [int(item["user_id"]) for item in winners]
        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_rows = users_result.scalars().all()
            wallet_map = {}
            for row in user_rows:
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet
            existing_result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key
                )
            )
            existing_rows = {
                int(row.user_id): row for row in existing_result.scalars().all()
            }
            created = 0
            queued = 0
            skipped_without_wallet = 0
            skipped_without_payout = 0
            skipped_locked = 0
            ton_price_usd_micros = int(round(ton_price_usd * 1_000_000))
            for winner in winners:
                user_id = int(winner["user_id"])
                payout_cents = int(winner.get("payout_cents") or 0)
                if (
                    payout_cents <= 0
                    or not bool(winner.get("eligible_for_payout", True))
                    or bool(winner.get("fraud_flag", False))
                ):
                    skipped_without_payout += 1
                    continue
                wallet = wallet_map.get(user_id)
                if not wallet:
                    skipped_without_wallet += 1
                    continue
                ton_amount_nano = max(
                    0, int(round(((payout_cents / 100.0) / ton_price_usd) * TON_NANO))
                )
                if ton_amount_nano <= 0:
                    skipped_without_payout += 1
                    continue
                row = existing_rows.get(user_id)
                if row is None:
                    row = WeeklyTournamentTonPayout(
                        season_key=season_key,
                        user_id=user_id,
                        username=winner.get("username"),
                        league=winner.get("league") or "bronze",
                        rank=int(winner.get("rank") or 0),
                        wallet_address=wallet["address"],
                        payout_cents=payout_cents,
                        ton_amount_nano=ton_amount_nano,
                        ton_price_usd_micros=ton_price_usd_micros,
                        status="queued",
                    )
                    session.add(row)
                    existing_rows[user_id] = row
                    created += 1
                else:
                    existing_status = str(row.status or "").strip().lower()
                    if existing_status not in {"", "queued", "failed", "cancelled"}:
                        skipped_locked += 1
                        continue
                    row.username = winner.get("username")
                    row.league = winner.get("league") or row.league
                    row.rank = int(winner.get("rank") or row.rank or 0)
                    row.wallet_address = wallet["address"]
                    row.payout_cents = payout_cents
                    row.ton_amount_nano = ton_amount_nano
                    row.ton_price_usd_micros = ton_price_usd_micros
                    row.updated_at = datetime.utcnow()
                    if existing_status in {"failed", "cancelled"}:
                        row.status = "queued"
                queued += 1
            await session.commit()
        return {
            "success": True,
            "season_key": season_key,
            "created": created,
            "queued": queued,
            "skipped_without_wallet": skipped_without_wallet,
            "skipped_without_payout": skipped_without_payout,
            "skipped_locked": skipped_locked,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_build_ton_payout_queue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payouts/send")
async def admin_send_ton_payouts(
    season_key: str, payload: AdminTonPayoutSendRequest, request: Request
):
    try:
        await require_admin_access(request)
        season_rows = await list_weekly_tournament_seasons(limit=52)
        season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")
        if str(season.get("status") or "").lower() != "finalized":
            raise HTTPException(
                status_code=400,
                detail="Payout send is allowed only for finalized seasons",
            )

        winners = await get_weekly_tournament_winners(season_key)
        if not winners:
            return {
                "success": True,
                "season_key": season_key,
                "sender_configured": bool(TON_PAYOUT_SENDER_URL),
                "queued": 0,
                "submitted": 0,
                "failed": 0,
                "created": 0,
                "skipped_without_wallet": 0,
                "skipped_without_payout": 0,
                "skipped_locked": 0,
                "total_fund_ton": float(payload.total_fund_ton),
                "total_fund_nano": int(round(float(payload.total_fund_ton) * TON_NANO)),
            }

        user_ids = [int(item["user_id"]) for item in winners]
        wallet_map: dict[int, dict] = {}
        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            for row in users_result.scalars().all():
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet

        skipped_without_wallet = 0
        skipped_without_payout = 0
        candidates: list[dict] = []
        for winner in winners:
            user_id = int(winner["user_id"])
            payout_cents = int(winner.get("payout_cents") or 0)
            if (
                payout_cents <= 0
                or not bool(winner.get("eligible_for_payout", True))
                or bool(winner.get("fraud_flag", False))
            ):
                skipped_without_payout += 1
                continue
            wallet = wallet_map.get(user_id)
            if not wallet:
                skipped_without_wallet += 1
                continue
            candidates.append(
                {
                    "user_id": user_id,
                    "username": winner.get("username"),
                    "league": winner.get("league") or "bronze",
                    "rank": int(winner.get("rank") or 0),
                    "payout_cents": payout_cents,
                    "wallet_address": wallet["address"],
                }
            )

        allocations, total_fund_nano = allocate_ton_nano_by_weights(
            candidates, float(payload.total_fund_ton)
        )
        if not allocations:
            return {
                "success": True,
                "season_key": season_key,
                "sender_configured": bool(TON_PAYOUT_SENDER_URL),
                "queued": 0,
                "submitted": 0,
                "failed": 0,
                "created": 0,
                "skipped_without_wallet": skipped_without_wallet,
                "skipped_without_payout": skipped_without_payout,
                "skipped_locked": 0,
                "total_fund_ton": float(payload.total_fund_ton),
                "total_fund_nano": total_fund_nano,
            }

        dry_run_rows = []
        for item in candidates:
            amount_nano = int(allocations.get(int(item["user_id"]), 0))
            if amount_nano <= 0:
                continue
            dry_run_rows.append(
                {
                    "user_id": int(item["user_id"]),
                    "rank": int(item["rank"]),
                    "league": str(item["league"]),
                    "wallet_address": str(item["wallet_address"]),
                    "ton_amount_nano": amount_nano,
                }
            )
        if payload.dry_run:
            dry_run_rows.sort(key=lambda row: (row["league"], row["rank"], row["user_id"]))
            return {
                "success": True,
                "season_key": season_key,
                "preview": True,
                "sender_configured": bool(TON_PAYOUT_SENDER_URL),
                "total_fund_ton": float(payload.total_fund_ton),
                "total_fund_nano": total_fund_nano,
                "queued": len(dry_run_rows),
                "skipped_without_wallet": skipped_without_wallet,
                "skipped_without_payout": skipped_without_payout,
                "payouts": dry_run_rows,
            }

        created = 0
        queued = 0
        skipped_locked = 0
        queued_payloads: list[dict] = []
        queued_user_ids: list[int] = []
        async with AsyncSessionLocal() as session:
            existing_result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key
                )
            )
            existing_rows = {
                int(row.user_id): row for row in existing_result.scalars().all()
            }
            for item in candidates:
                user_id = int(item["user_id"])
                ton_amount_nano = int(allocations.get(user_id) or 0)
                if ton_amount_nano <= 0:
                    continue
                row = existing_rows.get(user_id)
                if row is None:
                    row = WeeklyTournamentTonPayout(
                        season_key=season_key,
                        user_id=user_id,
                        username=item.get("username"),
                        league=item.get("league") or "bronze",
                        rank=int(item.get("rank") or 0),
                        wallet_address=item["wallet_address"],
                        payout_cents=int(item.get("payout_cents") or 0),
                        ton_amount_nano=ton_amount_nano,
                        ton_price_usd_micros=0,
                        status="queued",
                        note=(payload.note or "").strip() or None,
                    )
                    session.add(row)
                    existing_rows[user_id] = row
                    created += 1
                else:
                    existing_status = str(row.status or "").strip().lower()
                    if existing_status not in {"", "queued", "failed", "cancelled"}:
                        skipped_locked += 1
                        continue
                    row.username = item.get("username")
                    row.league = item.get("league") or row.league
                    row.rank = int(item.get("rank") or row.rank or 0)
                    row.wallet_address = item["wallet_address"]
                    row.payout_cents = int(item.get("payout_cents") or 0)
                    row.ton_amount_nano = ton_amount_nano
                    row.ton_price_usd_micros = 0
                    row.status = "queued"
                    row.tx_hash = None
                    row.note = (payload.note or "").strip() or None
                    row.updated_at = datetime.utcnow()
                queued += 1
                queued_user_ids.append(user_id)
                queued_payloads.append(
                    {
                        "user_id": user_id,
                        "league": item.get("league"),
                        "rank": int(item.get("rank") or 0),
                        "wallet_address": item["wallet_address"],
                        "ton_amount_nano": ton_amount_nano,
                    }
                )
            await session.commit()

        submitted = 0
        failed = 0
        sender_error = None
        if TON_PAYOUT_SENDER_URL and queued_payloads:
            try:
                sender_results = await send_ton_payouts_with_sender_service(
                    season_key=season_key,
                    payouts=queued_payloads,
                    note=payload.note,
                )
                if sender_results:
                    async with AsyncSessionLocal() as session:
                        rows_result = await session.execute(
                            select(WeeklyTournamentTonPayout).where(
                                WeeklyTournamentTonPayout.season_key == season_key,
                                WeeklyTournamentTonPayout.user_id.in_(queued_user_ids),
                            )
                        )
                        rows = rows_result.scalars().all()
                        for row in rows:
                            user_id = int(row.user_id)
                            result = sender_results.get(user_id)
                            if not result:
                                continue
                            ok = bool(result.get("ok"))
                            tx_hash = str(result.get("tx_hash") or "").strip() or None
                            error_text = str(result.get("error") or "").strip()
                            row.status = "submitted" if ok else "failed"
                            row.tx_hash = tx_hash
                            row.note = (
                                " | ".join(
                                    part
                                    for part in [row.note or "", error_text]
                                    if part
                                )
                                or row.note
                            )
                            row.updated_at = datetime.utcnow()
                            if ok:
                                submitted += 1
                            else:
                                failed += 1
                        await session.commit()
            except httpx.HTTPError as e:
                sender_error = str(e)
                logger.error(f"TON sender service error: {e}")

        if not TON_PAYOUT_SENDER_URL:
            sender_error = (
                "TON_PAYOUT_SENDER_URL is not configured, payouts remain in queued status"
            )

        return {
            "success": True,
            "season_key": season_key,
            "sender_configured": bool(TON_PAYOUT_SENDER_URL),
            "sender_error": sender_error,
            "total_fund_ton": float(payload.total_fund_ton),
            "total_fund_nano": total_fund_nano,
            "created": created,
            "queued": queued,
            "submitted": submitted,
            "failed": failed,
            "skipped_without_wallet": skipped_without_wallet,
            "skipped_without_payout": skipped_without_payout,
            "skipped_locked": skipped_locked,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_send_ton_payouts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payout-status")
async def admin_update_ton_payout_status(
    season_key: str, payload: AdminTonPayoutStatusUpdateRequest, request: Request
):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key,
                    WeeklyTournamentTonPayout.user_id == payload.user_id,
                )
            )
            row = result.scalar_one_or_none()
            if not row:
                raise HTTPException(status_code=404, detail="TON payout row not found")
            row.status = (payload.status or "queued").strip().lower()
            row.tx_hash = (payload.tx_hash or "").strip() or None
            row.note = (payload.note or "").strip() or None
            row.updated_at = datetime.utcnow()
            await session.commit()
            return {
                "success": True,
                "season_key": season_key,
                "user_id": payload.user_id,
                "status": row.status,
                "tx_hash": row.tx_hash,
                "note": row.note,
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_ton_payout_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payout-status/bulk")
async def admin_update_ton_payout_status_bulk(
    season_key: str,
    payload: AdminTonPayoutBulkStatusUpdateRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        user_ids = sorted(
            {int(user_id) for user_id in (payload.user_ids or []) if int(user_id) > 0}
        )
        if not user_ids:
            raise HTTPException(status_code=400, detail="user_ids are required")
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key,
                    WeeklyTournamentTonPayout.user_id.in_(user_ids),
                )
            )
            rows = result.scalars().all()
            if not rows:
                raise HTTPException(status_code=404, detail="TON payout rows not found")
            status = (payload.status or "queued").strip().lower()
            tx_hash = (payload.tx_hash or "").strip() or None
            note = (payload.note or "").strip() or None
            updated_user_ids = []
            for row in rows:
                row.status = status
                row.tx_hash = tx_hash
                row.note = note
                row.updated_at = datetime.utcnow()
                updated_user_ids.append(int(row.user_id))
            await session.commit()
        return {
            "success": True,
            "season_key": season_key,
            "status": status,
            "updated_count": len(updated_user_ids),
            "updated_user_ids": updated_user_ids,
            "tx_hash": tx_hash,
            "note": note,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_ton_payout_status_bulk: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payouts/confirm")
async def admin_confirm_ton_payouts(
    season_key: str,
    payload: AdminTonPayoutConfirmRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        sender_wallet_address = (payload.sender_wallet_address or "").strip()
        if not is_valid_ton_wallet_address(sender_wallet_address):
            raise HTTPException(status_code=400, detail="Invalid sender_wallet_address")
        lookback_minutes = max(5, int(payload.lookback_minutes or 180))
        requested_user_ids = sorted(
            {int(user_id) for user_id in (payload.user_ids or []) if int(user_id) > 0}
        )
        async with AsyncSessionLocal() as session:
            query = select(WeeklyTournamentTonPayout).where(
                WeeklyTournamentTonPayout.season_key == season_key,
                WeeklyTournamentTonPayout.status.in_(["queued", "submitted"]),
            )
            if requested_user_ids:
                query = query.where(
                    WeeklyTournamentTonPayout.user_id.in_(requested_user_ids)
                )
            result = await session.execute(
                query.order_by(WeeklyTournamentTonPayout.rank.asc())
            )
            rows = result.scalars().all()
            if not rows:
                return {
                    "success": True,
                    "season_key": season_key,
                    "checked": 0,
                    "confirmed": 0,
                    "confirmed_user_ids": [],
                    "missing_user_ids": [],
                }
            recipient_accounts = sorted(
                {row.wallet_address for row in rows if row.wallet_address}
            )
            start_utime = int(
                (datetime.utcnow() - timedelta(minutes=lookback_minutes)).timestamp()
            )
            transactions = []
            for index in range(0, len(recipient_accounts), 50):
                transactions.extend(
                    await fetch_ton_transactions_for_accounts(
                        recipient_accounts[index : index + 50], start_utime
                    )
                )
            matched_rows, matched_user_ids = match_ton_queue_rows_to_transactions(
                rows, transactions, sender_wallet_address
            )
            confirmed_user_ids: list[int] = []
            tx_hash_by_user = {
                int(item["user_id"]): item.get("tx_hash") or None
                for item in matched_rows
            }
            confirmed_at_by_user = {
                int(item["user_id"]): item.get("confirmed_at") or 0
                for item in matched_rows
            }
            for row in rows:
                user_id = int(row.user_id)
                if user_id not in matched_user_ids:
                    continue
                row.status = "sent"
                row.tx_hash = tx_hash_by_user.get(user_id) or row.tx_hash
                confirmed_at = confirmed_at_by_user.get(user_id)
                note_suffix = ""
                if confirmed_at:
                    note_suffix = f"confirmed_at={datetime.utcfromtimestamp(int(confirmed_at)).isoformat()}Z"
                row.note = " | ".join(
                    part
                    for part in [row.note or "", "verified_on_chain", note_suffix]
                    if part
                )
                row.updated_at = datetime.utcnow()
                confirmed_user_ids.append(user_id)
            await session.commit()
        missing_user_ids = [
            int(row.user_id)
            for row in rows
            if int(row.user_id) not in set(confirmed_user_ids)
        ]
        return {
            "success": True,
            "season_key": season_key,
            "checked": len(rows),
            "confirmed": len(confirmed_user_ids),
            "confirmed_user_ids": confirmed_user_ids,
            "missing_user_ids": missing_user_ids,
        }
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        logger.error(f"TON verifier HTTP error: {e}")
        raise HTTPException(status_code=502, detail="Failed to verify TON transactions")
    except Exception as e:
        logger.error(f"Error in admin_confirm_ton_payouts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/fund")
async def admin_set_weekly_tournament_fund(
    season_key: str,
    payload: WeeklyTournamentFundRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        starts_at = datetime.strptime(season_key, "%Y-%m-%d")
        ends_at = starts_at + timedelta(days=7)
        async with AsyncSessionLocal() as session:
            season = await ensure_weekly_tournament_season(
                session, season_key, starts_at, ends_at
            )
            season.gross_ad_revenue_cents = int(payload.gross_ad_revenue_cents or 0)
            season.payout_fund_cents = int(payload.payout_fund_cents or 0)
            if season.status == "finalized":
                await recalculate_finalized_weekly_winner_payouts(
                    session,
                    season_key,
                    int(payload.payout_fund_cents or 0),
                )
            await session.commit()
        return {
            "success": True,
            "season_key": season_key,
            "gross_ad_revenue_cents": int(payload.gross_ad_revenue_cents or 0),
            "payout_fund_cents": int(payload.payout_fund_cents or 0),
            "league_splits": WEEKLY_LEAGUE_FUND_SPLITS,
            "rank_splits": {
                "top1": WEEKLY_TOP3_PAYOUT_SPLITS[1],
                "top2": WEEKLY_TOP3_PAYOUT_SPLITS[2],
                "top3": WEEKLY_TOP3_PAYOUT_SPLITS[3],
                "ranks_4_10": WEEKLY_RANGE_PAYOUT_SPLITS[0]["share"],
                "ranks_11_20": WEEKLY_RANGE_PAYOUT_SPLITS[1]["share"],
                "ranks_21_50": WEEKLY_RANGE_PAYOUT_SPLITS[2]["share"],
            },
        }
    except ValueError:
        raise HTTPException(
            status_code=400, detail="season_key must use YYYY-MM-DD format"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_set_weekly_tournament_fund: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/winner-stars")
async def admin_set_weekly_tournament_winner_stars(
    season_key: str,
    payload: AdminWinnerStarsUpdateRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            winner_result = await session.execute(
                select(WeeklyTournamentWinner).where(
                    WeeklyTournamentWinner.season_key == season_key,
                    WeeklyTournamentWinner.user_id == payload.user_id,
                )
            )
            winner = winner_result.scalar_one_or_none()
            if not winner:
                raise HTTPException(
                    status_code=404, detail="Winner not found for this season"
                )
            winner.stars_reward = int(payload.stars_reward or 0)
            await session.commit()
            return {
                "success": True,
                "season_key": season_key,
                "user_id": payload.user_id,
                "stars_reward": int(winner.stars_reward or 0),
                "league": winner.league,
                "rank": int(winner.rank or 0),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_set_weekly_tournament_winner_stars: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
