from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from redis.asyncio import Redis
from redis.exceptions import ResponseError


@dataclass(frozen=True)
class ClickResult:
    status: int
    new_coins: int
    new_energy: int
    effective_clicks: int
    gained: int
    allowed_clicks: int
    referral_bonus: int
    suspicion_score: int
    tap_value: int
    profit_per_hour: int
    coin_per_tap: int
    max_energy: int
    mega_boost_active: bool
    ghost_boost_active: bool
    daily_infinite_energy_active: bool
    task_tap_boost_active: bool
    task_tap_boost_multiplier: int
    ghost_boost_multiplier: int
    energy_regen: int
    click_streak: int
    version: int


_LUA_PATH = Path(__file__).resolve().parent / "lua" / "click.lua"
_CLICK_LUA = _LUA_PATH.read_text(encoding="utf-8-sig")
_CLICK_LUA_SHA: str | None = None


async def process_click_lua(
    redis: Redis,
    user_id: int,
    clicks: int,
    batch_id: str,
    *,
    keys: Sequence[str],
    args: Sequence[str],
) -> ClickResult:
    global _CLICK_LUA_SHA

    if _CLICK_LUA_SHA is None:
        _CLICK_LUA_SHA = await redis.script_load(_CLICK_LUA)

    try:
        raw = await redis.evalsha(_CLICK_LUA_SHA, len(keys), *keys, *args)
    except ResponseError as exc:
        # Redis may evict script cache after restart; load and retry once.
        if "NOSCRIPT" not in str(exc).upper():
            raise
        _CLICK_LUA_SHA = await redis.script_load(_CLICK_LUA)
        raw = await redis.evalsha(_CLICK_LUA_SHA, len(keys), *keys, *args)

    values = list(raw or [])
    while len(values) < 21:
        values.append(0)
    return ClickResult(
        status=int(values[0]),
        new_coins=int(values[1]),
        new_energy=int(values[2]),
        effective_clicks=int(values[3]),
        gained=int(values[4]),
        allowed_clicks=int(values[5]),
        referral_bonus=int(values[6]),
        suspicion_score=int(values[7]),
        tap_value=int(values[8]),
        profit_per_hour=int(values[9]),
        coin_per_tap=int(values[10]),
        max_energy=int(values[11]),
        mega_boost_active=int(values[12]) == 1,
        ghost_boost_active=int(values[13]) == 1,
        daily_infinite_energy_active=int(values[14]) == 1,
        task_tap_boost_active=int(values[15]) == 1,
        task_tap_boost_multiplier=int(values[16]),
        ghost_boost_multiplier=int(values[17]),
        energy_regen=int(values[18]),
        click_streak=int(values[19]),
        version=int(values[20]),
    )
