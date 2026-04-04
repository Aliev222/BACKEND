from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from redis.asyncio import Redis


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


_LUA_PATH = Path(__file__).resolve().parent / "lua" / "click.lua"
_CLICK_LUA = _LUA_PATH.read_text(encoding="utf-8")


async def process_click_lua(
    redis: Redis,
    user_id: int,
    clicks: int,
    batch_id: str,
    *,
    keys: Sequence[str],
    args: Sequence[str],
) -> ClickResult:
    raw = await redis.eval(_CLICK_LUA, len(keys), *keys, *args)
    values = list(raw or [])
    while len(values) < 8:
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
    )
