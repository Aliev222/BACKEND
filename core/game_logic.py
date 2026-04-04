from datetime import datetime

from core.game_config import (
    BASE_MAX_ENERGY,
    CLICK_BURST_ALLOWANCE,
    CLICK_TIME_ACCUMULATION_CAP_SECONDS,
    INITIAL_CLICK_BATCH_ALLOWANCE,
    ENERGY_REGEN_SECONDS,
    MAX_CLICK_BATCH_SIZE,
    MAX_REAL_CLICKS_PER_SECOND,
)


def mask_username(username):
    if not username:
        return "Player"

    username = str(username)
    if len(username) <= 4:
        return username

    first_two = username[:2]
    last_two = username[-2:]
    middle_len = len(username) - 4
    return f"{first_two}{'*' * min(middle_len, 3)}{last_two}"


def normalize_dt(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def get_allowed_clicks(
    user: dict,
    now: datetime,
    requested_clicks: int,
    *,
    last_click_at: datetime | None = None,
) -> int:
    baseline = last_click_at or normalize_dt(user.get("last_energy_update"))

    if not baseline:
        return min(requested_clicks, INITIAL_CLICK_BATCH_ALLOWANCE, MAX_CLICK_BATCH_SIZE)

    elapsed = max(0.0, (now - baseline).total_seconds())
    elapsed = min(elapsed, CLICK_TIME_ACCUMULATION_CAP_SECONDS)
    allowed_by_time = int(elapsed * MAX_REAL_CLICKS_PER_SECOND) + CLICK_BURST_ALLOWANCE
    allowed = max(1, min(allowed_by_time, MAX_CLICK_BATCH_SIZE))
    return min(requested_clicks, allowed)


def get_tap_value(level: int) -> int:
    level = max(0, int(level))
    return 1 + level


def get_tap(level: int, rebirth_count: int) -> int:
    lvl = max(0, int(level))
    rebirths = max(0, int(rebirth_count))
    return 1 + (lvl * (1 + rebirths))


def get_tap_value_with_rebirth(level: int, rebirth_count: int) -> int:
    return get_tap(level, rebirth_count)


def get_hour_value(level: int) -> int:
    level = max(0, int(level))
    return 100 + (level * 35) + (level * level * 7)


def get_profit_per_hour(level: int) -> int:
    return get_hour_value(level)


def get_max_energy(level: int) -> int:
    level = max(0, int(level))
    return min(1000, BASE_MAX_ENERGY + level * 5)


def get_legacy_level_fallback(user: dict) -> int:
    return max(
        int(user.get("multitap_level", 0) or 0),
        int(user.get("profit_level", 0) or 0),
        int(user.get("energy_level", 0) or 0),
    )


def resolve_progression_level(user: dict) -> int:
    if user is None:
        return 0
    level_raw = user.get("level", None)
    legacy_max = get_legacy_level_fallback(user)
    if level_raw is None:
        return legacy_max
    try:
        level = int(level_raw)
    except Exception:
        return legacy_max
    # Safety migration fallback for pre-backfill rows.
    if level <= 0 and legacy_max > 0:
        return legacy_max
    return max(0, level)


def resolve_max_energy(user: dict) -> int:
    return get_max_energy(resolve_progression_level(user))


def calculate_current_energy(user: dict, now: datetime | None = None) -> int:
    now = now or datetime.utcnow()

    stored_energy = int(user.get("energy", 0))
    max_energy = resolve_max_energy(user)
    last_update = normalize_dt(user.get("last_energy_update"))

    if stored_energy >= max_energy:
        return max_energy

    if not last_update:
        return min(stored_energy, max_energy)

    seconds_passed = max(0, int((now - last_update).total_seconds()))
    gained = seconds_passed // ENERGY_REGEN_SECONDS

    return min(max_energy, stored_energy + gained)


def build_energy_payload(user: dict, now: datetime | None = None) -> dict:
    now = now or datetime.utcnow()
    max_energy = resolve_max_energy(user)
    current_energy = calculate_current_energy(user, now)

    return {
        "energy": current_energy,
        "max_energy": max_energy,
        "regen_seconds": ENERGY_REGEN_SECONDS,
        "server_time": now.isoformat()
    }
