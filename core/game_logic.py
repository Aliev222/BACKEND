from datetime import datetime

from core.game_config import (
    BASE_MAX_ENERGY,
    CLICK_BURST_ALLOWANCE,
    ENERGY_REGEN_SECONDS,
    HOUR_VALUES,
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


def get_allowed_clicks(user: dict, now: datetime, requested_clicks: int) -> int:
    last_update = normalize_dt(user.get("last_energy_update"))

    if not last_update:
        return min(requested_clicks, 60, MAX_CLICK_BATCH_SIZE)

    elapsed = max(0.0, (now - last_update).total_seconds())
    allowed_by_time = int(elapsed * MAX_REAL_CLICKS_PER_SECOND) + CLICK_BURST_ALLOWANCE
    allowed = max(1, min(allowed_by_time, MAX_CLICK_BATCH_SIZE))
    return min(requested_clicks, allowed)


def get_tap_value(level: int) -> int:
    return 1 + level


def get_hour_value(level: int) -> int:
    return HOUR_VALUES[min(level, len(HOUR_VALUES) - 1)]


def get_max_energy(level: int) -> int:
    return min(1000, BASE_MAX_ENERGY + level * 5)


def calculate_current_energy(user: dict, now: datetime | None = None) -> int:
    now = now or datetime.utcnow()

    stored_energy = int(user.get("energy", 0))
    max_energy = int(user.get("max_energy", 500))
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
    max_energy = int(user.get("max_energy", BASE_MAX_ENERGY))
    current_energy = calculate_current_energy(user, now)

    return {
        "energy": current_energy,
        "max_energy": max_energy,
        "regen_seconds": ENERGY_REGEN_SECONDS,
        "server_time": now.isoformat()
    }
