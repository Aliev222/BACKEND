from __future__ import annotations

from core.game_logic import get_hour_value, get_max_energy, get_tap_value

from .definitions import BASE_ENERGY_REGEN, GLOBAL_PRICES, UpgradeValues


def calc_upgrade_value(level: int) -> UpgradeValues:
    lvl = max(0, int(level))
    return UpgradeValues(
        tap_power=get_tap_value(lvl),
        energy_regen=BASE_ENERGY_REGEN,
        max_energy=get_max_energy(lvl),
        profit_per_hour=get_hour_value(lvl),
    )


def calc_upgrade_price(level: int, prices: list[int] | tuple[int, ...] | None = None) -> int:
    values = tuple(int(v) for v in (prices or GLOBAL_PRICES))
    idx = max(0, int(level))
    if idx >= len(values):
        return 0
    return int(values[idx])
