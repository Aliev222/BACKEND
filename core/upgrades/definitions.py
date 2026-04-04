from __future__ import annotations

from dataclasses import dataclass

from core.game_config import BASE_MAX_ENERGY, ENERGY_REGEN_SECONDS, GLOBAL_UPGRADE_PRICES


@dataclass(frozen=True)
class UpgradeValues:
    tap_power: int
    energy_regen: int
    max_energy: int
    profit_per_hour: int


BASE_TAP_POWER = 1
BASE_ENERGY_REGEN = ENERGY_REGEN_SECONDS
BASE_MAX_ENERGY_VALUE = BASE_MAX_ENERGY
GLOBAL_PRICES = tuple(int(v) for v in GLOBAL_UPGRADE_PRICES)
