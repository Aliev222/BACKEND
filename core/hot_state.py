from dataclasses import dataclass


@dataclass(frozen=True)
class UserHotState:
    coins: int
    energy: int
    last_energy_ts: float
    tap_power: int
    energy_regen: int
    max_energy: int
    click_streak: int
    suspicion_score: int
    version: int

