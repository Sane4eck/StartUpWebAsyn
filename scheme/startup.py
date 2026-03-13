from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple


@dataclass
class StartupConfig:
    # --- Starter duty schedule
    starter_steps: List[Tuple[float, float]] = field(default_factory=lambda: [
        (0.0, 0.05),
        (500.0, 0.06),
        (700.0, 0.07),
        (900.0, 0.09),
        (1300.0, 0.10),
        (2000.0, 0.12),
        (3000.0, 0.14),
        (4000.0, 0.15),
        (5000.0, 0.16),
    ])
    starter_step_hold_s: float = 0.2

    # --- Starter -> FuelRamp (by starter RPM)
    to_fuelramp_starter_rpm: float = 1000.0
    to_fuelramp_hold_s: float = 0.2
    starter_timeout_s: float = 180.0

    # --- Valve close + Starter off (by starter RPM) in FuelRamp
    valve_close_rpm: float = 4000.0
    starter_off_rpm: float = 6000.0
    valve_close_hold_s: float = 0.2
    starter_off_hold_s: float = 0.2

    # --- FuelRamp -> Running (by starter RPM) + freeze pump
    fuelramp_timeout_s: float = 300.0
    to_running_starter_rpm: float = 5000.0
    to_running_hold_s: float = 0.2

    # --- Valve behavior in FuelRamp (until valve_close_rpm reached)
    valve_boost_v: float = 18.0
    valve_boost_i: float = 20.0
    valve_boost_s: float = 2.0
    valve_hold_v: float = 5.0
    valve_hold_i: float = 20.0
