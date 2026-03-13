from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class CycleInputs:
    now: float
    t: float  # time since session start
    state_t: float  # time since current state entered
    pump_rpm: float
    starter_rpm: float
    pump_current: float
    starter_current: float
    psu_v_out: float
    psu_i_out: float
    psu_output: bool


@dataclass
class CycleTargets:
    # IMPORTANT: pump = RPM only, starter = DUTY only
    pump: Dict[str, Any] = field(default_factory=lambda: {"mode": "rpm", "value": 0.0})
    starter: Dict[str, Any] = field(default_factory=lambda: {"mode": "duty", "value": 0.0})
    psu: Dict[str, Any] = field(default_factory=lambda: {"v": 0.0, "i": 0.0, "out": False})
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Transition:
    cond: Callable[[CycleInputs], bool]
    next_state: str
    reason: Optional[str] = None


@dataclass
class State:
    name: str
    on_enter: Optional[Callable[[CycleInputs, CycleTargets], None]] = None
    on_tick: Optional[Callable[[CycleInputs, CycleTargets], None]] = None
    transitions: List[Transition] = field(default_factory=list)
    timeout_s: Optional[float] = None
    on_timeout: Optional[str] = None
    timeout_reason: Optional[str] = None
    terminal: bool = False
