from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(slots=True)
class VescTarget:
    mode: str = "rpm"  # "rpm" | "duty"
    value: float = 0.0


@dataclass(slots=True)
class VescSnapshot:
    connected: bool = False
    port: str = ""
    pole_pairs: int = 7

    rpm_mech: float = 0.0
    duty: float = 0.0
    current_motor: float = 0.0

    target_mode: str = "rpm"
    target_value: float = 0.0

    ts: float = 0.0
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PsuTarget:
    v: float = 0.0
    i: float = 0.0
    out: bool = False


@dataclass(slots=True)
class PsuSnapshot:
    connected: bool = False
    port: str = ""

    v_set: float = 0.0
    i_set: float = 0.0
    v_out: float = 0.0
    i_out: float = 0.0
    p_out: float = 0.0
    v_in: float = 0.0
    output: bool = False

    target_v: float = 0.0
    target_i: float = 0.0
    target_out: bool = False

    ts: float = 0.0
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)
