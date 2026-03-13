from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class VESCValues:
    rpm_mech: float = 0.0
    duty: float = 0.0
    current_motor: float = 0.0
    raw: Dict[str, Any] = field(default_factory=dict)
