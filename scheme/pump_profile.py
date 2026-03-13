from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class PumpProfile:
    t: List[float]
    rpm: List[float]

    @property
    def end_time(self) -> float:
        return self.t[-1] if self.t else 0.0
