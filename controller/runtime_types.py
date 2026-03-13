from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Dict, Literal, Optional


WorkerName = Literal["pump", "starter", "psu", "logger"]
EventKind = Literal["snapshot", "state", "error", "ack"]


def mono_ts() -> float:
    return time.monotonic()


def obj_to_dict(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {k: obj_to_dict(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [obj_to_dict(v) for v in value]
    return value


@dataclass(slots=True)
class VescTarget:
    mode: str = "rpm"   # "rpm" | "duty"
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

    ts: float = field(default_factory=mono_ts)
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

    ts: float = field(default_factory=mono_ts)
    error: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class LoggerState:
    opened: bool = False
    log_path: str = ""
    ts: float = field(default_factory=mono_ts)
    error: Optional[str] = None


@dataclass(slots=True)
class CommandEnvelope:
    worker: WorkerName
    kind: str
    payload: Dict[str, Any] = field(default_factory=dict)
    seq: int = 0
    ts: float = field(default_factory=mono_ts)


@dataclass(slots=True)
class EventEnvelope:
    worker: WorkerName
    kind: EventKind
    payload: Dict[str, Any] = field(default_factory=dict)
    seq: int = 0
    ts: float = field(default_factory=mono_ts)


def make_command(worker: WorkerName, kind: str, payload: Optional[Dict[str, Any]] = None, seq: int = 0) -> Dict[str, Any]:
    return obj_to_dict(
        CommandEnvelope(
            worker=worker,
            kind=kind,
            payload=payload or {},
            seq=seq,
        )
    )


def make_event(worker: WorkerName, kind: EventKind, payload: Optional[Dict[str, Any]] = None, seq: int = 0) -> Dict[str, Any]:
    return obj_to_dict(
        EventEnvelope(
            worker=worker,
            kind=kind,
            payload=payload or {},
            seq=seq,
        )
    )
