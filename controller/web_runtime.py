from __future__ import annotations

import multiprocessing as mp
import queue
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

from serial.tools import list_ports

from controller.cycle_fsm import CycleFSM
from controller.cyclogram_startup import build_cooling_fsm, build_startup_fsm
from controller.pump_profile import interp_profile, load_pump_profile_xlsx
from controller.runtime_types import (
    PsuSnapshot,
    PsuTarget,
    VescSnapshot,
    VescTarget,
    make_command,
)
from controller.workers import logger_worker_main, psu_worker_main, vesc_worker_main
from scheme.cycle import CycleInputs
from scheme.pump_profile import PumpProfile
from scheme.startup import StartupConfig


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _stage_public_name(name: str) -> str:
    return (name or "idle").strip().lower()


class _ProcHandle:
    def __init__(self, ctx: mp.context.BaseContext, name: str, target, args: tuple[Any, ...]):
        self.ctx = ctx
        self.name = name
        self.target = target
        self.args = args

        self.cmd_q = ctx.Queue()
        self.evt_q = ctx.Queue()
        self.stop_evt = ctx.Event()
        self.proc: mp.Process | None = None

    def start(self) -> None:
        if self.proc is not None and self.proc.is_alive():
            return

        self.stop_evt.clear()
        self.proc = self.ctx.Process(
            target=self.target,
            args=(self.name, self.cmd_q, self.evt_q, self.stop_evt, *self.args),
            name=f"{self.name}-proc",
            daemon=True,
        )
        self.proc.start()

    def stop(self) -> None:
        self.stop_evt.set()
        try:
            self.cmd_q.put_nowait({"kind": "__stop__", "payload": {}})
        except Exception:
            pass

        if self.proc is not None and self.proc.is_alive():
            self.proc.join(timeout=2.0)

        if self.proc is not None and self.proc.is_alive():
            self.proc.terminate()
            self.proc.join(timeout=1.0)

    def post(self, msg: dict[str, Any]) -> None:
        self.cmd_q.put(msg)

    def drain_events(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while True:
            try:
                out.append(self.evt_q.get_nowait())
            except queue.Empty:
                break
        return out


class WebControllerRuntime:
    def __init__(
        self,
        publish: Callable[[str, Any], None] | None = None,
        dt: float = 0.05,
    ):
        self.publish = publish or (lambda event, payload: None)

        self.dt = float(dt)
        self.ui_hz = 5.0
        self.log_hz = 5.0

        self._ui_dt = 1.0 / self.ui_hz
        self._log_dt = 1.0 / self.log_hz

        self.stale_vesc_s = 1.0
        self.stale_psu_s = 2.0

        self._lock = threading.RLock()
        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

        self._ctx = mp.get_context("spawn")
        self._pump_proc = _ProcHandle(self._ctx, "pump", vesc_worker_main, (0.02, 0.01, 115200))
        self._starter_proc = _ProcHandle(self._ctx, "starter", vesc_worker_main, (0.02, 0.01, 115200))
        self._psu_proc = _ProcHandle(self._ctx, "psu", psu_worker_main, (0.02, 0.5, 0.2, 115200, 0.2, 1))
        self._logger_proc = _ProcHandle(self._ctx, "logger", logger_worker_main, ("logs",))

        self.startup_cfg = StartupConfig()

        self._t0 = time.monotonic()
        self._last_ui = 0.0
        self._last_log = 0.0

        self._stage = "idle"
        self._last_error = ""
        self._logger_path = ""

        self._pump_snap = VescSnapshot()
        self._starter_snap = VescSnapshot()
        self._psu_snap = PsuSnapshot()

        self.pump_target = {"mode": "rpm", "value": 0.0}
        self.starter_target = {"mode": "duty", "value": 0.0}
        self.psu_target = {"v": 0.0, "i": 0.0, "out": False}

        self._fsm: CycleFSM | None = None
        self._fsm_prev_state: str | None = None

        self._pump_prof_active = False
        self._pump_prof_path = ""
        self._pump_prof: PumpProfile | None = None
        self._pump_prof_t0 = 0.0

        self._run_pump_profile: PumpProfile | None = None
        self._run_starter_profile: PumpProfile = PumpProfile([], [])

        self._valve_macro_active = False
        self._valve_macro_t0 = 0.0

        self._seq = 0

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_evt.clear()

        self._pump_proc.start()
        self._starter_proc.start()
        self._psu_proc.start()
        self._logger_proc.start()

        self._thread = threading.Thread(
            target=self._run_loop,
            name="startup-web-runtime",
            daemon=True,
        )
        self._thread.start()

    def shutdown(self) -> None:
        self._stop_evt.set()

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)

        self._safe_zero_outputs()

        self._pump_proc.stop()
        self._starter_proc.stop()
        self._psu_proc.stop()
        self._logger_proc.stop()

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def list_ports(self) -> list[str]:
        items = []
        for p in list_ports.comports():
            if p.device:
                items.append(str(p.device))
        return sorted(set(items))

    def snapshot(self) -> dict[str, Any]:
        return {
            "ports": self.list_ports(),
            "status": self._build_status(),
            "sample": self._build_sample(),
            "last_error": self._last_error,
        }

    def cmd_connect_pump(self, port: str) -> None:
        port = str(port or "").strip()
        if not port:
            raise ValueError("Pump COM port is empty")
        self._post(self._pump_proc, "pump", "connect", {"port": port})

    def cmd_disconnect_pump(self) -> None:
        self._post(self._pump_proc, "pump", "disconnect", {})

    def cmd_connect_starter(self, port: str) -> None:
        port = str(port or "").strip()
        if not port:
            raise ValueError("Starter COM port is empty")
        self._post(self._starter_proc, "starter", "connect", {"port": port})

    def cmd_disconnect_starter(self) -> None:
        self._post(self._starter_proc, "starter", "disconnect", {})

    def cmd_connect_psu(self, port: str) -> None:
        port = str(port or "").strip()
        if not port:
            raise ValueError("PSU COM port is empty")
        self._post(self._psu_proc, "psu", "connect", {"port": port})

    def cmd_disconnect_psu(self) -> None:
        self._post(self._psu_proc, "psu", "disconnect", {})

    def cmd_set_pole_pairs_pump(self, pole_pairs: int) -> None:
        self._post(
            self._pump_proc,
            "pump",
            "set_pole_pairs",
            {"pole_pairs": max(1, int(pole_pairs))},
        )

    def cmd_set_pole_pairs_starter(self, pole_pairs: int) -> None:
        self._post(
            self._starter_proc,
            "starter",
            "set_pole_pairs",
            {"pole_pairs": max(1, int(pole_pairs))},
        )

    def cmd_ready(self, prefix: str = "manual") -> None:
        now = time.monotonic()
        with self._lock:
            self._fsm = None
            self._fsm_prev_state = None
            self._stop_pump_profile_internal()
            self._valve_macro_active = False

            self._t0 = now
            self._stage = "ready"
            self._last_error = ""

            self.pump_target = {"mode": "rpm", "value": 0.0}
            self.starter_target = {"mode": "duty", "value": 0.0}
            self.psu_target = {"v": 0.0, "i": 0.0, "out": False}

        self._post(self._logger_proc, "logger", "open", {"prefix": prefix or "manual"})
        self._publish("status", {**self._build_status(), "ready": True, "reset_plot": True})

    def cmd_update_reset(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._t0 = now
            self._fsm = None
            self._fsm_prev_state = None
            self._stop_pump_profile_internal()
            self._valve_macro_active = False
            self._stage = "idle"
        self._publish("status", {**self._build_status(), "reset_plot": True})

    def cmd_run_cycle(self) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            self._valve_macro_active = False

            if self._run_pump_profile is None or not self._run_pump_profile.t:
                raise ValueError("Спочатку завантаж pump profile (.xlsx)")

            now = time.monotonic()
            inp = self._make_inputs(now)

            self._fsm = build_startup_fsm(
                self._run_pump_profile,
                self._run_starter_profile,
                self.startup_cfg,
            )
            self._fsm_prev_state = None
            self._fsm.start(inp)
            self._stage = _stage_public_name(self._fsm.state)

    def cmd_cooling_cycle(self, value: float) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            self._valve_macro_active = False

            now = time.monotonic()
            inp = self._make_inputs(now)

            self._fsm = build_cooling_fsm(float(value))
            self._fsm_prev_state = None
            self._fsm.start(inp)
            self._stage = _stage_public_name(self._fsm.state)

    def cmd_stop_all(self) -> None:
        with self._lock:
            self._fsm = None
            self._fsm_prev_state = None
            self._stop_pump_profile_internal()
            self._valve_macro_active = False
            self._stage = "stop"

            self.pump_target = {"mode": "rpm", "value": 0.0}
            self.starter_target = {"mode": "duty", "value": 0.0}
            self.psu_target = {
                "v": float(self.psu_target.get("v", 0.0)),
                "i": float(self.psu_target.get("i", 0.0)),
                "out": False,
            }

    def cmd_valve_on(self) -> None:
        with self._lock:
            if not self._psu_snap.connected:
                raise ValueError("Valve: PSU not connected")

            self._fsm = None
            self._fsm_prev_state = None
            self._stop_pump_profile_internal()

            self._valve_macro_active = True
            self._valve_macro_t0 = time.monotonic()
            self._stage = "manual"

    def cmd_valve_off(self) -> None:
        with self._lock:
            self._valve_macro_active = False
            self._stage = "manual"
            self.psu_target = {"v": 0.0, "i": 0.0, "out": False}

    def cmd_start_pump_profile(self, path: str) -> None:
        raw = str(path or "").strip()
        if not raw:
            raise ValueError("Pump profile path is empty")
        if not Path(raw).exists():
            raise ValueError(f"Pump profile file not found: {raw}")

        prof = load_pump_profile_xlsx(raw, sheet_name=None)
        if not prof.t:
            raise ValueError("Pump profile is empty")

        now = time.monotonic()
        with self._lock:
            self._fsm = None
            self._fsm_prev_state = None

            self._pump_prof = prof
            self._run_pump_profile = prof

            self._pump_prof_active = True
            self._pump_prof_path = raw
            self._pump_prof_t0 = now
            self._stage = "pump_profile"

    def cmd_stop_pump_profile(self) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            if self._stage == "pump_profile":
                self._stage = "manual"
            self.pump_target = {"mode": "rpm", "value": 0.0}

    def cmd_set_pump_rpm(self, value: float) -> None:
        with self._lock:
            self._stop_pump_profile_internal()

            if self._fsm is not None and self._fsm.state == "Running":
                self.pump_target = {"mode": "rpm", "value": float(value)}
                return

            self._fsm = None
            self._fsm_prev_state = None
            self._stage = "manual"
            self.pump_target = {"mode": "rpm", "value": float(value)}

    def cmd_set_pump_duty(self, value: float) -> None:
        with self._lock:
            self._stop_pump_profile_internal()

            if self._fsm is not None and self._fsm.state == "Running":
                self.pump_target = {"mode": "duty", "value": _clamp01(value)}
                return

            self._fsm = None
            self._fsm_prev_state = None
            self._stage = "manual"
            self.pump_target = {"mode": "duty", "value": _clamp01(value)}

    def cmd_set_starter_rpm(self, value: float) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            self._fsm = None
            self._fsm_prev_state = None
            self._stage = "manual"
            self.starter_target = {"mode": "rpm", "value": float(value)}

    def cmd_set_starter_duty(self, value: float) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            self._fsm = None
            self._fsm_prev_state = None
            self._stage = "manual"
            self.starter_target = {"mode": "duty", "value": _clamp01(value)}

    def cmd_psu_set_vi(self, v: float, i: float) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            self._fsm = None
            self._fsm_prev_state = None
            self._valve_macro_active = False
            self._stage = "manual"
            self.psu_target = {
                "v": float(v),
                "i": float(i),
                "out": bool(self.psu_target.get("out", False)),
            }

    def cmd_psu_output(self, value: bool) -> None:
        with self._lock:
            self._stop_pump_profile_internal()
            self._fsm = None
            self._fsm_prev_state = None
            self._valve_macro_active = False
            self._stage = "manual"
            self.psu_target = {
                "v": float(self.psu_target.get("v", 0.0)),
                "i": float(self.psu_target.get("i", 0.0)),
                "out": bool(value),
            }

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _post(self, proc: _ProcHandle, worker: str, kind: str, payload: dict[str, Any]) -> None:
        proc.post(make_command(worker=worker, kind=kind, payload=payload, seq=self._next_seq()))

    def _publish(self, event: str, payload: Any) -> None:
        try:
            self.publish(event, payload)
        except Exception:
            pass

    def _emit_error(self, text: str) -> None:
        self._last_error = str(text)
        self._publish("error", self._last_error)

    def _stop_pump_profile_internal(self) -> None:
        self._pump_prof_active = False
        self._pump_prof_path = ""
        self._pump_prof_t0 = 0.0

    def _make_inputs(self, now: float) -> CycleInputs:
        state_t = self._fsm.state_time(now) if self._fsm is not None else 0.0
        return CycleInputs(
            now=now,
            t=now - self._t0,
            state_t=state_t,
            pump_rpm=float(self._pump_snap.rpm_mech),
            starter_rpm=float(self._starter_snap.rpm_mech),
            pump_current=float(self._pump_snap.current_motor),
            starter_current=float(self._starter_snap.current_motor),
            psu_v_out=float(self._psu_snap.v_out),
            psu_i_out=float(self._psu_snap.i_out),
            psu_output=bool(self._psu_snap.output),
        )

    def _set_targets_from_fsm(self, targets) -> None:
        old_state = self._fsm_prev_state or self._fsm.state
        new_state = self._fsm.state

        self._stage = _stage_public_name(new_state)

        if new_state != old_state:
            reason = targets.meta.get("transition_reason")
            if new_state == "Fault" and reason:
                self._emit_error(str(reason))

        apply_pump = new_state != "Running"
        if (not apply_pump) and (old_state != "Running") and (new_state == "Running"):
            if targets.meta.get("apply_pump_once_on_running_entry"):
                apply_pump = True

        if apply_pump:
            self.pump_target = dict(targets.pump)

        self.starter_target = dict(targets.starter)
        self.psu_target = dict(targets.psu)
        self._fsm_prev_state = new_state

    def _safe_zero_outputs(self) -> None:
        try:
            self._post(self._pump_proc, "pump", "set_target", {"mode": "rpm", "value": 0.0})
            self._post(self._starter_proc, "starter", "set_target", {"mode": "duty", "value": 0.0})
            self._post(
                self._psu_proc,
                "psu",
                "set_target",
                {
                    "v": float(self.psu_target.get("v", 0.0)),
                    "i": float(self.psu_target.get("i", 0.0)),
                    "out": False,
                },
            )
            time.sleep(0.1)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # worker event handling
    # ------------------------------------------------------------------

    def _drain_worker_events(self) -> None:
        for proc in (self._pump_proc, self._starter_proc, self._psu_proc, self._logger_proc):
            for evt in proc.drain_events():
                worker = str(evt.get("worker", ""))
                kind = str(evt.get("kind", ""))
                payload = evt.get("payload", {}) or {}

                if worker == "pump" and kind == "snapshot":
                    self._pump_snap = VescSnapshot(**payload)
                elif worker == "starter" and kind == "snapshot":
                    self._starter_snap = VescSnapshot(**payload)
                elif worker == "psu" and kind == "snapshot":
                    self._psu_snap = PsuSnapshot(**payload)
                elif worker == "logger" and kind == "state":
                    self._logger_path = str(payload.get("log_path", "") or "")
                    err = payload.get("error")
                    if err:
                        self._emit_error(str(err))

                if kind == "error":
                    msg = str(payload.get("message", "worker error"))
                    self._emit_error(f"{worker}: {msg}")

    def _check_stale(self, now: float) -> None:
        if self._pump_snap.connected and (now - self._pump_snap.ts) > self.stale_vesc_s:
            self._fsm = None
            self._stage = "fault"
            self._emit_error("pump worker stale timeout")
            return

        if self._starter_snap.connected and (now - self._starter_snap.ts) > self.stale_vesc_s:
            self._fsm = None
            self._stage = "fault"
            self._emit_error("starter worker stale timeout")
            return

        if self._psu_snap.connected and (now - self._psu_snap.ts) > self.stale_psu_s:
            self._fsm = None
            self._stage = "fault"
            self._emit_error("psu worker stale timeout")

    # ------------------------------------------------------------------
    # main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        next_tick = time.monotonic()

        while not self._stop_evt.is_set():
            now = time.monotonic()
            if now < next_tick:
                time.sleep(min(0.005, next_tick - now))
                continue

            self._tick(now)
            next_tick += self.dt

            if next_tick < now - self.dt:
                next_tick = now + self.dt

    def _tick(self, now: float) -> None:
        self._drain_worker_events()

        with self._lock:
            self._check_stale(now)

            if self._fsm is None and self._pump_prof_active and self._pump_prof is not None:
                elapsed = now - self._pump_prof_t0
                end_t = self._pump_prof.end_time

                if end_t > 0.0 and elapsed >= end_t:
                    self._stop_pump_profile_internal()
                    self.pump_target = {"mode": "rpm", "value": 0.0}
                    if self._stage == "pump_profile":
                        self._stage = "manual"

                if self._pump_prof_active:
                    rpm_cmd = interp_profile(self._pump_prof, elapsed)
                    self.pump_target = {"mode": "rpm", "value": float(rpm_cmd)}
                    self._stage = "pump_profile"

            if self._fsm is not None:
                inp = self._make_inputs(now)
                targets = self._fsm.tick(inp)
                self._set_targets_from_fsm(targets)

                if not self._fsm.running and self._fsm.state in {"Stop", "Fault"}:
                    self._stage = _stage_public_name(self._fsm.state)
                    self._fsm = None
                    self._fsm_prev_state = None

            if self._fsm is None and self._valve_macro_active:
                elapsed = now - self._valve_macro_t0
                if elapsed < self.startup_cfg.valve_boost_s:
                    self.psu_target = {
                        "v": float(self.startup_cfg.valve_boost_v),
                        "i": float(self.startup_cfg.valve_boost_i),
                        "out": True,
                    }
                else:
                    self.psu_target = {
                        "v": float(self.startup_cfg.valve_hold_v),
                        "i": float(self.startup_cfg.valve_hold_i),
                        "out": True,
                    }

            pump_target = dict(self.pump_target)
            starter_target = dict(self.starter_target)
            psu_target = dict(self.psu_target)

        self._post(self._pump_proc, "pump", "set_target", pump_target)
        self._post(self._starter_proc, "starter", "set_target", starter_target)
        self._post(self._psu_proc, "psu", "set_target", psu_target)

        if (now - self._last_ui) >= self._ui_dt:
            self._last_ui = now
            self._publish("status", self._build_status())
            self._publish("sample", self._build_sample())

        if (now - self._last_log) >= self._log_dt:
            self._last_log = now
            self._post(self._logger_proc, "logger", "row", self._build_log_row())

    # ------------------------------------------------------------------
    # payload builders
    # ------------------------------------------------------------------

    def _build_status(self) -> dict[str, Any]:
        return {
            "stage": self._stage,
            "connected": {
                "pump": self._pump_snap.connected,
                "starter": self._starter_snap.connected,
                "psu": self._psu_snap.connected,
            },
            "log_path": self._logger_path,
            "pump_profile": {
                "active": self._pump_prof_active,
                "path": self._pump_prof_path,
            },
            "valve_macro": {
                "active": self._valve_macro_active,
            },
        }

    def _build_sample(self) -> dict[str, Any]:
        return {
            "t": max(0.0, time.monotonic() - self._t0),
            "stage": self._stage,
            "connected": {
                "pump": self._pump_snap.connected,
                "starter": self._starter_snap.connected,
                "psu": self._psu_snap.connected,
            },
            "pump": asdict(self._pump_snap),
            "starter": asdict(self._starter_snap),
            "psu": asdict(self._psu_snap),
        }

    def _build_log_row(self) -> dict[str, Any]:
        sample = self._build_sample()
        pump = sample["pump"]
        starter = sample["starter"]
        psu = sample["psu"]

        return {
            "t": round(float(sample["t"]), 6),
            "stage": sample["stage"],
            "pump_rpm": pump.get("rpm_mech", 0.0),
            "pump_duty": pump.get("duty", 0.0),
            "pump_current": pump.get("current_motor", 0.0),
            "starter_rpm": starter.get("rpm_mech", 0.0),
            "starter_duty": starter.get("duty", 0.0),
            "starter_current": starter.get("current_motor", 0.0),
            "psu_v_set": psu.get("v_set", 0.0),
            "psu_i_set": psu.get("i_set", 0.0),
            "psu_v_out": psu.get("v_out", 0.0),
            "psu_i_out": psu.get("i_out", 0.0),
            "psu_output": int(bool(psu.get("output", False))),
        }
