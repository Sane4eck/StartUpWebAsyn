# cyclogram_startup.py
from __future__ import annotations

from typing import List, Tuple

from controller.cycle_fsm import CycleFSM
from controller.pump_profile import interp_profile
from scheme.cycle import CycleInputs, CycleTargets, State, Transition
from scheme.pump_profile import PumpProfile
from scheme.startup import StartupConfig


# ---------- helpers ----------
def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def set_pump_rpm(out: CycleTargets, rpm: float):
    out.pump = {"mode": "rpm", "value": float(rpm)}


def set_starter_duty(out: CycleTargets, duty: float):
    out.starter = {"mode": "duty", "value": _clamp01(duty)}


def set_valve(out: CycleTargets, v: float, i: float, on: bool):
    out.psu = {"v": float(v), "i": float(i), "out": bool(on)}


def stop_all(out: CycleTargets):
    set_pump_rpm(out, 0.0)
    set_starter_duty(out, 0.0)
    set_valve(out, 0.0, 0.0, False)


def _hold_ge(mem: dict, state_t: float, value: float, thr: float, hold_s: float) -> bool:
    """True якщо value >= thr безперервно hold_s (по часу state_t)."""
    if value >= thr:
        if mem["armed_at"] is None:
            mem["armed_at"] = state_t
            return False
        return (state_t - float(mem["armed_at"])) >= float(hold_s)
    mem["armed_at"] = None
    return False


class StarterDutySchedule:
    """
    Таблиця duty по RPM, одна й та сама для Starter і FuelRamp.
    steps: [(rpm_threshold, duty), ...] у зростаючому порядку.
    Перехід на наступний крок тільки якщо rpm тримається >= threshold step_hold_s.
    """
    def __init__(self, steps: List[Tuple[float, float]], step_hold_s: float):
        self.steps = sorted([(float(r), float(d)) for r, d in steps], key=lambda x: x[0])
        self.step_hold_s = float(step_hold_s)
        self.idx = 0
        self._armed_at = None  # state_t коли rpm вперше стало >= next_threshold

    def reset_all(self):
        self.idx = 0
        self._armed_at = None

    def reset_timer_only(self):
        self._armed_at = None

    def value(self, rpm: float, state_t: float) -> float:
        if not self.steps:
            return 0.0

        while self.idx < len(self.steps) - 1:
            next_rpm, _ = self.steps[self.idx + 1]
            if rpm >= next_rpm:
                if self._armed_at is None:
                    self._armed_at = state_t
                    break
                if (state_t - float(self._armed_at)) >= self.step_hold_s:
                    self.idx += 1
                    self._armed_at = None
                    continue
                break
            else:
                self._armed_at = None
                break

        return float(self.steps[self.idx][1])



def build_startup_fsm(
    pump_profile: PumpProfile,      # _Cyclogram_Pump.xlsx (RPM)
    starter_profile: PumpProfile,   # не використовується (залишено для сумісності)
    cfg: StartupConfig | None = None,
) -> CycleFSM:
    cfg = cfg or StartupConfig()

    sched = StarterDutySchedule(cfg.starter_steps, cfg.starter_step_hold_s)

    # holds/latches
    mem_to_fuel = {"armed_at": None}

    mem_valve = {"armed_at": None, "latched": False}
    mem_starter_off = {"armed_at": None, "latched": False}

    mem_to_run = {"armed_at": None}
    pump_hold = {"rpm": 0.0}

    def cond_to_fuelramp(i: CycleInputs) -> bool:
        return _hold_ge(mem_to_fuel, i.state_t, i.starter_rpm, cfg.to_fuelramp_starter_rpm, cfg.to_fuelramp_hold_s)

    def cond_to_running(i: CycleInputs) -> bool:
        return _hold_ge(mem_to_run, i.state_t, i.starter_rpm, cfg.to_running_starter_rpm, cfg.to_running_hold_s)

    # ---------- Stop/Fault
    def stop_enter(_i: CycleInputs, out: CycleTargets):
        stop_all(out)

    def fault_enter(_i: CycleInputs, out: CycleTargets):
        stop_all(out)
        out.meta["fault"] = out.meta.get("transition_reason", "Fault")

    # ---------- Starter
    def starter_enter(_i: CycleInputs, out: CycleTargets):
        sched.reset_all()
        mem_to_fuel["armed_at"] = None
        set_starter_duty(out, sched.value(0.0, 0.0))
        set_pump_rpm(out, 0.0)
        set_valve(out, 0.0, 0.0, False)

    def starter_tick(i: CycleInputs, out: CycleTargets):
        set_starter_duty(out, sched.value(i.starter_rpm, i.state_t))
        set_pump_rpm(out, 0.0)
        set_valve(out, 0.0, 0.0, False)

    # ---------- FuelRamp
    def fuelramp_enter(_i: CycleInputs, out: CycleTargets):
        # state_t починається заново, тому скид таймера ступені (щоб не “перестрибувало”)
        sched.reset_timer_only()

        mem_valve["armed_at"] = None
        mem_valve["latched"] = False
        mem_starter_off["armed_at"] = None
        mem_starter_off["latched"] = False
        mem_to_run["armed_at"] = None

    def fuelramp_tick(i: CycleInputs, out: CycleTargets):
        # pump: by profile
        set_pump_rpm(out, interp_profile(pump_profile, i.state_t))

        # valve: close by starter RPM
        if mem_valve["latched"] or _hold_ge(mem_valve, i.state_t, i.starter_rpm, cfg.valve_close_rpm, cfg.valve_close_hold_s):
            mem_valve["latched"] = True
            set_valve(out, 0.0, 0.0, False)
        else:
            if i.state_t < cfg.valve_boost_s:
                set_valve(out, cfg.valve_boost_v, cfg.valve_boost_i, True)
            else:
                set_valve(out, cfg.valve_hold_v, cfg.valve_hold_i, True)

        # starter: schedule, but OFF by starter RPM
        if mem_starter_off["latched"] or _hold_ge(
            mem_starter_off, i.state_t, i.starter_rpm, cfg.starter_off_rpm, cfg.starter_off_hold_s
        ):
            mem_starter_off["latched"] = True
            set_starter_duty(out, 0.0)
        else:
            set_starter_duty(out, sched.value(i.starter_rpm, i.state_t))

    # ---------- Running
    def running_enter(i: CycleInputs, out: CycleTargets):
        # freeze pump at current measured rpm when starter reaches to_running_starter_rpm
        pump_hold["rpm"] = float(i.pump_rpm)
        set_pump_rpm(out, pump_hold["rpm"])

        set_starter_duty(out, 0.0)
        set_valve(out, 0.0, 0.0, False)

        # worker.py applies pump target once on entry to Running
        out.meta["apply_pump_once_on_running_entry"] = True

    def running_tick(_i: CycleInputs, out: CycleTargets):
        set_pump_rpm(out, pump_hold["rpm"])
        set_starter_duty(out, 0.0)
        set_valve(out, 0.0, 0.0, False)

    states = {
        "Stop": State("Stop", on_enter=stop_enter, terminal=True),
        "Fault": State("Fault", on_enter=fault_enter, terminal=True),

        "Starter": State(
            "Starter",
            on_enter=starter_enter,
            on_tick=starter_tick,
            transitions=[
                Transition(cond_to_fuelramp, "FuelRamp", reason="OK: Starter -> FuelRamp"),
            ],
            timeout_s=cfg.starter_timeout_s,
            on_timeout="Fault",
            timeout_reason=f"Stop: Starter timeout {cfg.starter_timeout_s:.0f}s",
        ),

        "FuelRamp": State(
            "FuelRamp",
            on_enter=fuelramp_enter,
            on_tick=fuelramp_tick,
            transitions=[
                Transition(cond_to_running, "Running", reason="OK: FuelRamp -> Running"),
            ],
            timeout_s=cfg.fuelramp_timeout_s,
            on_timeout="Fault",
            timeout_reason=f"Stop: FuelRamp timeout {cfg.fuelramp_timeout_s:.0f}s",
        ),

        "Running": State("Running", on_enter=running_enter, on_tick=running_tick),
    }

    return CycleFSM(states=states, initial="Starter", stop_state="Stop")


def build_cooling_fsm(duty: float, duration_s: float = 8.0) -> CycleFSM:
    duty = _clamp01(duty)

    def cooling_enter(_i: CycleInputs, out: CycleTargets):
        set_starter_duty(out, duty)
        set_pump_rpm(out, 0.0)
        set_valve(out, 0.0, 0.0, False)

    def stop_enter(_i: CycleInputs, out: CycleTargets):
        stop_all(out)

    states = {
        "Cooling": State("Cooling", on_enter=cooling_enter,
                         transitions=[Transition(lambda i: i.state_t >= duration_s, "Stop", reason="Cooling done")]),
        "Stop": State("Stop", on_enter=stop_enter, terminal=True),
    }
    return CycleFSM(states=states, initial="Cooling", stop_state="Stop")
