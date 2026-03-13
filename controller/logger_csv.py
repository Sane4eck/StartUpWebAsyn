# logger_csv.py
from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


class CSVLogger:
    """
    Формат CSV:
      t, stage,
      [pump fields...],
      [starter fields...],
      [psu fields...]
    """

    def __init__(self):
        self.f = None
        self.w: Optional[csv.writer] = None
        self.path: Optional[str] = None
        self.header: List[str] = []

    def start(self, folder: str = "file_logs", prefix: str = "session") -> str:
        os.makedirs(folder, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.path = os.path.join(folder, f"{prefix}_{ts}.csv")

        self.f = open(self.path, "w", newline="", encoding="utf-8", buffering=1)
        self.w = csv.writer(self.f)

        self.header = self.build_header()
        self.w.writerow(self.header)
        self.flush()
        return self.path

    def stop(self) -> None:
        try:
            self.flush()
            if self.f:
                self.f.close()
        finally:
            self.f = None
            self.w = None
            self.path = None
            self.header = []

    def flush(self) -> None:
        if self.f:
            try:
                self.f.flush()
            except Exception:
                pass

    # ---------------- schema
    def build_header(self) -> List[str]:
        return [
            "t", "stage",

            # ---- PUMP (pmp_)
            "pmp_rpm_cmd", "pmp_erpm_cmd", "pmp_duty_cmd",

            "pmp_rpm_get", "pmp_erpm_get", "pmp_duty_get",
            "pmp_current_get", "pmp_bat_current", "pmp_v_in_get",

            "pmp_raw_amp_hours", "pmp_raw_amp_hours_charged",
            "pmp_raw_watt_hours", "pmp_raw_watt_hours_charged",
            "pmp_raw_temp_fet", "pmp_raw_temp_motor",

            # ---- STARTER (strtr_)
            "strtr_rpm_cmd", "strtr_erpm_cmd", "strtr_duty_cmd",

            "strtr_rpm_get", "strtr_erpm_get", "strtr_duty_get",
            "strtr_current_get", "strtr_bat_current", "strtr_v_in_get",

            "strtr_raw_amp_hours", "strtr_raw_amp_hours_charged",
            "strtr_raw_watt_hours", "strtr_raw_watt_hours_charged",
            "strtr_raw_temp_fet", "strtr_raw_temp_motor",

            # ---- PSU (як є)
            "psu_v_set", "psu_i_set", "psu_v_out", "psu_i_out", "psu_p_out",
        ]

    def build_row(
        self,
        t: float,
        stage: str,
        pump_target: Dict[str, Any],
        starter_target: Dict[str, Any],
        pole_pairs_pump: int,
        pole_pairs_starter: int,
        pump_vals: Any,     # VESCValues: rpm_mech, duty, current_motor, raw(dict)
        starter_vals: Any,  # VESCValues
        psu: Dict[str, Any],
    ) -> List[Any]:
        row: Dict[str, Any] = {"t": t, "stage": stage}

        row.update(self._cmd_cols(pump_target, pole_pairs_pump, "pmp_"))
        row.update(self._get_cols(pump_vals, pole_pairs_pump, "pmp_"))
        row.update(self._raw_cols(pump_vals, "pmp_"))

        row.update(self._cmd_cols(starter_target, pole_pairs_starter, "strtr_"))
        row.update(self._get_cols(starter_vals, pole_pairs_starter, "strtr_"))
        row.update(self._raw_cols(starter_vals, "strtr_"))

        row["psu_v_set"] = _f(psu.get("v_set", 0.0)) if psu else 0.0
        row["psu_i_set"] = _f(psu.get("i_set", 0.0)) if psu else 0.0
        row["psu_v_out"] = _f(psu.get("v_out", 0.0)) if psu else 0.0
        row["psu_i_out"] = _f(psu.get("i_out", 0.0)) if psu else 0.0
        row["psu_p_out"] = _f(psu.get("p_out", 0.0)) if psu else 0.0

        return [row.get(col, "") for col in self.header]

    def write_row(self, row: List[Any]) -> None:
        if self.w:
            self.w.writerow(row)

    # ---------------- internals
    def _cmd_cols(self, target: Dict[str, Any], pole_pairs: int, prefix: str) -> Dict[str, Any]:
        """
        Пишемо тільки те, чим реально керуєш зараз.
        Інше залишаємо пустим.
        """
        mode = str(target.get("mode", "duty"))
        val = _f(target.get("value", 0.0))
        pp = max(1, int(pole_pairs))

        out = {
            f"{prefix}rpm_cmd": "",
            f"{prefix}erpm_cmd": "",
            f"{prefix}duty_cmd": "",
        }

        if mode == "rpm":
            out[f"{prefix}rpm_cmd"] = val
            out[f"{prefix}erpm_cmd"] = val * pp
        else:
            out[f"{prefix}duty_cmd"] = _clamp01(val)

        return out

    def _get_cols(self, vesc_vals: Any, pole_pairs: int, prefix: str) -> Dict[str, Any]:
        raw = getattr(vesc_vals, "raw", {}) or {}
        pp = max(1, int(pole_pairs))

        erpm = raw.get("rpm", None)
        duty = raw.get("duty_cycle_now", None)
        cur_m = raw.get("avg_motor_current", None)
        cur_b = raw.get("avg_input_current", None)
        v_in = raw.get("v_in", None)

        # rpm_get: або з rpm_mech (вже пораховано), або з erpm/pp
        rpm_mech_attr = getattr(vesc_vals, "rpm_mech", None)
        rpm_get = _f(rpm_mech_attr) if rpm_mech_attr is not None else (_f(erpm) / pp)

        return {
            f"{prefix}rpm_get": rpm_get,
            f"{prefix}erpm_get": _f(erpm, 0.0),
            f"{prefix}duty_get": _f(duty, 0.0),
            f"{prefix}current_get": _f(cur_m, 0.0),
            f"{prefix}bat_current": _f(cur_b, 0.0),
            f"{prefix}v_in_get": _f(v_in, 0.0),
        }

    def _raw_cols(self, vesc_vals: Any, prefix: str) -> Dict[str, Any]:
        raw = getattr(vesc_vals, "raw", {}) or {}
        return {
            f"{prefix}raw_amp_hours": _f(raw.get("amp_hours", 0.0)),
            f"{prefix}raw_amp_hours_charged": _f(raw.get("amp_hours_charged", 0.0)),
            f"{prefix}raw_watt_hours": _f(raw.get("watt_hours", 0.0)),
            f"{prefix}raw_watt_hours_charged": _f(raw.get("watt_hours_charged", 0.0)),
            f"{prefix}raw_temp_fet": _f(raw.get("temp_fet", 0.0)),
            f"{prefix}raw_temp_motor": _f(raw.get("temp_motor", 0.0)),
        }
