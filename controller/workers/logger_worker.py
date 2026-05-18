from __future__ import annotations

import csv
import queue
import re
import time
from datetime import datetime
from pathlib import Path

from controller.runtime_types import make_event


HEADER = [
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

    # ---- PSU
    "psu_v_set", "psu_i_set", "psu_v_out", "psu_i_out", "psu_p_out",

    # ---- HALL
    "hall_pairs", "hall_rpm", "hall_rpm_raw", "hall_pulses", "hall_sample_ms",
]


def _safe_prefix(text: str) -> str:
    raw = (text or "manual").strip()
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw)
    return safe or "manual"


def logger_worker_main(name: str, cmd_q, evt_q, stop_evt, logs_dir: str = "logs"):
    logs_path = Path(logs_dir)
    fh = None
    writer = None
    current_path = ""

    def push_state(error: str | None = None):
        evt_q.put(
            make_event(
                name,
                "state",
                {
                    "opened": fh is not None,
                    "log_path": current_path,
                    "error": error,
                },
            )
        )

    try:
        while not stop_evt.is_set():
            try:
                msg = cmd_q.get(timeout=0.2)
            except queue.Empty:
                if fh is not None:
                    try:
                        fh.flush()
                    except Exception:
                        pass
                continue

            kind = str(msg.get("kind", ""))
            payload = msg.get("payload", {}) or {}

            try:
                if kind == "open":
                    if fh is not None:
                        try:
                            fh.flush()
                            fh.close()
                        except Exception:
                            pass
                        fh = None
                        writer = None
                        current_path = ""

                    logs_path.mkdir(parents=True, exist_ok=True)
                    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    prefix = _safe_prefix(str(payload.get("prefix", "manual")))
                    path = logs_path / f"{stamp}_{prefix}.csv"

                    fh = open(path, "w", newline="", encoding="utf-8")
                    writer = csv.DictWriter(fh, fieldnames=HEADER)
                    writer.writeheader()
                    fh.flush()

                    current_path = str(path)
                    push_state()

                elif kind == "row":
                    if writer is not None and fh is not None:
                        row = dict(payload)
                        writer.writerow({k: row.get(k) for k in HEADER})

                elif kind == "close":
                    if fh is not None:
                        try:
                            fh.flush()
                            fh.close()
                        except Exception:
                            pass
                    fh = None
                    writer = None
                    current_path = ""
                    push_state()

            except Exception as e:
                push_state(str(e))

    finally:
        if fh is not None:
            try:
                fh.flush()
                fh.close()
            except Exception:
                pass
