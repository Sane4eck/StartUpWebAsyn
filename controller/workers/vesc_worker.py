from __future__ import annotations

import queue
import time
from dataclasses import asdict

from controller.devices_vesc import VESCDevice
from controller.runtime_types import VescSnapshot, VescTarget, make_event


def _drain_commands(cmd_q, last_target: VescTarget, last_pp: int):
    port_to_connect = None
    do_disconnect = False
    target = VescTarget(last_target.mode, last_target.value)
    pole_pairs = int(last_pp)

    while True:
        try:
            msg = cmd_q.get_nowait()
        except queue.Empty:
            break

        kind = str(msg.get("kind", ""))
        payload = msg.get("payload", {}) or {}

        if kind == "connect":
            port_to_connect = str(payload.get("port", "")).strip()
        elif kind == "disconnect":
            do_disconnect = True
        elif kind == "set_target":
            target = VescTarget(
                mode=str(payload.get("mode", "rpm")),
                value=float(payload.get("value", 0.0)),
            )
        elif kind == "set_pole_pairs":
            pole_pairs = max(1, int(payload.get("pole_pairs", 7)))

    return port_to_connect, do_disconnect, target, pole_pairs


def vesc_worker_main(name: str, cmd_q, evt_q, stop_evt, period_s: float = 0.02, read_timeout_s: float = 0.01, baudrate: int = 115200):
    dev = None
    target = VescTarget(mode="rpm", value=0.0)
    snap = VescSnapshot(pole_pairs=7)

    def push_snapshot():
        evt_q.put(make_event(name, "snapshot", asdict(snap)))

    def push_error(text: str):
        evt_q.put(make_event(name, "error", {"message": str(text)}))

    try:
        while not stop_evt.is_set():
            port_to_connect, do_disconnect, target, pole_pairs = _drain_commands(cmd_q, target, snap.pole_pairs)
            snap.pole_pairs = pole_pairs
            snap.target_mode = target.mode
            snap.target_value = target.value

            if do_disconnect and dev is not None:
                try:
                    dev.disconnect()
                except Exception:
                    pass
                dev = None
                snap.connected = False
                snap.port = ""
                snap.error = None
                snap.ts = time.monotonic()
                push_snapshot()

            if port_to_connect:
                try:
                    if dev is not None:
                        try:
                            dev.disconnect()
                        except Exception:
                            pass
                    dev = VESCDevice(baudrate=baudrate, timeout=read_timeout_s)
                    dev.connect(port_to_connect)

                    snap.connected = True
                    snap.port = port_to_connect
                    snap.error = None
                    snap.ts = time.monotonic()
                    push_snapshot()
                except Exception as e:
                    dev = None
                    snap.connected = False
                    snap.port = ""
                    snap.error = str(e)
                    snap.ts = time.monotonic()
                    push_error(e)
                    push_snapshot()

            if dev is not None:
                try:
                    if target.mode == "duty":
                        dev.set_duty(target.value)
                    else:
                        dev.set_rpm_mech(target.value, snap.pole_pairs)

                    dev.request_values()
                    values = dev.read_values(snap.pole_pairs, timeout_s=read_timeout_s)

                    if values is not None:
                        snap.connected = True
                        snap.port = dev.port or snap.port
                        snap.rpm_mech = float(values.rpm_mech)
                        snap.duty = float(values.duty)
                        snap.current_motor = float(values.current_motor)
                        snap.error = None
                        snap.raw = dict(values.raw)
                        snap.ts = time.monotonic()
                        push_snapshot()
                except Exception as e:
                    try:
                        dev.disconnect()
                    except Exception:
                        pass
                    dev = None
                    snap.connected = False
                    snap.error = str(e)
                    snap.ts = time.monotonic()
                    push_error(e)
                    push_snapshot()

            time.sleep(period_s)

    finally:
        if dev is not None:
            try:
                dev.disconnect()
            except Exception:
                pass
