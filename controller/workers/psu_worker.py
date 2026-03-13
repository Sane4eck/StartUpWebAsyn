from __future__ import annotations

import queue
import time
from dataclasses import asdict

from controller.devices_psu_riden import RidenPSU
from controller.runtime_types import PsuSnapshot, PsuTarget, make_event


def _drain_commands(cmd_q, last_target: PsuTarget):
    port_to_connect = None
    do_disconnect = False
    target = PsuTarget(last_target.v, last_target.i, last_target.out)

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
            target = PsuTarget(
                v=float(payload.get("v", 0.0)),
                i=float(payload.get("i", 0.0)),
                out=bool(payload.get("out", False)),
            )

    return port_to_connect, do_disconnect, target


def psu_worker_main(
    name: str,
    cmd_q,
    evt_q,
    stop_evt,
    period_s: float = 0.02,
    read_period_s: float = 0.5,
    cmd_period_s: float = 0.2,
    baudrate: int = 115200,
    timeout_s: float = 0.2,
    address: int = 1,
):
    dev = None
    target = PsuTarget()
    snap = PsuSnapshot()

    next_read = 0.0
    next_cmd = 0.0
    target_dirty = False

    def push_snapshot():
        evt_q.put(make_event(name, "snapshot", asdict(snap)))

    def push_error(text: str):
        evt_q.put(make_event(name, "error", {"message": str(text)}))

    try:
        while not stop_evt.is_set():
            now = time.monotonic()

            port_to_connect, do_disconnect, target = _drain_commands(cmd_q, target)
            snap.target_v = target.v
            snap.target_i = target.i
            snap.target_out = target.out

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
                    dev = RidenPSU(
                        baudrate=baudrate,
                        address=address,
                        timeout=timeout_s,
                        retries=1,
                    )
                    dev.connect(port_to_connect)

                    snap.connected = True
                    snap.port = port_to_connect
                    snap.error = None
                    snap.ts = time.monotonic()
                    next_read = 0.0
                    next_cmd = 0.0
                    target_dirty = True
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
                    if now >= next_cmd:
                        dev.set_vi(target.v, target.i)
                        dev.output(target.out)
                        next_cmd = now + cmd_period_s
                        target_dirty = False

                    if now >= next_read:
                        data = dev.read() or {}

                        snap.connected = True
                        snap.port = dev.port or snap.port
                        snap.v_set = float(data.get("v_set", 0.0))
                        snap.i_set = float(data.get("i_set", 0.0))
                        snap.v_out = float(data.get("v_out", 0.0))
                        snap.i_out = float(data.get("i_out", 0.0))
                        snap.p_out = float(data.get("p_out", 0.0))
                        snap.v_in = float(data.get("v_in", 0.0))
                        snap.output = bool(data.get("output", False))
                        snap.target_v = target.v
                        snap.target_i = target.i
                        snap.target_out = target.out
                        snap.error = None
                        snap.raw = dict(data)
                        snap.ts = time.monotonic()

                        next_read = now + read_period_s
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
