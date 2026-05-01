from __future__ import annotations

import queue
import re
import time

import serial
from serial import SerialException

from controller.runtime_types import HallSnapshot, make_event


LINE_RE = re.compile(
    r"RPM:\s*([+-]?\d+(?:\.\d+)?),\s*P:\s*(-?\d+),\s*T:\s*(\d+)",
    re.IGNORECASE,
)


def _drain_commands(cmd_q, last_pairs: int):
    port_to_connect = None
    do_disconnect = False
    pairs = int(last_pairs)

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
        elif kind == "set_pairs":
            pairs = max(1, int(payload.get("pairs", 1)))

    return port_to_connect, do_disconnect, pairs


def hall_worker_main(
    name: str,
    cmd_q,
    evt_q,
    stop_evt,
    period_s: float = 0.02,
    read_timeout_s: float = 0.2,
    baudrate: int = 115200,
):
    ser = None
    snap = HallSnapshot()

    def push_snapshot():
        evt_q.put(make_event(name, "snapshot", snap.__dict__))

    def push_error(text: str):
        evt_q.put(make_event(name, "error", {"message": str(text)}))

    try:
        while not stop_evt.is_set():
            port_to_connect, do_disconnect, pairs = _drain_commands(cmd_q, snap.pairs)

            if pairs != snap.pairs:
                snap.pairs = max(1, int(pairs))
                snap.ts = time.monotonic()
                push_snapshot()

            if do_disconnect and ser is not None:
                try:
                    ser.close()
                except Exception:
                    pass
                ser = None
                snap.connected = False
                snap.port = ""
                snap.error = None
                snap.raw_line = ""
                snap.ts = time.monotonic()
                push_snapshot()

            if port_to_connect:
                try:
                    if ser is not None:
                        try:
                            ser.close()
                        except Exception:
                            pass

                    ser = serial.Serial(port_to_connect, baudrate=baudrate, timeout=read_timeout_s)
                    try:
                        ser.reset_input_buffer()
                    except Exception:
                        pass

                    snap.connected = True
                    snap.port = port_to_connect
                    snap.error = None
                    snap.raw_line = ""
                    snap.ts = time.monotonic()
                    push_snapshot()

                except Exception as e:
                    ser = None
                    snap.connected = False
                    snap.port = ""
                    snap.error = str(e)
                    snap.ts = time.monotonic()
                    push_error(e)
                    push_snapshot()

            if ser is not None:
                try:
                    raw = ser.readline()
                    if raw:
                        line = raw.decode("utf-8", errors="ignore").strip()
                        if line and not line.startswith("#"):
                            m = LINE_RE.search(line)
                            if m:
                                rpm_raw = float(m.group(1))
                                pulses = int(m.group(2))
                                sample_ms = int(m.group(3))

                                snap.connected = True
                                snap.error = None
                                snap.rpm_raw = rpm_raw
                                snap.rpm = rpm_raw / max(1, int(snap.pairs))
                                snap.pulses = pulses
                                snap.sample_ms = sample_ms
                                snap.raw_line = line
                                snap.ts = time.monotonic()
                                push_snapshot()
                except (SerialException, OSError) as e:
                    try:
                        ser.close()
                    except Exception:
                        pass
                    ser = None
                    snap.connected = False
                    snap.error = str(e)
                    snap.ts = time.monotonic()
                    push_error(e)
                    push_snapshot()
                except Exception as e:
                    snap.error = str(e)
                    snap.ts = time.monotonic()
                    push_error(e)

            time.sleep(period_s)

    finally:
        if ser is not None:
            try:
                ser.close()
            except Exception:
                pass
