from __future__ import annotations

import queue
import threading
import time
from dataclasses import replace

from controller.devices_psu_riden import RidenPSU
from controller.runtime_types import PsuSnapshot, PsuTarget

from .base_worker import BaseWorker


class PsuWorker(BaseWorker):
    def __init__(
        self,
        name: str,
        period_s: float = 0.02,
        read_period_s: float = 0.5,
        cmd_period_s: float = 0.2,
        baudrate: int = 115200,
        timeout_s: float = 0.2,
        address: int = 1,
    ):
        super().__init__(name=name)
        self.period_s = float(period_s)
        self.read_period_s = float(read_period_s)
        self.cmd_period_s = float(cmd_period_s)
        self.baudrate = int(baudrate)
        self.timeout_s = float(timeout_s)
        self.address = int(address)

        self._lock = threading.RLock()
        self._dev: RidenPSU | None = None
        self._target = PsuTarget()

        self._snapshot = PsuSnapshot()
        self._target_dirty = False
        self._next_read = 0.0
        self._next_cmd = 0.0

    def connect(self, port: str) -> None:
        self.post("connect", str(port))

    def disconnect(self) -> None:
        self.post("disconnect", None)

    def set_target(self, v: float, i: float, out: bool) -> None:
        self.post("target", {"v": float(v), "i": float(i), "out": bool(out)})

    def snapshot(self) -> PsuSnapshot:
        with self._lock:
            return replace(self._snapshot, raw=dict(self._snapshot.raw))

    def _close_device(self) -> None:
        dev = None
        with self._lock:
            dev = self._dev
            self._dev = None
        if dev is not None:
            try:
                dev.disconnect()
            except Exception:
                pass

    def _set_error(self, message: str) -> None:
        with self._lock:
            self._snapshot.connected = False
            self._snapshot.error = message
            self._snapshot.ts = time.monotonic()

    def _handle_connect(self, port: str) -> None:
        self._close_device()
        dev = RidenPSU(
            baudrate=self.baudrate,
            address=self.address,
            timeout=self.timeout_s,
            retries=1,
        )
        dev.connect(port)

        with self._lock:
            self._dev = dev
            self._snapshot = PsuSnapshot(
                connected=True,
                port=port,
                target_v=self._target.v,
                target_i=self._target.i,
                target_out=self._target.out,
                ts=time.monotonic(),
                error=None,
                raw={},
            )
            self._target_dirty = True
            self._next_read = 0.0
            self._next_cmd = 0.0

    def _handle_disconnect(self) -> None:
        self._close_device()
        with self._lock:
            self._snapshot = PsuSnapshot(
                connected=False,
                port="",
                target_v=self._target.v,
                target_i=self._target.i,
                target_out=self._target.out,
                ts=time.monotonic(),
                error=None,
                raw={},
            )
            self._target_dirty = False
            self._next_read = 0.0
            self._next_cmd = 0.0

    def _run(self) -> None:
        while not self._stop_evt.is_set():
            now = time.monotonic()

            try:
                while True:
                    kind, payload = self._cmd_q.get_nowait()
                    if kind == "__stop__":
                        break
                    if kind == "connect":
                        self._handle_connect(str(payload))
                    elif kind == "disconnect":
                        self._handle_disconnect()
                    elif kind == "target":
                        with self._lock:
                            self._target = PsuTarget(
                                v=float(payload["v"]),
                                i=float(payload["i"]),
                                out=bool(payload["out"]),
                            )
                            self._snapshot.target_v = self._target.v
                            self._snapshot.target_i = self._target.i
                            self._snapshot.target_out = self._target.out
                            self._target_dirty = True
            except queue.Empty:
                pass
            except Exception as e:
                self._set_error(str(e))

            with self._lock:
                dev = self._dev
                target = PsuTarget(self._target.v, self._target.i, self._target.out)
                target_dirty = self._target_dirty
                next_cmd = self._next_cmd
                next_read = self._next_read

            if dev is not None:
                try:
                    if target_dirty and now >= next_cmd:
                        dev.set_vi(target.v, target.i)
                        dev.output(target.out)
                        with self._lock:
                            self._target_dirty = False
                            self._next_cmd = now + self.cmd_period_s

                    if now >= next_read:
                        data = dev.read() or {}
                        with self._lock:
                            self._snapshot.connected = True
                            self._snapshot.port = dev.port or ""
                            self._snapshot.v_set = float(data.get("v_set", 0.0))
                            self._snapshot.i_set = float(data.get("i_set", 0.0))
                            self._snapshot.v_out = float(data.get("v_out", 0.0))
                            self._snapshot.i_out = float(data.get("i_out", 0.0))
                            self._snapshot.p_out = float(data.get("p_out", 0.0))
                            self._snapshot.v_in = float(data.get("v_in", 0.0))
                            self._snapshot.output = bool(data.get("output", False))
                            self._snapshot.target_v = target.v
                            self._snapshot.target_i = target.i
                            self._snapshot.target_out = target.out
                            self._snapshot.ts = time.monotonic()
                            self._snapshot.error = None
                            self._snapshot.raw = dict(data)
                            self._next_read = now + self.read_period_s
                except Exception as e:
                    self._close_device()
                    self._set_error(str(e))

            time.sleep(self.period_s)

        self._close_device()
