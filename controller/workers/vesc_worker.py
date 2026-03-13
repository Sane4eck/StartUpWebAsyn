from __future__ import annotations

import queue
import threading
import time
from dataclasses import replace

from controller.devices_vesc import VESCDevice
from controller.runtime_types import VescSnapshot, VescTarget

from .base_worker import BaseWorker


class VescWorker(BaseWorker):
    def __init__(
        self,
        name: str,
        period_s: float = 0.02,
        read_timeout_s: float = 0.01,
        baudrate: int = 115200,
    ):
        super().__init__(name=name)
        self.period_s = float(period_s)
        self.read_timeout_s = float(read_timeout_s)
        self.baudrate = int(baudrate)

        self._lock = threading.RLock()
        self._dev: VESCDevice | None = None
        self._target = VescTarget(mode="rpm", value=0.0)
        self._snapshot = VescSnapshot()

    def connect(self, port: str) -> None:
        self.post("connect", str(port))

    def disconnect(self) -> None:
        self.post("disconnect", None)

    def set_pole_pairs(self, pole_pairs: int) -> None:
        self.post("pole_pairs", int(pole_pairs))

    def set_target(self, mode: str, value: float) -> None:
        self.post("target", {"mode": str(mode), "value": float(value)})

    def snapshot(self) -> VescSnapshot:
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
        dev = VESCDevice(baudrate=self.baudrate, timeout=self.read_timeout_s)
        dev.connect(port)

        with self._lock:
            pole_pairs = self._snapshot.pole_pairs
            self._dev = dev
            self._snapshot = VescSnapshot(
                connected=True,
                port=port,
                pole_pairs=pole_pairs,
                target_mode=self._target.mode,
                target_value=self._target.value,
                ts=time.monotonic(),
                error=None,
                raw={},
            )

    def _handle_disconnect(self) -> None:
        self._close_device()
        with self._lock:
            pole_pairs = self._snapshot.pole_pairs
            self._snapshot = VescSnapshot(
                connected=False,
                port="",
                pole_pairs=pole_pairs,
                target_mode=self._target.mode,
                target_value=self._target.value,
                ts=time.monotonic(),
                error=None,
                raw={},
            )

    def _run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                while True:
                    kind, payload = self._cmd_q.get_nowait()
                    if kind == "__stop__":
                        break
                    if kind == "connect":
                        self._handle_connect(str(payload))
                    elif kind == "disconnect":
                        self._handle_disconnect()
                    elif kind == "pole_pairs":
                        with self._lock:
                            self._snapshot.pole_pairs = max(1, int(payload))
                    elif kind == "target":
                        with self._lock:
                            self._target = VescTarget(
                                mode=str(payload["mode"]),
                                value=float(payload["value"]),
                            )
                            self._snapshot.target_mode = self._target.mode
                            self._snapshot.target_value = self._target.value
            except queue.Empty:
                pass
            except Exception as e:
                self._set_error(str(e))

            with self._lock:
                dev = self._dev
                target = VescTarget(self._target.mode, self._target.value)
                pole_pairs = int(self._snapshot.pole_pairs)

            if dev is not None:
                try:
                    if target.mode == "duty":
                        dev.set_duty(target.value)
                    else:
                        dev.set_rpm_mech(target.value, pole_pairs)

                    dev.request_values()
                    values = dev.read_values(pole_pairs, timeout_s=self.read_timeout_s)

                    if values is not None:
                        with self._lock:
                            self._snapshot.connected = True
                            self._snapshot.port = dev.port or ""
                            self._snapshot.rpm_mech = float(values.rpm_mech)
                            self._snapshot.duty = float(values.duty)
                            self._snapshot.current_motor = float(values.current_motor)
                            self._snapshot.target_mode = target.mode
                            self._snapshot.target_value = target.value
                            self._snapshot.ts = time.monotonic()
                            self._snapshot.error = None
                            self._snapshot.raw = dict(values.raw)
                except Exception as e:
                    self._close_device()
                    self._set_error(str(e))

            time.sleep(self.period_s)

        self._close_device()
