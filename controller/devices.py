from __future__ import annotations

import time
from typing import Any, Dict, Optional

import serial
from pyvesc import decode, encode, encode_request
from pyvesc.VESC.messages import GetValues, SetDutyCycle, SetRPM
from serial import SerialException

from scheme.vesc import VESCValues


def _msg_to_dict(msg: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name in dir(msg):
        if name.startswith("_"):
            continue
        try:
            val = getattr(msg, name)
        except Exception:
            continue
        if callable(val):
            continue
        if val is None or isinstance(val, (int, float, bool, str)):
            out[name] = val
            continue
        if isinstance(val, (list, tuple)) and all(
            isinstance(x, (int, float, bool)) for x in val
        ):
            out[name] = list(val)
    return out


class VESCDevice:
    def __init__(self, baudrate: int = 115200, timeout: float = 0.01):
        self.baudrate = int(baudrate)
        self.timeout = float(timeout)
        self.ser: Optional[serial.Serial] = None
        self.port: Optional[str] = None
        self._rxbuf: bytes = b""

    @property
    def is_connected(self) -> bool:
        return bool(self.ser and self.ser.is_open)

    def connect(self, port: str) -> None:
        self.disconnect()
        self.ser = serial.Serial(
            port=port,
            baudrate=self.baudrate,
            timeout=self.timeout,
            write_timeout=0.2,
        )
        self.port = port
        try:
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
        except Exception:
            pass
        self._rxbuf = b""
        time.sleep(0.03)

    def disconnect(self) -> None:
        if self.ser:
            try:
                if self.ser.is_open:
                    try:
                        self.ser.flush()
                    except Exception:
                        pass
                    self.ser.close()
            except Exception:
                pass
        self.ser = None
        self.port = None
        self._rxbuf = b""

    def set_duty(self, duty: float) -> None:
        if not self.is_connected:
            return
        d = max(0.0, min(1.0, float(duty)))
        self.ser.write(encode(SetDutyCycle(d)))

    def set_rpm_mech(self, rpm_mech: float, pole_pairs: int) -> None:
        if not self.is_connected:
            return
        pp = max(1, int(pole_pairs))
        erpm = int(float(rpm_mech) * pp)
        self.ser.write(encode(SetRPM(erpm)))

    def request_values(self) -> None:
        if not self.is_connected:
            return
        self.ser.write(encode_request(GetValues))

    def read_values(self, pole_pairs: int, timeout_s: float = 0.01) -> Optional[VESCValues]:
        if not self.is_connected:
            return None

        pp = max(1, int(pole_pairs))
        deadline = time.monotonic() + float(timeout_s)

        while time.monotonic() < deadline:
            try:
                chunk = self.ser.read(256)
            except (SerialException, OSError):
                raise

            if chunk:
                self._rxbuf += chunk

            try:
                msg, consumed = decode(self._rxbuf)
            except Exception:
                self._rxbuf = b""
                msg, consumed = None, 0

            if consumed:
                self._rxbuf = self._rxbuf[consumed:]

            if isinstance(msg, GetValues):
                raw = _msg_to_dict(msg)
                erpm = float(raw.get("rpm", 0.0) or 0.0)
                duty = float(raw.get("duty_cycle_now", 0.0) or 0.0)
                current = float(raw.get("avg_motor_current", 0.0) or 0.0)
                return VESCValues(
                    rpm_mech=erpm / pp,
                    duty=duty,
                    current_motor=current,
                    raw=raw,
                )

            if len(self._rxbuf) > 4096:
                self._rxbuf = self._rxbuf[-1024:]

            time.sleep(0.001)

        return None
