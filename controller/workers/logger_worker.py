from __future__ import annotations

import csv
import queue
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .base_worker import BaseWorker


class LoggerWorker(BaseWorker):
    HEADER = [
        "t",
        "stage",
        "pump_rpm",
        "pump_duty",
        "pump_current",
        "starter_rpm",
        "starter_duty",
        "starter_current",
        "psu_v_set",
        "psu_i_set",
        "psu_v_out",
        "psu_i_out",
        "psu_output",
    ]

    def __init__(self, name: str = "csv-logger", logs_dir: str = "logs"):
        super().__init__(name=name)
        self.logs_dir = Path(logs_dir)
        self._lock = threading.RLock()

        self._fh = None
        self._writer = None
        self._log_path = ""

    @property
    def log_path(self) -> str:
        with self._lock:
            return self._log_path

    def open_log(self, prefix: str) -> None:
        self.post("open", prefix)

    def close_log(self) -> None:
        self.post("close", None)

    def write_row(self, row: dict[str, Any]) -> None:
        self.post("row", dict(row))

    def _safe_prefix(self, text: str) -> str:
        raw = (text or "manual").strip()
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw)
        return safe or "manual"

    def _close_writer(self) -> None:
        with self._lock:
            fh = self._fh
            self._fh = None
            self._writer = None
            self._log_path = ""
        if fh is not None:
            try:
                fh.flush()
            except Exception:
                pass
            try:
                fh.close()
            except Exception:
                pass

    def _open_writer(self, prefix: str) -> None:
        self._close_writer()
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.logs_dir / f"{stamp}_{self._safe_prefix(prefix)}.csv"

        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=self.HEADER)
        writer.writeheader()
        fh.flush()

        with self._lock:
            self._fh = fh
            self._writer = writer
            self._log_path = str(path)

    def _write(self, row: dict[str, Any]) -> None:
        with self._lock:
            writer = self._writer
            fh = self._fh
        if writer is None or fh is None:
            return
        writer.writerow({k: row.get(k) for k in self.HEADER})

    def _run(self) -> None:
        last_flush = time.monotonic()

        while not self._stop_evt.is_set():
            kind = None
            payload = None
            try:
                kind, payload = self._cmd_q.get(timeout=0.2)
            except queue.Empty:
                pass

            try:
                if kind == "open":
                    self._open_writer(str(payload or "manual"))
                elif kind == "close":
                    self._close_writer()
                elif kind == "row":
                    self._write(dict(payload or {}))
                elif kind == "__stop__":
                    break
            except Exception:
                pass

            with self._lock:
                fh = self._fh
            if fh is not None and (time.monotonic() - last_flush) >= 1.0:
                try:
                    fh.flush()
                except Exception:
                    pass
                last_flush = time.monotonic()

        self._close_writer()
