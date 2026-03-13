from __future__ import annotations

import queue
import threading
from typing import Any


class BaseWorker:
    def __init__(self, name: str):
        self.name = name
        self._cmd_q: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._stop_evt = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._run, name=self.name, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_evt.set()
        self.post("__stop__", None)
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def post(self, kind: str, payload: Any = None) -> None:
        self._cmd_q.put((kind, payload))

    def _run(self) -> None:
        raise NotImplementedError
