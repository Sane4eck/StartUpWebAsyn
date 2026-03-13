from __future__ import annotations

import threading
import time
from pathlib import Path
from web_app import app

import uvicorn
import webview


HOST = "127.0.0.1"
PORT = 8000


class JsApi:
    def __init__(self) -> None:
        self.window = None

    def choose_xlsx(self) -> str:
        if self.window is None:
            return ""

        result = self.window.create_file_dialog(
            webview.OPEN_DIALOG,
            directory=str(Path.cwd()),
            allow_multiple=False,
            file_types=("Excel files (*.xlsx)", "All files (*.*)"),
        )

        if not result:
            return ""

        return str(result[0])


api = JsApi()


def _run_server() -> None:
    uvicorn.run(app, host=HOST, port=PORT, reload=False, log_level="warning")


if __name__ == "__main__":
    server_thread = threading.Thread(target=_run_server, daemon=True)
    server_thread.start()
    time.sleep(1.0)

    window = webview.create_window(
        "StartUp Web",
        f"http://{HOST}:{PORT}",
        width=1450,
        height=980,
        min_size=(1200, 780),
        background_color="#06080c",
        js_api=api,
    )
    api.window = window
    webview.start()
