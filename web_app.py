from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from controller import WebControllerRuntime


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


class BroadcastHub:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subs: set[asyncio.Queue] = set()

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._subs.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    async def publish(self, event: str, payload: Any) -> None:
        dead: list[asyncio.Queue] = []

        for q in list(self._subs):
            try:
                if q.full():
                    try:
                        q.get_nowait()
                    except Exception:
                        pass

                q.put_nowait({"event": event, "payload": payload})
            except Exception:
                dead.append(q)

        for q in dead:
            self._subs.discard(q)

    def publish_sync(self, event: str, payload: Any) -> None:
        if self._loop is None:
            return

        try:
            asyncio.run_coroutine_threadsafe(
                self.publish(event, payload),
                self._loop,
            )
        except Exception:
            pass


hub = BroadcastHub()
runtime = WebControllerRuntime(publish=hub.publish_sync)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class PortBody(BaseModel):
    port: str = ""


class ReadyBody(BaseModel):
    prefix: str = ""


class ValueBody(BaseModel):
    value: float


class PumpProfileBody(BaseModel):
    path: str = ""


class PolePairsBody(BaseModel):
    pump: int
    starter: int


class PsuViBody(BaseModel):
    v: float
    i: float


class BoolBody(BaseModel):
    value: bool


def _ok(**extra):
    data = {"ok": True}
    data.update(extra)
    return data


def _call(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
        return _ok()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.on_event("startup")
async def on_startup():
    hub.set_loop(asyncio.get_running_loop())
    runtime.start()


@app.on_event("shutdown")
async def on_shutdown():
    runtime.shutdown()


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/ports")
async def api_ports():
    return {"ports": runtime.list_ports()}


@app.get("/api/state")
async def api_state():
    return runtime.snapshot()


@app.post("/api/pump/connect")
async def api_pump_connect(body: PortBody):
    return _call(runtime.cmd_connect_pump, body.port)


@app.post("/api/pump/disconnect")
async def api_pump_disconnect():
    return _call(runtime.cmd_disconnect_pump)


@app.post("/api/starter/connect")
async def api_starter_connect(body: PortBody):
    return _call(runtime.cmd_connect_starter, body.port)


@app.post("/api/starter/disconnect")
async def api_starter_disconnect():
    return _call(runtime.cmd_disconnect_starter)


@app.post("/api/psu/connect")
async def api_psu_connect(body: PortBody):
    return _call(runtime.cmd_connect_psu, body.port)


@app.post("/api/psu/disconnect")
async def api_psu_disconnect():
    return _call(runtime.cmd_disconnect_psu)


@app.post("/api/pole-pairs")
async def api_pole_pairs(body: PolePairsBody):
    try:
        runtime.cmd_set_pole_pairs_pump(body.pump)
        runtime.cmd_set_pole_pairs_starter(body.starter)
        return _ok()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@app.post("/api/ready")
async def api_ready(body: ReadyBody):
    return _call(runtime.cmd_ready, body.prefix)


@app.post("/api/update-reset")
async def api_update_reset():
    return _call(runtime.cmd_update_reset)


@app.post("/api/run-cycle")
async def api_run_cycle():
    return _call(runtime.cmd_run_cycle)


@app.post("/api/cooling-cycle")
async def api_cooling_cycle(body: ValueBody):
    return _call(runtime.cmd_cooling_cycle, body.value)


@app.post("/api/stop-all")
async def api_stop_all():
    return _call(runtime.cmd_stop_all)


@app.post("/api/valve/on")
async def api_valve_on():
    return _call(runtime.cmd_valve_on)


@app.post("/api/valve/off")
async def api_valve_off():
    return _call(runtime.cmd_valve_off)


@app.post("/api/pump-profile/start")
async def api_pump_profile_start(body: PumpProfileBody):
    return _call(runtime.cmd_start_pump_profile, body.path)


@app.post("/api/pump-profile/stop")
async def api_pump_profile_stop():
    return _call(runtime.cmd_stop_pump_profile)


@app.post("/api/pump/rpm")
async def api_pump_rpm(body: ValueBody):
    return _call(runtime.cmd_set_pump_rpm, body.value)


@app.post("/api/pump/duty")
async def api_pump_duty(body: ValueBody):
    return _call(runtime.cmd_set_pump_duty, body.value)


@app.post("/api/starter/rpm")
async def api_starter_rpm(body: ValueBody):
    return _call(runtime.cmd_set_starter_rpm, body.value)


@app.post("/api/starter/duty")
async def api_starter_duty(body: ValueBody):
    return _call(runtime.cmd_set_starter_duty, body.value)


@app.post("/api/psu/vi")
async def api_psu_vi(body: PsuViBody):
    return _call(runtime.cmd_psu_set_vi, body.v, body.i)


@app.post("/api/psu/output")
async def api_psu_output(body: BoolBody):
    return _call(runtime.cmd_psu_output, body.value)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    q = await hub.subscribe()

    send_task = None
    recv_task = None

    try:
        await websocket.send_json({
            "event": "snapshot",
            "payload": runtime.snapshot(),
        })

        async def sender():
            while True:
                item = await q.get()
                await websocket.send_json(item)

        async def receiver():
            while True:
                msg = await websocket.receive_text()
                if msg == "ping":
                    continue

        send_task = asyncio.create_task(sender())
        recv_task = asyncio.create_task(receiver())

        done, pending = await asyncio.wait(
            {send_task, recv_task},
            return_when=asyncio.FIRST_EXCEPTION,
        )

        for task in done:
            exc = task.exception()
            if exc:
                raise exc

        for task in pending:
            task.cancel()

    except WebSocketDisconnect:
        pass
    finally:
        if send_task is not None and not send_task.done():
            send_task.cancel()
        if recv_task is not None and not recv_task.done():
            recv_task.cancel()
        await hub.unsubscribe(q)
