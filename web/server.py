"""
FastAPI Real-Time Web Monitoring Server.

Hosts WebSocket streaming endpoint (/ws/stream) and static dashboard assets
for institutional-grade live visualization of the BTC Straddle strategy.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("straddle.web")

# Paths
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="NautilusTrader BTC Straddle Dashboard", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class WebSocketHub:
    """Manages active WebSocket connections and broadcasts real-time payloads."""

    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.log_history: List[Dict[str, Any]] = []
        self._strategy_instance: Optional[Any] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def register_strategy(self, strategy: Any) -> None:
        """Binds the active strategy instance for state snapshots and manual actions."""
        self._strategy_instance = strategy

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket client connected. Total active: {len(self.active_connections)}")

        # Send initial snapshot upon connection
        snapshot = self.get_snapshot()
        await websocket.send_json({
            "event": "snapshot",
            "timestamp": asyncio.get_event_loop().time(),
            "data": snapshot,
            "logs": self.log_history[-30:],
        })

    def disconnect(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket client disconnected. Remaining: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """Broadcasts event dictionary to all connected WebSockets."""
        if not self.active_connections:
            return

        dead_connections: Set[WebSocket] = set()
        text_data = json.dumps(message)
        for connection in self.active_connections:
            try:
                await connection.send_text(text_data)
            except Exception:
                dead_connections.add(connection)

        for dead in dead_connections:
            self.active_connections.discard(dead)

    def dispatch_from_strategy(self, payload: Dict[str, Any]) -> None:
        """Threadsafe queue insertion called directly from NautilusTrader strategy callbacks."""
        if payload.get("event") in ("order_event", "risk_alert", "position_update", "discovery_update"):
            self.log_history.append(payload)
            if len(self.log_history) > 200:
                self.log_history.pop(0)

        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self.event_queue.put(payload), self._loop)
        else:
            try:
                self.event_queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass

    def get_snapshot(self) -> Dict[str, Any]:
        """Fetches current strategy snapshot or default state."""
        if self._strategy_instance and hasattr(self._strategy_instance, "get_state_snapshot"):
            try:
                return self._strategy_instance.get_state_snapshot()
            except Exception as e:
                logger.error(f"Error getting strategy snapshot: {e}")

        return {
            "status": "INITIALIZING",
            "underlying_price": "0",
            "is_entered": False,
            "is_closed": False,
        }

    async def run_broadcaster(self) -> None:
        """Continuous async loop draining event queue and broadcasting to subscribers."""
        logger.info("WebSocket broadcaster task running...")
        while True:
            try:
                payload = await self.event_queue.get()
                await self.broadcast(payload)
                self.event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Broadcaster exception: {e}")
                await asyncio.sleep(0.05)


hub = WebSocketHub()


# -----------------------------------------------------------------------------
# REST & Static Endpoints
# -----------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    loop = asyncio.get_event_loop()
    hub.set_loop(loop)
    asyncio.create_task(hub.run_broadcaster())


@app.get("/")
async def get_index() -> FileResponse:
    index_file = STATIC_DIR / "index.html"
    return FileResponse(index_file)


@app.get("/api/status")
async def get_status() -> JSONResponse:
    return JSONResponse(hub.get_snapshot())


@app.post("/api/manual_exit")
async def post_manual_exit() -> JSONResponse:
    if hub._strategy_instance and hasattr(hub._strategy_instance, "_close_positions"):
        try:
            hub._strategy_instance._close_positions(reason="MANUAL_UI_TRIGGER")
            return JSONResponse({"status": "SUCCESS", "message": "Manual exit submitted"})
        except Exception as e:
            return JSONResponse({"status": "ERROR", "message": str(e)}, status_code=500)
    return JSONResponse({"status": "ERROR", "message": "Strategy not initialized"}, status_code=400)


@app.websocket("/ws/stream")
async def websocket_stream_endpoint(websocket: WebSocket) -> None:
    await hub.connect(websocket)
    try:
        while True:
            # Keep-alive ping/pong listener
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        hub.disconnect(websocket)
    except Exception as e:
        logger.debug(f"WebSocket connection error: {e}")
        hub.disconnect(websocket)


# Mount static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
