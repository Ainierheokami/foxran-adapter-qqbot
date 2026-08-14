from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import websockets

from app.adapters.qqbot.client import qqbot_client
from app.adapters.qqbot.config import qqbot_config
from app.adapters.qqbot.handlers import handle_event
from app.logger import setup_logger

logger = setup_logger(__name__)


class QQBotGateway:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._session_id: str | None = None
        self._seq: int | None = None

    async def start(self) -> None:
        cfg = qqbot_config.get()
        if not cfg.get("enabled"):
            logger.info("QQ Bot 网关未启动：适配器已禁用")
            return
        if cfg.get("transport") not in {"websocket", "both"}:
            logger.info("QQ Bot 网关未启动：transport=%s", cfg.get("transport"))
            return
        if self._task:
            logger.debug("QQ Bot 网关已在运行")
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="qqbot-gateway")
        logger.info("QQ Bot 网关启动中：intents=%s", cfg.get("intents"))

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        logger.info("QQ Bot 网关已停止")

    async def _run(self) -> None:
        delay = 2.0
        while not self._stop.is_set():
            try:
                await self._connect()
                delay = 2.0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("QQ Bot 网关连接已断开: %s；%.1f 秒后重试", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, float(qqbot_config.get().get("reconnect_max_delay") or 30))

    async def _connect(self) -> None:
        logger.info("QQ Bot 正在获取 Gateway 地址")
        url = await qqbot_client.gateway_url()
        logger.info("QQ Bot 正在连接 Gateway: %s", url)
        async with websockets.connect(url, ping_interval=None) as ws:
            logger.info("QQ Bot Gateway WebSocket 已连接")
            hello = json.loads(await ws.recv())
            interval = float(hello["d"]["heartbeat_interval"]) / 1000
            token = f"QQBot {await qqbot_client.access_token()}"
            cfg = qqbot_config.get()
            if self._session_id is not None and self._seq is not None:
                logger.info("QQ Bot 正在恢复 Gateway 会话：seq=%s", self._seq)
                await ws.send(json.dumps({"op": 6, "d": {"token": token, "session_id": self._session_id, "seq": self._seq}}))
            else:
                logger.info("QQ Bot 正在鉴权 Gateway：intents=%s", cfg["intents"])
                await ws.send(json.dumps({"op": 2, "d": {"token": token, "intents": int(cfg["intents"]), "shard": [0, max(1, int(cfg.get("shards") or 1))], "properties": {"$os": "windows", "$browser": "foxran", "$device": "foxran"}}}))
            heartbeat = asyncio.create_task(self._heartbeat(ws, interval))
            try:
                async for raw in ws:
                    payload = json.loads(raw)
                    if payload.get("s") is not None:
                        self._seq = int(payload["s"])
                    if payload.get("t") == "READY":
                        self._session_id = payload.get("d", {}).get("session_id")
                        logger.info("QQ Bot Gateway 鉴权成功，已就绪：session=%s", self._session_id)
                    elif payload.get("t") == "RESUMED":
                        logger.info("QQ Bot Gateway 会话恢复成功：seq=%s", self._seq)
                    if payload.get("op") == 0:
                        event_type = str(payload.get("t") or "")
                        logger.debug("QQ Bot Gateway 收到事件：type=%s id=%s seq=%s", event_type, payload.get("id"), self._seq)
                        await handle_event(event_type, payload.get("d") or {}, payload.get("id"))
                    if payload.get("op") == 7:
                        logger.info("QQ Bot Gateway 要求重新连接")
                        return
            finally:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat
                logger.info("QQ Bot Gateway WebSocket 已关闭")

    async def _heartbeat(self, ws: Any, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            await ws.send(json.dumps({"op": 1, "d": self._seq}))


qqbot_gateway = QQBotGateway()
