from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import websockets

from app.adapters.qqbot.client import QQBotClient, qqbot_clients
from app.adapters.qqbot.config import qqbot_config
from app.adapters.qqbot.handlers import handle_event
from app.logger import setup_logger

logger = setup_logger(__name__)


class QQBotGateway:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.client: QQBotClient = qqbot_clients.get(account_id)
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._session_id: str | None = None
        self._seq: int | None = None
        self.state = "stopped"
        self.last_error = ""

    async def start(self) -> None:
        cfg = qqbot_config.get_account(self.account_id)
        if not cfg.get("enabled"):
            self.state = "disabled"
            logger.info("QQ Bot 网关未启动：account=%s 适配器已禁用", self.account_id)
            return
        if cfg.get("transport") not in {"websocket", "both"}:
            self.state = "webhook"
            logger.info("QQ Bot 网关未启动：account=%s transport=%s", self.account_id, cfg.get("transport"))
            return
        if self._task:
            logger.debug("QQ Bot 网关已在运行")
            return
        self._stop.clear()
        self.state = "connecting"
        self._task = asyncio.create_task(self._run(), name=f"qqbot-gateway-{self.account_id}")
        logger.info("QQ Bot 网关启动中：account=%s intents=%s", self.account_id, cfg.get("intents"))

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        self.state = "stopped"
        logger.info("QQ Bot 网关已停止：account=%s", self.account_id)

    async def _run(self) -> None:
        delay = 2.0
        while not self._stop.is_set():
            try:
                await self._connect()
                delay = 2.0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.state = "retrying"
                self.last_error = str(exc)
                logger.warning("QQ Bot 网关连接已断开：account=%s error=%s；%.1f 秒后重试", self.account_id, exc, delay)
            await asyncio.sleep(delay)
            cfg = qqbot_config.get_account(self.account_id)
            delay = min(delay * 2, float(cfg.get("reconnect_max_delay") or 30))

    async def _connect(self) -> None:
        self.state = "connecting"
        logger.info("QQ Bot 正在获取 Gateway 地址：account=%s", self.account_id)
        url = await self.client.gateway_url()
        logger.info("QQ Bot 正在连接 Gateway：account=%s url=%s", self.account_id, url)
        async with websockets.connect(url, ping_interval=None) as ws:
            self.state = "connected"
            self.last_error = ""
            logger.info("QQ Bot Gateway WebSocket 已连接：account=%s", self.account_id)
            hello = json.loads(await ws.recv())
            interval = float(hello["d"]["heartbeat_interval"]) / 1000
            token = f"QQBot {await self.client.access_token()}"
            cfg = qqbot_config.get_account(self.account_id)
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
                        logger.info("QQ Bot Gateway 鉴权成功，已就绪：account=%s session=%s", self.account_id, self._session_id)
                    elif payload.get("t") == "RESUMED":
                        logger.info("QQ Bot Gateway 会话恢复成功：seq=%s", self._seq)
                    if payload.get("op") == 0:
                        event_type = str(payload.get("t") or "")
                        logger.debug("QQ Bot Gateway 收到事件：type=%s id=%s seq=%s", event_type, payload.get("id"), self._seq)
                        await handle_event(event_type, payload.get("d") or {}, payload.get("id"), account_id=self.account_id, client=self.client)
                    if payload.get("op") == 7:
                        logger.info("QQ Bot Gateway 要求重新连接")
                        return
            finally:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat
                if not self._stop.is_set():
                    self.state = "retrying"
                logger.info("QQ Bot Gateway WebSocket 已关闭：account=%s", self.account_id)

    async def _heartbeat(self, ws: Any, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            await ws.send(json.dumps({"op": 1, "d": self._seq}))


class QQBotGatewayManager:
    def __init__(self) -> None:
        self._gateways: dict[str, QQBotGateway] = {}

    async def start(self) -> None:
        accounts = qqbot_config.get_accounts(force_reload=True)
        wanted = {str(account["id"]): account for account in accounts}
        for account_id in list(self._gateways):
            if account_id not in wanted:
                await self._gateways.pop(account_id).stop()
        qqbot_clients.prune(set(wanted))
        for account_id in wanted:
            gateway = self._gateways.setdefault(account_id, QQBotGateway(account_id))
            await gateway.start()

    async def stop(self) -> None:
        for gateway in list(self._gateways.values()):
            await gateway.stop()
        self._gateways.clear()
        qqbot_clients.clear()

    def status(self) -> dict[str, dict[str, str]]:
        return {
            account_id: {"state": gateway.state, "last_error": gateway.last_error}
            for account_id, gateway in self._gateways.items()
        }


qqbot_gateway = QQBotGatewayManager()
