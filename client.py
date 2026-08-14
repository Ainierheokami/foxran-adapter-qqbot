from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from urllib.request import Request, urlopen

from app.logger import setup_logger
from app.adapters.qqbot.config import qqbot_config

logger = setup_logger(__name__)


class QQBotAPIError(RuntimeError):
    pass


class QQBotClient:
    def __init__(self) -> None:
        self._token = ""
        self._expires_at = 0.0
        self._token_lock = asyncio.Lock()
        self._bot_openid = ""
        self._bot_openid_lock = asyncio.Lock()

    async def access_token(self) -> str:
        async with self._token_lock:
            if self._token and time.time() < self._expires_at - 60:
                return self._token
            cfg = qqbot_config.get()
            app_id, secret = str(cfg.get("app_id") or ""), str(cfg.get("app_secret") or "")
            if not app_id or not secret:
                raise QQBotAPIError("qqbot.yml 缺少 app_id 或 app_secret")
            data = await self._request_json(str(cfg["token_url"]), {"appId": app_id, "clientSecret": secret})
            token = str(data.get("access_token") or "")
            if not token:
                raise QQBotAPIError(f"获取 Access Token 失败: {data}")
            self._token = token
            self._expires_at = time.time() + max(60, int(data.get("expires_in") or 7200))
            return token

    async def gateway_url(self) -> str:
        result = await self.request("GET", "/gateway/bot")
        url = result.get("url")
        if not url:
            raise QQBotAPIError(f"网关地址缺失: {result}")
        return str(url)

    async def bot_openid(self) -> str:
        """Return the current bot's OpenID for full-group mention matching."""
        configured = str(qqbot_config.get().get("bot_openid") or "").strip()
        if configured:
            return configured
        if self._bot_openid:
            return self._bot_openid
        async with self._bot_openid_lock:
            if self._bot_openid:
                return self._bot_openid
            profile = await self.request("GET", "/users/@me")
            value = str(
                profile.get("user_openid")
                or profile.get("openid")
                or profile.get("id")
                or ""
            ).strip()
            if not value:
                raise QQBotAPIError(f"无法从当前机器人资料获取 OpenID: {profile}")
            self._bot_openid = value
            logger.info("QQ Bot 已获取机器人 OpenID，用于全量群消息艾特识别")
            return value

    async def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = qqbot_config.get()
        token = await self.access_token()
        url = str(cfg["api_base_url"]).rstrip("/") + path
        return await self._request_json(url, payload, method=method, headers={"Authorization": f"QQBot {token}"})

    async def send_message(self, target: dict[str, str], content: str, msg_id: str) -> str | None:
        kind, target_id = target["kind"], target["id"]
        paths = {"group": f"/v2/groups/{target_id}/messages", "c2c": f"/v2/users/{target_id}/messages", "channel": f"/channels/{target_id}/messages"}
        result = await self.request("POST", paths[kind], {"content": content, "msg_type": 0, "msg_id": msg_id})
        if result.get("err_code"):
            raise QQBotAPIError(f"发送失败 {result.get('err_code')}: {result.get('message')} trace={result.get('trace_id')}")
        return str(result.get("id")) if result.get("id") else None

    async def _request_json(self, url: str, payload: dict[str, Any] | None = None, *, method: str = "POST", headers: dict[str, str] | None = None) -> dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
        request = Request(url, data=body, method=method, headers={"Content-Type": "application/json", **(headers or {})})
        def execute() -> dict[str, Any]:
            try:
                with urlopen(request, timeout=15) as response:
                    return json.loads(response.read().decode() or "{}")
            except Exception as exc:
                raise QQBotAPIError(f"QQ Bot HTTP {method} {url} 失败: {exc}") from exc
        return await asyncio.to_thread(execute)


qqbot_client = QQBotClient()
