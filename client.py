from __future__ import annotations

import asyncio
from collections import OrderedDict
import json
import re
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app.logger import setup_logger
from app.adapters.qqbot.config import qqbot_config

logger = setup_logger(__name__)


class QQBotAPIError(RuntimeError):
    pass


_MEDIA_TAG = re.compile(
    r"\[(?P<kind>image|video|voice|file)\s*,\s*url=(?P<url>[^,\]\s]+)(?:\s*,[^\]]*)?\]",
    re.IGNORECASE,
)
_MEDIA_FILE_TYPES = {"image": 1, "video": 2, "voice": 3}
_MARKDOWN_MARKERS = re.compile(
    r"(?m)(?:^\s{0,3}(?:#{1,6}\s+|[-*+]\s+|\d+[.)]\s+|>\s+|```)|"
    r"\*\*[^*\n]+\*\*|__[^_\n]+__|`[^`\n]+`|\[[^\]\n]+\]\([^\s)]+\))"
)
_MAX_REPLY_SEQUENCE_KEYS = 4096


def _outgoing_parts(content: str) -> tuple[str, list[tuple[str, str]]]:
    """Split Foxran media tags from text; unsupported files degrade to links."""
    media: list[tuple[str, str]] = []

    def replace(match: re.Match[str]) -> str:
        kind, url = match.group("kind").lower(), match.group("url")
        if kind in _MEDIA_FILE_TYPES:
            media.append((kind, url))
            return ""
        return url

    text = _MEDIA_TAG.sub(replace, content)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text, media


def _looks_like_markdown(content: str) -> bool:
    return bool(_MARKDOWN_MARKERS.search(content))


class QQBotClient:
    def __init__(self, account_id: str = "default") -> None:
        self.account_id = account_id
        self._token = ""
        self._expires_at = 0.0
        self._token_lock = asyncio.Lock()
        self._bot_openid = ""
        self._bot_openid_lock = asyncio.Lock()
        self._reply_sequences: OrderedDict[tuple[str, str], int] = OrderedDict()
        self._reply_sequence_lock = asyncio.Lock()

    async def access_token(self) -> str:
        async with self._token_lock:
            if self._token and time.time() < self._expires_at - 60:
                return self._token
            cfg = self.config()
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
        configured = str(self.config().get("bot_openid") or "").strip()
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
        cfg = self.config()
        token = await self.access_token()
        url = str(cfg["api_base_url"]).rstrip("/") + path
        return await self._request_json(url, payload, method=method, headers={"Authorization": f"QQBot {token}"})

    def config(self, force_reload: bool = False) -> dict[str, Any]:
        cfg = qqbot_config.get_account(self.account_id, force_reload=force_reload)
        if not cfg:
            raise QQBotAPIError(f"QQ Bot 账户不存在: {self.account_id}")
        return cfg

    async def send_message(self, target: dict[str, str], content: str, msg_id: str) -> str | None:
        kind, target_id = target["kind"], target["id"]
        message_paths = {
            "group": f"/v2/groups/{target_id}/messages",
            "c2c": f"/v2/users/{target_id}/messages",
            "channel": f"/channels/{target_id}/messages",
        }
        if kind not in message_paths:
            raise QQBotAPIError(f"不支持的消息目标类型: {kind}")

        if kind == "channel":
            # Guild channels use the legacy message API rather than v2 rich media.
            channel_content = _MEDIA_TAG.sub(lambda match: match.group("url"), content).strip()
            return self._message_id(await self._send_channel_text(
                message_paths[kind], channel_content, msg_id
            ))

        text, media_items = _outgoing_parts(content)
        if not media_items:
            sequence = await self._reserve_reply_sequences(message_paths[kind], msg_id, 1)
            return self._message_id(await self._send_v2_text(
                message_paths[kind], text, msg_id, sequence
            ))

        file_path = (
            f"/v2/groups/{target_id}/files"
            if kind == "group"
            else f"/v2/users/{target_id}/files"
        )
        part_count = len(media_items) + bool(text)
        sequence = await self._reserve_reply_sequences(message_paths[kind], msg_id, part_count)
        platform_id: str | None = None
        if text:
            platform_id = self._message_id(await self._send_v2_text(
                message_paths[kind], text, msg_id, sequence
            ))
            sequence += 1

        for media_kind, url in media_items:
            uploaded = await self.request("POST", file_path, {
                "file_type": _MEDIA_FILE_TYPES[media_kind],
                "url": url,
                "srv_send_msg": False,
            })
            file_info = uploaded.get("file_info") or (uploaded.get("media") or {}).get("file_info")
            if not file_info:
                raise QQBotAPIError(f"QQ Bot {media_kind} 上传成功但响应缺少 file_info: {uploaded}")
            logger.info("QQ Bot 富媒体上传完成：target=%s type=%s", target_id, media_kind)
            platform_id = self._message_id(await self._send_v2_payload(
                message_paths[kind],
                {
                    "msg_type": 7,
                    "media": {"file_info": file_info},
                    "msg_id": msg_id,
                    "msg_seq": sequence,
                },
            ))
            sequence += 1
        return platform_id

    async def _reserve_reply_sequences(self, path: str, msg_id: str, count: int) -> int:
        """Reserve unique msg_seq values across separate replies to one QQ message."""
        key = (path, msg_id)
        async with self._reply_sequence_lock:
            first = self._reply_sequences.pop(key, 1)
            self._reply_sequences[key] = first + count
            while len(self._reply_sequences) > _MAX_REPLY_SEQUENCE_KEYS:
                self._reply_sequences.popitem(last=False)
            return first

    def _markdown_enabled(self) -> bool:
        return bool(self.config().get("markdown_enabled", True))

    async def _send_channel_text(self, path: str, text: str, msg_id: str) -> dict[str, Any]:
        if self._markdown_enabled() and _looks_like_markdown(text):
            try:
                return await self.request("POST", path, {
                    "markdown": {"content": text}, "msg_id": msg_id,
                })
            except QQBotAPIError as exc:
                if not self._can_fallback_markdown(exc):
                    raise
                logger.warning("QQ Bot Markdown 被拒绝，降级为纯文本：%s", exc)
        return await self.request("POST", path, {"content": text, "msg_id": msg_id})

    async def _send_v2_text(
        self,
        path: str,
        text: str,
        msg_id: str,
        msg_seq: int,
    ) -> dict[str, Any]:
        if self._markdown_enabled() and _looks_like_markdown(text):
            try:
                return await self._send_v2_payload(path, {
                    "msg_type": 2,
                    "markdown": {"content": text},
                    "msg_id": msg_id,
                    "msg_seq": msg_seq,
                })
            except QQBotAPIError as exc:
                if not self._can_fallback_markdown(exc):
                    raise
                logger.warning("QQ Bot Markdown 被拒绝，降级为纯文本：%s", exc)
        return await self._send_v2_payload(path, {
            "content": text,
            "msg_type": 0,
            "msg_id": msg_id,
            "msg_seq": msg_seq,
        })

    @staticmethod
    def _can_fallback_markdown(exc: QQBotAPIError) -> bool:
        detail = str(exc)
        return "HTTP 400" in detail or detail.startswith("发送失败 ")

    async def _send_v2_payload(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.request("POST", path, payload)
        if result.get("err_code"):
            raise QQBotAPIError(f"发送失败 {result.get('err_code')}: {result.get('message')} trace={result.get('trace_id')}")
        return result

    @staticmethod
    def _message_id(result: dict[str, Any]) -> str | None:
        return str(result.get("id")) if result.get("id") else None

    async def _request_json(self, url: str, payload: dict[str, Any] | None = None, *, method: str = "POST", headers: dict[str, str] | None = None) -> dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
        request = Request(url, data=body, method=method, headers={"Content-Type": "application/json", **(headers or {})})
        def execute() -> dict[str, Any]:
            try:
                with urlopen(request, timeout=15) as response:
                    return json.loads(response.read().decode() or "{}")
            except HTTPError as exc:
                detail = exc.read().decode(errors="replace").strip()
                raise QQBotAPIError(
                    f"QQ Bot HTTP {method} {url} 失败: HTTP {exc.code} {exc.reason}; "
                    f"response={detail[:1000] or '<empty>'}"
                ) from exc
            except Exception as exc:
                raise QQBotAPIError(f"QQ Bot HTTP {method} {url} 失败: {exc}") from exc
        return await asyncio.to_thread(execute)


class QQBotClientManager:
    def __init__(self) -> None:
        self._clients: dict[str, QQBotClient] = {}

    def get(self, account_id: str = "default") -> QQBotClient:
        account_id = str(account_id or "default")
        return self._clients.setdefault(account_id, QQBotClient(account_id))

    def prune(self, account_ids: set[str]) -> None:
        for account_id in list(self._clients):
            if account_id not in account_ids:
                self._clients.pop(account_id, None)

    def clear(self) -> None:
        self._clients.clear()


qqbot_clients = QQBotClientManager()
# Backwards-compatible default client for external integrations.
qqbot_client = qqbot_clients.get("default")
