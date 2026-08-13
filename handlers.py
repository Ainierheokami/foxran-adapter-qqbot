from __future__ import annotations

from collections import deque
from typing import Any

import app.api.core as api_core
from app.adapters.control.policy import platform_policy
from app.adapters.message_protocol import bind_platform_id, make_user_message
from app.adapters.qqbot.client import QQBotAPIError, qqbot_client
from app.adapters.qqbot.config import qqbot_config
from app.api.core import active_processors, get_or_create_session_context
from app.logger import setup_logger
from app.tasks.core.session_processor import SessionProcessor
from starlette.websockets import WebSocketState

logger = setup_logger(__name__)
MESSAGE_EVENTS = {
    "C2C_MESSAGE_CREATE",
    "GROUP_AT_MESSAGE_CREATE",
    "GROUP_MESSAGE_CREATE",
    "AT_MESSAGE_CREATE",
    "MESSAGE_CREATE",
    "DIRECT_MESSAGE_CREATE",
}
_seen_ids: deque[str] = deque(maxlen=4096)
_seen_set: set[str] = set()


def _remember(event_id: Any) -> bool:
    value = str(event_id or "")
    if not value:
        return True
    if value in _seen_set:
        return False
    if len(_seen_ids) == _seen_ids.maxlen:
        _seen_set.discard(_seen_ids.popleft())
    _seen_ids.append(value)
    _seen_set.add(value)
    return True


def target_for_event(event_type: str, data: dict[str, Any]) -> tuple[dict[str, str], str, str, str]:
    if event_type in {"GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"}:
        target = {"kind": "group", "id": str(data["group_openid"])}
        return target, "group", str(data.get("author", {}).get("member_openid") or "unknown"), target["id"]
    if event_type == "C2C_MESSAGE_CREATE":
        target = {"kind": "c2c", "id": str(data["author"]["user_openid"])}
        return target, "private", target["id"], target["id"]
    if event_type in {"AT_MESSAGE_CREATE", "MESSAGE_CREATE"}:
        target = {"kind": "channel", "id": str(data["channel_id"])}
        return target, "group", str(data.get("author", {}).get("id") or "unknown"), str(data.get("guild_id") or target["id"])
    target = {"kind": "channel", "id": str(data["channel_id"])}
    return target, "private", str(data.get("author", {}).get("id") or "unknown"), target["id"]


class QQBotReplySender:
    def __init__(self, session_ctx: Any) -> None:
        self.session_ctx = session_ctx

    @property
    def client_state(self) -> WebSocketState:
        return WebSocketState.CONNECTED

    async def send_json(self, data: dict[str, Any]) -> None:
        reply = data.get("reply")
        if reply is None and data.get("type") == "assistant_message":
            reply = data.get("content") or (data.get("message") or {}).get("content")
        reply = str(reply or "").strip()
        if not reply:
            return
        target = self.session_ctx.session_notes.get("qqbot_target")
        msg_id = self.session_ctx.session_notes.get("qqbot_msg_id")
        if not isinstance(target, dict) or not msg_id:
            logger.warning("QQ Bot 回复丢弃：缺少 target 或 msg_id")
            return
        try:
            platform_id = await qqbot_client.send_message(target, reply, str(msg_id))
            message_id = data.get("message_id") or data.get("id")
            if platform_id and message_id:
                self.session_ctx.set_platform_id_for_message(str(message_id), platform_id)
        except QQBotAPIError as exc:
            logger.error("QQ Bot 回复失败: %s", exc)


async def handle_event(event_type: str, data: dict[str, Any], event_id: Any = None) -> None:
    if event_type not in MESSAGE_EVENTS or not isinstance(data, dict) or not _remember(event_id or data.get("id")):
        return
    try:
        target, message_type, user_id, conversation_id = target_for_event(event_type, data)
    except KeyError:
        logger.warning("QQ Bot 事件缺少目标字段: %s", event_type)
        return
    decision = platform_policy.evaluate("qqbot", message_type, user_id, conversation_id if message_type == "group" else None, True)
    if not decision.should_reply:
        return
    cfg = qqbot_config.get()
    session_id = f"qqbot:{target['kind']}:{conversation_id if cfg.get('use_group_as_session', True) else user_id}"
    user = data.get("author") or {}
    try:
        session_ctx = await get_or_create_session_context(session_id, user_id, str(user.get("username") or user.get("user_openid") or user_id), "qqbot")
        session_ctx.session_notes["qqbot_target"] = target
        session_ctx.session_notes["qqbot_msg_id"] = str(data.get("id") or event_id)
        session_ctx.set_websocket(QQBotReplySender(session_ctx))
        from app.data_mappers import get_message_processor
        processor = get_message_processor()
        raw_content = data.get("content") or ""
        # Preserve QQ attachments for the platform adapter while retaining content as raw history.
        processed = await processor.process_incoming_message("qqbot", data, {"role": "user", "content": raw_content})
        message = make_user_message(content=processed.internal, user_id=user_id, user_name=str(user.get("username") or user_id), platform="qqbot", platform_id=data.get("id"), raw_content=raw_content)
        bind_platform_id(session_ctx, message, data.get("id"))
        if session_ctx.session_id not in active_processors:
            if not api_core.core_agent:
                logger.error("QQ Bot 接收消息失败：AI 核心尚未初始化")
                return
            worker = SessionProcessor(session_ctx, api_core.core_agent)
            await worker.start()
            active_processors[session_ctx.session_id] = worker
        await session_ctx.handle_new_message(message)
    except Exception:
        logger.exception("处理 QQ Bot 事件失败 type=%s", event_type)
