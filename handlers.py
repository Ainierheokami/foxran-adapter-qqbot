from __future__ import annotations

from collections import deque
import re
from typing import Any

from typing import Optional

from app.adapters.qqbot.adapter import outgoing_parts, parts_as_text
from app.adapters.qqbot.client import QQBotAPIError, QQBotClient, qqbot_client, qqbot_clients
from app.inbound import ConversationRef, InboundEvent, PlatformFacts, Sender, SessionIdOptions
from app.inbound import pipeline
from app.logger import setup_logger
from app.message import MessageChain
from app.outbound import OutboundMessage

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
MENTION_PATTERN = re.compile(r"<@!?([^>\s]+)>")


def _remember(event_id: Any, account_id: str = "default") -> bool:
    value = f"{account_id}:{event_id}" if event_id else ""
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


def mention_openids(data: dict[str, Any]) -> set[str]:
    """Extract mentioned OpenIDs from a full QQ group message payload."""
    values = {match.group(1) for match in MENTION_PATTERN.finditer(str(data.get("content") or ""))}
    mentions = data.get("mentions")
    if isinstance(mentions, list):
        for mention in mentions:
            if isinstance(mention, dict):
                value = mention.get("user_openid") or mention.get("openid") or mention.get("id")
                if value:
                    values.add(str(value))
    return values


async def event_is_mention(
    event_type: str,
    data: dict[str, Any],
    client: QQBotClient | None = None,
) -> bool:
    """Return whether an event explicitly mentions this bot, including full-group events."""
    if event_type in {"GROUP_AT_MESSAGE_CREATE", "AT_MESSAGE_CREATE"}:
        return True
    if event_type != "GROUP_MESSAGE_CREATE":
        return False
    mentioned = mention_openids(data)
    if not mentioned:
        return False
    try:
        return await (client or qqbot_client).bot_openid() in mentioned
    except QQBotAPIError as exc:
        logger.warning("QQ Bot 无法识别全量群消息中的艾特: %s", exc)
        return False


class QQBotBinding:
    """How the framework pipeline reaches back into QQBot for one event."""

    def __init__(self, target: dict[str, str], msg_id: str, account_id: str) -> None:
        self.target = target
        self.msg_id = msg_id
        self.account_id = account_id

    def on_conversation(self, session: Any) -> None:
        # QQ replies are passive: they go to the channel/group/user of the latest
        # message and must quote its id.
        session.platform_state["qqbot_target"] = self.target
        session.platform_state["qqbot_msg_id"] = self.msg_id

    async def before_agent(self, session: Any) -> None:
        pass

    async def fetch_message(self, session: Any, platform_message_id: str) -> Any:
        return None

    def log_message(self, segments: Any) -> None:
        pass

    def on_self_message(self, session: Any) -> None:
        pass


class QQBotOutboundPort:
    """Foxran outbound port for the QQ Bot OpenAPI (core-refactor R5a)."""

    def encode(self, session: Any, segments: MessageChain) -> OutboundMessage:
        parts = outgoing_parts(segments)
        return OutboundMessage(text=parts_as_text(parts), payload=parts)

    async def deliver(self, session: Any, conversation: Any, message: OutboundMessage, message_id: Optional[str]) -> Optional[str]:
        if not message.text:
            return None
        target = session.platform_state.get("qqbot_target")
        msg_id = session.platform_state.get("qqbot_msg_id")
        if not isinstance(target, dict) or not msg_id:
            logger.warning("QQ Bot 回复丢弃：缺少 target 或 msg_id")
            return None
        client = qqbot_clients.get(conversation.account_id)
        try:
            logger.info("QQ Bot 正在发送回复：target=%s msg_id=%s content=%s", target.get("id"), msg_id, message.text[:200])
            platform_id = await client.send_message(target, message.payload, str(msg_id))
        except QQBotAPIError as exc:
            logger.error("QQ Bot 回复失败: %s", exc)
            return None
        logger.info("QQ Bot 回复发送成功：target=%s platform_message_id=%s", target.get("id"), platform_id)
        return platform_id

    async def send_action(self, account_id: str, action: str, params: dict[str, Any], echo: Optional[str] = None) -> bool:
        logger.warning("QQ Bot 不支持平台动作: %s", action)
        return False


qqbot_outbound_port = QQBotOutboundPort()


async def handle_event(
    event_type: str,
    data: dict[str, Any],
    event_id: Any = None,
    *,
    account_id: str = "default",
    client: QQBotClient | None = None,
) -> None:
    client = client or qqbot_clients.get(account_id)
    if event_type not in MESSAGE_EVENTS:
        logger.debug("QQ Bot 忽略非消息事件：type=%s", event_type)
        return
    if not isinstance(data, dict):
        logger.warning("QQ Bot 忽略格式错误的消息事件：type=%s", event_type)
        return
    if not _remember(event_id or data.get("id"), account_id):
        logger.debug("QQ Bot 忽略重复事件：type=%s id=%s", event_type, event_id or data.get("id"))
        return
    try:
        target, message_type, user_id, conversation_id = target_for_event(event_type, data)
    except KeyError:
        logger.warning("QQ Bot 事件缺少目标字段: %s", event_type)
        return
    is_mention = await event_is_mention(event_type, data, client)
    cfg = client.config()
    bot_id = str(cfg.get("bot_openid") or cfg.get("app_id") or account_id)
    content = str(data.get("content") or "")
    logger.info(
        "QQ Bot 收到消息：account=%s type=%s target=%s user=%s mention=%s content=%s",
        account_id, event_type, target["id"], user_id, is_mention, content[:200],
    )
    user = data.get("author") or {}
    message_id = str(data.get("id") or event_id or "")
    # Session ids keep the format already stored in history.
    session_key = f"{target['kind']}:{conversation_id if cfg.get('use_group_as_session', True) else user_id}"
    try:
        await pipeline.submit(InboundEvent(
            kind="message",
            conversation=ConversationRef(
                platform="qqbot",
                scope="group" if message_type == "group" else "private",
                id=conversation_id,
                account_id=account_id,
                self_id=bot_id,
            ),
            sender=Sender(id=user_id, name=str(user.get("username") or user.get("user_openid") or user_id)),
            raw_content=data,
            raw_text=content,
            binding=QQBotBinding(target, message_id, account_id),
            session_options=SessionIdOptions(prefix="qqbot", include_bot_id=False),
            session_key=session_key,
            platform_message_id=message_id or None,
            facts=PlatformFacts(mentions_self=is_mention, mentioned_ids=tuple(sorted(mention_openids(data)))),
            raw_event=data,
        ))
    except Exception:
        logger.exception("处理 QQ Bot 事件失败 type=%s", event_type)
