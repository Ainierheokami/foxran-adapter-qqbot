from __future__ import annotations

import base64
import json
import mimetypes
import re
from typing import Any

from app.adapters.base.adapter import BasePlatformAdapter
from app.message import (
    ExtensionSegment,
    Face,
    File,
    Image,
    MediaRef,
    Mention,
    MessageChain,
    Reply,
    Segment,
    Text,
    Unknown,
    Video,
    Voice,
    segment_registry,
)

# QQ rich media the v2 API can upload; anything else is sent as text.
MEDIA_KINDS = {Image: "image", Video: "video", Voice: "voice"}

# 消息里的 @：频道和全量群消息写 <@openid>（频道也有 <@!id>），群消息的新写法是
# <qqbot-at-user id="openid" />；@全体是 <qqbot-at-everyone />。
MENTION_PATTERN = re.compile(
    r"<@!?(?P<legacy>[^>\s]+)>"
    r"|<qqbot-at-user\s+id=\"(?P<user>[^\"]+)\"\s*/?>"
    r"|(?P<everyone><qqbot-at-everyone\s*/?>)"
)


# QQ 小黄脸：<faceType=1,faceId="13",ext="base64">，ext 解出来是 {"text":"[呲牙]"}
FACE_PATTERN = re.compile(r'<faceType=\d+,faceId="(?P<face_id>[^"]*)"(?:,ext="(?P<ext>[^"]*)")?\s*/?>')
_INBOUND_MARKUP = re.compile(f"{MENTION_PATTERN.pattern}|{FACE_PATTERN.pattern}")


def mention_target(match: re.Match) -> str:
    return Mention.ALL if match.group("everyone") else (match.group("legacy") or match.group("user"))


def face_text(ext: str | None) -> str | None:
    try:
        text = json.loads(base64.b64decode(ext or "", validate=True)).get("text")
    except (ValueError, AttributeError):
        return None
    name = str(text or "").strip("[]")
    return name or None


def split_mentions(content: str, names: dict[str, str] | None = None) -> list[Segment | str]:
    """Inbound text with QQ's mention and face markup turned into ``Mention`` / ``Face`` segments."""
    names = names or {}
    pieces: list[Segment | str] = []
    position = 0
    for match in _INBOUND_MARKUP.finditer(content):
        if match.start() > position:
            pieces.append(content[position:match.start()])
        if match.group("face_id") is not None:
            pieces.append(Face(id=match.group("face_id"), text=face_text(match.group("ext"))))
        else:
            target = mention_target(match)
            pieces.append(Mention(target=target, name=names.get(target)))
        position = match.end()
    if position < len(content):
        pieces.append(content[position:])
    return pieces


def mention_names(platform_data: dict) -> dict[str, str]:
    """被 @ 的人的昵称（QQ 给了才有）：openid -> 昵称。"""
    names: dict[str, str] = {}
    for item in platform_data.get("mentions") or []:
        if not isinstance(item, dict):
            continue
        openid = item.get("member_openid") or item.get("user_openid") or item.get("openid") or item.get("id")
        name = item.get("nickname") or item.get("username") or item.get("member_name")
        if openid and name:
            names[str(openid)] = str(name)
    return names


def render_mention(target: str, kind: str) -> str:
    """How a ``("mention", openid)`` part is written for a target kind; private chats have no mentions."""
    if target == Mention.ALL:
        # @全体需要额外权限，被拒会整条发不出去；只写成文字
        return "@全体成员" if kind != "c2c" else ""
    if kind == "group":
        return f'<qqbot-at-user id="{target}" />'
    if kind == "channel":
        return f"<@{target}>"
    return ""


def render_mentions(parts: list[tuple[str, str]], kind: str) -> list[tuple[str, str]]:
    return [("text", render_mention(value, kind)) if part == "mention" else (part, value) for part, value in parts]


def public_url(media: MediaRef) -> str | None:
    """A URL QQ can fetch: media without one cannot be uploaded and is sent as text."""
    for value in (media.url, media.file):
        if value and value.startswith(("http://", "https://")):
            return value
    return None


def outgoing_parts(segments: MessageChain) -> list[tuple[str, str]]:
    """Reply segments as QQ parts: ``("text", s)``, ``("link", url)``, ``("mention", openid)`` or ``(<media kind>, url)``."""
    parts: list[tuple[str, str]] = []
    for seg in segments:
        if isinstance(seg, ExtensionSegment):
            parts.extend(outgoing_parts(MessageChain(segment_registry.fallback(seg))))
        elif isinstance(seg, Text):
            parts.append(("text", seg.text))
        elif isinstance(seg, Reply):
            continue  # QQ replies are always passive replies to the triggering message
        elif isinstance(seg, Mention):
            parts.append(("mention", seg.target))
        elif type(seg) in MEDIA_KINDS:
            url = public_url(seg.media)
            parts.append((MEDIA_KINDS[type(seg)], url) if url else ("text", seg.summary()))
        elif isinstance(seg, File):
            url = public_url(seg.media)
            parts.append(("link", url) if url else ("text", seg.summary()))
        elif isinstance(seg, Unknown):
            parts.append(("text", seg.fallback_text))
        else:
            parts.append(("text", seg.summary()))
    return parts


def _attachment_kind(attachment: dict) -> str:
    """image / video / voice / file，按 content_type 判断，缺失时按文件名猜。"""
    content_type = str(attachment.get("content_type") or "").split(";", 1)[0].strip().lower()
    if not content_type:
        from urllib.parse import urlparse

        url = str(attachment.get("url") or attachment.get("proxy_url") or "")
        for candidate in (str(attachment.get("filename") or ""), urlparse(url).path):
            content_type = mimetypes.guess_type(candidate)[0] or ""
            if content_type:
                break
    if content_type.startswith("image/"):
        return "image"
    if content_type.startswith("video/"):
        return "video"
    # QQ 语音附件的 content_type 就是 "voice"
    if content_type == "voice" or content_type.startswith("audio/"):
        return "voice"
    return "file"


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def attachment_segment(attachment: dict) -> Segment | None:
    url = attachment.get("url") or attachment.get("proxy_url")
    if not url:
        return None
    media = MediaRef(url=str(url))
    kind = _attachment_kind(attachment)
    if kind == "image":
        return Image(media=media, summary=attachment.get("filename"))
    if kind == "video":
        return Video(media=media)
    if kind == "voice":
        return Voice(media=media)
    return File(media=media, name=attachment.get("filename"), size=_int_or_none(attachment.get("size")))


def parts_as_text(parts: list[tuple[str, str]]) -> str:
    """Plain text with media inlined as URLs and mentions as ``@openid`` (for logs and history)."""
    return "".join(f"@{value}" if kind == "mention" else value for kind, value in parts).strip()


class QQBotAdapter(BasePlatformAdapter):
    """Convert QQ OpenAPI messages to Foxran's portable message segments."""

    # Foxran streams reply drafts to the outbound port; it renders them as C2C
    # stream messages (groups and channels still get whole messages).
    supports_streaming_output = True

    def from_platform_format(self, platform_data: Any) -> MessageChain:
        if isinstance(platform_data, dict):
            content = str(platform_data.get("content") or "")
            attachments = platform_data.get("attachments") or []
        else:
            content, attachments = str(platform_data or ""), []
        names = mention_names(platform_data) if isinstance(platform_data, dict) else {}
        result = MessageChain(split_mentions(content, names) if content else [])
        for attachment in attachments:
            if isinstance(attachment, dict) and (segment := attachment_segment(attachment)) is not None:
                result.append(segment)
        return result

    def to_platform_format(self, internal_data: Any) -> str:
        if isinstance(internal_data, Segment):
            internal_data = MessageChain([internal_data])
        if isinstance(internal_data, MessageChain):
            return parts_as_text(outgoing_parts(internal_data))
        return str(internal_data or "")

    def get_platform_prompts(self, session_ctx: Any) -> str:
        return (
            "当前通过 QQ 机器人开放平台回复。支持 Markdown；群聊消息必须回复触发消息；支持通过 URL 发送图片、MP4 视频和 SILK 语音，普通文件会降级为链接。"
            "群里要 @ 某人时写 [at,id=对方的 id]（就是消息里的长 ID），不能 @全体；私聊里不用 @。"
        )
