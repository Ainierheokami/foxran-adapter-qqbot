from __future__ import annotations

from typing import Any

from app.adapters.base.adapter import BasePlatformAdapter
from app.message import (
    ExtensionSegment,
    File,
    Image,
    MediaRef,
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


def public_url(media: MediaRef) -> str | None:
    """A URL QQ can fetch: media without one cannot be uploaded and is sent as text."""
    for value in (media.url, media.file):
        if value and value.startswith(("http://", "https://")):
            return value
    return None


def outgoing_parts(segments: MessageChain) -> list[tuple[str, str]]:
    """Reply segments as QQ parts: ``("text", s)``, ``("link", url)`` or ``(<media kind>, url)``."""
    parts: list[tuple[str, str]] = []
    for seg in segments:
        if isinstance(seg, ExtensionSegment):
            parts.extend(outgoing_parts(MessageChain(segment_registry.fallback(seg))))
        elif isinstance(seg, Text):
            parts.append(("text", seg.text))
        elif isinstance(seg, Reply):
            continue  # QQ replies are always passive replies to the triggering message
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


def parts_as_text(parts: list[tuple[str, str]]) -> str:
    """Plain text with media inlined as URLs (the form guild channels receive)."""
    return "".join(value for _kind, value in parts).strip()


class QQBotAdapter(BasePlatformAdapter):
    """Convert QQ OpenAPI messages to Foxran's portable message segments."""

    def from_platform_format(self, platform_data: Any) -> MessageChain:
        if isinstance(platform_data, dict):
            content = str(platform_data.get("content") or "")
            attachments = platform_data.get("attachments") or []
        else:
            content, attachments = str(platform_data or ""), []
        result = MessageChain([content] if content else [])
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            url = attachment.get("url") or attachment.get("proxy_url")
            if url:
                result.append(Image(media=MediaRef(url=str(url)), summary=attachment.get("filename")))
        return result

    def to_platform_format(self, internal_data: Any) -> str:
        if isinstance(internal_data, Segment):
            internal_data = MessageChain([internal_data])
        if isinstance(internal_data, MessageChain):
            return parts_as_text(outgoing_parts(internal_data))
        return str(internal_data or "")

    def get_platform_prompts(self, session_ctx: Any) -> str:
        return "当前通过 QQ 机器人开放平台回复。支持 Markdown；群聊消息必须回复触发消息；支持通过 URL 发送图片、MP4 视频和 SILK 语音，普通文件会降级为链接。"
