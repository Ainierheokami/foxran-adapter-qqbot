from __future__ import annotations

import mimetypes
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
        return "当前通过 QQ 机器人开放平台回复。支持 Markdown；群聊消息必须回复触发消息；支持通过 URL 发送图片、MP4 视频和 SILK 语音，普通文件会降级为链接。"
