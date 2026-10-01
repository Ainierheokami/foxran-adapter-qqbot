from __future__ import annotations

from typing import Any

from app.adapters.base.adapter import BasePlatformAdapter
from app.message import ExtensionSegment, Image, MediaRef, MessageChain, Reply, Segment, Text, segment_registry


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
        if isinstance(internal_data, MessageChain):
            return "".join(self._render_segment(seg) for seg in internal_data)
        if isinstance(internal_data, Segment):
            return self._render_segment(internal_data)
        return str(internal_data or "")

    def _render_segment(self, seg: Segment) -> str:
        if isinstance(seg, Text):
            return seg.text
        if isinstance(seg, Image):
            return f"[image, url={seg.media.url}]"
        if isinstance(seg, Reply):
            return ""
        if isinstance(seg, ExtensionSegment):
            return "".join(self._render_segment(item) for item in segment_registry.fallback(seg))
        # REMOVE-IN: R5 — the QQ client still parses agent tags for media;
        # R5 makes encode return a structured payload.
        from app.agent.codec import tag_codec

        return tag_codec.render_segment(seg)

    def get_platform_prompts(self, session_ctx: Any) -> str:
        return "当前通过 QQ 机器人开放平台回复。支持 Markdown；群聊消息必须回复触发消息；支持通过 URL 发送图片、MP4 视频和 SILK 语音，普通文件会降级为链接。"
