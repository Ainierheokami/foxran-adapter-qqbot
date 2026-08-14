from __future__ import annotations

import re
from typing import Any

from app.adapters.base.adapter import BasePlatformAdapter
from app.data_mappers.schemas import ImageSchema, MessageSegments, ReplySchema


class QQBotAdapter(BasePlatformAdapter):
    """Convert QQ OpenAPI messages to Foxran's portable message segments."""

    _image = re.compile(r"\[image,\s*url=(?P<url>[^,\]]+)(?:,\s*summary=(?P<summary>[^\]]+))?\]")

    def from_platform_format(self, platform_data: Any) -> MessageSegments:
        if isinstance(platform_data, dict):
            content = str(platform_data.get("content") or "")
            attachments = platform_data.get("attachments") or []
        else:
            content, attachments = str(platform_data or ""), []
        result = MessageSegments([content] if content else [])
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            url = attachment.get("url") or attachment.get("proxy_url")
            if url:
                result.append(ImageSchema(url=str(url), summary=attachment.get("filename")))
        return result

    def to_platform_format(self, internal_data: Any) -> str:
        if isinstance(internal_data, MessageSegments) or isinstance(internal_data, list):
            return "".join(self.to_platform_format(item) for item in internal_data)
        if isinstance(internal_data, ImageSchema):
            return f"[image, url={internal_data.url}]"
        if isinstance(internal_data, ReplySchema):
            return ""
        return str(internal_data or "")

    def get_platform_prompts(self, session_ctx: Any) -> str:
        return "当前通过 QQ 机器人开放平台回复。群聊消息必须回复触发消息；支持通过 URL 发送图片、MP4 视频和 SILK 语音，普通文件会降级为链接。"
