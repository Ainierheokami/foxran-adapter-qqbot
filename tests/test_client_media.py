from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types
import unittest


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


class _Config:
    @staticmethod
    def get_account(_account_id, force_reload=False):
        return {"api_base_url": "https://api.bot.qq.com"}


def _load_client_module():
    app = types.ModuleType("app")
    adapters = types.ModuleType("app.adapters")
    qqbot = types.ModuleType("app.adapters.qqbot")
    logger = types.ModuleType("app.logger")
    logger.setup_logger = lambda _name: _Logger()
    config = types.ModuleType("app.adapters.qqbot.config")
    config.qqbot_config = _Config()
    sys.modules.update({
        "app": app,
        "app.adapters": adapters,
        "app.adapters.qqbot": qqbot,
        "app.logger": logger,
        "app.adapters.qqbot.config": config,
    })
    path = Path(__file__).resolve().parents[1] / "client.py"
    spec = importlib.util.spec_from_file_location("qqbot_client_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


client_module = _load_client_module()


class RecordingClient(client_module.QQBotClient):
    def __init__(self):
        super().__init__()
        self.calls = []

    async def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path.endswith("/files"):
            return {"file_info": f"uploaded-{payload['file_type']}"}
        return {"id": f"message-{len(self.calls)}"}

    def config(self, force_reload=False):
        return {"markdown_enabled": True}


class RejectMarkdownClient(RecordingClient):
    async def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if payload and payload.get("msg_type") == 2:
            raise client_module.QQBotAPIError(
                "QQ Bot HTTP POST failed: HTTP 400 Bad Request"
            )
        return {"id": f"message-{len(self.calls)}"}


class QQBotMediaSendingTests(unittest.IsolatedAsyncioTestCase):
    async def test_markdown_reply_uses_msg_type_two(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"},
            "## 结果\n\n- 第一项\n- 第二项",
            "source-message",
        )

        self.assertEqual(client.calls[0][2], {
            "msg_type": 2,
            "markdown": {"content": "## 结果\n\n- 第一项\n- 第二项"},
            "msg_id": "source-message",
            "msg_seq": 1,
        })

    async def test_plain_sentence_remains_text(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"}, "普通回复", "source-message"
        )

        self.assertEqual(client.calls[0][2]["msg_type"], 0)

    async def test_rejected_markdown_falls_back_to_text_with_same_sequence(self):
        client = RejectMarkdownClient()

        result = await client.send_message(
            {"kind": "group", "id": "group-1"}, "**重点**", "source-message"
        )

        self.assertEqual(result, "message-2")
        self.assertEqual([call[2]["msg_type"] for call in client.calls], [2, 0])
        self.assertEqual([call[2]["msg_seq"] for call in client.calls], [1, 1])

    async def test_channel_markdown_uses_markdown_payload(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "channel", "id": "channel-1"}, "# 标题", "source-message"
        )

        self.assertEqual(client.calls[0][2], {
            "markdown": {"content": "# 标题"},
            "msg_id": "source-message",
        })

    async def test_markdown_text_keeps_sequence_before_media(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"},
            "**视频已生成**\n[video,url=https://example.com/a.mp4]",
            "source-message",
        )

        message_calls = [call for call in client.calls if call[1].endswith("/messages")]
        self.assertEqual(message_calls[0][2]["msg_type"], 2)
        self.assertEqual(message_calls[0][2]["msg_seq"], 1)
        self.assertEqual(message_calls[1][2]["msg_type"], 7)
        self.assertEqual(message_calls[1][2]["msg_seq"], 2)

    async def test_group_video_is_uploaded_then_sent_as_rich_media(self):
        client = RecordingClient()

        result = await client.send_message(
            {"kind": "group", "id": "group-1"},
            "[video,url=https://example.com/cache/video]",
            "source-message",
        )

        self.assertEqual(result, "message-2")
        self.assertEqual(client.calls, [
            ("POST", "/v2/groups/group-1/files", {
                "file_type": 2,
                "url": "https://example.com/cache/video",
                "srv_send_msg": False,
            }),
            ("POST", "/v2/groups/group-1/messages", {
                "msg_type": 7,
                "media": {"file_info": "uploaded-2"},
                "msg_id": "source-message",
                "msg_seq": 1,
            }),
        ])

    async def test_text_and_media_increment_reply_sequence(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "c2c", "id": "user-1"},
            "生成完成\n[image,url=https://example.com/a.jpg]\n[video,url=https://example.com/a.mp4]",
            "source-message",
        )

        message_calls = [call for call in client.calls if call[1].endswith("/messages")]
        self.assertEqual([call[2]["msg_seq"] for call in message_calls], [1, 2, 3])
        self.assertEqual(message_calls[0][2]["content"], "生成完成")
        self.assertEqual(message_calls[1][2]["media"], {"file_info": "uploaded-1"})
        self.assertEqual(message_calls[2][2]["media"], {"file_info": "uploaded-2"})

    async def test_general_file_degrades_to_text_url(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"},
            "下载：[file,url=https://example.com/archive.zip]",
            "source-message",
        )

        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0][2], {
            "content": "下载：https://example.com/archive.zip",
            "msg_type": 0,
            "msg_id": "source-message",
            "msg_seq": 1,
        })


if __name__ == "__main__":
    unittest.main()
