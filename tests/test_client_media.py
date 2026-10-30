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
    stubs = {
        "app": app,
        "app.adapters": adapters,
        "app.adapters.qqbot": qqbot,
        "app.logger": logger,
        "app.adapters.qqbot.config": config,
    }
    # Stub the framework only while client.py imports, so tests that run
    # inside Foxran still see the real ``app`` package afterwards.
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        path = Path(__file__).resolve().parents[1] / "client.py"
        spec = importlib.util.spec_from_file_location("qqbot_client_under_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous
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
            [("text", "## 结果\n\n- 第一项\n- 第二项")],
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
            {"kind": "group", "id": "group-1"}, [("text", "普通回复")], "source-message"
        )

        self.assertEqual(client.calls[0][2]["msg_type"], 0)

    async def test_rejected_markdown_falls_back_to_text_with_same_sequence(self):
        client = RejectMarkdownClient()

        result = await client.send_message(
            {"kind": "group", "id": "group-1"}, [("text", "**重点**")], "source-message"
        )

        self.assertEqual(result, "message-2")
        self.assertEqual([call[2]["msg_type"] for call in client.calls], [2, 0])
        self.assertEqual([call[2]["msg_seq"] for call in client.calls], [1, 1])

    async def test_channel_markdown_uses_markdown_payload(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "channel", "id": "channel-1"}, [("text", "# 标题")], "source-message"
        )

        self.assertEqual(client.calls[0][2], {
            "markdown": {"content": "# 标题"},
            "msg_id": "source-message",
        })

    async def test_markdown_text_keeps_sequence_before_media(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"},
            [("text", "**视频已生成**\n"), ("video", "https://example.com/a.mp4")],
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
            [("video", "https://example.com/cache/video")],
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

    async def test_separate_replies_to_same_message_keep_incrementing_sequence(self):
        client = RecordingClient()
        target = {"kind": "group", "id": "group-1"}

        await client.send_message(target, [("text", "图片生成中")], "source-message")
        await client.send_message(
            target,
            [("image", "https://example.com/result.jpg")],
            "source-message",
        )

        message_calls = [call for call in client.calls if call[1].endswith("/messages")]
        self.assertEqual([call[2]["msg_seq"] for call in message_calls], [1, 2])

    async def test_text_and_media_increment_reply_sequence(self):
        client = RecordingClient()

        await client.send_message(
            {"kind": "c2c", "id": "user-1"},
            [("text", "生成完成\n"), ("image", "https://example.com/a.jpg"), ("text", "\n"), ("video", "https://example.com/a.mp4")],
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
            [("text", "下载："), ("link", "https://example.com/archive.zip")],
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


class QQBotLocalImageUploadTests(unittest.IsolatedAsyncioTestCase):
    """Production 2026-10-25: a 3.67MB PNG on our own cache URL timed out while QQ pulled it."""

    def setUp(self):
        self._saved = (client_module._local_fetch_url, client_module._read_local)

    def tearDown(self):
        client_module._local_fetch_url, client_module._read_local = self._saved

    @staticmethod
    def _big_png() -> bytes:
        import os
        from io import BytesIO
        from PIL import Image

        image = Image.frombytes("RGB", (1200, 1600), os.urandom(1200 * 1600 * 3))
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        return buffer.getvalue()

    async def test_own_cached_image_is_sent_as_shrunk_file_data(self):
        import base64
        from io import BytesIO
        from PIL import Image

        png = self._big_png()
        self.assertGreater(len(png), client_module._INLINE_IMAGE_MAX_BYTES)
        client_module._local_fetch_url = lambda url: "http://127.0.0.1:8000/api/media/cache/X"
        client_module._read_local = lambda url: png
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"},
            [("image", "https://api.example.test/api/media/cache/X")],
            "source-message",
        )

        upload = client.calls[0][2]
        self.assertNotIn("url", upload)
        data = base64.b64decode(upload["file_data"])
        self.assertLess(len(data), len(png))
        with Image.open(BytesIO(data)) as sent:
            self.assertEqual(sent.format, "JPEG")

    async def test_webp_sticker_is_converted_before_upload(self):
        """2026-10-30：本服务上的 webp 表情包原样上传，QQ 回「富媒体文件格式不支持」。"""
        import base64
        from io import BytesIO
        from PIL import Image

        def webp(frames):
            buffer = BytesIO()
            images = [Image.new("RGBA", (64, 64), (255, 0, 0, 255)), Image.new("RGBA", (64, 64), (0, 0, 255, 255))]
            images[0].save(buffer, format="WEBP", save_all=frames > 1, append_images=images[1:frames], duration=80)
            return buffer.getvalue()

        client_module._local_fetch_url = lambda url: url
        for frames, expected in ((1, "PNG"), (2, "GIF")):
            client_module._read_local = lambda url, data=webp(frames): data
            client = RecordingClient()
            await client.send_message(
                {"kind": "group", "id": "group-1"}, [("image", "https://api.example.test/api/stickers/send/a.webp")], "m"
            )
            with Image.open(BytesIO(base64.b64decode(client.calls[0][2]["file_data"]))) as sent:
                self.assertEqual(sent.format, expected)
                self.assertEqual(getattr(sent, "n_frames", 1), frames)

    async def test_foreign_webp_is_downloaded_and_converted(self):
        import base64
        from io import BytesIO
        from PIL import Image

        buffer = BytesIO()
        Image.new("RGB", (8, 8)).save(buffer, format="WEBP")
        fetched = []
        client_module._local_fetch_url = lambda url: None
        client_module._read_local = lambda url: fetched.append(url) or buffer.getvalue()
        client = RecordingClient()

        await client.send_message({"kind": "group", "id": "group-1"}, [("image", "https://example.com/s.webp?x=1")], "m")

        self.assertEqual(fetched, ["https://example.com/s.webp?x=1"])
        with Image.open(BytesIO(base64.b64decode(client.calls[0][2]["file_data"]))) as sent:
            self.assertEqual(sent.format, "PNG")

    async def test_foreign_image_url_is_still_pulled_by_qq(self):
        client_module._local_fetch_url = lambda url: None
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"}, [("image", "https://example.com/a.jpg")], "source-message"
        )

        self.assertEqual(client.calls[0][2], {"file_type": 1, "srv_send_msg": False, "url": "https://example.com/a.jpg"})

    async def test_unreadable_local_image_falls_back_to_url(self):
        def fail(_url):
            raise OSError("down")

        client_module._local_fetch_url = lambda url: "http://127.0.0.1:8000/x"
        client_module._read_local = fail
        client = RecordingClient()

        await client.send_message(
            {"kind": "group", "id": "group-1"}, [("image", "https://api.example.test/x")], "source-message"
        )

        self.assertEqual(client.calls[0][2]["url"], "https://api.example.test/x")
