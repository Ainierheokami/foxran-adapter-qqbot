import asyncio
from types import SimpleNamespace

import pytest

from app.adapters.qqbot import handlers, streaming
from app.adapters.qqbot.client import QQBotAPIError
from app.message import MessageChain
from app.outbound import OutboundMessage


class _Client:
    def __init__(self, *, fail_states=()):
        self.account_id = "default"
        self.frames = []
        self.sent = []
        self.fail_states = set(fail_states)
        self.cfg = {"c2c_stream": True, "markdown_enabled": True, "c2c_stream_interval_ms": 300}

    def config(self):
        return self.cfg

    async def reserve_c2c_sequence(self, openid, msg_id):
        return 7

    async def send_c2c_stream_frame(self, openid, **frame):
        if frame["state"] in self.fail_states:
            raise QQBotAPIError("发送失败 40034: stream refused")
        self.frames.append(frame)
        return {"id": frame.get("stream_msg_id") or "stream-1"}

    async def send_message(self, target, parts, msg_id):
        self.sent.append((target, parts, msg_id))
        return "msg-1"


@pytest.fixture
def client(monkeypatch):
    fake = _Client()
    monkeypatch.setattr(handlers.qqbot_clients, "get", lambda account_id="default": fake)
    monkeypatch.setattr(handlers, "stream_interval", lambda _client: 0.02)
    monkeypatch.setattr(streaming, "_backoff_until", {})
    yield fake
    for key in list(streaming.c2c_streams._drafts):
        streaming.c2c_streams._drafts.pop(key)


def _session(kind="c2c", session_id="qqbot:c2c:u1"):
    return SimpleNamespace(
        session_id=session_id,
        platform_state={"qqbot_target": {"kind": kind, "id": "u1"}, "qqbot_msg_id": "m1"},
    )


CONVERSATION = SimpleNamespace(account_id="default")
PORT = handlers.qqbot_outbound_port


async def _event(session, kind, **data):
    await PORT.on_stream_event(session, CONVERSATION, {"type": kind, "session_id": session.session_id, **data})


def _encoded(text):
    return PORT.encode(None, MessageChain([text]))


def test_stable_prefix_holds_back_unfinished_markup_and_words():
    assert streaming.stable_prefix("你好 [at, id=12") == "你好 "
    assert streaming.stable_prefix("看 ![图](https://exa") == "看 "
    assert streaming.stable_prefix("版本是 Python3.1") == "版本是 "
    assert streaming.stable_prefix("中文句子。") == "中文句子。"
    # 很久没闭合的方括号是正文，不再等待。
    literal = "[" + "字" * 400
    assert streaming.stable_prefix(literal) == literal


async def test_private_reply_streams_then_finishes_with_delivered_text(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="第一段内容，")
    await asyncio.sleep(0.05)
    await _event(session, "content_token", stream_id="s1", token="第二段内容。")
    await asyncio.sleep(0.05)
    await _event(session, "content_end", stream_id="s1")

    platform_id = await PORT.deliver(session, CONVERSATION, _encoded("第一段内容，第二段内容。"), "internal-1", stream_id="s1")

    assert platform_id == "stream-1"
    assert client.sent == []
    states = [frame["state"] for frame in client.frames]
    assert states[0] == streaming.STATE_GENERATING and states[-1] == streaming.STATE_DONE
    assert client.frames[-1]["content"] == "第一段内容，第二段内容。"
    assert [frame["index"] for frame in client.frames] == list(range(len(client.frames)))
    assert {frame["msg_seq"] for frame in client.frames} == {7}
    assert all(frame["stream_msg_id"] == "stream-1" for frame in client.frames[1:])
    assert streaming.c2c_streams.get(session.session_id, "s1") is None


async def test_reply_finished_before_first_frame_is_sent_normally(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="很短")
    await _event(session, "content_end", stream_id="s1")

    platform_id = await PORT.deliver(session, CONVERSATION, _encoded("很短"), "internal-1", stream_id="s1")

    assert platform_id == "msg-1"
    assert client.frames == []
    assert len(client.sent) == 1


async def test_group_and_thought_streams_are_ignored(client):
    group = _session(kind="group", session_id="qqbot:group:g1")
    await _event(group, "content_start", stream_id="s1")
    await _event(_session(), "content_start", stream_id="t1", role="thought")

    assert len(streaming.c2c_streams) == 0


async def test_history_sync_closes_discarded_draft_with_shown_text(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="草稿内容。")
    await asyncio.sleep(0.05)
    await _event(session, "history_sync", reason="retry")
    await asyncio.sleep(0.02)

    assert client.frames[-1]["state"] == streaming.STATE_DONE
    assert client.frames[-1]["content"] == "草稿内容。"
    assert len(streaming.c2c_streams) == 0


async def test_refused_final_frame_falls_back_to_normal_message(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="内容。")
    await asyncio.sleep(0.05)
    client.fail_states = {streaming.STATE_DONE}

    platform_id = await PORT.deliver(session, CONVERSATION, _encoded("内容。完整"), "internal-1", stream_id="s1")

    assert platform_id == "msg-1"
    assert len(client.sent) == 1


async def test_refused_stream_backs_off_to_whole_messages(client):
    client.fail_states = {streaming.STATE_GENERATING}
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="内容。")
    await asyncio.sleep(0.05)

    assert await PORT.deliver(session, CONVERSATION, _encoded("内容。"), "internal-1", stream_id="s1") == "msg-1"
    await _event(session, "content_start", stream_id="s2")
    assert streaming.c2c_streams.get(session.session_id, "s2") is None


async def test_stream_disabled_by_config(client):
    client.cfg["c2c_stream"] = False
    await _event(_session(), "content_start", stream_id="s1")

    assert len(streaming.c2c_streams) == 0


async def test_media_after_streamed_text_is_sent_separately(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="看图：")
    await asyncio.sleep(0.05)
    message = OutboundMessage(text="看图：https://e/a.png", payload=[("text", "看图："), ("image", "https://e/a.png")])

    assert await PORT.deliver(session, CONVERSATION, message, "internal-1", stream_id="s1") == "msg-1"
    assert client.frames[-1]["state"] == streaming.STATE_DONE
    assert client.sent == [({"kind": "c2c", "id": "u1"}, [("image", "https://e/a.png")], "m1")]
