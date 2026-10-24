import asyncio
from types import SimpleNamespace

import pytest

from app.adapters.qqbot import handlers, streaming
from app.adapters.qqbot.client import QQBotAPIError
from app.message import MessageChain
from app.outbound import OutboundMessage, RecallRefused


class _Client:
    def __init__(self, *, fail_states=()):
        self.account_id = "default"
        self.frames = []
        self.sent = []
        self.recalled = []
        self.fail_recall = False
        self.fail_states = set(fail_states)
        self.open_streams = set()
        self.stream_count = 0
        # The client's typing animation is waited for only in the tests about it.
        self.cfg = {"c2c_stream": True, "markdown_enabled": True, "c2c_stream_interval_ms": 300,
                    "c2c_stream_settle_ms_per_char": 0}

    def config(self):
        return self.cfg

    async def reserve_c2c_sequence(self, openid, msg_id):
        return 7

    async def send_c2c_stream_frame(self, openid, **frame):
        if frame["state"] in self.fail_states:
            raise QQBotAPIError("发送失败 40034: stream refused")
        stream_id = frame.get("stream_msg_id")
        if stream_id is None:
            if self.open_streams:
                raise QQBotAPIError("发送失败 40034: another stream is generating")
            self.stream_count += 1
            stream_id = f"stream-{self.stream_count}"
            self.open_streams.add(stream_id)
        if frame["state"] == streaming.STATE_DONE:
            self.open_streams.discard(stream_id)
        self.frames.append({**frame, "stream": stream_id})
        return {"id": stream_id}

    async def recall_message(self, target, message_id):
        if self.fail_recall:
            raise QQBotAPIError("撤回失败 40054005: 超过可撤回时间")
        self.recalled.append((target, message_id))
        self.open_streams.discard(message_id)

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
    streaming.c2c_streams.clear()


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


async def test_short_reply_is_one_finished_stream_frame(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="很短")
    await _event(session, "content_end", stream_id="s1")

    platform_id = await PORT.deliver(session, CONVERSATION, _encoded("很短"), "internal-1", stream_id="s1")

    assert platform_id == "stream-1"
    assert [(f["state"], f["content"]) for f in client.frames] == [(streaming.STATE_DONE, "很短")]
    assert client.sent == []


async def test_two_messages_stream_one_after_another(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="第一句前半，")
    await asyncio.sleep(0.05)
    await _event(session, "content_token", stream_id="s1", token="第一句后半。")
    await _event(session, "content_end", stream_id="s1")
    await _event(session, "content_start", stream_id="s2")
    await _event(session, "content_token", stream_id="s2", token="第二句前半，")
    await asyncio.sleep(0.05)
    await _event(session, "content_token", stream_id="s2", token="第二句后半。")
    await asyncio.sleep(0.05)
    await _event(session, "content_end", stream_id="s2")

    first = await PORT.deliver(session, CONVERSATION, _encoded("第一句前半，第一句后半。"), "i1", stream_id="s1")
    second = await PORT.deliver(session, CONVERSATION, _encoded("第二句前半，第二句后半。"), "i2", stream_id="s2")

    assert (first, second) == ("stream-1", "stream-2")
    assert client.sent == []
    # 第一句收尾后第二句才开流，且第二句确实分多帧流式显示。
    first_done = next(i for i, f in enumerate(client.frames) if f["stream"] == "stream-1" and f["state"] == streaming.STATE_DONE)
    second_frames = [f for f in client.frames if f["stream"] == "stream-2"]
    assert client.frames.index(second_frames[0]) > first_done
    assert [f["state"] for f in second_frames][0] == streaming.STATE_GENERATING
    assert second_frames[-1]["content"] == "第二句前半，第二句后半。"


async def test_next_message_waits_for_the_client_to_type_the_previous_one_out(client):
    client.cfg["c2c_stream_settle_ms_per_char"] = 10
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="字" * 20)
    await _event(session, "content_end", stream_id="s1")
    await _event(session, "content_start", stream_id="s2")
    await _event(session, "content_token", stream_id="s2", token="下一条。")
    await _event(session, "content_end", stream_id="s2")
    loop = asyncio.get_running_loop()
    stamps = []
    original = client.send_c2c_stream_frame

    async def stamped(openid, **frame):
        stamps.append(loop.time())
        return await original(openid, **frame)

    client.send_c2c_stream_frame = stamped

    await PORT.deliver(session, CONVERSATION, _encoded("字" * 20), "i1", stream_id="s1")
    await PORT.deliver(session, CONVERSATION, _encoded("下一条。"), "i2", stream_id="s2")

    second_start = next(i for i, f in enumerate(client.frames) if f["stream"] == "stream-2")
    # 20 字 × 10ms：第二条至少晚 0.2 秒开始。
    assert stamps[second_start] - stamps[second_start - 1] >= 0.18


async def test_settle_wait_is_capped(client):
    draft = streaming.C2CStreamDraft(client, "u1", "m1", 0.02)
    client.cfg["c2c_stream_settle_ms_per_char"] = 200
    await draft._frame("字" * 1000, streaming.STATE_DONE)

    assert draft.settled_at - streaming.time.monotonic() <= streaming.SETTLE_MAX_SECONDS


async def test_delivered_text_different_from_stream_is_recalled_then_sent_again(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="草稿。")
    await _event(session, "content_end", stream_id="s1")

    assert await PORT.deliver(session, CONVERSATION, _encoded("完全不同的正式回复。"), "i1", stream_id="s1") == "msg-1"
    assert len(client.sent) == 1
    assert client.recalled == [({"kind": "c2c", "id": "u1"}, "stream-1")]


async def test_group_and_thought_streams_are_ignored(client):
    group = _session(kind="group", session_id="qqbot:group:g1")
    await _event(group, "content_start", stream_id="s1")
    await _event(_session(), "content_start", stream_id="t1", role="thought")

    assert len(streaming.c2c_streams) == 0


async def test_history_sync_recalls_discarded_draft(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="草稿内容。")
    await asyncio.sleep(0.05)
    await _event(session, "history_sync", reason="retry")
    await asyncio.sleep(0.02)

    assert client.frames[-1]["state"] == streaming.STATE_DONE
    assert client.frames[-1]["content"] == "草稿内容。"
    assert client.recalled == [({"kind": "c2c", "id": "u1"}, "stream-1")]
    assert len(streaming.c2c_streams) == 0


async def test_refused_final_frame_falls_back_to_normal_message(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="内容。")
    await asyncio.sleep(0.05)
    client.fail_states = {streaming.STATE_DONE}
    await _event(session, "content_end", stream_id="s1")

    platform_id = await PORT.deliver(session, CONVERSATION, _encoded("内容。"), "internal-1", stream_id="s1")

    assert platform_id == "msg-1"
    assert len(client.sent) == 1


async def test_refused_stream_backs_off_to_whole_messages(client):
    client.fail_states = {streaming.STATE_GENERATING}
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="内容。")
    await asyncio.sleep(0.05)
    await _event(session, "content_end", stream_id="s1")

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
    await _event(session, "content_end", stream_id="s1")
    message = OutboundMessage(text="看图：https://e/a.png", payload=[("text", "看图："), ("image", "https://e/a.png")])

    assert await PORT.deliver(session, CONVERSATION, message, "internal-1", stream_id="s1") == "msg-1"
    assert client.frames[-1]["state"] == streaming.STATE_DONE
    assert client.sent == [({"kind": "c2c", "id": "u1"}, [("image", "https://e/a.png")], "m1")]


LONG_REPLY = (
    "不完全是这么简单，可以这样记：\n\n"
    "- **效应**：更接近属性异常机制，由对应属性攻击积累或触发。\n"
    "- **偏谐**：一套独立的战斗机制，核心是偏谐值、失谐和谐度破坏。\n"
    "- **震谐**：偏谐体系里的进一步状态，通常在谐度破坏后触发追加攻击。\n\n"
    "所以导电等是属性伤害，效应依附于属性攻击，偏谐和震谐是另一套机制。"
)


async def _send_resolved(session, stream_id, text):
    """A send gated on a step: the executor streams its known text in one burst."""
    await _event(session, "content_start", stream_id=stream_id, role="assistant")
    for offset in range(0, len(text), 256):
        await _event(session, "content_token", stream_id=stream_id, token=text[offset:offset + 256])
    await _event(session, "content_end", stream_id=stream_id)
    return await PORT.deliver(session, CONVERSATION, _encoded(text), "i1", stream_id=stream_id)


def test_reveal_cuts_split_long_text_at_boundaries():
    cuts = streaming.reveal_cuts(LONG_REPLY)

    assert 1 <= len(cuts) < streaming.REVEAL_MAX_FRAMES
    assert cuts == sorted(set(cuts)) and cuts[-1] < len(LONG_REPLY)
    assert all(LONG_REPLY[cut - 1] in "\n。！？；，、.!?;, " for cut in cuts)
    assert streaming.reveal_cuts("很短的回复") == []


async def test_reply_resolved_before_sending_is_revealed_gradually(client):
    platform_id = await _send_resolved(_session(), "send_1", LONG_REPLY)

    assert platform_id == "stream-1"
    assert client.sent == []
    states = [frame["state"] for frame in client.frames]
    assert states.count(streaming.STATE_GENERATING) >= 1 and states[-1] == streaming.STATE_DONE
    contents = [frame["content"] for frame in client.frames]
    assert all(LONG_REPLY.startswith(content) for content in contents)
    assert contents[-1] == LONG_REPLY


async def test_reveal_can_be_turned_off(client):
    client.cfg["c2c_stream_reveal"] = False

    assert await _send_resolved(_session(), "send_1", LONG_REPLY) == "stream-1"
    assert [frame["state"] for frame in client.frames] == [streaming.STATE_DONE]


async def test_shown_counts_live_drafts_only(client):
    session = _session()
    assert not PORT.stream_drafts_shown(session)
    await _event(session, "content_start", stream_id="s1")
    assert PORT.stream_drafts_shown(session)
    assert PORT.stream_drafts_shown(session, ["s1"])
    assert not PORT.stream_drafts_shown(session, ["other"])
    await _event(session, "history_sync")
    assert not PORT.stream_drafts_shown(session)


async def test_failed_recall_blocks_replacement_and_retains_id(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="原来的回复。")
    await _event(session, "content_end", stream_id="s1")
    draft = streaming.c2c_streams.get(session.session_id, "s1")
    await draft.finished.wait()
    client.fail_recall = True
    with pytest.raises(RecallRefused):
        await _event(session, "history_sync")
    assert streaming.c2c_streams.get(session.session_id, "s1") is draft
    assert PORT.stream_drafts_shown(session)
    streaming.c2c_streams._forget((session.session_id, "s1"), (client.account_id, "u1"), draft)
    assert streaming.c2c_streams.get(session.session_id, "s1") is draft
    with pytest.raises(RecallRefused):
        await _event(session, "content_start", stream_id="s2")
    with pytest.raises(RecallRefused):
        await PORT.deliver(session, CONVERSATION, _encoded("新回复"), "i2")
    assert client.sent == [] and client.stream_count == 1
    client.fail_recall = False
    await _event(session, "history_sync")
    assert len(streaming.c2c_streams) == 0


async def test_changed_final_text_is_not_resent_when_recall_fails(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="原来的回复。")
    await _event(session, "content_end", stream_id="s1")
    client.fail_recall = True
    with pytest.raises(RecallRefused):
        await PORT.deliver(session, CONVERSATION, _encoded("改过的回复"), "i1", stream_id="s1")
    assert client.sent == []
    assert streaming.c2c_streams.get(session.session_id, "s1") is not None


async def test_recall_waits_for_inflight_first_frame_and_stops_queued_stream(client):
    session = _session()
    started, release = asyncio.Event(), asyncio.Event()
    original = client.send_c2c_stream_frame
    async def delayed(openid, **frame):
        started.set()
        await release.wait()
        return await original(openid, **frame)
    client.send_c2c_stream_frame = delayed
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="第一条。")
    await _event(session, "content_end", stream_id="s1")
    await started.wait()
    await _event(session, "content_start", stream_id="s2")
    await _event(session, "content_token", stream_id="s2", token="第二条。")
    await _event(session, "content_end", stream_id="s2")
    resetting = asyncio.create_task(_event(session, "history_sync"))
    await asyncio.sleep(0)
    assert not resetting.done() and client.recalled == []
    release.set()
    await resetting
    assert client.stream_count == 1
    assert client.recalled == [({"kind": "c2c", "id": "u1"}, "stream-1")]
    assert len(streaming.c2c_streams) == 0


async def test_concurrent_resets_recall_once_and_block_new_streams(client):
    session = _session()
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="原来的回复。")
    await _event(session, "content_end", stream_id="s1")
    draft = streaming.c2c_streams.get(session.session_id, "s1")
    await draft.finished.wait()
    started, release = asyncio.Event(), asyncio.Event()
    original = client.recall_message
    async def delayed(target, message_id):
        started.set()
        await release.wait()
        return await original(target, message_id)
    client.recall_message = delayed
    first = asyncio.create_task(_event(session, "history_sync"))
    second = asyncio.create_task(_event(session, "history_sync"))
    await started.wait()
    with pytest.raises(RecallRefused):
        await _event(session, "content_start", stream_id="s2")
    release.set()
    await asyncio.gather(first, second)
    assert len(client.recalled) == 1
    assert len(streaming.c2c_streams) == 0


async def test_inflight_timeout_blocks_resend_until_recall_can_finish(client, monkeypatch):
    session = _session()
    started, release = asyncio.Event(), asyncio.Event()
    original = client.send_c2c_stream_frame
    async def delayed(openid, **frame):
        started.set()
        await release.wait()
        return await original(openid, **frame)
    client.send_c2c_stream_frame = delayed
    monkeypatch.setattr(streaming, "FINALIZE_WAIT_SECONDS", 0.01)
    await _event(session, "content_start", stream_id="s1")
    await _event(session, "content_token", stream_id="s1", token="原来的回复。")
    await _event(session, "content_end", stream_id="s1")
    await started.wait()
    draft = streaming.c2c_streams.get(session.session_id, "s1")
    try:
        with pytest.raises(RecallRefused):
            await _event(session, "history_sync")
        assert not draft._task.cancelled()
        with pytest.raises(RecallRefused):
            await PORT.deliver(session, CONVERSATION, _encoded("新回复"), "i2")
        assert client.sent == []
    finally:
        release.set()
        await draft.finished.wait()
        await _event(session, "history_sync")
    assert len(client.recalled) == 1
