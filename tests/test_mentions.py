"""QQ 的 @：入站转成 Mention，出站按群 / 频道 / 私聊写成 QQ 认识的格式。"""

import pytest

from app.adapters.qqbot.adapter import QQBotAdapter, outgoing_parts, parts_as_text
from app.adapters.qqbot.client import QQBotClient, _v2_parts
from app.adapters.qqbot.handlers import display_name, mention_openids
from app.agent.codec import tag_codec
from app.message import Mention, MessageChain

BOT = "BOTOPENID0000000000000000000000A"
USER = "E1A2B3C4D5E6F708192A3B4C5D6E7F80"


@pytest.fixture(autouse=True)
def _forget_names():
    from app.adapters.qqbot import adapter

    adapter._KNOWN_NAMES.clear()
    yield
    adapter._KNOWN_NAMES.clear()


def test_inbound_mentions_in_both_syntaxes_become_segments():
    chain = QQBotAdapter().from_platform_format({
        "content": f'<@{BOT}> 你看 <qqbot-at-user id="{USER}" /> 说的<qqbot-at-everyone />',
        "mentions": [{"member_openid": USER, "nickname": "阿霜"}],
    })

    mentions = [seg for seg in chain if isinstance(seg, Mention)]
    assert [(m.target, m.name) for m in mentions] == [(BOT, None), (USER, "阿霜"), (Mention.ALL, None)]
    # 模型读到的是统一的 [at,...] 标签，不再是一串尖括号乱码
    rendered = tag_codec.render(chain)
    assert "<@" not in rendered and "qqbot-at-user" not in rendered
    assert f"id={USER}" in rendered


def test_mention_detection_understands_the_new_syntax_and_ignores_everyone():
    data = {"content": f'<qqbot-at-user id="{BOT}" /> 在吗 <qqbot-at-everyone />'}
    assert mention_openids(data) == {BOT}


def _recording_client(sent, **config):
    class Recording(QQBotClient):
        async def request(self, method, path, payload=None):
            sent.append(payload)
            return {"id": "m1"}

        def config(self, force_reload=False):
            return {"markdown_enabled": False, **config}

    return Recording()


@pytest.mark.parametrize("kind, expected", [
    # 群里的 <qqbot-at-user /> 会被 QQ 原样显示，默认写成「@昵称」文字
    ("group", "你好@QQ用户7F80"),
    ("channel", f"你好<@{USER}>"),
    ("c2c", "你好"),
])
async def test_outbound_mention_is_written_for_the_target(kind, expected):
    sent = []
    chain = MessageChain(["你好", Mention(target=USER)])
    await _recording_client(sent).send_message({"kind": kind, "id": "t"}, outgoing_parts(chain), "src")

    assert sent[0]["content"] == expected


async def test_group_mention_uses_the_nickname_seen_inbound_and_keeps_a_space():
    QQBotAdapter().from_platform_format({
        "content": f'<qqbot-at-user id="{USER}" /> 早',
        "mentions": [{"member_openid": USER, "nickname": "阿霜"}],
    })
    sent = []
    chain = MessageChain([Mention(target=USER), "你好"])
    await _recording_client(sent).send_message({"kind": "group", "id": "t"}, outgoing_parts(chain), "src")

    assert sent[0]["content"] == "@阿霜 你好"


async def test_group_mention_tag_is_sent_as_markdown_so_qq_renders_it():
    # 标记在纯文本（msg_type 0）里会原样显示，只有 Markdown 消息才渲染成 @
    sent = []
    chain = MessageChain(["你好", Mention(target=USER)])
    await _recording_client(sent, markdown_enabled=True).send_message({"kind": "group", "id": "t"}, outgoing_parts(chain), "src")

    assert sent[0]["msg_type"] == 2
    assert sent[0]["markdown"]["content"] == f'你好<qqbot-at-user id="{USER}" />'


async def test_group_mention_falls_back_to_nickname_when_markdown_is_refused():
    from app.adapters.qqbot.client import QQBotAPIError

    sent = []

    class Refusing(QQBotClient):
        async def request(self, method, path, payload=None):
            if payload.get("msg_type") == 2:
                raise QQBotAPIError("QQ Bot HTTP POST 失败: HTTP 400")
            sent.append(payload)
            return {"id": "m1"}

        def config(self, force_reload=False):
            return {"markdown_enabled": True}

    chain = MessageChain(["你好", Mention(target=USER)])
    await Refusing().send_message({"kind": "group", "id": "t"}, outgoing_parts(chain), "src")

    assert sent == [{"content": "你好@QQ用户7F80", "msg_type": 0, "msg_id": "src", "msg_seq": 1}]


async def test_group_mention_text_style_never_sends_the_tag():
    sent = []
    chain = MessageChain(["你好", Mention(target=USER)])
    await _recording_client(sent, markdown_enabled=True, group_mention="text").send_message(
        {"kind": "group", "id": "t"}, outgoing_parts(chain), "src",
    )

    assert sent[0]["msg_type"] == 0 and sent[0]["content"] == "你好@QQ用户7F80"


def test_everyone_is_never_sent_as_a_real_mention():
    from app.adapters.qqbot.adapter import render_mention

    assert render_mention(Mention.ALL, "group") == "@全体成员"


def test_leftover_mentions_are_dropped_from_private_stream_text_and_shown_in_logs():
    parts = outgoing_parts(MessageChain(["嗨", Mention(target=USER)]))
    assert _v2_parts(parts) == ("嗨", [])
    assert parts_as_text(parts) == f"嗨@{USER}"


def test_sender_without_nickname_gets_a_short_name():
    assert display_name({"member_openid": USER}, USER) == "QQ用户7F80"
    assert display_name({"username": "阿霜"}, USER) == "阿霜"
    assert display_name({}, "12345") == "12345"


def test_qq_faces_become_readable_face_segments():
    import base64
    import json

    from app.message import Face

    ext = base64.b64encode(json.dumps({"text": "[呲牙]"}, ensure_ascii=False).encode()).decode()
    chain = QQBotAdapter().from_platform_format({"content": f'哈哈<faceType=1,faceId="13",ext="{ext}">好<faceType=1,faceId="9",ext="坏的">'})

    faces = [seg for seg in chain if isinstance(seg, Face)]
    assert [(f.id, f.text) for f in faces] == [("13", "呲牙"), ("9", None)]
    assert "faceType" not in tag_codec.render(chain)
