"""一条触发消息最多回 5 条：第 5 个名额起，后面的句子合成一条发。"""

from types import SimpleNamespace

import pytest

from app.adapters.qqbot import handlers
from app.adapters.qqbot.client import QQBotClient
from app.inbound import ConversationRef
from app.outbound import OutboundMessage


CONVERSATION = ConversationRef(platform="qqbot", scope="group", id="G", account_id="default")


def _session(msg_id="M1"):
    return SimpleNamespace(
        session_id="qqbot:group:G",
        platform_state={"qqbot_target": {"kind": "group", "id": "G"}, "qqbot_msg_id": msg_id},
    )


@pytest.mark.asyncio
async def test_sentences_past_the_last_slot_are_merged(monkeypatch):
    client = QQBotClient("default")
    posted = []

    async def request(method, path, payload=None):
        posted.append(payload)
        return {"id": f"P{len(posted)}"}

    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr(client, "config", lambda: {"markdown_enabled": False})
    monkeypatch.setattr(handlers.qqbot_clients, "get", lambda account_id: client)
    monkeypatch.setattr(handlers, "REPLY_TAIL_QUIET_SECONDS", 0.01)
    port = handlers.QQBotOutboundPort()
    session = _session()

    ids = []
    for index in range(1, 9):
        text = f"第{index}句"
        ids.append(await port.deliver(session, CONVERSATION, OutboundMessage(text=text, payload=[("text", text)]), None))
    await handlers.reply_tails.wait_idle()

    assert ids[:4] == ["P1", "P2", "P3", "P4"] and ids[4:] == [None] * 4
    assert [p["content"] for p in posted] == ["第1句", "第2句", "第3句", "第4句", "第5句\n第6句\n第7句\n第8句"]
    assert [p["msg_seq"] for p in posted] == [1, 2, 3, 4, 5]

    # 换了一条触发消息，名额重新算
    session.platform_state["qqbot_msg_id"] = "M2"
    assert await port.deliver(session, CONVERSATION, OutboundMessage(text="新的", payload=[("text", "新的")]), None) == "P6"
