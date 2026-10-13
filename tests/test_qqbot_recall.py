"""QQ Bot recall capability used by the framework's ``delete_msg`` tool."""

from types import SimpleNamespace

import pytest

from app.adapters.qqbot import handlers
from app.adapters.qqbot.client import QQBotAPIError, QQBotClient
from app.inbound import ConversationRef
from app.outbound import RecallRefused


CONVERSATION = ConversationRef(platform="qqbot", scope="group", id="group-openid", account_id="default")


def _session(kind="group", target_id="group-openid"):
    return SimpleNamespace(platform_state={"qqbot_target": {"kind": kind, "id": target_id}})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "path"),
    [
        ("group", "/v2/groups/T/messages/M"),
        ("c2c", "/v2/users/T/messages/M"),
        ("channel", "/channels/T/messages/M"),
    ],
)
async def test_client_recalls_by_target_kind(monkeypatch, kind, path):
    client = QQBotClient("default")
    calls = []

    async def request(method, request_path, payload=None):
        calls.append((method, request_path))
        return {}

    monkeypatch.setattr(client, "request", request)
    await client.recall_message({"kind": kind, "id": "T"}, "M")

    assert calls == [("DELETE", path)]


@pytest.mark.asyncio
async def test_port_recalls_own_messages_only(monkeypatch):
    recalled = []

    class _Client:
        async def recall_message(self, target, message_id):
            recalled.append((target["kind"], target["id"], message_id))

    monkeypatch.setattr(handlers.qqbot_clients, "get", lambda account_id: _Client())
    port = handlers.QQBotOutboundPort()

    await port.recall(_session(), CONVERSATION, "M1", own=True)
    with pytest.raises(RecallRefused, match="只能撤回自己"):
        await port.recall(_session(), CONVERSATION, "M2", own=False)

    assert recalled == [("group", "group-openid", "M1")]


@pytest.mark.asyncio
async def test_platform_errors_become_refusals(monkeypatch):
    class _Client:
        async def recall_message(self, target, message_id):
            raise QQBotAPIError("撤回失败 40054005: 超过可撤回时间")

    monkeypatch.setattr(handlers.qqbot_clients, "get", lambda account_id: _Client())

    with pytest.raises(RecallRefused, match="超过可撤回时间"):
        await handlers.QQBotOutboundPort().recall(_session(), CONVERSATION, "M", own=True)
    with pytest.raises(RecallRefused, match="找不到"):
        await handlers.QQBotOutboundPort().recall(SimpleNamespace(platform_state={}), CONVERSATION, "M", own=True)
