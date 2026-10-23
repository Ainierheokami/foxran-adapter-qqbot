"""QQ C2C (private chat) stream messages for Foxran's streamed reply drafts.

Foxran streams a reply draft as ``content_start`` / ``content_token`` /
``content_end`` events and later delivers the validated reply with the same
stream id. QQ only streams in C2C, through ``/v2/users/{openid}/stream_messages``
in replace mode, where each frame carries the full text so far and text QQ has
already shown cannot be rewritten. So a draft:

* streams one at a time per user: a reply of several messages shows them in
  order, each one starting once the previous one is finished;
* only sends text that extends what QQ already shows, holding back a trailing
  tag, link or word that may still change once complete;
* finishes as soon as the model closes the message (``content_end``), so the
  next message can stream. The later delivery only sends the reply's media; if
  its text differs from the shown draft, or the stream failed, the reply is sent
  as an ordinary message.

The QQ client types each frame's new text out locally, and stops that animation
when another message arrives, leaving the earlier one looking cut off until the
chat is reopened. So what follows a stream (the next stream, or the reply's
media) waits until the client has had time to type out what was last sent.

A draft Foxran discards (``history_sync`` after a failed validation) cannot be
recalled from QQ; it is closed with what it already shows.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

from app.adapters.qqbot.adapter import outgoing_parts
from app.adapters.qqbot.client import QQBotAPIError, QQBotClient, _v2_parts
from app.logger import setup_logger

logger = setup_logger(__name__)

STATE_GENERATING = 1
STATE_DONE = 10
MIN_INTERVAL_MS = 300
# A draft the model never closes is finished with what it shows after this long.
DRAFT_TIMEOUT_SECONDS = 180.0
# How long a delivery waits for its draft (and any draft queued before it) to finish.
FINALIZE_WAIT_SECONDS = 20.0
# After QQ refuses to open a stream (no permission, unsupported bot...) stop
# trying for a while instead of failing every reply.
STREAM_BACKOFF_SECONDS = 600.0
# An unclosed ``[`` or ``](`` longer than this is literal text, not a tag in progress.
_MAX_PENDING_MARKUP = 300
_PENDING_LINK = re.compile(r"!?\[[^\]\n]*\]\([^)\s]*$")
_PENDING_TAG = re.compile(r"\[[^\]]*$")
_PENDING_WORD = re.compile(r"[A-Za-z0-9_\-:/.?=&%#~+@]+$")
_WHITESPACE = re.compile(r"\s+")
# Revealing a reply that arrived complete: at most this many frames
# (≈ (frames - 1) × interval of delay), one per this many characters.
REVEAL_MIN_CHARS = 40
REVEAL_MAX_FRAMES = 5
REVEAL_CHARS_PER_FRAME = 120
_REVEAL_BOUNDARY = re.compile(r"[\n。！？；，、.!?;, ]")
# The client's typing animation is waited for at most this long.
SETTLE_MAX_SECONDS = 3.0

_backoff_until: dict[str, float] = {}


def stable_prefix(raw: str) -> str:
    """``raw`` without a trailing tag, link or word that may still change."""
    end = len(raw)
    for pattern in (_PENDING_LINK, _PENDING_TAG):
        match = pattern.search(raw, 0, end)
        if match and end - match.start() <= _MAX_PENDING_MARKUP:
            end = match.start()
    match = _PENDING_WORD.search(raw, 0, end)
    if match:
        end = match.start()
    return raw[:end]


def render_draft(raw: str, *, complete: bool = False) -> str:
    """The text QQ would show for ``raw`` model output, as the final delivery encodes it.

    Extension segments (stickers...) are left out: delivery turns them into
    media, which follows the stream as separate messages.
    """
    from app.agent.codec import tag_codec
    from app.agent.compression.url_reference import canonicalize_segments_for_history
    from app.agent.compression.url_reference import canonicalize_text_for_history
    from app.message import ExtensionSegment, MessageChain

    source = raw if complete else stable_prefix(raw)
    text = canonicalize_text_for_history(source)
    chain = canonicalize_segments_for_history(tag_codec.parse(str(text or "")))
    chain = MessageChain([seg for seg in chain if not isinstance(seg, ExtensionSegment)])
    rendered, _media = _v2_parts(outgoing_parts(chain))
    return rendered


def same_text(left: str, right: str) -> bool:
    return _WHITESPACE.sub("", left or "") == _WHITESPACE.sub("", right or "")


def reveal_cuts(text: str) -> list[int]:
    """Prefix lengths for revealing complete ``text`` gradually (empty when too short).

    Cuts land just after a line break, punctuation or space, so a frame never
    ends inside a word or link.
    """
    if len(text) < REVEAL_MIN_CHARS:
        return []
    frames = min(REVEAL_MAX_FRAMES, max(2, -(-len(text) // REVEAL_CHARS_PER_FRAME)))
    cuts: list[int] = []
    for i in range(1, frames):
        target = len(text) * i // frames
        match = _REVEAL_BOUNDARY.search(text, target, min(len(text), target + 40))
        cut = match.end() if match else target
        if (not cuts or cut > cuts[-1]) and cut < len(text):
            cuts.append(cut)
    return cuts


def reveal_enabled(client: QQBotClient) -> bool:
    return bool(client.config().get("c2c_stream_reveal", True))


def stream_enabled(client: QQBotClient) -> bool:
    cfg = client.config()
    if not cfg.get("c2c_stream", True) or not cfg.get("markdown_enabled", True):
        return False
    return time.monotonic() >= _backoff_until.get(client.account_id, 0.0)


def settle_per_char(client: QQBotClient) -> float:
    """Seconds the client takes to type out one character (0 = don't wait)."""
    try:
        value = float(client.config().get("c2c_stream_settle_ms_per_char", 25))
    except (TypeError, ValueError):
        value = 25.0
    return max(0.0, min(value, 200.0)) / 1000


def stream_interval(client: QQBotClient) -> float:
    try:
        value = int(client.config().get("c2c_stream_interval_ms") or 600)
    except (TypeError, ValueError):
        value = 600
    return max(MIN_INTERVAL_MS, value) / 1000


class C2CStreamDraft:
    """One streamed reply message rendered as a QQ C2C stream message."""

    def __init__(
        self,
        client: QQBotClient,
        openid: str,
        msg_id: str,
        interval: float,
        previous: "C2CStreamDraft | None" = None,
    ) -> None:
        self.client = client
        self.openid = openid
        self.msg_id = msg_id
        self.interval = interval
        self.previous = previous
        self.raw = ""
        self.sent = ""
        # Text the final (DONE) frame showed; None until the stream finished cleanly.
        self.done_text: str | None = None
        self.stream_msg_id: str | None = None
        self.msg_seq: int | None = None
        self.index = 0
        self.ended = False
        self.abandoned = False
        self.failed = False
        self.finished = asyncio.Event()
        # When the client will have typed out everything sent so far.
        self.settled_at = 0.0
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None

    @property
    def opened(self) -> bool:
        return self.stream_msg_id is not None

    async def wait_settled(self) -> None:
        """Wait until the client has typed out this stream's text."""
        delay = self.settled_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

    def start(self, on_exit: Any) -> None:
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(lambda _task: on_exit(self))

    def feed(self, token: str) -> None:
        if not self.ended:
            self.raw += token

    def end(self) -> None:
        self.ended = True
        self._wake.set()

    def abandon(self) -> None:
        """Foxran discarded this draft: QQ cannot recall it, so it closes as shown."""
        self.abandoned = True
        self._wake.set()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        self._wake.clear()

    async def _run(self) -> None:
        try:
            if self.previous is not None:
                # QQ shows one stream at a time: wait for the previous message.
                await self.previous.finished.wait()
                await self.previous.wait_settled()
                self.previous = None
            deadline = time.monotonic() + DRAFT_TIMEOUT_SECONDS
            if not self.ended:
                await self._sleep(self.interval)
            while True:
                if self.abandoned:
                    if self.opened:
                        logger.info("QQ Bot 流式草稿被作废（模型重试），已显示的 %d 字保留", len(self.sent))
                    await self._finish(self.sent)
                    return
                if self.ended:
                    text = self._render(complete=True)
                    if not self.opened and reveal_enabled(self.client):
                        # Text that arrived all at once (a send gated on a step,
                        # or a reply faster than one interval) is revealed over
                        # a few frames so every message streams alike.
                        await self._reveal(text)
                    await self._finish(self.sent if self.abandoned else text)
                    return
                await self._push()
                if time.monotonic() > deadline:
                    logger.warning("QQ Bot 流式草稿 %ss 未结束，按已显示内容收尾", int(DRAFT_TIMEOUT_SECONDS))
                    await self._finish(self.sent)
                    return
                await self._sleep(self.interval)
        except Exception:
            logger.warning("QQ Bot 流式草稿处理异常", exc_info=True)
        finally:
            self.finished.set()

    async def _reveal(self, text: str) -> None:
        for cut in reveal_cuts(text):
            if self.abandoned or self.failed:
                return
            try:
                await self._frame(text[:cut], STATE_GENERATING)
            except QQBotAPIError as exc:
                self._on_failure(exc)
                return
            await asyncio.sleep(self.interval)

    def _render(self, *, complete: bool = False) -> str:
        try:
            return render_draft(self.raw, complete=complete)
        except Exception:
            logger.debug("QQ Bot 流式草稿渲染失败", exc_info=True)
            return ""

    async def _push(self) -> None:
        if self.failed:
            return
        text = self._render()
        if not text or text == self.sent or not text.startswith(self.sent):
            return
        try:
            await self._frame(text, STATE_GENERATING)
        except QQBotAPIError as exc:
            self._on_failure(exc)

    async def _finish(self, text: str) -> None:
        """Send the DONE frame. Nothing is sent for a draft QQ never showed."""
        if self.failed and not self.opened:
            return
        if not self.opened and (self.abandoned or not text):
            return
        try:
            await self._frame(text or self.sent, STATE_DONE)
            self.done_text = text or self.sent
        except QQBotAPIError as exc:
            self._on_failure(exc)
            if self.opened and text != self.sent:
                try:
                    await self._frame(self.sent, STATE_DONE)
                except QQBotAPIError:
                    pass

    def _on_failure(self, exc: QQBotAPIError) -> None:
        self.failed = True
        if not self.opened:
            _backoff_until[self.client.account_id] = time.monotonic() + STREAM_BACKOFF_SECONDS
            logger.warning(
                "QQ Bot 私聊流式消息不可用，%d 分钟内改为整条发送：%s",
                int(STREAM_BACKOFF_SECONDS // 60), exc,
            )
        else:
            logger.warning("QQ Bot 流式分片发送失败：%s", exc)

    async def _frame(self, text: str, state: int) -> None:
        if self.msg_seq is None:
            self.msg_seq = await self.client.reserve_c2c_sequence(self.openid, self.msg_id)
        index = self.index
        self.index += 1
        result = await self.client.send_c2c_stream_frame(
            self.openid,
            content=text,
            state=state,
            msg_id=self.msg_id,
            msg_seq=self.msg_seq,
            index=index,
            stream_msg_id=self.stream_msg_id,
        )
        if not self.stream_msg_id:
            self.stream_msg_id = str(result.get("id") or "") or None
            if self.stream_msg_id is None:
                raise QQBotAPIError(f"流式消息响应缺少 id: {result}")
        added = len(text) - len(self.sent) if text.startswith(self.sent) else len(text)
        now = time.monotonic()
        self.settled_at = min(
            max(self.settled_at, now) + max(0, added) * settle_per_char(self.client),
            now + SETTLE_MAX_SECONDS,
        )
        self.sent = text

    async def finalize(self, text: str) -> str | None:
        """Match the delivered reply text against the finished stream.

        Returns the stream's message id when QQ already shows ``text``, or None
        when the reply must be sent as an ordinary message.
        """
        self.end()
        try:
            await asyncio.wait_for(self.finished.wait(), timeout=FINALIZE_WAIT_SECONDS)
        except asyncio.TimeoutError:
            # Still queued behind a message that never finished: send this one
            # normally and make sure the draft never shows up later.
            logger.warning("QQ Bot 流式草稿 %ss 内未完成，改为整条发送", int(FINALIZE_WAIT_SECONDS))
            self.abandon()
            return None
        if self.done_text is None:
            return None
        if not same_text(self.done_text, text):
            logger.warning(
                "QQ Bot 流式内容与正式回复不一致，整条补发：shown=%r final=%r",
                self.done_text[:80], text[:80],
            )
            return None
        return self.stream_msg_id


class C2CStreamRegistry:
    """Open drafts keyed by (session id, Foxran stream id)."""

    def __init__(self) -> None:
        self._drafts: dict[tuple[str, str], C2CStreamDraft] = {}
        # Latest draft per (account, user): the next one waits for it.
        self._latest: dict[tuple[str, str], C2CStreamDraft] = {}

    def __len__(self) -> int:
        return len(self._drafts)

    def create(self, session_id: str, stream_id: str, client: QQBotClient, openid: str, msg_id: str, interval: float) -> C2CStreamDraft:
        key = (session_id, stream_id)
        previous_same_id = self._drafts.get(key)
        if previous_same_id is not None:
            previous_same_id.abandon()
        channel = (client.account_id, openid)
        latest = self._latest.get(channel)
        draft = C2CStreamDraft(
            client, openid, msg_id, interval,
            previous=latest if latest is not None and not latest.finished.is_set() else None,
        )
        self._drafts[key] = draft
        self._latest[channel] = draft

        def forget(finished: C2CStreamDraft) -> None:
            # Delivery pops the draft it finalizes; a draft never delivered is
            # dropped a while after it finished.
            asyncio.get_running_loop().call_later(DRAFT_TIMEOUT_SECONDS, self._forget, key, channel, finished)

        draft.start(forget)
        return draft

    def _forget(self, key: tuple[str, str], channel: tuple[str, str], draft: C2CStreamDraft) -> None:
        if self._drafts.get(key) is draft:
            self._drafts.pop(key, None)
        if self._latest.get(channel) is draft:
            self._latest.pop(channel, None)

    def get(self, session_id: str, stream_id: str) -> C2CStreamDraft | None:
        return self._drafts.get((session_id, stream_id))

    def pop(self, session_id: str, stream_id: str) -> C2CStreamDraft | None:
        return self._drafts.pop((session_id, stream_id), None)

    def shown(self, session_id: str, stream_ids: Any = None) -> bool:
        """Whether one of the session's drafts is (or is about to be) on the user's screen.

        A draft QQ refused before it opened shows nothing; any other draft that
        was not abandoned is either shown or will be once its turn comes.
        """
        wanted = None if stream_ids is None else {str(value) for value in stream_ids}
        return any(
            not draft.abandoned and not (draft.failed and not draft.opened)
            for (sid, stream_id), draft in self._drafts.items()
            if sid == session_id and (wanted is None or stream_id in wanted)
        )

    def pop_session(self, session_id: str) -> list[C2CStreamDraft]:
        keys = [key for key in self._drafts if key[0] == session_id]
        return [self._drafts.pop(key) for key in keys]

    def clear(self) -> None:
        self._drafts.clear()
        self._latest.clear()


c2c_streams = C2CStreamRegistry()
