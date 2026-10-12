"""QQ C2C (private chat) stream messages for Foxran's streamed reply drafts.

Foxran streams a reply draft as ``content_start`` / ``content_token`` /
``content_end`` events and later delivers the validated reply with the same
stream id. QQ only streams in C2C, through ``/v2/users/{openid}/stream_messages``
in replace mode, where each frame carries the full text so far and text QQ has
already shown cannot be rewritten. So a draft:

* waits one interval before its first frame — replies that finish sooner (and
  sends whose text is already known) go out as ordinary messages;
* only sends text that extends what QQ already shows, holding back a trailing
  tag, link or word that may still change once complete;
* ends with the delivered reply as the final frame. Media in the reply follows
  as separate messages; if QQ refuses the final frame the reply is sent normally.

A draft Foxran discards (``history_sync`` after a failed validation, or no
delivery within ``FINALIZE_GRACE_SECONDS``) cannot be recalled from QQ; it is
closed with what it already shows.
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
FINALIZE_GRACE_SECONDS = 120.0
# After QQ refuses to open a stream (no permission, unsupported bot...) stop
# trying for a while instead of failing every reply.
STREAM_BACKOFF_SECONDS = 600.0
# An unclosed ``[`` or ``](`` longer than this is literal text, not a tag in progress.
_MAX_PENDING_MARKUP = 300
_PENDING_LINK = re.compile(r"!?\[[^\]\n]*\]\([^)\s]*$")
_PENDING_TAG = re.compile(r"\[[^\]]*$")
_PENDING_WORD = re.compile(r"[A-Za-z0-9_\-:/.?=&%#~+@]+$")

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


def render_draft(raw: str) -> str:
    """The text QQ would show for ``raw`` model output, as the final delivery encodes it."""
    from app.agent.codec import tag_codec
    from app.data_mappers import get_message_processor
    from app.data_mappers.url_reference import canonicalize_segments_for_history

    text = get_message_processor().content_compressor.decompress_sync(stable_prefix(raw))
    chain = canonicalize_segments_for_history(tag_codec.parse(str(text or "")))
    rendered, _media = _v2_parts(outgoing_parts(chain))
    return rendered


def stream_enabled(client: QQBotClient) -> bool:
    cfg = client.config()
    if not cfg.get("c2c_stream", True) or not cfg.get("markdown_enabled", True):
        return False
    return time.monotonic() >= _backoff_until.get(client.account_id, 0.0)


def stream_interval(client: QQBotClient) -> float:
    try:
        value = int(client.config().get("c2c_stream_interval_ms") or 600)
    except (TypeError, ValueError):
        value = 600
    return max(MIN_INTERVAL_MS, value) / 1000


class C2CStreamDraft:
    """One streamed reply draft rendered as a QQ C2C stream message."""

    def __init__(self, client: QQBotClient, openid: str, msg_id: str, interval: float) -> None:
        self.client = client
        self.openid = openid
        self.msg_id = msg_id
        self.interval = interval
        self.raw = ""
        self.sent = ""
        self.stream_msg_id: str | None = None
        self.msg_seq: int | None = None
        self.index = 0
        self.ended_at: float | None = None
        self.closed = False
        self.failed = False
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    @property
    def opened(self) -> bool:
        return self.stream_msg_id is not None

    def start(self, on_exit: Any) -> None:
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(lambda _task: on_exit(self))

    def feed(self, token: str) -> None:
        if not self.closed:
            self.raw += token

    def end(self) -> None:
        self.ended_at = time.monotonic()

    async def _run(self) -> None:
        try:
            await asyncio.sleep(self.interval)
            while not self.closed:
                await self._push()
                if self.ended_at is not None and time.monotonic() - self.ended_at > FINALIZE_GRACE_SECONDS:
                    logger.warning("QQ Bot 流式草稿 %ss 内没有等到正式回复，按已显示内容结束", int(FINALIZE_GRACE_SECONDS))
                    await self._close()
                    return
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass

    async def _push(self) -> None:
        async with self._lock:
            if self.closed or self.failed:
                return
            try:
                text = render_draft(self.raw)
            except Exception:
                logger.debug("QQ Bot 流式草稿渲染失败", exc_info=True)
                return
            if not text or text == self.sent or not text.startswith(self.sent):
                return
            try:
                await self._frame(text, STATE_GENERATING)
            except QQBotAPIError as exc:
                self.failed = True
                if not self.opened:
                    _backoff_until[self.client.account_id] = time.monotonic() + STREAM_BACKOFF_SECONDS
                    logger.warning("QQ Bot 私聊流式消息不可用，%d 分钟内改为整条发送：%s", int(STREAM_BACKOFF_SECONDS // 60), exc)
                else:
                    logger.warning("QQ Bot 流式分片发送失败，等待正式回复收尾：%s", exc)

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
        self.sent = text

    def _stop_task(self) -> None:
        if self._task is not None and self._task is not asyncio.current_task():
            self._task.cancel()

    async def _close(self) -> None:
        """Finish the stream with what QQ already shows (the lock must be free)."""
        async with self._lock:
            if self.closed:
                return
            self.closed = True
            if not self.opened:
                return
            try:
                await self._frame(self.sent, STATE_DONE)
            except QQBotAPIError as exc:
                logger.warning("QQ Bot 结束流式消息失败：%s", exc)

    async def abandon(self) -> None:
        """Foxran discarded this draft: QQ cannot recall it, so close it as shown."""
        self._stop_task()
        if self.opened and not self.closed:
            logger.info("QQ Bot 流式草稿被作废（模型重试），已显示的 %d 字保留", len(self.sent))
        await self._close()

    async def finalize(self, text: str) -> str | None:
        """End the stream with the delivered reply text.

        Returns the stream's message id, or None when the reply must be sent as
        an ordinary message (no frame was shown, or QQ refused the final frame).
        """
        self._stop_task()
        async with self._lock:
            if self.closed:
                return None
            self.closed = True
            if not self.opened:
                return None
            if not text:
                text = self.sent
            try:
                await self._frame(text, STATE_DONE)
                return self.stream_msg_id
            except QQBotAPIError as exc:
                logger.warning("QQ Bot 流式消息收尾失败，改为整条补发：%s", exc)
            if text != self.sent:
                try:
                    await self._frame(self.sent, STATE_DONE)
                except QQBotAPIError:
                    pass
            return None


class C2CStreamRegistry:
    """Open drafts keyed by (session id, Foxran stream id)."""

    def __init__(self) -> None:
        self._drafts: dict[tuple[str, str], C2CStreamDraft] = {}

    def __len__(self) -> int:
        return len(self._drafts)

    def start(self, session_id: str, stream_id: str, draft: C2CStreamDraft) -> None:
        key = (session_id, stream_id)
        previous = self._drafts.get(key)
        if previous is not None:
            asyncio.create_task(previous.abandon())
        self._drafts[key] = draft

        def forget(finished: C2CStreamDraft) -> None:
            # The loop exits on cancel (finalize/abandon) or after the grace
            # period; a finalized draft has already been popped.
            if finished.closed and self._drafts.get(key) is finished:
                self._drafts.pop(key, None)

        draft.start(forget)

    def get(self, session_id: str, stream_id: str) -> C2CStreamDraft | None:
        return self._drafts.get((session_id, stream_id))

    def pop(self, session_id: str, stream_id: str) -> C2CStreamDraft | None:
        return self._drafts.pop((session_id, stream_id), None)

    def pop_session(self, session_id: str) -> list[C2CStreamDraft]:
        keys = [key for key in self._drafts if key[0] == session_id]
        return [self._drafts.pop(key) for key in keys]


c2c_streams = C2CStreamRegistry()
