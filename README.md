# QQ Bot OpenAPI adapter

Configure one or more entries under `accounts` in `config/qqbot.yml` with the **AppID** and **AppSecret** created at [QQ Open Platform](https://q.qq.com/), then set the account's `enabled: true`. Legacy top-level single-account configuration remains compatible as the `default` account. The Foxran console provides a dedicated QQBot account page; secrets are never returned to the browser and leaving the secret field blank preserves the stored value.

- `transport: websocket` connects to `/gateway/bot`, identifies with the configured intents, heartbeats and resumes after a reconnect.
- `transport: webhook` exposes `POST /qqbot/webhook/{account_id}`; the legacy `POST /qqbot/webhook` maps to the `default` account. Configure an HTTPS URL on port 80, 443, 8080, or 8443 in the QQ console. It implements opcode 13 callback validation and opcode 12 acknowledgement.
- `transport: both` enables both delivery mechanisms. Event IDs are deduplicated in-process.

The adapter supports `C2C_MESSAGE_CREATE`, `GROUP_AT_MESSAGE_CREATE`, `GROUP_MESSAGE_CREATE`, `AT_MESSAGE_CREATE`, `MESSAGE_CREATE`, and `DIRECT_MESSAGE_CREATE`. In full-group mode, QQ delivers messages as `GROUP_MESSAGE_CREATE`; the adapter compares `<@OpenID>` targets in the content with `bot_openid` so direct mentions still trigger the mention policy. Set `bot_openid` in `config/qqbot.yml` to the OpenID seen in a group message's `<@…>` tag: in some QQ scenes, `/users/@me` returns a bot ID that is not the group-message OpenID. Replies route to the matching v2 user, group, or channel endpoint and retain the triggering `msg_id` required by QQ Bot.

For group and C2C replies, `[image,url=…]`, `[video,url=…]`, and `[voice,url=…]` are uploaded through QQ's rich-media endpoint and then sent as `msg_type: 7`. Reply parts keep increasing `msg_seq` values for the same triggering message, including later sends such as an initial progress reply followed by generated media. QQ currently does not expose general file (`file_type: 4`) delivery, so `[file,url=…]` is sent as a clickable URL instead. Remote media must satisfy QQ's format and accessibility requirements: PNG/JPG images, MP4 videos, and SILK voice files reachable from QQ's servers.

Markdown replies are enabled by default with `markdown_enabled: true`. The adapter detects common Markdown constructs and sends them using QQ's `msg_type: 2` payload while leaving ordinary sentences as text. A definite QQ validation or business rejection falls back to plain text; transport and server errors are not retried to avoid duplicate replies.

Private (C2C) replies stream as they are generated, using QQ's `stream_messages` API (`c2c_stream: true`; groups and channels have no stream API and still receive whole messages). Foxran builds whose outbound sender does not forward stream events keep sending whole replies. QQ shows one stream at a time, so a reply of several messages streams them in order: each message is finished as soon as the model closes it, then the next one starts. A frame is sent at most every `c2c_stream_interval_ms` (default 600, minimum 300). A message the agent sends only after a tool or task step (`after=` in its plan) is already complete when it is sent, so it cannot stream as it is generated; with `c2c_stream_reveal: true` (default) it is revealed over at most five frames, adding about (frames − 1) × interval of delay, otherwise it appears at once. Images and other media in a streamed reply follow as separate messages; if the delivered text differs from what the stream showed, the reply is sent again as an ordinary message. The QQ client types each frame's new text out locally and stops when another message arrives, which leaves the earlier message looking cut off until the chat is reopened; so the next stream, and media following a stream, wait until the client has typed out the last text (`c2c_stream_settle_ms_per_char`, default 25 ms per character, at most 3 seconds; 0 disables). Text QQ has already shown cannot be recalled: the port reports shown drafts (`stream_drafts_shown`), so when new input arrives Foxran still delivers a reply the user already sees instead of discarding and regenerating it; when Foxran does discard a draft (a failed validation), the discarded draft stays visible. If QQ refuses to open a stream (for example the bot lacks Markdown permission), the adapter sends whole messages for the next 10 minutes. Streaming requires `markdown_enabled: true`.

Mentions: inbound `<@openid>`, `<qqbot-at-user id="openid" />` and `<qqbot-at-everyone />` become mention segments, so the agent reads `[at,id=…]` like on other platforms; the nickname is attached when QQ includes it in `mentions`. Outbound `[at,id=…]` is written as `<qqbot-at-user id="…" />` in groups and `<@…>` in guild channels, and dropped in private chats; `@全体成员` is only written as text, since a refused @everyone would block the whole message. QQ faces (`<faceType=1,faceId="13",ext="…">`) become face segments with their name (`[表情:呲牙]`). Group and C2C events carry no nickname, so a sender without one is shown as `QQ用户` plus the last four characters of the OpenID; the full OpenID stays the user ID.

Recalling messages is the host's framework tool `delete_msg`; this adapter implements `recall` on its outbound port. QQ only lets bots recall their own messages, so recalling another member's message is refused.

Do not put AppSecret in source control. Grant the selected intents in the QQ console; requesting an unauthorized intent causes the gateway to close with 4014.

## 📦 Python 依赖

`requirements.txt` 列出本适配器自己用到的库（`websockets>=14`、`cryptography`），插件市场安装和更新时会自动 `pip install`。手动安装时在宿主的 Python 环境里执行 `pip install -r requirements.txt`。

## 🖥 WebUI 账户设置

账户设置页随适配器一起发布：`web/` 是源码，构建产物是根目录的 `index.umd.js`（`foxran.yaml` 中的 `adapter.frontend_bundle`）。WebUI 启动时加载它，页面通过 `settingsPanels` 出现在“设置 → 适配器”里，并复用宿主提供的 `window.Foxran.api` 与 `window.Foxran.ui` 组件。需要宿主 4.0.0 及以上。

修改 `web/` 后重新构建：

```bash
cd web && npm install && npm run build
```
