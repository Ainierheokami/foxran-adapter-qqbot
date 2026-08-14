# QQ Bot OpenAPI adapter

Configure `config/qqbot.yml` with the **AppID** and **AppSecret** created at [QQ Open Platform](https://q.qq.com/), then set `enabled: true`.

- `transport: websocket` connects to `/gateway/bot`, identifies with the configured intents, heartbeats and resumes after a reconnect.
- `transport: webhook` exposes `POST /qqbot/webhook`; configure an HTTPS URL on port 80, 443, 8080, or 8443 in the QQ console. It implements opcode 13 callback validation and opcode 12 acknowledgement.
- `transport: both` enables both delivery mechanisms. Event IDs are deduplicated in-process.

The adapter supports `C2C_MESSAGE_CREATE`, `GROUP_AT_MESSAGE_CREATE`, `GROUP_MESSAGE_CREATE`, `AT_MESSAGE_CREATE`, `MESSAGE_CREATE`, and `DIRECT_MESSAGE_CREATE`. In full-group mode, QQ delivers messages as `GROUP_MESSAGE_CREATE`; the adapter compares `<@OpenID>` targets in the content with the bot's OpenID so direct mentions still trigger the mention policy. It obtains that OpenID from `/users/@me` by default, or you can set `bot_openid` in `config/qqbot.yml` as an override. Text replies route to the matching v2 user, group, or channel endpoint and retain the triggering `msg_id` required by QQ Bot.

Do not put AppSecret in source control. Grant the selected intents in the QQ console; requesting an unauthorized intent causes the gateway to close with 4014.
