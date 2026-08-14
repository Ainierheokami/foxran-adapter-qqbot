from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from app.utils.yaml_utils import ensure_config_from_template, load_yaml_config_with_template

CONFIG_PATH = Path("config/qqbot.yml")
TEMPLATE_PATH = Path(__file__).parent / "template.yml"
DEFAULTS: dict[str, Any] = {
    "enabled": False, "app_id": "", "app_secret": "", "transport": "websocket",
    "bot_openid": "",
    "webhook_path": "/qqbot/webhook", "api_base_url": "https://api.bot.qq.com",
    "token_url": "https://bots.qq.com/app/getAppAccessToken", "intents": (1 << 25) | (1 << 30),
    "shards": 1, "use_group_as_session": True, "reconnect_initial_delay": 2.0,
    "reconnect_max_delay": 30.0,
}


class QQBotConfig:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mtime = -1.0
        self._value: dict[str, Any] | None = None

    def get(self, force_reload: bool = False) -> dict[str, Any]:
        if not CONFIG_PATH.exists():
            ensure_config_from_template(CONFIG_PATH, TEMPLATE_PATH)
        mtime = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0.0
        with self._lock:
            if self._value is not None and not force_reload and self._mtime >= mtime:
                return dict(self._value)
            self._value = load_yaml_config_with_template(CONFIG_PATH, TEMPLATE_PATH, DEFAULTS)
            self._mtime = mtime
            return dict(self._value)


qqbot_config = QQBotConfig()
