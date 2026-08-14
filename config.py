from __future__ import annotations

import threading
import os
from pathlib import Path
from typing import Any

import yaml

from app.utils.yaml_utils import ensure_config_from_template, load_yaml_config_with_template

CONFIG_PATH = Path("config/qqbot.yml")
TEMPLATE_PATH = Path(__file__).parent / "template.yml"
DEFAULTS: dict[str, Any] = {
    "enabled": False, "app_id": "", "app_secret": "", "transport": "websocket",
    "bot_openid": "",
    "webhook_path": "/qqbot/webhook", "api_base_url": "https://api.bot.qq.com",
    "token_url": "https://bots.qq.com/app/getAppAccessToken", "intents": (1 << 25) | (1 << 30),
    "shards": 1, "use_group_as_session": True, "markdown_enabled": True,
    "reconnect_initial_delay": 2.0,
    "reconnect_max_delay": 30.0,
}


def _normalize_accounts(value: dict[str, Any]) -> list[dict[str, Any]]:
    raw_accounts = value.get("accounts")
    if not isinstance(raw_accounts, list) or not raw_accounts:
        legacy = {
            **DEFAULTS,
            **{key: item for key, item in value.items() if key != "accounts"},
        }
        legacy["id"] = str(legacy.get("id") or "default")
        legacy["name"] = str(legacy.get("name") or "默认 QQBot")
        return [legacy]

    accounts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_accounts):
        if not isinstance(item, dict):
            continue
        account = {**DEFAULTS, **item}
        account_id = str(account.get("id") or f"qqbot-{index + 1}").strip()
        if not account_id or account_id in seen:
            continue
        seen.add(account_id)
        account["id"] = account_id
        account["name"] = str(account.get("name") or account_id)
        accounts.append(account)
    return accounts or [{**DEFAULTS, "id": "default", "name": "默认 QQBot"}]


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
            self._value["accounts"] = _normalize_accounts(self._value)
            self._mtime = mtime
            return dict(self._value)

    def get_accounts(self, force_reload: bool = False) -> list[dict[str, Any]]:
        value = self.get(force_reload=force_reload)
        return [dict(account) for account in value.get("accounts", []) if isinstance(account, dict)]

    def get_account(self, account_id: str, force_reload: bool = False) -> dict[str, Any]:
        wanted = str(account_id or "default")
        return next(
            (account for account in self.get_accounts(force_reload) if str(account.get("id")) == wanted),
            {},
        )

    def save_accounts(self, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized = _normalize_accounts({"accounts": accounts})
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + ".tmp")
        temporary.write_text(
            yaml.safe_dump({"accounts": normalized}, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        os.replace(temporary, CONFIG_PATH)
        self.get(force_reload=True)
        return self.get_accounts()


qqbot_config = QQBotConfig()
