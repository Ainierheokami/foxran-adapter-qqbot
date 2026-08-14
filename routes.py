from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from app.adapters.qqbot.config import qqbot_config
from app.adapters.qqbot.handlers import handle_event
from app.adapters.qqbot.client import qqbot_clients
from app.api.endpoints.auth import require_auth

router = APIRouter()


def validation_signature(secret: str, event_ts: str, plain_token: str) -> str:
    """Implement the official seed expansion and Ed25519 callback signature."""
    seed = secret.encode("utf-8")
    while len(seed) < 32:
        seed += seed
    return Ed25519PrivateKey.from_private_bytes(seed[:32]).sign((event_ts + plain_token).encode()).hex()


def _public_account(account: dict[str, Any], status: dict[str, Any]) -> dict[str, Any]:
    result = dict(account)
    result["app_secret"] = ""
    result["app_secret_set"] = bool(account.get("app_secret"))
    result["status"] = status or {"state": "disabled" if not account.get("enabled") else "stopped", "last_error": ""}
    result["webhook_url"] = f"/qqbot/webhook/{account['id']}"
    return result


@router.get("/api/adapters/qqbot/accounts")
async def get_accounts(_: bool = Depends(require_auth)):
    from app.adapters.qqbot.gateway import qqbot_gateway

    statuses = qqbot_gateway.status()
    return {"accounts": [_public_account(account, statuses.get(str(account["id"]), {})) for account in qqbot_config.get_accounts(force_reload=True)]}


@router.put("/api/adapters/qqbot/accounts")
async def put_accounts(request: Request, _: bool = Depends(require_auth)):
    body = await request.json()
    accounts = body.get("accounts") if isinstance(body, dict) else None
    if not isinstance(accounts, list) or not accounts:
        raise HTTPException(status_code=400, detail="至少需要一个 QQBot 账户")
    existing = {str(item["id"]): item for item in qqbot_config.get_accounts(force_reload=True)}
    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in accounts:
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="账户配置格式错误")
        account_id = str(raw.get("id") or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", account_id) or account_id in seen:
            raise HTTPException(status_code=400, detail=f"账户 ID 无效或重复: {account_id}")
        seen.add(account_id)
        account = dict(raw)
        account.pop("status", None)
        account.pop("app_secret_set", None)
        account.pop("webhook_url", None)
        if not str(account.get("app_secret") or ""):
            account["app_secret"] = str(existing.get(account_id, {}).get("app_secret") or "")
        transport = str(account.get("transport") or "websocket")
        if transport not in {"websocket", "webhook", "both"}:
            raise HTTPException(status_code=400, detail=f"不支持的 transport: {transport}")
        account["intents"] = int(account.get("intents") or 0)
        account["shards"] = max(1, int(account.get("shards") or 1))
        cleaned.append(account)
    from app.adapters.qqbot.gateway import qqbot_gateway

    await qqbot_gateway.stop()
    qqbot_config.save_accounts(cleaned)
    await qqbot_gateway.start()
    return await get_accounts(True)


@router.post("/api/adapters/qqbot/reload")
async def reload_accounts(_: bool = Depends(require_auth)):
    from app.adapters.qqbot.gateway import qqbot_gateway

    await qqbot_gateway.stop()
    qqbot_config.get(force_reload=True)
    await qqbot_gateway.start()
    return await get_accounts(True)


async def _qqbot_webhook(request: Request, account_id: str):
    cfg = qqbot_config.get_account(account_id, force_reload=True)
    if not cfg:
        raise HTTPException(status_code=404, detail="unknown QQ Bot account")
    if not cfg.get("enabled") or cfg.get("transport") not in {"webhook", "both"}:
        raise HTTPException(status_code=404, detail="QQ Bot webhook disabled")
    configured_app_id = str(cfg.get("app_id") or "")
    received_app_id = request.headers.get("X-Bot-Appid", "")
    if configured_app_id and received_app_id and received_app_id != configured_app_id:
        raise HTTPException(status_code=401, detail="unexpected bot app id")
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid QQ Bot payload")
    data, op = payload.get("d") or {}, payload.get("op")
    if op == 13:
        secret = str(cfg.get("app_secret") or "")
        if not secret:
            raise HTTPException(status_code=500, detail="missing QQ Bot app_secret")
        plain_token, event_ts = str(data.get("plain_token") or ""), str(data.get("event_ts") or "")
        if not plain_token or not event_ts:
            raise HTTPException(status_code=400, detail="invalid callback validation payload")
        return {"plain_token": plain_token, "signature": validation_signature(secret, event_ts, plain_token)}
    if op == 0:
        await handle_event(
            str(payload.get("t") or ""), data, payload.get("id"),
            account_id=account_id, client=qqbot_clients.get(account_id),
        )
    # QQ Bot requires HTTP callback ACK (opcode 12) after accepting an event.
    return {"op": 12, "d": {}}


@router.post("/qqbot/webhook")
async def qqbot_webhook(request: Request):
    """Backwards-compatible webhook for the default account."""
    return await _qqbot_webhook(request, "default")


@router.post("/qqbot/webhook/{account_id}")
async def qqbot_webhook_account(request: Request, account_id: str):
    return await _qqbot_webhook(request, account_id)
