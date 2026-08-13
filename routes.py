from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import APIRouter, HTTPException, Request

from app.adapters.qqbot.config import qqbot_config
from app.adapters.qqbot.handlers import handle_event

router = APIRouter()


def validation_signature(secret: str, event_ts: str, plain_token: str) -> str:
    """Implement the official seed expansion and Ed25519 callback signature."""
    seed = secret.encode("utf-8")
    while len(seed) < 32:
        seed += seed
    return Ed25519PrivateKey.from_private_bytes(seed[:32]).sign((event_ts + plain_token).encode()).hex()


@router.post("/qqbot/webhook")
async def qqbot_webhook(request: Request):
    cfg = qqbot_config.get(force_reload=True)
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
        await handle_event(str(payload.get("t") or ""), data, payload.get("id"))
    # QQ Bot requires HTTP callback ACK (opcode 12) after accepting an event.
    return {"op": 12, "d": {}}
