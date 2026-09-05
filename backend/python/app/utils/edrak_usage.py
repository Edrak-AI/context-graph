"""Edrak: forward LLM token usage to edrak-ai's AiUsageEvent ledger.

edrak-ai is the only UI and owns spend limits/dashboards; CGraph calls models on its own (chat,
agents, indexing-time extraction), so those tokens are invisible to edrak-ai unless CGraph reports
them. This posts one event per model call to ``EDRAK_USAGE_URL`` (edrak-ai
``POST /api/internal/cgraph-usage``) with a scoped HS256 JWT (scope ``edrak:usage``) signed with
the shared ``scopedJwtSecret`` — the same secret edrak-ai already uses to mint tokens for us.

Fire-and-forget: never awaited on the request path, never raises. Inert when ``EDRAK_USAGE_URL``
is unset, so upstream behaviour is unchanged outside Edrak's deployment.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import httpx
import jwt

from app.config.configuration_service import ConfigurationService
from app.config.constants.service import config_node_constants

logger = logging.getLogger(__name__)

_EDRAK_USAGE_URL_ENV = "EDRAK_USAGE_URL"
_SCOPE = "edrak:usage"
_TIMEOUT_S = 5.0


def is_enabled() -> bool:
    return bool(os.getenv(_EDRAK_USAGE_URL_ENV, "").strip())


async def _scoped_token(config_service: ConfigurationService) -> str | None:
    secret_keys = await config_service.get_config(config_node_constants.SECRET_KEYS.value)
    secret = (secret_keys or {}).get("scopedJwtSecret") if isinstance(secret_keys, dict) else None
    if not secret:
        return None
    now = int(time.time())
    return jwt.encode(
        {"scopes": [_SCOPE], "issuer": "cgraph", "iat": now, "exp": now + 300},
        secret,
        algorithm="HS256",
    )


async def _post(config_service: ConfigurationService, event: dict[str, Any]) -> None:
    url = os.getenv(_EDRAK_USAGE_URL_ENV, "").strip()
    if not url:
        return
    try:
        token = await _scoped_token(config_service)
        if not token:
            logger.debug("edrak usage: no scopedJwtSecret configured; skipping")
            return
        async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
            resp = await client.post(url, json=event, headers={"Authorization": f"Bearer {token}"})
            if resp.status_code >= 400:
                logger.warning("edrak usage: %s -> HTTP %s %s", url, resp.status_code, resp.text[:200])
    except Exception as exc:  # noqa: BLE001 — accounting must never affect the request
        logger.warning("edrak usage: post failed: %s", exc)


def report_usage(
    config_service: ConfigurationService,
    *,
    org_id: str | None,
    user_id: str | None,
    model: str | None,
    provider: str | None,
    input_tokens: int,
    output_tokens: int,
    kind: str,
    request_id: str | None = None,
) -> None:
    """Schedule a usage event; safe to call from any coroutine (no-op without EDRAK_USAGE_URL)."""
    if not is_enabled():
        return
    if (input_tokens or 0) <= 0 and (output_tokens or 0) <= 0:
        return
    event = {
        "cgraphOrgId": org_id,
        "cgraphUserId": user_id,
        "model": model,
        "provider": provider,
        "inputTokens": int(input_tokens or 0),
        "outputTokens": int(output_tokens or 0),
        "kind": kind,
        "requestId": request_id,
        "occurredAt": int(time.time() * 1000),
    }
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(_post(config_service, event))
