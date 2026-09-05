"""Edrak: pre-dispatch spend check against edrak-ai before an agent run / chat turn starts.

The usage callback in ``edrak_usage.py`` is post-hoc, so it cannot stop a run that would push an
org past its budget. This asks edrak-ai ``GET /api/internal/spend-gate`` (contract:
``edrak-ai/docs/11-cgraph-internal-endpoints.md``) once per conversation turn, authenticated with
the same scoped HS256 JWT (scope ``edrak:usage``) the usage reporter mints.

Decision rules (deliberately asymmetric so an edrak-ai outage never stops CGraph):

* **fail closed** only on an explicit ``{"allowed": false, ...}`` from edrak-ai;
* **fail open** on network errors, timeouts, non-2xx, unparsable bodies and ``reason:"unknown_user"``
  (logged at most once per minute);
* ``allowed: true`` answers from edrak-ai are cached for :data:`CACHE_TTL_S` per user so a
  multi-turn conversation does not pay one round-trip per turn (denials and fail-open results are
  never cached — a lifted limit must take effect on the next turn);
* inert (always allowed, no network) when neither ``EDRAK_SPEND_GATE_URL`` nor ``EDRAK_USAGE_URL``
  is set, so upstream behaviour is unchanged outside Edrak's deployment.

``httpx``/``jwt``/the configuration service are imported lazily so the decision logic is unit
testable with a fake transport and no third-party dependencies (see
``tests/unit/utils/test_edrak_spend_gate.py``).
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:  # pragma: no cover
    from app.config.configuration_service import ConfigurationService

logger = logging.getLogger(__name__)

SPEND_GATE_URL_ENV = "EDRAK_SPEND_GATE_URL"
USAGE_URL_ENV = "EDRAK_USAGE_URL"
_USAGE_PATH = "/api/internal/cgraph-usage"
_GATE_PATH = "/api/internal/spend-gate"

TIMEOUT_S = 2.0
CACHE_TTL_S = 30.0
FAIL_OPEN_LOG_INTERVAL_S = 60.0
_CACHE_MAX_ENTRIES = 10_000

# (http_status, parsed_json_or_None); raises on transport failure.
Transport = Callable[[str, dict[str, str], str], Awaitable[tuple[int, Any]]]
TokenProvider = Callable[[], Awaitable[str | None]]
Clock = Callable[[], float]


@dataclass(frozen=True)
class SpendGateResult:
    allowed: bool
    reason: str | None = None
    error_key: str | None = None
    scope: str | None = None
    spend_micro_usd: int | None = None
    limit_micro_usd: int | None = None
    #: ``disabled`` | ``edrak`` | ``cache`` | ``fail_open``
    source: str = "edrak"

    def http_detail(self) -> dict[str, Any]:
        """Body of the 429 the fork returns when ``allowed`` is False."""
        return {
            "error": "spend_limit",
            "reason": self.reason,
            "errorKey": self.error_key,
            "message": "AI spend limit reached; contact your organisation admin.",
        }


_ALLOWED_DISABLED = SpendGateResult(allowed=True, source="disabled")


def gate_url() -> str | None:
    """``EDRAK_SPEND_GATE_URL``, else derived from ``EDRAK_USAGE_URL``; ``None`` when neither is set."""
    explicit = os.getenv(SPEND_GATE_URL_ENV, "").strip()
    if explicit:
        return explicit
    usage = os.getenv(USAGE_URL_ENV, "").strip()
    if not usage:
        return None
    parts = urlsplit(usage)
    path = parts.path.rstrip("/")
    if path.endswith(_USAGE_PATH):
        path = path[: -len(_USAGE_PATH)]
    return urlunsplit((parts.scheme, parts.netloc, path + _GATE_PATH, "", ""))


def is_enabled() -> bool:
    return gate_url() is not None


def parse_response(status: int, body: Any) -> SpendGateResult:
    """Map an edrak-ai response to a decision. Anything not an explicit denial is an allow."""
    if status < 200 or status >= 300 or not isinstance(body, dict):
        return SpendGateResult(allowed=True, reason=f"http_{status}", source="fail_open")
    if body.get("allowed") is False:
        return SpendGateResult(
            allowed=False,
            reason=_opt_str(body.get("reason")) or "spend_limit",
            error_key=_opt_str(body.get("errorKey")),
            scope=_opt_str(body.get("scope")),
            spend_micro_usd=_opt_int(body.get("spendMicroUsd")),
            limit_micro_usd=_opt_int(body.get("limitMicroUsd")),
            source="edrak",
        )
    reason = _opt_str(body.get("reason"))
    if reason == "unknown_user":
        return SpendGateResult(allowed=True, reason=reason, source="fail_open")
    return SpendGateResult(allowed=True, reason=reason, source="edrak")


def _opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _opt_int(value: Any) -> int | None:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


# --- default collaborators (lazy third-party imports) -----------------------------------------

async def _httpx_transport(url: str, params: dict[str, str], token: str) -> tuple[int, Any]:
    import httpx  # noqa: PLC0415 — lazy so the decision logic imports without httpx

    async with httpx.AsyncClient(timeout=TIMEOUT_S) as client:
        resp = await client.get(url, params=params, headers={"Authorization": f"Bearer {token}"})
        try:
            body = resp.json()
        except ValueError:
            body = None
        return resp.status_code, body


def _token_provider_for(config_service: "ConfigurationService | None") -> TokenProvider:
    async def _provide() -> str | None:
        if config_service is None:
            return None
        from app.utils.edrak_usage import _scoped_token  # noqa: PLC0415 — same token as the usage reporter

        return await _scoped_token(config_service)

    return _provide


# --- cache + throttled logging ---------------------------------------------------------------

_cache: dict[str, tuple[float, SpendGateResult]] = {}
_last_fail_open_log: float = 0.0


def clear_cache() -> None:
    _cache.clear()


def _cache_get(user_id: str, now: float) -> SpendGateResult | None:
    hit = _cache.get(user_id)
    if hit is None:
        return None
    expires_at, result = hit
    if expires_at <= now:
        _cache.pop(user_id, None)
        return None
    return result


def _cache_put(user_id: str, result: SpendGateResult, now: float) -> None:
    if len(_cache) >= _CACHE_MAX_ENTRIES:
        expired = [k for k, (exp, _) in _cache.items() if exp <= now]
        for k in expired:
            _cache.pop(k, None)
        if len(_cache) >= _CACHE_MAX_ENTRIES:
            _cache.clear()
    _cache[user_id] = (now + CACHE_TTL_S, result)


def _log_fail_open(now: float, message: str, *args: Any) -> None:
    global _last_fail_open_log
    if now - _last_fail_open_log >= FAIL_OPEN_LOG_INTERVAL_S:
        _last_fail_open_log = now
        logger.warning("edrak spend gate: fail-open: " + message, *args)
    else:
        logger.debug("edrak spend gate: fail-open: " + message, *args)


# --- entry point ------------------------------------------------------------------------------

async def check_spend_allowed(
    user_id: str | None,
    org_id: str | None,
    config_service: "ConfigurationService | None" = None,
    *,
    transport: Transport | None = None,
    token_provider: TokenProvider | None = None,
    clock: Clock = time.monotonic,
) -> SpendGateResult:
    """Ask edrak-ai whether ``user_id`` may start a model run. Never raises.

    ``org_id`` is CGraph's org id; it is forwarded for logging only — edrak-ai resolves the
    budget owner from the user (``cgraphUserId``) itself.
    """
    url = gate_url()
    if url is None:
        return _ALLOWED_DISABLED
    if not user_id:
        return SpendGateResult(allowed=True, reason="no_user", source="fail_open")

    now = clock()
    cached = _cache_get(user_id, now)
    if cached is not None:
        return SpendGateResult(**{**cached.__dict__, "source": "cache"})

    try:
        token = await (token_provider or _token_provider_for(config_service))()
        if not token:
            _log_fail_open(now, "no scopedJwtSecret configured (user=%s org=%s)", user_id, org_id)
            return SpendGateResult(allowed=True, reason="no_token", source="fail_open")
        status, body = await (transport or _httpx_transport)(url, {"cgraphUserId": user_id}, token)
    except Exception as exc:  # noqa: BLE001 — a spend check must never take the request down
        _log_fail_open(now, "%s: %s (user=%s org=%s)", url, exc, user_id, org_id)
        return SpendGateResult(allowed=True, reason="transport_error", source="fail_open")

    result = parse_response(status, body)
    if result.source == "fail_open":
        _log_fail_open(now, "HTTP %s reason=%s (user=%s org=%s)", status, result.reason, user_id, org_id)
    elif result.allowed:
        _cache_put(user_id, result, now)
    else:
        logger.info(
            "edrak spend gate: blocked user=%s org=%s reason=%s errorKey=%s",
            user_id, org_id, result.reason, result.error_key,
        )
    return result
