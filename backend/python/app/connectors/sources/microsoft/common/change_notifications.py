"""Microsoft change notifications ("Layer 2" event-driven sync) — shared helpers.

CGraph is internal-only, so Microsoft posts notifications to edrak-ai
(``FRONTEND_PUBLIC_URL``), which forwards them to
``POST /api/v1/connectors/internal/{connector_id}/notify``.  This module holds
what every Microsoft connector needs to register for those notifications:

* the shared secret each registration carries (Graph/BC ``clientState``,
  Dataverse ``x-edrak-webhook-key``), derived from the ``scopedJwtSecret`` the
  fork already shares with edrak-ai;
* the public receiver URL per connector instance;
* expiry arithmetic and a small persisted registry (sync point ``webhooks``);
* :class:`GraphSubscriptionManager` — create / renew / recreate / remove Microsoft
  Graph subscriptions over plain HTTPS (no Graph SDK dependency).

Registration failures never fail a sync: callers go through
:func:`sync_graph_subscriptions`, which logs and returns.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import quote

import httpx

from app.config.constants.service import config_node_constants

if TYPE_CHECKING:
    from logging import Logger

WEBHOOKS_SYNC_POINT_KEY = "webhooks"
CLIENT_STATE_PREFIX = "ms-webhook:"
CLIENT_STATE_LENGTH = 40
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_TOKEN_SCOPE = "https://graph.microsoft.com/.default"

# Maximum subscription lifetimes Microsoft Graph accepts, in minutes.
DRIVE_ITEM_MAX_MINUTES = 4230          # driveItem ≈ 3 days
MAIL_MAX_MINUTES = 4230                # message ≈ 3 days
CHAT_MESSAGE_MAX_MINUTES = 60          # chatMessage
DIRECTORY_MAX_MINUTES = 41 * 24 * 60   # group / user ≈ 41 days
EXPIRY_MARGIN_MINUTES = 5
DEFAULT_RENEW_WITHIN_MINUTES = 120

TokenGetter = Callable[[], Awaitable[str]]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def webhook_client_state(scoped_jwt_secret: str, connector_id: str) -> str:
    """``hex(HMAC-SHA256(key=scopedJwtSecret, msg="ms-webhook:" + connectorId))[:40]``."""
    digest = hmac.new(
        scoped_jwt_secret.encode("utf-8"),
        f"{CLIENT_STATE_PREFIX}{connector_id}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest[:CLIENT_STATE_LENGTH]


def receiver_url(public_origin: str, kind: str, connector_id: str) -> str:
    """edrak-ai receiver for one connector instance; ``kind`` is ``graph``, ``dataverse`` or ``bc``."""
    origin = public_origin.strip().rstrip("/")
    return f"{origin}/api/webhooks/microsoft/{kind}/{quote(connector_id, safe='')}"


def public_origin() -> str:
    return os.getenv("FRONTEND_PUBLIC_URL", "").strip().rstrip("/")


def format_graph_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Graph may return more than 6 fractional digits, which fromisoformat rejects.
    if "." in text:
        head, _, rest = text.partition(".")
        digits = ""
        idx = 0
        while idx < len(rest) and rest[idx].isdigit():
            digits += rest[idx]
            idx += 1
        text = f"{head}.{digits[:6].ljust(6, '0')}{rest[idx:]}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def expiration_for(max_minutes: int, now: datetime | None = None) -> datetime:
    minutes = max(max_minutes - EXPIRY_MARGIN_MINUTES, 1)
    return (now or utcnow()) + timedelta(minutes=minutes)


def is_expiring(expires_at: object, within_minutes: int, now: datetime | None = None) -> bool:
    """True when ``expires_at`` is missing, unparsable or inside the renewal window."""
    parsed = parse_iso_datetime(expires_at)
    if parsed is None:
        return True
    return parsed <= (now or utcnow()) + timedelta(minutes=within_minutes)


@dataclass(frozen=True)
class WebhookSettings:
    notification_url: str
    client_state: str


async def load_webhook_settings(
    config_service: ConfigServiceLike,
    connector_id: str,
    kind: str,
    logger: Logger,
) -> WebhookSettings | None:
    """Receiver URL + shared secret, or ``None`` (logged) when the deployment lacks either."""
    origin = public_origin()
    if not origin:
        logger.info("FRONTEND_PUBLIC_URL is not set; Microsoft change notifications stay disabled for %s", connector_id)
        return None
    secret_keys = await config_service.get_config(config_node_constants.SECRET_KEYS.value)
    secret = (secret_keys or {}).get("scopedJwtSecret") if isinstance(secret_keys, dict) else None
    if not secret:
        logger.warning("scopedJwtSecret is not configured; Microsoft change notifications stay disabled for %s", connector_id)
        return None
    return WebhookSettings(
        notification_url=receiver_url(origin, kind, connector_id),
        client_state=webhook_client_state(str(secret), connector_id),
    )


@dataclass
class Registration:
    id: str
    resource: str
    change_type: str
    expires_at: str | None
    max_minutes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "resource": self.resource,
            "changeType": self.change_type,
            "expiresAt": self.expires_at,
            "maxMinutes": self.max_minutes,
        }

    @classmethod
    def from_dict(cls, data: object) -> Registration | None:
        if not isinstance(data, dict) or not data.get("id") or not data.get("resource"):
            return None
        try:
            max_minutes = int(data.get("maxMinutes") or DRIVE_ITEM_MAX_MINUTES)
        except (TypeError, ValueError):
            max_minutes = DRIVE_ITEM_MAX_MINUTES
        return cls(
            id=str(data["id"]),
            resource=str(data["resource"]),
            change_type=str(data.get("changeType") or "updated"),
            expires_at=data.get("expiresAt") if isinstance(data.get("expiresAt"), str) else None,
            max_minutes=max_minutes,
        )


class SyncPointLike(Protocol):
    async def read_sync_point(self, sync_point_key: str) -> dict[str, Any]: ...

    async def update_sync_point(self, sync_point_key: str, sync_point_data: dict[str, Any]) -> object: ...


class ConfigServiceLike(Protocol):
    async def get_config(self, key: str) -> object: ...


class SubscriptionStore:
    """Registrations of one connector, persisted in its sync point under ``webhooks``."""

    def __init__(self, sync_point: SyncPointLike, key: str = WEBHOOKS_SYNC_POINT_KEY) -> None:
        self._sync_point = sync_point
        self._key = key

    async def load(self) -> list[Registration]:
        point = await self._sync_point.read_sync_point(self._key)
        rows = point.get(WEBHOOKS_SYNC_POINT_KEY) if isinstance(point, dict) else None
        if not isinstance(rows, list):
            return []
        loaded = [Registration.from_dict(row) for row in rows]
        return [reg for reg in loaded if reg is not None]

    async def save(self, registrations: list[Registration]) -> None:
        await self._sync_point.update_sync_point(
            self._key, {WEBHOOKS_SYNC_POINT_KEY: [reg.to_dict() for reg in registrations]}
        )


@dataclass(frozen=True)
class GraphResource:
    resource: str
    change_type: str
    max_minutes: int


class GraphSubscriptionError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class GraphSubscriptionTransport(Protocol):
    """HTTP shape of one subscription API. ``create``/``renew`` return ``{"id", "expirationDateTime"}``;
    ``list`` returns rows with ``id``, ``resource``, ``notificationUrl``, ``expirationDateTime``."""

    async def create(self, resource: GraphResource, notification_url: str, client_state: str, expiration: str) -> dict[str, Any]: ...

    async def renew(self, subscription_id: str, expiration: str) -> dict[str, Any]: ...

    async def delete(self, subscription_id: str) -> None: ...

    async def list(self) -> list[dict[str, Any]]: ...


class HttpxGraphSubscriptionTransport:
    """``/subscriptions`` over httpx with a bearer token from ``token_getter``."""

    def __init__(
        self,
        token_getter: TokenGetter,
        http: httpx.AsyncClient | None = None,
        base_url: str = GRAPH_BASE_URL,
        timeout_s: float = 30.0,
    ) -> None:
        self._token_getter = token_getter
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s

    async def create(self, resource: GraphResource, notification_url: str, client_state: str, expiration: str) -> dict[str, Any]:
        body = {
            "changeType": resource.change_type,
            "notificationUrl": notification_url,
            "resource": resource.resource,
            "expirationDateTime": expiration,
            "clientState": client_state,
        }
        return await self._send("POST", "/subscriptions", body)

    async def renew(self, subscription_id: str, expiration: str) -> dict[str, Any]:
        return await self._send(
            "PATCH", f"/subscriptions/{quote(subscription_id, safe='')}", {"expirationDateTime": expiration}
        )

    async def delete(self, subscription_id: str) -> None:
        await self._send("DELETE", f"/subscriptions/{quote(subscription_id, safe='')}")

    async def list(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        url: str | None = "/subscriptions"
        while url:
            payload = await self._send("GET", url)
            rows.extend(v for v in (payload.get("value") or []) if isinstance(v, dict))
            url = payload.get("@odata.nextLink") or None
        return rows

    async def _send(self, method: str, path_or_url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = path_or_url if path_or_url.startswith("http") else f"{self._base_url}{path_or_url}"
        headers = {"Authorization": f"Bearer {await self._token_getter()}", "Accept": "application/json"}
        if self._http is not None:
            response = await self._http.request(method, url, json=body, headers=headers)
        else:
            async with httpx.AsyncClient(timeout=httpx.Timeout(self._timeout_s, connect=15.0)) as client:
                response = await client.request(method, url, json=body, headers=headers)
        return response_payload(response)


def response_payload(response: httpx.Response) -> dict[str, Any]:
    """JSON object of a 2xx response (``{}`` when empty); ``GraphSubscriptionError`` on 4xx/5xx."""
    if response.status_code >= 400:
        raise GraphSubscriptionError(response.status_code, response.text[:500])
    if not response.content:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


class GraphSubscriptionManager:
    """Keeps one connector's change-notification subscriptions alive.

    Written for Microsoft Graph; Business Central reuses it through its own
    transport (``business_central/webhooks.py``) because the lifecycle is the same.

    ``ensure`` creates what is missing for the given resources, ``renew_expiring``
    extends (or recreates after a 404) the ones close to expiry, ``remove_all``
    deletes everything.  A 403 (missing application permission or unapproved
    protected API) is logged once per manager and that resource is skipped —
    polling stays the fallback.
    """

    def __init__(
        self,
        client: GraphSubscriptionTransport,
        connector_id: str,
        notification_url: str,
        client_state: str,
        logger: Logger,
        store: SubscriptionStore | None = None,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        self._client = client
        self.connector_id = connector_id
        self.notification_url = notification_url
        self.client_state = client_state
        self.logger = logger
        self._store = store
        self._now = now
        self._registrations: list[Registration] | None = None
        self._forbidden_logged = False

    async def registrations(self) -> list[Registration]:
        if self._registrations is None:
            self._registrations = await self._store.load() if self._store is not None else []
            if not self._registrations:
                await self._adopt_existing()
        return self._registrations

    async def _adopt_existing(self) -> None:
        """Pick up subscriptions Graph still holds for this receiver (state lost by a full sync)."""
        try:
            existing = await self._client.list()
        except Exception as e:
            self.logger.debug("Could not list Graph subscriptions for %s: %s", self.connector_id, e)
            return
        adopted: list[Registration] = []
        for row in existing:
            if row.get("notificationUrl") != self.notification_url or not row.get("id") or not row.get("resource"):
                continue
            adopted.append(
                Registration(
                    id=str(row["id"]),
                    resource=str(row["resource"]).lstrip("/"),
                    change_type=str(row.get("changeType") or "updated"),
                    expires_at=row.get("expirationDateTime"),
                    max_minutes=DRIVE_ITEM_MAX_MINUTES,
                )
            )
        if adopted:
            self._registrations = adopted
            self.logger.info("Adopted %d existing Graph subscription(s) for %s", len(adopted), self.connector_id)

    async def _save(self) -> None:
        if self._store is not None and self._registrations is not None:
            await self._store.save(self._registrations)

    def _log_forbidden(self, resource: str, error: Exception) -> None:
        if self._forbidden_logged:
            self.logger.debug("Graph subscription for %s skipped (403)", resource)
            return
        self._forbidden_logged = True
        self.logger.warning(
            "Microsoft Graph refused a change-notification subscription for %s (403). The app lacks the "
            "permission for that resource (Teams channel messages also need Microsoft's protected-API "
            "approval); polling remains the fallback. (%s)",
            resource, str(error)[:200],
        )

    async def _create(self, resource: GraphResource) -> Registration | None:
        expires = expiration_for(resource.max_minutes, self._now())
        try:
            created = await self._client.create(
                resource, self.notification_url, self.client_state, format_graph_datetime(expires)
            )
        except GraphSubscriptionError as e:
            if e.status_code == 403:
                self._log_forbidden(resource.resource, e)
            else:
                self.logger.warning("Could not create Graph subscription for %s: %s", resource.resource, e)
            return None
        subscription_id = created.get("id")
        if not subscription_id:
            self.logger.warning("Graph returned no subscription id for %s", resource.resource)
            return None
        return Registration(
            id=str(subscription_id),
            resource=resource.resource,
            change_type=resource.change_type,
            expires_at=created.get("expirationDateTime") or format_graph_datetime(expires),
            max_minutes=resource.max_minutes,
        )

    async def ensure(self, resources: Iterable[GraphResource]) -> list[Registration]:
        """Create subscriptions for resources that have none yet; other registrations are left alone."""
        current = await self.registrations()
        known = {reg.resource.lstrip("/") for reg in current}
        created_count = 0
        for resource in resources:
            if resource.resource.lstrip("/") in known:
                continue
            registration = await self._create(resource)
            if registration is None:
                continue
            current.append(registration)
            known.add(resource.resource.lstrip("/"))
            created_count += 1
        if created_count:
            self.logger.info("Created %d Graph subscription(s) for %s", created_count, self.connector_id)
        await self._save()
        return current

    async def renew_expiring(self, within_minutes: int = DEFAULT_RENEW_WITHIN_MINUTES) -> None:
        current = await self.registrations()
        now = self._now()
        kept: list[Registration] = []
        changed = False
        for reg in current:
            if not is_expiring(reg.expires_at, within_minutes, now):
                kept.append(reg)
                continue
            changed = True
            expires = expiration_for(reg.max_minutes, now)
            try:
                renewed = await self._client.renew(reg.id, format_graph_datetime(expires))
            except GraphSubscriptionError as e:
                if e.status_code == 404:
                    # Graph dropped it (expired or deleted): start over for that resource.
                    recreated = await self._create(GraphResource(reg.resource, reg.change_type, reg.max_minutes))
                    if recreated is not None:
                        kept.append(recreated)
                    continue
                if e.status_code == 403:
                    self._log_forbidden(reg.resource, e)
                    continue
                self.logger.warning("Could not renew Graph subscription %s (%s): %s", reg.id, reg.resource, e)
                kept.append(reg)
                continue
            reg.expires_at = renewed.get("expirationDateTime") or format_graph_datetime(expires)
            kept.append(reg)
        if changed:
            self._registrations = kept
            await self._save()

    async def remove_all(self) -> None:
        current = await self.registrations()
        for reg in current:
            try:
                await self._client.delete(reg.id)
            except GraphSubscriptionError as e:
                if e.status_code != 404:
                    self.logger.warning("Could not delete Graph subscription %s (%s): %s", reg.id, reg.resource, e)
            except Exception as e:
                self.logger.warning("Could not delete Graph subscription %s (%s): %s", reg.id, reg.resource, e)
        self._registrations = []
        await self._save()


async def sync_graph_subscriptions(
    *,
    config_service: ConfigServiceLike,
    connector_id: str,
    token_getter: TokenGetter,
    sync_point: SyncPointLike,
    resources: Iterable[GraphResource],
    logger: Logger,
    renew_within_minutes: int = DEFAULT_RENEW_WITHIN_MINUTES,
) -> None:
    """Post-run hook for the Graph connectors: ensure + renew, never raises."""
    resources = list(resources)
    try:
        settings = await load_webhook_settings(config_service, connector_id, "graph", logger)
        if settings is None:
            return
        manager = GraphSubscriptionManager(
            HttpxGraphSubscriptionTransport(token_getter),
            connector_id,
            settings.notification_url,
            settings.client_state,
            logger,
            store=SubscriptionStore(sync_point),
        )
        if resources:
            await manager.ensure(resources)
        await manager.renew_expiring(renew_within_minutes)
    except Exception as e:
        logger.warning("Microsoft Graph change-notification upkeep failed for %s: %s", connector_id, e)


async def remove_graph_subscriptions(
    *,
    config_service: ConfigServiceLike,
    connector_id: str,
    token_getter: TokenGetter,
    sync_point: SyncPointLike,
    logger: Logger,
) -> None:
    """Best-effort teardown on connector delete/disable; never raises."""
    try:
        settings = await load_webhook_settings(config_service, connector_id, "graph", logger)
        if settings is None:
            return
        manager = GraphSubscriptionManager(
            HttpxGraphSubscriptionTransport(token_getter),
            connector_id,
            settings.notification_url,
            settings.client_state,
            logger,
            store=SubscriptionStore(sync_point),
        )
        await manager.remove_all()
    except Exception as e:
        logger.warning("Could not remove Microsoft Graph subscriptions for %s: %s", connector_id, e)
