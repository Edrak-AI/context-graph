"""Business Central API subscriptions for near-real-time sync (Layer 2).

BC's ``/api/v2.0/subscriptions`` behaves like Microsoft Graph's subscriptions
(handshake echo on create, ``clientState`` on every notification, 3-day maximum
lifetime, renewal by ``PATCH``), so the shared :class:`GraphSubscriptionManager`
drives the lifecycle and this module only supplies the BC-shaped transport and
the resource list: one subscription per synced company × entity set,
``/api/v2.0/companies(<id>)/<entitySet>``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol
from urllib.parse import quote

from app.connectors.sources.microsoft.common.change_notifications import (
    GraphResource,
    GraphSubscriptionError,
)

BC_SUBSCRIPTION_MAX_MINUTES = 3 * 24 * 60
BC_RENEW_WITHIN_MINUTES = 12 * 60
BC_CHANGE_TYPE = "created,updated,deleted"

# (method, path, body, extra headers) -> (status, payload)
SendJson = Callable[[str, str, dict[str, Any] | None, dict[str, str] | None], Awaitable[tuple[int, dict[str, Any]]]]


class HasId(Protocol):
    id: str


class HasEntitySet(Protocol):
    entity_set: str


def bc_resource(company_id: str, entity_set: str) -> str:
    return f"/api/v2.0/companies({company_id})/{entity_set}"


def bc_resources(companies: Sequence[HasId], specs: Sequence[HasEntitySet]) -> list[GraphResource]:
    return [
        GraphResource(bc_resource(company.id, spec.entity_set), BC_CHANGE_TYPE, BC_SUBSCRIPTION_MAX_MINUTES)
        for company in companies
        for spec in specs
    ]


def subscription_body(notification_url: str, resource: str, client_state: str) -> dict[str, Any]:
    return {"notificationUrl": notification_url, "resource": resource, "clientState": client_state}


def renewal_body(expiration: str) -> dict[str, Any]:
    return {"expirationDateTime": expiration}


def subscription_ref(subscription_id: str) -> str:
    return f"subscriptions('{quote(subscription_id, safe='')}')"


def normalize_subscription_row(row: dict[str, Any]) -> dict[str, Any]:
    """BC names the key ``subscriptionId``; the manager expects Graph's ``id``."""
    normalized = dict(row)
    if "id" not in normalized and row.get("subscriptionId"):
        normalized["id"] = row["subscriptionId"]
    return normalized


class BcSubscriptionTransport:
    """``GraphSubscriptionTransport`` over the connector's authenticated ``_send_json``."""

    def __init__(self, send_json: SendJson) -> None:
        self._send_json = send_json

    async def _call(
        self, method: str, path: str, body: dict[str, Any] | None, headers: dict[str, str] | None = None
    ) -> dict[str, Any]:
        status, payload = await self._send_json(method, path, body, headers)
        if status >= 400:
            raise GraphSubscriptionError(status, str(payload)[:500])
        return normalize_subscription_row(payload) if isinstance(payload, dict) else {}

    async def create(self, resource: GraphResource, notification_url: str, client_state: str, expiration: str) -> dict[str, Any]:
        # BC fixes the lifetime itself (3 days); expiration is not part of the request.
        return await self._call("POST", "subscriptions", subscription_body(notification_url, resource.resource, client_state))

    async def renew(self, subscription_id: str, expiration: str) -> dict[str, Any]:
        return await self._call("PATCH", subscription_ref(subscription_id), renewal_body(expiration), {"If-Match": "*"})

    async def delete(self, subscription_id: str) -> None:
        await self._call("DELETE", subscription_ref(subscription_id), None, {"If-Match": "*"})

    async def list(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        path: str | None = "subscriptions"
        while path:
            payload = await self._call("GET", path, None)
            rows.extend(normalize_subscription_row(v) for v in (payload.get("value") or []) if isinstance(v, dict))
            path = payload.get("@odata.nextLink") or None
        return rows
