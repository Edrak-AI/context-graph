"""Business Central subscription resources (pure) and the BC transport under the shared manager."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from app.connectors.sources.microsoft.business_central.webhooks import (
    BC_CHANGE_TYPE,
    BC_RENEW_WITHIN_MINUTES,
    BC_SUBSCRIPTION_MAX_MINUTES,
    BcSubscriptionTransport,
    bc_resource,
    bc_resources,
    normalize_subscription_row,
    subscription_body,
    subscription_ref,
)
from app.connectors.sources.microsoft.common.change_notifications import (
    GraphSubscriptionManager,
    SubscriptionStore,
)

LOGGER = logging.getLogger("test-bc-webhooks")
NOW = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)
URL = "https://dev.edrak.com/api/webhooks/microsoft/bc/conn-1"


@dataclass
class Company:
    id: str


@dataclass
class Spec:
    entity_set: str


class TestResources:
    def test_resource_per_company_and_entity_set(self) -> None:
        resources = bc_resources([Company("c-1"), Company("c-2")], [Spec("customers"), Spec("salesOrders")])
        assert [r.resource for r in resources] == [
            "/api/v2.0/companies(c-1)/customers", "/api/v2.0/companies(c-1)/salesOrders",
            "/api/v2.0/companies(c-2)/customers", "/api/v2.0/companies(c-2)/salesOrders",
        ]
        assert {r.change_type for r in resources} == {BC_CHANGE_TYPE}
        assert {r.max_minutes for r in resources} == {BC_SUBSCRIPTION_MAX_MINUTES}
        assert BC_SUBSCRIPTION_MAX_MINUTES == 3 * 24 * 60
        assert bc_resource("c", "items") == "/api/v2.0/companies(c)/items"

    def test_bodies_and_refs(self) -> None:
        assert subscription_body(URL, "/api/v2.0/companies(c)/items", "state") == {
            "notificationUrl": URL, "resource": "/api/v2.0/companies(c)/items", "clientState": "state",
        }
        assert subscription_ref("ab c") == "subscriptions('ab%20c')"
        assert normalize_subscription_row({"subscriptionId": "s1", "resource": "r"})["id"] == "s1"
        assert BC_RENEW_WITHIN_MINUTES == 12 * 60


class FakeBc:
    """``_send_json`` stand-in: BC returns ``subscriptionId`` and fixes a 3-day lifetime itself."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, str, dict[str, str] | None]] = []
        self._n = 0

    async def send_json(
        self, method: str, path: str, body: dict[str, Any] | None, headers: dict[str, str] | None = None
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, headers))
        if method == "POST":
            self._n += 1
            row = {
                "subscriptionId": f"s-{self._n}",
                "notificationUrl": (body or {})["notificationUrl"],
                "resource": (body or {})["resource"],
                "clientState": (body or {})["clientState"],
                "expirationDateTime": (NOW + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            self.rows[row["subscriptionId"]] = row
            return 201, dict(row)
        if method == "GET":
            return 200, {"value": [dict(r) for r in self.rows.values()]}
        sub_id = path[len("subscriptions('"):-2]
        if sub_id not in self.rows:
            return 404, {"error": {"code": "BadRequest_NotFound"}}
        if method == "PATCH":
            self.rows[sub_id]["expirationDateTime"] = (body or {})["expirationDateTime"]
            return 200, dict(self.rows[sub_id])
        del self.rows[sub_id]
        return 204, {}


class FakeSyncPoint:
    def __init__(self) -> None:
        self.points: dict[str, dict[str, Any]] = {}

    async def read_sync_point(self, key: str) -> dict[str, Any]:
        return dict(self.points.get(key, {}))

    async def update_sync_point(self, key: str, data: dict[str, Any], encrypt_fields: list[str] | None = None) -> None:
        self.points[key] = data


def _manager(api: FakeBc, point: FakeSyncPoint, now: datetime = NOW) -> GraphSubscriptionManager:
    return GraphSubscriptionManager(
        BcSubscriptionTransport(api.send_json), "conn-1", URL, "state", LOGGER,
        store=SubscriptionStore(point), now=lambda: now,
    )


class TestBcTransport:
    def test_create_renew_delete_lifecycle(self) -> None:
        api, point = FakeBc(), FakeSyncPoint()
        resources = bc_resources([Company("c-1")], [Spec("customers")])
        regs = asyncio.run(_manager(api, point).ensure(resources))
        assert [r.id for r in regs] == ["s-1"]
        assert api.rows["s-1"]["clientState"] == "state"
        assert point.points["webhooks"]["webhooks"][0]["id"] == "s-1"

        # more than 12 h left: untouched
        asyncio.run(_manager(api, point, now=NOW + timedelta(days=2)).renew_expiring(BC_RENEW_WITHIN_MINUTES))
        assert not [c for c in api.calls if c[0] == "PATCH"]

        # inside the window: PATCH with If-Match: *
        late = NOW + timedelta(days=2, hours=13)
        asyncio.run(_manager(api, point, now=late).renew_expiring(BC_RENEW_WITHIN_MINUTES))
        patches = [c for c in api.calls if c[0] == "PATCH"]
        assert patches == [("PATCH", "subscriptions('s-1')", {"If-Match": "*"})]

        # BC dropped it: renew 404 -> recreate
        del api.rows["s-1"]
        asyncio.run(_manager(api, point, now=late + timedelta(days=3)).renew_expiring(BC_RENEW_WITHIN_MINUTES))
        assert [r["id"] for r in point.points["webhooks"]["webhooks"]] == ["s-2"]

        asyncio.run(_manager(api, point).remove_all())
        assert api.rows == {}
        assert ("DELETE", "subscriptions('s-2')", {"If-Match": "*"}) in api.calls

    def test_adopts_bc_rows_by_subscription_id(self) -> None:
        api, point = FakeBc(), FakeSyncPoint()
        asyncio.run(api.send_json("POST", "subscriptions", subscription_body(URL, "/api/v2.0/companies(c-1)/items", "state")))
        regs = asyncio.run(_manager(api, point).ensure(bc_resources([Company("c-1")], [Spec("items")])))
        assert [r.id for r in regs] == ["s-1"]
        assert len([c for c in api.calls if c[0] == "POST"]) == 1  # adopted, not re-created
