"""Pure helpers and ``GraphSubscriptionManager`` with a fake Graph transport."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from app.connectors.sources.microsoft.common.change_notifications import (
    CHAT_MESSAGE_MAX_MINUTES,
    DRIVE_ITEM_MAX_MINUTES,
    EXPIRY_MARGIN_MINUTES,
    GraphResource,
    GraphSubscriptionError,
    GraphSubscriptionManager,
    Registration,
    SubscriptionStore,
    expiration_for,
    format_graph_datetime,
    is_expiring,
    load_webhook_settings,
    parse_iso_datetime,
    receiver_url,
    sync_graph_subscriptions,
    webhook_client_state,
)

if TYPE_CHECKING:
    import pytest

NOW = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)
LOGGER = logging.getLogger("test-change-notifications")


class TestPureHelpers:
    def test_client_state_is_truncated_hmac(self) -> None:
        expected = hmac.new(b"secret", b"ms-webhook:conn-1", hashlib.sha256).hexdigest()[:40]
        assert webhook_client_state("secret", "conn-1") == expected
        assert len(webhook_client_state("secret", "conn-1")) == 40
        assert webhook_client_state("secret", "conn-1") != webhook_client_state("secret", "conn-2")

    def test_receiver_url_per_kind(self) -> None:
        assert receiver_url("https://dev.edrak.com/", "graph", "c 1") == "https://dev.edrak.com/api/webhooks/microsoft/graph/c%201"
        assert receiver_url("https://dev.edrak.com", "dataverse", "x") == "https://dev.edrak.com/api/webhooks/microsoft/dataverse/x"
        assert receiver_url("https://dev.edrak.com", "bc", "x") == "https://dev.edrak.com/api/webhooks/microsoft/bc/x"

    def test_expiry_math(self) -> None:
        exp = expiration_for(DRIVE_ITEM_MAX_MINUTES, NOW)
        assert exp == NOW + timedelta(minutes=DRIVE_ITEM_MAX_MINUTES - EXPIRY_MARGIN_MINUTES)
        assert format_graph_datetime(exp) == "2026-09-12T10:25:00Z"
        assert expiration_for(1, NOW) == NOW + timedelta(minutes=1)

    def test_parse_iso_accepts_graph_precision(self) -> None:
        assert parse_iso_datetime("2026-09-09T12:00:00.1234567Z") == NOW + timedelta(microseconds=123456)
        assert parse_iso_datetime("2026-09-09T12:00:00Z") == NOW
        assert parse_iso_datetime("garbage") is None
        assert parse_iso_datetime(None) is None

    def test_is_expiring(self) -> None:
        assert is_expiring("2026-09-09T13:00:00Z", within_minutes=120, now=NOW)
        assert not is_expiring("2026-09-09T15:00:00Z", within_minutes=120, now=NOW)
        assert is_expiring(None, within_minutes=120, now=NOW)

    def test_load_settings_requires_origin_and_secret(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class Config:
            def __init__(self, keys: object) -> None:
                self.keys = keys

            async def get_config(self, key: str) -> object:
                return self.keys

        monkeypatch.delenv("FRONTEND_PUBLIC_URL", raising=False)
        assert asyncio.run(load_webhook_settings(Config({"scopedJwtSecret": "s"}), "c1", "graph", LOGGER)) is None
        monkeypatch.setenv("FRONTEND_PUBLIC_URL", "https://dev.edrak.com/")
        assert asyncio.run(load_webhook_settings(Config({}), "c1", "graph", LOGGER)) is None
        settings = asyncio.run(load_webhook_settings(Config({"scopedJwtSecret": "s"}), "c1", "graph", LOGGER))
        assert settings is not None
        assert settings.notification_url == "https://dev.edrak.com/api/webhooks/microsoft/graph/c1"
        assert settings.client_state == webhook_client_state("s", "c1")


class FakeSyncPoint:
    def __init__(self) -> None:
        self.points: dict[str, dict[str, Any]] = {}

    async def read_sync_point(self, key: str) -> dict[str, Any]:
        return dict(self.points.get(key, {}))

    async def update_sync_point(self, key: str, data: dict[str, Any], encrypt_fields: list[str] | None = None) -> None:
        self.points[key] = data


class FakeGraph:
    """In-memory ``/subscriptions``. ``fail`` maps a resource (create) or id (renew) to a status code."""

    def __init__(self, fail: dict[str, int] | None = None, existing: list[dict[str, Any]] | None = None) -> None:
        self.fail = fail or {}
        self.subscriptions: dict[str, dict[str, Any]] = {row["id"]: row for row in existing or []}
        self.calls: list[tuple[str, str]] = []
        self._counter = 0

    async def create(self, resource: GraphResource, notification_url: str, client_state: str, expiration: str) -> dict[str, Any]:
        self.calls.append(("create", resource.resource))
        if resource.resource in self.fail:
            raise GraphSubscriptionError(self.fail[resource.resource], "nope")
        self._counter += 1
        row = {
            "id": f"sub-{self._counter}",
            "resource": resource.resource,
            "changeType": resource.change_type,
            "notificationUrl": notification_url,
            "clientState": client_state,
            "expirationDateTime": expiration,
        }
        self.subscriptions[row["id"]] = row
        return dict(row)

    async def renew(self, subscription_id: str, expiration: str) -> dict[str, Any]:
        self.calls.append(("renew", subscription_id))
        if subscription_id in self.fail:
            raise GraphSubscriptionError(self.fail[subscription_id], "nope")
        if subscription_id not in self.subscriptions:
            raise GraphSubscriptionError(404, "gone")
        self.subscriptions[subscription_id]["expirationDateTime"] = expiration
        return dict(self.subscriptions[subscription_id])

    async def delete(self, subscription_id: str) -> None:
        self.calls.append(("delete", subscription_id))
        if subscription_id not in self.subscriptions:
            raise GraphSubscriptionError(404, "gone")
        del self.subscriptions[subscription_id]

    async def list(self) -> list[dict[str, Any]]:
        self.calls.append(("list", ""))
        return [dict(row) for row in self.subscriptions.values()]


def _manager(graph: FakeGraph, point: FakeSyncPoint, now: datetime = NOW) -> GraphSubscriptionManager:
    return GraphSubscriptionManager(
        graph, "conn-1", "https://dev.edrak.com/api/webhooks/microsoft/graph/conn-1", "state", LOGGER,
        store=SubscriptionStore(point), now=lambda: now,
    )


DRIVE = GraphResource("users/u1/drive/root", "updated", DRIVE_ITEM_MAX_MINUTES)
CHANNEL = GraphResource("teams/t1/channels/c1/messages", "created,updated,deleted", CHAT_MESSAGE_MAX_MINUTES)


class TestGraphSubscriptionManager:
    def test_ensure_creates_once_and_persists(self) -> None:
        graph, point = FakeGraph(), FakeSyncPoint()
        regs = asyncio.run(_manager(graph, point).ensure([DRIVE, CHANNEL]))
        assert [r.resource for r in regs] == [DRIVE.resource, CHANNEL.resource]
        created = graph.subscriptions["sub-1"]
        assert created["clientState"] == "state"
        assert created["notificationUrl"].endswith("/graph/conn-1")
        assert created["expirationDateTime"] == format_graph_datetime(expiration_for(DRIVE_ITEM_MAX_MINUTES, NOW))
        assert graph.subscriptions["sub-2"]["expirationDateTime"] == format_graph_datetime(expiration_for(CHAT_MESSAGE_MAX_MINUTES, NOW))
        stored = point.points["webhooks"]["webhooks"]
        assert [(r["id"], r["resource"], r["maxMinutes"]) for r in stored] == [
            ("sub-1", DRIVE.resource, DRIVE_ITEM_MAX_MINUTES), ("sub-2", CHANNEL.resource, CHAT_MESSAGE_MAX_MINUTES),
        ]

        # a second manager (new run) loads the state and creates nothing
        asyncio.run(_manager(graph, point).ensure([DRIVE, CHANNEL]))
        assert [c for c in graph.calls if c[0] == "create"] == [("create", DRIVE.resource), ("create", CHANNEL.resource)]

    def test_renew_expiring_only_touches_expiring_ones(self) -> None:
        graph, point = FakeGraph(), FakeSyncPoint()
        asyncio.run(_manager(graph, point).ensure([DRIVE, CHANNEL]))
        later = NOW + timedelta(minutes=30)
        asyncio.run(_manager(graph, point, now=later).renew_expiring(within_minutes=120))
        renews = [c for c in graph.calls if c[0] == "renew"]
        assert renews == [("renew", "sub-2")]  # the 60-minute channel subscription, not the 3-day drive one
        assert graph.subscriptions["sub-2"]["expirationDateTime"] == format_graph_datetime(expiration_for(CHAT_MESSAGE_MAX_MINUTES, later))
        assert point.points["webhooks"]["webhooks"][1]["expiresAt"] == graph.subscriptions["sub-2"]["expirationDateTime"]

    def test_renew_404_recreates(self) -> None:
        graph, point = FakeGraph(), FakeSyncPoint()
        asyncio.run(_manager(graph, point).ensure([CHANNEL]))
        del graph.subscriptions["sub-1"]  # Graph dropped it
        asyncio.run(_manager(graph, point, now=NOW + timedelta(hours=2)).renew_expiring(within_minutes=120))
        assert ("renew", "sub-1") in graph.calls
        assert graph.calls[-1] == ("create", CHANNEL.resource)
        assert [r["id"] for r in point.points["webhooks"]["webhooks"]] == ["sub-2"]

    def test_403_is_logged_once_and_skipped(self, caplog: pytest.LogCaptureFixture) -> None:
        graph, point = FakeGraph(fail={CHANNEL.resource: 403, "teams/t2/channels/c2/messages": 403}), FakeSyncPoint()
        other = GraphResource("teams/t2/channels/c2/messages", "created,updated,deleted", CHAT_MESSAGE_MAX_MINUTES)
        with caplog.at_level(logging.WARNING, logger=LOGGER.name):
            regs = asyncio.run(_manager(graph, point).ensure([DRIVE, CHANNEL, other]))
        assert [r.resource for r in regs] == [DRIVE.resource]
        assert sum("403" in rec.getMessage() for rec in caplog.records if rec.levelno == logging.WARNING) == 1

    def test_adopts_existing_subscriptions_when_state_is_lost(self) -> None:
        existing = [
            {"id": "old-1", "resource": DRIVE.resource, "changeType": "updated",
             "notificationUrl": "https://dev.edrak.com/api/webhooks/microsoft/graph/conn-1", "expirationDateTime": "2026-09-11T00:00:00Z"},
            {"id": "foreign", "resource": "users/x/drive/root", "changeType": "updated",
             "notificationUrl": "https://other.example/hook", "expirationDateTime": "2026-09-11T00:00:00Z"},
        ]
        graph, point = FakeGraph(existing=existing), FakeSyncPoint()
        regs = asyncio.run(_manager(graph, point).ensure([DRIVE]))
        assert [r.id for r in regs] == ["old-1"]
        assert not [c for c in graph.calls if c[0] == "create"]

    def test_remove_all_deletes_and_clears(self) -> None:
        graph, point = FakeGraph(), FakeSyncPoint()
        asyncio.run(_manager(graph, point).ensure([DRIVE, CHANNEL]))
        del graph.subscriptions["sub-2"]  # already gone on Graph's side: 404 is tolerated
        asyncio.run(_manager(graph, point).remove_all())
        assert graph.subscriptions == {}
        assert point.points["webhooks"]["webhooks"] == []

    def test_registration_roundtrip(self) -> None:
        reg = Registration("id", "res", "updated", "2026-09-11T00:00:00Z", 60)
        assert Registration.from_dict(reg.to_dict()) == reg
        assert Registration.from_dict({"resource": "no-id"}) is None


class TestSyncGraphSubscriptionsHook:
    def test_never_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FRONTEND_PUBLIC_URL", "https://dev.edrak.com")

        class Config:
            async def get_config(self, key: str) -> object:
                raise RuntimeError("kv down")

        async def token() -> str:
            return "t"

        asyncio.run(sync_graph_subscriptions(
            config_service=Config(), connector_id="c", token_getter=token, sync_point=FakeSyncPoint(),
            resources=[DRIVE], logger=LOGGER,
        ))
