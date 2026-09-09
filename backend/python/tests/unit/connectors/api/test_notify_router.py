"""``POST /api/v1/connectors/internal/{id}/notify`` — auth, lookup, coalescing, event shape."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from jose import jwt

from app.connectors.api import notify_router as notify_router_module
from app.connectors.services import notify_service
from app.connectors.services.notify_service import (
    NotifyScheduler,
    build_resync_event,
    remove_change_notifications_best_effort,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import httpx

SECRET = "scoped-secret"
CONNECTOR_ID = "11111111-2222-3333-4444-555555555555"


def _token(scopes: list[str] | None = None, secret: str = SECRET) -> str:
    now = int(time.time())
    return jwt.encode(
        {"scopes": scopes if scopes is not None else ["connector:notify"], "issuer": "edrak-ai", "iat": now, "exp": now + 300},
        secret,
        algorithm="HS256",
    )


class FakeKv:
    """SET NX EX emulation: a key can be claimed once until it is dropped."""

    def __init__(self) -> None:
        self.keys: dict[str, Any] = {}

    async def create_key(self, key: str, value: object, overwrite: bool = True, ttl: int | None = None) -> bool:
        if not overwrite and key in self.keys:
            return False
        self.keys[key] = value
        return True


class Harness:
    def __init__(self, document: dict[str, Any] | None, *, delay_s: float = 5.0) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []
        self.kv = FakeKv()
        self.scheduler = NotifyScheduler(delay_s=delay_s, window_s=25, poll_s=0.01, max_wait_s=1.0)

        async def publish(topic: str, event: dict[str, Any]) -> bool:
            self.published.append((topic, event))
            return True

        config_service = MagicMock()

        async def get_config(key: str, *args: object, **kwargs: object) -> dict[str, str]:
            return {"scopedJwtSecret": SECRET, "jwtSecret": "user-secret"}

        config_service.get_config = get_config
        kafka_service = MagicMock()
        kafka_service.publish_event = publish
        logger = MagicMock()

        container = MagicMock()
        container.logger = MagicMock(return_value=logger)
        container.config_service = MagicMock(return_value=config_service)
        container.kafka_service = MagicMock(return_value=kafka_service)
        container.key_value_store = MagicMock(return_value=self.kv)

        graph_provider = MagicMock()

        async def get_document(key: str, collection: str) -> dict[str, Any] | None:
            assert collection == "apps"
            return document if key == CONNECTOR_ID else None

        graph_provider.get_document = get_document

        self.app = FastAPI()
        self.app.include_router(notify_router_module.router)
        self.app.container = container  # type: ignore[attr-defined]
        self.app.state.graph_provider = graph_provider


ACTIVE_DOC = {"_key": CONNECTOR_ID, "type": "Microsoft Teams", "orgId": "org-1", "isActive": True, "isAuthenticated": True}


@pytest.fixture
def run_route(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Harness]:
    def _run(document: dict[str, Any] | None, *, delay_s: float = 5.0) -> Harness:
        harness = Harness(document, delay_s=delay_s)
        monkeypatch.setattr(notify_router_module, "notify_scheduler", harness.scheduler)
        return harness

    return _run


def _post(client: TestClient, token: str | None, body: object = None, connector_id: str = CONNECTOR_ID) -> httpx.Response:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    payload = body if body is not None else {"source": "graph", "events": [{"resource": "teams/t/channels/c/messages", "changeType": "created"}]}
    if isinstance(payload, (bytes, str)):
        return client.post(f"/api/v1/connectors/internal/{connector_id}/notify", headers={**headers, "content-type": "application/json"}, content=payload)
    return client.post(f"/api/v1/connectors/internal/{connector_id}/notify", headers=headers, json=payload)


class TestNotifyRoute:
    def test_valid_token_schedules_once_per_window(self, run_route: Callable[..., Harness]) -> None:
        h = run_route(ACTIVE_DOC)
        with TestClient(h.app) as client:
            first = _post(client, _token())
            second = _post(client, _token(), body={"source": "dataverse", "events": [{"entity": "account", "recordId": "r1"}]})
            assert (first.status_code, first.json()) == (202, {"accepted": True, "scheduled": True})
            assert (second.status_code, second.json()) == (202, {"accepted": True, "scheduled": False})
            assert h.scheduler.is_pending(CONNECTOR_ID)
            assert list(h.kv.keys) == [f"cgraph:notify:{CONNECTOR_ID}"]
            client.portal.call(h.scheduler.cancel_all)
        assert h.published == []  # cancelled before the delay elapsed

    def test_pending_run_publishes_incremental_resync_event(self, run_route: Callable[..., Harness]) -> None:
        h = run_route(ACTIVE_DOC, delay_s=0.01)
        with TestClient(h.app) as client:
            assert _post(client, _token()).json()["scheduled"] is True

            async def wait_published() -> None:
                for _ in range(200):
                    if h.published:
                        return
                    await asyncio.sleep(0.01)
                raise AssertionError("event not published")

            client.portal.call(wait_published)
        topic, event = h.published[0]
        assert topic == "sync-events"
        assert event["eventType"] == "microsoftteams.resync"
        assert event["payload"]["connectorId"] == CONNECTOR_ID
        assert event["payload"]["orgId"] == "org-1"
        assert event["payload"]["incremental"] is True
        assert event["payload"]["source"] == "graph"

    def test_bad_token_is_401(self, run_route: Callable[..., Harness]) -> None:
        h = run_route(ACTIVE_DOC)
        with TestClient(h.app) as client:
            assert _post(client, None).status_code == 401
            assert _post(client, _token(secret="wrong")).status_code == 401
            assert _post(client, _token(scopes=["record:content"])).status_code == 401
            # a user JWT signed with the regular secret must not be accepted either
            assert _post(client, _token(secret="user-secret")).status_code == 401
        assert h.published == [] and not h.kv.keys

    def test_inactive_or_unknown_connector_is_404(self, run_route: Callable[..., Harness]) -> None:
        h = run_route({**ACTIVE_DOC, "isActive": False})
        with TestClient(h.app) as client:
            assert _post(client, _token()).status_code == 404
            assert _post(client, _token(), connector_id="other").status_code == 404
        assert not h.kv.keys

    def test_bad_body_is_400(self, run_route: Callable[..., Harness]) -> None:
        h = run_route(ACTIVE_DOC)
        with TestClient(h.app) as client:
            assert _post(client, _token(), body={"source": "slack", "events": []}).status_code == 400
            assert _post(client, _token(), body=b"not json").status_code == 400
        assert not h.kv.keys


class TestScheduler:
    def test_waits_for_running_sync_before_publishing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        running = {"value": True}
        monkeypatch.setattr(notify_service.sync_task_manager, "is_running", lambda cid: running["value"])
        published: list[dict[str, Any]] = []

        async def publish(topic: str, event: dict[str, Any]) -> None:
            published.append(event)

        async def scenario() -> None:
            scheduler = NotifyScheduler(delay_s=0.0, window_s=25, poll_s=0.01, max_wait_s=5.0)
            assert await scheduler.request(
                connector_id="c", org_id="o", connector_type="OneDrive", source="graph",
                kv_store=None, publish=publish, logger=MagicMock(),
            )
            await asyncio.sleep(0.05)
            assert published == []  # still blocked by the running sync
            running["value"] = False
            for _ in range(100):
                if published:
                    break
                await asyncio.sleep(0.01)
            assert published[0]["eventType"] == "onedrive.resync"
            # in-process window without a KV store
            assert not await scheduler.request(
                connector_id="c", org_id="o", connector_type="OneDrive", source="graph",
                kv_store=None, publish=publish, logger=MagicMock(),
            )

        asyncio.run(scenario())

    def test_build_resync_event_shape(self) -> None:
        event = build_resync_event(org_id="o", connector_type="SharePoint Online", connector_id="c", source="graph")
        assert event["eventType"] == "sharepointonline.resync"
        assert event["payload"]["connector"] == "SharePoint Online"
        assert event["payload"]["incremental"] is True

    def test_remove_change_notifications_best_effort(self) -> None:
        calls: list[str] = []

        class Connector:
            async def remove_change_notifications(self) -> None:
                calls.append("removed")
                raise RuntimeError("boom")

        container = SimpleNamespace(connectors_map={"c": Connector()})
        asyncio.run(remove_change_notifications_best_effort(container, "c", MagicMock()))
        asyncio.run(remove_change_notifications_best_effort(container, "missing", MagicMock()))
        assert calls == ["removed"]
