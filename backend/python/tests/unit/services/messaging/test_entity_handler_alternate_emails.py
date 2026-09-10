"""Mongo -> graph mirror of `alternateEmails` through the userAdded / userUpdated entity events."""

import importlib
import sys
import types
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames


def _load_entity_module() -> ModuleType:
    try:
        return importlib.import_module("app.services.messaging.kafka.handlers.entity")
    except TypeError:
        # Some local venvs cannot import ConnectorFactory (kiota metaclass conflict); the handler
        # under test never touches it, so a stub is enough to load the module.
        stub = types.ModuleType("app.connectors.core.factory.connector_factory")
        stub.ConnectorFactory = MagicMock()
        sys.modules[stub.__name__] = stub
        return importlib.import_module("app.services.messaging.kafka.handlers.entity")


entity = _load_entity_module()

PRIMARY = "sujit@edrak.com"
ALTERNATE = "sujit@demo.edrak.com"


def _service(graph_provider: AsyncMock) -> "entity.EntityEventService":
    svc = entity.EntityEventService(logger=MagicMock(), graph_provider=graph_provider, app_container=MagicMock())
    svc._get_or_create_knowledge_base = AsyncMock(return_value={})
    svc._get_or_create_all_team_and_add_user = AsyncMock()
    return svc


def _graph_provider(existing_by_email: dict[str, object] | None = None) -> AsyncMock:
    existing_by_email = existing_by_email or {}

    async def get_user_by_email(email: str, *args, **kwargs) -> object | None:
        return existing_by_email.get(email.lower())

    gp = AsyncMock()
    gp.get_user_by_email = AsyncMock(side_effect=get_user_by_email)
    gp.get_document = AsyncMock(return_value={"_key": "org-1", "accountType": "enterprise"})
    gp.batch_upsert_nodes = AsyncMock()
    gp.batch_create_edges = AsyncMock()
    return gp


def _upserted_user(gp: AsyncMock) -> dict:
    call = gp.batch_upsert_nodes.await_args
    assert call.args[1] == CollectionNames.USERS.value
    return call.args[0][0]


class TestNormalize:
    def test_none_when_field_absent(self) -> None:
        assert entity._normalize_alternate_emails({"email": PRIMARY}) is None

    def test_lowercases_trims_and_drops_blanks(self) -> None:
        assert entity._normalize_alternate_emails({"alternateEmails": [" Sujit@Demo.edrak.com ", "", None]}) == [ALTERNATE]
        assert entity._normalize_alternate_emails({"alternateEmails": None}) == []


class TestUserAdded:
    async def test_new_user_persists_alternate_emails(self) -> None:
        gp = _graph_provider()
        svc = _service(gp)

        ok = await svc.process_event(
            "userAdded",
            {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY, "alternateEmails": ["Sujit@Demo.edrak.com"]},
        )

        assert ok is True
        user_data = _upserted_user(gp)
        assert user_data["email"] == PRIMARY
        assert user_data["alternateEmails"] == [ALTERNATE]

    async def test_new_user_without_field_gets_empty_list(self) -> None:
        gp = _graph_provider()
        svc = _service(gp)

        assert await svc.process_event("userAdded", {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY}) is True
        assert _upserted_user(gp)["alternateEmails"] == []

    async def test_adopts_connector_created_node_found_by_alternate(self) -> None:
        """A connector already created an inactive node for the alternate; the platform user takes it over."""
        existing = MagicMock()
        existing.id = "connector-node"
        existing.email = ALTERNATE
        existing.source_emails = ["sujit@edrak.onmicrosoft.com"]
        gp = _graph_provider({ALTERNATE: existing})
        svc = _service(gp)

        ok = await svc.process_event(
            "userAdded",
            {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY, "alternateEmails": [ALTERNATE]},
        )

        assert ok is True
        looked_up = [c.args[0] for c in gp.get_user_by_email.await_args_list]
        assert looked_up == [PRIMARY, ALTERNATE]
        # the alias lookup stays inside the event's org: the same alias may exist in another tenant
        assert gp.get_user_by_email.await_args_list[1].kwargs == {"org_id": "org-1"}
        user_data = _upserted_user(gp)
        assert user_data["id"] == "connector-node"
        assert user_data["userId"] == "mongo-1"
        assert user_data["email"] == PRIMARY
        assert user_data["alternateEmails"] == [ALTERNATE]
        # the address the connector created the node under stays resolvable (sourceEmails), joined with what it learned
        assert user_data["sourceEmails"] == ["sujit@edrak.onmicrosoft.com", ALTERNATE]
        assert user_data["isActive"] is True

    async def test_existing_user_without_field_keeps_graph_value(self) -> None:
        existing = MagicMock()
        existing.id = "existing"
        existing.email = PRIMARY
        gp = _graph_provider({PRIMARY: existing})
        svc = _service(gp)

        assert await svc.process_event("userAdded", {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY}) is True
        assert "alternateEmails" not in _upserted_user(gp)
        assert "sourceEmails" not in _upserted_user(gp)  # same primary address: nothing to carry over


class TestUserUpdated:
    @pytest.fixture
    def gp(self) -> AsyncMock:
        gp = AsyncMock()
        gp.get_user_by_user_id = AsyncMock(return_value={"id": "existing", "userId": "mongo-1"})
        gp.batch_upsert_nodes = AsyncMock()
        return gp

    async def test_replaces_alternate_emails(self, gp: AsyncMock) -> None:
        svc = _service(gp)

        ok = await svc.process_event(
            "userUpdated",
            {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY, "alternateEmails": [" SUJIT@demo.edrak.com "]},
        )

        assert ok is True
        assert _upserted_user(gp)["alternateEmails"] == [ALTERNATE]

    async def test_empty_list_clears(self, gp: AsyncMock) -> None:
        svc = _service(gp)
        await svc.process_event("userUpdated", {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY, "alternateEmails": []})
        assert _upserted_user(gp)["alternateEmails"] == []

    async def test_absent_field_leaves_graph_untouched(self, gp: AsyncMock) -> None:
        svc = _service(gp)
        await svc.process_event("userUpdated", {"orgId": "org-1", "userId": "mongo-1", "email": PRIMARY})
        assert "alternateEmails" not in _upserted_user(gp)
