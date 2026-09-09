"""Connector-level tests for the Dynamics 365 connector's user identity mapping (no network).

Checks that ``_sync_security_model`` prefers the Entra primary e-mail returned by
``EntraUserEmailResolver`` over the address stored on the Dataverse ``systemuser`` row,
falls back to the Dataverse address when Graph knows nothing, and skips the resolver
entirely when the connector has no credentials.  Everything else (paging, business
units, teams, roles, sync points) is faked.

``connector.py`` imports the fork runtime (httpx, azure-identity, fastapi, pydantic
models, config service ...); when that chain is not importable here the module is
skipped rather than failing, like ``test_business_central_connector.py``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

try:  # pragma: no cover - environment dependent
    from app.config.constants.arangodb import Connectors
    from app.connectors.sources.microsoft.dynamics365 import connector as dyn_connector
    from app.connectors.sources.microsoft.dynamics365.connector import (
        MicrosoftDynamics365Connector,
    )
    IMPORT_ERROR: BaseException | None = None
except BaseException as e:  # ImportError, pydantic/env errors from the runtime chain
    IMPORT_ERROR = e

pytestmark = pytest.mark.skipif(IMPORT_ERROR is not None, reason=f"Dynamics 365 connector runtime not importable here: {IMPORT_ERROR!r}")


SYSTEMUSERS: list[dict[str, Any]] = [
    # UPN-style Dataverse address, Graph knows the vanity-domain primary address
    {"systemuserid": "u-1", "fullname": "Sujit", "internalemailaddress": "sujit@edrak.onmicrosoft.com",
     "domainname": "sujit@edrak.onmicrosoft.com", "azureactivedirectoryobjectid": "aad-1", "isdisabled": False},
    # Graph has no record for this object id → Dataverse address is kept
    {"systemuserid": "u-2", "fullname": "Nour", "internalemailaddress": "nour@edrak.com",
     "azureactivedirectoryobjectid": "aad-2", "isdisabled": False},
    # No Entra object id at all (e.g. stub user) → Dataverse address
    {"systemuserid": "u-3", "fullname": "Stub", "internalemailaddress": "stub@edrak.com", "isdisabled": True},
    # The connector's own S2S application user is never an AppUser
    {"systemuserid": "u-app", "fullname": "# CGraph", "applicationid": "app-1", "azureactivedirectoryobjectid": "aad-app"},
    # No address anywhere → dropped
    {"systemuserid": "u-4", "fullname": "Nobody", "azureactivedirectoryobjectid": "aad-4"},
]


class FakeResolver:
    instances: list[FakeResolver] = []

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, logger: logging.Logger, http: object = None) -> None:
        self.args = (tenant_id, client_id, client_secret)
        self.requested: list[list[str]] = []
        self.closed = False
        FakeResolver.instances.append(self)

    async def resolve_emails(self, object_ids: list[str]) -> dict[str, str]:
        self.requested.append(list(object_ids))
        return {"aad-1": "sujit@edrak.com", "aad-app": "app@edrak.com"}

    async def close(self) -> None:
        self.closed = True


class FakeProcessor:
    org_id = "org-1"

    def __init__(self) -> None:
        self.users: list[Any] = []
        self.groups: list[Any] = []
        self.roles: list[Any] = []

    async def on_new_app_users(self, users: list[Any]) -> None:
        self.users.extend(users)

    async def on_new_user_groups(self, groups: list[Any]) -> None:
        self.groups.extend(groups)

    async def on_new_app_roles(self, roles: list[Any]) -> None:
        self.roles.extend(roles)


class FakeSyncPoint:
    def __init__(self) -> None:
        self.points: dict[str, dict[str, Any]] = {}

    async def update_sync_point(self, key: str, data: dict[str, Any]) -> None:
        self.points[key] = data


def _connector(*, with_credentials: bool = True) -> tuple[MicrosoftDynamics365Connector, FakeProcessor]:
    c = MicrosoftDynamics365Connector.__new__(MicrosoftDynamics365Connector)
    processor = FakeProcessor()
    c.logger = logging.getLogger("test-dyn")
    c.connector_id = "conn-1"
    c.connector_name = Connectors.MICROSOFT_DYNAMICS_365
    c.data_entities_processor = processor
    c.user_sync_point = FakeSyncPoint()
    c.records_sync_point = FakeSyncPoint()
    c._tenant_id, c._client_id, c._client_secret = ("tenant-1", "client", "secret") if with_credentials else ("", "", "")
    c._entra_users = None
    c._http = None
    c.credential = None
    c._token = None
    c._token_expires_on = 0

    async def iter_pages(entity_set: str, params: dict[str, str] | None = None, **_: object) -> AsyncIterator[list[dict[str, Any]]]:
        assert entity_set == "systemusers"
        assert params and "azureactivedirectoryobjectid" in params["$select"]
        yield SYSTEMUSERS[:3]
        yield SYSTEMUSERS[3:]

    async def fetch_all(path: str, params: dict[str, str] | None = None, **_: object) -> list[dict[str, Any]]:
        return []

    c._iter_pages = iter_pages  # type: ignore[method-assign]
    c._fetch_all = fetch_all  # type: ignore[method-assign]
    return c, processor


class TestUserIdentity:
    def test_prefers_entra_primary_address_and_falls_back_to_dataverse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        FakeResolver.instances = []
        monkeypatch.setattr(dyn_connector, "EntraUserEmailResolver", FakeResolver)
        c, processor = _connector()
        ctx = asyncio.run(c._sync_security_model([]))

        assert {u.source_user_id: u.email for u in processor.users} == {
            "u-1": "sujit@edrak.com",       # remapped from the onmicrosoft UPN
            "u-2": "nour@edrak.com",        # unknown to Graph → Dataverse address
            "u-3": "stub@edrak.com",        # no Entra id → Dataverse address
        }
        assert ctx.user_email_by_id == {"u-1": "sujit@edrak.com", "u-2": "nour@edrak.com", "u-3": "stub@edrak.com"}
        assert {u.source_user_id: u.is_active for u in processor.users}["u-3"] is False

        resolver = FakeResolver.instances[0]
        assert resolver.args == ("tenant-1", "client", "secret")
        # one lookup per page, application users excluded before the lookup, resolver reused across pages
        assert resolver.requested == [["aad-1", "aad-2"], ["aad-4"]]
        assert len(FakeResolver.instances) == 1
        assert c.user_sync_point.points["users"]["lastSyncTimestamp"] > 0

    def test_without_credentials_uses_dataverse_addresses_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        FakeResolver.instances = []
        monkeypatch.setattr(dyn_connector, "EntraUserEmailResolver", FakeResolver)
        c, processor = _connector(with_credentials=False)
        asyncio.run(c._sync_security_model([]))
        assert FakeResolver.instances == []
        assert {u.source_user_id: u.email for u in processor.users}["u-1"] == "sujit@edrak.onmicrosoft.com"

    def test_cleanup_closes_resolver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        FakeResolver.instances = []
        monkeypatch.setattr(dyn_connector, "EntraUserEmailResolver", FakeResolver)
        c, _ = _connector()
        asyncio.run(c._sync_security_model([]))
        asyncio.run(c.cleanup())
        assert FakeResolver.instances[0].closed is True
        assert c._entra_users is None
