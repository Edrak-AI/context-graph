"""Connector-level tests for the Dynamics 365 connector (no network).

Checks that ``_sync_security_model`` prefers the Entra primary e-mail returned by
``EntraUserEmailResolver`` over the address stored on the Dataverse ``systemuser`` row,
falls back to the Dataverse address when Graph knows nothing, and skips the resolver
entirely when the connector has no credentials, and that ``_sync_entity`` re-applies
grants to records whose ``principalobjectaccess`` shares changed without the record
itself appearing in the delta.  Everything else (paging, business units, teams, roles,
sync points) is faked.

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
    import httpx

    from app.config.constants.arangodb import Connectors
    from app.connectors.core.registry.filters import FilterCollection
    from app.connectors.sources.microsoft.dynamics365 import connector as dyn_connector
    from app.connectors.sources.microsoft.dynamics365.change_tracking import (
        FIELD_CHANGE_TRACKING,
        FIELD_DELTA_LINK,
        FIELD_SHARE_DIGESTS,
        EntitySyncState,
    )
    from app.connectors.sources.microsoft.dynamics365.connector import (
        MicrosoftDynamics365Connector,
    )
    from app.connectors.sources.microsoft.dynamics365.mapping import (
        ACCESS_READ,
        ENTITY_SPECS,
        PRINCIPAL_TYPE_SYSTEMUSER,
        PRINCIPAL_TYPE_TEAM,
        SecurityContext,
        ShareEntry,
        share_digest,
    )
    from app.models.permission import EntityType, PermissionType
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
        self.records: list[Any] = []

    async def on_new_app_users(self, users: list[Any]) -> None:
        self.users.extend(users)

    async def on_new_user_groups(self, groups: list[Any]) -> None:
        self.groups.extend(groups)

    async def on_new_app_roles(self, roles: list[Any]) -> None:
        self.roles.extend(roles)

    async def on_new_records(self, records: list[Any]) -> None:
        self.records.extend(records)


class FakeSyncPoint:
    def __init__(self) -> None:
        self.points: dict[str, dict[str, Any]] = {}

    async def read_sync_point(self, key: str) -> dict[str, Any]:
        return dict(self.points.get(key) or {})

    async def update_sync_point(self, key: str, data: dict[str, Any], encrypt_fields: list[str] | None = None) -> None:
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
        # the disabled user is still upserted (inactive) but resolves no owner / share edge
        assert {u.source_user_id: u.is_active for u in processor.users}["u-3"] is False
        assert ctx.user_email_by_id == {"u-1": "sujit@edrak.com", "u-2": "nour@edrak.com"}

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


# ---------------------------------------------------------------------------
# Share propagation on incremental syncs
# ---------------------------------------------------------------------------

OPP = ENTITY_SPECS["opportunity"]
OPP_A = "0a3f1e30-aaaa-4a2b-9c3d-000000000001"  # unchanged, newly shared → re-fetched
OPP_B = "0a3f1e30-bbbb-4a2b-9c3d-000000000002"  # in the delta and newly shared → not fetched twice
OPP_C = "0a3f1e30-cccc-4a2b-9c3d-000000000003"  # share unchanged → untouched
ALICE = "11111111-1111-1111-1111-111111111111"
BOB = "22222222-2222-2222-2222-222222222222"
TEAM = "33333333-3333-3333-3333-333333333333"
BU = "44444444-4444-4444-4444-444444444444"
DELTA_1 = "https://contoso.crm4.dynamics.com/api/data/v9.2/opportunities?$deltatoken=1"
DELTA_2 = "https://contoso.crm4.dynamics.com/api/data/v9.2/opportunities?$deltatoken=2"
ENTITY_KEY = "entity/opportunity"


def _opp(guid: str, name: str) -> dict[str, Any]:
    return {
        "opportunityid": guid, "name": name, "statecode": 0, "statuscode": 1,
        "_ownerid_value": ALICE, "_owninguser_value": ALICE, "_owningbusinessunit_value": BU,
        "createdon": "2026-01-01T00:00:00Z", "modifiedon": "2026-02-01T00:00:00Z",
    }


def _poa(object_id: str, principal_id: str, type_code: int) -> dict[str, Any]:
    return {"objectid": object_id, "principalid": principal_id, "principaltypecode": type_code, "accessrightsmask": ACCESS_READ}


class FakeDataverse:
    """``_get_json`` / ``_fetch_all`` stand-in: one delta page and key-filtered row lookups."""

    def __init__(self, delta_rows: list[dict[str, Any]], rows_by_id: dict[str, dict[str, Any]], poa_rows: list[dict[str, Any]] | None) -> None:
        self.delta_rows, self.rows_by_id, self.poa_rows = delta_rows, rows_by_id, poa_rows
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    async def get_json(self, path_or_url: str, params: dict[str, str] | None = None, **_: object) -> dict[str, Any]:
        self.calls.append((path_or_url, params))
        if path_or_url == DELTA_1:
            return {"value": self.delta_rows, "@odata.deltaLink": DELTA_2}
        assert path_or_url == "opportunities" and params is not None
        assert params["$select"] == ",".join(OPP.select_fields)
        wanted = [clause.split(" eq ")[1] for clause in params["$filter"].split(" or ")]
        assert all(clause.startswith("opportunityid eq ") for clause in params["$filter"].split(" or "))
        return {"value": [self.rows_by_id[g] for g in wanted if g in self.rows_by_id]}

    async def fetch_all(self, path: str, params: dict[str, str] | None = None, **_: object) -> list[dict[str, Any]]:
        assert path == "principalobjectaccessset" and params and params["$filter"] == "objecttypecode eq 'opportunity'"
        if self.poa_rows is None:
            request = httpx.Request("GET", "https://contoso.crm4.dynamics.com/api/data/v9.2/principalobjectaccessset")
            raise httpx.HTTPStatusError("forbidden", request=request, response=httpx.Response(403, request=request))
        return self.poa_rows

    def refetches(self) -> list[dict[str, str] | None]:
        return [params for path, params in self.calls if path == "opportunities"]


def _entity_connector(dataverse: FakeDataverse, baseline: dict[str, str]) -> tuple[MicrosoftDynamics365Connector, FakeProcessor]:
    c, processor = _connector()
    del c._iter_pages  # the security-model fake; the real pager drives ``_get_json`` here
    c._get_json = dataverse.get_json  # type: ignore[method-assign]
    c._fetch_all = dataverse.fetch_all  # type: ignore[method-assign]
    c.environment_url = "https://contoso.crm4.dynamics.com"
    c.sync_filters = FilterCollection()
    c._shares_unavailable_logged = False
    c._security = SecurityContext(user_email_by_id={ALICE: "alice@contoso.com", BOB: "bob@contoso.com"}, known_team_ids={TEAM})
    state = EntitySyncState(last_sync_timestamp=1, delta_link=DELTA_1, share_digests=baseline)
    state.mark_change_tracking_enabled(DELTA_1)
    c.records_sync_point.points[ENTITY_KEY] = state.to_sync_point()
    return c, processor


def _grants(processor: FakeProcessor, guid: str) -> set[tuple[Any, Any, str | None, str | None]]:
    out = set()
    for record, permissions in processor.records:
        if record.external_record_id == f"opportunity:{guid}":
            out.update((p.entity_type, p.type, p.external_id, p.email) for p in permissions)
    return out


class TestSharePropagation:
    def test_share_added_to_unchanged_record_is_refetched_once(self) -> None:
        team_share = [ShareEntry(TEAM, PRINCIPAL_TYPE_TEAM, ACCESS_READ)]
        dataverse = FakeDataverse(
            delta_rows=[_opp(OPP_B, "B changed")],
            rows_by_id={OPP_A: _opp(OPP_A, "A"), OPP_B: _opp(OPP_B, "B changed"), OPP_C: _opp(OPP_C, "C")},
            poa_rows=[_poa(OPP_A, BOB, PRINCIPAL_TYPE_SYSTEMUSER), _poa(OPP_B, BOB, PRINCIPAL_TYPE_SYSTEMUSER), _poa(OPP_C, TEAM, PRINCIPAL_TYPE_TEAM)],
        )
        c, processor = _entity_connector(dataverse, baseline={OPP_C: share_digest(team_share)})

        asyncio.run(c._sync_entity(OPP, incremental=True))

        # exactly one key-filtered request, for A alone: B came through the delta, C's share did not move
        assert dataverse.refetches() == [{"$select": ",".join(OPP.select_fields), "$filter": f"opportunityid eq {OPP_A}"}]
        assert [r.external_record_id for r, _ in processor.records] == [f"opportunity:{OPP_B}", f"opportunity:{OPP_A}"]
        assert (EntityType.USER, PermissionType.READ, None, "bob@contoso.com") in _grants(processor, OPP_A)
        assert (EntityType.USER, PermissionType.READ, None, "bob@contoso.com") in _grants(processor, OPP_B)

        saved = EntitySyncState.from_sync_point(c.records_sync_point.points[ENTITY_KEY])
        assert saved.delta_link == DELTA_2
        assert set(saved.share_digests) == {OPP_A, OPP_B, OPP_C}
        assert saved.share_digests[OPP_A] == share_digest([ShareEntry(BOB, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ)])
        assert isinstance(c.records_sync_point.points[ENTITY_KEY][FIELD_SHARE_DIGESTS], str)

    def test_unshared_record_is_reprocessed_and_deleted_one_is_skipped(self) -> None:
        bob_share = [ShareEntry(BOB, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ)]
        dataverse = FakeDataverse(
            delta_rows=[],
            rows_by_id={OPP_A: _opp(OPP_A, "A")},  # C no longer exists in Dataverse
            poa_rows=[],
        )
        c, processor = _entity_connector(dataverse, baseline={OPP_A: share_digest(bob_share), OPP_C: share_digest(bob_share)})

        asyncio.run(c._sync_entity(OPP, incremental=True))

        assert len(dataverse.refetches()) == 1
        assert dataverse.refetches()[0]["$filter"] == f"opportunityid eq {OPP_A} or opportunityid eq {OPP_C}"
        assert [r.external_record_id for r, _ in processor.records] == [f"opportunity:{OPP_A}"]
        assert not any(email == "bob@contoso.com" for _, _, _, email in _grants(processor, OPP_A))
        assert EntitySyncState.from_sync_point(c.records_sync_point.points[ENTITY_KEY]).share_digests == {}

    def test_nothing_changed_means_no_refetch_but_baseline_is_kept(self) -> None:
        bob_share = [ShareEntry(BOB, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ)]
        dataverse = FakeDataverse(delta_rows=[], rows_by_id={}, poa_rows=[_poa(OPP_A, BOB, PRINCIPAL_TYPE_SYSTEMUSER)])
        c, processor = _entity_connector(dataverse, baseline={OPP_A: share_digest(bob_share)})
        asyncio.run(c._sync_entity(OPP, incremental=True))
        assert dataverse.refetches() == [] and processor.records == []
        assert EntitySyncState.from_sync_point(c.records_sync_point.points[ENTITY_KEY]).share_digests == {OPP_A: share_digest(bob_share)}

    def test_unreadable_poa_table_leaves_baseline_untouched(self) -> None:
        bob_share = [ShareEntry(BOB, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ)]
        dataverse = FakeDataverse(delta_rows=[], rows_by_id={OPP_A: _opp(OPP_A, "A")}, poa_rows=None)
        c, processor = _entity_connector(dataverse, baseline={OPP_A: share_digest(bob_share)})
        asyncio.run(c._sync_entity(OPP, incremental=True))
        # a failed POA read is not "everything got unshared": no re-fetch, digest baseline preserved
        assert dataverse.refetches() == [] and processor.records == []
        saved = c.records_sync_point.points[ENTITY_KEY]
        assert EntitySyncState.from_sync_point(saved).share_digests == {OPP_A: share_digest(bob_share)}
        assert saved[FIELD_DELTA_LINK] == DELTA_2 and saved[FIELD_CHANGE_TRACKING] == "enabled"
