"""Connector-level tests for the Business Central connector (no network).

Exercises ``MicrosoftBusinessCentralConnector`` request plumbing (bearer refresh
on 401, 429 + ``Retry-After`` back-off, ``@odata.nextLink`` paging), ``init()``
company scoping, the access model (company groups from Entra, org-wide default)
and the per-entity sync with its seen-set / key-set deletion reconcile — all
against fakes for httpx, the entities processor and the sync points.  Fixtures
are shaped like Business Central API v2.0 JSON.

``connector.py`` imports the fork runtime (httpx, fastapi, pydantic models,
config service ...); when that chain is not importable in the current
interpreter the module is skipped instead of failing, so
``test_business_central_mapping.py`` (stdlib only) still runs everywhere.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import httpx
import pytest

try:  # pragma: no cover - environment dependent
    from app.config.constants.arangodb import Connectors
    from app.connectors.core.base.connector.connector_service import ConnectorInitError
    from app.connectors.core.registry.filters import FilterCollection
    from app.connectors.sources.microsoft.business_central import (
        connector as bc_connector,
    )
    from app.connectors.sources.microsoft.business_central.connector import (
        MicrosoftBusinessCentralConnector,
        _entity_sync_point_key,
        grants_to_permissions,
    )
    from app.models.permission import EntityType, PermissionType
    IMPORT_ERROR: BaseException | None = None
except BaseException as e:  # ImportError, pydantic/env errors from the runtime chain
    IMPORT_ERROR = e

from app.connectors.sources.microsoft.business_central.mapping import (
    ENTITY_SPECS as SPECS,
)
from app.connectors.sources.microsoft.business_central.mapping import (
    FIELD_LAST_RECONCILE,
    FIELD_LAST_SYNC,
    MS_PER_HOUR,
    Company,
    CompanyAccess,
    GrantEntity,
    GrantRole,
    PermissionGrant,
    company_group_external_id,
    parse_company_access_mapping,
    record_external_id,
)

pytestmark = pytest.mark.skipif(IMPORT_ERROR is not None, reason=f"Business Central connector runtime not importable here: {IMPORT_ERROR!r}")

TENANT = "11111111-2222-3333-4444-555555555555"
BASE = f"https://api.businesscentral.dynamics.com/v2.0/{TENANT}/Production/api/v2.0/"
CRONUS = Company(id="aaaaaaaa-0000-0000-0000-000000000001", name="CRONUS SA", display_name="CRONUS SA")
ARABIC = Company(id="aaaaaaaa-0000-0000-0000-000000000002", name="شركة المثال", display_name="شركة المثال")
SO = SPECS["salesOrders"]
CUST = SPECS["customers"]
COMPANIES_PAYLOAD = {"value": [
    {"id": CRONUS.id, "name": "CRONUS SA", "displayName": "CRONUS SA"},
    {"id": ARABIC.id, "name": "شركة المثال", "displayName": "شركة المثال"},
]}

Handler = Callable[[str, str, dict[str, str] | None], httpx.Response]


def _response(status: int, body: object = None, headers: dict[str, str] | None = None, url: str = BASE) -> httpx.Response:
    return httpx.Response(status, json=body if body is not None else {}, headers=headers or {}, request=httpx.Request("GET", url))


class FakeHttp:
    """Just the ``httpx.AsyncClient`` surface the connector touches."""

    def __init__(self, handler: Handler, **_: object) -> None:
        self.handler = handler
        self.calls: list[tuple[str, str, dict[str, str] | None]] = []
        self.token_posts = 0
        self.closed = False

    async def get(self, url: str, params: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> httpx.Response:
        self.calls.append(("GET", url, dict(params) if params else None))
        return self.handler("GET", url, params)

    async def post(self, url: str, data: dict[str, str] | None = None) -> httpx.Response:
        self.token_posts += 1
        self.calls.append(("POST", url, None))
        return httpx.Response(200, json={"access_token": f"tok-{self.token_posts}", "expires_in": 3600}, request=httpx.Request("POST", url))

    async def aclose(self) -> None:
        self.closed = True


class FakeRecord:
    def __init__(self, record_id: str, external_id: str) -> None:
        self.id = record_id
        self.external_record_id = external_id


class FakeProcessor:
    def __init__(self, records: list[FakeRecord] | None = None) -> None:
        self.org_id = "org-1"
        self.records = sorted(records or [], key=lambda r: r.id)
        self.upserts: list[tuple[Any, list[Any]]] = []
        self.cascade_calls: list[list[str]] = []
        self.users: list[Any] = []
        self.groups: list[tuple[Any, list[Any]]] = []
        self.record_groups: list[tuple[Any, list[Any]]] = []
        self.page_calls: list[dict[str, Any]] = []

    async def on_new_records(self, batch: list[tuple[Any, list[Any]]]) -> None:
        self.upserts.extend(batch)

    async def on_new_app_users(self, users: list[Any]) -> None:
        self.users.extend(users)

    async def on_new_user_groups(self, groups: list[tuple[Any, list[Any]]]) -> None:
        self.groups.extend(groups)

    async def on_new_record_groups(self, groups: list[tuple[Any, list[Any]]]) -> None:
        self.record_groups.extend(groups)

    async def get_records_by_status(self, connector_id: str, status_filters: list[str], limit: int | None = None,
                                    offset: int = 0, record_group_id: str | None = None, is_placeholder: bool | None = None,
                                    after_key: str | None = None, exclude_statuses: list[str] | None = None) -> list[FakeRecord]:
        self.page_calls.append({"after_key": after_key, "limit": limit})
        rows = [r for r in self.records if after_key is None or r.id > after_key]
        return rows[:limit] if limit else rows

    async def on_records_deleted_cascade(self, record_ids: list[str], connector_id: str) -> dict[str, Any]:
        self.cascade_calls.append(list(record_ids))
        self.records = [r for r in self.records if r.id not in set(record_ids)]
        return {"success": True, "successfully_deleted": len(record_ids), "failed_records": []}


class FakeSyncPoint:
    def __init__(self, points: dict[str, dict[str, Any]] | None = None) -> None:
        self.points: dict[str, dict[str, Any]] = dict(points or {})

    async def read_sync_point(self, key: str) -> dict[str, Any]:
        return dict(self.points.get(key, {}))

    async def update_sync_point(self, key: str, data: dict[str, Any], **_: object) -> dict[str, Any]:
        self.points[key] = dict(data)
        return data


class FakeConfigService:
    def __init__(self, auth: dict[str, Any]) -> None:
        self.auth = auth

    async def get_config(self, path: str) -> dict[str, Any]:
        return {"auth": self.auth, "sync": {}}


def _connector(
    handler: Handler,
    processor: FakeProcessor | None = None,
    points: dict[str, dict[str, Any]] | None = None,
    companies: list[Company] | None = None,
    access_mapping: str | None = None,
) -> tuple[MicrosoftBusinessCentralConnector, FakeHttp, FakeProcessor]:
    c = MicrosoftBusinessCentralConnector.__new__(MicrosoftBusinessCentralConnector)
    processor = processor or FakeProcessor()
    http = FakeHttp(handler)
    c.logger = logging.getLogger("test-bc")
    c.connector_id = "conn-1"
    c.connector_name = Connectors.MICROSOFT_BUSINESS_CENTRAL
    c.data_entities_processor = processor
    c.records_sync_point = FakeSyncPoint(points)
    c.user_sync_point = FakeSyncPoint()
    c.tenant_id = TENANT
    c.environment_name = "Production"
    c._client_id = "client"
    c._client_secret = "secret"
    c._company_filter = []
    c._access_mapping = parse_company_access_mapping(access_mapping)
    c._http = http
    c._token = "tok-0"
    c._token_expires_on = 10**12
    c._request_semaphore = asyncio.Semaphore(4)
    c._entra = None
    c.sync_filters = FilterCollection()
    c.indexing_filters = FilterCollection()
    c._companies = list(companies or [CRONUS, ARABIC])
    # what ``_sync_access_model`` leaves behind for an empty mapping: every company resolved, readable by nobody
    c._access_by_company = {co.id: CompanyAccess(company=co) for co in c._companies}
    c._known_records_cache = None
    return c, http, processor


def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(bc_connector.asyncio, "sleep", fake_sleep)
    return waits


# ---------------------------------------------------------------------------
# Request plumbing
# ---------------------------------------------------------------------------


class TestHttpPlumbing:
    def test_429_honours_retry_after_then_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        waits = _no_sleep(monkeypatch)
        responses = iter([
            _response(429, {"error": {"code": "Application_TooManyRequests"}}, headers={"Retry-After": "3"}),
            _response(503, {}, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}),  # past date → minimum wait
            _response(200, {"value": [{"id": "1"}]}),
        ])
        c, http, _ = _connector(lambda *_: next(responses))
        payload = asyncio.run(c._get_json("companies", params={"$top": "1"}))
        assert payload == {"value": [{"id": "1"}]}
        assert waits == [3.0, 0.5]
        assert len(http.calls) == 3 and http.token_posts == 0

    def test_401_refreshes_token_once(self) -> None:
        responses = iter([_response(401, {"error": "expired"}), _response(200, {"value": []})])
        c, http, _ = _connector(lambda *_: next(responses))
        assert asyncio.run(c._get_json("companies")) == {"value": []}
        assert http.token_posts == 1
        assert c._token == "tok-1"

    def test_non_retryable_error_raises(self) -> None:
        c, _, _ = _connector(lambda *_: _response(403, {"error": {"code": "Forbidden"}}))
        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(c._get_json("companies"))

    def test_iter_pages_follows_next_link_verbatim(self) -> None:
        next_link = f"{BASE}companies({CRONUS.id})/customers?$top=1000&$skiptoken=XYZ"

        def handler(method: str, url: str, params: dict[str, str] | None) -> httpx.Response:
            if url == next_link:
                assert params is None  # the link already carries the query
                return _response(200, {"value": [{"id": "3"}]})
            assert params == {"$top": "1000"}
            return _response(200, {"value": [{"id": "1"}, {"id": "2"}], "@odata.nextLink": next_link})

        c, http, _ = _connector(handler)

        async def collect() -> list[list[str]]:
            return [[r["id"] for r in rows] async for rows in c._iter_pages(f"companies({CRONUS.id})/customers", {"$top": "1000"})]

        assert asyncio.run(collect()) == [["1", "2"], ["3"]]
        assert [u for _, u, _ in http.calls] == [f"companies({CRONUS.id})/customers", next_link]


# ---------------------------------------------------------------------------
# init(): credentials, company scoping
# ---------------------------------------------------------------------------


class TestInit:
    def _init(self, monkeypatch: pytest.MonkeyPatch, auth: dict[str, Any], companies_payload: dict[str, Any] = COMPANIES_PAYLOAD) -> tuple[MicrosoftBusinessCentralConnector, FakeHttp]:
        created: list[FakeHttp] = []

        def factory(**kwargs: object) -> FakeHttp:
            environment = auth.get("environmentName") or "Production"
            assert kwargs.get("base_url") == BASE.replace("/Production/", f"/{environment}/")
            http = FakeHttp(lambda *_: _response(200, companies_payload))
            created.append(http)
            return http

        monkeypatch.setattr(bc_connector.httpx, "AsyncClient", factory)
        c = MicrosoftBusinessCentralConnector.__new__(MicrosoftBusinessCentralConnector)
        c.logger = logging.getLogger("test-bc")
        c.connector_id = "conn-1"
        c.config_service = FakeConfigService(auth)
        c._http = None
        c._entra = None
        c._token = None
        c._token_expires_on = 0
        c._request_semaphore = asyncio.Semaphore(4)
        c._companies = []
        assert asyncio.run(c.init()) is True
        return c, created[0]

    def test_init_selects_configured_companies_and_defaults_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        c, http = self._init(monkeypatch, {
            "clientId": "client", "clientSecret": "secret", "tenantId": TENANT,
            "environmentName": "", "companies": "شركة  المثال", "companyAccessGroups": "",
        })
        assert c.environment_name == "Production"
        assert [co.id for co in c._companies] == [ARABIC.id]
        assert http.token_posts == 1 and c._token == "tok-1"
        assert http.calls[1][1] == "companies"

    def test_init_without_company_filter_takes_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        c, _ = self._init(monkeypatch, {"clientId": "client", "clientSecret": "secret", "tenantId": TENANT, "environmentName": "Sandbox"})
        assert c.environment_name == "Sandbox"
        assert len(c._companies) == 2 and c._access_mapping.is_empty()

    def test_init_rejects_incomplete_credentials(self) -> None:
        c = MicrosoftBusinessCentralConnector.__new__(MicrosoftBusinessCentralConnector)
        c.logger = logging.getLogger("test-bc")
        c.connector_id = "conn-1"
        c.config_service = FakeConfigService({"clientId": "client", "tenantId": TENANT})
        with pytest.raises(ConnectorInitError):
            asyncio.run(c.init())

    def test_init_rejects_malformed_access_mapping(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(ConnectorInitError, match="companyAccessGroups.*'broken line'"):
            self._init(monkeypatch, {"clientId": "c", "clientSecret": "s", "tenantId": TENANT, "companyAccessGroups": "broken line"})

    def test_init_fails_when_no_configured_company_matches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(ConnectorInitError, match="None of the configured companies"):
            self._init(monkeypatch, {"clientId": "c", "clientSecret": "s", "tenantId": TENANT, "companies": "Nope Ltd"})

    def test_init_fails_when_app_sees_no_companies(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with pytest.raises(ConnectorInitError, match="D365 AUTOMATION"):
            self._init(monkeypatch, {"clientId": "c", "clientSecret": "s", "tenantId": TENANT}, companies_payload={"value": []})


# ---------------------------------------------------------------------------
# Access model and record groups
# ---------------------------------------------------------------------------


class FakeEntra:
    instances: list[FakeEntra] = []

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, logger: logging.Logger) -> None:
        self.args = (tenant_id, client_id, client_secret)
        self.requested: list[str] = []
        FakeEntra.instances.append(self)

    async def resolve_many(self, names: list[str]) -> dict[str, list[str] | None]:
        self.requested = list(names)
        return {n: (["a@edrak.com", "b@edrak.com"] if n == "BC Readers" else None) for n in names}

    async def close(self) -> None:
        return None


class TestAccessModel:
    def test_company_groups_from_entra_and_unmapped_company_grants_nobody(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        FakeEntra.instances = []
        monkeypatch.setattr(bc_connector, "EntraGroupResolver", FakeEntra)
        c, _, processor = _connector(lambda *_: _response(200, {}), access_mapping="CRONUS SA = BC Readers, Missing Group")
        with caplog.at_level(logging.WARNING, logger="test-bc"):
            asyncio.run(c._sync_access_model())
        asyncio.run(c._sync_record_groups())

        assert FakeEntra.instances[0].args == (TENANT, "client", "secret")
        assert FakeEntra.instances[0].requested == ["BC Readers", "Missing Group"]
        groups = {g.source_user_group_id: (g, members) for g, members in processor.groups}
        cronus_group, cronus_members = groups[company_group_external_id(CRONUS.id)]
        assert cronus_group.name == "Business Central · CRONUS SA"
        assert sorted(u.email for u in cronus_members) == ["a@edrak.com", "b@edrak.com"]
        arabic_group, arabic_members = groups[company_group_external_id(ARABIC.id)]
        assert arabic_group.name == "Business Central · شركة المثال" and arabic_members == []
        assert sorted(u.email for u in processor.users) == ["a@edrak.com", "b@edrak.com"]

        record_groups = {rg.external_group_id: (rg, perms) for rg, perms in processor.record_groups}
        _, cronus_perms = record_groups[company_group_external_id(CRONUS.id)]
        assert [(p.entity_type, p.type, p.external_id) for p in cronus_perms] == [
            (EntityType.GROUP, PermissionType.READ, company_group_external_id(CRONUS.id)),
        ]
        # no entry and no '*' default: the (empty) company group only, never an ORG grant
        arabic_rg, arabic_perms = record_groups[company_group_external_id(ARABIC.id)]
        assert [(p.entity_type, p.external_id) for p in arabic_perms] == [(EntityType.GROUP, company_group_external_id(ARABIC.id))]
        assert not c._access_by_company[ARABIC.id].org_wide
        assert arabic_rg.group_type.value == "ERP_ENTITY"
        assert arabic_rg.web_url.endswith("?company=%D8%B4%D8%B1%D9%83%D8%A9%20%D8%A7%D9%84%D9%85%D8%AB%D8%A7%D9%84")
        assert c.user_sync_point.points["users"][FIELD_LAST_SYNC] > 0

        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("شركة المثال" in m and "no companyAccessGroups entry" in m and "nobody can read it" in m for m in warnings)
        assert any("'Missing Group'" in m and "CRONUS SA" in m and "grants nobody" in m for m in warnings)
        assert not any(r.levelno >= logging.ERROR for r in caplog.records)

    def test_star_value_grants_org_wide_read(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        FakeEntra.instances = []
        monkeypatch.setattr(bc_connector, "EntraGroupResolver", FakeEntra)
        c, _, processor = _connector(lambda *_: _response(200, {}), access_mapping="CRONUS SA = *\nشركة المثال = BC Readers")
        with caplog.at_level(logging.INFO, logger="test-bc"):
            asyncio.run(c._sync_access_model())
        asyncio.run(c._sync_record_groups())

        assert FakeEntra.instances[0].requested == ["BC Readers"]  # '*' is never looked up in Entra
        record_groups = {rg.external_group_id: perms for rg, perms in processor.record_groups}
        assert [(p.entity_type, p.type, p.external_id) for p in record_groups[company_group_external_id(CRONUS.id)]] == [
            (EntityType.GROUP, PermissionType.READ, company_group_external_id(CRONUS.id)),
            (EntityType.ORG, PermissionType.READ, None),
        ]
        assert [(p.entity_type, p.external_id) for p in record_groups[company_group_external_id(ARABIC.id)]] == [
            (EntityType.GROUP, company_group_external_id(ARABIC.id)),
        ]
        groups = {g.source_user_group_id: members for g, members in processor.groups}
        assert groups[company_group_external_id(CRONUS.id)] == []
        assert sorted(u.email for u in groups[company_group_external_id(ARABIC.id)]) == ["a@edrak.com", "b@edrak.com"]
        assert any("CRONUS SA" in r.getMessage() and "org-wide" in r.getMessage() for r in caplog.records if r.levelno == logging.INFO)
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_no_mapping_means_no_graph_call_and_nobody_reads(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        FakeEntra.instances = []
        monkeypatch.setattr(bc_connector, "EntraGroupResolver", FakeEntra)
        c, _, processor = _connector(lambda *_: _response(200, {}))
        c._access_by_company = {}
        with caplog.at_level(logging.WARNING, logger="test-bc"):
            asyncio.run(c._sync_access_model())
        assert FakeEntra.instances == []
        assert processor.users == [] and len(processor.groups) == 2
        assert set(c._access_by_company) == {CRONUS.id, ARABIC.id}
        assert not any(access.org_wide or access.group_refs for access in c._access_by_company.values())
        named = {co.label for co in (CRONUS, ARABIC) if any(co.label in r.getMessage() and "nobody can read it" in r.getMessage() for r in caplog.records)}
        assert named == {CRONUS.label, ARABIC.label}

    def test_entry_matching_no_synced_company_is_an_error(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        monkeypatch.setattr(bc_connector, "EntraGroupResolver", FakeEntra)
        c, _, _ = _connector(lambda *_: _response(200, {}), access_mapping="Nope Ltd = BC Readers")
        with caplog.at_level(logging.ERROR, logger="test-bc"):
            asyncio.run(c._sync_access_model())
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1 and "['Nope Ltd']" in errors[0] and "grant nobody" in errors[0]
        assert not any(access.org_wide or access.group_refs for access in c._access_by_company.values())

    def test_permissions_fail_closed_before_the_access_sync(self) -> None:
        c, _, _ = _connector(lambda *_: _response(200, {}))
        c._access_by_company = {}
        with pytest.raises(RuntimeError, match="CRONUS SA has no resolved access model"):
            c._company_permissions(CRONUS)
        with pytest.raises(RuntimeError):
            asyncio.run(c._sync_entity(CRONUS, SPECS["salesOrders"], incremental=False))

    def test_grants_to_permissions(self) -> None:
        perms = grants_to_permissions([
            PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id="bc:company:x"),
            PermissionGrant(GrantEntity.ORG, GrantRole.READER),
        ])
        assert [(p.entity_type, p.type, p.external_id) for p in perms] == [
            (EntityType.GROUP, PermissionType.READ, "bc:company:x"), (EntityType.ORG, PermissionType.READ, None),
        ]


# ---------------------------------------------------------------------------
# Entity sync + deletion reconcile
# ---------------------------------------------------------------------------


def _so_rows(*ids: str) -> list[dict[str, Any]]:
    return [
        {"id": i, "number": f"10{i}", "status": "Open", "customerName": "مؤسسة النور", "lastModifiedDateTime": "2026-03-02T10:15:30Z",
         "salesOrderLines": [{"id": f"{i}-l", "sequence": 10000, "lineType": "Item", "description": "كرسي", "quantity": 1}]}
        for i in ids
    ]


def _path(company: Company, entity_set: str) -> str:
    return f"companies({company.id})/{entity_set}"


class TestEntitySync:
    def test_full_read_upserts_with_company_permissions_and_prunes_missing(self) -> None:
        known = [
            FakeRecord("r1", record_external_id(CRONUS.id, SO, "1")),
            FakeRecord("r2", record_external_id(CRONUS.id, SO, "2")),          # gone in BC → deleted
            FakeRecord("r9", record_external_id(CRONUS.id, CUST, "9")),        # other entity set, untouched
            FakeRecord("rA", record_external_id(ARABIC.id, SO, "2")),          # other company, untouched
        ]

        def handler(method: str, url: str, params: dict[str, str] | None) -> httpx.Response:
            assert url == _path(CRONUS, "salesOrders")
            assert params == {"$top": "1000", "$expand": "salesOrderLines"}   # full read: no $filter
            return _response(200, {"value": _so_rows("1", "3")})

        c, http, processor = _connector(handler, FakeProcessor(known), access_mapping="* = BC Readers")
        c._access_by_company = {CRONUS.id: CompanyAccess(company=CRONUS, group_refs=("BC Readers",))}
        asyncio.run(c._sync_entity(CRONUS, SO, incremental=False))

        assert [r.external_record_id for r, _ in processor.upserts] == [
            record_external_id(CRONUS.id, SO, "1"), record_external_id(CRONUS.id, SO, "3"),
        ]
        record, perms = processor.upserts[0]
        assert record.record_name == "Sales order 101 · مؤسسة النور"
        assert record.external_record_group_id == company_group_external_id(CRONUS.id)
        assert record.record_group_type.value == "ERP_ENTITY" and record.mime_type == "text/markdown"
        assert record.source_updated_at == 1772446530000 and record.external_revision_id == "1772446530000"
        assert "company=CRONUS%20SA&page=42&filter=" in record.weburl
        assert [(p.entity_type, p.external_id) for p in perms] == [(EntityType.GROUP, company_group_external_id(CRONUS.id))]
        assert processor.cascade_calls == [["r2"]]
        assert {r.id for r in processor.records} == {"r1", "r9", "rA"}
        assert len(http.calls) == 1  # seen-set reconcile needs no key pull
        state = c.records_sync_point.points[_entity_sync_point_key(CRONUS.id, SO)]
        assert state[FIELD_LAST_SYNC] > 0 and state[FIELD_LAST_RECONCILE] == state[FIELD_LAST_SYNC]

    def test_incremental_filters_by_last_sync_and_skips_reconcile_when_not_due(self) -> None:
        now = bc_connector.get_epoch_timestamp_in_ms()
        since = 1770091506000
        points = {_entity_sync_point_key(CRONUS.id, SO): {FIELD_LAST_SYNC: since, FIELD_LAST_RECONCILE: now - MS_PER_HOUR}}

        def handler(method: str, url: str, params: dict[str, str] | None) -> httpx.Response:
            assert params == {"$top": "1000", "$expand": "salesOrderLines", "$filter": "lastModifiedDateTime gt 2026-02-03T04:05:06Z"}
            return _response(200, {"value": _so_rows("5")})

        known = [FakeRecord("old", record_external_id(CRONUS.id, SO, "old"))]
        c, http, processor = _connector(handler, FakeProcessor(known), points=points)
        asyncio.run(c._sync_entity(CRONUS, SO, incremental=True))
        assert [r.external_record_id for r, _ in processor.upserts] == [record_external_id(CRONUS.id, SO, "5")]
        assert processor.cascade_calls == [] and len(http.calls) == 1
        state = c.records_sync_point.points[_entity_sync_point_key(CRONUS.id, SO)]
        assert state[FIELD_LAST_SYNC] >= now and state[FIELD_LAST_RECONCILE] == now - MS_PER_HOUR

    def test_incremental_key_set_reconcile_when_due(self) -> None:
        now = bc_connector.get_epoch_timestamp_in_ms()
        points = {_entity_sync_point_key(CRONUS.id, SO): {FIELD_LAST_SYNC: now - MS_PER_HOUR, FIELD_LAST_RECONCILE: now - 25 * MS_PER_HOUR}}

        def handler(method: str, url: str, params: dict[str, str] | None) -> httpx.Response:
            assert url == _path(CRONUS, "salesOrders")
            if params and params.get("$select") == "id":
                assert params == {"$select": "id", "$top": "5000"}
                return _response(200, {"value": [{"id": "1"}, {"id": "3"}]})
            return _response(200, {"value": []})  # nothing modified since the last sync

        known = [
            FakeRecord("r1", record_external_id(CRONUS.id, SO, "1")),
            FakeRecord("r2", record_external_id(CRONUS.id, SO, "2")),
            FakeRecord("r3", record_external_id(CRONUS.id, SO, "3")),
        ]
        c, http, processor = _connector(handler, FakeProcessor(known), points=points)
        asyncio.run(c._sync_entity(CRONUS, SO, incremental=True))
        assert processor.upserts == []
        assert processor.cascade_calls == [["r2"]]
        assert [p for _, _, p in http.calls] == [
            {"$top": "1000", "$expand": "salesOrderLines", "$filter": f"lastModifiedDateTime gt {bc_connector.build_modified_filter(since_ms=now - MS_PER_HOUR).split(' gt ')[1]}"},
            {"$select": "id", "$top": "5000"},
        ]
        assert c.records_sync_point.points[_entity_sync_point_key(CRONUS.id, SO)][FIELD_LAST_RECONCILE] >= now

    def test_empty_full_read_never_wipes_known_records(self) -> None:
        known = [FakeRecord("r1", record_external_id(CRONUS.id, SO, "1"))]
        c, _, processor = _connector(lambda *_: _response(200, {"value": []}), FakeProcessor(known))
        asyncio.run(c._sync_entity(CRONUS, SO, incremental=False))
        assert processor.cascade_calls == [] and {r.id for r in processor.records} == {"r1"}
        # the reconcile ran (and was skipped by the guard), so the interval restarts
        assert c.records_sync_point.points[_entity_sync_point_key(CRONUS.id, SO)][FIELD_LAST_RECONCILE] is not None

    def test_reconcile_disabled_with_zero_interval(self) -> None:
        known = [FakeRecord("r2", record_external_id(CRONUS.id, SO, "2"))]
        c, http, processor = _connector(lambda *_: _response(200, {"value": _so_rows("1")}), FakeProcessor(known))
        c._reconcile_interval_hours = lambda: 0.0  # type: ignore[method-assign]
        asyncio.run(c._sync_entity(CRONUS, SO, incremental=False))
        assert processor.cascade_calls == [] and len(http.calls) == 1
        assert c.records_sync_point.points[_entity_sync_point_key(CRONUS.id, SO)][FIELD_LAST_RECONCILE] is None

    def test_failed_reconcile_does_not_fail_the_upsert_sync(self) -> None:
        def handler(method: str, url: str, params: dict[str, str] | None) -> httpx.Response:
            if params and params.get("$select") == "id":
                return _response(500, {"error": "boom"})
            return _response(200, {"value": _so_rows("1")})

        points = {_entity_sync_point_key(CRONUS.id, SO): {FIELD_LAST_SYNC: 1, FIELD_LAST_RECONCILE: None}}
        c, _, processor = _connector(handler, FakeProcessor([]), points=points)
        asyncio.run(c._sync_entity(CRONUS, SO, incremental=True))
        assert len(processor.upserts) == 1
        state = c.records_sync_point.points[_entity_sync_point_key(CRONUS.id, SO)]
        assert state[FIELD_LAST_SYNC] > 1 and state[FIELD_LAST_RECONCILE] is None

    def test_items_become_product_records(self) -> None:
        row = {"id": "i1", "number": "1000", "displayName": "كرسي مكتب", "itemCategoryCode": "FURNITURE", "gtin": "0123", "unitPrice": 500.25, "blocked": False}
        c, _, _ = _connector(lambda *_: _response(200, {}))
        record = c._build_record(CRONUS, SPECS["items"], row)
        assert record is not None and record.record_type.value == "PRODUCT"
        assert (record.product_code, record.product_family, record.sku, record.list_price, record.is_active) == ("1000", "FURNITURE", "0123", 500.25, True)
        assert c._build_record(CRONUS, SPECS["items"], {"number": "no-id"}) is None


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------


class TestStreaming:
    def test_stream_record_renders_current_row(self) -> None:
        def handler(method: str, url: str, params: dict[str, str] | None) -> httpx.Response:
            assert url == f"companies({CRONUS.id})/salesOrders(1)" and params == {"$expand": "salesOrderLines"}
            return _response(200, _so_rows("1")[0])

        c, _, _ = _connector(handler)
        record = c._build_record(CRONUS, SO, _so_rows("1")[0])
        assert record is not None
        response = asyncio.run(c.stream_record(record))

        async def body() -> bytes:
            return b"".join([chunk async for chunk in response.body_iterator])  # type: ignore[union-attr]

        text = asyncio.run(body()).decode("utf-8")
        assert text.startswith("# Sales order 101 · مؤسسة النور\n")
        assert "| 10000 | Item |  | كرسي | 1 |" in text
