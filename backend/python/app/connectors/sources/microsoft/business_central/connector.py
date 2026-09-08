"""Microsoft Dynamics 365 Business Central connector (API v2.0, app-only auth).

Indexes ERP master data and documents (customers, vendors, items, sales orders,
sales invoices, purchase orders, purchase invoices — document lines expanded into
the record body) of every Business Central company the Entra application may
see, one record group per company, with company-level read permissions.  The
mapping itself lives in ``mapping.py`` (pure, unit-tested); this module owns
auth, HTTP, paging and the ``BaseConnector`` lifecycle.

Auth: Entra ID client credentials (``POST login.microsoftonline.com/<tenant>/oauth2/v2.0/token``,
scope ``https://api.businesscentral.dynamics.com/.default``) done with ``httpx`` —
no user redirect, so ``AuthType.OAUTH_ADMIN_CONSENT`` is reused like Dynamics 365
and SAP.  Business Central ignores Entra application permissions: the app
registration must additionally be registered **inside Business Central** (page
9800 *Microsoft Entra Applications*: client id, state *Enabled*, permission sets
``D365 READ`` and ``D365 AUTOMATION`` — the latter is what exposes ``companies``)
and *Grant consent* clicked once.  ``init()`` validates with ``GET companies``.

Permissions: one AppUserGroup per company (``bc:company:<id>``); members come
from the Entra groups listed in the ``companyAccessGroups`` auth field (resolved
with Microsoft Graph through the SAP connector's ``EntraGroupResolver``, which
needs ``GroupMember.Read.All`` + ``User.Read.All`` application permissions on the
same app).  A company without an entry is readable by the whole organisation
(``ORG`` READER) — the admin acknowledges that in the edrak-ai policy dialog.
The full model is written up in ``mapping.py``.

Deletes: the API exposes no delta feed, so every ``reconcile_interval_hours``
(default 24 h) the connector pulls ``$select=id`` for each company × entity set and
removes records Business Central no longer returns through
``on_records_deleted_cascade``.  A full sync reconciles for free.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

import httpx

from app.config.constants.arangodb import Connectors, MimeTypes, OriginTypes
from app.config.constants.http_status_code import HttpStatusCode
from app.connectors.core.base.connector.connector_service import (
    BaseConnector,
    ConnectorInitError,
)
from app.connectors.core.base.sync_point.sync_point import SyncDataPointType, SyncPoint
from app.connectors.core.constants import CONNECTOR_EMAIL_IDENTITY_INFO, IconPaths
from app.connectors.core.registry.auth_builder import AuthBuilder, AuthType
from app.connectors.core.registry.connector_builder import (
    AuthField,
    CommonFields,
    ConnectorBuilder,
    ConnectorScope,
    DocumentationLink,
    SyncStrategy,
)
from app.connectors.core.registry.filters import (
    FilterCategory,
    FilterCollection,
    FilterField,
    FilterOption,
    FilterOptionsResponse,
    FilterType,
    OptionSourceType,
    SyncFilterKey,
    load_connector_filters,
)
from app.connectors.sources.microsoft.business_central.mapping import (
    DEFAULT_ENTITY_ORDER,
    DEFAULT_RECONCILE_INTERVAL_HOURS,
    ENTITIES_FILTER_KEY,
    ENTITY_SPECS,
    MODIFIED_FIELD,
    RECONCILE_INTERVAL_FILTER_KEY,
    TOKEN_SCOPE,
    Company,
    CompanyAccess,
    CompanyAccessMapping,
    EntitySpec,
    EntitySyncState,
    GrantEntity,
    PermissionGrant,
    ReconcileMode,
    api_base_url,
    build_key_page_params,
    build_modified_filter,
    build_page_params,
    build_single_params,
    company_grants,
    company_group_external_id,
    company_group_name,
    company_path,
    company_web_url,
    diff_known_against_live,
    entity_path,
    normalize_environment_name,
    normalize_tenant_id,
    parse_bc_timestamp,
    parse_companies,
    parse_company,
    parse_company_access_mapping,
    parse_company_names,
    parse_page,
    parse_reconcile_interval_hours,
    plan_reconcile,
    record_external_id,
    record_id_prefix,
    record_title,
    record_web_url,
    render_record_markdown,
    resolve_company_access,
    resolve_selected_entities,
    retry_delay,
    seen_external_ids,
    select_companies,
    split_external_id,
    token_url,
)
from app.connectors.sources.microsoft.common.apps import MicrosoftBusinessCentralApp
from app.connectors.sources.sap.connector import EntraGroupResolver
from app.models.entities import (
    AppUser,
    AppUserGroup,
    ProductRecord,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.utils.streaming import create_stream_record_response
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from logging import Logger

    from fastapi.responses import StreamingResponse

    from app.config.configuration_service import ConfigurationService
    from app.connectors.core.base.data_processor.data_source_entities_processor import (
        DataSourceEntitiesProcessor,
    )
    from app.connectors.core.base.data_store.data_store import DataStoreProvider

CONNECTOR_KEY = "microsoftbusinesscentral"  # ConnectorFactory registry key / filters config name
USERS_SYNC_POINT_KEY = "users"
GROUPS_SYNC_POINT_KEY = "groups"
COMPANIES_SYNC_POINT_KEY = "companies"
ENTITY_SYNC_POINT_PREFIX = "entity"

_MAX_HTTP_RETRIES = 5
_RETRY_STATUS = {HttpStatusCode.TOO_MANY_REQUESTS.value, 502, HttpStatusCode.SERVICE_UNAVAILABLE.value, 504}
_TOKEN_REFRESH_SKEW_S = 120
_MAX_CONCURRENT_REQUESTS = 4
_KNOWN_RECORDS_PAGE = 1000
_DELETE_BATCH_SIZE = 100

_GRANT_ENTITY_TO_PERMISSION = {
    GrantEntity.GROUP: EntityType.GROUP,
    GrantEntity.ORG: EntityType.ORG,
}


def _entity_sync_point_key(company_id: str, spec: EntitySpec) -> str:
    return f"{ENTITY_SYNC_POINT_PREFIX}/{company_id}/{spec.entity_set}"


def grants_to_permissions(grants: list[PermissionGrant]) -> list[Permission]:
    """Convert connector-agnostic grants (mapping.py) into graph ``Permission`` objects."""
    return [
        Permission(
            entity_type=_GRANT_ENTITY_TO_PERMISSION[grant.entity_type],
            type=PermissionType.READ,
            external_id=grant.external_id,
        )
        for grant in grants
    ]


@ConnectorBuilder("Microsoft Business Central")\
    .in_group("Microsoft 365")\
    .with_description(
        "Sync customers, vendors, items, sales orders, sales invoices, purchase orders and "
        "purchase invoices from Microsoft Dynamics 365 Business Central with company-level permissions"
    )\
    .with_categories(["ERP", "Finance", "Sales"])\
    .with_scopes([ConnectorScope.TEAM.value])\
    .with_auth([
        AuthBuilder.type(AuthType.OAUTH_ADMIN_CONSENT).fields([
            AuthField(
                name="clientId",
                display_name="Application (Client) ID",
                placeholder="Enter your Entra ID Application ID",
                description="The Application (Client) ID from the Entra ID app registration",
            ),
            AuthField(
                name="clientSecret",
                display_name="Client Secret",
                placeholder="Enter your Entra ID client secret",
                description="A client secret of the Entra ID app registration",
                field_type="PASSWORD",
                is_secret=True,
            ),
            AuthField(
                name="tenantId",
                display_name="Directory (Tenant) ID",
                placeholder="Enter your Entra ID tenant ID",
                description="The Directory (Tenant) ID of the Entra ID tenant that hosts Business Central",
            ),
            AuthField(
                name="environmentName",
                display_name="Business Central environment",
                placeholder="Production",
                description=(
                    "Name of the Business Central environment (admin center → Environments), "
                    "e.g. Production or Sandbox. Defaults to Production."
                ),
                required=False,
                default_value="Production",
                min_length=0,
                max_length=200,
            ),
            AuthField(
                name="companies",
                display_name="Companies (optional)",
                placeholder="CRONUS SA, شركة المثال",
                description=(
                    "Comma-separated company names to sync. Leave empty to sync every company the "
                    "application can see in this environment."
                ),
                required=False,
                min_length=0,
                max_length=4000,
            ),
            AuthField(
                name="companyAccessGroups",
                display_name="Company access groups (optional)",
                placeholder="CRONUS SA = BC Finance Readers, 3f2b0c9e-...",
                description=(
                    "One line per company: 'Company = Entra group[, group...]' (display name or object id). "
                    "Members of the listed groups can find that company's records. '* = group' applies to "
                    "the remaining companies. Companies without an entry are searchable by everyone in the "
                    "organisation. Resolving groups needs GroupMember.Read.All and User.Read.All "
                    "application permissions on this app."
                ),
                field_type="TEXTAREA",
                required=False,
                min_length=0,
                max_length=20000,
            ),
            AuthField(
                name="hasAdminConsent",
                display_name="Entra application registered in Business Central",
                description=(
                    "Confirm the app registration has been added in Business Central (page 'Microsoft Entra "
                    "Applications', state Enabled, permission sets D365 READ and D365 AUTOMATION) and consent "
                    "was granted"
                ),
                field_type="CHECKBOX",
                required=True,
                default_value=False,
            ),
        ])
    ])\
    .with_info(CONNECTOR_EMAIL_IDENTITY_INFO)\
    .configure(lambda builder: builder
        .with_icon(IconPaths.connector_icon(Connectors.MICROSOFT_BUSINESS_CENTRAL.value))
        .add_documentation_link(DocumentationLink(
            "Register an Entra app for Business Central APIs (service-to-service)",
            "https://learn.microsoft.com/dynamics365/business-central/dev-itpro/administration/automation-apis-using-s2s-authentication",
            "setup",
        ))
        .add_documentation_link(DocumentationLink(
            "Business Central API v2.0 reference",
            "https://learn.microsoft.com/dynamics365/business-central/dev-itpro/api-reference/v2.0/",
            "setup",
        ))
        .add_filter_field(FilterField(
            name=ENTITIES_FILTER_KEY,
            display_name="Entities",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Business Central entity sets to sync. Leave empty to sync all supported entities.",
            options=list(DEFAULT_ENTITY_ORDER),
            option_source_type=OptionSourceType.STATIC,
            default_value=list(DEFAULT_ENTITY_ORDER),
        ))
        .add_filter_field(CommonFields.modified_date_filter(
            "Only sync rows whose lastModifiedDateTime falls in this range."
        ))
        .add_filter_field(FilterField(
            name=RECONCILE_INTERVAL_FILTER_KEY,
            display_name="Delete reconcile interval (hours)",
            filter_type=FilterType.NUMBER,
            category=FilterCategory.SYNC,
            description=(
                "Business Central exposes no deleted-row feed. Every N hours the connector re-reads the ids "
                "of each entity set and removes records that no longer exist. Default 24; 0 disables the "
                "reconcile (deletes are then never detected)."
            ),
            default_value=DEFAULT_RECONCILE_INTERVAL_HOURS,
        ))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 60)
        .with_sync_support(True)
        .with_agent_support(False)
    )\
    .build_decorator()
class MicrosoftBusinessCentralConnector(BaseConnector):
    """Business Central API v2.0 connector. See module docstring and ``mapping.py``."""

    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str = "",
        created_by: str = "",
    ) -> None:
        super().__init__(
            MicrosoftBusinessCentralApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_name = Connectors.MICROSOFT_BUSINESS_CENTRAL

        def _sync_point(kind: SyncDataPointType) -> SyncPoint:
            return SyncPoint(
                connector_id=self.connector_id,
                org_id=self.data_entities_processor.org_id,
                sync_data_point_type=kind,
                data_store_provider=self.data_store_provider,
            )

        self.user_sync_point = _sync_point(SyncDataPointType.USERS)
        self.records_sync_point = _sync_point(SyncDataPointType.RECORDS)

        self.tenant_id: str = ""
        self.environment_name: str = ""
        self._client_id: str = ""
        self._client_secret: str = ""
        self._company_filter: list[str] = []
        self._access_mapping = CompanyAccessMapping()

        self._http: httpx.AsyncClient | None = None
        self._token: str | None = None
        self._token_expires_on: int = 0
        self._request_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REQUESTS)
        self._entra: EntraGroupResolver | None = None

        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self._companies: list[Company] = []
        self._access_by_company: dict[str, CompanyAccess] = {}
        self._known_records_cache: dict[str, dict[str, str]] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> bool:
        config = await self.config_service.get_config(f"/services/connectors/{self.connector_id}/config")
        if not config:
            self.logger.error("Microsoft Business Central config not found")
            return False
        auth = config.get("auth", {}) or {}
        client_id = auth.get("clientId")
        client_secret = auth.get("clientSecret")
        tenant_id = auth.get("tenantId")
        if not all((tenant_id, client_id, client_secret)):
            raise ConnectorInitError(
                "Incomplete Microsoft Business Central credentials. tenantId, clientId and clientSecret are all required."
            )
        try:
            self.tenant_id = normalize_tenant_id(tenant_id)
        except ValueError as e:
            raise ConnectorInitError(str(e)) from e
        self.environment_name = normalize_environment_name(auth.get("environmentName"))
        self._client_id = str(client_id)
        self._client_secret = str(client_secret)
        self._company_filter = parse_company_names(auth.get("companies"))
        self._access_mapping = parse_company_access_mapping(auth.get("companyAccessGroups"))
        for warning in self._access_mapping.warnings:
            self.logger.warning("Business Central companyAccessGroups: %s", warning)

        await self._close_http()
        self._http = httpx.AsyncClient(
            base_url=api_base_url(self.tenant_id, self.environment_name),
            timeout=httpx.Timeout(120.0, connect=15.0),
            headers={"Accept": "application/json"},
        )
        try:
            await self._refresh_token()
            companies = await self._list_companies()
        except ConnectorInitError:
            await self._close_http()
            raise
        except Exception as e:
            await self._close_http()
            raise ConnectorInitError(
                "Could not connect to Business Central. Check tenantId/clientId/clientSecret and the environment "
                "name, and that the app registration is registered in Business Central (page 'Microsoft Entra "
                f"Applications') with the D365 READ and D365 AUTOMATION permission sets. ({type(e).__name__}: {str(e)[:200]})"
            ) from e
        if not companies:
            await self._close_http()
            raise ConnectorInitError(
                "Business Central returned no companies for this application. Grant the Entra application the "
                "D365 AUTOMATION permission set in Business Central (page 'Microsoft Entra Applications')."
            )
        selection = select_companies(companies, self._company_filter)
        if self._company_filter and not selection.selected:
            await self._close_http()
            raise ConnectorInitError(
                f"None of the configured companies ({', '.join(self._company_filter)}) exist in environment "
                f"{self.environment_name}. Available: {', '.join(c.label for c in companies)}"
            )
        self._companies = list(selection.selected)
        self.logger.info(
            "Business Central connector initialised for environment %s (%d of %d companies selected)",
            self.environment_name, len(self._companies), len(companies),
        )
        return True

    async def test_connection_and_access(self) -> bool:
        try:
            payload = await self._get_json("companies", params={"$top": "1"})
            return bool(payload.get("value"))
        except Exception as e:
            self.logger.error("Business Central connection test failed: %s", e)
            return False

    async def cleanup(self) -> None:
        await self._close_http()

    async def _close_http(self) -> None:
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._http.aclose()
            self._http = None
        if self._entra is not None:
            with contextlib.suppress(Exception):
                await self._entra.close()
            self._entra = None
        self._token = None
        self._token_expires_on = 0

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str,
        created_by: str,
        data_entities_processor: DataSourceEntitiesProcessor,
        **kwargs: object,
    ) -> BaseConnector:
        return cls(
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )

    # ------------------------------------------------------------------
    # HTTP / auth plumbing
    # ------------------------------------------------------------------

    async def _refresh_token(self) -> str:
        """Client-credentials grant; 429/503 on the token endpoint honour ``Retry-After`` too."""
        if self._http is None:
            raise RuntimeError("Business Central connector not initialised")
        delay = 1.0
        for attempt in range(_MAX_HTTP_RETRIES + 1):
            response = await self._http.post(
                token_url(self.tenant_id),
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": TOKEN_SCOPE,
                },
            )
            if response.status_code in _RETRY_STATUS and attempt < _MAX_HTTP_RETRIES:
                wait = retry_delay(response.headers.get("Retry-After"), delay)
                self.logger.warning("Entra token endpoint returned %s, retrying in %.1fs", response.status_code, wait)
                await asyncio.sleep(wait)
                delay = min(delay * 2, 30.0)
                continue
            response.raise_for_status()
            payload = response.json()
            self._token = str(payload["access_token"])
            self._token_expires_on = get_epoch_timestamp_in_ms() // 1000 + int(payload.get("expires_in") or 3600)
            return self._token
        raise RuntimeError("Entra token request exhausted retries")

    async def _get_token(self) -> str:
        now_s = get_epoch_timestamp_in_ms() // 1000
        if self._token and now_s < self._token_expires_on - _TOKEN_REFRESH_SKEW_S:
            return self._token
        return await self._refresh_token()

    async def _get_json(self, path_or_url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """GET with bearer auth, 401 re-auth and bounded retry on 429/5xx (honours Retry-After).

        ``path_or_url`` is a path relative to the API base or an absolute ``@odata.nextLink``
        (which already carries its own query string)."""
        if self._http is None:
            raise RuntimeError("Business Central connector not initialised")
        headers: dict[str, str] = {}
        refreshed = False
        delay = 1.0
        for attempt in range(_MAX_HTTP_RETRIES + 1):
            headers["Authorization"] = f"Bearer {await self._get_token()}"
            async with self._request_semaphore:
                try:
                    response = await self._http.get(path_or_url, params=params, headers=headers)
                except (httpx.TimeoutException, httpx.TransportError) as e:
                    if attempt >= _MAX_HTTP_RETRIES:
                        raise
                    self.logger.warning("Business Central request failed (%s), retrying in %.1fs", e, delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    continue
            if response.status_code == HttpStatusCode.UNAUTHORIZED.value and not refreshed:
                refreshed = True
                await self._refresh_token()
                continue
            if response.status_code in _RETRY_STATUS and attempt < _MAX_HTTP_RETRIES:
                wait = retry_delay(response.headers.get("Retry-After"), delay)
                self.logger.warning(
                    "Business Central returned %s for %s, retrying in %.1fs", response.status_code, path_or_url, wait
                )
                await asyncio.sleep(wait)
                delay = min(delay * 2, 30.0)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Business Central request to {path_or_url} exhausted retries")

    async def _iter_pages(
        self, path: str, params: dict[str, str] | None = None
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Follow ``@odata.nextLink`` (an absolute URL that already carries the query)."""
        page = parse_page(await self._get_json(path, params=params))
        while True:
            yield page.rows
            if not page.next_link:
                return
            page = parse_page(await self._get_json(page.next_link))

    async def _list_companies(self) -> list[Company]:
        companies: list[Company] = []
        async for rows in self._iter_pages("companies"):
            companies.extend(parse_companies({"value": rows}))
        return companies

    async def _company_by_id(self, company_id: str) -> Company | None:
        for company in self._companies:
            if company.id == company_id:
                return company
        try:
            return parse_company(await self._get_json(f"companies({company_id})"))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                return None
            raise

    async def _fetch_row(self, company_id: str, spec: EntitySpec, row_id: str) -> dict[str, Any] | None:
        try:
            return await self._get_json(entity_path(company_id, spec, row_id), params=build_single_params(spec))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                return None
            raise

    # ------------------------------------------------------------------
    # Sync entry points
    # ------------------------------------------------------------------

    async def run_sync(self) -> None:
        """Full sync: companies → access groups → every selected entity set of every company (all rows)."""
        await self._run(incremental=False)

    async def run_incremental_sync(self) -> None:
        """Incremental: ``lastModifiedDateTime gt <last sync>`` per company × entity set, plus the
        periodic key-set reconcile for deletes.  See ``mapping.plan_reconcile``."""
        await self._run(incremental=True)

    async def _run(self, *, incremental: bool) -> None:
        if self._http is None:
            self.logger.error("Business Central connector not initialised")
            return
        self.logger.info("Starting Business Central %s sync", "incremental" if incremental else "full")
        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, CONNECTOR_KEY, self.connector_id, self.logger
        )
        specs = resolve_selected_entities(self._selected_entity_names())

        selection = select_companies(await self._list_companies(), self._company_filter)
        for name in selection.unmatched:
            self.logger.warning("Business Central: configured company %r matched no company in %s", name, self.environment_name)
        self._companies = list(selection.selected)
        await self.records_sync_point.update_sync_point(COMPANIES_SYNC_POINT_KEY, {"lastSyncTimestamp": get_epoch_timestamp_in_ms()})

        self._known_records_cache = None
        await self._sync_access_model()
        await self._sync_record_groups()
        for company in self._companies:
            for spec in specs:
                await self._sync_entity(company, spec, incremental=incremental)
        self.logger.info("Business Central sync completed")

    def _selected_entity_names(self) -> list[str] | None:
        entity_filter = self.sync_filters.get(ENTITIES_FILTER_KEY) if self.sync_filters else None
        if entity_filter is None or entity_filter.is_empty():
            return None
        return [str(v) for v in entity_filter.as_list()]

    def _modified_bounds(self) -> tuple[int | None, int | None]:
        modified = self.sync_filters.get(SyncFilterKey.MODIFIED) if self.sync_filters else None
        if modified is None or modified.is_empty():
            return None, None
        return modified.get_datetime_start(), modified.get_datetime_end()

    def _reconcile_interval_hours(self) -> float:
        interval = self.sync_filters.get(RECONCILE_INTERVAL_FILTER_KEY) if self.sync_filters else None
        return parse_reconcile_interval_hours(interval.get_value() if interval is not None else None)

    # ------------------------------------------------------------------
    # Company access model → AppUser / AppUserGroup
    # ------------------------------------------------------------------

    def _app_user(self, email: str) -> AppUser:
        return AppUser(
            app_name=self.connector_name,
            connector_id=self.connector_id,
            source_user_id=email,
            email=email,
            full_name=email,
            org_id=self.data_entities_processor.org_id,
            is_active=True,
        )

    async def _sync_access_model(self) -> None:
        accesses = resolve_company_access(self._companies, self._access_mapping)
        self._access_by_company = {a.company.id: a for a in accesses}

        resolved: dict[str, list[str] | None] = {}
        refs = self._access_mapping.referenced_groups()
        if refs:
            if self._entra is None:
                self._entra = EntraGroupResolver(self.tenant_id, self._client_id, self._client_secret, self.logger)
            resolved = await self._entra.resolve_many(refs)

        groups: list[tuple[AppUserGroup, list[AppUser]]] = []
        all_emails: set[str] = set()
        for access in accesses:
            members: set[str] = set()
            for ref in access.group_refs:
                emails = resolved.get(ref)
                if emails is None:
                    self.logger.warning(
                        "Business Central: Entra group %r (company %s) was not found; it grants nobody",
                        ref, access.company.label,
                    )
                    continue
                members.update(emails)
            if access.org_wide:
                self.logger.info("Business Central company %s has no access groups; readable org-wide", access.company.label)
            elif not members:
                self.logger.warning(
                    "Business Central company %s: the configured access groups resolved to no members; nobody can read it",
                    access.company.label,
                )
            all_emails.update(members)
            groups.append((
                AppUserGroup(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_user_group_id=company_group_external_id(access.company.id),
                    name=company_group_name(access.company),
                    org_id=self.data_entities_processor.org_id,
                    description="Business Central company access (members from the connector's companyAccessGroups)",
                ),
                [self._app_user(e) for e in sorted(members)],
            ))
        if all_emails:
            await self.data_entities_processor.on_new_app_users([self._app_user(e) for e in sorted(all_emails)])
        if groups:
            await self.data_entities_processor.on_new_user_groups(groups)
        now_ms = get_epoch_timestamp_in_ms()
        await self.user_sync_point.update_sync_point(USERS_SYNC_POINT_KEY, {"lastSyncTimestamp": now_ms})
        await self.records_sync_point.update_sync_point(GROUPS_SYNC_POINT_KEY, {"lastSyncTimestamp": now_ms})
        self.logger.info("Business Central access model: %d company groups, %d users", len(groups), len(all_emails))

    def _company_permissions(self, company: Company) -> list[Permission]:
        access = self._access_by_company.get(company.id) or CompanyAccess(company=company)
        return grants_to_permissions(company_grants(access))

    # ------------------------------------------------------------------
    # Record groups and records
    # ------------------------------------------------------------------

    def _record_group(self, company: Company) -> RecordGroup:
        return RecordGroup(
            name=company_group_name(company),
            short_name=company.label,
            description=f"Microsoft Dynamics 365 Business Central company {company.label} ({self.environment_name})",
            external_group_id=company_group_external_id(company.id),
            connector_name=self.connector_name,
            connector_id=self.connector_id,
            group_type=RecordGroupType.ERP_ENTITY,
            web_url=company_web_url(self.tenant_id, self.environment_name, company.name),
            org_id=self.data_entities_processor.org_id,
        )

    async def _sync_record_groups(self) -> None:
        groups: list[tuple[RecordGroup, list[Permission]]] = [
            (self._record_group(company), self._company_permissions(company)) for company in self._companies
        ]
        if groups:
            await self.data_entities_processor.on_new_record_groups(groups)

    async def _sync_entity(self, company: Company, spec: EntitySpec, *, incremental: bool) -> None:
        key = _entity_sync_point_key(company.id, spec)
        state = EntitySyncState.from_sync_point(await self.records_sync_point.read_sync_point(key))
        start_ms, end_ms = self._modified_bounds()
        since_ms = state.last_sync_timestamp if incremental else None
        odata_filter = build_modified_filter(since_ms=since_ms, start_ms=start_ms, end_ms=end_ms)
        sync_started_ms = get_epoch_timestamp_in_ms()

        seen: set[str] = set()
        upserted = 0
        async for rows in self._iter_pages(company_path(company.id, spec), build_page_params(spec, odata_filter)):
            seen.update(seen_external_ids(company.id, spec, rows))
            upserted += await self._process_rows(company, spec, rows)
        state.last_sync_timestamp = sync_started_ms

        mode = plan_reconcile(
            state, full_read=odata_filter is None, now_ms=sync_started_ms, interval_hours=self._reconcile_interval_hours()
        )
        deleted = 0
        if mode is not ReconcileMode.NONE:
            try:
                live = seen if mode is ReconcileMode.SEEN else await self._live_external_ids(company, spec)
                deleted = await self._reconcile(company, spec, live)
                state.last_reconcile_timestamp = sync_started_ms
            except Exception as e:  # a failed deletion check must never fail the upsert sync
                self.logger.error(
                    "Business Central %s/%s deletion check failed: %s", company.label, spec.entity_set, e, exc_info=True
                )
        await self.records_sync_point.update_sync_point(key, state.to_sync_point())
        self.logger.info(
            "Synced %d %s records of company %s%s, %d deleted (%s)",
            upserted, spec.display_name.lower(), company.label,
            " (incremental)" if since_ms else "", deleted, mode.value,
        )

    async def _process_rows(self, company: Company, spec: EntitySpec, rows: list[dict[str, Any]]) -> int:
        permissions = self._company_permissions(company)
        batch: list[tuple[Record, list[Permission]]] = []
        for row in rows:
            record = self._build_record(company, spec, row)
            if record is not None:
                batch.append((record, list(permissions)))
        if batch:
            await self.data_entities_processor.on_new_records(batch)
        return len(batch)

    def _build_record(self, company: Company, spec: EntitySpec, row: dict[str, Any]) -> Record | None:
        row_id = row.get("id")
        if not row_id:
            return None
        row_id = str(row_id)
        modified_ms = parse_bc_timestamp(row.get(MODIFIED_FIELD))
        common: dict[str, Any] = {
            "org_id": self.data_entities_processor.org_id,
            "record_name": record_title(spec, row),
            "record_type": RecordType(spec.record_type),
            "record_group_type": RecordGroupType.ERP_ENTITY,
            "external_record_id": record_external_id(company.id, spec, row_id),
            "external_record_group_id": company_group_external_id(company.id),
            "external_revision_id": str(modified_ms) if modified_ms is not None else None,
            "version": 0,
            "origin": OriginTypes.CONNECTOR,
            "connector_name": self.connector_name,
            "connector_id": self.connector_id,
            "mime_type": MimeTypes.MARKDOWN.value,
            "weburl": record_web_url(self.tenant_id, self.environment_name, company.name, spec, row),
            "source_created_at": None,  # API v2.0 rows carry no creation timestamp
            "source_updated_at": modified_ms,
            "inherit_permissions": False,
            "preview_renderable": False,
        }
        if spec.entity_set == "items":
            blocked = row.get("blocked")
            return ProductRecord(
                **common,
                product_code=str(row.get("number") or "") or None,
                product_family=str(row.get("itemCategoryCode") or "") or None,
                is_active=(not bool(blocked)) if blocked is not None else None,
                sku=str(row.get("gtin") or "") or None,
                list_price=_as_float(row.get("unitPrice")),
            )
        return Record(**common)

    # ------------------------------------------------------------------
    # Delete detection (periodic key-set reconcile)
    # ------------------------------------------------------------------

    async def _live_external_ids(self, company: Company, spec: EntitySpec) -> set[str]:
        live: set[str] = set()
        async for rows in self._iter_pages(company_path(company.id, spec), build_key_page_params()):
            live.update(seen_external_ids(company.id, spec, rows))
        return live

    async def _known_records(self, company: Company, spec: EntitySpec) -> dict[str, str]:
        """``{external_record_id: record_id}`` the graph holds for one company × entity set.

        The connector's records are enumerated once per sync run (keyset-paged) and bucketed
        by ``bc:<companyId>:<entitySet>:`` prefix; later lookups are dictionary hits."""
        if self._known_records_cache is None:
            cache: dict[str, dict[str, str]] = {}
            after_key: str | None = None
            while True:
                page = await self.data_entities_processor.get_records_by_status(
                    self.connector_id, status_filters=[], limit=_KNOWN_RECORDS_PAGE, after_key=after_key,
                )
                if not page:
                    break
                for record in page:
                    external_id = str(getattr(record, "external_record_id", "") or "")
                    record_id = getattr(record, "id", None)
                    if not external_id or not record_id:
                        continue
                    prefix = external_id[: external_id.rfind(":") + 1]
                    cache.setdefault(prefix, {})[external_id] = str(record_id)
                after_key = str(page[-1].id) if getattr(page[-1], "id", None) else None
                if len(page) < _KNOWN_RECORDS_PAGE or not after_key:
                    break
            self._known_records_cache = cache
        return dict(self._known_records_cache.get(record_id_prefix(company.id, spec), {}))

    async def _reconcile(self, company: Company, spec: EntitySpec, live: set[str]) -> int:
        known = await self._known_records(company, spec)
        plan = diff_known_against_live(known, live)
        if plan.skipped_reason:
            self.logger.warning(
                "Business Central %s/%s deletion check skipped: %s (known=%d)",
                company.label, spec.entity_set, plan.skipped_reason, plan.known,
            )
            return 0
        if not plan.delete_record_ids:
            return 0
        self.logger.info(
            "Pruning %d %s records of company %s no longer present in Business Central",
            len(plan.delete_record_ids), spec.display_name.lower(), company.label,
        )
        deleted = await self._delete_record_ids(list(plan.delete_record_ids))
        if self._known_records_cache is not None:
            bucket = self._known_records_cache.get(record_id_prefix(company.id, spec), {})
            for external_id in [e for e, r in bucket.items() if r in plan.delete_record_ids]:
                bucket.pop(external_id, None)
        return deleted

    async def _delete_record_ids(self, record_ids: list[str]) -> int:
        """Standard cascade delete: drops the record, its permission edges and children and
        publishes the vector cleanup events."""
        deleted = 0
        for start in range(0, len(record_ids), _DELETE_BATCH_SIZE):
            chunk = record_ids[start:start + _DELETE_BATCH_SIZE]
            try:
                result = await self.data_entities_processor.on_records_deleted_cascade(chunk, self.connector_id)
            except Exception as e:
                self.logger.error("Failed to delete %d Business Central records: %s", len(chunk), e, exc_info=True)
                continue
            deleted += int((result or {}).get("successfully_deleted") or 0)
            failed = (result or {}).get("failed_records") or []
            if failed:
                self.logger.warning("%d Business Central record deletions failed: %s", len(failed), failed[:5])
        return deleted

    # ------------------------------------------------------------------
    # Streaming / reindex
    # ------------------------------------------------------------------

    async def get_signed_url(self, record: Record) -> str | None:
        return None  # Business Central has no downloadable content; the markdown is streamed.

    async def stream_record(self, record: Record, user_id: str | None = None, convertTo: str | None = None) -> StreamingResponse:
        company_id, spec, row_id = split_external_id(record.external_record_id)
        company = await self._company_by_id(company_id)
        row = await self._fetch_row(company_id, spec, row_id) if company is not None else None
        if company is None or row is None:
            markdown = f"# {record.record_name}\n\nThis Business Central record no longer exists.\n"
        else:
            markdown, _ = render_record_markdown(company, spec, row, self.tenant_id, self.environment_name)
        return create_stream_record_response(
            _bytes_stream(markdown.encode("utf-8")),
            filename=f"{record.record_name}.md",
            mime_type=MimeTypes.MARKDOWN.value,
            fallback_filename=f"record_{record.id}.md",
        )

    async def reindex_records(self, record_results: list[Record]) -> None:
        """Rebuild records whose ``lastModifiedDateTime`` moved past the stored revision; reindex the rest as-is."""
        if not record_results:
            return
        unchanged: list[Record] = []
        for record in record_results:
            try:
                company_id, spec, row_id = split_external_id(record.external_record_id)
            except ValueError:
                unchanged.append(record)
                continue
            company = await self._company_by_id(company_id)
            row = await self._fetch_row(company_id, spec, row_id) if company is not None else None
            if company is None or row is None:
                self.logger.warning("Business Central record %s no longer exists; reindexing stored copy", record.external_record_id)
                unchanged.append(record)
                continue
            modified_ms = parse_bc_timestamp(row.get(MODIFIED_FIELD))
            stored = _as_int(record.external_revision_id)
            if modified_ms is not None and (stored is None or modified_ms > stored):
                rebuilt = self._build_record(company, spec, row)
                if rebuilt is not None:
                    await self.data_entities_processor.on_record_content_update(rebuilt)
                    continue
            unchanged.append(record)
        if unchanged:
            await self.data_entities_processor.reindex_existing_records(unchanged)

    # ------------------------------------------------------------------
    # Webhooks / filters
    # ------------------------------------------------------------------

    async def handle_webhook_notification(self, notification: dict[str, Any]) -> bool:
        """Business Central webhook subscriptions are not wired yet; acknowledge like Dynamics 365 does."""
        return True

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: str | None = None,
        cursor: str | None = None,
    ) -> FilterOptionsResponse:
        if filter_key != ENTITIES_FILTER_KEY:
            raise ValueError(f"Unsupported filter key: {filter_key}")
        needle = (search or "").strip().lower()
        options = [
            FilterOption(id=name, label=ENTITY_SPECS[name].display_name)
            for name in DEFAULT_ENTITY_ORDER
            if not needle or needle in name.lower() or needle in ENTITY_SPECS[name].display_name.lower()
        ]
        start = max(page - 1, 0) * limit
        chunk = options[start:start + limit]
        return FilterOptionsResponse(success=True, options=chunk, page=page, limit=limit, has_more=start + limit < len(options))


def _as_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _as_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


async def _bytes_stream(data: bytes) -> AsyncGenerator[bytes, None]:
    yield data
