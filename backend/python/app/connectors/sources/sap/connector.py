"""SAP S/4HANA connector (OData v2/v4 APIs, technical-user or OAuth2 client credentials).

Indexes business documents (business partners, sales orders, purchase orders,
products, supplier invoices, optional DMS attachments) as markdown records and
mirrors SAP authorization as permission edges so permission-aware search only
surfaces documents the user could display in SAP.  The mapping itself lives in
``mapping.py`` (pure, unit-tested); this module owns auth, HTTP, paging, the
Microsoft Entra group resolver and the ``BaseConnector`` lifecycle.

Auth (``authType`` in the connector auth config, like Jira Data Center):

* ``BASIC_AUTH`` — technical user.  On-prem: a service user on the SAP Gateway
  (``SAP_ALL``-free role with ``S_SERVICE`` on the activated OData services);
  S/4HANA Cloud: a *communication user* from the communication arrangement.
* ``OAUTH_ADMIN_CONSENT`` — OAuth 2.0 client credentials (S/4HANA Cloud /
  SAP BTP: ``clientId`` / ``clientSecret`` / ``tokenUrl`` of the communication
  arrangement or XSUAA service instance).  ``AuthType.OAUTH_ADMIN_CONSENT`` is
  reused because, like Dynamics 365, it is an app-only flow without a user
  redirect; there is no dedicated client-credentials auth type in
  ``auth_builder``.

What the SAP admin must expose: S/4HANA Cloud — one communication arrangement
per scenario (``SAP_COM_0008`` Business Partner, ``SAP_COM_0109`` Sales Order,
``SAP_COM_0053`` Purchase Order, ``SAP_COM_0009`` Product, ``SAP_COM_0057``
Supplier Invoice, ``SAP_COM_0134`` Attachments) with inbound *read* services
only; on-prem (1909+) — activate the same ``API_*`` services in ``/IWFND/MAINT_SERVICE``
and grant the technical user display authorizations for the underlying objects.

Read-only: every call is a ``GET``; SAP requires ``x-csrf-token`` only for
modifying requests, so no token fetch is needed.  Writes are out of scope.

Deletions: the ``API_*`` services have no delta/change tracking, so the
incremental ``<change_field> gt <last sync>`` filter never sees a deleted row.
Every ``reconcileIntervalHours`` (sync setting, default 24 h, ``0`` = off) the
connector pulls the bare key set of each entity set, compares it with the record
external ids it holds and deletes the missing ones through the standard cascade
delete (``on_records_deleted_cascade``; attachments go with their document).  A
full sync reconciles for free.  See ``mapping.plan_reconcile``.

``sapClient`` is applied on every SAP request as both the ``sap-client`` query
parameter (unless the URL — e.g. a server ``__next`` link — already carries it)
and the ``sap-client`` header; it is never sent to the OAuth token endpoint.
``init()`` validates with ``GET <baseUrl>/sap/opu/odata/sap/API_BUSINESS_PARTNER/$metadata``
(falls back to the first *selected* service when the BP scenario is not active).
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from logging import Logger
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlsplit

import httpx
from fastapi.responses import StreamingResponse

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import Connectors, MimeTypes, OriginTypes
from app.config.constants.http_status_code import HttpStatusCode
from app.connectors.core.base.connector.connector_service import (
    BaseConnector,
    ConnectorInitError,
)
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.base.sync_point.sync_point import SyncDataPointType, SyncPoint
from app.connectors.core.constants import CONNECTOR_EMAIL_IDENTITY_INFO, IconPaths
from app.connectors.core.registry.auth_builder import AuthBuilder, AuthType
from app.connectors.core.registry.connector_builder import (
    AuthField,
    CommonFields,
    ConnectorBuilder,
    ConnectorScope,
    CustomField,
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
from app.connectors.sources.microsoft.common.entra_identity import (
    GRAPH_BASE_URL,
    USER_EMAIL_SELECT,
    EntraGraphClient,
    graph_user_email,
)
from app.connectors.sources.sap.apps import SapApp
from app.connectors.sources.sap.mapping import (
    ALL_FILTER_ENTITIES,
    AUTH_MODE_BASIC,
    COMPANY_CODES_FILTER_KEY,
    DEFAULT_ENTITY_ORDER,
    DEFAULT_RECONCILE_INTERVAL_HOURS,
    ENTITIES_FILTER_KEY,
    ENTITY_FILTER_LABELS,
    ENTITY_SPECS,
    RECONCILE_INTERVAL_KEY,
    RECONCILE_KEY_PAGE_SIZE,
    SALES_ORGS_FILTER_KEY,
    SAP_PAGE_SIZE,
    SCOPE_COMPANY_CODE,
    SCOPE_SALES_ORG,
    SUPPORTED_AUTH_MODES,
    AuthorizationContext,
    AuthorizationMapping,
    EntitySpec,
    ExpandedMapping,
    GrantEntity,
    GrantRole,
    Page,
    PermissionGrant,
    attachment_content_url,
    attachment_external_id,
    attachment_key,
    attachment_list_url,
    attachments_selected,
    build_key_page_params,
    build_modified_filter,
    build_page_params,
    build_scope_filter,
    combine_filters,
    derive_grants,
    entity_list_web_url,
    entity_set_url,
    entity_url,
    file_extension,
    group_display_name,
    group_ids_in_grants,
    metadata_url,
    next_page_skip,
    normalize_base_url,
    parse_authorization_mapping,
    parse_entity,
    parse_page,
    parse_reconcile_interval_hours,
    parse_sap_timestamp,
    plan_reconcile,
    reconcile_due,
    record_external_id,
    record_group_external_id,
    record_title,
    record_web_url,
    render_record_markdown,
    resolve_selected_entities,
    retry_delay,
    row_key_values,
    scope_codes,
    split_external_id,
)
from app.models.entities import (
    AppUser,
    AppUserGroup,
    FileRecord,
    ProductRecord,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.utils.streaming import create_stream_record_response
from app.utils.time_conversion import get_epoch_timestamp_in_ms

CONNECTOR_KEY = "sap"  # ConnectorFactory registry key / filters config name
USERS_SYNC_POINT_KEY = "users"
GROUPS_SYNC_POINT_KEY = "groups"
ENTITY_SYNC_POINT_PREFIX = "entity"
RECONCILE_SYNC_POINT_PREFIX = "reconcile"
RECONCILE_TIMESTAMP_FIELD = "lastReconcileTimestamp"
SAP_CLIENT_PARAM = "sap-client"

_MAX_HTTP_RETRIES = 5
_RETRY_STATUS = {HttpStatusCode.TOO_MANY_REQUESTS.value, 502, HttpStatusCode.SERVICE_UNAVAILABLE.value, 504}
_TOKEN_REFRESH_SKEW_S = 120
_MAX_CONCURRENT_REQUESTS = 4
_KNOWN_RECORDS_PAGE = 1000       # keyset page size when enumerating stored records
_RECONCILE_DELETE_BATCH = 100    # record ids per cascade-delete call
_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

_GRANT_ROLE_TO_PERMISSION = {
    GrantRole.READER: PermissionType.READ,
    GrantRole.WRITER: PermissionType.WRITE,
    GrantRole.OWNER: PermissionType.OWNER,
}
_GRANT_ENTITY_TO_PERMISSION = {
    GrantEntity.USER: EntityType.USER,
    GrantEntity.GROUP: EntityType.GROUP,
}


def _entity_sync_point_key(spec: EntitySpec) -> str:
    return f"{ENTITY_SYNC_POINT_PREFIX}/{spec.name}"


def _reconcile_sync_point_key(spec: EntitySpec) -> str:
    return f"{RECONCILE_SYNC_POINT_PREFIX}/{spec.name}"


def grants_to_permissions(grants: List[PermissionGrant]) -> List[Permission]:
    """Convert connector-agnostic grants (mapping.py) into graph ``Permission`` objects."""
    return [
        Permission(
            entity_type=_GRANT_ENTITY_TO_PERMISSION[grant.entity_type],
            type=_GRANT_ROLE_TO_PERMISSION[grant.role],
            external_id=grant.external_id,
            email=grant.email,
        )
        for grant in grants
    ]


def _shared_auth_fields() -> List[AuthField]:
    return [
        AuthField(
            name="baseUrl",
            display_name="SAP API base URL",
            placeholder="https://my300000-api.s4hana.cloud.sap",
            description="Host of the OData services (no path). S/4HANA Cloud: the -api host; on-prem: the SAP Gateway host.",
            field_type="URL",
            max_length=2048,
        ),
        AuthField(
            name="sapClient",
            display_name="SAP client (optional)",
            placeholder="100",
            description="Adds sap-client=<n> to every request (on-prem systems with several clients).",
            required=False,
            min_length=0,
            max_length=3,
        ),
        AuthField(
            name="fioriBaseUrl",
            display_name="Fiori launchpad URL (optional)",
            placeholder="https://my300000.s4hana.cloud.sap",
            description="When set, records link to the Fiori object page (…/ui#SalesOrder-displayFactSheet?SalesOrder=…) instead of the raw OData entity.",
            field_type="URL",
            required=False,
            min_length=0,
            max_length=2048,
        ),
    ]


@ConnectorBuilder("SAP")\
    .in_group("SAP")\
    .with_description(
        "Sync business partners, sales orders, purchase orders, products, supplier invoices "
        "and DMS attachments from SAP S/4HANA (Cloud or on-prem Gateway) over OData, with "
        "permissions derived from SAP organisational data and configurable authorization groups"
    )\
    .with_categories(["ERP", "Finance", "Sales"])\
    .with_scopes([ConnectorScope.TEAM.value])\
    .with_auth([
        AuthBuilder.type(AuthType.BASIC_AUTH).fields(_shared_auth_fields() + [
            AuthField(
                name="username",
                display_name="Technical user",
                placeholder="CGRAPH_SYNC",
                description="Communication user (S/4HANA Cloud) or Gateway service user (on-prem) with display authorization on the synced services.",
                max_length=500,
            ),
            AuthField(
                name="password",
                display_name="Password",
                placeholder="Enter the technical user's password",
                description="Password of the technical user.",
                field_type="PASSWORD",
                max_length=2000,
                is_secret=True,
            ),
        ]),
        AuthBuilder.type(AuthType.OAUTH_ADMIN_CONSENT).fields(_shared_auth_fields() + [
            AuthField(
                name="clientId",
                display_name="OAuth client ID",
                placeholder="sb-cgraph!b1234|s4hana!b5",
                description="Client id of the communication arrangement / XSUAA service instance (client credentials grant).",
                max_length=2000,
            ),
            AuthField(
                name="clientSecret",
                display_name="OAuth client secret",
                placeholder="Enter the client secret",
                description="Client secret used with the client credentials grant.",
                field_type="PASSWORD",
                max_length=4000,
                is_secret=True,
            ),
            AuthField(
                name="tokenUrl",
                display_name="OAuth token URL",
                placeholder="https://my300000.authentication.eu10.hana.ondemand.com/oauth/token",
                description="Token endpoint that issues bearer tokens for the OData services.",
                field_type="URL",
                max_length=2048,
            ),
        ]),
    ])\
    .with_info(
        "Permissions: SAP does not expose PFCG role assignments over OData. Access is derived from the "
        "document's organisational fields (company code, sales organisation, plant, purchasing "
        "organisation) mapped to principals in the 'Authorization mapping' JSON — Microsoft Entra ID "
        "groups are the recommended source (configure the Entra fields), literal e-mails also work.\n\n"
        + CONNECTOR_EMAIL_IDENTITY_INFO
    )\
    .configure(lambda builder: builder
        .with_icon(IconPaths.connector_icon(Connectors.SAP.value))
        .add_documentation_link(DocumentationLink(
            "SAP Business Accelerator Hub — S/4HANA Cloud OData APIs",
            "https://api.sap.com/products/SAPS4HANACloud/apis/ODATA",
            "api",
        ))
        .add_documentation_link(DocumentationLink(
            "Maintain communication arrangements (S/4HANA Cloud)",
            "https://help.sap.com/docs/SAP_S4HANA_CLOUD/0f69f8fb28ac4bf48d2b57b9637e81fa/1e4e6e6e6c5f4e3aa2e0c0f6c3b5f1a0.html",
            "setup",
        ))
        .add_documentation_link(DocumentationLink(
            "Activate OData services on SAP Gateway (/IWFND/MAINT_SERVICE)",
            "https://help.sap.com/docs/ABAP_PLATFORM_NEW/68bf513362174d54b58cddec28794093/bb2bfe50645c741ae10000000a423f68.html",
            "setup",
        ))
        .add_filter_field(FilterField(
            name=ENTITIES_FILTER_KEY,
            display_name="Entities",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="SAP business objects to sync. Attachments are opt-in (one extra request per document).",
            options=list(ALL_FILTER_ENTITIES),
            option_source_type=OptionSourceType.STATIC,
            default_value=list(DEFAULT_ENTITY_ORDER),
        ))
        .add_filter_field(FilterField(
            name=COMPANY_CODES_FILTER_KEY,
            display_name="Company codes",
            filter_type=FilterType.LIST,
            category=FilterCategory.SYNC,
            description="Only sync purchase orders / supplier invoices of these company codes (empty = all).",
            option_source_type=OptionSourceType.DYNAMIC,
        ))
        .add_filter_field(FilterField(
            name=SALES_ORGS_FILTER_KEY,
            display_name="Sales organisations",
            filter_type=FilterType.LIST,
            category=FilterCategory.SYNC,
            description="Only sync sales orders of these sales organisations (empty = all).",
            option_source_type=OptionSourceType.DYNAMIC,
        ))
        .add_filter_field(CommonFields.modified_date_filter(
            "Only sync documents whose last-change timestamp falls in this range."
        ))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .add_sync_custom_field(CustomField(
            name="authorization_mapping",
            display_name="Authorization mapping (JSON)",
            field_type="TEXT",
            required=False,
            description=(
                'JSON object mapping SAP scopes to principals, e.g. {"admins": ["group:SAP Administrators"], '
                '"companycode:1010": ["group:SAP Finance 1010", "user:cfo@edrak.com"], '
                '"salesorg:1010": ["group:<entra-object-id>"], "plant:1010": [...], '
                '"entity:product": ["group:Everyone"], "groups": {"Name": ["a@x.com"]}, '
                '"users": {"CB9980000042": "jane@edrak.com"}}. group: entries are Entra groups '
                "(display name or object id) when the Entra fields are set, else inline 'groups'."
            ),
        ))
        .add_sync_custom_field(CustomField(
            name="userEmailDomain",
            display_name="User e-mail domain (optional)",
            field_type="TEXT",
            required=False,
            description="Maps SAP user ids to <sapuser>@<domain> for the document creator (OWNER). Leave empty to skip; use the 'users' map for technical ids.",
        ))
        .add_sync_custom_field(CustomField(
            name=RECONCILE_INTERVAL_KEY,
            display_name="Deletion check interval (hours)",
            field_type="NUMBER",
            required=False,
            default_value=str(int(DEFAULT_RECONCILE_INTERVAL_HOURS)),
            min_length=0,
            max_length=8760,
            description=(
                "SAP OData exposes no deletions, so every N hours the connector re-reads the key list of each "
                "synced entity set and removes records SAP no longer returns (attachments follow their document). "
                "Default 24; 0 disables the check (deleted SAP documents then stay searchable until a full sync)."
            ),
        ))
        .add_sync_custom_field(CustomField(
            name="entraTenantId",
            display_name="Entra tenant ID (optional)",
            field_type="TEXT",
            required=False,
            description="Microsoft Entra tenant used to expand group: principals via Microsoft Graph.",
        ))
        .add_sync_custom_field(CustomField(
            name="entraClientId",
            display_name="Entra application (client) ID (optional)",
            field_type="TEXT",
            required=False,
            description="App registration with application permissions GroupMember.Read.All and User.Read.All (admin consented).",
        ))
        .add_sync_custom_field(CustomField(
            name="entraClientSecret",
            display_name="Entra client secret (optional)",
            field_type="PASSWORD",
            required=False,
            is_secret=True,
            description="Client secret of the Entra app registration.",
        ))
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 60)
        .with_sync_support(True)
        .with_agent_support(False)
    )\
    .build_decorator()
class SapConnector(BaseConnector):
    """SAP S/4HANA OData connector. See module docstring and ``mapping.py``."""

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
            SapApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_name = Connectors.SAP

        def _sync_point(kind: SyncDataPointType) -> SyncPoint:
            return SyncPoint(
                connector_id=self.connector_id,
                org_id=self.data_entities_processor.org_id,
                sync_data_point_type=kind,
                data_store_provider=self.data_store_provider,
            )

        self.user_sync_point = _sync_point(SyncDataPointType.USERS)
        self.records_sync_point = _sync_point(SyncDataPointType.RECORDS)

        self.base_url: str = ""
        self.fiori_base_url: Optional[str] = None
        self.sap_client: Optional[str] = None
        self.auth_mode: str = AUTH_MODE_BASIC
        self._basic_auth: Optional[Tuple[str, str]] = None
        self._oauth: Dict[str, str] = {}
        self._token: Optional[str] = None
        self._token_expires_on: int = 0
        self._http: Optional[httpx.AsyncClient] = None
        self._request_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REQUESTS)

        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self._sync_config: Dict[str, Any] = {}
        self._auth_ctx: Optional[AuthorizationContext] = None
        self._expanded: Optional[ExpandedMapping] = None
        self._entra: Optional["EntraGroupResolver"] = None
        self._known_groups: set[str] = set()
        self._known_users: set[str] = set()
        self._seen_codes: Dict[str, set[str]] = {}
        self._full_read_entities: set[str] = set()  # entities read completely this run (see reconcile)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> bool:
        config = await self.config_service.get_config(f"/services/connectors/{self.connector_id}/config")
        if not config:
            self.logger.error("SAP config not found")
            return False
        auth = config.get("auth", {}) or {}
        self._sync_config = config.get("sync", {}) or {}

        auth_mode = str(auth.get("authType") or AUTH_MODE_BASIC).strip().upper()
        if auth_mode not in SUPPORTED_AUTH_MODES:
            raise ConnectorInitError(f"Unsupported SAP authType {auth_mode!r} (expected BASIC_AUTH or OAUTH_ADMIN_CONSENT)")
        self.auth_mode = auth_mode
        try:
            self.base_url = normalize_base_url(auth.get("baseUrl") or "")
        except ValueError as e:
            raise ConnectorInitError(str(e)) from e
        fiori = (auth.get("fioriBaseUrl") or "").strip()
        self.fiori_base_url = normalize_base_url(fiori) if fiori else None
        client = str(auth.get("sapClient") or "").strip()
        self.sap_client = client or None

        if auth_mode == AUTH_MODE_BASIC:
            username, password = auth.get("username"), auth.get("password")
            if not username or not password:
                raise ConnectorInitError("Incomplete SAP credentials: username and password are required for BASIC_AUTH.")
            self._basic_auth = (str(username), str(password))
            self._oauth = {}
        else:
            missing = [k for k in ("clientId", "clientSecret", "tokenUrl") if not auth.get(k)]
            if missing:
                raise ConnectorInitError(f"Incomplete SAP OAuth credentials: {', '.join(missing)} required for client credentials.")
            self._oauth = {k: str(auth[k]) for k in ("clientId", "clientSecret", "tokenUrl")}
            self._basic_auth = None

        await self._close_http()
        # ``sap-client`` is added per SAP request in ``_sap_request_args`` (param + header)
        # rather than as a client default, so it never leaks onto the OAuth token POST and
        # is not duplicated on ``__next`` links that already carry it.
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=15.0),
            headers={"Accept": "application/json"},
            follow_redirects=False,
        )
        try:
            if self._oauth:
                await self._refresh_token()
            await self._validate_access()
        except ConnectorInitError:
            await self._close_http()
            raise
        except Exception as e:
            await self._close_http()
            raise ConnectorInitError(
                "Could not connect to SAP. Check baseUrl, credentials and that the OData services are "
                f"activated / the communication arrangement exists. ({type(e).__name__}: {str(e)[:200]})"
            ) from e
        self.logger.info("SAP connector initialised for %s (%s)", self.base_url, self.auth_mode)
        return True

    async def _validate_access(self) -> None:
        """``$metadata`` of API_BUSINESS_PARTNER, else the first service that answers."""
        tried: List[str] = []
        for name in ("business_partner",) + tuple(n for n in DEFAULT_ENTITY_ORDER if n != "business_partner"):
            spec = ENTITY_SPECS[name]
            try:
                await self._get_raw(metadata_url(self.base_url, spec), accept="application/xml")
                return
            except httpx.HTTPStatusError as e:
                tried.append(f"{spec.service}={e.response.status_code}")
                if e.response.status_code in (HttpStatusCode.UNAUTHORIZED.value, HttpStatusCode.FORBIDDEN.value):
                    raise ConnectorInitError(
                        f"SAP rejected the credentials for {spec.service} (HTTP {e.response.status_code})."
                    ) from e
        raise ConnectorInitError(f"No SAP OData service reachable at {self.base_url}: {', '.join(tried)}")

    async def test_connection_and_access(self) -> bool:
        try:
            await self._validate_access()
            return True
        except Exception as e:
            self.logger.error("SAP connection test failed: %s", e)
            return False

    async def cleanup(self) -> None:
        await self._close_http()
        self._auth_ctx = None
        self._expanded = None

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
        **kwargs: Any,
    ) -> "BaseConnector":
        return cls(logger, data_entities_processor, data_store_provider, config_service, connector_id, scope, created_by)

    # ------------------------------------------------------------------
    # HTTP / auth plumbing
    # ------------------------------------------------------------------

    async def _refresh_token(self) -> str:
        """Client-credentials grant.  The token endpoint (XSUAA / IAS) rate-limits too, so
        429/503 are retried honouring ``Retry-After`` exactly like data requests."""
        if self._http is None or not self._oauth:
            raise RuntimeError("SAP connector not initialised for OAuth")
        delay = 1.0
        for attempt in range(_MAX_HTTP_RETRIES + 1):
            try:
                response = await self._http.post(
                    self._oauth["tokenUrl"],
                    data={"grant_type": "client_credentials"},
                    auth=(self._oauth["clientId"], self._oauth["clientSecret"]),
                    headers={"Accept": "application/json"},
                )
            except (httpx.TimeoutException, httpx.TransportError) as e:
                if attempt >= _MAX_HTTP_RETRIES:
                    raise
                self.logger.warning("SAP token request failed (%s), retrying in %.1fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            if response.status_code in _RETRY_STATUS and attempt < _MAX_HTTP_RETRIES:
                wait = retry_delay(response.headers.get("Retry-After"), delay)
                self.logger.warning("SAP token endpoint returned %s, retrying in %.1fs", response.status_code, wait)
                await asyncio.sleep(wait)
                delay = min(delay * 2, 30.0)
                continue
            break
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise ConnectorInitError("SAP token endpoint returned no access_token")
        self._token = str(token)
        expires_in = int(payload.get("expires_in") or 3600)
        self._token_expires_on = get_epoch_timestamp_in_ms() // 1000 + expires_in
        return self._token

    async def _auth_headers(self) -> Dict[str, str]:
        if not self._oauth:
            return {}
        now_s = get_epoch_timestamp_in_ms() // 1000
        if not self._token or now_s >= self._token_expires_on - _TOKEN_REFRESH_SKEW_S:
            await self._refresh_token()
        return {"Authorization": f"Bearer {self._token}"}

    def _sap_request_args(
        self, url: str, params: Optional[Dict[str, str]]
    ) -> Tuple[Optional[Dict[str, str]], Dict[str, str]]:
        """``(params, headers)`` carrying ``sapClient`` consistently for every SAP call:
        the ``sap-client`` query parameter (skipped when ``url`` already has one, as SAP's
        ``__next`` links do) plus the equivalent ``sap-client`` header (honoured by the
        ICF layer even when a proxy strips query strings).  Both auth modes share this."""
        if not self.sap_client:
            return params, {}
        headers = {SAP_CLIENT_PARAM: self.sap_client}
        query = urlsplit(url).query
        if any(k == SAP_CLIENT_PARAM for k, _ in parse_qsl(query, keep_blank_values=True)):
            return params, headers
        merged = dict(params or {})
        merged.setdefault(SAP_CLIENT_PARAM, self.sap_client)
        return merged, headers

    async def _get_raw(
        self,
        url: str,
        params: Optional[Dict[str, str]] = None,
        accept: str = "application/json",
    ) -> httpx.Response:
        """GET with basic/bearer auth, one 401 re-auth, bounded retry on 429/5xx honouring
        ``Retry-After`` (delay-seconds or HTTP-date) — identical for BASIC and OAuth."""
        if self._http is None:
            raise RuntimeError("SAP connector not initialised")
        refreshed = False
        delay = 1.0
        params, client_headers = self._sap_request_args(url, params)
        for attempt in range(_MAX_HTTP_RETRIES + 1):
            headers = {"Accept": accept, **client_headers, **(await self._auth_headers())}
            async with self._request_semaphore:
                try:
                    response = await self._http.get(url, params=params, headers=headers, auth=self._basic_auth)
                except (httpx.TimeoutException, httpx.TransportError) as e:
                    if attempt >= _MAX_HTTP_RETRIES:
                        raise
                    self.logger.warning("SAP request failed (%s), retrying in %.1fs", e, delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    continue
            if response.status_code == HttpStatusCode.UNAUTHORIZED.value and self._oauth and not refreshed:
                refreshed = True
                await self._refresh_token()
                continue
            if response.status_code in _RETRY_STATUS and attempt < _MAX_HTTP_RETRIES:
                wait = retry_delay(response.headers.get("Retry-After"), delay)
                self.logger.warning("SAP returned %s for %s, retrying in %.1fs", response.status_code, url, wait)
                await asyncio.sleep(wait)
                delay = min(delay * 2, 30.0)
                continue
            response.raise_for_status()
            return response
        raise RuntimeError(f"SAP request to {url} exhausted retries")

    async def _get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        response = await self._get_raw(url, params=params)
        payload = response.json()
        return payload if isinstance(payload, dict) else {"d": payload}

    async def _iter_pages(
        self,
        spec: EntitySpec,
        odata_filter: Optional[str],
        params_builder: Callable[..., Dict[str, str]] = build_page_params,
        page_size: int = SAP_PAGE_SIZE,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """``$top``/``$skip`` paging; a server-side ``__next`` / ``@odata.nextLink`` wins when present.
        ``params_builder`` is :func:`build_page_params` (documents) or :func:`build_key_page_params` (reconcile)."""
        url = entity_set_url(self.base_url, spec)
        skip = 0
        while True:
            params = params_builder(spec, odata_filter, skip=skip, top=page_size)
            page: Page = parse_page(await self._get_json(url, params=params), spec.odata_version)
            yield page.rows
            while page.next_link:
                page = parse_page(await self._get_json(page.next_link), spec.odata_version)
                yield page.rows
            nxt = next_page_skip(page, skip, page_size)
            if nxt is None:
                return
            skip = nxt

    async def _iter_rows_with_fallback(self, spec: EntitySpec, odata_filter: Optional[str], scope_filter: Optional[str]) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Some releases lack the change-tracking property; on HTTP 400 retry without the
        modified clause (full re-read) instead of failing the sync.  A fallback read is
        complete within scope, so it is recorded in ``_full_read_entities`` and lets the
        entity reconcile deletions without a second key pull."""
        try:
            async for rows in self._iter_pages(spec, combine_filters(odata_filter, scope_filter)):
                yield rows
        except httpx.HTTPStatusError as e:
            if e.response.status_code != HttpStatusCode.BAD_REQUEST.value or not odata_filter:
                raise
            self.logger.warning(
                "SAP %s rejected the %s filter (HTTP 400); falling back to a full read of the entity",
                spec.service, spec.change_field,
            )
            self._full_read_entities.add(spec.name)
            async for rows in self._iter_pages(spec, scope_filter):
                yield rows

    async def _iter_live_external_ids(self, spec: EntitySpec, scope_filter: Optional[str]) -> AsyncGenerator[str, None]:
        """Key-only pull of the whole entity set (within the scope filters) → record external ids."""
        async for rows in self._iter_pages(spec, scope_filter, params_builder=build_key_page_params, page_size=RECONCILE_KEY_PAGE_SIZE):
            for row in rows:
                keys = row_key_values(spec, row)
                if keys:
                    yield record_external_id(spec, keys)

    async def _fetch_row(self, spec: EntitySpec, key_values: Sequence[str]) -> Optional[Dict[str, Any]]:
        params: Dict[str, str] = {}
        if spec.odata_version != 4:
            params["$format"] = "json"
        if spec.select_fields:
            params["$select"] = ",".join(spec.select_fields)
        if spec.expand:
            params["$expand"] = ",".join(spec.expand)
        try:
            payload = await self._get_json(entity_url(self.base_url, spec, key_values), params=params)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                return None
            raise
        return parse_entity(payload, spec.odata_version)

    # ------------------------------------------------------------------
    # Sync entry points
    # ------------------------------------------------------------------

    async def run_sync(self) -> None:
        """Full sync: authorization groups → record groups → every selected entity (all rows)."""
        await self._run(incremental=False)

    async def run_incremental_sync(self) -> None:
        """Incremental: same pipeline, entity pages filtered by ``<change_field> gt <last sync>``.

        Deletes are undetectable through that filter (no change tracking / delta links
        on the ``API_*`` services), so every ``reconcileIntervalHours`` the entity's key
        set is re-read and records SAP no longer returns are deleted (``_reconcile_entity``).
        """
        await self._run(incremental=True)

    async def _run(self, incremental: bool) -> None:
        if self._http is None:
            self.logger.error("SAP connector not initialised")
            return
        self.logger.info("Starting SAP %s sync", "incremental" if incremental else "full")
        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, CONNECTOR_KEY, self.connector_id, self.logger
        )
        selected = self._selected_entity_names()
        specs = resolve_selected_entities(selected)
        with_attachments = attachments_selected(selected)

        self._full_read_entities = set()
        await self._sync_authorization_model(specs)
        await self._sync_record_groups(specs)
        for spec in specs:
            seen_ids = await self._sync_entity(spec, incremental=incremental, with_attachments=with_attachments)
            await self._maybe_reconcile_entity(spec, seen_ids)
        self.logger.info("SAP sync completed")

    def _selected_entity_names(self) -> Optional[List[str]]:
        entity_filter = self.sync_filters.get(ENTITIES_FILTER_KEY) if self.sync_filters else None
        if entity_filter is None or entity_filter.is_empty():
            return None
        return [str(v) for v in entity_filter.as_list()]

    def _list_filter(self, key: str) -> List[str]:
        f = self.sync_filters.get(key) if self.sync_filters else None
        if f is None or f.is_empty():
            return []
        return [str(v).strip() for v in f.as_list() if str(v).strip()]

    def _modified_bounds(self) -> Tuple[Optional[int], Optional[int]]:
        modified = self.sync_filters.get(SyncFilterKey.MODIFIED) if self.sync_filters else None
        if modified is None or modified.is_empty():
            return None, None
        return modified.get_datetime_start(), modified.get_datetime_end()

    # ------------------------------------------------------------------
    # Authorization groups (mapping + Entra) → AppUser / AppUserGroup
    # ------------------------------------------------------------------

    def _sync_setting(self, *names: str) -> Optional[str]:
        for name in names:
            value = self._sync_config.get(name)
            if value not in (None, ""):
                return str(value)
        return None

    async def _sync_authorization_model(self, specs: List[EntitySpec]) -> None:
        raw_mapping = self._sync_config.get("authorization_mapping")
        try:
            mapping = parse_authorization_mapping(raw_mapping)
        except ValueError as e:
            self.logger.error("SAP authorization_mapping invalid, syncing with admins/creators only: %s", e)
            mapping = AuthorizationMapping()
        for warning in mapping.warnings:
            self.logger.warning("SAP authorization_mapping: %s", warning)

        resolver = None
        tenant = self._sync_setting("entraTenantId", "entra_tenant_id")
        client_id = self._sync_setting("entraClientId", "entra_client_id")
        secret = self._sync_setting("entraClientSecret", "entra_client_secret")
        if tenant and client_id and secret:
            if self._entra is None:
                self._entra = EntraGroupResolver(tenant, client_id, secret, self.logger)
            resolved = await self._entra.resolve_many(mapping.referenced_named_groups())
            resolver = resolved.get
        elif mapping.referenced_named_groups():
            self.logger.info(
                "SAP: Entra credentials not configured; group: principals resolve against the inline 'groups' section only"
            )
        expanded = mapping.expand(resolver)
        for name in expanded.unresolved_groups:
            self.logger.warning("SAP authorization_mapping: group %r resolved nowhere (Entra/inline); it grants nobody", name)

        self._auth_ctx = AuthorizationContext(
            mapping=mapping,
            user_email_domain=self._sync_setting("userEmailDomain", "user_email_domain"),
        )
        self._expanded = expanded
        self._known_groups = set()
        self._known_users = set()

        await self._ensure_users(expanded.all_emails())
        await self._ensure_groups(mapping.group_ids() + [f"sap:entity:{spec.name}" for spec in specs])
        await self.user_sync_point.update_sync_point(USERS_SYNC_POINT_KEY, {"lastSyncTimestamp": get_epoch_timestamp_in_ms()})
        await self.records_sync_point.update_sync_point(GROUPS_SYNC_POINT_KEY, {"lastSyncTimestamp": get_epoch_timestamp_in_ms()})
        self.logger.info(
            "SAP authorization model: %d groups, %d users, %d unresolved group refs",
            len(self._known_groups), len(self._known_users), len(expanded.unresolved_groups),
        )

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

    async def _ensure_users(self, emails: Sequence[str]) -> None:
        new = [e for e in emails if e and e not in self._known_users]
        if not new:
            return
        await self.data_entities_processor.on_new_app_users([self._app_user(e) for e in new])
        self._known_users.update(new)

    async def _ensure_groups(self, group_ids: Sequence[str]) -> None:
        """Upsert ``sap:*`` groups (membership from the expanded mapping; empty when unknown)."""
        assert self._expanded is not None
        pending = [g for g in dict.fromkeys(group_ids) if g not in self._known_groups]
        if not pending:
            return
        groups: List[Tuple[AppUserGroup, List[AppUser]]] = []
        for gid in pending:
            members = self._expanded.members.get(gid, [])
            await self._ensure_users(members)
            groups.append((
                AppUserGroup(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_user_group_id=gid,
                    name=group_display_name(gid, self._auth_ctx.mapping if self._auth_ctx else None),
                    org_id=self.data_entities_processor.org_id,
                    description="SAP authorization group (membership from the connector's authorization mapping)",
                ),
                [self._app_user(e) for e in members],
            ))
        await self.data_entities_processor.on_new_user_groups(groups)
        self._known_groups.update(pending)

    # ------------------------------------------------------------------
    # Record groups and records
    # ------------------------------------------------------------------

    async def _sync_record_groups(self, specs: List[EntitySpec]) -> None:
        assert self._auth_ctx is not None
        groups: List[Tuple[RecordGroup, List[Permission]]] = []
        for spec in specs:
            groups.append((
                RecordGroup(
                    name=f"SAP · {spec.display_name}",
                    short_name=spec.display_name,
                    description=spec.description,
                    external_group_id=record_group_external_id(spec),
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    group_type=RecordGroupType(spec.record_group_type),
                    web_url=entity_list_web_url(self.base_url, spec, self.fiori_base_url),
                    org_id=self.data_entities_processor.org_id,
                ),
                grants_to_permissions(self._auth_ctx.entity_grants(spec)),
            ))
        if groups:
            await self.data_entities_processor.on_new_record_groups(groups)

    async def _sync_entity(self, spec: EntitySpec, incremental: bool, with_attachments: bool) -> Optional[set[str]]:
        """Upsert every (changed) row of ``spec``.  Returns the external ids seen when the
        pass read the *complete* entity set within scope (full sync, no modified window,
        or the HTTP-400 fallback) — ``None`` when it was a partial, filtered read."""
        assert self._auth_ctx is not None
        key = _entity_sync_point_key(spec)
        since_ms: Optional[int] = None
        if incremental:
            point = await self.records_sync_point.read_sync_point(key)
            since_ms = point.get("lastSyncTimestamp") if point else None
        start_ms, end_ms = self._modified_bounds()
        modified_filter = build_modified_filter(spec, since_ms=since_ms, start_ms=start_ms, end_ms=end_ms)
        scope_filter = build_scope_filter(
            spec,
            company_codes=self._list_filter(COMPANY_CODES_FILTER_KEY),
            sales_orgs=self._list_filter(SALES_ORGS_FILTER_KEY),
        )
        sync_started_ms = get_epoch_timestamp_in_ms()
        if modified_filter is None:
            self._full_read_entities.add(spec.name)

        total = 0
        seen_ids: set[str] = set()
        async for rows in self._iter_rows_with_fallback(spec, modified_filter, scope_filter):
            batch: List[Tuple[Record, List[Permission]]] = []
            creators: List[str] = []
            group_ids: List[str] = []
            for row in rows:
                built = self._build_record_with_permissions(spec, row)
                if built is None:
                    continue
                record, permissions, grants = built
                seen_ids.add(record.external_record_id)
                batch.append((record, permissions))
                group_ids.extend(group_ids_in_grants(grants))
                creators.extend(g.email for g in grants if g.entity_type == GrantEntity.USER and g.email)
                for kind, codes in scope_codes(spec, row).items():
                    self._seen_codes.setdefault(kind, set()).update(codes)
                if with_attachments:
                    batch.extend(await self._build_attachment_records(spec, row, permissions))
            if batch:
                await self._ensure_users(creators)
                await self._ensure_groups(group_ids)
                await self.data_entities_processor.on_new_records(batch)
                total += len(batch)
        await self.records_sync_point.update_sync_point(key, {"lastSyncTimestamp": sync_started_ms})
        self.logger.info("Synced %d %s records%s", total, spec.display_name.lower(), " (incremental)" if since_ms else "")
        return seen_ids if spec.name in self._full_read_entities else None

    # ------------------------------------------------------------------
    # Deletion detection (periodic key-set reconcile)
    # ------------------------------------------------------------------

    def _reconcile_interval_hours(self) -> float:
        return parse_reconcile_interval_hours(self._sync_setting(RECONCILE_INTERVAL_KEY, "reconcile_interval_hours"))

    async def _maybe_reconcile_entity(self, spec: EntitySpec, seen_ids: Optional[set[str]]) -> None:
        """Run the deletion check when the interval elapsed, or for free after a complete read."""
        interval_hours = self._reconcile_interval_hours()
        if interval_hours <= 0:
            return
        now_ms = get_epoch_timestamp_in_ms()
        if seen_ids is None:
            point = await self.records_sync_point.read_sync_point(_reconcile_sync_point_key(spec))
            last_ms = point.get(RECONCILE_TIMESTAMP_FIELD) if point else None
            if not reconcile_due(last_ms, now_ms, interval_hours):
                return
        try:
            await self._reconcile_entity(spec, seen_ids)
        except Exception as e:  # a failed deletion check must never fail the upsert sync
            self.logger.error("SAP %s deletion check failed: %s", spec.display_name.lower(), e, exc_info=True)

    async def _reconcile_entity(self, spec: EntitySpec, live_ids: Optional[set[str]] = None) -> int:
        """Delete stored ``spec`` records whose key SAP no longer returns.

        ``live_ids`` are the external ids of a complete read just performed; when
        ``None`` the key set is pulled with ``$select=<keys>`` only (scope filters
        applied, the modified-date window deliberately not — records outside the window
        still exist in SAP).  Returns the number of records deleted.
        """
        started_ms = get_epoch_timestamp_in_ms()
        if live_ids is None:
            scope_filter = build_scope_filter(
                spec,
                company_codes=self._list_filter(COMPANY_CODES_FILTER_KEY),
                sales_orgs=self._list_filter(SALES_ORGS_FILTER_KEY),
            )
            live_ids = set()
            async for external_id in self._iter_live_external_ids(spec, scope_filter):
                live_ids.add(external_id)
        known = await self._known_record_ids(spec)
        plan = plan_reconcile(spec, known.keys(), live_ids)
        if plan.skipped_reason:
            self.logger.warning("SAP %s deletion check skipped: %s (known=%d)", spec.display_name.lower(), plan.skipped_reason, plan.known)
            return 0
        deleted = 0
        record_ids = [known[e] for e in plan.delete_external_ids if known.get(e)]
        for start in range(0, len(record_ids), _RECONCILE_DELETE_BATCH):
            chunk = record_ids[start:start + _RECONCILE_DELETE_BATCH]
            deleted += await self._delete_records(chunk)
        await self.records_sync_point.update_sync_point(_reconcile_sync_point_key(spec), {RECONCILE_TIMESTAMP_FIELD: started_ms})
        self.logger.info(
            "SAP %s deletion check: %d known, %d live, %d deleted",
            spec.display_name.lower(), plan.known, plan.live, deleted,
        )
        return deleted

    async def _known_record_ids(self, spec: EntitySpec) -> Dict[str, str]:
        """``external_record_id -> record id`` of every stored document record of ``spec``
        (keyset-paged over the connector's records; attachments and other entities skipped)."""
        prefix = f"{spec.name}:"
        out: Dict[str, str] = {}
        after_key: Optional[str] = None
        while True:
            page = await self.data_entities_processor.get_records_by_status(
                self.connector_id, status_filters=[], limit=_KNOWN_RECORDS_PAGE, after_key=after_key,
            )
            if not page:
                break
            for record in page:
                external_id = record.external_record_id or ""
                if external_id.startswith(prefix) and record.id:
                    out[external_id] = record.id
            after_key = page[-1].id
            if len(page) < _KNOWN_RECORDS_PAGE or not after_key:
                break
        return out

    async def _delete_records(self, record_ids: List[str]) -> int:
        """Standard record-deletion path: cascade (document + attachment children); per-record
        fallback when the cascade is unavailable."""
        if not record_ids:
            return 0
        try:
            result = await self.data_entities_processor.on_records_deleted_cascade(record_ids, self.connector_id)
            count = (result or {}).get("successfully_deleted")
            return int(count) if count is not None else len(record_ids)
        except Exception as e:
            self.logger.warning("SAP cascade delete failed (%s); deleting %d records one by one", e, len(record_ids))
        deleted = 0
        for record_id in record_ids:
            try:
                await self.data_entities_processor.on_record_deleted(record_id)
                deleted += 1
            except Exception as e:
                self.logger.error("SAP could not delete record %s: %s", record_id, e)
        return deleted

    def _build_record_with_permissions(
        self, spec: EntitySpec, row: Dict[str, Any]
    ) -> Optional[Tuple[Record, List[Permission], List[PermissionGrant]]]:
        assert self._auth_ctx is not None
        record = self._build_record(spec, row)
        if record is None:
            return None
        grants = derive_grants(spec, row, self._auth_ctx)
        return record, grants_to_permissions(grants), grants

    def _build_record(self, spec: EntitySpec, row: Dict[str, Any]) -> Optional[Record]:
        keys = row_key_values(spec, row)
        if not keys:
            return None
        changed_ms = parse_sap_timestamp(row.get(spec.change_field)) if spec.change_field else None
        created_ms = parse_sap_timestamp(row.get(spec.created_at_field)) if spec.created_at_field else None
        common: Dict[str, Any] = {
            "org_id": self.data_entities_processor.org_id,
            "record_name": record_title(spec, row),
            "record_type": RecordType(spec.record_type),
            "record_group_type": RecordGroupType(spec.record_group_type),
            "external_record_id": record_external_id(spec, keys),
            "external_record_group_id": record_group_external_id(spec),
            "external_revision_id": str(changed_ms) if changed_ms is not None else None,
            "version": 0,
            "origin": OriginTypes.CONNECTOR,
            "connector_name": self.connector_name,
            "connector_id": self.connector_id,
            "mime_type": MimeTypes.MARKDOWN.value,
            "weburl": record_web_url(self.base_url, spec, keys, self.fiori_base_url, self.sap_client),
            "source_created_at": created_ms,
            "source_updated_at": changed_ms,
            "inherit_permissions": False,
            "preview_renderable": False,
        }
        if spec.name == "product":
            marked = row.get("IsMarkedForDeletion")
            return ProductRecord(
                **common,
                product_code=keys[0],
                product_family=row.get("ProductGroup") or None,
                is_active=(not bool(marked)) if marked is not None else None,
                sku=row.get("ProductOldID") or None,
                list_price=None,
            )
        return Record(**common)

    async def _build_attachment_records(
        self, spec: EntitySpec, row: Dict[str, Any], permissions: List[Permission]
    ) -> List[Tuple[Record, List[Permission]]]:
        """DMS originals of one document become FILE children (same ACL, streamed via ``$value``)."""
        keys = row_key_values(spec, row)
        if not keys:
            return []
        url = attachment_list_url(self.base_url, spec, keys)
        if not url:
            return []
        try:
            page = parse_page(await self._get_json(url), spec.odata_version)
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (HttpStatusCode.NOT_FOUND.value, HttpStatusCode.FORBIDDEN.value):
                self.logger.debug("SAP attachments unavailable for %s (%s)", record_external_id(spec, keys), e.response.status_code)
                return []
            raise
        out: List[Tuple[Record, List[Permission]]] = []
        for entry in page.rows:
            att_key = attachment_key(entry)
            filename = entry.get("FileName")
            if not att_key or not filename:
                continue
            changed_ms = parse_sap_timestamp(entry.get("LastChangedDateTime") or entry.get("CreationDateTime"))
            out.append((
                FileRecord(
                    org_id=self.data_entities_processor.org_id,
                    record_name=str(filename),
                    record_type=RecordType.FILE,
                    record_group_type=RecordGroupType(spec.record_group_type),
                    external_record_id=attachment_external_id(spec, keys, att_key),
                    external_record_group_id=record_group_external_id(spec),
                    parent_external_record_id=record_external_id(spec, keys),
                    external_revision_id=str(changed_ms) if changed_ms is not None else None,
                    version=0,
                    origin=OriginTypes.CONNECTOR,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=entry.get("MimeType") or MimeTypes.BIN.value,
                    size_in_bytes=_as_int(entry.get("FileSize")),
                    weburl=attachment_content_url(self.base_url, entry),
                    source_created_at=parse_sap_timestamp(entry.get("CreationDateTime")),
                    source_updated_at=changed_ms,
                    inherit_permissions=False,
                    is_file=True,
                    extension=file_extension(str(filename)),
                ),
                list(permissions),
            ))
        return out

    # ------------------------------------------------------------------
    # Streaming / reindex
    # ------------------------------------------------------------------

    async def get_signed_url(self, record: Record) -> Optional[str]:
        return None  # SAP has no pre-signed download URLs; content is streamed.

    async def stream_record(self, record: Record, user_id: Optional[str] = None, convertTo: Optional[str] = None) -> StreamingResponse:
        entity, keys, att_key = split_external_id(record.external_record_id)
        spec = ENTITY_SPECS.get(entity)
        if spec is None:
            raise ValueError(f"Unsupported SAP record kind: {entity}")
        if att_key is not None:
            # ``weburl`` holds the exact ``AttachmentContentSet(...)/$value`` URL built at sync time.
            if not record.weburl:
                raise ValueError("SAP attachment record has no content URL")
            response = await self._get_raw(record.weburl, accept="*/*")
            return create_stream_record_response(
                _bytes_stream(response.content),
                filename=record.record_name,
                mime_type=response.headers.get("Content-Type") or record.mime_type,
                fallback_filename=f"record_{record.id}",
            )
        row = await self._fetch_row(spec, keys)
        if row is None:
            markdown = f"# {record.record_name}\n\nThis SAP document no longer exists or is not visible to the technical user.\n"
        else:
            markdown, _ = render_record_markdown(spec, row, self.base_url, self.fiori_base_url, self.sap_client)
        return create_stream_record_response(
            _bytes_stream(markdown.encode("utf-8")),
            filename=f"{record.record_name}.md",
            mime_type=MimeTypes.MARKDOWN.value,
            fallback_filename=f"record_{record.id}.md",
        )

    async def reindex_records(self, record_results: List[Record]) -> None:
        """Rebuild records whose change timestamp moved past the stored revision; reindex the rest as-is."""
        if not record_results:
            return
        unchanged: List[Record] = []
        for record in record_results:
            try:
                entity, keys, att_key = split_external_id(record.external_record_id)
            except ValueError:
                unchanged.append(record)
                continue
            spec = ENTITY_SPECS.get(entity)
            if spec is None or att_key is not None:
                unchanged.append(record)
                continue
            row = await self._fetch_row(spec, keys)
            if row is None:
                self.logger.warning("SAP record %s no longer exists; reindexing stored copy", record.external_record_id)
                unchanged.append(record)
                continue
            changed_ms = parse_sap_timestamp(row.get(spec.change_field)) if spec.change_field else None
            stored = _as_int(record.external_revision_id)
            if changed_ms is not None and (stored is None or changed_ms > stored):
                rebuilt = self._build_record(spec, row)
                if rebuilt is not None:
                    await self.data_entities_processor.on_record_content_update(rebuilt)
                    continue
            unchanged.append(record)
        if unchanged:
            await self.data_entities_processor.reindex_existing_records(unchanged)

    # ------------------------------------------------------------------
    # Webhooks / filters
    # ------------------------------------------------------------------

    async def handle_webhook_notification(self, notification: Dict) -> bool:
        """SAP event mesh / business events are not wired yet; acknowledge like Outlook does."""
        return True

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> FilterOptionsResponse:
        needle = (search or "").strip().lower()
        if filter_key == ENTITIES_FILTER_KEY:
            options = [
                FilterOption(id=name, label=ENTITY_FILTER_LABELS[name])
                for name in ALL_FILTER_ENTITIES
                if not needle or needle in name or needle in ENTITY_FILTER_LABELS[name].lower()
            ]
        elif filter_key in (COMPANY_CODES_FILTER_KEY, SALES_ORGS_FILTER_KEY):
            kind = SCOPE_COMPANY_CODE if filter_key == COMPANY_CODES_FILTER_KEY else SCOPE_SALES_ORG
            codes: set[str] = set(self._seen_codes.get(kind, set()))
            if self._auth_ctx is not None:
                codes.update(self._auth_ctx.mapping.codes_for(kind))
            else:
                with contextlib.suppress(ValueError):
                    codes.update(parse_authorization_mapping(self._sync_config.get("authorization_mapping")).codes_for(kind))
            options = [FilterOption(id=c, label=c) for c in sorted(codes) if not needle or needle in c.lower()]
        else:
            raise ValueError(f"Unsupported filter key: {filter_key}")
        start = max(page - 1, 0) * limit
        chunk = options[start:start + limit]
        return FilterOptionsResponse(success=True, options=chunk, page=page, limit=limit, has_more=start + limit < len(options))


class EntraGroupResolver(EntraGraphClient):
    """Expands ``group:<display name or object id>`` into member e-mails via Microsoft Graph
    (app-only client credentials; needs ``GroupMember.Read.All`` + ``User.Read.All``).

    Members are identified by their Entra primary address (``graph_user_email``) so the
    result matches the addresses people sign in to Edrak with.  Results are cached for
    the resolver's lifetime (= one connector instance / sync).  Unknown groups resolve to
    ``None`` so the caller can fall back to inline members.
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str, logger: Logger) -> None:
        super().__init__(tenant_id, client_id, client_secret, logger)
        self._cache: Dict[str, Optional[List[str]]] = {}

    async def _group_id(self, name_or_id: str) -> Optional[str]:
        if _GUID_RE.match(name_or_id):
            try:
                await self._get(f"{GRAPH_BASE_URL}/groups/{name_or_id}", {"$select": "id"})
                return name_or_id
            except httpx.HTTPStatusError as e:
                if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                    return None
                raise
        escaped = name_or_id.replace("'", "''")
        payload = await self._get(
            f"{GRAPH_BASE_URL}/groups",
            {"$filter": f"displayName eq '{escaped}'", "$select": "id,displayName", "$top": "2"},
        )
        matches = [g for g in payload.get("value") or [] if isinstance(g, dict) and g.get("id")]
        if not matches:
            return None
        if len(matches) > 1:
            self._logger.warning("Entra: %d groups named %r; using %s", len(matches), name_or_id, matches[0]["id"])
        return str(matches[0]["id"])

    async def _members(self, group_id: str) -> List[str]:
        emails: List[str] = []
        url: Optional[str] = f"{GRAPH_BASE_URL}/groups/{group_id}/transitiveMembers/microsoft.graph.user"
        params: Optional[Dict[str, str]] = {"$select": USER_EMAIL_SELECT, "$top": "999"}
        while url:
            payload = await self._get(url, params)
            for user in payload.get("value") or []:
                if not isinstance(user, dict) or user.get("accountEnabled") is False:
                    continue
                email = graph_user_email(user)
                if email:
                    emails.append(email)
            url = payload.get("@odata.nextLink")
            params = None
        return sorted(set(emails))

    async def resolve(self, name_or_id: str) -> Optional[List[str]]:
        key = name_or_id.strip()
        if key in self._cache:
            return self._cache[key]
        result: Optional[List[str]] = None
        try:
            group_id = await self._group_id(key)
            if group_id:
                result = await self._members(group_id)
        except httpx.HTTPStatusError as e:
            self._logger.error("Entra lookup for group %r failed (HTTP %s)", key, e.response.status_code)
        except (httpx.TransportError, KeyError, ValueError) as e:
            self._logger.error("Entra lookup for group %r failed: %s", key, e)
        self._cache[key] = result
        return result

    async def resolve_many(self, names: Sequence[str]) -> Dict[str, Optional[List[str]]]:
        unique = list(dict.fromkeys(n.strip() for n in names if n and n.strip()))
        results = await asyncio.gather(*(self.resolve(n) for n in unique))
        return dict(zip(unique, results))


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


async def _bytes_stream(data: bytes) -> AsyncGenerator[bytes, None]:
    yield data
