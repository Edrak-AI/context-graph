"""Microsoft Dynamics 365 connector (Dataverse Web API, app-only auth).

Indexes CRM rows (accounts, contacts, leads, opportunities, cases, notes) as
records and mirrors Dataverse ownership / security roles / shares as permission
edges so permission-aware search only surfaces rows the user could open in
Dynamics.  The mapping itself lives in ``mapping.py`` (pure, unit-tested); this
module owns auth, HTTP, paging and the ``BaseConnector`` lifecycle.

Auth: Entra ID client-credentials (``ClientSecretCredential`` like OneDrive)
for the scope ``<environmentUrl>/.default``.  Dynamics CRM only exposes the
*delegated* ``user_impersonation`` permission, so for app-only access the app
registration must additionally be added as an **Application User** in the
Dataverse environment (Power Platform admin center → Environments → Settings →
Users + permissions → Application users) and given a security role that can
read the synced tables plus ``systemuser``, ``team``, ``businessunit``,
``role`` and ``principalobjectaccess``.  ``init()`` validates with ``WhoAmI``.

Deletes: each entity set is pulled once with ``Prefer: odata.track-changes`` and
the returned ``@odata.deltaLink`` is stored in the entity's sync point; later
incremental syncs GET the delta link and receive upserts plus ``reason: deleted``
entries, which are removed through ``on_records_deleted_cascade`` (the record's
ACL edges and any attachment child go with it).  Tables without
``ChangeTrackingEnabled`` (HTTP 400 / ``0x80060888``) fall back to the
``modifiedon`` incremental filter plus a periodic full reconcile that prunes
records no longer returned by Dataverse (``reconcile_interval_hours`` filter,
default 24h).  The decision logic lives in ``change_tracking.py`` (pure).

Shares: ``principalobjectaccess`` is re-read every run, but sharing a record changes
neither its ``modifiedon`` nor the delta feed.  A per-record digest of the share list
(``mapping.share_digests``) is kept in the entity's sync point; records whose digest
moved and that the incremental pull did not already cover are re-fetched by primary
key and re-processed so their permission edges follow the share.

Identity: ``systemuser`` rows are matched to platform accounts by the person's
Entra primary address (Graph ``getByIds`` via ``EntraUserEmailResolver``, needs the
application permission ``User.Read.All``) and only fall back to the address stored
in Dataverse (``internalemailaddress`` / UPN) when Graph is not permitted — the
Dataverse copy is frequently the ``@<tenant>.onmicrosoft.com`` UPN, which nobody
signs in to Edrak with.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
from logging import Logger
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

import httpx
from azure.identity.aio import ClientSecretCredential
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
from app.connectors.sources.microsoft.common.apps import MicrosoftDynamics365App
from app.connectors.sources.microsoft.common.change_notifications import (
    load_webhook_settings,
)
from app.connectors.sources.microsoft.common.entra_identity import (
    EntraUserEmailResolver,
)
from app.connectors.sources.microsoft.dynamics365.change_tracking import (
    DEFAULT_RECONCILE_INTERVAL_HOURS,
    FIELD_DELTA_LINK,
    RECONCILE_INTERVAL_FILTER_KEY,
    TRACK_CHANGES_PREFERENCE,
    ChangeTrackingStatus,
    DeltaPage,
    EntitySyncState,
    SyncMode,
    is_change_tracking_disabled_error,
    missing_record_ids,
    parse_delta_page,
    plan_sync,
    prune_is_safe,
    resolve_reconcile_interval_hours,
    row_in_modified_bounds,
    seen_external_ids,
)
from app.connectors.sources.microsoft.dynamics365.mapping import (
    ATTACHMENT_ID_PREFIX,
    DATAVERSE_PAGE_SIZE,
    DEFAULT_ENTITY_ORDER,
    ENTITIES_FILTER_KEY,
    ENTITY_SPECS,
    PRIMARY_ID_FILTER_BATCH_SIZE,
    STATE_LOST_OR_CANCELLED,
    STATE_WON_OR_RESOLVED,
    EntitySpec,
    GrantEntity,
    GrantRole,
    PermissionGrant,
    RoleCopy,
    SecurityContext,
    api_base_url,
    attachment_external_id,
    bu_entity_group_external_id,
    bu_group_external_id,
    build_modified_filter,
    build_primary_id_filter,
    changed_share_record_ids,
    derive_grants,
    display_value,
    entity_list_web_url,
    entity_readers,
    global_read_group_external_id,
    index_shares,
    is_application_user,
    normalize_environment_url,
    owner_reference,
    parse_dataverse_timestamp,
    read_privilege_name,
    record_external_id,
    record_title,
    record_web_url,
    render_record_markdown,
    resolve_selected_entities,
    role_external_id,
    share_digests,
    split_external_id,
    system_administrator_match,
    systemuser_email,
    team_group_external_id,
    token_scope,
)
from app.connectors.sources.microsoft.dynamics365.webhooks import (
    DataverseWebhookRegistrar,
)
from app.models.entities import (
    AppRole,
    AppUser,
    AppUserGroup,
    DealRecord,
    FileRecord,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    TicketRecord,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.utils.streaming import create_stream_record_response
from app.utils.time_conversion import get_epoch_timestamp_in_ms

CONNECTOR_KEY = "microsoftdynamics365"  # ConnectorFactory registry key / filters config name
USERS_SYNC_POINT_KEY = "users"
SECURITY_SYNC_POINT_KEY = "security"
ENTITY_SYNC_POINT_PREFIX = "entity"

_MAX_HTTP_RETRIES = 5
_RETRY_STATUS = {HttpStatusCode.TOO_MANY_REQUESTS.value, 502, 503, 504}
_TOKEN_REFRESH_SKEW_S = 120
_MAX_CONCURRENT_REQUESTS = 4
_KNOWN_RECORDS_PAGE_SIZE = 500
_WEBHOOK_REVERIFY_MS = 6 * 60 * 60 * 1000
_DELETE_BATCH_SIZE = 100

_GRANT_ROLE_TO_PERMISSION = {
    GrantRole.READER: PermissionType.READ,
    GrantRole.WRITER: PermissionType.WRITE,
    GrantRole.OWNER: PermissionType.OWNER,
}
_GRANT_ENTITY_TO_PERMISSION = {
    GrantEntity.USER: EntityType.USER,
    GrantEntity.GROUP: EntityType.GROUP,
    GrantEntity.ROLE: EntityType.ROLE,
}


def _entity_sync_point_key(spec: EntitySpec) -> str:
    return f"{ENTITY_SYNC_POINT_PREFIX}/{spec.logical_name}"


def grants_to_permissions(grants: List[PermissionGrant]) -> List[Permission]:
    """Convert connector-agnostic grants (mapping.py) into graph ``Permission`` objects."""
    permissions: List[Permission] = []
    for grant in grants:
        permissions.append(
            Permission(
                entity_type=_GRANT_ENTITY_TO_PERMISSION[grant.entity_type],
                type=_GRANT_ROLE_TO_PERMISSION[grant.role],
                external_id=grant.external_id,
                email=grant.email,
            )
        )
    return permissions


@ConnectorBuilder("Microsoft Dynamics 365")\
    .in_group("Microsoft 365")\
    .with_description(
        "Sync accounts, contacts, leads, opportunities, cases and notes from "
        "Microsoft Dynamics 365 (Dataverse) with ownership- and role-based permissions"
    )\
    .with_categories(["CRM", "Sales"])\
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
                description="The Directory (Tenant) ID of the Entra ID tenant that hosts Dynamics 365",
            ),
            AuthField(
                name="environmentUrl",
                display_name="Dynamics 365 Environment URL",
                placeholder="https://org.crm4.dynamics.com",
                description=(
                    "The Dataverse environment URL (no path) of any region, e.g. "
                    "https://org.crm4.dynamics.com (Europe) or the host shown in the Power Platform "
                    "admin center for the Saudi Arabia region. The app registration must be added "
                    "as an Application User in this environment with a security role that can read "
                    "the synced tables, users, teams, business units, roles and shares. Enable "
                    "'Track changes' on the synced tables so deleted rows are detected promptly."
                ),
                field_type="URL",
                max_length=2048,
            ),
            AuthField(
                name="hasAdminConsent",
                display_name="Application User configured",
                description=(
                    "Confirm the app registration has been added as an Application User in the "
                    "Dynamics 365 environment and assigned a security role. Also grant it the "
                    "Microsoft Graph application permission User.Read.All (admin consent) so people "
                    "are matched by their primary e-mail address rather than the address stored in "
                    "Dynamics; without it the Dynamics address is used."
                ),
                field_type="CHECKBOX",
                required=True,
                default_value=False,
            ),
        ])
    ])\
    .with_info(CONNECTOR_EMAIL_IDENTITY_INFO)\
    .configure(lambda builder: builder
        .with_icon(IconPaths.connector_icon(Connectors.MICROSOFT_DYNAMICS_365.value))
        .add_documentation_link(DocumentationLink(
            "Register an app for Dataverse (server-to-server)",
            "https://learn.microsoft.com/power-apps/developer/data-platform/use-single-tenant-server-server-authentication",
            "setup",
        ))
        .add_documentation_link(DocumentationLink(
            "Manage application users in the Power Platform admin center",
            "https://learn.microsoft.com/power-platform/admin/manage-application-users",
            "setup",
        ))
        .add_filter_field(FilterField(
            name=ENTITIES_FILTER_KEY,
            display_name="Entities",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Dynamics 365 tables to sync. Leave empty to sync all supported tables.",
            options=list(DEFAULT_ENTITY_ORDER),
            option_source_type=OptionSourceType.STATIC,
            default_value=list(DEFAULT_ENTITY_ORDER),
        ))
        .add_filter_field(CommonFields.modified_date_filter(
            "Only sync rows whose modifiedon falls in this range."
        ))
        .add_filter_field(FilterField(
            name=RECONCILE_INTERVAL_FILTER_KEY,
            display_name="Delete reconcile interval (hours)",
            filter_type=FilterType.NUMBER,
            category=FilterCategory.SYNC,
            description=(
                "Deleted rows are normally detected through Dataverse change tracking. For tables "
                "where change tracking is not enabled, the connector instead re-reads the whole "
                "table every N hours and removes records that no longer exist. Default 24; "
                "0 disables the reconcile (deletes on such tables are then never detected)."
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
class MicrosoftDynamics365Connector(BaseConnector):
    """Dataverse Web API connector. See module docstring and ``mapping.py``."""

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
            MicrosoftDynamics365App(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_name = Connectors.MICROSOFT_DYNAMICS_365

        def _sync_point(kind: SyncDataPointType) -> SyncPoint:
            return SyncPoint(
                connector_id=self.connector_id,
                org_id=self.data_entities_processor.org_id,
                sync_data_point_type=kind,
                data_store_provider=self.data_store_provider,
            )

        self.user_sync_point = _sync_point(SyncDataPointType.USERS)
        self.records_sync_point = _sync_point(SyncDataPointType.RECORDS)

        self.environment_url: str = ""
        self._tenant_id: str = ""
        self._client_id: str = ""
        self._client_secret: str = ""
        self.credential: Optional[ClientSecretCredential] = None
        self._entra_users: Optional[EntraUserEmailResolver] = None
        self._http: Optional[httpx.AsyncClient] = None
        self._token: Optional[str] = None
        self._token_expires_on: int = 0
        self._request_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REQUESTS)

        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self._security: Optional[SecurityContext] = None
        self._shares_unavailable_logged = False
        # receiver URL the Dataverse webhook was last verified against (per process)
        self._webhooks_verified_for: str | None = None
        self._webhooks_verified_at_ms: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> bool:
        config = await self.config_service.get_config(f"/services/connectors/{self.connector_id}/config")
        if not config:
            self.logger.error("Microsoft Dynamics 365 config not found")
            return False
        auth = config.get("auth", {}) or {}
        tenant_id = auth.get("tenantId")
        client_id = auth.get("clientId")
        client_secret = auth.get("clientSecret")
        environment_url = auth.get("environmentUrl")
        if not all((tenant_id, client_id, client_secret, environment_url)):
            raise ConnectorInitError(
                "Incomplete Microsoft Dynamics 365 credentials. tenantId, clientId, clientSecret "
                "and environmentUrl are all required."
            )
        try:
            self.environment_url = normalize_environment_url(environment_url)
        except ValueError as e:
            raise ConnectorInitError(str(e)) from e

        await self._close_http()
        self._tenant_id, self._client_id, self._client_secret = tenant_id, client_id, client_secret
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        self._http = httpx.AsyncClient(
            base_url=api_base_url(self.environment_url),
            timeout=httpx.Timeout(90.0, connect=15.0),
            headers={
                "Accept": "application/json",
                "OData-MaxVersion": "4.0",
                "OData-Version": "4.0",
            },
        )
        try:
            await self._refresh_token()
            who = await self._get_json("WhoAmI")
        except ConnectorInitError:
            raise
        except Exception as e:
            await self._close_http()
            raise ConnectorInitError(
                "Could not authenticate to Dynamics 365. Check tenantId/clientId/clientSecret, "
                "the environment URL, and that the app registration is an Application User with "
                f"a security role in this environment. ({type(e).__name__}: {str(e)[:200]})"
            ) from e
        self.logger.info(
            "Dynamics 365 connector initialised for %s (application user %s, business unit %s)",
            self.environment_url, who.get("UserId"), who.get("BusinessUnitId"),
        )
        return True

    async def test_connection_and_access(self) -> bool:
        try:
            who = await self._get_json("WhoAmI")
            return bool(who.get("UserId"))
        except Exception as e:
            self.logger.error("Dynamics 365 connection test failed: %s", e)
            return False

    async def cleanup(self) -> None:
        await self._close_http()
        self._security = None

    def _webhook_registrar(self, notification_url: str, header_value: str, tables: list[str]) -> DataverseWebhookRegistrar:
        return DataverseWebhookRegistrar(
            get_json=self._get_json,
            send_json=self._send_json,
            connector_id=self.connector_id,
            notification_url=notification_url,
            header_value=header_value,
            logger=self.logger,
            tables=tables,
        )

    async def _sync_change_notifications(self, specs: list[EntitySpec]) -> None:
        """Ensure the Dataverse webhook (service endpoint + steps) for the synced tables; never raises.
        Re-verified once per process and then every ``_WEBHOOK_REVERIFY_MS``."""
        try:
            settings = await load_webhook_settings(self.config_service, self.connector_id, "dataverse", self.logger)
            if settings is None:
                return
            now_ms = get_epoch_timestamp_in_ms()
            if (
                self._webhooks_verified_for == settings.notification_url
                and now_ms - self._webhooks_verified_at_ms < _WEBHOOK_REVERIFY_MS
            ):
                return
            registrar = self._webhook_registrar(
                settings.notification_url, settings.client_state, [spec.logical_name for spec in specs]
            )
            if await registrar.ensure() is not None:
                self._webhooks_verified_for = settings.notification_url
                self._webhooks_verified_at_ms = now_ms
        except Exception as e:
            self.logger.warning("Dataverse webhook upkeep failed for connector %s: %s", self.connector_id, e)

    async def remove_change_notifications(self) -> None:
        try:
            settings = await load_webhook_settings(self.config_service, self.connector_id, "dataverse", self.logger)
            if settings is None:
                return
            await self._webhook_registrar(settings.notification_url, settings.client_state, []).remove_all()
        except Exception as e:
            self.logger.warning("Could not remove the Dataverse webhook for connector %s: %s", self.connector_id, e)
        self._webhooks_verified_for = None

    async def _close_http(self) -> None:
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._http.aclose()
            self._http = None
        if self.credential is not None:
            with contextlib.suppress(Exception):
                await self.credential.close()
            self.credential = None
        if self._entra_users is not None:
            await self._entra_users.close()
            self._entra_users = None
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
        if self.credential is None:
            raise RuntimeError("Dynamics 365 connector not initialised")
        token = await self.credential.get_token(token_scope(self.environment_url))
        self._token = token.token
        self._token_expires_on = int(token.expires_on)
        return self._token

    async def _get_token(self) -> str:
        now_s = get_epoch_timestamp_in_ms() // 1000
        if self._token and now_s < self._token_expires_on - _TOKEN_REFRESH_SKEW_S:
            return self._token
        return await self._refresh_token()

    async def _get_json(
        self,
        path_or_url: str,
        params: Optional[Dict[str, str]] = None,
        page_size: Optional[int] = None,
        include_annotations: bool = False,
        track_changes: bool = False,
    ) -> Dict[str, Any]:
        """GET with bearer auth, 401 re-auth, and bounded retry on 429/5xx (honours Retry-After)."""
        if self._http is None:
            raise RuntimeError("Dynamics 365 connector not initialised")
        prefer: List[str] = []
        if page_size:
            prefer.append(f"odata.maxpagesize={page_size}")
        if include_annotations:
            prefer.append('odata.include-annotations="*"')
        if track_changes:
            prefer.append(TRACK_CHANGES_PREFERENCE)
        headers: Dict[str, str] = {}
        if prefer:
            headers["Prefer"] = ",".join(prefer)

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
                    self.logger.warning("Dynamics 365 request failed (%s), retrying in %.1fs", e, delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
                    continue
            if response.status_code == HttpStatusCode.UNAUTHORIZED.value and not refreshed:
                refreshed = True
                await self._refresh_token()
                continue
            if response.status_code in _RETRY_STATUS and attempt < _MAX_HTTP_RETRIES:
                retry_after = response.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else delay
                except ValueError:
                    wait = delay
                wait = min(max(wait, 0.5), 60.0)
                self.logger.warning(
                    "Dynamics 365 returned %s for %s, retrying in %.1fs", response.status_code, path_or_url, wait
                )
                await asyncio.sleep(wait)
                delay = min(delay * 2, 30.0)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Dynamics 365 request to {path_or_url} exhausted retries")

    async def _send_json(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Write request (POST/PATCH/DELETE) with bearer auth and one 401 re-auth; returns (status, JSON object)."""
        if self._http is None:
            raise RuntimeError("%s connector not initialised" % self.connector_name.value)
        request_headers: dict[str, str] = {"Content-Type": "application/json", "Prefer": "return=representation"}
        request_headers.update(headers or {})
        refreshed = False
        while True:
            request_headers["Authorization"] = f"Bearer {await self._get_token()}"
            async with self._request_semaphore:
                response = await self._http.request(method, path, json=body, headers=request_headers)
            if response.status_code == HttpStatusCode.UNAUTHORIZED.value and not refreshed:
                refreshed = True
                await self._refresh_token()
                continue
            payload = _response_body(response)
            return response.status_code, payload if isinstance(payload, dict) else {}

    async def _iter_pages(
        self,
        entity_set: str,
        params: Optional[Dict[str, str]] = None,
        page_size: int = DATAVERSE_PAGE_SIZE,
        include_annotations: bool = False,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Follow ``@odata.nextLink`` (an absolute URL that already carries the query)."""
        payload = await self._get_json(entity_set, params=params, page_size=page_size, include_annotations=include_annotations)
        while True:
            yield list(payload.get("value") or [])
            next_link = payload.get("@odata.nextLink")
            if not next_link:
                return
            payload = await self._get_json(next_link, page_size=page_size, include_annotations=include_annotations)

    async def _iter_delta_pages(
        self, spec: EntitySpec, path_or_url: str, params: Optional[Dict[str, str]] = None
    ) -> AsyncGenerator[DeltaPage, None]:
        """Tracked pull / delta follow-up: every request (incl. ``@odata.nextLink`` pages)
        carries ``Prefer: odata.track-changes``; the last page carries the delta link."""
        payload = await self._get_json(
            path_or_url, params=params, page_size=DATAVERSE_PAGE_SIZE, include_annotations=True, track_changes=True
        )
        while True:
            page = parse_delta_page(payload, spec)
            yield page
            if not page.next_link:
                return
            payload = await self._get_json(
                page.next_link, page_size=DATAVERSE_PAGE_SIZE, include_annotations=True, track_changes=True
            )

    async def _fetch_all(self, entity_set: str, params: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        async for page in self._iter_pages(entity_set, params=params):
            rows.extend(page)
        return rows

    async def _fetch_row(self, spec: EntitySpec, row_id: str) -> Optional[Dict[str, Any]]:
        try:
            return await self._get_json(
                f"{spec.entity_set}({row_id})",
                params={"$select": ",".join(spec.select_fields)},
                include_annotations=True,
            )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                return None
            raise

    # ------------------------------------------------------------------
    # Sync entry points
    # ------------------------------------------------------------------

    async def run_sync(self) -> None:
        """Full sync: users → teams/BUs → roles → every selected entity (all rows)."""
        await self._run(incremental=False)

    async def run_incremental_sync(self) -> None:
        """Incremental: same pipeline; each entity set follows its stored delta link
        (upserts + deletes) or, without change tracking, ``modifiedon gt <last sync>``
        plus the periodic full reconcile.  See ``change_tracking.plan_sync``."""
        await self._run(incremental=True)

    async def _run(self, incremental: bool) -> None:
        if self._http is None:
            self.logger.error("Dynamics 365 connector not initialised")
            return
        self.logger.info("Starting Dynamics 365 %s sync", "incremental" if incremental else "full")
        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, CONNECTOR_KEY, self.connector_id, self.logger
        )
        specs = resolve_selected_entities(self._selected_entity_names())

        self._security = await self._sync_security_model(specs)
        await self._sync_record_groups(specs)
        for spec in specs:
            await self._sync_entity(spec, incremental=incremental)
        self.logger.info("Dynamics 365 sync completed")
        await self._sync_change_notifications(specs)

    def _selected_entity_names(self) -> Optional[List[str]]:
        entity_filter = self.sync_filters.get(ENTITIES_FILTER_KEY) if self.sync_filters else None
        if entity_filter is None or entity_filter.is_empty():
            return None
        return [str(v) for v in entity_filter.as_list()]

    def _modified_bounds(self) -> Tuple[Optional[int], Optional[int]]:
        modified = self.sync_filters.get(SyncFilterKey.MODIFIED) if self.sync_filters else None
        if modified is None or modified.is_empty():
            return None, None
        return modified.get_datetime_start(), modified.get_datetime_end()

    # ------------------------------------------------------------------
    # Users, teams, business units, roles
    # ------------------------------------------------------------------

    async def _official_identities(self, rows: List[Dict[str, Any]]) -> Dict[str, Tuple[str, List[str]]]:
        """Entra object id → (primary e-mail, alternates) for one page of ``systemuser`` rows; ``{}`` when Graph is not permitted."""
        ids = [str(r["azureactivedirectoryobjectid"]) for r in rows if r.get("azureactivedirectoryobjectid")]
        if not ids or not all((self._tenant_id, self._client_id, self._client_secret)):
            return {}
        if self._entra_users is None:
            self._entra_users = EntraUserEmailResolver(self._tenant_id, self._client_id, self._client_secret, self.logger)
        return await self._entra_users.resolve_identities(ids)

    async def _role_privileges(self, role_id: str) -> List[Dict[str, Any]]:
        """``RetrieveRolePrivilegesRole`` of one role copy; unreadable copies grant nothing."""
        try:
            payload = await self._get_json(f"roles({role_id})/Microsoft.Dynamics.CRM.RetrieveRolePrivilegesRole()")
        except httpx.HTTPStatusError as e:
            self.logger.warning("Could not read privileges of role copy %s: %s", role_id, e.response.status_code)
            return []
        return list(payload.get("RolePrivileges") or [])

    async def _sync_security_model(self, specs: List[EntitySpec]) -> SecurityContext:
        ctx = SecurityContext()
        org_id = self.data_entities_processor.org_id

        # 1. systemusers → AppUser. Disabled users are upserted (inactive) but join no
        #    membership and resolve no owner, so nothing they held leaks to them.
        active_users: Dict[str, AppUser] = {}
        bu_members: Dict[str, List[AppUser]] = {}
        synced = disabled = remapped = 0
        async for page in self._iter_pages(
            "systemusers",
            params={
                "$select": "systemuserid,fullname,internalemailaddress,domainname,"
                           "azureactivedirectoryobjectid,isdisabled,applicationid,"
                           "_businessunitid_value,createdon,modifiedon",
            },
        ):
            rows = [row for row in page if not is_application_user(row)]
            official = await self._official_identities(rows)
            batch: List[AppUser] = []
            for row in rows:
                user_id = row.get("systemuserid")
                stored = systemuser_email(row)
                primary, alternates = official.get(str(row.get("azureactivedirectoryobjectid") or ""), (None, []))
                email = primary or stored
                if not email or not user_id:
                    continue
                if stored and email != stored:
                    remapped += 1
                app_user = AppUser(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_user_id=str(user_id),
                    email=email,
                    # the Dataverse address is kept as an alternate so grants keyed on it still resolve
                    alternate_emails=[*alternates, stored] if stored else list(alternates),
                    full_name=row.get("fullname") or email,
                    org_id=org_id,
                    is_active=not bool(row.get("isdisabled")),
                    source_created_at=parse_dataverse_timestamp(row.get("createdon")),
                    source_updated_at=parse_dataverse_timestamp(row.get("modifiedon")),
                )
                batch.append(app_user)
                synced += 1
                if not app_user.is_active:
                    disabled += 1
                    continue
                active_users[str(user_id)] = app_user
                ctx.user_email_by_id[str(user_id)] = email
                bu_id = row.get("_businessunitid_value")
                if bu_id:
                    bu_members.setdefault(str(bu_id), []).append(app_user)
            if batch:
                await self.data_entities_processor.on_new_app_users(batch)
        await self.user_sync_point.update_sync_point(USERS_SYNC_POINT_KEY, {"lastSyncTimestamp": get_epoch_timestamp_in_ms()})
        self.logger.info(
            "Synced %d Dynamics 365 users (%d disabled and excluded from permissions, %d matched by their "
            "Entra primary address rather than the Dataverse one)", synced, disabled, remapped,
        )

        # 2. businessunits → AppUserGroup (bu:<id>) of the active users whose home BU it is
        bu_rows = await self._fetch_all("businessunits", {"$select": "businessunitid,name,_parentbusinessunitid_value,createdon,modifiedon"})
        bu_names: Dict[str, str] = {}
        bu_groups: List[Tuple[AppUserGroup, List[AppUser]]] = []
        for row in bu_rows:
            bu_id = row.get("businessunitid")
            if not bu_id:
                continue
            ctx.known_business_unit_ids.add(str(bu_id))
            bu_names[str(bu_id)] = str(row.get("name") or bu_id)
            bu_groups.append((
                AppUserGroup(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_user_group_id=bu_group_external_id(str(bu_id)),
                    name=f"Business unit · {bu_names[str(bu_id)]}",
                    org_id=org_id,
                    description="Dynamics 365 business unit members",
                    source_created_at=parse_dataverse_timestamp(row.get("createdon")),
                    source_updated_at=parse_dataverse_timestamp(row.get("modifiedon")),
                ),
                bu_members.get(str(bu_id), []),
            ))
        if bu_groups:
            await self.data_entities_processor.on_new_user_groups(bu_groups)

        # 3. teams → AppUserGroup (team:<id>) with teammembership members
        team_rows = await self._fetch_all("teams", {"$select": "teamid,name,teamtype,_businessunitid_value,createdon,modifiedon"})
        team_members: Dict[str, List[AppUser]] = {}

        async def _load_team_members(team_id: str) -> None:
            member_rows = await self._fetch_all(f"teams({team_id})/teammembership_association", {"$select": "systemuserid"})
            team_members[team_id] = [
                active_users[str(m["systemuserid"])] for m in member_rows if str(m.get("systemuserid")) in active_users
            ]

        await asyncio.gather(*(_load_team_members(str(r["teamid"])) for r in team_rows if r.get("teamid")))
        team_groups: List[Tuple[AppUserGroup, List[AppUser]]] = []
        for row in team_rows:
            team_id = row.get("teamid")
            if not team_id:
                continue
            ctx.known_team_ids.add(str(team_id))
            team_groups.append((
                AppUserGroup(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_user_group_id=team_group_external_id(str(team_id)),
                    name=f"Team · {row.get('name') or team_id}",
                    org_id=org_id,
                    description="Dynamics 365 team",
                    source_created_at=parse_dataverse_timestamp(row.get("createdon")),
                    source_updated_at=parse_dataverse_timestamp(row.get("modifiedon")),
                ),
                team_members.get(str(team_id), []),
            ))
        if team_groups:
            await self.data_entities_processor.on_new_user_groups(team_groups)
        self.logger.info("Synced %d business units and %d teams", len(bu_groups), len(team_groups))

        # 4. roles: holders and privileges are loaded per business-unit copy (Dataverse
        #    evaluates the copy a user holds); the AppRole (role:<root>) unions the copies.
        role_rows = await self._fetch_all(
            "roles",
            {"$select": "roleid,name,roletemplateid,_businessunitid_value,_parentrootroleid_value,createdon,modifiedon"},
        )
        roles_by_ext: Dict[str, Dict[str, Any]] = {}
        copies_by_ext: Dict[str, List[str]] = {}
        for row in role_rows:
            if not row.get("roleid"):
                continue
            ext = role_external_id(row)
            copies_by_ext.setdefault(ext, []).append(str(row["roleid"]))
            # keep the root copy's row (or the first one seen) as the representative
            if ext not in roles_by_ext or not row.get("_parentrootroleid_value"):
                roles_by_ext[ext] = row

        holders_by_role_id: Dict[str, Dict[str, AppUser]] = {}
        privileges_by_role_id: Dict[str, List[Dict[str, Any]]] = {}

        async def _load_role_copy(role_id: str) -> None:
            holders: Dict[str, AppUser] = {}
            user_rows = await self._fetch_all(f"roles({role_id})/systemuserroles_association", {"$select": "systemuserid"})
            for m in user_rows:
                uid = str(m.get("systemuserid"))
                if uid in active_users:
                    holders[uid] = active_users[uid]
            team_role_rows = await self._fetch_all(f"roles({role_id})/teamroles_association", {"$select": "teamid"})
            for t in team_role_rows:
                for member in team_members.get(str(t.get("teamid")), []):
                    holders[member.source_user_id] = member
            holders_by_role_id[role_id] = holders
            privileges_by_role_id[role_id] = await self._role_privileges(role_id)

        await asyncio.gather(*(_load_role_copy(role_id) for copies in copies_by_ext.values() for role_id in copies))

        app_roles: List[Tuple[AppRole, List[AppUser]]] = []
        for ext, row in roles_by_ext.items():
            members: Dict[str, AppUser] = {}
            for role_id in copies_by_ext[ext]:
                members.update(holders_by_role_id[role_id])
            app_roles.append((
                AppRole(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_role_id=ext,
                    name=row.get("name") or ext,
                    org_id=org_id,
                    source_created_at=parse_dataverse_timestamp(row.get("createdon")),
                    source_updated_at=parse_dataverse_timestamp(row.get("modifiedon")),
                ),
                list(members.values()),
            ))
            matched_by = system_administrator_match(row)
            if matched_by:
                if ctx.system_admin_role_id:
                    self.logger.warning("Several roles look like System Administrator (%s, %s); keeping the first", ctx.system_admin_role_id, ext)
                    continue
                ctx.system_admin_role_id = ext
                self.logger.info("Dynamics 365 System Administrator role is %s (matched by %s)", ext, matched_by)
        if app_roles:
            await self.data_entities_processor.on_new_app_roles(app_roles)
        if ctx.system_admin_role_id is None:
            self.logger.warning("Dynamics 365 System Administrator role not found among %d roles; administrators get no blanket read", len(app_roles))

        # 5. per-table groups the record grants point at: globalread:<entity> and
        #    bu:<id>:<entity> (BU members ∩ users with a read-privileged role copy)
        copies = [
            RoleCopy(role_id, frozenset(holders_by_role_id[role_id]), tuple(privileges_by_role_id[role_id]))
            for role_id in privileges_by_role_id
        ]
        entity_groups: List[Tuple[AppUserGroup, List[AppUser]]] = []
        for spec in specs:
            readers = entity_readers(copies, spec)
            entity_groups.append((
                AppUserGroup(
                    app_name=self.connector_name,
                    connector_id=self.connector_id,
                    source_user_group_id=global_read_group_external_id(spec),
                    name=f"Global read · {spec.display_name}",
                    org_id=org_id,
                    description=f"Dynamics 365 users whose security role reads every {spec.singular.lower()} ({read_privilege_name(spec)} Global)",
                ),
                [active_users[uid] for uid in sorted(readers.global_read)],
            ))
            for bu_id, bu_name in bu_names.items():
                entity_groups.append((
                    AppUserGroup(
                        app_name=self.connector_name,
                        connector_id=self.connector_id,
                        source_user_group_id=bu_entity_group_external_id(bu_id, spec),
                        name=f"Business unit · {bu_name} · {spec.display_name}",
                        org_id=org_id,
                        description=f"Dynamics 365 business unit members whose security role reads {spec.display_name.lower()}",
                    ),
                    [u for u in bu_members.get(bu_id, []) if u.source_user_id in readers.bu_read],
                ))
        if entity_groups:
            await self.data_entities_processor.on_new_user_groups(entity_groups)
        await self.records_sync_point.update_sync_point(SECURITY_SYNC_POINT_KEY, {"lastSyncTimestamp": get_epoch_timestamp_in_ms()})
        self.logger.info(
            "Synced %d Dynamics 365 security roles (%d business-unit copies) and %d per-table access groups",
            len(app_roles), len(copies), len(entity_groups),
        )
        return ctx

    async def _load_shares(self, spec: EntitySpec, ctx: SecurityContext) -> bool:
        """``principalobjectaccess`` rows for one table; skipped gracefully when the
        application user's role cannot read the POA table.  Returns whether shares
        were readable (a failed read must not be mistaken for "everything unshared")."""
        try:
            rows = await self._fetch_all(
                "principalobjectaccessset",
                {
                    "$select": "objectid,principalid,principaltypecode,accessrightsmask,inheritedaccessrightsmask",
                    "$filter": f"objecttypecode eq '{spec.logical_name}'",
                },
            )
        except httpx.HTTPStatusError as e:
            if not self._shares_unavailable_logged:
                self._shares_unavailable_logged = True
                self.logger.warning(
                    "Dynamics 365 shares (principalobjectaccess) unavailable (HTTP %s); explicit "
                    "record shares will not be mirrored. Grant the application user read on the "
                    "PrincipalObjectAccess table to enable them.", e.response.status_code,
                )
            ctx.shares_by_entity[spec.logical_name] = {}
            return False
        ctx.shares_by_entity[spec.logical_name] = index_shares(rows)
        return True

    # ------------------------------------------------------------------
    # Record groups and records
    # ------------------------------------------------------------------

    def _record_group_external_id(self, spec: EntitySpec) -> str:
        return f"d365:{spec.logical_name}"

    async def _sync_record_groups(self, specs: List[EntitySpec]) -> None:
        assert self._security is not None
        groups: List[Tuple[RecordGroup, List[Permission]]] = []
        for spec in specs:
            groups.append((
                RecordGroup(
                    name=f"Dynamics 365 · {spec.display_name}",
                    short_name=spec.display_name,
                    description=f"Microsoft Dynamics 365 {spec.display_name.lower()} ({spec.entity_set})",
                    external_group_id=self._record_group_external_id(spec),
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    group_type=RecordGroupType(spec.record_group_type),
                    web_url=entity_list_web_url(self.environment_url, spec),
                    org_id=self.data_entities_processor.org_id,
                ),
                grants_to_permissions(self._security.entity_wide_grants(spec)),
            ))
        if groups:
            await self.data_entities_processor.on_new_record_groups(groups)

    async def _sync_entity(self, spec: EntitySpec, incremental: bool) -> None:
        assert self._security is not None
        key = _entity_sync_point_key(spec)
        state = EntitySyncState.from_sync_point(await self.records_sync_point.read_sync_point(key))
        interval_hours = self._reconcile_interval_hours()
        sync_started_ms = get_epoch_timestamp_in_ms()
        mode = plan_sync(state, incremental=incremental, now_ms=sync_started_ms, interval_hours=interval_hours)
        start_ms, end_ms = self._modified_bounds()

        shares_available = await self._load_shares(spec, self._security)
        current_digests = share_digests(self._security.shares_by_entity.get(spec.logical_name, {}))

        if mode is SyncMode.DELTA:
            try:
                upserted, deleted, handled = await self._apply_delta(spec, state, start_ms, end_ms)
            except httpx.HTTPStatusError as e:
                if is_change_tracking_disabled_error(e.response.status_code, _response_body(e.response)):
                    self._mark_change_tracking_disabled(spec, state)
                else:
                    # Expired / invalid delta token: re-baseline. The full pull's reconcile
                    # catches the deletes that happened while the link was unusable.
                    self.logger.warning(
                        "Dynamics 365 delta link for %s rejected (HTTP %s); re-baselining with a full pull",
                        spec.entity_set, e.response.status_code,
                    )
                    state.delta_link = None
                mode = SyncMode.FULL_RECONCILE
            else:
                if shares_available:
                    await self._reapply_changed_shares(spec, state, current_digests, handled, start_ms, end_ms)
                state.last_sync_timestamp = sync_started_ms
                await self._save_entity_state(key, state)
                self.logger.info("Synced %d %s records, %d deleted (delta)", upserted, spec.display_name.lower(), deleted)
                return

        if mode is SyncMode.MODIFIED_ON:
            upserted, handled = await self._sync_entity_modified_on(spec, state.last_sync_timestamp, start_ms, end_ms)
            if shares_available:
                await self._reapply_changed_shares(spec, state, current_digests, handled, start_ms, end_ms)
            state.last_sync_timestamp = sync_started_ms
            await self._save_entity_state(key, state)
            self.logger.info("Synced %d %s records (incremental, modifiedon)", upserted, spec.display_name.lower())
            return

        upserted, deleted = await self._full_pull_and_reconcile(spec, state, start_ms, end_ms)
        # The full pull applied whatever shares were readable (none, when the POA read failed).
        state.share_digests = current_digests
        state.last_sync_timestamp = sync_started_ms
        state.last_reconcile_timestamp = sync_started_ms
        await self._save_entity_state(key, state)
        self.logger.info(
            "Synced %d %s records, %d deleted (full, change tracking %s)",
            upserted, spec.display_name.lower(), deleted, state.change_tracking.value,
        )

    async def _save_entity_state(self, key: str, state: EntitySyncState) -> None:
        await self.records_sync_point.update_sync_point(key, state.to_sync_point(), encrypt_fields=[FIELD_DELTA_LINK])

    def _reconcile_interval_hours(self) -> float:
        interval = self.sync_filters.get(RECONCILE_INTERVAL_FILTER_KEY) if self.sync_filters else None
        return resolve_reconcile_interval_hours(interval.get_value() if interval is not None else None)

    def _mark_change_tracking_disabled(self, spec: EntitySpec, state: EntitySyncState) -> None:
        state.mark_change_tracking_disabled()
        self.logger.warning(
            "Dynamics 365 table %s has change tracking disabled; deletes will only be detected by the "
            "full reconcile every %.1fh. Enable 'Track changes' on the table (Power Apps → Tables → %s → "
            "Properties → Advanced options) for delta-based delete detection.",
            spec.logical_name, self._reconcile_interval_hours(), spec.display_name,
        )

    async def _apply_delta(
        self, spec: EntitySpec, state: EntitySyncState, start_ms: int | None, end_ms: int | None
    ) -> tuple[int, int, set[str]]:
        """GET the stored delta link; upsert changed rows, delete ``reason: deleted`` ones,
        store the new delta link. Raises ``httpx.HTTPStatusError`` for the caller to classify.
        The returned set holds the source ids the delta covered (upserts and deletes)."""
        assert state.delta_link
        upserted = deleted = 0
        handled: set[str] = set()
        new_delta_link: Optional[str] = None
        async for page in self._iter_delta_pages(spec, state.delta_link):
            handled.update(_row_ids(spec, page.upserts))
            handled.update(page.deleted_ids)
            upserted += await self._process_rows(spec, page.upserts, start_ms, end_ms)
            deleted += await self._delete_by_source_ids(spec, page.deleted_ids)
            if page.delta_link:
                new_delta_link = page.delta_link
        if new_delta_link:
            state.mark_change_tracking_enabled(new_delta_link)
        else:
            self.logger.warning("Dynamics 365 delta response for %s carried no new delta link; will re-baseline", spec.entity_set)
            state.delta_link = None
        return upserted, deleted, handled

    async def _sync_entity_modified_on(
        self, spec: EntitySpec, since_ms: Optional[int], start_ms: Optional[int], end_ms: Optional[int]
    ) -> tuple[int, set[str]]:
        """Server-side ``modifiedon`` filter (no delete detection) for tables without change tracking."""
        params: Dict[str, str] = {"$select": ",".join(spec.select_fields), "$orderby": "modifiedon asc"}
        odata_filter = build_modified_filter(since_ms=since_ms, start_ms=start_ms, end_ms=end_ms)
        if odata_filter:
            params["$filter"] = odata_filter
        upserted = 0
        handled: set[str] = set()
        async for rows in self._iter_pages(spec.entity_set, params=params, include_annotations=True):
            handled.update(_row_ids(spec, rows))
            upserted += await self._process_rows(spec, rows, start_ms, end_ms)
        return upserted, handled

    async def _reapply_changed_shares(
        self,
        spec: EntitySpec,
        state: EntitySyncState,
        current_digests: dict[str, str],
        handled: set[str],
        start_ms: int | None,
        end_ms: int | None,
    ) -> int:
        """Sharing / unsharing a record does not touch its ``modifiedon`` and is invisible
        to change tracking, so records whose share list differs from the stored baseline
        are re-read by primary key and re-processed; rows the incremental pull already
        covered are skipped.  Ids that Dataverse no longer returns were deleted meanwhile."""
        changed = sorted(changed_share_record_ids(state.share_digests, current_digests) - handled)
        reapplied = 0
        for start in range(0, len(changed), PRIMARY_ID_FILTER_BATCH_SIZE):
            rows = await self._fetch_rows_by_id(spec, changed[start:start + PRIMARY_ID_FILTER_BATCH_SIZE])
            reapplied += len(rows)
            await self._process_rows(spec, rows, start_ms, end_ms)
        state.share_digests = current_digests
        self.logger.info("Re-applied grants for %d %s records whose shares changed", reapplied, spec.display_name.lower())
        return reapplied

    async def _fetch_rows_by_id(self, spec: EntitySpec, row_ids: list[str]) -> list[dict[str, Any]]:
        """Same ``$select`` and annotations as the regular pull, so the rows render identically."""
        odata_filter = build_primary_id_filter(spec, row_ids)
        if not odata_filter:
            return []
        params = {"$select": ",".join(spec.select_fields), "$filter": odata_filter}
        rows: list[dict[str, Any]] = []
        async for page in self._iter_pages(spec.entity_set, params=params, include_annotations=True):
            rows.extend(page)
        return rows

    async def _full_pull_and_reconcile(
        self, spec: EntitySpec, state: EntitySyncState, start_ms: Optional[int], end_ms: Optional[int]
    ) -> Tuple[int, int]:
        """Every row of the table (tracked when Dataverse allows it, so a delta link comes
        back), then prune graph records the pull did not return.

        The user's modified-date bounds are applied client-side here: a tracked request
        cannot carry ``$filter``, and the *seen* set must cover the whole table anyway so
        rows outside the bounds are never mistaken for deletes.
        """
        known = await self._known_records(spec)
        seen: set[str] = set()
        upserted = 0
        select = {"$select": ",".join(spec.select_fields)}

        if state.change_tracking is not ChangeTrackingStatus.DISABLED:
            try:
                delta_link: Optional[str] = None
                async for page in self._iter_delta_pages(spec, spec.entity_set, params=select):
                    seen.update(seen_external_ids(spec, page.upserts))
                    upserted += await self._process_rows(spec, page.upserts, start_ms, end_ms)
                    if page.delta_link:
                        delta_link = page.delta_link
                state.mark_change_tracking_enabled(delta_link)
                if not delta_link:
                    self.logger.warning("Dynamics 365 tracked pull of %s returned no delta link", spec.entity_set)
            except httpx.HTTPStatusError as e:
                if not is_change_tracking_disabled_error(e.response.status_code, _response_body(e.response)):
                    raise
                self._mark_change_tracking_disabled(spec, state)
                seen.clear()
                upserted = 0

        if state.change_tracking is ChangeTrackingStatus.DISABLED:
            params = {**select, "$orderby": "modifiedon asc"}
            async for rows in self._iter_pages(spec.entity_set, params=params, include_annotations=True):
                seen.update(seen_external_ids(spec, rows))
                upserted += await self._process_rows(spec, rows, start_ms, end_ms)

        deleted = await self._prune_missing(spec, known, seen)
        return upserted, deleted

    async def _process_rows(
        self, spec: EntitySpec, rows: List[Dict[str, Any]], start_ms: Optional[int], end_ms: Optional[int]
    ) -> int:
        batch: List[Tuple[Record, List[Permission]]] = []
        for row in rows:
            if not row_in_modified_bounds(row, start_ms, end_ms):
                continue
            built = self._build_record_with_permissions(spec, row)
            if built is None:
                continue
            batch.append(built)
            attachment = self._build_attachment_record(spec, row, built[1])
            if attachment is not None:
                batch.append(attachment)
        if batch:
            await self.data_entities_processor.on_new_records(batch)
        return len(batch)

    # ------------------------------------------------------------------
    # Delete detection
    # ------------------------------------------------------------------

    async def _known_records(self, spec: EntitySpec) -> Dict[str, str]:
        """``{external_record_id: record_id}`` for everything the graph holds under the
        entity's record group (records and note attachments alike)."""
        known: Dict[str, str] = {}
        org_id = self.data_entities_processor.org_id
        async with self.data_store_provider.transaction() as tx_store:
            group = await tx_store.get_record_group_by_external_id(
                connector_id=self.connector_id, external_id=self._record_group_external_id(spec)
            )
            if not group:
                return known
            offset = 0
            while True:
                page = await tx_store.get_records_by_status(
                    org_id=org_id,
                    connector_id=self.connector_id,
                    status_filters=None,
                    limit=_KNOWN_RECORDS_PAGE_SIZE,
                    offset=offset,
                    record_group_id=group.id,
                )
                if not page:
                    break
                for record in page:
                    external_id = getattr(record, "external_record_id", None)
                    record_id = getattr(record, "id", None)
                    if external_id and record_id:
                        known[str(external_id)] = str(record_id)
                if len(page) < _KNOWN_RECORDS_PAGE_SIZE:
                    break
                offset += _KNOWN_RECORDS_PAGE_SIZE
        return known

    async def _prune_missing(self, spec: EntitySpec, known: Dict[str, str], seen: set[str]) -> int:
        missing = missing_record_ids(known, seen)
        if not missing:
            return 0
        if not prune_is_safe(len(known), len(seen)):
            self.logger.warning(
                "Refusing to prune %d %s records: the full pull returned no rows at all",
                len(missing), spec.display_name.lower(),
            )
            return 0
        self.logger.info("Pruning %d %s records no longer present in Dynamics 365", len(missing), spec.display_name.lower())
        return await self._delete_record_ids(missing)

    async def _delete_by_source_ids(self, spec: EntitySpec, source_ids: List[str]) -> int:
        """Delta ``reason: deleted`` entries → graph records (unknown ids are ignored)."""
        record_ids: List[str] = []
        for source_id in source_ids:
            record = await self.data_entities_processor.get_record_by_external_id(
                self.connector_id, record_external_id(spec, source_id)
            )
            if record is not None and getattr(record, "id", None):
                record_ids.append(str(record.id))
        return await self._delete_record_ids(record_ids)

    async def _delete_record_ids(self, record_ids: List[str]) -> int:
        """Standard cascade delete: drops the record, its permission edges and children
        (note attachments) and publishes the vector cleanup events."""
        deleted = 0
        for start in range(0, len(record_ids), _DELETE_BATCH_SIZE):
            chunk = record_ids[start:start + _DELETE_BATCH_SIZE]
            try:
                result = await self.data_entities_processor.on_records_deleted_cascade(chunk, self.connector_id)
            except Exception as e:
                self.logger.error("Failed to delete %d Dynamics 365 records: %s", len(chunk), e, exc_info=True)
                continue
            deleted += int((result or {}).get("successfully_deleted") or 0)
            failed = (result or {}).get("failed_records") or []
            if failed:
                self.logger.warning("%d Dynamics 365 record deletions failed: %s", len(failed), failed[:5])
        return deleted

    def _build_record_with_permissions(self, spec: EntitySpec, row: Dict[str, Any]) -> Optional[Tuple[Record, List[Permission]]]:
        assert self._security is not None
        record = self._build_record(spec, row)
        if record is None:
            return None
        permissions = grants_to_permissions(derive_grants(spec, row, self._security))
        return record, permissions

    def _build_record(self, spec: EntitySpec, row: Dict[str, Any]) -> Optional[Record]:
        row_id = row.get(spec.primary_id)
        if not row_id:
            return None
        row_id = str(row_id)
        modified_ms = parse_dataverse_timestamp(row.get("modifiedon"))
        created_ms = parse_dataverse_timestamp(row.get("createdon"))
        owner_id, owner_kind = owner_reference(row)
        owner_email = self._security.user_email_by_id.get(owner_id) if (self._security and owner_kind == "systemuser" and owner_id) else None
        common: Dict[str, Any] = {
            "org_id": self.data_entities_processor.org_id,
            "record_name": record_title(spec, row),
            "record_type": RecordType(spec.record_type),
            "record_group_type": RecordGroupType(spec.record_group_type),
            "external_record_id": record_external_id(spec, row_id),
            "external_record_group_id": self._record_group_external_id(spec),
            "external_revision_id": str(modified_ms) if modified_ms is not None else None,
            "version": 0,
            "origin": OriginTypes.CONNECTOR,
            "connector_name": self.connector_name,
            "connector_id": self.connector_id,
            "mime_type": MimeTypes.MARKDOWN.value,
            "weburl": record_web_url(self.environment_url, spec, row_id),
            "source_created_at": created_ms,
            "source_updated_at": modified_ms,
            "inherit_permissions": False,
            "preview_renderable": False,
        }
        statecode = row.get("statecode")
        if spec.logical_name == "opportunity":
            return DealRecord(
                **common,
                name=common["record_name"],
                amount=_as_float(row.get("estimatedvalue")),
                expected_revenue=_as_float(row.get("actualvalue")),
                expected_close_date=row.get("estimatedclosedate"),
                conversion_probability=_as_float(row.get("closeprobability")),
                type=display_value(row, "salesstage"),
                owner_id=owner_id,
                is_won=statecode == STATE_WON_OR_RESOLVED if statecode is not None else None,
                is_closed=statecode in (STATE_WON_OR_RESOLVED, STATE_LOST_OR_CANCELLED) if statecode is not None else None,
                created_date=row.get("createdon"),
                close_date=row.get("actualclosedate"),
            )
        if spec.logical_name == "incident":
            ticket_number = row.get("ticketnumber")
            return TicketRecord(
                **common,
                status=display_value(row, "statuscode"),
                priority=display_value(row, "prioritycode"),
                type=display_value(row, "casetypecode"),
                assignee=display_value(row, "_ownerid_value"),
                assignee_email=owner_email,
                reporter_name=display_value(row, "_customerid_value"),
                labels=[str(ticket_number)] if ticket_number else [],
            )
        return Record(**common)

    def _build_attachment_record(
        self, spec: EntitySpec, row: Dict[str, Any], permissions: List[Permission]
    ) -> Optional[Tuple[Record, List[Permission]]]:
        """Note attachments become FILE children of the note (same ACL, streamed from ``documentbody``)."""
        if spec.logical_name != "annotation" or not row.get("isdocument") or not row.get("filename"):
            return None
        annotation_id = str(row.get("annotationid") or "")
        if not annotation_id:
            return None
        filename = str(row["filename"])
        extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else None
        modified_ms = parse_dataverse_timestamp(row.get("modifiedon"))
        record = FileRecord(
            org_id=self.data_entities_processor.org_id,
            record_name=filename,
            record_type=RecordType.FILE,
            record_group_type=RecordGroupType(spec.record_group_type),
            external_record_id=attachment_external_id(annotation_id),
            external_record_group_id=self._record_group_external_id(spec),
            parent_external_record_id=record_external_id(spec, annotation_id),
            external_revision_id=str(modified_ms) if modified_ms is not None else None,
            version=0,
            origin=OriginTypes.CONNECTOR,
            connector_name=self.connector_name,
            connector_id=self.connector_id,
            mime_type=row.get("mimetype") or MimeTypes.BIN.value,
            size_in_bytes=_as_int(row.get("filesize")),
            weburl=record_web_url(self.environment_url, spec, annotation_id),
            source_created_at=parse_dataverse_timestamp(row.get("createdon")),
            source_updated_at=modified_ms,
            inherit_permissions=False,
            is_file=True,
            extension=extension,
        )
        return record, list(permissions)

    # ------------------------------------------------------------------
    # Streaming / reindex
    # ------------------------------------------------------------------

    async def get_signed_url(self, record: Record) -> Optional[str]:
        return None  # Dataverse has no pre-signed download URLs; content is streamed.

    async def stream_record(self, record: Record, user_id: Optional[str] = None, convertTo: Optional[str] = None) -> StreamingResponse:
        kind, guid = split_external_id(record.external_record_id)
        if kind == ATTACHMENT_ID_PREFIX:
            payload = await self._get_json(f"annotations({guid})", params={"$select": "documentbody,mimetype,filename"})
            body = base64.b64decode(payload.get("documentbody") or "")
            return create_stream_record_response(
                _bytes_stream(body),
                filename=payload.get("filename") or record.record_name,
                mime_type=payload.get("mimetype") or record.mime_type,
                fallback_filename=f"record_{record.id}",
            )
        spec = ENTITY_SPECS.get(kind)
        if spec is None:
            raise ValueError(f"Unsupported Dynamics 365 record kind: {kind}")
        row = await self._fetch_row(spec, guid)
        if row is None:
            markdown = f"# {record.record_name}\n\nThis Dynamics 365 record no longer exists.\n"
        else:
            markdown, _ = render_record_markdown(spec, row, self.environment_url)
        return create_stream_record_response(
            _bytes_stream(markdown.encode("utf-8")),
            filename=f"{record.record_name}.md",
            mime_type=MimeTypes.MARKDOWN.value,
            fallback_filename=f"record_{record.id}.md",
        )

    async def reindex_records(self, record_results: List[Record]) -> None:
        """Rebuild records whose ``modifiedon`` moved past the stored revision; reindex the rest as-is."""
        if not record_results:
            return
        if self._security is None:
            # Permissions are only refreshed during a sync; reindex reuses the stored ACL.
            self._security = SecurityContext()
        unchanged: List[Record] = []
        for record in record_results:
            try:
                kind, guid = split_external_id(record.external_record_id)
            except ValueError:
                unchanged.append(record)
                continue
            spec = ENTITY_SPECS.get(kind)
            if spec is None:
                unchanged.append(record)
                continue
            row = await self._fetch_row(spec, guid)
            if row is None:
                self.logger.warning("Dynamics 365 record %s no longer exists; reindexing stored copy", record.external_record_id)
                unchanged.append(record)
                continue
            modified_ms = parse_dataverse_timestamp(row.get("modifiedon"))
            stored = _as_int(record.external_revision_id)
            if modified_ms is not None and (stored is None or modified_ms > stored):
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
        """Dataverse webhooks/Service Bus are not wired yet; acknowledge like Outlook does."""
        return True

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> FilterOptionsResponse:
        if filter_key != ENTITIES_FILTER_KEY:
            raise ValueError(f"Unsupported filter key: {filter_key}")
        needle = (search or "").strip().lower()
        options = [
            FilterOption(id=name, label=ENTITY_SPECS[name].display_name)
            for name in DEFAULT_ENTITY_ORDER
            if not needle or needle in name or needle in ENTITY_SPECS[name].display_name.lower()
        ]
        start = max(page - 1, 0) * limit
        chunk = options[start:start + limit]
        return FilterOptionsResponse(success=True, options=chunk, page=page, limit=limit, has_more=start + limit < len(options))


def _row_ids(spec: EntitySpec, rows: list[dict[str, Any]]) -> list[str]:
    return [str(row[spec.primary_id]) for row in rows if row.get(spec.primary_id)]


def _response_body(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _bytes_stream(data: bytes) -> AsyncGenerator[bytes, None]:
    yield data
