"""Microsoft Teams connector (Microsoft Graph, app-only auth).

Indexes channel message threads (root + replies as one markdown record per
thread) and, optionally, 1:1 / group chats, and mirrors team / channel / chat
membership as READER permission edges so permission-aware search only surfaces
messages the user could read in Teams.  Files shared in messages become child
``FileRecord`` s of the thread / chat record (same shape and streaming path as
the OneDrive connector: driveItem metadata, bytes from
``@microsoft.graph.downloadUrl``), carrying exactly the parent's grants.  The
mapping itself lives in ``mapping.py`` (pure, unit-tested); this module owns
auth, HTTP, paging and the ``BaseConnector`` lifecycle — the same split as
``..dynamics365``.

Incremental channel sync = ``/messages/delta`` (roots only) **plus** a sweep of
``/messages?$expand=replies`` — Graph sorts that listing by the last-modified
time of the whole reply chain, so it is walked newest-first until a chain older
than the previous run appears.  See ``mapping`` "Incremental sync model".

Auth: Entra ID client-credentials (``ClientSecretCredential`` like OneDrive /
Dynamics 365) for ``https://graph.microsoft.com/.default``.  Directory users
come through the shared ``MSGraphClient`` (kiota SDK); Teams payloads are read
as plain JSON over ``httpx`` because ``mapping.py`` works on dicts and the
delta / next links Graph hands back are opaque absolute URLs.

Application permissions (admin consent): see
``mapping.REQUIRED_APPLICATION_PERMISSIONS`` / ``CHAT_APPLICATION_PERMISSIONS``.
``ChannelMessage.Read.All`` and ``Chat.Read.All`` are Microsoft **protected
APIs** — without Microsoft's approval Graph returns 403 and the connector logs
and skips the channel / chat.

Personal scope (``scope=personal``, ``AuthType.OAUTH``): the member signs in with
their own Microsoft account (delegated ``Chat.Read`` — not a protected API) and
only **their 1:1 / group chats** (``/me/chats``) are indexed; teams, channels,
directory users and groups are skipped and every record is READER for the
connector creator alone (``common.personal_scope``).  The bearer token comes
from the instance config through ``common.delegated_auth.DelegatedTokenProvider``
and is refreshed by the platform ``TokenRefreshService``.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx
from azure.identity.aio import ClientSecretCredential
from fastapi import HTTPException
from msgraph import GraphServiceClient

from app.config.constants.arangodb import (
    Connectors,
    MimeTypes,
    OriginTypes,
    ProgressStatus,
)
from app.config.constants.http_status_code import HttpStatusCode
from app.connectors.core.base.connector.connector_service import (
    BaseConnector,
    ConnectorInitError,
)
from app.connectors.core.base.sync_point.sync_point import SyncDataPointType, SyncPoint
from app.connectors.core.constants import CONNECTOR_EMAIL_IDENTITY_INFO, IconPaths
from app.connectors.core.registry.auth_builder import (
    AuthBuilder,
    AuthType,
    OAuthScopeConfig,
)
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
    IndexingFilterKey,
    OptionSourceType,
    load_connector_filters,
)
from app.connectors.sources.microsoft.common.apps import MicrosoftTeamsApp
from app.connectors.sources.microsoft.common.change_notifications import (
    CHAT_MESSAGE_MAX_MINUTES,
    DIRECTORY_MAX_MINUTES,
    GraphResource,
    remove_graph_subscriptions,
    sync_graph_subscriptions,
)
from app.connectors.sources.microsoft.common.constants import (
    MicrosoftGraphScopes,
    MicrosoftOAuth,
    MicrosoftOAuthParams,
)
from app.connectors.sources.microsoft.common.content_type_utils import (
    derive_attachment_extension,
)
from app.connectors.sources.microsoft.common.delegated_auth import (
    DelegatedTokenProvider,
)
from app.connectors.sources.microsoft.common.msgraph_client import MSGraphClient
from app.connectors.sources.microsoft.common.personal_scope import (
    is_personal_scope,
    signed_in_account_matches_creator,
)
from app.connectors.sources.microsoft.teams.mapping import (
    CHAT_APPLICATION_PERMISSIONS,
    CHAT_ID_PREFIX,
    CHAT_LOOKBACK_DAYS_FILTER_KEY,
    CHATS_RECORD_GROUP_ID,
    CHATS_RECORD_GROUP_NAME,
    CHATS_SYNC_POINT_KEY,
    DEFAULT_CHAT_LOOKBACK_DAYS,
    FILE_APPLICATION_PERMISSIONS,
    FILE_ID_PREFIX,
    GRAPH_BASE_URL,
    GRAPH_SCOPE,
    HOSTED_ID_PREFIX,
    HOSTED_IMAGE_EXTENSION,
    HOSTED_IMAGE_MIME_TYPE,
    INCLUDE_CHATS_FILTER_KEY,
    INCLUDE_INLINE_IMAGES_FILTER_KEY,
    INCLUDE_PRIVATE_CHANNELS_FILTER_KEY,
    PERSONAL_DELEGATED_PERMISSIONS,
    PROTECTED_API_PERMISSIONS,
    REQUIRED_APPLICATION_PERMISSIONS,
    TEAMS_FILTER_KEY,
    THREAD_ID_PREFIX,
    Attachment,
    DeltaChanges,
    FileInfo,
    GrantEntity,
    GrantRole,
    HostedImage,
    Member,
    MessageView,
    PermissionGrant,
    Thread,
    build_thread,
    channel_delta_url,
    channel_display_name,
    channel_grants,
    channel_members_group_external_id,
    channel_message_url,
    channel_messages_url,
    channel_record_group_external_id,
    channel_record_group_name,
    channel_sync_point_key,
    chat_external_id,
    chat_grants,
    chat_messages_url,
    chat_revision,
    chat_title,
    classify_delta_items,
    classify_listing_items,
    delta_sync_point_data,
    drive_item_file_info,
    drive_item_url,
    file_attachments,
    hosted_content_value_url,
    hosted_external_id,
    hosted_images,
    is_delta_unsupported_status,
    is_private_or_shared_channel,
    lookback_start_ms,
    me_chats_url,
    message_replies_url,
    parse_conversation_members,
    parse_delta_page,
    parse_graph_timestamp,
    parse_group_members,
    personal_chat_grants,
    read_delta_link,
    read_last_sync_ms,
    render_chat_markdown,
    render_thread_markdown,
    resolve_chat_lookback_days,
    select_chat_messages,
    select_teams,
    shared_drive_item_url,
    should_sync_channel,
    skipped_attachments,
    split_external_id,
    team_display_name,
    team_group_external_id,
    thread_external_id,
    thread_mentioned_user_ids,
    thread_participant_ids,
    thread_revision,
    thread_title,
    user_chats_url,
)
from app.models.entities import (
    AppUser,
    AppUserGroup,
    FileRecord,
    MessageRecord,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.utils.streaming import create_stream_record_response, stream_content
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

CONNECTOR_KEY = "microsoftteams"  # ConnectorFactory registry key / filters config name
USERS_SYNC_POINT_KEY = "users"

_MAX_HTTP_RETRIES = 5
_RETRY_STATUS = {HttpStatusCode.TOO_MANY_REQUESTS.value, 502, 503, 504}
_TOKEN_REFRESH_SKEW_S = 120
_MAX_CONCURRENT_REQUESTS = 4
_RECORD_BATCH_SIZE = 25

_GRANT_ROLE_TO_PERMISSION = {GrantRole.READER: PermissionType.READ}
_GRANT_ENTITY_TO_PERMISSION = {GrantEntity.USER: EntityType.USER, GrantEntity.GROUP: EntityType.GROUP}


def grants_to_permissions(grants: list[PermissionGrant]) -> list[Permission]:
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


class _GraphForbidden(Exception):
    """403 from Graph — missing application permission or unapproved protected API."""


@dataclass(frozen=True)
class _ChannelContext:
    """What every thread of one channel shares: names for rendering, grants for its records."""

    team_id: str
    team_name: str
    channel_id: str
    channel_name: str
    permissions: list[Permission]


@ConnectorBuilder("Microsoft Teams")\
    .in_group("Microsoft 365")\
    .with_description(
        "Sync Microsoft Teams channel conversations (and optionally chats) with "
        "permissions derived from team and channel membership"
    )\
    .with_categories(["Communication", "Collaboration"])\
    .with_scopes([ConnectorScope.PERSONAL.value, ConnectorScope.TEAM.value])\
    .with_auth([
        # team scope: app-only Entra app + admin consent (first entry = default auth type)
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
                description="The Directory (Tenant) ID of the Entra ID tenant that hosts Microsoft Teams",
            ),
            AuthField(
                name="hasAdminConsent",
                display_name="Admin consent granted",
                description=(
                    "Confirm an admin granted these application permissions: "
                    + ", ".join(REQUIRED_APPLICATION_PERMISSIONS)
                    + " (plus " + ", ".join(CHAT_APPLICATION_PERMISSIONS) + " when chats are included). "
                    + ", ".join(PROTECTED_API_PERMISSIONS)
                    + " are protected APIs that also need Microsoft's approval form."
                ),
                field_type="CHECKBOX",
                required=True,
                default_value=False,
            ),
        ]),
        # personal scope: the member signs in with their own account; chats only.
        # Client id / secret come from the org's shared OAuth app (``/services/oauth/microsoftteams``,
        # referenced by ``auth.oauthConfigId``), exactly like Outlook Personal.
        AuthBuilder.type(AuthType.OAUTH).oauth(
            connector_name="Microsoft Teams",
            authorize_url=MicrosoftOAuth.authorize_url(),
            token_url=MicrosoftOAuth.token_url(),
            redirect_uri="connectors/oauth/callback/MicrosoftTeams",
            scopes=OAuthScopeConfig(
                personal_sync=[
                    MicrosoftGraphScopes.CHAT_READ,
                    MicrosoftGraphScopes.FILES_READ,  # files shared in the user's chats
                    MicrosoftGraphScopes.USER_READ,
                    MicrosoftGraphScopes.OFFLINE_ACCESS,
                ],
                team_sync=[],
                agent=[],
            ),
            fields=[
                CommonFields.tenant_id("Entra ID App Registration"),
                CommonFields.client_id("Entra ID App Registration"),
                CommonFields.client_secret("Entra ID App Registration"),
            ],
            icon_path=IconPaths.connector_icon(Connectors.MICROSOFT_TEAMS.value),
            app_group="Microsoft 365",
            app_description="OAuth application for reading a user's own Microsoft Teams chats",
            app_categories=["Communication", "Collaboration"],
            additional_params={
                "response_mode": MicrosoftOAuthParams.RESPONSE_MODE_QUERY,
                "prompt": MicrosoftOAuthParams.PROMPT_SELECT_ACCOUNT,
            },
        ),
    ])\
    .with_info(CONNECTOR_EMAIL_IDENTITY_INFO)\
    .configure(lambda builder: builder
        .with_icon(IconPaths.connector_icon(Connectors.MICROSOFT_TEAMS.value))
        .add_documentation_link(DocumentationLink(
            "Register an application with the Microsoft identity platform",
            "https://learn.microsoft.com/entra/identity-platform/quickstart-register-app",
            "setup",
        ))
        .add_documentation_link(DocumentationLink(
            "Protected APIs in Microsoft Teams (approval form)",
            "https://learn.microsoft.com/graph/teams-protected-apis",
            "setup",
        ))
        .add_filter_field(FilterField(
            name=TEAMS_FILTER_KEY,
            display_name="Teams",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Only sync these teams. Leave empty to sync every team the app can see.",
            option_source_type=OptionSourceType.DYNAMIC,
        ))
        .add_filter_field(FilterField(
            name=INCLUDE_PRIVATE_CHANNELS_FILTER_KEY,
            display_name="Include private and shared channels",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.SYNC,
            description="Private/shared channels get their own membership group (needs ChannelMember.Read.All).",
            default_value=True,
        ))
        .add_filter_field(FilterField(
            name=INCLUDE_CHATS_FILTER_KEY,
            display_name="Include 1:1 and group chats",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.SYNC,
            description=(
                "Index chats as one rolling record per chat, readable only by its participants "
                "(needs Chat.Read.All, a protected API). Personal connectors always sync the "
                "signed-in user's chats and nothing else."
            ),
            default_value=False,
        ))
        .add_filter_field(FilterField(
            name=CHAT_LOOKBACK_DAYS_FILTER_KEY,
            display_name="Chat lookback (days)",
            filter_type=FilterType.NUMBER,
            category=FilterCategory.SYNC,
            description="How many days of chat history each chat record keeps.",
            default_value=DEFAULT_CHAT_LOOKBACK_DAYS,
        ))
        .add_filter_field(FilterField(
            name=INCLUDE_INLINE_IMAGES_FILTER_KEY,
            display_name="Include pasted images",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.SYNC,
            description=(
                "Also index pictures pasted into messages (screenshots) as image files of the "
                "message. Every image is OCR'd, so this is off by default."
            ),
            default_value=False,
        ))
        .add_filter_field(FilterField(
            name=IndexingFilterKey.ATTACHMENTS.value,
            display_name="Index attachments",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description=(
                "Index files shared in channel and chat messages (SharePoint / OneDrive links). "
                "Needs Files.Read.All; when off the files are still listed but not indexed."
            ),
            default_value=True,
        ))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 60)
        .with_sync_support(True)
        .with_agent_support(False)
    )\
    .build_decorator()
class MicrosoftTeamsConnector(BaseConnector):
    """Microsoft Graph Teams connector. See module docstring and ``mapping.py``."""

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
            MicrosoftTeamsApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_name = Connectors.MICROSOFT_TEAMS

        def _sync_point(kind: SyncDataPointType) -> SyncPoint:
            return SyncPoint(
                connector_id=self.connector_id,
                org_id=self.data_entities_processor.org_id,
                sync_data_point_type=kind,
                data_store_provider=self.data_store_provider,
            )

        self.user_sync_point = _sync_point(SyncDataPointType.USERS)
        self.records_sync_point = _sync_point(SyncDataPointType.RECORDS)

        self.credential: ClientSecretCredential | None = None
        self.graph_client: GraphServiceClient | None = None
        self.msgraph_client: MSGraphClient | None = None
        self._http: httpx.AsyncClient | None = None
        self._token: str | None = None
        self._token_expires_on: int = 0
        self._request_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_REQUESTS)
        # personal scope: delegated token provider (None in team scope)
        self._delegated: DelegatedTokenProvider | None = None
        self._me_oid: str | None = None

        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self._users_by_id: dict[str, AppUser] = {}
        self._user_email_by_id: dict[str, str] = {}
        self._team_names: dict[str, str] = {}
        self._channel_names: dict[tuple[str, str], str] = {}
        self._protected_api_logged = False
        # (team_id, channel_id) synced by the current run; Graph change-notification subscriptions follow them
        self._touched_channels: set[tuple[str, str]] = set()
        # per-run: the same SharePoint link shows up in many threads; None = unresolvable
        self._shared_file_cache: dict[str, FileInfo | None] = {}
        self._files_forbidden_logged = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _is_personal(self) -> bool:
        return is_personal_scope(self.scope)

    def _personal_permissions(self) -> list[Permission]:
        """Creator-only READER grants for personal-scope records and record groups."""
        return grants_to_permissions(personal_chat_grants(self.creator_email))

    async def init(self) -> bool:
        config = await self.config_service.get_config(f"/services/connectors/{self.connector_id}/config")
        if not config:
            self.logger.error("Microsoft Teams config not found")
            return False
        if self._is_personal():
            return await self._init_personal()
        auth = config.get("auth", {}) or {}
        tenant_id = auth.get("tenantId")
        client_id = auth.get("clientId")
        client_secret = auth.get("clientSecret")
        if not all((tenant_id, client_id, client_secret)):
            raise ConnectorInitError(
                "Incomplete Microsoft Teams credentials. tenantId, clientId and clientSecret are all required."
            )

        await self._close_http()
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        self._http = httpx.AsyncClient(
            base_url=GRAPH_BASE_URL,
            timeout=httpx.Timeout(90.0, connect=15.0),
            headers={"Accept": "application/json"},
        )
        try:
            await self._refresh_token()
            org = await self._get_json("organization", params={"$select": "id,displayName"})
        except ConnectorInitError:
            raise
        except Exception as e:
            await self._close_http()
            raise ConnectorInitError(
                "Could not authenticate to Microsoft Graph. Check tenantId/clientId/clientSecret and that "
                f"admin consent was granted for {', '.join(REQUIRED_APPLICATION_PERMISSIONS)}. "
                f"({type(e).__name__}: {str(e)[:200]})"
            ) from e
        self.graph_client = GraphServiceClient(self.credential, scopes=[GRAPH_SCOPE])
        self.msgraph_client = MSGraphClient(self.connector_name, self.connector_id, self.graph_client, self.logger)
        tenant_name = next((o.get("displayName") for o in org.get("value") or [] if o.get("displayName")), tenant_id)
        self.logger.info("Microsoft Teams connector initialised for tenant %s", tenant_name)
        return True

    async def _init_personal(self) -> bool:
        """Personal scope: delegated token from the instance config, no ClientSecretCredential."""
        await self._load_creator_email()
        if not self.creator_email:
            raise ConnectorInitError(
                "Cannot resolve the creator of this personal Microsoft Teams connector; "
                "records would be readable by nobody."
            )
        await self._close_http()
        self._delegated = DelegatedTokenProvider(
            self.config_service, self.connector_id, Connectors.MICROSOFT_TEAMS.value, self.logger
        )
        self._http = httpx.AsyncClient(
            base_url=GRAPH_BASE_URL,
            timeout=httpx.Timeout(90.0, connect=15.0),
            headers={"Accept": "application/json"},
        )
        try:
            await self._delegated.get_token()
            me = await self._get_json("me", params={"$select": "id,displayName,mail,userPrincipalName"})
        except Exception as e:
            await self._close_http()
            raise ConnectorInitError(
                "Could not read the signed-in Microsoft account. Sign in again to re-authorise this "
                f"personal Teams connector ({', '.join(PERSONAL_DELEGATED_PERMISSIONS)}). "
                f"({type(e).__name__}: {str(e)[:200]})"
            ) from e
        self._me_oid = str(me.get("id") or self._delegated.user_oid() or "")
        if signed_in_account_matches_creator(self.creator_email, me.get("mail"), me.get("userPrincipalName")) is False:
            self.logger.warning(
                "Personal Teams connector %s: signed-in account %s differs from creator %s; "
                "records stay readable by the creator only",
                self.connector_id, me.get("userPrincipalName") or me.get("mail"), self.creator_email,
            )
        self.logger.info("Microsoft Teams personal connector initialised for %s", self.creator_email)
        return True

    async def test_connection_and_access(self) -> bool:
        try:
            if self._is_personal():
                payload = await self._get_json("me/chats", params={"$top": "1", "$select": "id"})
            else:
                payload = await self._get_json("teams", params={"$top": "1", "$select": "id"})
            return isinstance(payload.get("value"), list)
        except Exception as e:
            self.logger.error("Microsoft Teams connection test failed: %s", e)
            return False

    async def _sync_change_notifications(self) -> None:
        """Channel-message subscriptions for the channels this run synced plus ``groups`` for
        membership (team scope only; never raises). Channel messages need the protected
        ``ChannelMessage.Read.All`` — a 403 is logged once and polling remains."""
        if self._is_personal():
            return
        resources = [
            GraphResource(f"teams/{team_id}/channels/{channel_id}/messages", "created,updated,deleted", CHAT_MESSAGE_MAX_MINUTES)
            for team_id, channel_id in sorted(self._touched_channels)
        ]
        resources.append(GraphResource("groups", "updated", DIRECTORY_MAX_MINUTES))
        self._touched_channels = set()
        await sync_graph_subscriptions(
            config_service=self.config_service,
            connector_id=self.connector_id,
            token_getter=self._get_token,
            sync_point=self.records_sync_point,
            resources=resources,
            logger=self.logger,
        )

    async def remove_change_notifications(self) -> None:
        if self._is_personal():
            return
        await remove_graph_subscriptions(
            config_service=self.config_service,
            connector_id=self.connector_id,
            token_getter=self._get_token,
            sync_point=self.records_sync_point,
            logger=self.logger,
        )

    async def cleanup(self) -> None:
        await self._close_http()

    async def _close_http(self) -> None:
        if self._http is not None:
            with contextlib.suppress(Exception):
                await self._http.aclose()
            self._http = None
        self.graph_client = None
        self.msgraph_client = None
        if self.credential is not None:
            with contextlib.suppress(Exception):
                await self.credential.close()
            self.credential = None
        self._token = None
        self._token_expires_on = 0
        self._delegated = None

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
        if self._delegated is not None:
            # personal: ask the platform refresh service, then fall back to whatever is stored
            self._token = await self._delegated.refresh() or await self._delegated.get_token()
            return self._token
        if self.credential is None:
            raise RuntimeError("Microsoft Teams connector not initialised")
        token = await self.credential.get_token(GRAPH_SCOPE)
        self._token = token.token
        self._token_expires_on = int(token.expires_on)
        return self._token

    async def _get_token(self) -> str:
        if self._delegated is not None:
            # re-read every call: the background TokenRefreshService rewrites the stored token
            return await self._delegated.get_token()
        now_s = get_epoch_timestamp_in_ms() // 1000
        if self._token and now_s < self._token_expires_on - _TOKEN_REFRESH_SKEW_S:
            return self._token
        return await self._refresh_token()

    async def _get_json(self, path_or_url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """GET with bearer auth, 401 re-auth and bounded retry on 429/5xx (honours Retry-After).

        ``path_or_url`` may be a relative Graph path or an absolute ``@odata.nextLink`` /
        ``@odata.deltaLink`` (which already carries its own query string)."""
        if self._http is None:
            raise RuntimeError("Microsoft Teams connector not initialised")
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
                    self.logger.warning("Microsoft Graph request failed (%s), retrying in %.1fs", e, delay)
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
                wait = min(max(wait, 0.5), 120.0)
                self.logger.warning(
                    "Microsoft Graph returned %s for %s, retrying in %.1fs", response.status_code, path_or_url, wait
                )
                await asyncio.sleep(wait)
                delay = min(delay * 2, 30.0)
                continue
            if response.status_code == HttpStatusCode.FORBIDDEN.value:
                raise _GraphForbidden(f"403 from Microsoft Graph for {path_or_url}: {response.text[:300]}")
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Microsoft Graph request to {path_or_url} exhausted retries")

    async def _iter_pages(self, path_or_url: str, params: dict[str, str] | None = None) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Follow ``@odata.nextLink`` (absolute URL that already carries the query)."""
        payload = await self._get_json(path_or_url, params=params)
        while True:
            yield [v for v in (payload.get("value") or []) if isinstance(v, dict)]
            next_link = payload.get("@odata.nextLink")
            if not next_link:
                return
            payload = await self._get_json(next_link)

    async def _fetch_all(self, path_or_url: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        async for page in self._iter_pages(path_or_url, params=params):
            rows.extend(page)
        return rows

    def _log_protected_api(self, what: str, error: Exception) -> None:
        if self._is_personal():
            self.logger.warning(
                "Microsoft Graph denied access (403) while reading %s with the signed-in user's token. "
                "Re-authorise the personal connector so it carries %s. (%s)",
                what, ", ".join(PERSONAL_DELEGATED_PERMISSIONS), str(error)[:200],
            )
            return
        if not self._protected_api_logged:
            self._protected_api_logged = True
            self.logger.warning(
                "Microsoft Graph denied access (403) while reading %s. Check that admin consent covers %s "
                "and that Microsoft approved the protected APIs (%s) for this app/tenant: "
                "https://learn.microsoft.com/graph/teams-protected-apis. Affected items are skipped. (%s)",
                what, ", ".join(REQUIRED_APPLICATION_PERMISSIONS + CHAT_APPLICATION_PERMISSIONS),
                ", ".join(PROTECTED_API_PERMISSIONS), str(error)[:200],
            )
        else:
            self.logger.warning("Skipping %s: Microsoft Graph returned 403", what)

    # ------------------------------------------------------------------
    # Sync entry points
    # ------------------------------------------------------------------

    async def run_sync(self) -> None:
        """Full sync: users -> teams -> channels (members, record group, every thread) -> chats."""
        await self._run(incremental=False)

    async def run_incremental_sync(self) -> None:
        """Incremental: per-channel ``/messages/delta`` from the stored deltaLink
        (``lastModifiedDateTime`` filter where delta is unsupported); chats by modified filter."""
        await self._run(incremental=True)

    async def _run(self, *, incremental: bool) -> None:
        if self._http is None:
            self.logger.error("Microsoft Teams connector not initialised")
            return
        self.logger.info("Starting Microsoft Teams %s sync", "incremental" if incremental else "full")
        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, CONNECTOR_KEY, self.connector_id, self.logger
        )
        self._shared_file_cache = {}
        self._files_forbidden_logged = False
        if self._is_personal():
            # signed-in user's chats only: no directory, no teams / channels, creator-only permissions
            self.logger.info("Personal scope: syncing chats of %s only", self.creator_email)
            try:
                await self._sync_chats(incremental=incremental)
            except _GraphForbidden as e:
                self._log_protected_api("chats", e)
            self.logger.info("Microsoft Teams sync completed")
            return
        await self._sync_users()

        teams = select_teams(await self._list_teams(), self._selected_team_values())
        self.logger.info("Syncing %d Microsoft Teams team(s)", len(teams))
        for team in teams:
            try:
                await self._sync_team(team, incremental=incremental)
            except _GraphForbidden as e:
                self._log_protected_api(f"team {team_display_name(team)}", e)
            except Exception as e:
                self.logger.error("Failed to sync team %s: %s", team_display_name(team), e, exc_info=True)

        if self._include_chats():
            try:
                await self._sync_chats(incremental=incremental)
            except _GraphForbidden as e:
                self._log_protected_api("chats", e)
        self.logger.info("Microsoft Teams sync completed")
        await self._sync_change_notifications()

    def _selected_team_values(self) -> list[str] | None:
        team_filter = self.sync_filters.get(TEAMS_FILTER_KEY) if self.sync_filters else None
        if team_filter is None or team_filter.is_empty():
            return None
        return [str(v) for v in team_filter.as_list()]

    def _bool_filter(self, key: str, *, default: bool) -> bool:
        # not FilterCollection.is_enabled: that helper is for indexing filters and is
        # overridden by enable_manual_sync
        flag = self.sync_filters.get(key) if self.sync_filters else None
        if flag is None or flag.is_empty():
            return default
        return bool(flag.value)

    def _include_private_channels(self) -> bool:
        return self._bool_filter(INCLUDE_PRIVATE_CHANNELS_FILTER_KEY, default=True)

    def _include_chats(self) -> bool:
        return self._bool_filter(INCLUDE_CHATS_FILTER_KEY, default=False)

    def _include_inline_images(self) -> bool:
        return self._bool_filter(INCLUDE_INLINE_IMAGES_FILTER_KEY, default=False)

    def _index_attachments(self) -> bool:
        if not self.indexing_filters:
            return True
        return self.indexing_filters.is_enabled(IndexingFilterKey.ATTACHMENTS, default=True)

    def _chat_lookback_days(self) -> int:
        value = self.sync_filters.get_value(CHAT_LOOKBACK_DAYS_FILTER_KEY) if self.sync_filters else None
        return resolve_chat_lookback_days(value)

    # ------------------------------------------------------------------
    # Users, teams, channels, membership
    # ------------------------------------------------------------------

    async def _sync_users(self) -> None:
        """Directory users through the shared ``MSGraphClient`` (``User.Read.All``).
        Member rosters carry emails too, so a 403 here only degrades name/title data."""
        if self.msgraph_client is None:
            return
        try:
            users = await self.msgraph_client.get_all_users()
        except Exception as e:
            self.logger.warning("Could not list directory users (%s); continuing with roster data only", str(e)[:200])
            return
        batch: list[AppUser] = []
        for user in users:
            if not user.email or not user.source_user_id:
                continue
            user.email = user.email.strip().lower()
            self._users_by_id[user.source_user_id] = user
            self._user_email_by_id[user.source_user_id] = user.email
            batch.append(user)
        if batch:
            await self.data_entities_processor.on_new_app_users(batch)
        await self.user_sync_point.update_sync_point(USERS_SYNC_POINT_KEY, {"lastSyncTimestamp": get_epoch_timestamp_in_ms()})
        self.logger.info("Synced %d Microsoft Teams users", len(batch))

    async def _list_teams(self) -> list[dict[str, Any]]:
        teams = await self._fetch_all("teams", params={"$select": "id,displayName,description,webUrl,isArchived,createdDateTime"})
        for team in teams:
            if team.get("id"):
                self._team_names[str(team["id"])] = team_display_name(team)
        return teams

    async def _list_channels(self, team_id: str) -> list[dict[str, Any]]:
        channels = await self._fetch_all(
            f"teams/{team_id}/channels",
            params={"$select": "id,displayName,description,membershipType,webUrl,isArchived,createdDateTime"},
        )
        for channel in channels:
            if channel.get("id"):
                self._channel_names[(team_id, str(channel["id"]))] = channel_display_name(channel)
        return channels

    async def _team_members(self, team_id: str) -> list[Member]:
        try:
            return parse_conversation_members(await self._fetch_all(f"teams/{team_id}/members"))
        except _GraphForbidden:
            # a team is a group: GroupMember.Read.All is the documented fallback roster
            rows = await self._fetch_all(f"groups/{team_id}/members", params={"$select": "id,displayName,mail,userPrincipalName"})
            return parse_group_members(rows)

    async def _channel_members(self, team_id: str, channel_id: str) -> list[Member]:
        return parse_conversation_members(await self._fetch_all(f"teams/{team_id}/channels/{channel_id}/members"))

    def _members_to_app_users(self, members: list[Member]) -> list[AppUser]:
        app_users: list[AppUser] = []
        for member in members:
            known = self._users_by_id.get(member.user_id)
            if known is not None:
                app_users.append(known)
                continue
            if not member.email:
                continue
            app_user = AppUser(
                app_name=self.connector_name,
                connector_id=self.connector_id,
                source_user_id=member.user_id,
                email=member.email,
                full_name=member.display_name or member.email,
                org_id=self.data_entities_processor.org_id,
                is_active=True,
            )
            self._users_by_id[member.user_id] = app_user
            self._user_email_by_id[member.user_id] = member.email
            app_users.append(app_user)
        return app_users

    async def _sync_team(self, team: dict[str, Any], *, incremental: bool) -> None:
        team_id = str(team["id"])
        team_name = team_display_name(team)
        org_id = self.data_entities_processor.org_id

        members = await self._team_members(team_id)
        team_group = AppUserGroup(
            app_name=self.connector_name,
            connector_id=self.connector_id,
            source_user_group_id=team_group_external_id(team_id),
            name=f"Team · {team_name}",
            org_id=org_id,
            description=team.get("description") or "Microsoft Teams team members",
            source_created_at=parse_graph_timestamp(team.get("createdDateTime")),
        )
        await self.data_entities_processor.on_new_user_groups([(team_group, self._members_to_app_users(members))])

        channels = await self._list_channels(team_id)
        include_private = self._include_private_channels()
        record_groups: list[tuple[RecordGroup, list[Permission]]] = []
        channel_groups: list[tuple[AppUserGroup, list[AppUser]]] = []
        synced_channels: list[dict[str, Any]] = []
        for channel in channels:
            if not should_sync_channel(channel, include_private_channels=include_private):
                continue
            channel_id = str(channel["id"])
            channel_name = channel_display_name(channel)
            if is_private_or_shared_channel(channel):
                try:
                    channel_members = await self._channel_members(team_id, channel_id)
                except _GraphForbidden as e:
                    # fail closed: without the roster we cannot scope the channel's messages
                    self._log_protected_api(f"members of {channel_name} ({channel.get('membershipType')} channel)", e)
                    continue
                channel_groups.append((
                    AppUserGroup(
                        app_name=self.connector_name,
                        connector_id=self.connector_id,
                        source_user_group_id=channel_members_group_external_id(team_id, channel_id),
                        name=f"Channel · {channel_record_group_name(team_name, channel_name)}",
                        org_id=org_id,
                        description=f"Members of the {channel.get('membershipType')} channel",
                        source_created_at=parse_graph_timestamp(channel.get("createdDateTime")),
                    ),
                    self._members_to_app_users(channel_members),
                ))
            record_groups.append((
                RecordGroup(
                    name=channel_record_group_name(team_name, channel_name),
                    short_name=channel_name,
                    description=channel.get("description") or f"Microsoft Teams channel in {team_name}",
                    external_group_id=channel_record_group_external_id(team_id, channel_id),
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    group_type=RecordGroupType.TEAMS_CHANNEL,
                    web_url=channel.get("webUrl"),
                    org_id=org_id,
                ),
                grants_to_permissions(channel_grants(team_id, channel)),
            ))
            synced_channels.append(channel)
        if channel_groups:
            await self.data_entities_processor.on_new_user_groups(channel_groups)
        if record_groups:
            await self.data_entities_processor.on_new_record_groups(record_groups)

        for channel in synced_channels:
            try:
                await self._sync_channel_messages(team_id, team_name, channel, incremental=incremental)
            except _GraphForbidden as e:
                self._log_protected_api(f"messages of {team_name} › {channel_display_name(channel)}", e)
            except Exception as e:
                self.logger.error(
                    "Failed to sync channel %s › %s: %s", team_name, channel_display_name(channel), e, exc_info=True
                )
        self._touched_channels.update((team_id, str(channel["id"])) for channel in synced_channels)
        self.logger.info("Synced team %s (%d channel(s), %d member(s))", team_name, len(synced_channels), len(members))

    # ------------------------------------------------------------------
    # Channel messages
    # ------------------------------------------------------------------

    async def _sync_channel_messages(self, team_id: str, team_name: str, channel: dict[str, Any], *, incremental: bool) -> None:
        channel_id = str(channel["id"])
        channel_name = channel_display_name(channel)
        key = channel_sync_point_key(team_id, channel_id)
        point = await self.records_sync_point.read_sync_point(key) if incremental else {}
        stored_delta = read_delta_link(point)
        last_sync_ms = read_last_sync_ms(point)
        started_ms = get_epoch_timestamp_in_ms()
        ctx = _ChannelContext(team_id, team_name, channel_id, channel_name, grants_to_permissions(channel_grants(team_id, channel)))

        use_delta = True
        payload: dict[str, Any] | None = None
        try:
            payload = await self._get_json(stored_delta or channel_delta_url(team_id, channel_id))
        except httpx.HTTPStatusError as e:
            if stored_delta and e.response.status_code in (HttpStatusCode.BAD_REQUEST.value, 410):
                # expired / invalid delta token: start a fresh delta enumeration
                self.logger.warning("Delta link for channel %s expired; re-enumerating", channel_name)
                payload = await self._get_json(channel_delta_url(team_id, channel_id))
            elif is_delta_unsupported_status(e.response.status_code):
                use_delta = False
                self.logger.info("Delta not supported for channel %s; using the reply-chain listing", channel_name)
            else:
                raise

        total = 0
        deleted = 0
        delta_link: str | None = None
        handled: set[str] = set()
        if use_delta and payload is not None:
            while True:
                page = parse_delta_page(payload)
                changes = classify_delta_items(page.items)
                handled.update(changes.dirty_root_ids)
                handled.update(changes.deleted_root_ids)
                indexed, gone = await self._process_thread_changes(ctx, changes)
                total += indexed
                deleted += gone
                if page.next_link:
                    payload = await self._get_json(page.next_link)
                    continue
                delta_link = page.delta_link
                break
        if not use_delta or last_sync_ms is not None:
            # delta lists roots only and a reply leaves its root untouched: walk the listing
            # Graph sorts by reply-chain modification until it gets older than the last run
            indexed, gone = await self._sweep_channel_threads(ctx, since_ms=last_sync_ms, skip_root_ids=handled)
            total += indexed
            deleted += gone

        await self.records_sync_point.update_sync_point(
            key,
            delta_sync_point_data(delta_link if use_delta else None, started_ms),
            encrypt_fields=["deltaLink"],
        )
        self.logger.info(
            "Synced %d thread(s) (%d deleted) in %s › %s%s",
            total, deleted, team_name, channel_name, " (incremental)" if incremental else "",
        )

    async def _process_thread_changes(self, ctx: _ChannelContext, changes: DeltaChanges) -> tuple[int, int]:
        """Rebuild the dirty threads of one delta page; returns ``(indexed, deleted)``.
        A dirty root that turns out soft-deleted, gone (404) or a system event drops its record."""

        async def _load(root_id: str) -> Thread | None:
            root = changes.roots.get(root_id)
            try:
                if root is None:
                    root = await self._get_json(channel_message_url(ctx.team_id, ctx.channel_id, root_id))
                replies = await self._fetch_all(message_replies_url(ctx.team_id, ctx.channel_id, root_id))
            except httpx.HTTPStatusError as e:
                if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                    return None
                raise
            thread = build_thread(root, replies)
            return thread if thread.root.is_indexable else None

        indexed = 0
        gone: list[str] = list(changes.deleted_root_ids)
        ids = list(changes.dirty_root_ids)
        for start in range(0, len(ids), _RECORD_BATCH_SIZE):
            chunk = ids[start:start + _RECORD_BATCH_SIZE]
            threads = await asyncio.gather(*(_load(root_id) for root_id in chunk))
            gone.extend(root_id for root_id, thread in zip(chunk, threads, strict=True) if thread is None)
            indexed += await self._emit_threads(ctx, [t for t in threads if t is not None])
        return indexed, await self._delete_threads(ctx, gone)

    async def _sweep_channel_threads(self, ctx: _ChannelContext, *, since_ms: int | None, skip_root_ids: set[str]) -> tuple[int, int]:
        """Page ``/messages?$expand=replies`` newest-chain-first and rebuild every thread with
        activity after ``since_ms`` (``None`` = the whole channel); returns ``(indexed, deleted)``."""
        indexed = 0
        deleted = 0
        payload = await self._get_json(channel_messages_url(ctx.team_id, ctx.channel_id, expand_replies=True))
        while True:
            page = parse_delta_page(payload)
            changes = classify_listing_items(page.items, since_ms, skip_root_ids)
            threads: list[Thread] = []
            for root, inline_replies, more_replies in changes.threads:
                # > 200 replies: Graph pages the rest behind replies@odata.nextLink
                extra = await self._fetch_all(more_replies) if more_replies else []
                threads.append(build_thread(root, [*inline_replies, *extra]))
            indexed += await self._emit_threads(ctx, threads)
            deleted += await self._delete_threads(ctx, changes.deleted_root_ids)
            if changes.stale or not page.next_link:
                return indexed, deleted
            payload = await self._get_json(page.next_link)

    async def _emit_threads(self, ctx: _ChannelContext, threads: list[Thread]) -> int:
        total = 0
        for start in range(0, len(threads), _RECORD_BATCH_SIZE):
            batch: list[tuple[Record, list[Permission]]] = []
            reconcile: list[tuple[str, set[str]]] = []
            for thread in threads[start:start + _RECORD_BATCH_SIZE]:
                record = self._build_thread_record(ctx.team_id, ctx.team_name, ctx.channel_id, ctx.channel_name, thread)
                children = await self._attachment_records(record, thread.messages)
                batch.append((record, list(ctx.permissions)))
                batch.extend((child, list(ctx.permissions)) for child in children)
                reconcile.append((record.external_record_id, {child.external_record_id for child in children}))
            total += await self._flush_records(batch, reconcile)
        return total

    async def _flush_records(self, batch: list[tuple[Record, list[Permission]]], reconcile: list[tuple[str, set[str]]]) -> int:
        """Upsert parents + their file children, then drop children of files no longer attached."""
        if not batch:
            return 0
        await self.data_entities_processor.on_new_records(batch)
        for parent_external_id, keep in reconcile:
            await self._reconcile_children(parent_external_id, keep)
        return len(reconcile)

    async def _delete_threads(self, ctx: _ChannelContext, root_ids: list[str]) -> int:
        deleted = 0
        for root_id in root_ids:
            deleted += await self._delete_record_tree(thread_external_id(ctx.team_id, ctx.channel_id, root_id))
        return deleted

    async def _delete_record_tree(self, external_id: str) -> int:
        """Delete a thread / chat record together with its attachment records; 1 if it existed."""
        record = await self.data_entities_processor.get_record_by_external_id(self.connector_id, external_id)
        if record is None:
            return 0
        for child in await self.data_entities_processor.get_records_by_parent(self.connector_id, external_id):
            await self.data_entities_processor.on_record_deleted(record_id=child.id)
        await self.data_entities_processor.on_record_deleted(record_id=record.id)
        return 1

    async def _reconcile_children(self, parent_external_id: str, keep: set[str]) -> None:
        children = await self.data_entities_processor.get_records_by_parent(
            self.connector_id, parent_external_id, RecordType.FILE.value
        )
        for child in children:
            if child.external_record_id not in keep:
                await self.data_entities_processor.on_record_deleted(record_id=child.id)

    # ------------------------------------------------------------------
    # Attachments (files shared in messages, pasted images)
    # ------------------------------------------------------------------

    async def _attachment_records(self, parent: MessageRecord, messages: list[MessageView]) -> list[FileRecord]:
        """Child ``FileRecord`` s of a thread / chat record.  Cards, tabs and quoted
        messages never become records."""
        for attachment in skipped_attachments(messages):
            self.logger.debug(
                "Skipping non-file attachment %r (%s) on %s",
                attachment.name, attachment.content_type or "unknown type", parent.external_record_id,
            )
        records: list[FileRecord] = []
        seen: set[str] = set()
        for attachment in file_attachments(messages):
            info = await self._resolve_shared_file(attachment)
            if info is None or info.external_id in seen:
                continue
            seen.add(info.external_id)
            records.append(self._build_file_record(info, parent))
        if self._include_inline_images():
            records.extend(self._build_hosted_image_record(image, parent) for image in hosted_images(messages))
        if records:
            # children must point at the stored parent node, not at a fresh uuid
            existing = await self.data_entities_processor.get_record_by_external_id(self.connector_id, parent.external_record_id)
            if existing is not None:
                parent.id = existing.id
                for child in records:
                    child.parent_node_id = existing.id
        return records

    async def _resolve_shared_file(self, attachment: Attachment) -> FileInfo | None:
        url = attachment.url or ""
        if url in self._shared_file_cache:
            return self._shared_file_cache[url]
        info: FileInfo | None = None
        try:
            info = drive_item_file_info(await self._get_json(shared_drive_item_url(url)))
            if info is None:
                self.logger.debug("Attachment %r is not a single file (folder / notebook); skipped", attachment.name)
        except _GraphForbidden as e:
            if not self._files_forbidden_logged:
                self._files_forbidden_logged = True
                needed = (
                    "delegated Files.Read (re-authorise the personal connector)"
                    if self._is_personal()
                    else f"the application permission {', '.join(FILE_APPLICATION_PERMISSIONS)}"
                )
                self.logger.warning(
                    "Microsoft Graph denied access to files shared in messages (403); attachments are "
                    "skipped for the rest of this run. Grant %s. (%s)", needed, str(e)[:200],
                )
        except httpx.HTTPStatusError as e:
            self.logger.debug("Could not resolve attachment %r: HTTP %s", attachment.name, e.response.status_code)
        self._shared_file_cache[url] = info
        return info

    def _build_file_record(self, info: FileInfo, parent: MessageRecord) -> FileRecord:
        record = FileRecord(
            org_id=self.data_entities_processor.org_id,
            record_name=info.name,
            record_type=RecordType.FILE,
            record_group_type=RecordGroupType.TEAMS_CHANNEL,
            parent_record_type=RecordType.MESSAGE,
            parent_external_record_id=parent.external_record_id,
            external_record_id=info.external_id,
            external_record_group_id=parent.external_record_group_id,
            external_revision_id=info.etag,
            version=0,
            origin=OriginTypes.CONNECTOR,
            connector_name=self.connector_name,
            connector_id=self.connector_id,
            mime_type=info.mime_type,
            weburl=info.web_url,
            source_created_at=info.created_ms,
            source_updated_at=info.modified_ms,
            size_in_bytes=info.size,
            is_file=True,
            extension=derive_attachment_extension(info.name, info.mime_type),
            etag=info.etag,
            ctag=info.ctag,
            quick_xor_hash=info.quick_xor_hash,
            crc32_hash=info.crc32_hash,
            sha1_hash=info.sha1_hash,
            sha256_hash=info.sha256_hash,
            inherit_permissions=False,
            is_dependent_node=True,
            parent_node_id=parent.id,
        )
        return self._apply_attachment_indexing(record)

    def _build_hosted_image_record(self, image: HostedImage, parent: MessageRecord) -> FileRecord:
        record = FileRecord(
            org_id=self.data_entities_processor.org_id,
            record_name=image.file_name(),
            record_type=RecordType.FILE,
            record_group_type=RecordGroupType.TEAMS_CHANNEL,
            parent_record_type=RecordType.MESSAGE,
            parent_external_record_id=parent.external_record_id,
            external_record_id=hosted_external_id(image.graph_path),
            external_record_group_id=parent.external_record_group_id,
            external_revision_id=image.hosted_id,  # hosted content is immutable
            version=0,
            origin=OriginTypes.CONNECTOR,
            connector_name=self.connector_name,
            connector_id=self.connector_id,
            mime_type=HOSTED_IMAGE_MIME_TYPE,
            weburl=parent.weburl,
            source_created_at=parent.source_created_at,
            source_updated_at=parent.source_created_at,
            is_file=True,
            extension=HOSTED_IMAGE_EXTENSION,
            inherit_permissions=False,
            is_dependent_node=True,
            parent_node_id=parent.id,
        )
        return self._apply_attachment_indexing(record)

    def _apply_attachment_indexing(self, record: FileRecord) -> FileRecord:
        if not self._index_attachments():
            record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value
        return record

    def _build_thread_record(self, team_id: str, team_name: str, channel_id: str, channel_name: str, thread: Thread) -> MessageRecord:
        root = thread.root
        markdown = render_thread_markdown(team_name, channel_name, thread)
        return MessageRecord(
            org_id=self.data_entities_processor.org_id,
            record_name=thread_title(thread, channel_name),
            record_type=RecordType.MESSAGE,
            record_group_type=RecordGroupType.TEAMS_CHANNEL,
            external_record_id=thread_external_id(team_id, channel_id, root.id),
            external_record_group_id=channel_record_group_external_id(team_id, channel_id),
            external_revision_id=thread_revision(thread),
            version=0,
            origin=OriginTypes.CONNECTOR,
            connector_name=self.connector_name,
            connector_id=self.connector_id,
            mime_type=MimeTypes.MARKDOWN.value,
            weburl=root.web_url,
            source_created_at=root.created_ms,
            source_updated_at=thread.last_activity_ms,
            inherit_permissions=False,
            preview_renderable=False,
            size_in_bytes=len(markdown.encode("utf-8")),
            content=markdown,
            thread_id=root.id,
            has_replies=bool(thread.replies),
            is_edited=root.edited_ms is not None,
            author_id=root.author_id,
            author_email=self._user_email_by_id.get(root.author_id) if root.author_id else None,
            mentioned_user_ids=thread_mentioned_user_ids(thread),
            involved_user_source_ids=thread_participant_ids(thread),
        )

    # ------------------------------------------------------------------
    # Chats
    # ------------------------------------------------------------------

    async def _sync_chats(self, *, incremental: bool) -> None:
        """One rolling record per chat.  App-only Graph has no tenant-wide ``/chats``
        listing, so chats are discovered through ``/users/{id}/chats`` of every
        synced user (O(users) calls — the filter defaults to off)."""
        lookback_days = self._chat_lookback_days()
        now_ms = get_epoch_timestamp_in_ms()
        window_start_ms = lookback_start_ms(now_ms, lookback_days)
        point = await self.records_sync_point.read_sync_point(CHATS_SYNC_POINT_KEY) if incremental else {}
        last_sync_ms = read_last_sync_ms(point)

        personal = self._is_personal()
        await self.data_entities_processor.on_new_record_groups([(
            RecordGroup(
                name=CHATS_RECORD_GROUP_NAME,
                short_name=CHATS_RECORD_GROUP_NAME,
                description=(
                    f"Microsoft Teams chats of {self.creator_email} (personal connector)"
                    if personal
                    else "Microsoft Teams 1:1 and group chats (each record readable only by its participants)"
                ),
                external_group_id=CHATS_RECORD_GROUP_ID,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                group_type=RecordGroupType.TEAMS_CHANNEL,
                org_id=self.data_entities_processor.org_id,
            ),
            self._personal_permissions() if personal else [],
        )])

        chats_by_id: dict[str, dict[str, Any]] = {}
        if personal:
            for chat in await self._fetch_all(me_chats_url()):
                if chat.get("id"):
                    chats_by_id.setdefault(str(chat["id"]), chat)
            self.logger.info("Discovered %d chat(s) for %s", len(chats_by_id), self.creator_email)
        else:
            for user_id in list(self._users_by_id):
                try:
                    for chat in await self._fetch_all(user_chats_url(user_id)):
                        if chat.get("id"):
                            chats_by_id.setdefault(str(chat["id"]), chat)
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                        continue  # user without a Teams license
                    raise
            self.logger.info("Discovered %d chat(s) across %d user(s)", len(chats_by_id), len(self._users_by_id))

        total = 0
        deleted = 0
        batch: list[tuple[Record, list[Permission]]] = []
        reconcile: list[tuple[str, set[str]]] = []
        for chat_id, chat in chats_by_id.items():
            since = max(window_start_ms, last_sync_ms) if last_sync_ms is not None else window_start_ms
            raw_messages = await self._fetch_all(chat_messages_url(chat_id, since_ms=since))
            if incremental and last_sync_ms is not None:
                if not raw_messages:
                    continue  # nothing changed since the last run (edits and deletes bump lastModifiedDateTime)
                raw_messages = await self._fetch_all(chat_messages_url(chat_id, since_ms=window_start_ms))
            members = parse_conversation_members(chat.get("members") or [])
            for member in members:  # roster emails resolve author_email when no directory sync ran
                if member.email:
                    self._user_email_by_id.setdefault(member.user_id, member.email)
            messages = select_chat_messages(raw_messages, window_start_ms)
            if not messages:
                # every message in the window was deleted or aged out: a stored record would be stale
                deleted += await self._delete_record_tree(chat_external_id(chat_id))
                continue
            permissions = self._personal_permissions() if personal else grants_to_permissions(chat_grants(members))
            record = self._build_chat_record(chat, members, messages, lookback_days)
            children = await self._attachment_records(record, messages)
            batch.append((record, permissions))
            batch.extend((child, list(permissions)) for child in children)
            reconcile.append((record.external_record_id, {child.external_record_id for child in children}))
            if len(reconcile) >= _RECORD_BATCH_SIZE:
                total += await self._flush_records(batch, reconcile)
                batch, reconcile = [], []
        total += await self._flush_records(batch, reconcile)
        await self.records_sync_point.update_sync_point(CHATS_SYNC_POINT_KEY, {"lastSyncTimestamp": now_ms})
        self.logger.info(
            "Synced %d chat record(s) (%d deleted)%s", total, deleted, " (incremental)" if incremental else ""
        )

    def _build_chat_record(self, chat: dict[str, Any], members: list[Member], messages: list[MessageView], lookback_days: int) -> MessageRecord:
        markdown = render_chat_markdown(chat, members, messages, lookback_days)
        authors: list[str] = []
        for view in messages:
            if view.author_id and view.author_id not in authors:
                authors.append(view.author_id)
        mentioned: list[str] = []
        for view in messages:
            for mention in view.mentions:
                if mention.user_id and mention.user_id not in mentioned:
                    mentioned.append(mention.user_id)
        return MessageRecord(
            org_id=self.data_entities_processor.org_id,
            record_name=chat_title(chat, members),
            record_type=RecordType.MESSAGE,
            record_group_type=RecordGroupType.TEAMS_CHANNEL,
            external_record_id=chat_external_id(str(chat["id"])),
            external_record_group_id=CHATS_RECORD_GROUP_ID,
            external_revision_id=chat_revision(messages),
            version=0,
            origin=OriginTypes.CONNECTOR,
            connector_name=self.connector_name,
            connector_id=self.connector_id,
            mime_type=MimeTypes.MARKDOWN.value,
            weburl=chat.get("webUrl"),
            source_created_at=messages[0].created_ms,
            source_updated_at=messages[-1].last_activity_ms,
            inherit_permissions=False,
            preview_renderable=False,
            size_in_bytes=len(markdown.encode("utf-8")),
            content=markdown,
            thread_id=str(chat["id"]),
            has_replies=len(messages) > 1,
            author_id=messages[0].author_id,
            author_email=self._user_email_by_id.get(messages[0].author_id) if messages[0].author_id else None,
            mentioned_user_ids=mentioned,
            involved_user_source_ids=authors,
        )

    # ------------------------------------------------------------------
    # Streaming / reindex
    # ------------------------------------------------------------------

    async def get_signed_url(self, record: Record) -> str | None:
        """Shared files download from ``@microsoft.graph.downloadUrl`` (as in OneDrive);
        messages and pasted images are served from Graph on demand."""
        if record.record_type != RecordType.FILE:
            return None
        kind, parts = split_external_id(record.external_record_id)
        if kind != FILE_ID_PREFIX:
            return None
        item = await self._get_json(drive_item_url(*parts))
        return item.get("@microsoft.graph.downloadUrl") or None

    async def _stream_graph_bytes(self, path: str) -> AsyncGenerator[bytes, None]:
        """Bearer-authenticated download for content Graph serves itself (hosted images)."""
        if self._http is None:
            raise RuntimeError("Microsoft Teams connector not initialised")
        for attempt in range(2):
            headers = {"Authorization": f"Bearer {await self._get_token()}"}
            async with self._request_semaphore, self._http.stream("GET", path, headers=headers) as response:
                if response.status_code == HttpStatusCode.UNAUTHORIZED.value and attempt == 0:
                    await self._refresh_token()
                    continue
                if response.status_code == HttpStatusCode.NOT_FOUND.value:
                    raise HTTPException(status_code=HttpStatusCode.NOT_FOUND.value, detail="Hosted content no longer exists")
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    yield chunk
                return

    async def _stream_file(self, record: Record) -> StreamingResponse:
        kind, parts = split_external_id(record.external_record_id)
        extension = getattr(record, "extension", None) or "bin"
        if kind == FILE_ID_PREFIX:
            try:
                download_url = await self.get_signed_url(record)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                    raise HTTPException(
                        status_code=HttpStatusCode.NOT_FOUND.value, detail="File no longer exists in SharePoint / OneDrive"
                    ) from e
                raise
            if not download_url:
                raise HTTPException(status_code=HttpStatusCode.NOT_FOUND.value, detail="File not found or access denied")
            stream = stream_content(download_url, record.id, record.record_name)
        elif kind == HOSTED_ID_PREFIX:
            stream = self._stream_graph_bytes(hosted_content_value_url(parts[0]))
        else:
            raise HTTPException(status_code=HttpStatusCode.BAD_REQUEST.value, detail=f"Unsupported Microsoft Teams file kind: {kind}")
        return create_stream_record_response(
            stream,
            filename=record.record_name,
            mime_type=record.mime_type,
            fallback_filename=f"record_{record.id}.{extension}",
        )

    async def _team_name(self, team_id: str) -> str:
        if team_id not in self._team_names:
            try:
                team = await self._get_json(f"teams/{team_id}", params={"$select": "id,displayName"})
                self._team_names[team_id] = team_display_name(team)
            except Exception:
                return team_id
        return self._team_names[team_id]

    async def _channel_name(self, team_id: str, channel_id: str) -> str:
        key = (team_id, channel_id)
        if key not in self._channel_names:
            try:
                channel = await self._get_json(f"teams/{team_id}/channels/{channel_id}", params={"$select": "id,displayName"})
                self._channel_names[key] = channel_display_name(channel)
            except Exception:
                return channel_id
        return self._channel_names[key]

    async def _load_thread(self, team_id: str, channel_id: str, message_id: str) -> Thread | None:
        try:
            root = await self._get_json(channel_message_url(team_id, channel_id, message_id))
            replies = await self._fetch_all(message_replies_url(team_id, channel_id, message_id))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                return None
            raise
        return build_thread(root, replies)

    async def _render_record(self, record: Record) -> tuple[str, str | None]:
        """Return ``(markdown, revision)``; revision is ``None`` when the source is gone."""
        kind, parts = split_external_id(record.external_record_id)
        if kind == THREAD_ID_PREFIX:
            team_id, channel_id, message_id = parts
            thread = await self._load_thread(team_id, channel_id, message_id)
            if thread is None or not thread.root.is_indexable:
                return f"# {record.record_name}\n\nThis Microsoft Teams message no longer exists.\n", None
            team_name = await self._team_name(team_id)
            channel_name = await self._channel_name(team_id, channel_id)
            return render_thread_markdown(team_name, channel_name, thread), thread_revision(thread)
        if kind == CHAT_ID_PREFIX:
            (chat_id,) = parts
            lookback_days = self._chat_lookback_days()
            window_start_ms = lookback_start_ms(get_epoch_timestamp_in_ms(), lookback_days)
            try:
                chat = await self._get_json(f"chats/{chat_id}", params={"$expand": "members"})
                raw_messages = await self._fetch_all(chat_messages_url(chat_id, since_ms=window_start_ms))
            except httpx.HTTPStatusError as e:
                if e.response.status_code == HttpStatusCode.NOT_FOUND.value:
                    return f"# {record.record_name}\n\nThis Microsoft Teams chat no longer exists.\n", None
                raise
            members = parse_conversation_members(chat.get("members") or [])
            messages = select_chat_messages(raw_messages, window_start_ms)
            return render_chat_markdown(chat, members, messages, lookback_days), chat_revision(messages)
        raise ValueError(f"Unsupported Microsoft Teams record kind: {kind}")

    async def stream_record(self, record: Record, user_id: str | None = None, convertTo: str | None = None) -> StreamingResponse:
        if record.record_type == RecordType.FILE:
            return await self._stream_file(record)
        markdown, _ = await self._render_record(record)
        return create_stream_record_response(
            _bytes_stream(markdown.encode("utf-8")),
            filename=f"{record.record_name}.md",
            mime_type=MimeTypes.MARKDOWN.value,
            fallback_filename=f"record_{record.id}.md",
        )

    async def reindex_records(self, record_results: list[Record]) -> None:
        """Rebuild records whose thread/chat changed since the stored revision; reindex the rest as-is."""
        if not record_results:
            return
        unchanged: list[Record] = []
        for record in record_results:
            if record.record_type == RecordType.FILE:
                unchanged.append(record)  # file bytes are re-streamed from Graph; nothing to re-render
                continue
            try:
                markdown, revision = await self._render_record(record)
            except (ValueError, _GraphForbidden) as e:
                self.logger.warning("Cannot refresh Teams record %s (%s); reindexing stored copy", record.external_record_id, e)
                unchanged.append(record)
                continue
            if revision is None or revision == record.external_revision_id:
                unchanged.append(record)
                continue
            record.external_revision_id = revision
            record.size_in_bytes = len(markdown.encode("utf-8"))
            if isinstance(record, MessageRecord):
                record.content = markdown
            await self.data_entities_processor.on_record_content_update(record)
        if unchanged:
            await self.data_entities_processor.reindex_existing_records(unchanged)

    # ------------------------------------------------------------------
    # Webhooks / filters
    # ------------------------------------------------------------------

    async def handle_webhook_notification(self, notification: dict[str, Any]) -> bool:
        """Graph change notifications for chatMessage are not wired yet; acknowledge like Outlook does."""
        return True

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: str | None = None,
        cursor: str | None = None,
    ) -> FilterOptionsResponse:
        if filter_key != TEAMS_FILTER_KEY:
            raise ValueError(f"Unsupported filter key: {filter_key}")
        if self._http is None:
            return FilterOptionsResponse(success=False, options=[], page=page, limit=limit, has_more=False)
        if self._is_personal():
            # teams / channels are not synced in personal scope (chats only)
            return FilterOptionsResponse(success=True, options=[], page=page, limit=limit, has_more=False)
        needle = (search or "").strip().lower()
        teams = await self._list_teams()
        options = [
            FilterOption(id=str(team["id"]), label=team_display_name(team))
            for team in sorted(teams, key=team_display_name)
            if team.get("id") and (not needle or needle in team_display_name(team).lower())
        ]
        start = max(page - 1, 0) * limit
        chunk = options[start:start + limit]
        return FilterOptionsResponse(success=True, options=chunk, page=page, limit=limit, has_more=start + limit < len(options))


async def _bytes_stream(data: bytes) -> AsyncGenerator[bytes, None]:
    yield data
