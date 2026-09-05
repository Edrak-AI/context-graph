"""Delegated (per-user OAuth) token plumbing shared by the personal scope of the
OneDrive / Outlook / Microsoft Teams connectors.

The OAuth callback (``connectors/api/router.py``) stores the user's tokens on the
connector-instance config under ``credentials.access_token`` /
``credentials.refresh_token`` and the background ``TokenRefreshService`` keeps
them fresh using the org's shared OAuth app (client id / secret live on
``/services/oauth/<connector>``, referenced by ``auth.oauthConfigId``).

``DelegatedTokenProvider`` re-reads the stored access token (short cache) on
each request, so a refresh that lands mid-sync is picked up without rebuilding
clients (the same idea as ``OutlookIndividualConnector._get_fresh_graph_client``
but without the compare-and-rebuild dance), and can ask the refresh service for
an on-demand refresh after a 401 (used by the Teams httpx path).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

from kiota_abstractions.authentication import (
    AccessTokenProvider,
    AllowedHostsValidator,
    BaseBearerTokenAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from msgraph import GraphServiceClient

from app.config.configuration_service import ConfigurationService
from app.connectors.core.constants import ConfigPaths, OAuthConfigKeys
from app.connectors.sources.microsoft.common.personal_scope import (
    user_oid_from_access_token,
)
from app.sources.client.microsoft.microsoft import GraphMode

GRAPH_ALLOWED_HOSTS = [
    "graph.microsoft.com",
    "graph.microsoft.us",
    "dod-graph.microsoft.us",
    "microsoftgraph.chinacloudapi.cn",
    "canary.graph.microsoft.com",
]


# ``ConfigurationService.get_config`` reads the KV store on every call; a short cache keeps a
# paginated sync from hammering it while still noticing a background refresh within seconds.
_TOKEN_CACHE_TTL_S = 30.0


class DelegatedTokenProvider(AccessTokenProvider):
    """Kiota ``AccessTokenProvider`` backed by the connector-instance config."""

    def __init__(
        self,
        config_service: ConfigurationService,
        connector_id: str,
        connector_type: str,
        logger: logging.Logger,
        cache_ttl_s: float = _TOKEN_CACHE_TTL_S,
    ) -> None:
        self.config_service = config_service
        self.connector_id = connector_id
        self.connector_type = connector_type
        self.logger = logger
        self._config_path = ConfigPaths.CONNECTOR_CONFIG.format(connector_id=connector_id)
        self._cache_ttl_s = cache_ttl_s
        self._last_token: Optional[str] = None
        self._last_read_at: float = 0.0

    async def _credentials(self) -> dict[str, Any]:
        config = await self.config_service.get_config(self._config_path)
        return ((config or {}).get(OAuthConfigKeys.CREDENTIALS) or {}) if isinstance(config, dict) else {}

    def invalidate(self) -> None:
        """Drop the cached token so the next call re-reads the config (e.g. after a 401)."""
        self._last_read_at = 0.0

    async def get_token(self) -> str:
        """Current delegated access token (raises when the OAuth flow was never completed)."""
        now = time.monotonic()
        if self._last_token and now - self._last_read_at < self._cache_ttl_s:
            return self._last_token
        token = (await self._credentials()).get(OAuthConfigKeys.ACCESS_TOKEN)
        if not token:
            raise ValueError(
                f"No delegated access token stored for connector {self.connector_id}. "
                "Complete the OAuth sign-in for this personal connector first."
            )
        self._last_token = str(token)
        self._last_read_at = now
        return self._last_token

    async def refresh(self) -> Optional[str]:
        """Best-effort on-demand refresh through the shared ``TokenRefreshService``.

        Returns the new access token, or ``None`` when no refresh token / service is
        available (the caller then simply retries with whatever is stored).
        """
        self.invalidate()
        refresh_token = (await self._credentials()).get(OAuthConfigKeys.REFRESH_TOKEN)
        if not refresh_token:
            return None
        try:
            from app.connectors.core.base.token_service.startup_service import (
                startup_service,
            )

            service = startup_service.get_token_refresh_service()
            if service is None:
                return None
            await service.refresh_now(self.connector_id, self.connector_type, refresh_token)
        except Exception as e:  # noqa: BLE001 - refresh is opportunistic
            self.logger.warning(
                "On-demand token refresh failed for connector %s: %s", self.connector_id, str(e)[:200]
            )
            return None
        return await self.get_token()

    @property
    def last_token(self) -> Optional[str]:
        return self._last_token

    def user_oid(self) -> Optional[str]:
        """Entra object id of the signed-in user, from the last token read."""
        return user_oid_from_access_token(self._last_token)

    # -- kiota AccessTokenProvider interface ---------------------------------

    async def get_authorization_token(
        self,
        uri: str,
        additional_authentication_context: Optional[dict[str, Any]] = None,
    ) -> str:
        return await self.get_token()

    def get_allowed_hosts_validator(self) -> AllowedHostsValidator:
        return AllowedHostsValidator(GRAPH_ALLOWED_HOSTS)


def build_delegated_graph_client(provider: DelegatedTokenProvider) -> GraphServiceClient:
    """``GraphServiceClient`` whose bearer token is resolved per request by ``provider``."""
    adapter = HttpxRequestAdapter(
        authentication_provider=BaseBearerTokenAuthenticationProvider(provider)
    )
    return GraphServiceClient(request_adapter=adapter)


class DelegatedGraphClientHandle:
    """Minimal stand-in for ``MSGraphClientWithDelegatedAuth`` so the external
    data sources (``OutlookCalendarContactsDataSource`` etc.) can be fed a
    ``GraphServiceClient`` that refreshes its token on its own.

    Exposes the three members the wrappers touch: ``get_ms_graph_service_client``,
    ``get_mode`` and ``close``.
    """

    def __init__(self, client: GraphServiceClient) -> None:
        self.client = client
        self.mode = GraphMode.DELEGATED

    def get_ms_graph_service_client(self) -> GraphServiceClient:
        return self.client

    def get_mode(self) -> GraphMode:
        return self.mode

    async def close(self) -> None:
        return None
