"""Microsoft Graph identity helpers shared by the app-only Microsoft connectors.

CGraph links a connector's ``AppUser`` to the person's platform account by e-mail,
and the platform account is created with the address they sign in to Edrak with
(their "official" address).  Dataverse and Business Central store their own copy
of a user's address (often the ``@<tenant>.onmicrosoft.com`` UPN), so matching on
that alone silently drops permissions.  ``primary_smtp_address`` picks the address
Entra treats as primary — the ``SMTP:`` (upper-case) entry of ``proxyAddresses``,
then ``mail``, then the UPN — and ``EntraUserEmailResolver`` fetches those fields
for a batch of Entra object ids through ``POST /directoryObjects/getByIds``.

Graph calls need the *application* permission ``User.Read.All`` (admin consent).
When the tenant has not granted it Graph answers 401/403; the resolver then logs
once, marks itself unavailable and returns nothing, so callers fall back to the
address their own API supplied.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

import httpx

from app.config.constants.http_status_code import HttpStatusCode
from app.connectors.sources.sap.mapping import retry_delay
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from logging import Logger

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
USER_EMAIL_SELECT = "id,mail,userPrincipalName,proxyAddresses,accountEnabled"

_MAX_HTTP_RETRIES = 5
_RETRY_STATUS = {HttpStatusCode.TOO_MANY_REQUESTS.value, 502, HttpStatusCode.SERVICE_UNAVAILABLE.value, 504}
_TOKEN_REFRESH_SKEW_S = 120
_GET_BY_IDS_MAX = 1000  # Graph's documented limit per getByIds call
_PRIMARY_SMTP_PREFIX = "SMTP:"  # upper-case = primary; "smtp:" entries are aliases
_DENIED_STATUS = {HttpStatusCode.UNAUTHORIZED.value, HttpStatusCode.FORBIDDEN.value}


def _clean_email(value: object) -> str | None:
    if value and "@" in str(value):
        return str(value).strip().lower()
    return None


def primary_smtp_address(
    mail: object,
    user_principal_name: object,
    proxy_addresses: Iterable[object] | None = None,
) -> str | None:
    """The address Entra marks as primary, else ``mail``, else the UPN (lower-cased)."""
    for entry in proxy_addresses or ():
        text = str(entry or "")
        if text.startswith(_PRIMARY_SMTP_PREFIX):
            email = _clean_email(text[len(_PRIMARY_SMTP_PREFIX):])
            if email:
                return email
    return _clean_email(mail) or _clean_email(user_principal_name)


def graph_user_email(user: Mapping[str, Any]) -> str | None:
    """``primary_smtp_address`` over a Graph ``user`` JSON object."""
    proxies = user.get("proxyAddresses")
    return primary_smtp_address(
        user.get("mail"),
        user.get("userPrincipalName"),
        proxies if isinstance(proxies, list) else None,
    )


class EntraGraphClient:
    """App-only Microsoft Graph client: client-credentials token, retries on 429/5xx."""

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        logger: Logger,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._logger = logger
        self._http = http or httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=15.0))
        self._token: str | None = None
        self._token_expires_on: int = 0

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._http.aclose()

    async def _token_header(self) -> dict[str, str]:
        now_s = get_epoch_timestamp_in_ms() // 1000
        if not self._token or now_s >= self._token_expires_on - _TOKEN_REFRESH_SKEW_S:
            response = await self._http.post(
                f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": GRAPH_SCOPE,
                },
            )
            response.raise_for_status()
            payload = response.json()
            self._token = str(payload["access_token"])
            self._token_expires_on = now_s + int(payload.get("expires_in") or 3600)
        return {"Authorization": f"Bearer {self._token}"}

    async def _request(
        self,
        method: str,
        url: str,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        delay = 1.0
        for attempt in range(_MAX_HTTP_RETRIES + 1):
            response = await self._http.request(method, url, params=params, json=json, headers=await self._token_header())
            if response.status_code in _RETRY_STATUS and attempt < _MAX_HTTP_RETRIES:
                await asyncio.sleep(retry_delay(response.headers.get("Retry-After"), delay))
                delay = min(delay * 2, 30.0)
                continue
            response.raise_for_status()
            return response.json()
        raise RuntimeError(f"Graph request to {url} exhausted retries")

    async def _get(self, url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        return await self._request("GET", url, params=params)

    async def _post(self, url: str, json: dict[str, Any], params: dict[str, str] | None = None) -> dict[str, Any]:
        return await self._request("POST", url, params=params, json=json)


class EntraUserEmailResolver(EntraGraphClient):
    """Entra object id → primary e-mail, batched and cached for one connector sync.

    ``unavailable`` flips to ``True`` on the first 401/403 (missing ``User.Read.All``
    consent) and every later call returns ``{}`` without touching Graph.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        logger: Logger,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(tenant_id, client_id, client_secret, logger, http)
        self._cache: dict[str, str | None] = {}
        self.unavailable = False

    async def resolve_emails(self, object_ids: Sequence[str]) -> dict[str, str]:
        """Return ``{object_id: email}`` for the ids Graph knows; unknown ids are absent."""
        wanted = list(dict.fromkeys(str(i).strip() for i in object_ids if i and str(i).strip()))
        missing = [i for i in wanted if i not in self._cache]
        if missing and not self.unavailable:
            for start in range(0, len(missing), _GET_BY_IDS_MAX):
                chunk = missing[start:start + _GET_BY_IDS_MAX]
                if not await self._fetch_chunk(chunk):
                    break
        return {i: email for i in wanted if (email := self._cache.get(i))}

    async def _fetch_chunk(self, ids: list[str]) -> bool:
        try:
            payload = await self._post(
                f"{GRAPH_BASE_URL}/directoryObjects/getByIds",
                {"ids": ids, "types": ["user"]},
                params={"$select": USER_EMAIL_SELECT},
            )
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status in _DENIED_STATUS:
                self.unavailable = True
                self._logger.warning(
                    "Microsoft Graph refused the user lookup (HTTP %s); grant the app registration the "
                    "application permission User.Read.All to match people by their primary e-mail. "
                    "Falling back to the addresses stored in the connected app.", status,
                )
            else:
                self._logger.error("Microsoft Graph user lookup failed (HTTP %s)", status)
            return False
        except (httpx.TransportError, RuntimeError, KeyError, ValueError) as e:
            self._logger.error("Microsoft Graph user lookup failed: %s", e)
            return False

        found: dict[str, str | None] = {}
        for user in payload.get("value") or []:
            if isinstance(user, dict) and user.get("id"):
                found[str(user["id"])] = graph_user_email(user)
        for object_id in ids:
            self._cache[object_id] = found.get(object_id)
        return True
