"""Microsoft Graph identity helpers shared by the app-only Microsoft connectors.

CGraph links a connector's ``AppUser`` to the person's platform account by e-mail,
and the platform account is created with the address they sign in to Edrak with
(their "official" address).  Dataverse and Business Central store their own copy
of a user's address (often the ``@<tenant>.onmicrosoft.com`` UPN), so matching on
that alone silently drops permissions.  ``primary_smtp_address`` picks the address
Entra treats as primary — the ``SMTP:`` (upper-case) entry of ``proxyAddresses``,
then ``mail``, then the UPN — and ``alternate_addresses`` collects every other
address the directory lists (``smtp:`` aliases, ``otherMails``, the UPN) so the
graph can link the person even when they sign in to Edrak with an alias domain.
``EntraUserEmailResolver`` fetches those fields for a batch of Entra object ids
through ``POST /directoryObjects/getByIds``.

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
USER_EMAIL_SELECT = "id,mail,userPrincipalName,proxyAddresses,otherMails,accountEnabled"

_MAX_HTTP_RETRIES = 5
_RETRY_STATUS = {HttpStatusCode.TOO_MANY_REQUESTS.value, 502, HttpStatusCode.SERVICE_UNAVAILABLE.value, 504}
_TOKEN_REFRESH_SKEW_S = 120
_GET_BY_IDS_MAX = 1000  # Graph's documented limit per getByIds call
_PRIMARY_SMTP_PREFIX = "SMTP:"  # upper-case = primary; "smtp:" entries are aliases
_SMTP_PREFIX_LEN = len(_PRIMARY_SMTP_PREFIX)
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


def alternate_addresses(
    mail: object,
    user_principal_name: object,
    proxy_addresses: Iterable[object] | None = None,
    other_mails: Iterable[object] | None = None,
) -> list[str]:
    """Every address of the user other than ``primary_smtp_address`` (lower-cased, deduped, in directory order)."""
    primary = primary_smtp_address(mail, user_principal_name, proxy_addresses)
    candidates: list[object] = []
    for entry in proxy_addresses or ():
        text = str(entry or "")
        if text[:_SMTP_PREFIX_LEN].upper() == _PRIMARY_SMTP_PREFIX:
            candidates.append(text[_SMTP_PREFIX_LEN:])
    candidates.extend(other_mails or ())
    candidates.extend((mail, user_principal_name))
    result: list[str] = []
    for candidate in candidates:
        email = _clean_email(candidate)
        if email and email != primary and email not in result:
            result.append(email)
    return result


def _string_list(value: object) -> list[object] | None:
    return value if isinstance(value, list) else None


def graph_user_email(user: Mapping[str, Any]) -> str | None:
    """``primary_smtp_address`` over a Graph ``user`` JSON object."""
    return primary_smtp_address(user.get("mail"), user.get("userPrincipalName"), _string_list(user.get("proxyAddresses")))


def graph_user_identity(user: Mapping[str, Any]) -> tuple[str | None, list[str]]:
    """``(primary, alternates)`` over a Graph ``user`` JSON object."""
    proxies = _string_list(user.get("proxyAddresses"))
    return graph_user_email(user), alternate_addresses(
        user.get("mail"), user.get("userPrincipalName"), proxies, _string_list(user.get("otherMails"))
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
    """Entra object id → primary e-mail (+ alternates), batched and cached for one connector sync.

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
        self._cache: dict[str, tuple[str, list[str]] | None] = {}
        self.unavailable = False

    async def resolve_identities(self, object_ids: Sequence[str]) -> dict[str, tuple[str, list[str]]]:
        """Return ``{object_id: (primary email, alternate emails)}`` for the ids Graph knows; unknown ids are absent."""
        wanted = list(dict.fromkeys(str(i).strip() for i in object_ids if i and str(i).strip()))
        missing = [i for i in wanted if i not in self._cache]
        if missing and not self.unavailable:
            for start in range(0, len(missing), _GET_BY_IDS_MAX):
                chunk = missing[start:start + _GET_BY_IDS_MAX]
                if not await self._fetch_chunk(chunk):
                    break
        return {i: identity for i in wanted if (identity := self._cache.get(i))}

    async def resolve_emails(self, object_ids: Sequence[str]) -> dict[str, str]:
        """Return ``{object_id: primary email}`` for the ids Graph knows; unknown ids are absent."""
        return {i: primary for i, (primary, _) in (await self.resolve_identities(object_ids)).items()}

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

        found: dict[str, tuple[str, list[str]] | None] = {}
        for user in payload.get("value") or []:
            if isinstance(user, dict) and user.get("id"):
                primary, alternates = graph_user_identity(user)
                found[str(user["id"])] = (primary, alternates) if primary else None
        for object_id in ids:
            self._cache[object_id] = found.get(object_id)
        return True
