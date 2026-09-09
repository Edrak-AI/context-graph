"""Tests for app.connectors.sources.microsoft.common.entra_identity.

``primary_smtp_address`` is pure; ``EntraUserEmailResolver`` is exercised against an
``httpx.MockTransport`` (token endpoint + ``directoryObjects/getByIds``), so no network.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

import httpx

from app.connectors.sources.microsoft.common import entra_identity
from app.connectors.sources.microsoft.common.entra_identity import (
    USER_EMAIL_SELECT,
    EntraUserEmailResolver,
    graph_user_email,
    primary_smtp_address,
)

if TYPE_CHECKING:
    import pytest

LOGGER = logging.getLogger("test-entra")


class TestPrimarySmtpAddress:
    def test_primary_smtp_wins_over_mail_and_upn(self) -> None:
        assert primary_smtp_address(
            "alias@contoso.com",
            "sujit@contoso.onmicrosoft.com",
            ["smtp:old@contoso.com", "SMTP:Sujit@Contoso.com", "SIP:sujit@contoso.com"],
        ) == "sujit@contoso.com"

    def test_lowercase_smtp_entries_are_aliases_not_primary(self) -> None:
        assert primary_smtp_address("mail@contoso.com", "upn@contoso.onmicrosoft.com", ["smtp:alias@contoso.com"]) == "mail@contoso.com"

    def test_mail_before_upn(self) -> None:
        assert primary_smtp_address("Mail@Contoso.com ", "upn@contoso.onmicrosoft.com", None) == "mail@contoso.com"

    def test_upn_when_no_mailbox(self) -> None:
        assert primary_smtp_address(None, "upn@contoso.onmicrosoft.com", []) == "upn@contoso.onmicrosoft.com"

    def test_none_when_nothing_is_an_address(self) -> None:
        assert primary_smtp_address("", "not-an-email", ["SMTP:"]) is None

    def test_graph_user_email_tolerates_bad_shapes(self) -> None:
        assert graph_user_email({"mail": "a@b.com", "proxyAddresses": "SMTP:x@y.com"}) == "a@b.com"
        assert graph_user_email({}) is None


# ---------------------------------------------------------------------------
# Resolver against a mocked Graph
# ---------------------------------------------------------------------------

TOKEN_URL = "https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token"
GET_BY_IDS = "https://graph.microsoft.com/v1.0/directoryObjects/getByIds"

USERS: dict[str, dict[str, Any]] = {
    "aad-1": {"id": "aad-1", "mail": "alias@edrak.com", "userPrincipalName": "sujit@edrak.onmicrosoft.com",
              "proxyAddresses": ["smtp:alias@edrak.com", "SMTP:Sujit@edrak.com"]},
    "aad-2": {"id": "aad-2", "mail": None, "userPrincipalName": "Nour@edrak.onmicrosoft.com", "proxyAddresses": []},
    "aad-3": {"id": "aad-3", "userPrincipalName": "no-at-sign"},
}


class Graph:
    """Scripted Graph: records calls; ``statuses`` pops one status per getByIds call (default 200)."""

    def __init__(self, statuses: list[int] | None = None) -> None:
        self.calls: list[httpx.Request] = []
        self.statuses = list(statuses or [])
        self.tokens_issued = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if str(request.url) == TOKEN_URL:
            self.tokens_issued += 1
            return httpx.Response(200, json={"access_token": f"tok-{self.tokens_issued}", "expires_in": 3600})
        assert str(request.url).startswith(GET_BY_IDS), request.url
        assert request.headers["Authorization"].startswith("Bearer tok-")
        status = self.statuses.pop(0) if self.statuses else 200
        if status != 200:
            return httpx.Response(status, json={"error": {"code": "Authorization_RequestDenied"}}, headers={"Retry-After": "2"} if status == 429 else {})
        ids = json.loads(request.content)["ids"]
        return httpx.Response(200, json={"value": [USERS[i] for i in ids if i in USERS]})

    def get_by_ids_calls(self) -> list[httpx.Request]:
        return [c for c in self.calls if str(c.url).startswith(GET_BY_IDS)]


def _resolver(graph: Graph) -> EntraUserEmailResolver:
    http = httpx.AsyncClient(transport=httpx.MockTransport(graph.handler))
    return EntraUserEmailResolver("tenant-1", "client", "secret", LOGGER, http=http)


class TestEntraUserEmailResolver:
    def test_resolves_primary_addresses_and_caches(self) -> None:
        graph = Graph()
        r = _resolver(graph)
        result = asyncio.run(r.resolve_emails(["aad-1", "aad-2", "aad-3", "aad-missing", "aad-1", ""]))
        assert result == {"aad-1": "sujit@edrak.com", "aad-2": "nour@edrak.onmicrosoft.com"}

        call = graph.get_by_ids_calls()[0]
        assert call.url.params["$select"] == USER_EMAIL_SELECT
        assert json.loads(call.content) == {"ids": ["aad-1", "aad-2", "aad-3", "aad-missing"], "types": ["user"]}

        # Every id is cached (including the ones Graph did not return): no second round-trip.
        again = asyncio.run(r.resolve_emails(["aad-1", "aad-missing", "aad-3"]))
        assert again == {"aad-1": "sujit@edrak.com"}
        assert len(graph.get_by_ids_calls()) == 1 and graph.tokens_issued == 1

    def test_chunks_at_graph_limit(self) -> None:
        graph = Graph()
        ids = [f"id-{i}" for i in range(entra_identity._GET_BY_IDS_MAX + 1)]
        asyncio.run(_resolver(graph).resolve_emails(ids))
        sizes = [len(json.loads(c.content)["ids"]) for c in graph.get_by_ids_calls()]
        assert sizes == [entra_identity._GET_BY_IDS_MAX, 1]

    def test_forbidden_marks_unavailable_and_stops_calling(self, caplog: pytest.LogCaptureFixture) -> None:
        graph = Graph(statuses=[403])
        r = _resolver(graph)
        with caplog.at_level(logging.WARNING, logger="test-entra"):
            assert asyncio.run(r.resolve_emails(["aad-1"])) == {}
        assert r.unavailable is True
        assert "User.Read.All" in caplog.text
        assert asyncio.run(r.resolve_emails(["aad-2"])) == {}
        assert len(graph.get_by_ids_calls()) == 1

    def test_server_error_falls_back_without_disabling(self) -> None:
        graph = Graph(statuses=[500])
        r = _resolver(graph)
        assert asyncio.run(r.resolve_emails(["aad-1"])) == {}
        assert r.unavailable is False
        assert asyncio.run(r.resolve_emails(["aad-1"])) == {"aad-1": "sujit@edrak.com"}

    def test_throttling_honours_retry_after(self, monkeypatch: pytest.MonkeyPatch) -> None:
        waits: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            waits.append(seconds)

        monkeypatch.setattr(entra_identity.asyncio, "sleep", fake_sleep)
        graph = Graph(statuses=[429, 200])
        assert asyncio.run(_resolver(graph).resolve_emails(["aad-1"])) == {"aad-1": "sujit@edrak.com"}
        assert waits == [2.0]
        assert len(graph.get_by_ids_calls()) == 2
