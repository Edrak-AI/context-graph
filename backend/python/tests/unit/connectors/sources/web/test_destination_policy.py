"""SSRF guard of the web crawler (security review S07).

Exercises ``destination_policy.DestinationPolicy`` with a fake resolver — no network —
and the transport bindings in ``fetch_strategy`` / ``crawl4ai_fetcher`` / the connector:
every hop (initial URL, redirect ``Location``, image URL, headless-browser request) is
resolved, *all* addresses validated, and the validated addresses pinned to the
connection; DNS failures and non-public results (IPv4, IPv6, IPv4-mapped ``::ffff:a00:1``)
fail closed; only blocked hosts are cached, briefly.
"""
from __future__ import annotations

import logging
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests

from app.connectors.sources.web import fetch_strategy as fs
from app.connectors.sources.web.crawl4ai_fetcher import guard_browser_request
from app.connectors.sources.web.destination_policy import (
    DestinationBlocked,
    DestinationPolicy,
    PinnedTarget,
    PolicyResolver,
    blocked_address_reason,
)

PUBLIC_IP = "93.184.216.34"
PUBLIC_V6 = "2606:2800:220:1:248:1893:25c8:1946"

HOSTS: dict[str, list[str]] = {
    "public.example": [PUBLIC_IP],
    "public6.example": [PUBLIC_V6],
    "dual.example": [PUBLIC_IP, PUBLIC_V6],
    "intranet.example": ["10.0.0.1"],
    "metadata.example": ["169.254.169.254"],
    "loop6.example": ["::1"],
    "mapped.example": ["::ffff:a00:1"],  # IPv4-mapped 10.0.0.1 in hex form
    "mixed.example": [PUBLIC_IP, "10.0.0.2"],  # one public, one private → blocked
    "cgnat.example": ["100.64.0.1"],
    "nat64.example": ["64:ff9b::a00:1"],
    "sixtofour.example": ["2002:0a00:0001::1"],
    "empty.example": [],
}


class FakeResolver:
    def __init__(self, table: dict[str, list[str]] | None = None) -> None:
        self.table = dict(HOSTS if table is None else table)
        self.calls: list[str] = []

    async def __call__(self, host: str, port: int) -> list[str]:
        self.calls.append(host)
        if host not in self.table:
            raise socket.gaierror(-2, "Name or service not known")
        return list(self.table[host])


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _policy(**kw: object) -> tuple[DestinationPolicy, FakeResolver]:
    resolver = kw.pop("resolver", None) or FakeResolver()
    assert isinstance(resolver, FakeResolver)
    allow_private = bool(kw.pop("allow_private", False))
    return DestinationPolicy(resolver=resolver, allow_private=allow_private, **kw), resolver  # type: ignore[arg-type]


@pytest.fixture
def log() -> logging.Logger:
    return logging.getLogger("test_destination_policy")


# ---------------------------------------------------------------------------
# Address classification
# ---------------------------------------------------------------------------


class TestBlockedAddressReason:
    @pytest.mark.parametrize("address", [PUBLIC_IP, PUBLIC_V6, "8.8.8.8", "2606:4700::1111"])
    def test_public(self, address: str) -> None:
        assert blocked_address_reason(address) is None

    @pytest.mark.parametrize("address", [
        "10.0.0.1", "172.16.5.4", "192.168.1.1", "127.0.0.1", "169.254.169.254", "0.0.0.0",
        "100.64.0.1", "224.0.0.1", "255.255.255.255", "192.0.0.1",
        "::1", "::", "fe80::1", "fc00::1", "ff02::1",
        "::ffff:a00:1", "::ffff:10.0.0.1", "::ffff:7f00:1", "::ffff:169.254.169.254",
        "64:ff9b::a00:1", "2002:0a00:0001::1", "fe80::1%eth0", "[::1]",
    ])
    def test_non_public_and_transition_forms(self, address: str) -> None:
        assert blocked_address_reason(address) is not None

    def test_garbage(self) -> None:
        assert "unparseable" in (blocked_address_reason("not-an-ip") or "")


# ---------------------------------------------------------------------------
# Policy: resolve + validate + cache
# ---------------------------------------------------------------------------


class TestDestinationPolicy:
    async def test_public_host_ok_and_pinned(self) -> None:
        policy, resolver = _policy()
        target = await policy.resolve("https://public.example/page?x=1")
        assert target == PinnedTarget(url="https://public.example/page?x=1", scheme="https", host="public.example", port=443, addresses=(PUBLIC_IP,))
        assert target.primary == PUBLIC_IP and target.host_header == "public.example"
        assert target.curl_resolve_entry() == f"public.example:443:{PUBLIC_IP}"
        assert resolver.calls == ["public.example"]

    async def test_all_addresses_kept_and_pinned(self) -> None:
        policy, _ = _policy()
        target = await policy.resolve("http://dual.example:8080/")
        assert target.addresses == (PUBLIC_IP, PUBLIC_V6) and target.port == 8080
        assert target.host_header == "dual.example:8080"
        assert target.curl_resolve_entry() == f"dual.example:8080:{PUBLIC_IP},{PUBLIC_V6}"

    @pytest.mark.parametrize("host", [
        "intranet.example", "metadata.example", "loop6.example", "mapped.example", "mixed.example",
        "cgnat.example", "nat64.example", "sixtofour.example",
    ])
    async def test_non_public_results_block(self, host: str) -> None:
        policy, _ = _policy()
        with pytest.raises(DestinationBlocked) as e:
            await policy.resolve(f"https://{host}/")
        assert e.value.url == f"https://{host}/" and "public" in e.value.reason
        assert await policy.is_blocked(f"https://{host}/") is True

    @pytest.mark.parametrize("url", [
        "http://10.0.0.1/", "http://169.254.169.254/latest/meta-data/", "http://[::1]:8080/", "http://[::ffff:a00:1]/",
        "http://127.0.0.1/", "http://0.0.0.0/",
    ])
    async def test_literal_private_ip_blocks_without_resolving(self, url: str) -> None:
        policy, resolver = _policy()
        with pytest.raises(DestinationBlocked):
            await policy.resolve(url)
        assert resolver.calls == []

    @pytest.mark.parametrize("url", ["http://localhost/", "http://LOCALHOST./", "http://svc.localhost/", "http://localhost:8080/x"])
    async def test_localhost_names_block(self, url: str) -> None:
        policy, resolver = _policy()
        with pytest.raises(DestinationBlocked, match="localhost"):
            await policy.resolve(url)
        assert resolver.calls == []

    @pytest.mark.parametrize("url", ["ftp://public.example/", "file:///etc/passwd", "gopher://public.example/", "javascript:alert(1)", "https:///nohost", "not a url"])
    async def test_non_http_or_hostless_urls_block(self, url: str) -> None:
        policy, _ = _policy()
        with pytest.raises(DestinationBlocked):
            await policy.resolve(url)

    async def test_dns_failure_fails_closed(self) -> None:
        policy, _ = _policy()
        with pytest.raises(DestinationBlocked, match="could not resolve"):
            await policy.resolve("https://nx.example/")
        assert await policy.is_blocked("https://nx.example/") is True

    async def test_empty_answer_fails_closed(self) -> None:
        policy, _ = _policy()
        with pytest.raises(DestinationBlocked, match="no addresses"):
            await policy.resolve("https://empty.example/")

    async def test_public_result_is_never_cached(self) -> None:
        policy, resolver = _policy()
        await policy.resolve("https://public.example/a")
        resolver.table["public.example"] = ["10.9.9.9"]  # DNS now points inside → must be re-checked
        with pytest.raises(DestinationBlocked):
            await policy.resolve("https://public.example/b")
        assert resolver.calls == ["public.example", "public.example"]

    async def test_blocked_result_cached_briefly_then_rechecked(self) -> None:
        clock = Clock()
        policy, resolver = _policy(negative_cache_ttl=60.0, clock=clock)
        for _ in range(3):
            with pytest.raises(DestinationBlocked):
                await policy.resolve("https://intranet.example/")
        assert resolver.calls == ["intranet.example"]  # served from the negative cache
        clock.now += 61
        resolver.table["intranet.example"] = [PUBLIC_IP]
        target = await policy.resolve("https://intranet.example/")
        assert target.addresses == (PUBLIC_IP,) and resolver.calls == ["intranet.example", "intranet.example"]

    async def test_opt_out_flag_allows_private(self, monkeypatch: pytest.MonkeyPatch) -> None:
        resolver = FakeResolver()
        policy = DestinationPolicy(resolver=resolver)  # env-driven
        monkeypatch.delenv("CGRAPH_WEB_ALLOW_PRIVATE_NETWORKS", raising=False)
        assert await policy.is_blocked("https://intranet.example/") is True
        monkeypatch.setenv("CGRAPH_WEB_ALLOW_PRIVATE_NETWORKS", "true")
        target = await policy.resolve("https://intranet.example/")
        assert target.addresses == ("10.0.0.1",)
        assert (await policy.resolve("http://127.0.0.1/")).addresses == ("127.0.0.1",)
        # an unresolvable host still fails (fail closed either way)
        with pytest.raises(DestinationBlocked):
            await policy.resolve("https://nx.example/")

    async def test_hostname_normalised(self) -> None:
        policy, resolver = _policy()
        target = await policy.resolve("HTTPS://Public.Example./x")
        assert target.host == "public.example" and resolver.calls == ["public.example"]


# ---------------------------------------------------------------------------
# aiohttp resolver binding
# ---------------------------------------------------------------------------


class TestPolicyResolver:
    async def test_returns_only_validated_addresses(self) -> None:
        policy, _ = _policy()
        results = await PolicyResolver(policy).resolve("dual.example", 443, socket.AF_UNSPEC)
        assert [(r["host"], r["family"], r["port"], r["hostname"]) for r in results] == [
            (PUBLIC_IP, socket.AF_INET, 443, "dual.example"), (PUBLIC_V6, socket.AF_INET6, 443, "dual.example"),
        ]
        v4_only = await PolicyResolver(policy).resolve("dual.example", 443, socket.AF_INET)
        assert [r["host"] for r in v4_only] == [PUBLIC_IP]

    @pytest.mark.parametrize("host", ["intranet.example", "mapped.example", "nx.example", "mixed.example"])
    async def test_blocked_hosts_raise_oserror(self, host: str) -> None:
        policy, _ = _policy()
        with pytest.raises(OSError, match="destination blocked"):
            await PolicyResolver(policy).resolve(host, 80)

    async def test_connector_uses_policy_resolver(self) -> None:
        policy, _ = _policy()
        connector = policy.aiohttp_connector()
        try:
            assert isinstance(connector._resolver, PolicyResolver)
        finally:
            await connector.close()


# ---------------------------------------------------------------------------
# requests / cloudscraper pinning
# ---------------------------------------------------------------------------


class TestRequestsPinning:
    def test_pool_built_on_validated_ip_with_sni_on_hostname(self) -> None:
        policy, _ = _policy()
        session = requests.Session()
        target = PinnedTarget(url="https://public.example/", scheme="https", host="public.example", port=443, addresses=(PUBLIC_IP,))
        policy.pin_requests_session(session, target)
        adapter = session.get_adapter("https://public.example/")
        prepared = requests.Request("GET", "https://public.example/x").prepare()
        host_params, pool_kwargs = adapter.build_connection_pool_key_attributes(prepared, True)
        assert host_params == {"scheme": "https", "host": PUBLIC_IP, "port": None}
        assert pool_kwargs["server_hostname"] == "public.example" and pool_kwargs["assert_hostname"] == "public.example"
        assert pool_kwargs["cert_reqs"] == "CERT_REQUIRED"

        # a host the policy did not validate is refused instead of resolved by requests
        other = requests.Request("GET", "https://intranet.example/").prepare()
        with pytest.raises(RuntimeError, match="not validated"):
            adapter.build_connection_pool_key_attributes(other, True)

        # re-pinning the same session for the next hop extends the mapping without re-wrapping
        hop = PinnedTarget(url="http://dual.example/", scheme="http", host="dual.example", port=80, addresses=(PUBLIC_IP, PUBLIC_V6))
        policy.pin_requests_session(session, hop)
        http_adapter = session.get_adapter("http://dual.example/")
        host_params, pool_kwargs = http_adapter.build_connection_pool_key_attributes(requests.Request("GET", "http://dual.example/").prepare(), True)
        assert host_params["host"] == PUBLIC_IP and "server_hostname" not in pool_kwargs
        assert adapter.build_connection_pool_key_attributes(prepared, True)[0]["host"] == PUBLIC_IP

    def test_sync_cloudscraper_fetch_pins_and_disables_redirects(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        target = PinnedTarget(url="https://public.example/", scheme="https", host="public.example", port=443, addresses=(PUBLIC_IP,))
        scraper = MagicMock()
        scraper.adapters = requests.Session().adapters  # real HTTPAdapters, pinned in place
        resp = MagicMock(status_code=200, content=b"ok", headers={"Content-Type": "text/html"}, url="https://public.example/")
        scraper.get.return_value = resp
        result = fs._sync_cloudscraper_fetch("https://public.example/", {"Accept": "*/*"}, 5, log, target, policy, scraper)
        assert result is not None and result.status_code == 200 and result.strategy == "cloudscraper"
        kwargs = scraper.get.call_args.kwargs
        assert kwargs["allow_redirects"] is False and kwargs["headers"]["Host"] == "public.example"
        for adapter in scraper.adapters.values():
            assert adapter._cgraph_pins == {"public.example": target}

    def test_sync_cloudscraper_fetch_refuses_unpinned_when_policy_given(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        scraper = MagicMock()
        assert fs._sync_cloudscraper_fetch("https://public.example/", {}, 5, log, None, policy, scraper) is None
        scraper.get.assert_not_called()


# ---------------------------------------------------------------------------
# curl_cffi pinning
# ---------------------------------------------------------------------------


class TestCurlPinning:
    def test_sync_curl_fetch_sets_resolve_and_disables_redirects(self, log: logging.Logger) -> None:
        sess = MagicMock()
        sess.__enter__ = MagicMock(return_value=sess)
        sess.__exit__ = MagicMock(return_value=False)
        sess.get.return_value = MagicMock(status_code=200, content=b"ok", headers={}, url="https://public.example/")
        fake_curl_cffi = MagicMock()
        fake_curl_cffi.requests.Session.return_value = sess
        with patch.dict("sys.modules", {"curl_cffi": fake_curl_cffi, "curl_cffi.requests": fake_curl_cffi.requests}), \
                patch.object(fs, "_CURL_PROFILES", ["chrome120"]):
            result = fs._sync_curl_cffi_fetch(
                "https://public.example/", {"Accept": "*/*"}, 5, True, None, log, f"public.example:443:{PUBLIC_IP}",
            )
        assert result is not None and result.status_code == 200
        sess.curl.setopt.assert_any_call(fake_curl_cffi.CurlOpt.RESOLVE, [f"public.example:443:{PUBLIC_IP}".encode()])
        assert sess.get.call_args.kwargs["allow_redirects"] is False

    async def test_try_curl_passes_pin_per_hop(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        calls: list[tuple] = []

        def fake_sync(url: str, headers: dict, timeout: int, use_http2: bool, profiles: object, logger: object, resolve_entry: str) -> fs.FetchResponse:
            calls.append((url, resolve_entry))
            if url == "https://public.example/":
                return fs.FetchResponse(302, b"", {"Location": "/moved"}, url, "curl_cffi(chrome120, h2=True)")
            return fs.FetchResponse(200, b"done", {}, url, "curl_cffi(chrome120, h2=True)")

        with patch.object(fs, "_sync_curl_cffi_fetch", side_effect=fake_sync):
            result = await fs._try_curl_cffi("https://public.example/", {}, 5, True, log, policy=policy)
        assert result is not None and result.status_code == 200 and result.final_url == "https://public.example/moved"
        assert calls == [
            ("https://public.example/", f"public.example:443:{PUBLIC_IP}"),
            ("https://public.example/moved", f"public.example:443:{PUBLIC_IP}"),
        ]


# ---------------------------------------------------------------------------
# Redirect hops (aiohttp strategy + orchestrator)
# ---------------------------------------------------------------------------


def _aiohttp_session(responses: dict[str, tuple[int, dict, bytes]]) -> MagicMock:
    """Fake ``aiohttp.ClientSession`` serving ``url -> (status, headers, body)``, no redirects."""
    session = MagicMock()
    session.requests: list[tuple[str, dict]] = []

    def get(url: str, **kwargs: object) -> MagicMock:
        session.requests.append((url, kwargs))
        status, headers, body = responses[url]
        resp = MagicMock(status=status, headers=headers, url=url)
        resp.read = AsyncMock(return_value=body)
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=resp)
        ctx.__aexit__ = AsyncMock(return_value=False)
        return ctx

    session.get = MagicMock(side_effect=get)
    session.head = MagicMock(side_effect=get)
    return session


class TestRedirectHops:
    async def test_public_to_public_redirect_followed_without_transport_redirects(self, log: logging.Logger) -> None:
        policy, resolver = _policy()
        session = _aiohttp_session({
            "https://public.example/": (301, {"Location": "https://dual.example/final"}, b""),
            "https://dual.example/final": (200, {"Content-Type": "text/html"}, b"<html>ok</html>"),
        })
        result = await fs._try_aiohttp(session, "https://public.example/", {}, 5, log, policy=policy)
        assert result is not None and result.status_code == 200 and result.content_bytes == b"<html>ok</html>"
        assert result.final_url == "https://dual.example/final"
        assert all(kw["allow_redirects"] is False for _, kw in session.requests)
        assert resolver.calls == ["public.example", "dual.example"]  # every hop validated

    async def test_redirect_from_public_to_private_blocked_at_the_hop(self, log: logging.Logger) -> None:
        policy, resolver = _policy()
        session = _aiohttp_session({
            "https://public.example/": (302, {"Location": "http://intranet.example/admin"}, b""),
            "http://intranet.example/admin": (200, {}, b"SECRET"),
        })
        with pytest.raises(DestinationBlocked) as e:
            await fs._try_aiohttp(session, "https://public.example/", {}, 5, log, policy=policy)
        assert e.value.url == "http://intranet.example/admin"
        assert [u for u, _ in session.requests] == ["https://public.example/"]  # second hop never fetched
        assert resolver.calls == ["public.example", "intranet.example"]

    @pytest.mark.parametrize("location", ["http://169.254.169.254/latest/meta-data/", "http://[::ffff:a00:1]/", "http://mapped.example/", "http://localhost:6379/"])
    async def test_redirect_targets_that_must_block(self, log: logging.Logger, location: str) -> None:
        policy, _ = _policy()
        session = _aiohttp_session({"https://public.example/": (307, {"location": location}, b"")})
        with pytest.raises(DestinationBlocked):
            await fs._try_aiohttp(session, "https://public.example/", {}, 5, log, policy=policy)
        assert len(session.requests) == 1

    async def test_relative_location_resolved_against_current_hop(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        session = _aiohttp_session({
            "https://public.example/a/b": (302, {"Location": "../c"}, b""),
            "https://public.example/c": (200, {}, b"c"),
        })
        result = await fs._try_aiohttp(session, "https://public.example/a/b", {}, 5, log, policy=policy)
        assert result is not None and result.final_url == "https://public.example/c"

    async def test_redirect_loop_capped(self, log: logging.Logger) -> None:
        policy, _ = _policy(max_redirects=3)
        session = _aiohttp_session({"https://public.example/": (302, {"Location": "https://public.example/"}, b"")})
        assert await fs._try_aiohttp(session, "https://public.example/", {}, 5, log, policy=policy) is None
        assert len(session.requests) == 4  # initial + 3 hops, then give up

    async def test_orchestrator_returns_451_for_blocked_hop_and_stops(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        session = _aiohttp_session({"https://public.example/": (302, {"Location": "http://intranet.example/"}, b"")})
        cloud = AsyncMock(return_value=None)
        with patch.object(fs, "_try_curl_cffi", AsyncMock(side_effect=DestinationBlocked("http://intranet.example/", "10.0.0.1 is not a public address"))), \
                patch.object(fs, "_try_cloudscraper", cloud):
            result = await fs.fetch_url_with_fallback("https://public.example/", session, log, policy=policy)
        assert result is not None
        assert result.status_code == fs.DESTINATION_BLOCKED_STATUS and result.strategy == "destination_policy"
        assert result.headers["X-Fetch-Skip-Reason"] == "destination_blocked" and result.success is False
        assert result.final_url == "http://intranet.example/"
        cloud.assert_not_called()  # no other strategy gets to try the blocked destination
        assert result.status_code not in fs._RATE_LIMIT_CODES | fs._BOT_DETECTION_CODES | fs._NON_RETRYABLE_CLIENT_ERRORS

    async def test_orchestrator_blocks_initial_private_url_before_any_transport(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        session = _aiohttp_session({})
        curl = AsyncMock(return_value=None)
        with patch.object(fs, "_try_curl_cffi", curl), patch.object(fs, "_try_cloudscraper", AsyncMock(return_value=None)):
            result = await fs.fetch_url_with_fallback("http://intranet.example/", session, log, policy=policy)
        assert result is not None and result.status_code == fs.DESTINATION_BLOCKED_STATUS
        curl.assert_not_called()
        assert session.requests == []

    async def test_size_guard_head_follows_hops_under_policy(self, log: logging.Logger) -> None:
        policy, resolver = _policy()
        session = _aiohttp_session({
            "https://public.example/big.pdf": (302, {"Location": "https://dual.example/big.pdf"}, b""),
            "https://dual.example/big.pdf": (200, {"Content-Length": str(50 * 1024 * 1024)}, b""),
        })
        result = await fs.fetch_url_with_fallback("https://public.example/big.pdf", session, log, max_size_mb=10, policy=policy)
        assert result is not None and result.status_code == 413 and result.strategy == "size_guard"
        assert all(kw["allow_redirects"] is False for _, kw in session.requests)
        # orchestrator pre-check, then the HEAD hop chain validates every hop again
        assert resolver.calls == ["public.example", "public.example", "dual.example"]

    async def test_size_guard_head_redirect_to_private_blocks(self, log: logging.Logger) -> None:
        policy, _ = _policy()
        session = _aiohttp_session({"https://public.example/big.pdf": (302, {"Location": "http://metadata.example/"}, b"")})
        with patch.object(fs, "_try_curl_cffi", AsyncMock(return_value=None)) as curl:
            result = await fs.fetch_url_with_fallback("https://public.example/big.pdf", session, log, max_size_mb=10, policy=policy)
        assert result is not None and result.status_code == fs.DESTINATION_BLOCKED_STATUS
        curl.assert_not_called()


# ---------------------------------------------------------------------------
# Image / subresource fetches go through the same transport policy
# ---------------------------------------------------------------------------


class TestImageFetch:
    @pytest.mark.parametrize("image_url", ["http://intranet.example/logo.png", "http://169.254.169.254/latest/meta-data/", "http://[::ffff:a00:1]/x.png", "http://nx.example/x.png"])
    async def test_image_url_to_private_or_unresolvable_host_blocked(self, log: logging.Logger, image_url: str) -> None:
        policy, _ = _policy()
        session = _aiohttp_session({})
        with patch.object(fs, "_try_curl_cffi", AsyncMock(return_value=None)) as curl, \
                patch.object(fs, "_try_cloudscraper", AsyncMock(return_value=None)) as cloud:
            result = await fs.fetch_url_with_fallback(
                image_url, session, log, referer="https://public.example/", extra_headers={"Accept": "image/png"},
                preferred_strategy="curl_cffi", policy=policy,
            )
        assert result is not None and result.status_code == fs.DESTINATION_BLOCKED_STATUS
        curl.assert_not_called()
        cloud.assert_not_called()
        assert session.requests == []

    async def test_connector_image_download_is_policed(self) -> None:
        """``_process_single_image`` reaches the transport with the connector's policy and a
        private image host is never contacted (no exception, image dropped)."""
        from bs4 import BeautifulSoup

        from app.connectors.sources.web.connector import WebConnector

        c = WebConnector.__new__(WebConnector)
        c.logger = MagicMock()
        c.session = _aiohttp_session({})
        policy, resolver = _policy()
        c._destination_policy = policy
        soup = BeautifulSoup('<html><img src="http://intranet.example/secret.png"></html>', "html.parser")
        img = soup.find("img")
        with patch.object(fs, "_try_curl_cffi", AsyncMock(return_value=None)) as curl, patch.object(fs, "_try_cloudscraper", AsyncMock(return_value=None)):
            await c._process_single_image(img, soup, "https://public.example/", {})
        curl.assert_not_called()
        assert c.session.requests == [] and resolver.calls == ["intranet.example"]
        assert img.get("src") == "http://intranet.example/secret.png"  # not inlined, nothing fetched

    async def test_connector_private_check_delegates_and_fails_closed(self) -> None:
        from app.connectors.sources.web.connector import WebConnector

        c = WebConnector.__new__(WebConnector)
        c.logger = MagicMock()
        policy, resolver = _policy()
        c._destination_policy = policy
        assert await c._is_private_network_target("https://public.example/") is False
        assert await c._is_private_network_target("https://intranet.example/") is True
        assert await c._is_private_network_target("https://nx.example/") is True  # DNS failure → blocked
        assert await c._is_private_network_target("https://public.example/") is False
        assert resolver.calls.count("public.example") == 2  # never cached as public


# ---------------------------------------------------------------------------
# Headless browser: request interception
# ---------------------------------------------------------------------------


def _route(url: str) -> MagicMock:
    route = MagicMock()
    route.request = MagicMock(url=url)
    route.abort = AsyncMock()
    route.continue_ = AsyncMock()
    return route


class TestBrowserGuard:
    async def test_public_request_continues(self) -> None:
        policy, _ = _policy()
        route = _route("https://public.example/app.js")
        assert await guard_browser_request(policy, route) is True
        route.continue_.assert_awaited_once()
        route.abort.assert_not_called()

    @pytest.mark.parametrize("url", [
        "http://intranet.example/", "http://169.254.169.254/computeMetadata/v1/", "http://[::ffff:a00:1]/",
        "http://mapped.example/img.png", "http://nx.example/", "http://localhost:9200/", "file:///etc/passwd",
    ])
    async def test_private_or_unresolvable_request_aborted(self, url: str) -> None:
        policy, _ = _policy()
        route = _route(url)
        assert await guard_browser_request(policy, route, route.request) is False
        route.abort.assert_awaited_once_with("blockedbyclient")
        route.continue_.assert_not_called()

    @pytest.mark.parametrize("url", ["data:text/plain,hi", "blob:https://public.example/uuid", "about:blank"])
    async def test_browser_local_schemes_pass(self, url: str) -> None:
        policy, resolver = _policy()
        route = _route(url)
        assert await guard_browser_request(policy, route) is True
        assert resolver.calls == []

    async def test_fetcher_installs_guard_on_context(self) -> None:
        from app.connectors.sources.web.crawl4ai_fetcher import Crawl4AIFetcher

        fetcher = Crawl4AIFetcher.__new__(Crawl4AIFetcher)
        fetcher._destination_policy, _ = _policy()
        page, context = MagicMock(), MagicMock()
        context.route = AsyncMock()
        await fetcher._install_request_guard(page, context=context, config=None)
        context.route.assert_awaited_once_with("**/*", fetcher._guard_route)
        route = _route("http://intranet.example/")
        await fetcher._guard_route(route)
        route.abort.assert_awaited_once()

    def test_crawler_start_registers_hook(self) -> None:
        from app.connectors.sources.web import crawl4ai_fetcher as mod

        fetcher = mod.Crawl4AIFetcher.__new__(mod.Crawl4AIFetcher)
        fetcher._browser_config = MagicMock()
        fetcher._destination_policy, _ = _policy()
        strategy = MagicMock()
        crawler = MagicMock()
        crawler.start = AsyncMock()
        with patch.object(mod, "AsyncPlaywrightCrawlerStrategy", return_value=strategy), \
                patch.object(mod, "UndetectedAdapter", return_value=MagicMock()), \
                patch.object(mod, "AsyncWebCrawler", return_value=crawler):
            import asyncio

            asyncio.run(fetcher._create_and_start_crawler())
        strategy.set_hook.assert_called_once_with("on_page_context_created", fetcher._install_request_guard)
