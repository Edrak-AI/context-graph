"""Egress destination policy for the web crawler (SSRF guard).

The crawler runs inside the cluster.  Without a guard, any URL that resolves to a
private, loopback, link-local or otherwise non-public address (intranet hosts,
Kubernetes services, the cloud metadata endpoint) would be fetched, indexed and —
for a team-scope connector — granted org-wide READ.

One ``DestinationPolicy`` is applied to **every** request the crawler makes: the
initial page, each redirect hop (transports never follow redirects on their own),
images and other subresources, and every request the headless browser issues
(request interception in ``crawl4ai_fetcher``).  For each request the host is
resolved once, *all* returned addresses are validated (fail closed on any
non-public address and on resolution errors) and the validated addresses are
pinned to the connection — aiohttp through a resolver that only ever returns
validated addresses, curl through ``CURLOPT_RESOLVE``, requests/cloudscraper by
building the connection pool on the pinned address with SNI/hostname checks on
the original host — so a second lookup by the transport cannot rebind the
connection to another address.

Only negative results are cached, briefly: "public" is never remembered, because a
name that pointed to a public address a minute ago may point at 10.x now.

``CGRAPH_WEB_ALLOW_PRIVATE_NETWORKS=true`` disables the check for deployments that
deliberately crawl internal sites (checked on every call, like the original guard).
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import aiohttp
from aiohttp.abc import AbstractResolver

__all__ = [
    "DestinationBlocked",
    "DestinationPolicy",
    "PinnedTarget",
    "PolicyResolver",
    "private_networks_allowed",
]

logger = logging.getLogger(__name__)

ALLOWED_SCHEMES = ("http", "https")
DEFAULT_PORTS = {"http": 80, "https": 443}
DEFAULT_MAX_REDIRECTS = 5
DEFAULT_NEGATIVE_CACHE_TTL = 60.0  # seconds; blocked hosts only

# NAT64 well-known prefix: the last 32 bits embed an IPv4 address.
_NAT64_NETWORK = ipaddress.ip_network("64:ff9b::/96")

Resolver = Callable[[str, int], Awaitable[list[str]]]


def private_networks_allowed() -> bool:
    """``CGRAPH_WEB_ALLOW_PRIVATE_NETWORKS`` opt-out, evaluated on every call."""
    return os.getenv("CGRAPH_WEB_ALLOW_PRIVATE_NETWORKS", "").strip().lower() in ("1", "true", "yes")


class DestinationBlocked(Exception):
    """The destination must not be contacted (non-public address, resolution failure, bad URL)."""

    def __init__(self, url: str, reason: str) -> None:
        super().__init__(f"{url}: {reason}")
        self.url = url
        self.reason = reason


@dataclass(frozen=True)
class PinnedTarget:
    """A URL whose host has been resolved and validated; ``addresses`` are all the
    (validated) addresses the transport may connect to."""

    url: str
    scheme: str
    host: str
    port: int
    addresses: tuple[str, ...]

    @property
    def primary(self) -> str:
        return self.addresses[0]

    @property
    def host_header(self) -> str:
        """``Host`` header value for transports that connect to the IP directly."""
        if self.port == DEFAULT_PORTS.get(self.scheme):
            return self.host
        return f"{self.host}:{self.port}"

    def curl_resolve_entry(self) -> str:
        """``CURLOPT_RESOLVE`` entry (``host:port:addr1,addr2``)."""
        return f"{self.host}:{self.port}:{','.join(self.addresses)}"


def _embedded_ipv4(ip: ipaddress.IPv6Address) -> Optional[ipaddress.IPv4Address]:
    """IPv4 address embedded in an IPv6 transition address, if any."""
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip.sixtofour is not None:
        return ip.sixtofour
    teredo = ip.teredo
    if teredo is not None:
        return teredo[1]  # the client address; the server is checked separately below
    if ip in _NAT64_NETWORK:
        return ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return None


def blocked_address_reason(address: str) -> Optional[str]:
    """Why ``address`` must not be contacted, or ``None`` when it is publicly routable.

    Covers RFC1918 / CGNAT / loopback / link-local (incl. 169.254.169.254) /
    multicast / unspecified / reserved for IPv4 and IPv6, and unwraps IPv4-mapped
    (``::ffff:a00:1`` as well as ``::ffff:10.0.0.1``), 6to4, Teredo and NAT64
    forms so the embedded IPv4 address is held to the same rules.
    """
    try:
        ip = ipaddress.ip_address(address.split("%", 1)[0].strip("[]"))
    except ValueError:
        return f"unparseable address {address!r}"
    if (
        not ip.is_global or ip.is_multicast or ip.is_reserved or ip.is_unspecified
        or ip.is_loopback or ip.is_link_local or ip.is_private
    ):
        return f"{ip} is not a public address"
    if isinstance(ip, ipaddress.IPv6Address):
        embedded = _embedded_ipv4(ip)
        if embedded is not None and not embedded.is_global:
            return f"{ip} embeds non-public IPv4 address {embedded}"
        if ip.teredo is not None and not ip.teredo[0].is_global:
            return f"{ip} embeds non-public Teredo server {ip.teredo[0]}"
    return None


async def _default_resolver(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses: list[str] = []
    for info in infos:
        sockaddr = info[4] if len(info) > 4 else None
        if not sockaddr:
            continue
        address = str(sockaddr[0]).split("%", 1)[0]
        if address not in addresses:
            addresses.append(address)
    return addresses


class DestinationPolicy:
    """Resolve-and-validate every crawler destination; see the module docstring.

    ``resolver(host, port) -> [addresses]`` is injectable for tests.  ``allow_private``
    pins the opt-out flag; ``None`` (default) reads the environment on every call.
    """

    def __init__(
        self,
        *,
        allow_private: Optional[bool] = None,
        resolver: Optional[Resolver] = None,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        negative_cache_ttl: float = DEFAULT_NEGATIVE_CACHE_TTL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._allow_private = allow_private
        self._resolver: Resolver = resolver or _default_resolver
        self.max_redirects = max_redirects
        self._negative_cache_ttl = negative_cache_ttl
        self._clock = clock
        self._blocked_hosts: dict[str, tuple[float, str]] = {}

    @property
    def allow_private(self) -> bool:
        return private_networks_allowed() if self._allow_private is None else self._allow_private

    @classmethod
    def default(cls) -> "DestinationPolicy":
        """Policy driven by the environment (what the connector and the shared browser use)."""
        return cls()

    # ------------------------------------------------------------------
    # URL parsing
    # ------------------------------------------------------------------

    @staticmethod
    def parse(url: str) -> tuple[str, str, int]:
        """``(scheme, host, port)`` of ``url``; raises ``DestinationBlocked`` for anything
        that is not a plain http(s) URL with a host."""
        try:
            parsed = urlparse(url)
            scheme = (parsed.scheme or "").lower()
            host = parsed.hostname
            port = parsed.port
        except ValueError as e:
            raise DestinationBlocked(url, f"unparseable URL ({e})") from None
        if scheme not in ALLOWED_SCHEMES:
            raise DestinationBlocked(url, f"scheme {scheme or '<none>'!r} is not allowed")
        if not host:
            raise DestinationBlocked(url, "URL has no host")
        host = host.lower().rstrip(".")
        if host == "localhost" or host.endswith(".localhost"):
            raise DestinationBlocked(url, "localhost is not a public host")
        return scheme, host, port or DEFAULT_PORTS[scheme]

    # ------------------------------------------------------------------
    # Resolution + validation
    # ------------------------------------------------------------------

    async def resolve_host(self, host: str, port: int) -> tuple[str, ...]:
        """All addresses of ``host``, every one validated; raises ``DestinationBlocked``
        (with ``url=host``) when any address is non-public, when resolution fails or
        returns nothing.  With the opt-out on, addresses are returned unvalidated."""
        key = host.lower().rstrip(".")
        try:
            literal = ipaddress.ip_address(key.strip("[]"))
        except ValueError:
            literal = None

        if not self.allow_private:
            cached = self._blocked_hosts.get(key)
            if cached is not None:
                if cached[0] > self._clock():
                    raise DestinationBlocked(host, cached[1])
                self._blocked_hosts.pop(key, None)

        if literal is not None:
            addresses: tuple[str, ...] = (str(literal),)
        else:
            try:
                addresses = tuple(await self._resolver(key, port))
            except DestinationBlocked:
                raise
            except Exception as e:  # fail closed: an unresolvable host is never contacted
                self._remember_blocked(key, f"could not resolve ({e})")
                raise DestinationBlocked(host, f"could not resolve ({e})") from e
            if not addresses:
                self._remember_blocked(key, "resolved to no addresses")
                raise DestinationBlocked(host, "resolved to no addresses")

        if self.allow_private:
            return addresses

        for address in addresses:
            reason = blocked_address_reason(address)
            if reason is not None:
                self._remember_blocked(key, reason)
                raise DestinationBlocked(host, reason)
        return addresses

    def _remember_blocked(self, host: str, reason: str) -> None:
        if self._negative_cache_ttl > 0:
            self._blocked_hosts[host] = (self._clock() + self._negative_cache_ttl, reason)

    async def resolve(self, url: str) -> PinnedTarget:
        """Validate ``url`` and pin its host to validated addresses; raises ``DestinationBlocked``."""
        scheme, host, port = self.parse(url)
        try:
            addresses = await self.resolve_host(host, port)
        except DestinationBlocked as e:
            raise DestinationBlocked(url, e.reason) from None
        return PinnedTarget(url=url, scheme=scheme, host=host, port=port, addresses=addresses)

    async def is_blocked(self, url: str) -> bool:
        """``True`` when ``url`` must not be fetched (never raises)."""
        try:
            await self.resolve(url)
        except DestinationBlocked as e:
            logger.warning(
                "Skipping %s: %s (CGRAPH_WEB_ALLOW_PRIVATE_NETWORKS is off)", url, e.reason
            )
            return True
        return False

    # ------------------------------------------------------------------
    # Transport bindings
    # ------------------------------------------------------------------

    def aiohttp_connector(self, **kwargs: object) -> aiohttp.TCPConnector:
        """``TCPConnector`` whose resolver only ever hands validated addresses to aiohttp."""
        return aiohttp.TCPConnector(resolver=PolicyResolver(self), **kwargs)  # type: ignore[arg-type]

    def pin_requests_session(self, session: object, target: PinnedTarget) -> None:
        """Make a ``requests.Session`` (cloudscraper included) connect to ``target``'s
        validated address for ``target.host`` while keeping TLS SNI + certificate
        hostname checks on the real host.  Raises ``RuntimeError`` when the installed
        ``requests`` cannot be pinned (callers must then skip the transport)."""
        adapters = getattr(session, "adapters", None)
        if not adapters:
            raise RuntimeError("requests session has no adapters to pin")
        for adapter in adapters.values():
            _pin_requests_adapter(adapter, target)


def _pin_requests_adapter(adapter: object, target: PinnedTarget) -> None:
    existing = getattr(adapter, "_cgraph_pins", None)
    if isinstance(existing, dict):  # already wrapped: extend the mapping for the next hop
        existing[target.host] = target
        return
    pins: dict[str, PinnedTarget] = {target.host: target}

    original = getattr(adapter, "build_connection_pool_key_attributes", None)
    if original is None:  # requests < 2.32.2
        raise RuntimeError("requests.adapters.HTTPAdapter.build_connection_pool_key_attributes is unavailable; cannot pin DNS")

    def pinned(request: object, verify: object, cert: object = None) -> tuple[dict, dict]:
        host_params, pool_kwargs = original(request, verify, cert)
        host = str(host_params.get("host") or "").lower().rstrip(".")
        pin = pins.get(host)
        if pin is None:
            # Never let requests resolve on its own: every host must have been pinned by the caller.
            raise RuntimeError(f"destination {host!r} was not validated by the destination policy")
        host_params["host"] = pin.primary
        if str(host_params.get("scheme") or "").lower() == "https":
            pool_kwargs["server_hostname"] = pin.host
            pool_kwargs["assert_hostname"] = pin.host
        return host_params, pool_kwargs

    adapter.build_connection_pool_key_attributes = pinned  # type: ignore[attr-defined]
    adapter._cgraph_pins = pins  # type: ignore[attr-defined]


class PolicyResolver(AbstractResolver):
    """aiohttp resolver that validates every address before the connector may use it.

    aiohttp connects to exactly the addresses returned here, so validation is bound
    to the connection.  (Literal-IP hosts bypass aiohttp resolvers entirely, which is
    why the fetch helpers also call ``DestinationPolicy.resolve`` before each hop.)
    """

    def __init__(self, policy: DestinationPolicy) -> None:
        self._policy = policy

    async def resolve(self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET) -> list:  # type: ignore[override]
        try:
            addresses = await self._policy.resolve_host(host, port)
        except DestinationBlocked as e:
            raise OSError(f"destination blocked: {e.reason}") from None
        results = []
        for address in addresses:
            ip = ipaddress.ip_address(address)
            ip_family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
            if family not in (socket.AF_UNSPEC, ip_family):
                continue
            results.append({
                "hostname": host,
                "host": address,
                "port": port,
                "family": ip_family,
                "proto": socket.IPPROTO_TCP,
                "flags": socket.AI_NUMERICHOST,
            })
        if not results:
            raise OSError(f"destination blocked: no address of {host} matches family {family}")
        return results

    async def close(self) -> None:
        return None
