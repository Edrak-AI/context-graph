"""Keep the connector unit tests off the network.

The web crawler's egress guard (``destination_policy.DestinationPolicy``) resolves every
destination and fails closed when DNS is unavailable.  Tests that exercise crawler code
paths with mocked transports must not depend on the sandbox's DNS, so the default policy
resolves every name to a public address here.  ``test_destination_policy.py`` builds its
own policies with explicit fake resolvers and is left alone.
"""
from __future__ import annotations

import pytest

from app.connectors.sources.web.destination_policy import DestinationPolicy

_PUBLIC_IP = "93.184.216.34"


async def _public_resolver(host: str, port: int) -> list[str]:
    return [_PUBLIC_IP]


@pytest.fixture(autouse=True)
def _offline_destination_policy(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.node.fspath.basename == "test_destination_policy.py":
        return
    monkeypatch.setattr(
        DestinationPolicy, "default",
        classmethod(lambda cls: cls(resolver=_public_resolver, allow_private=False)),
    )
