"""Persisted registry of one connector's change-notification registrations.

Rows live in the connector's records sync point under key ``webhooks`` so they
share the connector's lifecycle (deleted with it, encrypted like the rest). The
row shape is owned by each provider module (Microsoft Graph subscriptions, Google
Drive channels, Gmail watches); this class only stores and reloads dict rows.

The rows are stored as **one JSON string** (same convention as the Dynamics
``shareDigests`` field): Neo4j node properties can only hold primitives or
arrays of primitives, so a list of dicts is rejected with
``Neo.ClientError.Statement.TypeError ... Encountered: Map{...}`` and the whole
sync point write fails.  Loading still accepts the legacy list-of-dicts shape
written by earlier builds on Arango.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

WEBHOOKS_SYNC_POINT_KEY = "webhooks"

logger = logging.getLogger(__name__)


class SyncPointLike(Protocol):
    async def read_sync_point(self, sync_point_key: str) -> dict[str, Any]: ...

    async def update_sync_point(self, sync_point_key: str, sync_point_data: dict[str, Any]) -> object: ...


def encode_rows(rows: list[dict[str, Any]]) -> str:
    """Compact JSON for the ``webhooks`` property."""
    return json.dumps(list(rows), separators=(",", ":"), sort_keys=True)


def decode_rows(value: object) -> list[dict[str, Any]]:
    """Rows from the stored ``webhooks`` value: JSON string (current), list of dicts (legacy), else ``[]``."""
    if isinstance(value, str):
        if not value.strip():
            return []
        try:
            value = json.loads(value)
        except ValueError:
            logger.warning("Ignoring malformed webhook registry (not JSON): %.80r", value)
            return []
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


class SubscriptionStore:
    def __init__(self, sync_point: SyncPointLike, key: str = WEBHOOKS_SYNC_POINT_KEY) -> None:
        self._sync_point = sync_point
        self._key = key

    async def load_rows(self) -> list[dict[str, Any]]:
        point = await self._sync_point.read_sync_point(self._key)
        return decode_rows(point.get(WEBHOOKS_SYNC_POINT_KEY) if isinstance(point, dict) else None)

    async def save_rows(self, rows: list[dict[str, Any]]) -> None:
        await self._sync_point.update_sync_point(self._key, {WEBHOOKS_SYNC_POINT_KEY: encode_rows(rows)})
