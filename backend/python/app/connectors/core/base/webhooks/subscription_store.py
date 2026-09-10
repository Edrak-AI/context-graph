"""Persisted registry of one connector's change-notification registrations.

Rows live in the connector's records sync point under key ``webhooks`` so they
share the connector's lifecycle (deleted with it, encrypted like the rest). The
row shape is owned by each provider module (Microsoft Graph subscriptions, Google
Drive channels, Gmail watches); this class only stores and reloads dict rows.
"""

from __future__ import annotations

from typing import Any, Protocol

WEBHOOKS_SYNC_POINT_KEY = "webhooks"


class SyncPointLike(Protocol):
    async def read_sync_point(self, sync_point_key: str) -> dict[str, Any]: ...

    async def update_sync_point(self, sync_point_key: str, sync_point_data: dict[str, Any]) -> object: ...


class SubscriptionStore:
    def __init__(self, sync_point: SyncPointLike, key: str = WEBHOOKS_SYNC_POINT_KEY) -> None:
        self._sync_point = sync_point
        self._key = key

    async def load_rows(self) -> list[dict[str, Any]]:
        point = await self._sync_point.read_sync_point(self._key)
        rows = point.get(WEBHOOKS_SYNC_POINT_KEY) if isinstance(point, dict) else None
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    async def save_rows(self, rows: list[dict[str, Any]]) -> None:
        await self._sync_point.update_sync_point(self._key, {WEBHOOKS_SYNC_POINT_KEY: list(rows)})
