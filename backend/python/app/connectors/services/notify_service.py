"""Turns forwarded Microsoft change notifications into one near-real-time sync run.

edrak-ai posts to ``/api/v1/connectors/internal/{id}/notify``; this module
coalesces the bursts Microsoft sends (one notification per changed item) into a
single ``<connector>.resync`` sync-event, published on the same topic the Node
scheduler uses. The sync consumer then applies its usual serialisation
(``sync_task_manager.start_if_idle``), so a notification can never start a second
concurrent sync of the same connector.

Coalescing: the first notification for a connector claims Redis key
``cgraph:notify:{connector_id}`` (SET NX, ``NOTIFY_COALESCE_WINDOW_S``) and
schedules a run ``NOTIFY_DELAY_S`` later; later ones inside the window — or while a
run is still pending here — report ``scheduled=false``. If a sync is running when
the delay elapses, the run waits for it to finish before publishing, so changes that
arrived mid-sync are not lost.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from logging import Logger
from typing import Any, Protocol

from app.connectors.core.sync.task_manager import sync_task_manager
from app.services.messaging.config import Topic
from app.utils.time_conversion import get_epoch_timestamp_in_ms

NOTIFY_COALESCE_WINDOW_S = 25
NOTIFY_DELAY_S = 10.0
NOTIFY_RUNNING_POLL_S = 5.0
NOTIFY_MAX_WAIT_S = 30 * 60
NOTIFY_KEY_PREFIX = "cgraph:notify:"
NOTIFY_ORIGIN = "WEBHOOK"

Publisher = Callable[[str, dict[str, Any]], Awaitable[object]]


class KeyValueStoreLike(Protocol):
    async def create_key(self, key: str, value: object, overwrite: bool = True, ttl: int | None = None) -> bool: ...  # noqa: FBT001, FBT002


def normalize_connector_type(connector_type: str) -> str:
    return connector_type.replace(" ", "").lower()


def build_resync_event(*, org_id: str, connector_type: str, connector_id: str, source: str) -> dict[str, Any]:
    """Same envelope as the Node scheduler's ``constructSyncConnectorEvent`` plus ``incremental``."""
    now_ms = get_epoch_timestamp_in_ms()
    return {
        "eventType": f"{normalize_connector_type(connector_type)}.resync",
        "timestamp": now_ms,
        "payload": {
            "orgId": org_id,
            "origin": NOTIFY_ORIGIN,
            "connector": connector_type,
            "connectorId": connector_id,
            "incremental": True,
            "source": source,
            "createdAtTimestamp": str(now_ms),
            "updatedAtTimestamp": str(now_ms),
            "sourceCreatedAtTimestamp": str(now_ms),
        },
    }


class NotifyScheduler:
    """At most one pending notification-triggered run per connector, per process."""

    def __init__(
        self,
        *,
        delay_s: float = NOTIFY_DELAY_S,
        window_s: int = NOTIFY_COALESCE_WINDOW_S,
        poll_s: float = NOTIFY_RUNNING_POLL_S,
        max_wait_s: float = NOTIFY_MAX_WAIT_S,
    ) -> None:
        self._delay_s = delay_s
        self._window_s = window_s
        self._poll_s = poll_s
        self._max_wait_s = max_wait_s
        self._pending: dict[str, asyncio.Task] = {}
        self._local_claims: dict[str, float] = {}

    def is_pending(self, connector_id: str) -> bool:
        task = self._pending.get(connector_id)
        return task is not None and not task.done()

    async def request(
        self,
        *,
        connector_id: str,
        org_id: str,
        connector_type: str,
        source: str,
        kv_store: KeyValueStoreLike | None,
        publish: Publisher,
        logger: Logger,
    ) -> bool:
        """Schedule a run for the connector; False when it joins an already-pending one."""
        if self.is_pending(connector_id):
            return False
        if not await self._claim_window(connector_id, kv_store, logger):
            return False
        event = build_resync_event(org_id=org_id, connector_type=connector_type, connector_id=connector_id, source=source)
        task = asyncio.create_task(
            self._run(connector_id, event, publish, logger), name=f"notify_{connector_id}"
        )
        self._pending[connector_id] = task
        task.add_done_callback(lambda t, cid=connector_id: self._on_done(cid, t))
        return True

    async def _claim_window(self, connector_id: str, kv_store: KeyValueStoreLike | None, logger: Logger) -> bool:
        key = f"{NOTIFY_KEY_PREFIX}{connector_id}"
        if kv_store is not None:
            try:
                return bool(
                    await kv_store.create_key(
                        key, {"at": get_epoch_timestamp_in_ms()}, overwrite=False, ttl=self._window_s
                    )
                )
            except Exception as e:
                logger.warning("notify: KV claim for %s failed (%s); coalescing in-process only", connector_id, e)
        now = time.monotonic()
        claimed_at = self._local_claims.get(connector_id)
        if claimed_at is not None and now - claimed_at < self._window_s:
            return False
        self._local_claims[connector_id] = now
        return True

    async def _run(self, connector_id: str, event: dict[str, Any], publish: Publisher, logger: Logger) -> None:
        await asyncio.sleep(self._delay_s)
        waited = 0.0
        while sync_task_manager.is_running(connector_id) and waited < self._max_wait_s:
            await asyncio.sleep(self._poll_s)
            waited += self._poll_s
        if waited:
            logger.info("notify: waited %.0fs for the running sync of %s before scheduling the follow-up", waited, connector_id)
        try:
            await publish(Topic.SYNC_EVENTS.value, event)
            logger.info("notify: published %s for connector %s", event["eventType"], connector_id)
        except Exception as e:
            logger.error("notify: could not publish %s for connector %s: %s", event["eventType"], connector_id, e)

    def _on_done(self, connector_id: str, task: asyncio.Task) -> None:
        if self._pending.get(connector_id) is task:
            del self._pending[connector_id]
        if not task.cancelled() and task.exception() is not None:
            logging.getLogger(__name__).error(
                "notify task for %s failed", connector_id, exc_info=task.exception()
            )

    async def cancel_all(self) -> None:
        tasks = [t for t in self._pending.values() if not t.done()]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._pending.clear()


notify_scheduler = NotifyScheduler()


def get_live_connector(app_container: object, connector_id: str) -> object | None:
    """In-memory connector instance, looked up the way ``EventService._get_connector`` does."""
    connector_key = f"{connector_id}_connector"
    if hasattr(app_container, connector_key):
        return getattr(app_container, connector_key)()
    connectors_map = getattr(app_container, "connectors_map", None)
    if isinstance(connectors_map, dict):
        return connectors_map.get(connector_id)
    return None


async def remove_change_notifications_best_effort(app_container: object, connector_id: str, logger: Logger) -> None:
    """Ask a live connector to drop its Microsoft registrations (delete / disable); never raises."""
    connector = get_live_connector(app_container, connector_id)
    hook = getattr(connector, "remove_change_notifications", None) if connector is not None else None
    if hook is None:
        return
    try:
        await hook()
    except Exception as e:
        logger.warning("Could not remove change notifications for connector %s: %s", connector_id, e)
