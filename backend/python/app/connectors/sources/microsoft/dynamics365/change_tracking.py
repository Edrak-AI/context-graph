"""Delete detection for the Dynamics 365 connector (pure, standard-library only).

Dataverse ``modifiedon`` filters cannot see deleted rows, so the connector uses
`Dataverse change tracking`_: the first pull of an entity set is requested with
``Prefer: odata.track-changes`` and ends with an ``@odata.deltaLink``; later
syncs GET that link (same header) and receive only the rows changed since, plus
one entry per deleted row::

    {"@odata.context": ".../$metadata#accounts/$deletedEntity",
     "id": "<guid>", "reason": "deleted"}

Change tracking has to be switched on per table (``ChangeTrackingEnabled``).
When it is not, Dataverse rejects the tracked request with HTTP 400 and error
code ``0x80060888``; the connector then falls back to the historical
``modifiedon`` incremental sync and detects deletes with a periodic *full
reconcile*: pull every row, compare the ids seen with the records already in
the graph, delete the ones that are gone.  The reconcile interval is the
``reconcile_interval_hours`` sync filter (default 24h, ``0`` disables it).

Everything here is I/O free so the state machine can be unit-tested without
the connector's runtime dependencies; ``connector.py`` supplies HTTP and the
graph store.

.. _Dataverse change tracking:
   https://learn.microsoft.com/power-apps/developer/data-platform/webapi/use-change-tracking-synchronize-data-external-systems
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from app.connectors.sources.microsoft.dynamics365.mapping import (
    EntitySpec,
    attachment_external_id,
    parse_dataverse_timestamp,
    record_external_id,
)

TRACK_CHANGES_PREFERENCE = "odata.track-changes"
ODATA_NEXT_LINK = "@odata.nextLink"
ODATA_DELTA_LINK = "@odata.deltaLink"
ODATA_CONTEXT = "@odata.context"
ODATA_REMOVED = "@removed"
DELETED_ENTITY_CONTEXT_SUFFIX = "$deletedEntity"
DELETED_REASON = "deleted"

# Dataverse error code for "change tracking is not enabled on this table".
CHANGE_TRACKING_DISABLED_CODE = "0x80060888"
_CHANGE_TRACKING_DISABLED_PHRASES = ("change tracking", "track-changes", "track changes")

RECONCILE_INTERVAL_FILTER_KEY = "reconcile_interval_hours"
DEFAULT_RECONCILE_INTERVAL_HOURS = 24.0
MS_PER_HOUR = 3_600_000

# Sync-point document fields (camelCase like the other connectors' sync points).
FIELD_LAST_SYNC = "lastSyncTimestamp"
FIELD_DELTA_LINK = "deltaLink"
FIELD_CHANGE_TRACKING = "changeTracking"
FIELD_LAST_RECONCILE = "lastReconcileTimestamp"
# JSON text, not a nested map: Neo4j node properties must be primitives.
FIELD_SHARE_DIGESTS = "shareDigests"


class ChangeTrackingStatus(str, Enum):
    UNKNOWN = "unknown"    # never tried (fresh connector or pre-change-tracking sync point)
    ENABLED = "enabled"    # tracked pull succeeded; ``delta_link`` is authoritative
    DISABLED = "disabled"  # Dataverse rejected the tracked pull; modifiedon + reconcile


class SyncMode(str, Enum):
    DELTA = "delta"                    # GET stored deltaLink: upserts + deletes
    MODIFIED_ON = "modifiedon"         # ``modifiedon gt <last sync>`` (no deletes)
    FULL_RECONCILE = "full_reconcile"  # every row (tracked when possible) + prune missing


@dataclass
class EntitySyncState:
    """Per-entity-set sync state persisted under ``records/entity/<logical_name>``.

    ``last_sync_timestamp`` keeps the pre-existing ``lastSyncTimestamp`` field so
    sync points written before change tracking existed still load (they come
    back as ``UNKNOWN`` with no delta link, which re-baselines once).
    """

    last_sync_timestamp: int | None = None
    delta_link: str | None = None
    change_tracking: ChangeTrackingStatus = ChangeTrackingStatus.UNKNOWN
    last_reconcile_timestamp: int | None = None
    # record id -> share digest (``mapping.share_digests``) as last applied to the graph
    share_digests: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_sync_point(cls, data: Mapping[str, object] | None) -> EntitySyncState:
        data = data or {}
        try:
            status = ChangeTrackingStatus(str(data.get(FIELD_CHANGE_TRACKING) or ChangeTrackingStatus.UNKNOWN.value))
        except ValueError:
            status = ChangeTrackingStatus.UNKNOWN
        delta_link = data.get(FIELD_DELTA_LINK)
        return cls(
            last_sync_timestamp=_as_int(data.get(FIELD_LAST_SYNC)),
            delta_link=str(delta_link) if delta_link else None,
            change_tracking=status,
            last_reconcile_timestamp=_as_int(data.get(FIELD_LAST_RECONCILE)),
            share_digests=_parse_share_digests(data.get(FIELD_SHARE_DIGESTS)),
        )

    def to_sync_point(self) -> dict[str, object]:
        # ``deltaLink`` is always present (possibly None) so clearing it overwrites the stored value.
        return {
            FIELD_LAST_SYNC: self.last_sync_timestamp,
            FIELD_DELTA_LINK: self.delta_link,
            FIELD_CHANGE_TRACKING: self.change_tracking.value,
            FIELD_LAST_RECONCILE: self.last_reconcile_timestamp,
            FIELD_SHARE_DIGESTS: json.dumps(self.share_digests, separators=(",", ":"), sort_keys=True),
        }

    def mark_change_tracking_disabled(self) -> None:
        self.change_tracking = ChangeTrackingStatus.DISABLED
        self.delta_link = None

    def mark_change_tracking_enabled(self, delta_link: str | None) -> None:
        self.change_tracking = ChangeTrackingStatus.ENABLED
        self.delta_link = delta_link or None


def resolve_reconcile_interval_hours(value: object) -> float:
    """Parse the ``reconcile_interval_hours`` filter value; ``0`` disables the reconcile."""
    if value is None or value == "" or isinstance(value, bool):
        return DEFAULT_RECONCILE_INTERVAL_HOURS
    if isinstance(value, (int, float)):
        hours = float(value)
    elif isinstance(value, str):
        try:
            hours = float(value.strip())
        except ValueError:
            return DEFAULT_RECONCILE_INTERVAL_HOURS
    else:
        return DEFAULT_RECONCILE_INTERVAL_HOURS
    if math.isnan(hours) or math.isinf(hours):
        return DEFAULT_RECONCILE_INTERVAL_HOURS
    return max(hours, 0.0)


def reconcile_due(state: EntitySyncState, now_ms: int, interval_hours: float) -> bool:
    if interval_hours <= 0:
        return False
    if state.last_reconcile_timestamp is None:
        return True
    return now_ms - state.last_reconcile_timestamp >= int(interval_hours * MS_PER_HOUR)


def plan_sync(state: EntitySyncState, *, incremental: bool, now_ms: int, interval_hours: float) -> SyncMode:
    """Decide how one entity set is synced this run.

    * full sync (``run_sync``) → always a full pull + reconcile;
    * a stored delta link → delta;
    * change tracking known to be off → ``modifiedon``, or a full reconcile when due;
    * otherwise (first sync, or the delta link was reset) → tracked full pull + reconcile.
    """
    if not incremental:
        return SyncMode.FULL_RECONCILE
    if state.delta_link and state.change_tracking != ChangeTrackingStatus.DISABLED:
        return SyncMode.DELTA
    if state.change_tracking == ChangeTrackingStatus.DISABLED:
        return SyncMode.FULL_RECONCILE if reconcile_due(state, now_ms, interval_hours) else SyncMode.MODIFIED_ON
    return SyncMode.FULL_RECONCILE


@dataclass
class DeltaPage:
    """One page of a tracked / delta response, split into what the connector does with it."""

    upserts: list[dict[str, Any]] = field(default_factory=list)
    deleted_ids: list[str] = field(default_factory=list)
    next_link: str | None = None
    delta_link: str | None = None


def is_deleted_entry(entry: Mapping[str, object]) -> bool:
    """Dataverse marks deletes with ``reason: deleted`` and a ``$deletedEntity``
    context; Graph-style ``@removed`` is accepted too."""
    if str(entry.get("reason") or "").lower() == DELETED_REASON:
        return True
    if isinstance(entry.get(ODATA_REMOVED), Mapping):
        return True
    context = entry.get(ODATA_CONTEXT)
    return isinstance(context, str) and context.endswith(DELETED_ENTITY_CONTEXT_SUFFIX)


def deleted_entry_id(entry: Mapping[str, object], spec: EntitySpec) -> str | None:
    value = entry.get("id") or entry.get(spec.primary_id)
    return str(value) if value else None


def parse_delta_page(payload: Mapping[str, object], spec: EntitySpec) -> DeltaPage:
    page = DeltaPage()
    entries = payload.get("value")
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, Mapping):
            continue
        if is_deleted_entry(entry):
            deleted_id = deleted_entry_id(entry, spec)
            if deleted_id:
                page.deleted_ids.append(deleted_id)
            continue
        if entry.get(spec.primary_id):
            page.upserts.append(dict(entry))
    next_link = payload.get(ODATA_NEXT_LINK)
    delta_link = payload.get(ODATA_DELTA_LINK)
    page.next_link = str(next_link) if next_link else None
    page.delta_link = str(delta_link) if delta_link else None
    return page


def odata_error(body: object) -> tuple[str, str]:
    """``(code, message)`` from a Dataverse error body (dict, JSON text or plain text)."""
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8", errors="replace")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except ValueError:
            return "", body
    if isinstance(body, Mapping):
        error = body.get("error")
        if isinstance(error, Mapping):
            return str(error.get("code") or ""), str(error.get("message") or "")
        return str(body.get("code") or ""), str(body.get("message") or "")
    return "", str(body or "")


def is_change_tracking_disabled_error(status_code: int, body: object) -> bool:
    if status_code != 400:
        return False
    code, message = odata_error(body)
    if code.lower() == CHANGE_TRACKING_DISABLED_CODE:
        return True
    lowered = message.lower()
    return any(phrase in lowered for phrase in _CHANGE_TRACKING_DISABLED_PHRASES)


def row_in_modified_bounds(row: Mapping[str, object], start_ms: int | None, end_ms: int | None) -> bool:
    """Client-side twin of ``build_modified_filter`` for tracked pulls, where the
    change-tracking request cannot carry a ``$filter``.  Rows without a parsable
    ``modifiedon`` are kept rather than silently dropped."""
    if start_ms is None and end_ms is None:
        return True
    modified_ms = parse_dataverse_timestamp(row.get("modifiedon"))
    if modified_ms is None:
        return True
    if start_ms is not None and modified_ms < start_ms:
        return False
    return not (end_ms is not None and modified_ms > end_ms)


def seen_external_ids(spec: EntitySpec, rows: Iterable[Mapping[str, object]]) -> set[str]:
    """External ids a full pull proves still exist: every row, plus the attachment
    child of notes that still carry a document."""
    seen: set[str] = set()
    for row in rows:
        row_id = row.get(spec.primary_id)
        if not row_id:
            continue
        row_id = str(row_id)
        seen.add(record_external_id(spec, row_id))
        if spec.logical_name == "annotation" and row.get("isdocument") and row.get("filename"):
            seen.add(attachment_external_id(row_id))
    return seen


def missing_record_ids(known: Mapping[str, str], seen: Iterable[str]) -> list[str]:
    """Graph record ids whose external id was not seen in the full pull (stable order)."""
    seen_set = set(seen)
    return [record_id for external_id, record_id in known.items() if external_id not in seen_set]


def prune_is_safe(known_count: int, seen_count: int) -> bool:
    """Refuse to prune when a pull saw nothing while the graph knows records: an
    empty pull is far more likely a misconfiguration than a table wiped clean."""
    return not (known_count > 0 and seen_count == 0)


def _parse_share_digests(value: object) -> dict[str, str]:
    """Accept the stored JSON text (or an already-decoded map); anything else is an empty baseline."""
    if isinstance(value, (str, bytes, bytearray)):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    if not isinstance(value, Mapping):
        return {}
    return {str(k): str(v) for k, v in value.items() if isinstance(v, str) and v}


def _as_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return None
    return None
