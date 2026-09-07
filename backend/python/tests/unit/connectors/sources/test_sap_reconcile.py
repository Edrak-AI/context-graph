"""Connector-level tests for the SAP deletion reconcile and request plumbing.

Exercises ``SapConnector._reconcile_entity`` / ``_maybe_reconcile_entity`` /
``_sap_request_args`` with fakes for the entities processor, sync point and the
OData pager — no network.  Fixtures are shaped like SAP OData v2 JSON (``d.results``).

``connector.py`` imports the fork runtime (httpx, fastapi, pydantic models, config
service ...); when that chain is not importable in the current interpreter the
module is skipped instead of failing, so ``test_sap_mapping.py`` (stdlib only)
still runs everywhere.
"""

import asyncio
import logging
import os
import sys
import unittest
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

try:  # pragma: no cover - environment dependent
    from app.connectors.core.registry.filters import FilterCollection
    from app.connectors.sources.sap import connector as sap_connector
    from app.connectors.sources.sap.connector import (
        RECONCILE_TIMESTAMP_FIELD,
        SapConnector,
        _reconcile_sync_point_key,
    )
    IMPORT_ERROR: Optional[BaseException] = None
except BaseException as e:  # ImportError, pydantic/env errors from the runtime chain
    IMPORT_ERROR = e

from app.connectors.sources.sap.mapping import (
    ENTITY_SPECS,
    attachment_external_id,
    parse_page,
    record_external_id,
)

SO = ENTITY_SPECS["sales_order"]
INV = ENTITY_SPECS["supplier_invoice"]


class FakeRecord:
    def __init__(self, record_id: str, external_id: str) -> None:
        self.id = record_id
        self.external_record_id = external_id


class FakeProcessor:
    """Just the surface ``_reconcile_entity`` touches."""

    def __init__(self, records: List[FakeRecord], cascade_ok: bool = True) -> None:
        self.records = sorted(records, key=lambda r: r.id)
        self.cascade_ok = cascade_ok
        self.cascade_calls: List[List[str]] = []
        self.single_deletes: List[str] = []
        self.page_calls: List[Dict[str, Any]] = []
        self.org_id = "org-1"

    async def get_records_by_status(self, connector_id: str, status_filters: List[str], limit: Optional[int] = None,
                                    offset: int = 0, record_group_id: Optional[str] = None, is_placeholder: Optional[bool] = None,
                                    after_key: Optional[str] = None, exclude_statuses: Optional[List[str]] = None) -> List[FakeRecord]:
        self.page_calls.append({"after_key": after_key, "limit": limit, "status_filters": status_filters})
        rows = [r for r in self.records if after_key is None or r.id > after_key]
        return rows[:limit] if limit else rows

    async def on_records_deleted_cascade(self, record_ids: List[str], connector_id: str) -> Dict[str, Any]:
        if not self.cascade_ok:
            raise RuntimeError("cascade unavailable")
        self.cascade_calls.append(list(record_ids))
        self.records = [r for r in self.records if r.id not in set(record_ids)]
        return {"success": True, "successfully_deleted": len(record_ids)}

    async def on_record_deleted(self, record_id: str) -> None:
        self.single_deletes.append(record_id)
        self.records = [r for r in self.records if r.id != record_id]


class FakeSyncPoint:
    def __init__(self, points: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        self.points: Dict[str, Dict[str, Any]] = dict(points or {})

    async def read_sync_point(self, key: str) -> Dict[str, Any]:
        return dict(self.points.get(key, {}))

    async def update_sync_point(self, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        self.points[key] = dict(data)
        return data


def _v2_feed(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"d": {"results": rows, "__count": str(len(rows))}}


@unittest.skipIf(IMPORT_ERROR is not None, f"SAP connector runtime not importable here: {IMPORT_ERROR!r}")
class TestSapReconcile(unittest.TestCase):
    def _connector(self, processor: FakeProcessor, live_feed: Optional[Dict[str, Any]], sync_config: Optional[Dict[str, Any]] = None,
                   points: Optional[Dict[str, Dict[str, Any]]] = None) -> "SapConnector":
        c = SapConnector.__new__(SapConnector)
        c.logger = logging.getLogger("test-sap")
        c.connector_id = "conn-1"
        c.data_entities_processor = processor
        c.records_sync_point = FakeSyncPoint(points)
        c.base_url = "https://gw.corp:8000"
        c.sap_client = "100"
        c._sync_config = sync_config or {}
        c.sync_filters = FilterCollection()
        c._full_read_entities = set()
        c._http = object()
        c.pager_calls: List[Dict[str, Any]] = []

        async def fake_iter_pages(spec, odata_filter, params_builder=None, page_size=None):
            c.pager_calls.append({"spec": spec.name, "filter": odata_filter,
                                  "builder": getattr(params_builder, "__name__", None), "page_size": page_size})
            if live_feed is None:
                raise AssertionError("key pull was not expected")
            yield parse_page(live_feed, spec.odata_version).rows

        c._iter_pages = fake_iter_pages
        return c

    def test_deletes_missing_documents_via_cascade_and_stamps_sync_point(self):
        processor = FakeProcessor([
            FakeRecord("r1", "sales_order:1"),
            FakeRecord("r2", "sales_order:2"),
            FakeRecord("r3", "sales_order:3"),
            FakeRecord("r3a", attachment_external_id(SO, ["3"], "LD-3")),   # child of a deleted document
            FakeRecord("r9", "purchase_order:4500000001"),                  # other entity
        ])
        live = _v2_feed([{"SalesOrder": "1"}])
        c = self._connector(processor, live)
        deleted = asyncio.run(c._reconcile_entity(SO))
        self.assertEqual(deleted, 2)
        self.assertEqual(processor.cascade_calls, [["r2", "r3"]])
        self.assertEqual(processor.single_deletes, [])
        self.assertEqual({r.id for r in processor.records}, {"r1", "r3a", "r9"})  # cascade removes r3a in the real store
        self.assertEqual(c.pager_calls[0]["builder"], "build_key_page_params")
        self.assertEqual(c.pager_calls[0]["page_size"], sap_connector.RECONCILE_KEY_PAGE_SIZE)
        self.assertIsNone(c.pager_calls[0]["filter"])
        self.assertEqual(processor.page_calls[0]["status_filters"], [])
        stamp = c.records_sync_point.points[_reconcile_sync_point_key(SO)]
        self.assertIn(RECONCILE_TIMESTAMP_FIELD, stamp)

    def test_uses_seen_ids_from_a_full_read_without_key_pull(self):
        processor = FakeProcessor([FakeRecord("r1", "sales_order:1"), FakeRecord("r2", "sales_order:2")])
        c = self._connector(processor, live_feed=None)
        deleted = asyncio.run(c._reconcile_entity(SO, live_ids={record_external_id(SO, ("1",))}))
        self.assertEqual(deleted, 1)
        self.assertEqual(processor.cascade_calls, [["r2"]])
        self.assertEqual(c.pager_calls, [])

    def test_refuses_to_wipe_when_sap_returns_no_keys(self):
        processor = FakeProcessor([FakeRecord("r1", "sales_order:1")])
        c = self._connector(processor, _v2_feed([]))
        deleted = asyncio.run(c._reconcile_entity(SO))
        self.assertEqual(deleted, 0)
        self.assertEqual(processor.cascade_calls, [])
        self.assertNotIn(_reconcile_sync_point_key(SO), c.records_sync_point.points)  # retried next run

    def test_falls_back_to_single_deletes_when_cascade_fails(self):
        processor = FakeProcessor([FakeRecord("r1", "sales_order:1"), FakeRecord("r2", "sales_order:2")], cascade_ok=False)
        c = self._connector(processor, _v2_feed([{"SalesOrder": "2"}]))
        self.assertEqual(asyncio.run(c._reconcile_entity(SO)), 1)
        self.assertEqual(processor.single_deletes, ["r1"])

    def test_known_records_are_keyset_paged(self):
        records = [FakeRecord(f"r{i:04d}", f"supplier_invoice:{i}/2024") for i in range(2500)]
        processor = FakeProcessor(records)
        c = self._connector(processor, live_feed=None)
        known = asyncio.run(c._known_record_ids(INV))
        self.assertEqual(len(known), 2500)
        self.assertEqual(len(processor.page_calls), 3)
        self.assertEqual(processor.page_calls[1]["after_key"], "r0999")

    def test_interval_gating(self):
        hour = 3_600_000
        now = sap_connector.get_epoch_timestamp_in_ms()
        # not due: reconciled an hour ago with the 24 h default
        processor = FakeProcessor([FakeRecord("r1", "sales_order:1")])
        c = self._connector(processor, _v2_feed([]), points={_reconcile_sync_point_key(SO): {RECONCILE_TIMESTAMP_FIELD: now - hour}})
        asyncio.run(c._maybe_reconcile_entity(SO, None))
        self.assertEqual(c.pager_calls, [])
        # due: 25 h ago
        c = self._connector(processor, _v2_feed([{"SalesOrder": "1"}]), points={_reconcile_sync_point_key(SO): {RECONCILE_TIMESTAMP_FIELD: now - 25 * hour}})
        asyncio.run(c._maybe_reconcile_entity(SO, None))
        self.assertEqual(len(c.pager_calls), 1)
        # disabled by config
        c = self._connector(processor, _v2_feed([]), sync_config={"reconcileIntervalHours": "0"})
        asyncio.run(c._maybe_reconcile_entity(SO, {"sales_order:1"}))
        self.assertEqual(c.pager_calls, [])
        # first run ever -> due; failures inside the check never propagate
        c = self._connector(processor, _v2_feed([{"SalesOrder": "1"}]), sync_config={"reconcileIntervalHours": "6"})

        async def boom(spec, live_ids=None):
            raise RuntimeError("store down")

        c._reconcile_entity = boom
        asyncio.run(c._maybe_reconcile_entity(SO, None))  # no exception

    def test_sap_client_param_and_header_are_consistent(self):
        c = self._connector(FakeProcessor([]), live_feed=None)
        params, headers = c._sap_request_args("https://gw.corp:8000/sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder", {"$top": "5"})
        self.assertEqual(params, {"$top": "5", "sap-client": "100"})
        self.assertEqual(headers, {"sap-client": "100"})
        # server __next link already carries it -> header only, no duplicate param
        params, headers = c._sap_request_args("https://gw.corp:8000/sap/opu/odata/sap/X/A?sap-client=100&$skiptoken=5", None)
        self.assertIsNone(params)
        self.assertEqual(headers, {"sap-client": "100"})
        c.sap_client = None
        self.assertEqual(c._sap_request_args("https://gw.corp:8000/x", {"a": "1"}), ({"a": "1"}, {}))

    def test_config_schema_exposes_reconcile_interval(self):
        metadata = getattr(SapConnector, "_connector_metadata", {})
        fields = {f["name"]: f for f in metadata.get("config", {}).get("sync", {}).get("customFields", [])}
        self.assertIn("reconcileIntervalHours", fields)
        field = fields["reconcileIntervalHours"]
        self.assertEqual(field["fieldType"], "NUMBER")
        self.assertEqual(field["defaultValue"], "24")
        self.assertFalse(field.get("required"))
        self.assertIn("deletion", (field.get("description") or "").lower())
        # legacy behaviour untouched: the other sync fields are still there
        self.assertTrue({"authorization_mapping", "userEmailDomain", "entraTenantId"} <= set(fields))

if __name__ == "__main__":
    unittest.main(verbosity=1)
