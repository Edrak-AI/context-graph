"""Tests for app.connectors.sources.microsoft.dynamics365.change_tracking.

Pure state machine only: sync-state (delta link) persistence, Dataverse delta
payload parsing (upserts + ``reason: deleted`` entries), detection of the
"change tracking not enabled" error and the fallback plan, and the
full-reconcile diff.  Fixtures mirror real Dataverse Web API payloads; no
network, no pydantic / httpx / azure.
"""

import pytest

from app.connectors.sources.microsoft.dynamics365.change_tracking import (
    CHANGE_TRACKING_DISABLED_CODE,
    DEFAULT_RECONCILE_INTERVAL_HOURS,
    FIELD_CHANGE_TRACKING,
    FIELD_DELTA_LINK,
    FIELD_LAST_RECONCILE,
    FIELD_LAST_SYNC,
    MS_PER_HOUR,
    TRACK_CHANGES_PREFERENCE,
    ChangeTrackingStatus,
    EntitySyncState,
    SyncMode,
    is_change_tracking_disabled_error,
    is_deleted_entry,
    missing_record_ids,
    odata_error,
    parse_delta_page,
    plan_sync,
    prune_is_safe,
    reconcile_due,
    resolve_reconcile_interval_hours,
    row_in_modified_bounds,
    seen_external_ids,
)
from app.connectors.sources.microsoft.dynamics365.mapping import (
    ENTITY_SPECS,
    attachment_external_id,
    record_external_id,
)

API = "https://contoso.crm4.dynamics.com/api/data/v9.2"
ACCOUNT = ENTITY_SPECS["account"]
CONTACT = ENTITY_SPECS["contact"]
NOTE = ENTITY_SPECS["annotation"]

ACC_1 = "0a3f1e30-1111-4a2b-9c3d-000000000001"
ACC_2 = "0a3f1e30-2222-4a2b-9c3d-000000000002"
ACC_3 = "0a3f1e30-3333-4a2b-9c3d-000000000003"
NOTE_1 = "7b1c9d40-aaaa-4e5f-8a9b-000000000001"
NOTE_2 = "7b1c9d40-bbbb-4e5f-8a9b-000000000002"
OWNER = "11111111-1111-1111-1111-111111111111"

DELTA_LINK_1 = (
    f"{API}/accounts?$select=accountid,name,modifiedon"
    "&$deltatoken=919042%2108%2f22%2f2026%2008%3a10%3a44"
)
DELTA_LINK_2 = (
    f"{API}/accounts?$select=accountid,name,modifiedon"
    "&$deltatoken=925100%2109%2f06%2f2026%2010%3a00%3a12"
)
NEXT_LINK = (
    f"{API}/accounts?$select=accountid,name,modifiedon"
    "&$skiptoken=%3Ccookie%20pagenumber%3D%222%22%20pagingcookie%3D%22%253ccookie%2520page%253d%25221%2522%253e"
    "%253caccountid%2520last%253d%2522%257b0A3F1E30-2222-4A2B-9C3D-000000000002%257d%2522%2520%252f%253e"
    "%253c%252fcookie%253e%22%20istracking%3D%22True%22%20%2F%3E"
)


def _account(guid: str, name: str, modified: str, etag: str = 'W/"1234567"') -> dict:
    return {
        "@odata.etag": etag,
        "accountid": guid,
        "name": name,
        "modifiedon": modified,
        "createdon": "2026-01-10T08:00:00Z",
        "statecode": 0,
        "statuscode": 1,
        "statuscode@OData.Community.Display.V1.FormattedValue": "Active",
        "_ownerid_value": OWNER,
        "_ownerid_value@Microsoft.Dynamics.CRM.lookuplogicalname": "systemuser",
        "_owninguser_value": OWNER,
        "_owningteam_value": None,
        "_owningbusinessunit_value": "44444444-4444-4444-4444-444444444444",
    }


def _deleted(guid: str, entity_set: str = "accounts") -> dict:
    return {
        "@odata.context": f"{API}/$metadata#{entity_set}/$deletedEntity",
        "id": guid,
        "reason": "deleted",
    }


# Initial tracked pull: two pages, the last one carrying the delta link.
INITIAL_PAGE_1 = {
    "@odata.context": f"{API}/$metadata#accounts(accountid,name,modifiedon)",
    "value": [
        _account(ACC_1, "Contoso Ltd", "2026-02-01T09:00:00Z"),
        _account(ACC_2, "Fabrikam Inc", "2026-02-02T09:00:00Z"),
    ],
    "@odata.nextLink": NEXT_LINK,
}
INITIAL_PAGE_2 = {
    "@odata.context": f"{API}/$metadata#accounts(accountid,name,modifiedon)",
    "value": [_account(ACC_3, "Northwind Traders", "2026-02-03T09:00:00Z")],
    "@odata.deltaLink": DELTA_LINK_1,
}
# Follow-up GET on DELTA_LINK_1: one renamed account, one deleted.
DELTA_RESPONSE = {
    "@odata.context": f"{API}/$metadata#accounts(accountid,name,modifiedon)",
    "value": [
        _account(ACC_1, "Contoso Ltd (renamed)", "2026-03-01T12:30:00Z", etag='W/"1234999"'),
        _deleted(ACC_2),
    ],
    "@odata.deltaLink": DELTA_LINK_2,
}
CHANGE_TRACKING_DISABLED_BODY = {
    "error": {
        "code": CHANGE_TRACKING_DISABLED_CODE,
        "message": (
            "Change tracking is not enabled for the entity 'annotation'. "
            "Enable change tracking for the entity to use the odata.track-changes preference."
        ),
    },
}
OTHER_400_BODY = {
    "error": {
        "code": "0x80060888".replace("888", "000"),
        "message": "Could not find a property named 'bogus' on type 'Microsoft.Dynamics.CRM.account'.",
    },
}


class TestSyncStatePersistence:
    def test_legacy_sync_point_loads_as_untracked(self):
        state = EntitySyncState.from_sync_point({"lastSyncTimestamp": 1_700_000_000_000})
        assert state.last_sync_timestamp == 1_700_000_000_000
        assert state.delta_link is None
        assert state.change_tracking is ChangeTrackingStatus.UNKNOWN
        assert state.last_reconcile_timestamp is None

    def test_empty_or_missing_sync_point(self):
        assert EntitySyncState.from_sync_point(None) == EntitySyncState()
        assert EntitySyncState.from_sync_point({}) == EntitySyncState()

    def test_delta_link_round_trips_through_sync_point_document(self):
        state = EntitySyncState(
            last_sync_timestamp=1_700_000_000_000,
            delta_link=DELTA_LINK_1,
            change_tracking=ChangeTrackingStatus.ENABLED,
            last_reconcile_timestamp=1_699_000_000_000,
        )
        doc = state.to_sync_point()
        assert doc == {
            FIELD_LAST_SYNC: 1_700_000_000_000,
            FIELD_DELTA_LINK: DELTA_LINK_1,
            FIELD_CHANGE_TRACKING: "enabled",
            FIELD_LAST_RECONCILE: 1_699_000_000_000,
        }
        assert EntitySyncState.from_sync_point(doc) == state

    def test_cleared_delta_link_is_written_explicitly(self):
        # SyncPoint.update_sync_point replaces the document; an absent key would not
        # matter, but an explicit None also survives a merge-style store.
        state = EntitySyncState(delta_link=DELTA_LINK_1, change_tracking=ChangeTrackingStatus.ENABLED)
        state.mark_change_tracking_disabled()
        doc = state.to_sync_point()
        assert FIELD_DELTA_LINK in doc and doc[FIELD_DELTA_LINK] is None
        assert doc[FIELD_CHANGE_TRACKING] == "disabled"

    def test_encrypted_field_name_matches_document_key(self):
        # connector.py passes encrypt_fields=[FIELD_DELTA_LINK]; SyncPoint encrypts by key name.
        assert FIELD_DELTA_LINK == "deltaLink"
        assert FIELD_DELTA_LINK in EntitySyncState(delta_link="x").to_sync_point()

    def test_tolerates_garbage_values(self):
        state = EntitySyncState.from_sync_point({
            FIELD_LAST_SYNC: "not-a-number",
            FIELD_DELTA_LINK: "",
            FIELD_CHANGE_TRACKING: "sometimes",
            FIELD_LAST_RECONCILE: "1700000000000",
        })
        assert state.last_sync_timestamp is None
        assert state.delta_link is None
        assert state.change_tracking is ChangeTrackingStatus.UNKNOWN
        assert state.last_reconcile_timestamp == 1_700_000_000_000

    def test_mark_enabled_normalises_empty_link(self):
        state = EntitySyncState()
        state.mark_change_tracking_enabled("")
        assert state.change_tracking is ChangeTrackingStatus.ENABLED
        assert state.delta_link is None
        state.mark_change_tracking_enabled(DELTA_LINK_2)
        assert state.delta_link == DELTA_LINK_2


class TestDeltaPayloadParsing:
    def test_initial_tracked_pull_pages(self):
        page1 = parse_delta_page(INITIAL_PAGE_1, ACCOUNT)
        assert [r["accountid"] for r in page1.upserts] == [ACC_1, ACC_2]
        assert page1.deleted_ids == []
        assert page1.next_link == NEXT_LINK
        assert page1.delta_link is None

        page2 = parse_delta_page(INITIAL_PAGE_2, ACCOUNT)
        assert [r["accountid"] for r in page2.upserts] == [ACC_3]
        assert page2.next_link is None
        assert page2.delta_link == DELTA_LINK_1

    def test_delta_response_splits_upserts_and_deletes(self):
        page = parse_delta_page(DELTA_RESPONSE, ACCOUNT)
        assert len(page.upserts) == 1
        assert page.upserts[0]["name"] == "Contoso Ltd (renamed)"
        assert page.deleted_ids == [ACC_2]
        assert page.delta_link == DELTA_LINK_2
        # deleted entries never leak into the upsert list (they have no primary id)
        assert all("accountid" in r for r in page.upserts)

    def test_deleted_entry_variants(self):
        assert is_deleted_entry(_deleted(ACC_1)) is True
        # context only (no reason) — still a delete
        assert is_deleted_entry({"@odata.context": f"{API}/$metadata#accounts/$deletedEntity", "id": ACC_1}) is True
        # Graph-style @removed
        assert is_deleted_entry({"id": ACC_1, "@removed": {"reason": "deleted"}}) is True
        assert is_deleted_entry({"id": ACC_1, "@removed": {"reason": "changed"}}) is True
        # ordinary rows are not deletes even when they carry an id-like field
        assert is_deleted_entry(_account(ACC_1, "x", "2026-01-01T00:00:00Z")) is False
        assert is_deleted_entry({"reason": "updated", "id": ACC_1}) is False

    def test_deleted_entry_falls_back_to_primary_id_and_skips_blank(self):
        payload = {
            "value": [
                {"reason": "deleted", "accountid": ACC_3},
                {"reason": "deleted"},
                "not-a-dict",
                {"name": "no primary id -> ignored"},
            ],
        }
        page = parse_delta_page(payload, ACCOUNT)
        assert page.deleted_ids == [ACC_3]
        assert page.upserts == []

    def test_payload_without_value_list(self):
        page = parse_delta_page({"@odata.context": "x"}, ACCOUNT)
        assert page.upserts == [] and page.deleted_ids == []
        assert page.next_link is None and page.delta_link is None
        assert parse_delta_page({"value": "oops"}, ACCOUNT).upserts == []

    def test_prefer_header_value(self):
        assert TRACK_CHANGES_PREFERENCE == "odata.track-changes"


class TestChangeTrackingDisabledDetection:
    def test_dataverse_error_code(self):
        assert is_change_tracking_disabled_error(400, CHANGE_TRACKING_DISABLED_BODY) is True

    def test_code_is_case_insensitive_and_message_alone_suffices(self):
        assert is_change_tracking_disabled_error(400, {"error": {"code": "0X80060888", "message": ""}}) is True
        assert is_change_tracking_disabled_error(
            400, {"error": {"code": "0x0", "message": "Entity 'annotation' does not have change tracking enabled"}}
        ) is True

    def test_body_may_be_json_text_or_bytes_or_plain_text(self):
        import json

        text = json.dumps(CHANGE_TRACKING_DISABLED_BODY)
        assert is_change_tracking_disabled_error(400, text) is True
        assert is_change_tracking_disabled_error(400, text.encode("utf-8")) is True
        assert is_change_tracking_disabled_error(400, "change tracking is not enabled") is True
        assert is_change_tracking_disabled_error(400, "") is False
        assert is_change_tracking_disabled_error(400, None) is False

    def test_other_400s_and_other_statuses_are_not_mistaken(self):
        assert is_change_tracking_disabled_error(400, OTHER_400_BODY) is False
        assert is_change_tracking_disabled_error(403, CHANGE_TRACKING_DISABLED_BODY) is False
        assert is_change_tracking_disabled_error(500, CHANGE_TRACKING_DISABLED_BODY) is False

    def test_odata_error_shapes(self):
        assert odata_error(CHANGE_TRACKING_DISABLED_BODY) == (
            CHANGE_TRACKING_DISABLED_CODE, CHANGE_TRACKING_DISABLED_BODY["error"]["message"],
        )
        assert odata_error({"code": "c", "message": "m"}) == ("c", "m")
        assert odata_error("plain text") == ("", "plain text")
        assert odata_error(None) == ("", "")


class TestPlanning:
    NOW = 1_800_000_000_000

    def _plan(self, state, incremental=True, interval=DEFAULT_RECONCILE_INTERVAL_HOURS, now=NOW):
        return plan_sync(state, incremental=incremental, now_ms=now, interval_hours=interval)

    def test_full_sync_always_reconciles(self):
        tracked = EntitySyncState(delta_link=DELTA_LINK_1, change_tracking=ChangeTrackingStatus.ENABLED)
        assert self._plan(tracked, incremental=False) is SyncMode.FULL_RECONCILE
        disabled = EntitySyncState(change_tracking=ChangeTrackingStatus.DISABLED, last_reconcile_timestamp=self.NOW)
        assert self._plan(disabled, incremental=False) is SyncMode.FULL_RECONCILE

    def test_first_incremental_sync_baselines_with_tracked_full_pull(self):
        assert self._plan(EntitySyncState()) is SyncMode.FULL_RECONCILE
        # pre-change-tracking sync point: same one-time re-baseline
        legacy = EntitySyncState.from_sync_point({"lastSyncTimestamp": 1})
        assert self._plan(legacy) is SyncMode.FULL_RECONCILE

    def test_stored_delta_link_drives_delta_sync(self):
        state = EntitySyncState(delta_link=DELTA_LINK_1, change_tracking=ChangeTrackingStatus.ENABLED)
        assert self._plan(state) is SyncMode.DELTA
        # a link is trusted even if the status field was never written
        assert self._plan(EntitySyncState(delta_link=DELTA_LINK_1)) is SyncMode.DELTA

    def test_lost_delta_link_re_baselines(self):
        state = EntitySyncState(change_tracking=ChangeTrackingStatus.ENABLED, delta_link=None)
        assert self._plan(state) is SyncMode.FULL_RECONCILE

    def test_disabled_uses_modifiedon_until_reconcile_is_due(self):
        one_hour_ago = self.NOW - MS_PER_HOUR
        state = EntitySyncState(change_tracking=ChangeTrackingStatus.DISABLED, last_reconcile_timestamp=one_hour_ago)
        assert self._plan(state) is SyncMode.MODIFIED_ON
        state.last_reconcile_timestamp = self.NOW - 25 * MS_PER_HOUR
        assert self._plan(state) is SyncMode.FULL_RECONCILE
        # never reconciled -> due now
        assert self._plan(EntitySyncState(change_tracking=ChangeTrackingStatus.DISABLED)) is SyncMode.FULL_RECONCILE
        # stale link on a disabled table is ignored (never GET it)
        stale = EntitySyncState(change_tracking=ChangeTrackingStatus.DISABLED, delta_link=DELTA_LINK_1, last_reconcile_timestamp=one_hour_ago)
        assert self._plan(stale) is SyncMode.MODIFIED_ON

    def test_interval_zero_disables_reconcile(self):
        state = EntitySyncState(change_tracking=ChangeTrackingStatus.DISABLED)
        assert self._plan(state, interval=0) is SyncMode.MODIFIED_ON
        assert reconcile_due(state, self.NOW, 0) is False
        assert reconcile_due(state, self.NOW, -1) is False

    def test_reconcile_due_boundary_is_inclusive(self):
        state = EntitySyncState(last_reconcile_timestamp=self.NOW - 6 * MS_PER_HOUR)
        assert reconcile_due(state, self.NOW, 6) is True
        assert reconcile_due(state, self.NOW - 1, 6) is False
        assert reconcile_due(state, self.NOW, 6.5) is False

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, DEFAULT_RECONCILE_INTERVAL_HOURS),
            ("", DEFAULT_RECONCILE_INTERVAL_HOURS),
            (24, 24.0),
            (6.5, 6.5),
            ("12", 12.0),
            (" 48 ", 48.0),
            (0, 0.0),
            (-3, 0.0),
            ("abc", DEFAULT_RECONCILE_INTERVAL_HOURS),
            (True, DEFAULT_RECONCILE_INTERVAL_HOURS),
            (float("nan"), DEFAULT_RECONCILE_INTERVAL_HOURS),
            (float("inf"), DEFAULT_RECONCILE_INTERVAL_HOURS),
            ([24], DEFAULT_RECONCILE_INTERVAL_HOURS),
        ],
    )
    def test_resolve_reconcile_interval_hours(self, value, expected):
        assert resolve_reconcile_interval_hours(value) == expected

    def test_default_is_24_hours(self):
        assert DEFAULT_RECONCILE_INTERVAL_HOURS == 24


class TestFullReconcile:
    def test_seen_ids_cover_rows_and_note_attachments(self):
        rows = [
            _account(ACC_1, "a", "2026-01-01T00:00:00Z"),
            {"accountid": None, "name": "no id -> skipped"},
        ]
        assert seen_external_ids(ACCOUNT, rows) == {record_external_id(ACCOUNT, ACC_1)}

        notes = [
            {"annotationid": NOTE_1, "subject": "with file", "isdocument": True, "filename": "quote.pdf"},
            {"annotationid": NOTE_2, "subject": "file removed", "isdocument": False, "filename": None},
        ]
        assert seen_external_ids(NOTE, notes) == {
            record_external_id(NOTE, NOTE_1),
            attachment_external_id(NOTE_1),
            record_external_id(NOTE, NOTE_2),
        }

    def test_missing_ids_are_marked_deleted_in_known_order(self):
        known = {
            record_external_id(ACCOUNT, ACC_1): "rec-1",
            record_external_id(ACCOUNT, ACC_2): "rec-2",
            record_external_id(ACCOUNT, ACC_3): "rec-3",
        }
        seen = seen_external_ids(ACCOUNT, [_account(ACC_2, "kept", "2026-01-01T00:00:00Z")])
        assert missing_record_ids(known, seen) == ["rec-1", "rec-3"]
        assert missing_record_ids(known, set(known)) == []
        assert missing_record_ids({}, seen) == []

    def test_detached_attachment_is_pruned_while_note_survives(self):
        known = {
            record_external_id(NOTE, NOTE_1): "note-1",
            attachment_external_id(NOTE_1): "file-1",
        }
        seen = seen_external_ids(NOTE, [{"annotationid": NOTE_1, "isdocument": False}])
        assert missing_record_ids(known, seen) == ["file-1"]

    def test_prune_guard(self):
        assert prune_is_safe(known_count=10, seen_count=0) is False
        assert prune_is_safe(known_count=10, seen_count=1) is True
        assert prune_is_safe(known_count=0, seen_count=0) is True

    def test_row_in_modified_bounds(self):
        row = _account(ACC_1, "a", "2026-02-01T09:00:00Z")
        ts = 1_769_936_400_000  # 2026-02-01T09:00:00Z
        assert row_in_modified_bounds(row, None, None) is True
        assert row_in_modified_bounds(row, ts, ts) is True  # inclusive both ends
        assert row_in_modified_bounds(row, ts + 1, None) is False
        assert row_in_modified_bounds(row, None, ts - 1) is False
        assert row_in_modified_bounds(row, ts - 1, ts + 1) is True
        # unparsable / missing modifiedon is kept, never silently dropped
        assert row_in_modified_bounds({"accountid": ACC_1}, ts, None) is True
        assert row_in_modified_bounds({"modifiedon": "garbage"}, None, ts) is True


class TestFallbackScenario:
    """Walk the state machine the way connector._sync_entity drives it, with the
    HTTP layer replaced by the fixture payloads."""

    NOW = 1_800_000_000_000

    def test_change_tracking_enabled_end_to_end(self):
        state = EntitySyncState.from_sync_point({"lastSyncTimestamp": 1})  # legacy point
        assert plan_sync(state, incremental=True, now_ms=self.NOW, interval_hours=24) is SyncMode.FULL_RECONCILE

        # tracked full pull: two pages, collect seen ids, keep the last delta link
        seen: set[str] = set()
        delta_link = None
        for payload in (INITIAL_PAGE_1, INITIAL_PAGE_2):
            page = parse_delta_page(payload, ACCOUNT)
            seen |= seen_external_ids(ACCOUNT, page.upserts)
            delta_link = page.delta_link or delta_link
        state.mark_change_tracking_enabled(delta_link)
        state.last_reconcile_timestamp = state.last_sync_timestamp = self.NOW
        known = {record_external_id(ACCOUNT, g): f"rec-{g[-1]}" for g in (ACC_1, ACC_2, ACC_3)} | {
            record_external_id(ACCOUNT, "gone-guid"): "rec-gone",
        }
        assert missing_record_ids(known, seen) == ["rec-gone"]

        # persisted, reloaded, next run is a delta
        reloaded = EntitySyncState.from_sync_point(state.to_sync_point())
        assert reloaded.delta_link == DELTA_LINK_1
        assert plan_sync(reloaded, incremental=True, now_ms=self.NOW + MS_PER_HOUR, interval_hours=24) is SyncMode.DELTA

        page = parse_delta_page(DELTA_RESPONSE, ACCOUNT)
        assert page.deleted_ids == [ACC_2]
        assert record_external_id(ACCOUNT, page.deleted_ids[0]) == f"account:{ACC_2}"
        reloaded.mark_change_tracking_enabled(page.delta_link)
        assert reloaded.to_sync_point()[FIELD_DELTA_LINK] == DELTA_LINK_2

    def test_change_tracking_disabled_falls_back_to_modifiedon_and_reconcile(self):
        state = EntitySyncState()
        assert plan_sync(state, incremental=True, now_ms=self.NOW, interval_hours=24) is SyncMode.FULL_RECONCILE

        # Dataverse rejects the tracked request -> fallback
        assert is_change_tracking_disabled_error(400, CHANGE_TRACKING_DISABLED_BODY)
        state.mark_change_tracking_disabled()
        state.last_sync_timestamp = state.last_reconcile_timestamp = self.NOW
        doc = state.to_sync_point()
        assert doc[FIELD_CHANGE_TRACKING] == "disabled" and doc[FIELD_DELTA_LINK] is None

        # hourly runs inside the window: modifiedon only, no tracked request
        for hours in (1, 12, 23):
            assert plan_sync(state, incremental=True, now_ms=self.NOW + hours * MS_PER_HOUR, interval_hours=24) is SyncMode.MODIFIED_ON
        # 24h later: full reconcile prunes what the untracked pull no longer returns
        assert plan_sync(state, incremental=True, now_ms=self.NOW + 24 * MS_PER_HOUR, interval_hours=24) is SyncMode.FULL_RECONCILE
        known = {record_external_id(CONTACT, "c1"): "rec-c1", record_external_id(CONTACT, "c2"): "rec-c2"}
        seen = seen_external_ids(CONTACT, [{"contactid": "c1", "fullname": "kept"}])
        assert prune_is_safe(len(known), len(seen))
        assert missing_record_ids(known, seen) == ["rec-c2"]

    def test_expired_delta_link_re_baselines_without_losing_tracking(self):
        state = EntitySyncState(delta_link=DELTA_LINK_1, change_tracking=ChangeTrackingStatus.ENABLED)
        assert plan_sync(state, incremental=True, now_ms=self.NOW, interval_hours=24) is SyncMode.DELTA
        # a 400 that is *not* the change-tracking error: connector clears the link
        assert is_change_tracking_disabled_error(400, OTHER_400_BODY) is False
        state.delta_link = None
        assert state.change_tracking is ChangeTrackingStatus.ENABLED
        assert plan_sync(state, incremental=True, now_ms=self.NOW, interval_hours=24) is SyncMode.FULL_RECONCILE
