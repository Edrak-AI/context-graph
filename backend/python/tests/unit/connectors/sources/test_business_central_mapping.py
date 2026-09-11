"""Tests for app.connectors.sources.microsoft.business_central.mapping.

Pure functions only (entity mapping, markdown rendering incl. Arabic names and
document lines, company scoping, access-group mapping, OData filter / paging
helpers, Retry-After parsing, reconcile planning) — no network, no pydantic, no
httpx.  Fixtures are shaped like Business Central API v2.0 payloads.
"""

import pytest

from app.connectors.sources.microsoft.business_central import mapping as m
from app.connectors.sources.microsoft.business_central.mapping import (
    COMPANY_GROUP_PREFIX,
    DEFAULT_ENTITY_ORDER,
    DEFAULT_RECONCILE_INTERVAL_HOURS,
    DOCUMENT_PAGE_SIZE,
    ENTITY_SPECS,
    FIELD_LAST_RECONCILE,
    FIELD_LAST_SYNC,
    KEY_PAGE_SIZE,
    MS_PER_HOUR,
    TOKEN_SCOPE,
    Company,
    CompanyAccess,
    CompanyAccessMapping,
    EntitySyncState,
    GrantEntity,
    GrantRole,
    ReconcileMode,
    api_base_url,
    build_key_page_params,
    build_metadata,
    build_modified_filter,
    build_page_params,
    company_grants,
    company_group_external_id,
    company_group_name,
    company_path,
    company_web_url,
    diff_known_against_live,
    display_value,
    entity_path,
    epoch_ms_to_odata,
    is_posted,
    normalize_environment_name,
    normalize_name,
    parse_bc_timestamp,
    parse_companies,
    parse_company_access_mapping,
    parse_company_names,
    parse_page,
    parse_reconcile_interval_hours,
    parse_retry_after,
    plan_reconcile,
    record_external_id,
    record_id_prefix,
    record_title,
    record_web_url,
    render_record_markdown,
    resolve_company_access,
    resolve_selected_entities,
    retry_delay,
    seen_external_ids,
    select_companies,
    split_external_id,
    token_url,
)

TENANT = "11111111-2222-3333-4444-555555555555"
ENV = "Production"
CRONUS = Company(id="aaaaaaaa-0000-0000-0000-000000000001", name="CRONUS SA", display_name="CRONUS SA")
ARABIC = Company(id="aaaaaaaa-0000-0000-0000-000000000002", name="شركة المثال", display_name="شركة المثال للتجارة")
SO = ENTITY_SPECS["salesOrders"]
CUST = ENTITY_SPECS["customers"]
ITEM = ENTITY_SPECS["items"]
SINV = ENTITY_SPECS["salesInvoices"]
PO = ENTITY_SPECS["purchaseOrders"]
PINV = ENTITY_SPECS["purchaseInvoices"]
SO_ID = "bbbbbbbb-0000-0000-0000-000000000001"


def _sales_order(**overrides: object) -> dict:
    row = {
        "id": SO_ID,
        "number": "101005",
        "status": "Open",
        "orderDate": "2026-03-01",
        "postingDate": "2026-03-01",
        "requestedDeliveryDate": "0001-01-01",
        "customerId": "cccccccc-0000-0000-0000-000000000001",
        "customerNumber": "C00010",
        "customerName": "مؤسسة النور للتجارة",
        "externalDocumentNumber": "PO-77",
        "billToName": "مؤسسة النور للتجارة",
        "shipToName": "مستودع الرياض",
        "salesperson": "JR",
        "currencyCode": "SAR",
        "discountAmount": 0,
        "totalAmountExcludingTax": 1500.5,
        "totalTaxAmount": 225.075,
        "totalAmountIncludingTax": 1725.575,
        "fullyShipped": False,
        "lastModifiedDateTime": "2026-03-02T10:15:30.1234567Z",
        "salesOrderLines": [
            {
                "id": "l1", "sequence": 10000, "lineType": "Item", "lineObjectNumber": "1000",
                "description": "كرسي مكتب | أسود", "quantity": 2.0, "unitOfMeasureCode": "PCS",
                "unitPrice": 500.25, "discountPercent": 0, "amountExcludingTax": 1000.5, "amountIncludingTax": 1150.575,
            },
            {
                "id": "l2", "sequence": 20000, "lineType": "Item", "lineObjectNumber": "1001",
                "description": "Desk lamp\nwith dimmer", "quantity": 1, "unitOfMeasureCode": "PCS",
                "unitPrice": 500, "discountPercent": 0, "amountExcludingTax": 500, "amountIncludingTax": 575,
            },
        ],
    }
    row.update(overrides)
    return row


def _customer(**overrides: object) -> dict:
    row = {
        "id": "cccccccc-0000-0000-0000-000000000001",
        "number": "C00010",
        "displayName": "مؤسسة النور للتجارة",
        "type": "Company",
        "email": "info@alnoor.example",
        "phoneNumber": "+966 11 000 0000",
        "addressLine1": "طريق الملك فهد",
        "city": "الرياض",
        "country": "SA",
        "currencyCode": "",
        "taxRegistrationNumber": "300000000000003",
        "salespersonCode": "JR",
        "creditLimit": 0,
        "balanceDue": 1725.575,
        "blocked": " ",
        "lastModifiedDateTime": "2026-02-03T04:05:06Z",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Entity registry / ids / urls
# ---------------------------------------------------------------------------


class TestEntityRegistry:
    def test_specs_are_consistent(self) -> None:
        for name, spec in ENTITY_SPECS.items():
            assert spec.entity_set == name
            assert ("number", "No.") in spec.summary_fields
            if spec.lines_property:
                assert spec.line_columns, name
                assert spec.party_field in ("customerName", "vendorName")
            else:
                assert spec.is_master_data
        assert set(DEFAULT_ENTITY_ORDER) == set(ENTITY_SPECS)
        assert DEFAULT_ENTITY_ORDER[:3] == ("customers", "vendors", "items")  # master data first

    def test_record_types(self) -> None:
        assert ITEM.record_type == "PRODUCT"
        for name in ("customers", "vendors", "salesOrders", "salesInvoices", "purchaseOrders", "purchaseInvoices"):
            assert ENTITY_SPECS[name].record_type == "OTHERS"

    def test_purchase_lines_use_direct_unit_cost(self) -> None:
        sales_attrs = [a for a, _ in SO.line_columns]
        purchase_attrs = [a for a, _ in PO.line_columns]
        assert "unitPrice" in sales_attrs and "directUnitCost" not in sales_attrs
        assert "directUnitCost" in purchase_attrs and "unitPrice" not in purchase_attrs

    def test_resolve_selected_entities(self) -> None:
        assert [s.entity_set for s in resolve_selected_entities(None)] == list(DEFAULT_ENTITY_ORDER)
        assert [s.entity_set for s in resolve_selected_entities([])] == list(DEFAULT_ENTITY_ORDER)
        selected = resolve_selected_entities(["salesorders", "Customers", "bogus"])
        assert [s.entity_set for s in selected] == ["customers", "salesOrders"]

    def test_external_ids_round_trip(self) -> None:
        ext = record_external_id(CRONUS.id, SO, SO_ID)
        assert ext == f"bc:{CRONUS.id}:salesOrders:{SO_ID}"
        assert ext.startswith(record_id_prefix(CRONUS.id, SO))
        assert split_external_id(ext) == (CRONUS.id, SO, SO_ID)
        assert company_group_external_id(CRONUS.id) == f"{COMPANY_GROUP_PREFIX}{CRONUS.id}"
        for bad in ("", "sales_order:1", "bc:x:unknownSet:1", f"bc:{CRONUS.id}:salesOrders:"):
            with pytest.raises(ValueError):
                split_external_id(bad)

    def test_urls(self) -> None:
        assert api_base_url(TENANT, ENV) == f"https://api.businesscentral.dynamics.com/v2.0/{TENANT}/Production/api/v2.0/"
        assert token_url(TENANT) == f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token"
        assert TOKEN_SCOPE == "https://api.businesscentral.dynamics.com/.default"
        assert company_path(CRONUS.id, SO) == f"companies({CRONUS.id})/salesOrders"
        assert entity_path(CRONUS.id, SO, SO_ID) == f"companies({CRONUS.id})/salesOrders({SO_ID})"
        assert normalize_environment_name("") == "Production"
        assert normalize_environment_name(" Sandbox/ ") == "Sandbox"

    def test_record_web_url_deep_links_with_number_filter(self) -> None:
        url = record_web_url(TENANT, ENV, CRONUS.name, SO, _sales_order())
        assert url.startswith(f"https://businesscentral.dynamics.com/{TENANT}/Production?company=CRONUS%20SA&page=42")
        assert "filter=%27No.%27%20IS%20%27101005%27" in url
        # Arabic company names are percent-encoded UTF-8, never transliterated
        url_ar = company_web_url(TENANT, ENV, ARABIC.name)
        assert url_ar.endswith("?company=%D8%B4%D8%B1%D9%83%D8%A9%20%D8%A7%D9%84%D9%85%D8%AB%D8%A7%D9%84")
        # a row without a number still links to the company + page
        assert record_web_url(TENANT, ENV, CRONUS.name, CUST, {"id": "x"}).endswith("&page=21")

    def test_posted_invoices_use_the_posted_card_page(self) -> None:
        draft = {"id": "i", "number": "S-INV-1", "status": "Draft"}
        posted = {"id": "i", "number": "S-INV-1", "status": "Paid"}
        assert not is_posted(SINV, draft) and is_posted(SINV, posted)
        assert "&page=43&" in record_web_url(TENANT, ENV, CRONUS.name, SINV, draft)
        assert "&page=132&" in record_web_url(TENANT, ENV, CRONUS.name, SINV, posted)
        assert "&page=138&" in record_web_url(TENANT, ENV, CRONUS.name, PINV, {"id": "i", "number": "P-1", "status": "Open"})
        assert not is_posted(SO, {"id": "o", "status": "Open"})  # orders have no posted page


# ---------------------------------------------------------------------------
# Timestamps, filters, paging, query options
# ---------------------------------------------------------------------------


class TestQueryHelpers:
    def test_parse_bc_timestamp(self) -> None:
        assert parse_bc_timestamp("2026-02-03T04:05:06Z") == 1770091506000
        assert parse_bc_timestamp("2026-02-03T04:05:06.1234567Z") == 1770091506123
        assert parse_bc_timestamp("2026-02-03T04:05:06+03:00") == 1770091506000 - 3 * MS_PER_HOUR
        assert parse_bc_timestamp("0001-01-01T00:00:00Z") is None
        assert parse_bc_timestamp("") is None
        assert parse_bc_timestamp(None) is None
        assert parse_bc_timestamp("not a date") is None

    def test_epoch_ms_to_odata(self) -> None:
        assert epoch_ms_to_odata(1770091506123) == "2026-02-03T04:05:06Z"

    def test_build_modified_filter(self) -> None:
        since = 1770091506000
        assert build_modified_filter() is None
        assert build_modified_filter(since_ms=since) == "lastModifiedDateTime gt 2026-02-03T04:05:06Z"
        assert build_modified_filter(start_ms=since) == "lastModifiedDateTime ge 2026-02-03T04:05:06Z"
        assert build_modified_filter(since_ms=since, start_ms=since - 1000) == "lastModifiedDateTime gt 2026-02-03T04:05:06Z"
        assert build_modified_filter(since_ms=since - 1000, start_ms=since) == "lastModifiedDateTime ge 2026-02-03T04:05:06Z"
        assert build_modified_filter(end_ms=since) == "lastModifiedDateTime le 2026-02-03T04:05:06Z"
        assert build_modified_filter(since_ms=since, end_ms=since + MS_PER_HOUR) == (
            "lastModifiedDateTime gt 2026-02-03T04:05:06Z and lastModifiedDateTime le 2026-02-03T05:05:06Z"
        )

    def test_page_params(self) -> None:
        assert build_page_params(SO, None) == {"$top": str(DOCUMENT_PAGE_SIZE), "$expand": "salesOrderLines"}
        assert build_page_params(CUST, "lastModifiedDateTime gt X") == {"$top": str(DOCUMENT_PAGE_SIZE), "$filter": "lastModifiedDateTime gt X"}
        assert build_key_page_params() == {"$select": "id", "$top": str(KEY_PAGE_SIZE)}

    def test_parse_page_follows_next_link_and_skips_idless_rows(self) -> None:
        payload = {
            "@odata.context": "…/$metadata#companies(x)/customers",
            "value": [{"id": "1", "number": "C1"}, {"number": "no-id"}, "garbage", {"id": "2"}],
            "@odata.nextLink": "https://api.businesscentral.dynamics.com/v2.0/t/e/api/v2.0/companies(x)/customers?$skiptoken=abc",
        }
        page = parse_page(payload)
        assert [r["id"] for r in page.rows] == ["1", "2"]
        assert page.next_link is not None and page.next_link.endswith("$skiptoken=abc")
        last = parse_page({"value": [{"id": "3"}]})
        assert last.next_link is None and len(last.rows) == 1
        assert parse_page({}).rows == []

    def test_parse_companies(self) -> None:
        payload = {"value": [
            {"id": CRONUS.id, "name": "CRONUS SA", "displayName": "CRONUS SA"},
            {"id": ARABIC.id, "name": "شركة المثال", "displayName": "شركة المثال للتجارة"},
            {"id": "", "name": "broken"},
            {"id": "x", "name": ""},
        ]}
        companies = parse_companies(payload)
        assert companies == [CRONUS, ARABIC]
        assert ARABIC.label == "شركة المثال للتجارة"
        assert Company(id="i", name="N", display_name="").label == "N"


# ---------------------------------------------------------------------------
# Retry-After and reconcile helpers
# ---------------------------------------------------------------------------


class TestRetryAndReconcile:
    def test_parse_retry_after_delay_seconds_and_http_date(self) -> None:
        assert parse_retry_after("30") == 30.0
        assert parse_retry_after(" 2.5 ") == 2.5
        assert parse_retry_after("-3") == 0.0
        assert parse_retry_after(None) is None
        assert parse_retry_after("") is None
        assert parse_retry_after("soon") is None
        # HTTP-date 90 s after "now"
        assert parse_retry_after("Wed, 21 Oct 2015 07:29:30 GMT", now_s=1445412480.0) == 90.0

    def test_retry_delay_prefers_header_and_clamps(self) -> None:
        assert retry_delay("7", fallback=1.0) == 7.0
        assert retry_delay(None, fallback=4.0) == 4.0
        assert retry_delay("0", fallback=4.0) == 0.5          # minimum
        assert retry_delay("3600", fallback=1.0) == 60.0      # maximum
        assert retry_delay("garbage", fallback=2.0) == 2.0

    def test_parse_reconcile_interval_hours(self) -> None:
        assert parse_reconcile_interval_hours(None) == DEFAULT_RECONCILE_INTERVAL_HOURS
        assert parse_reconcile_interval_hours("") == DEFAULT_RECONCILE_INTERVAL_HOURS
        assert parse_reconcile_interval_hours("12") == 12.0
        assert parse_reconcile_interval_hours(6) == 6.0
        assert parse_reconcile_interval_hours("0") == 0.0
        assert parse_reconcile_interval_hours("-5") == 0.0
        assert parse_reconcile_interval_hours("abc") == DEFAULT_RECONCILE_INTERVAL_HOURS
        assert parse_reconcile_interval_hours(True) == DEFAULT_RECONCILE_INTERVAL_HOURS

    def test_sync_state_round_trip(self) -> None:
        state = EntitySyncState.from_sync_point({FIELD_LAST_SYNC: 10, FIELD_LAST_RECONCILE: "5"})
        assert (state.last_sync_timestamp, state.last_reconcile_timestamp) == (10, 5)
        assert state.to_sync_point() == {FIELD_LAST_SYNC: 10, FIELD_LAST_RECONCILE: 5}
        empty = EntitySyncState.from_sync_point(None)
        assert empty.last_sync_timestamp is None and empty.last_reconcile_timestamp is None

    def test_plan_reconcile(self) -> None:
        now = 100 * MS_PER_HOUR
        fresh = EntitySyncState()
        assert plan_reconcile(fresh, full_read=True, now_ms=now, interval_hours=24) is ReconcileMode.SEEN
        assert plan_reconcile(fresh, full_read=False, now_ms=now, interval_hours=24) is ReconcileMode.KEY_SET  # never reconciled
        recent = EntitySyncState(last_sync_timestamp=now - MS_PER_HOUR, last_reconcile_timestamp=now - 2 * MS_PER_HOUR)
        assert plan_reconcile(recent, full_read=False, now_ms=now, interval_hours=24) is ReconcileMode.NONE
        stale = EntitySyncState(last_sync_timestamp=now - MS_PER_HOUR, last_reconcile_timestamp=now - 25 * MS_PER_HOUR)
        assert plan_reconcile(stale, full_read=False, now_ms=now, interval_hours=24) is ReconcileMode.KEY_SET
        assert plan_reconcile(stale, full_read=False, now_ms=now, interval_hours=48) is ReconcileMode.NONE
        # 0 disables the reconcile even for full reads
        assert plan_reconcile(fresh, full_read=True, now_ms=now, interval_hours=0) is ReconcileMode.NONE

    def test_diff_known_against_live(self) -> None:
        known = {"bc:c:salesOrders:1": "r1", "bc:c:salesOrders:2": "r2", "bc:c:salesOrders:3": "r3"}
        plan = diff_known_against_live(known, {"bc:c:salesOrders:1", "bc:c:salesOrders:3", "bc:c:salesOrders:9"})
        assert plan.delete_record_ids == ("r2",)
        assert (plan.known, plan.live, plan.skipped_reason) == (3, 3, None)
        # safety guard: an empty live set never wipes the graph
        guarded = diff_known_against_live(known, set())
        assert guarded.delete_record_ids == () and guarded.skipped_reason
        assert diff_known_against_live({}, set()).skipped_reason is None

    def test_seen_external_ids(self) -> None:
        rows = [{"id": "1"}, {"id": ""}, {"number": "x"}, {"id": "2"}]
        assert seen_external_ids(CRONUS.id, SO, rows) == {
            record_external_id(CRONUS.id, SO, "1"), record_external_id(CRONUS.id, SO, "2"),
        }


# ---------------------------------------------------------------------------
# Company scoping and access mapping
# ---------------------------------------------------------------------------


class TestCompanyScoping:
    def test_parse_company_names(self) -> None:
        assert parse_company_names(None) == []
        assert parse_company_names("") == []
        assert parse_company_names("CRONUS SA, شركة المثال ;  CRONUS SA\nOther") == ["CRONUS SA", "شركة المثال", "Other"]
        assert parse_company_names(["A", " B "]) == ["A", "B"]

    def test_normalize_name_keeps_arabic_letters(self) -> None:
        assert normalize_name("  CRONUS   sa ") == "cronus sa"
        assert normalize_name("شركة  المثال") == "شركة المثال"
        assert normalize_name("شركة المثال") != normalize_name("شركة المثال للتجارة")

    def test_select_companies(self) -> None:
        companies = [CRONUS, ARABIC]
        everything = select_companies(companies, [])
        assert everything.selected == (CRONUS, ARABIC) and everything.unmatched == ()
        by_name = select_companies(companies, ["cronus sa"])
        assert by_name.selected == (CRONUS,) and by_name.unmatched == ()
        by_display_name = select_companies(companies, ["شركة المثال للتجارة"])
        assert by_display_name.selected == (ARABIC,)
        by_id = select_companies(companies, [ARABIC.id.upper()])
        assert by_id.selected == (ARABIC,)
        missing = select_companies(companies, ["شركة المثال", "Nope Ltd"])
        assert missing.selected == (ARABIC,) and missing.unmatched == ("Nope Ltd",)

    def test_parse_company_access_mapping_text(self) -> None:
        mapping = parse_company_access_mapping(
            "CRONUS SA = BC Finance Readers, 3f2b0c9e-0000-0000-0000-000000000001\n"
            "شركة المثال = قرّاء الحسابات; * = All Staff\n"
        )
        assert mapping.groups_by_company[normalize_name("CRONUS SA")] == ("BC Finance Readers", "3f2b0c9e-0000-0000-0000-000000000001")
        assert mapping.groups_by_company[normalize_name("شركة المثال")] == ("قرّاء الحسابات",)
        assert mapping.entry_labels == {normalize_name("CRONUS SA"): "CRONUS SA", normalize_name("شركة المثال"): "شركة المثال"}
        assert mapping.default_groups == ("All Staff",)
        assert mapping.referenced_groups() == ["All Staff", "BC Finance Readers", "3f2b0c9e-0000-0000-0000-000000000001", "قرّاء الحسابات"]
        assert mapping.groups_for(CRONUS) == ("BC Finance Readers", "3f2b0c9e-0000-0000-0000-000000000001")
        assert mapping.groups_for(ARABIC) == ("قرّاء الحسابات",)
        assert mapping.groups_for(Company(id="z", name="Zeta", display_name="Zeta")) == ("All Staff",)

    @pytest.mark.parametrize(
        ("value", "fragment"),
        [
            ("CRONUS SA = Readers\nbroken line", "broken line"),
            ("Empty Co =", "Empty Co"),
            ("{not json", "not valid JSON"),
            ('["CRONUS SA"]', "Company = group"),  # not an object → read as text → not a 'Company = ...' line
            ({"CRONUS SA": []}, "lists no group"),
        ],
    )
    def test_parse_company_access_mapping_rejects_malformed_input(self, value: object, fragment: str) -> None:
        # fail-closed: a mapping that is silently dropped would leave companies readable by the wrong people
        with pytest.raises(ValueError, match=fragment):
            parse_company_access_mapping(value)

    def test_parse_company_access_mapping_json_and_empty(self) -> None:
        mapping = parse_company_access_mapping('{"CRONUS SA": ["Readers"], "*": "Everyone Finance"}')
        assert mapping.groups_by_company == {"cronus sa": ("Readers",)}
        assert mapping.default_groups == ("Everyone Finance",)
        assert parse_company_access_mapping(None).is_empty()
        assert parse_company_access_mapping("   ").is_empty()
        assert parse_company_access_mapping({"CRONUS SA": "A, B"}).groups_by_company == {"cronus sa": ("A", "B")}

    def test_unmatched_entries_are_surfaced_as_written(self) -> None:
        mapping = parse_company_access_mapping("Cronus  SA = Readers\nNope Ltd = Readers\n* = Staff")
        assert mapping.unmatched_entries([CRONUS, ARABIC]) == ["Nope Ltd"]
        assert parse_company_access_mapping({ARABIC.id.upper(): "Readers"}).unmatched_entries([ARABIC]) == []

    def test_company_without_entry_grants_nobody(self) -> None:
        accesses = resolve_company_access([CRONUS, ARABIC], parse_company_access_mapping("CRONUS SA = Readers"))
        gated, unmapped = accesses
        assert gated.group_refs == ("Readers",) and gated.entra_refs == ("Readers",) and not gated.org_wide
        assert [(g.entity_type, g.role, g.external_id) for g in company_grants(gated)] == [
            (GrantEntity.GROUP, GrantRole.READER, company_group_external_id(CRONUS.id)),
        ]
        # no entry and no '*' key: only the (empty) company group, never the org
        assert unmapped.group_refs == () and not unmapped.org_wide
        assert [(g.entity_type, g.role, g.external_id) for g in company_grants(unmapped)] == [
            (GrantEntity.GROUP, GrantRole.READER, company_group_external_id(ARABIC.id)),
        ]
        assert not CompanyAccess(company=CRONUS).org_wide
        assert CompanyAccessMapping().groups_for(CRONUS) == ()
        assert company_group_name(ARABIC) == "Business Central · شركة المثال للتجارة"

    def test_star_value_is_the_explicit_org_wide_opt_in(self) -> None:
        mapping = parse_company_access_mapping("CRONUS SA = *\nشركة المثال = Readers, *")
        assert mapping.referenced_groups() == ["Readers"]  # '*' is not an Entra group
        cronus, arabic = resolve_company_access([CRONUS, ARABIC], mapping)
        assert cronus.org_wide and cronus.entra_refs == ()
        assert [(g.entity_type, g.role, g.external_id) for g in company_grants(cronus)] == [
            (GrantEntity.GROUP, GrantRole.READER, company_group_external_id(CRONUS.id)),
            (GrantEntity.ORG, GrantRole.READER, None),
        ]
        # '*' mixed with groups: org-wide wins, the groups are still resolved for the company group
        assert arabic.org_wide and arabic.entra_refs == ("Readers",)
        assert any(g.entity_type == GrantEntity.ORG for g in company_grants(arabic))
        # '* = *' as the default makes every company without its own entry org-wide
        by_default = resolve_company_access([CRONUS, ARABIC], parse_company_access_mapping("CRONUS SA = Readers; * = *"))
        assert not by_default[0].org_wide and by_default[1].org_wide


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


class TestRendering:
    def test_display_value(self) -> None:
        row = {"a": True, "b": False, "c": 12.0, "d": 12.5, "e": 0, "f": " ", "g": "0001-01-01", "h": "00000000-0000-0000-0000-000000000000", "i": [1], "j": "SAR"}
        assert display_value(row, "a") == "Yes" and display_value(row, "b") == "No"
        assert display_value(row, "c") == "12" and display_value(row, "d") == "12.5"
        assert display_value(row, "e") == "0"
        assert display_value(row, "f") is None
        assert display_value(row, "g") is None and display_value(row, "h") is None
        assert display_value(row, "i") is None
        assert display_value(row, "j") == "SAR"
        assert display_value(row, "missing") is None

    def test_record_titles(self) -> None:
        assert record_title(CUST, _customer()) == "مؤسسة النور للتجارة (C00010)"
        assert record_title(CUST, {"id": "x", "displayName": "Only name"}) == "Only name"
        assert record_title(CUST, {"id": "x", "number": "C1"}) == "Customer C1"
        assert record_title(SO, _sales_order()) == "Sales order 101005 · مؤسسة النور للتجارة"
        assert record_title(SO, {"id": "o1", "number": "5"}) == "Sales order 5"
        assert record_title(PO, {"id": "p1", "number": "106001", "vendorName": "Fabrikam"}) == "Purchase order 106001 · Fabrikam"

    def test_sales_order_markdown_with_arabic_lines(self) -> None:
        markdown, metadata = render_record_markdown(ARABIC, SO, _sales_order(), TENANT, ENV)
        assert markdown.startswith("# Sales order 101005 · مؤسسة النور للتجارة\n")
        assert "**Type:** Business Central Sales order · **Company:** شركة المثال للتجارة · **Status:** Open" in markdown
        assert "- **Customer:** مؤسسة النور للتجارة" in markdown
        assert "- **Ship-to:** مستودع الرياض" in markdown
        assert "- **Total incl. tax:** 1725.575" in markdown
        assert "- **Fully shipped:** No" in markdown
        assert "Requested delivery date" not in markdown  # 0001-01-01 sentinel dropped
        assert "## Lines" in markdown
        assert "| # | Type | No. | Description | Quantity | Unit | Unit price | Discount % | Amount excl. tax | Amount incl. tax |" in markdown
        assert "| 10000 | Item | 1000 | كرسي مكتب \\| أسود | 2 | PCS | 500.25 | 0 | 1000.5 | 1150.575 |" in markdown
        assert "| 20000 | Item | 1001 | Desk lamp with dimmer | 1 | PCS | 500 | 0 | 500 | 575 |" in markdown
        assert "- Business Central company: شركة المثال للتجارة (aaaaaaaa-0000-0000-0000-000000000002)" in markdown
        assert "- Entity set: salesOrders" in markdown
        assert f"- Record id: {SO_ID}" in markdown
        assert "- Modified: 2026-03-02T10:15:30.1234567Z" in markdown
        assert f"- URL: {metadata['url']}" in markdown
        assert markdown.endswith("\n") and not markdown.endswith("\n\n")
        assert metadata["company_id"] == ARABIC.id and metadata["number"] == "101005" and metadata["status"] == "Open"

    def test_customer_markdown_passes_arabic_through_unchanged(self) -> None:
        markdown, _ = render_record_markdown(CRONUS, CUST, _customer(), TENANT, ENV)
        assert markdown.startswith("# مؤسسة النور للتجارة (C00010)\n")
        assert "**Type:** Business Central Customer · **Company:** CRONUS SA" in markdown
        assert "**Status:**" not in markdown
        assert "- **Address:** طريق الملك فهد" in markdown
        assert "- **City:** الرياض" in markdown
        assert "- **Tax registration no.:** 300000000000003" in markdown
        assert "- **Balance due:** 1725.575" in markdown
        assert "**Currency:**" not in markdown and "**Blocked:**" not in markdown and "**Credit limit:** 0" in markdown
        assert "## Lines" not in markdown
        for fragment in ("مؤسسة النور للتجارة", "طريق الملك فهد", "الرياض"):
            assert fragment in markdown  # byte-identical, no NFKC / trimming of the content itself

    def test_item_markdown_renders_description_and_product_fields(self) -> None:
        row = {
            "id": "i1", "number": "1000", "displayName": "كرسي مكتب", "displayName2": "قابل للتعديل، أسود",
            "type": "Inventory", "itemCategoryCode": "FURNITURE", "baseUnitOfMeasureCode": "PCS",
            "unitPrice": 500.25, "unitCost": 300, "inventory": 12, "blocked": False, "priceIncludesTax": False,
            "lastModifiedDateTime": "2026-01-01T00:00:00Z",
        }
        markdown, _ = render_record_markdown(CRONUS, ITEM, row, TENANT, ENV)
        assert markdown.startswith("# كرسي مكتب (1000)\n")
        assert "## Description\nقابل للتعديل، أسود\n" in markdown
        assert "- **Item category:** FURNITURE" in markdown
        assert "- **Unit price:** 500.25" in markdown
        assert "- **Blocked:** No" in markdown

    def test_purchase_invoice_markdown_uses_direct_unit_cost_and_posted_url(self) -> None:
        row = {
            "id": "pi1", "number": "108001", "status": "Open", "vendorName": "Fabrikam, Inc.", "vendorNumber": "V10000",
            "vendorInvoiceNumber": "F-2231", "currencyCode": "USD", "totalAmountIncludingTax": 100,
            "purchaseInvoiceLines": [{"id": "l", "sequence": 10000, "lineType": "Item", "lineObjectNumber": "1896-S",
                                      "description": "ATHENS Desk", "quantity": 1, "directUnitCost": 100, "amountExcludingTax": 100, "amountIncludingTax": 100}],
        }
        markdown, metadata = render_record_markdown(CRONUS, PINV, row, TENANT, ENV)
        assert "| Direct unit cost |" in markdown
        assert "| 10000 | Item | 1896-S | ATHENS Desk | 1 |  | 100 |  | 100 | 100 |" in markdown
        assert "- **Vendor invoice no.:** F-2231" in markdown
        assert "&page=138&" in metadata["url"]

    def test_build_metadata_without_id(self) -> None:
        metadata = build_metadata(CRONUS, SO, {"number": "1"}, TENANT, ENV)
        assert metadata["id"] == "" and metadata["url"] is None
        assert metadata["source"] == "microsoft-business-central"


class TestAccessPolicyFingerprint:
    """``access_policy_fingerprint`` / ``changed_access_policies`` (security review S06)."""

    def test_fingerprint_tracks_effective_grants_not_group_membership(self) -> None:
        company = m.Company(id="c1", name="Finance", display_name="Finance")
        restricted = m.access_policy_fingerprint(m.CompanyAccess(company=company, group_refs=("Finance Readers",)))
        other_group = m.access_policy_fingerprint(m.CompanyAccess(company=company, group_refs=("Other Readers",)))
        nobody = m.access_policy_fingerprint(m.CompanyAccess(company=company))
        org_wide = m.access_policy_fingerprint(m.CompanyAccess(company=company, group_refs=("*",)))
        org_and_group = m.access_policy_fingerprint(m.CompanyAccess(company=company, group_refs=("Finance Readers", "*")))
        assert len(restricted) == 64 and restricted == m.access_policy_fingerprint(m.CompanyAccess(company=company, group_refs=("Finance Readers",)))
        # the Entra group named in the mapping is membership, not a grant: the record edges are identical
        assert restricted == other_group == nobody
        assert org_wide != restricted and org_wide == org_and_group
        # another company gets another company group → different digest
        assert m.access_policy_fingerprint(m.CompanyAccess(company=m.Company(id="c2", name="X", display_name="X"))) != nobody

    def test_changed_access_policies(self) -> None:
        current = {"c1": "aaa", "c2": "bbb", "c3": "ccc"}
        assert m.changed_access_policies(None, current) == ["c1", "c2", "c3"]
        assert m.changed_access_policies({}, current) == ["c1", "c2", "c3"]
        assert m.changed_access_policies({"c1": "aaa", "c2": "bbb", "c3": "ccc"}, current) == []
        assert m.changed_access_policies({"c1": "aaa", "c2": "OLD", "gone": "zzz"}, current) == ["c2", "c3"]
        assert m.changed_access_policies({"c1": 1}, {"c1": "aaa"}) == ["c1"]

    def test_company_id_of_external_id(self) -> None:
        spec = m.ENTITY_SPECS["salesInvoices"]
        assert m.company_id_of_external_id(m.record_external_id("c1", spec, "row-1")) == "c1"
        assert m.company_id_of_external_id("bc:c1:salesInvoices:") is None
        assert m.company_id_of_external_id("opportunity:abc") is None
        assert m.company_id_of_external_id("") is None
        assert m.company_id_of_external_id("bc:c1:x") is None

