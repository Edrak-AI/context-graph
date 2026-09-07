"""Tests for app.connectors.sources.sap.mapping.

Pure functions only (entity registry, OData helpers, rendering, authorization
mapping + RBAC derivation) — no network, no pydantic, no httpx.  Written with
``unittest`` so they run under pytest *and* with a bare interpreter::

    cd backend/python && python3 tests/unit/connectors/sources/test_sap_mapping.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from app.connectors.sources.sap.mapping import (  # noqa: E402
    ADMINS_GROUP_ID,
    ALL_FILTER_ENTITIES,
    ATTACHMENT_ID_PREFIX,
    DEFAULT_ENTITY_ORDER,
    DEFAULT_RECONCILE_INTERVAL_HOURS,
    ENTITY_SPECS,
    LANGUAGE_PREFERENCE,
    RECONCILE_KEY_PAGE_SIZE,
    SCOPE_COMPANY_CODE,
    SCOPE_PLANT,
    SCOPE_SALES_ORG,
    AuthorizationContext,
    GrantEntity,
    GrantRole,
    PermissionGrant,
    attachment_content_url,
    attachment_external_id,
    attachment_key,
    attachment_list_url,
    attachments_selected,
    build_key_page_params,
    build_metadata,
    build_modified_filter,
    build_page_params,
    build_scope_filter,
    combine_filters,
    derive_grants,
    entity_list_web_url,
    entity_url,
    external_id_entity,
    fiori_web_url,
    group_display_name,
    group_ids_in_grants,
    key_predicate,
    localized_texts,
    metadata_url,
    named_group_external_id,
    next_page_skip,
    normalize_base_url,
    normalize_language_code,
    odata_collection,
    odata_datetime_literal,
    parse_authorization_mapping,
    parse_entity,
    parse_page,
    parse_reconcile_interval_hours,
    parse_retry_after,
    parse_sap_timestamp,
    parse_scope_key,
    plan_reconcile,
    preferred_text,
    product_description,
    product_descriptions,
    reconcile_due,
    record_external_id,
    record_group_external_id,
    record_title,
    record_web_url,
    render_record_markdown,
    resolve_selected_entities,
    retry_delay,
    scope_codes,
    scope_group_external_id,
    slugify_group_name,
    split_external_id,
)

BASE = "https://my300000-api.s4hana.cloud.sap"
FIORI = "https://my300000.s4hana.cloud.sap"
SO = ENTITY_SPECS["sales_order"]
PO = ENTITY_SPECS["purchase_order"]
BP = ENTITY_SPECS["business_partner"]
PROD = ENTITY_SPECS["product"]
INV = ENTITY_SPECS["supplier_invoice"]

MAPPING = {
    "admins": ["group:SAP Administrators", "user:erp-admin@edrak.com"],
    "companycode:1010": ["finance@edrak.com", "group:SAP Finance 1010"],
    "salesorg:1010": ["group:0f3f6e1c-3f0e-4d3e-9a8b-3a1a4e0c7b21"],
    "plant:1010": ["group:Unknown Plant Group"],
    "entity:product": ["group:Everyone"],
    "groups": {"SAP Finance 1010": ["cfo@edrak.com", "Finance@Edrak.com"], "Everyone": ["all@edrak.com"]},
    "users": {"cb9980000042": "Jane.Doe@edrak.com"},
    "bogus:1": ["x@edrak.com"],
}

ENTRA = {
    "SAP Administrators": ["admin1@edrak.com", "admin2@edrak.com"],
    "SAP Finance 1010": ["fin-entra@edrak.com"],
    "0f3f6e1c-3f0e-4d3e-9a8b-3a1a4e0c7b21": ["sales-de@edrak.com"],
    "Everyone": [],
}


def _so_row(**overrides) -> dict:
    row = {
        "SalesOrder": "0000000123",
        "SalesOrderType": "OR",
        "SalesOrganization": "1010",
        "DistributionChannel": "10",
        "OrganizationDivision": "00",
        "SoldToParty": "10100001",
        "PurchaseOrderByCustomer": "PO-ACME-77",
        "TotalNetAmount": "1500.00",
        "TransactionCurrency": "EUR",
        "SalesOrderDate": "/Date(1714521600000)/",
        "RequestedDeliveryDate": "/Date(1715126400000)/",
        "OverallSDProcessStatus": "A",
        "CreatedByUser": "CB9980000042",
        "CreationDate": "/Date(1714521600000)/",
        "LastChangeDateTime": "/Date(1714608000000+0000)/",
        "to_Item": {"results": [
            {"SalesOrderItem": "10", "Material": "TG11", "SalesOrderItemText": "Trading | Good", "RequestedQuantity": "2",
             "RequestedQuantityUnit": "PC", "NetAmount": "1000.00", "ProductionPlant": "1010"},
            {"SalesOrderItem": "20", "Material": "TG12", "SalesOrderItemText": "Second", "RequestedQuantity": "1",
             "RequestedQuantityUnit": "PC", "NetAmount": "500.00", "ProductionPlant": "1710"},
        ]},
    }
    row.update(overrides)
    return row


class TestRegistry(unittest.TestCase):
    def test_default_order_covers_all_specs(self):
        self.assertEqual(set(DEFAULT_ENTITY_ORDER), set(ENTITY_SPECS))
        self.assertEqual(ALL_FILTER_ENTITIES[-1], "attachment")

    def test_specs_are_consistent(self):
        for spec in ENTITY_SPECS.values():
            for key in spec.key_fields:
                self.assertIn(key, spec.select_fields, f"{spec.name}: key {key} not selected")
            if spec.change_field:
                self.assertIn(spec.change_field, spec.select_fields, f"{spec.name}: change field not selected")
            self.assertIn(spec.change_field_kind, ("datetime", "datetimeoffset", "date"))
            self.assertTrue(spec.fiori_intent and spec.attachment_object_type)
            for prop, _label in spec.summary_fields:
                self.assertIn(prop, spec.select_fields, f"{spec.name}: summary {prop} not selected")

    def test_resolve_selected_entities(self):
        self.assertEqual([s.name for s in resolve_selected_entities(None)], list(DEFAULT_ENTITY_ORDER))
        self.assertEqual([s.name for s in resolve_selected_entities(["product", "Sales_Order", "nope", "attachment"])],
                         ["sales_order", "product"])
        self.assertTrue(attachments_selected(["product", "attachment"]))
        self.assertFalse(attachments_selected(["product"]))
        self.assertFalse(attachments_selected(None))

    def test_sales_orders_are_plain_records_and_products_are_products(self):
        self.assertEqual(SO.record_type, "OTHERS")
        self.assertEqual(PROD.record_type, "PRODUCT")
        self.assertEqual(PROD.record_group_type, "PRODUCT")


class TestKeysAndIds(unittest.TestCase):
    def test_single_and_composite_key_predicates(self):
        self.assertEqual(key_predicate(SO, ["123"]), "'123'")
        self.assertEqual(key_predicate(INV, ["5105600001", "2024"]), "SupplierInvoice='5105600001',FiscalYear='2024'")
        self.assertEqual(key_predicate(SO, ["O'Neil"]), "'O''Neil'")
        with self.assertRaises(ValueError):
            key_predicate(INV, ["only-one"])

    def test_external_ids_round_trip(self):
        ext = record_external_id(INV, ["5105600001", "2024"])
        self.assertEqual(ext, "supplier_invoice:5105600001/2024")
        self.assertEqual(split_external_id(ext), ("supplier_invoice", ("5105600001", "2024"), None))
        ext = record_external_id(SO, ["A/B"])
        self.assertEqual(split_external_id(ext), ("sales_order", ("A/B",), None))
        att = attachment_external_id(SO, ["123"], "LD-77")
        self.assertTrue(att.startswith(f"{ATTACHMENT_ID_PREFIX}:sales_order:123:"))
        self.assertEqual(split_external_id(att), ("sales_order", ("123",), "LD-77"))
        with self.assertRaises(ValueError):
            split_external_id("garbage")
        self.assertEqual(record_group_external_id(PO), "sap:purchase_order")


class TestUrls(unittest.TestCase):
    def test_normalize_base_url(self):
        self.assertEqual(normalize_base_url(" my300000-api.s4hana.cloud.sap/ "), BASE)
        self.assertEqual(normalize_base_url("http://gw.corp:8000/"), "http://gw.corp:8000")
        with self.assertRaises(ValueError):
            normalize_base_url("  ")

    def test_odata_urls(self):
        self.assertEqual(metadata_url(BASE, BP), f"{BASE}/sap/opu/odata/sap/API_BUSINESS_PARTNER/$metadata")
        self.assertEqual(entity_url(BASE, SO, ["123"]), f"{BASE}/sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder('123')")
        self.assertEqual(entity_url(BASE, SO, ["123"], sap_client="100"),
                         f"{BASE}/sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder('123')?sap-client=100")
        self.assertEqual(entity_url(BASE, INV, ["1", "2024"]),
                         f"{BASE}/sap/opu/odata/sap/API_SUPPLIERINVOICE_PROCESS_SRV/A_SupplierInvoice(SupplierInvoice='1',FiscalYear='2024')")

    def test_fiori_deep_links_win_when_configured(self):
        self.assertEqual(fiori_web_url(FIORI, SO, ["123"]), f"{FIORI}/ui#SalesOrder-displayFactSheet?SalesOrder=123")
        self.assertEqual(fiori_web_url(f"{FIORI}/ui", INV, ["1", "2024"]),
                         f"{FIORI}/ui#SupplierInvoice-displayFactSheet?SupplierInvoice=1&FiscalYear=2024")
        self.assertIsNone(fiori_web_url("", SO, ["123"]))
        self.assertEqual(record_web_url(BASE, SO, ["123"], FIORI), fiori_web_url(FIORI, SO, ["123"]))
        self.assertEqual(record_web_url(BASE, SO, ["123"], None, "100"), entity_url(BASE, SO, ["123"], "100"))
        self.assertEqual(entity_list_web_url(BASE, SO), f"{BASE}/sap/opu/odata/sap/API_SALES_ORDER_SRV/A_SalesOrder")
        self.assertEqual(entity_list_web_url(BASE, SO, FIORI), f"{FIORI}/ui#SalesOrder-manage")


class TestODataPayloads(unittest.TestCase):
    def test_collection_unwrap(self):
        self.assertEqual(odata_collection({"results": [1, 2]}), [1, 2])
        self.assertEqual(odata_collection({"value": [3]}), [3])
        self.assertEqual(odata_collection([4]), [4])
        self.assertEqual(odata_collection({"__deferred": {"uri": "x"}}), [])
        self.assertEqual(odata_collection(None), [])

    def test_parse_page_v2_and_v4(self):
        page = parse_page({"d": {"results": [{"a": 1}], "__count": "42", "__next": "https://x/next"}})
        self.assertEqual(page.rows, [{"a": 1}])
        self.assertEqual(page.total_count, 42)
        self.assertEqual(page.next_link, "https://x/next")
        page = parse_page({"value": [{"b": 2}], "@odata.count": 7, "@odata.nextLink": "n"}, odata_version=4)
        self.assertEqual((page.rows, page.total_count, page.next_link), ([{"b": 2}], 7, "n"))
        self.assertEqual(parse_page({"d": [{"c": 3}]}).rows, [{"c": 3}])
        self.assertEqual(parse_page({}).rows, [])
        self.assertEqual(parse_entity({"d": {"SalesOrder": "1"}}), {"SalesOrder": "1"})
        self.assertIsNone(parse_entity({"d": {}}))
        self.assertEqual(parse_entity({"SalesOrder": "1"}, 4), {"SalesOrder": "1"})

    def test_page_params_and_skip_logic(self):
        params = build_page_params(SO, "SalesOrganization eq '1010'", skip=500, top=500)
        self.assertEqual(params["$top"], "500")
        self.assertEqual(params["$skip"], "500")
        self.assertEqual(params["$inlinecount"], "allpages")
        self.assertEqual(params["$format"], "json")
        self.assertEqual(params["$expand"], "to_Item")
        self.assertEqual(params["$orderby"], "LastChangeDateTime asc")
        self.assertEqual(params["$filter"], "SalesOrganization eq '1010'")
        self.assertIn("SalesOrder", params["$select"].split(","))

        full = type("P", (), {})()
        self.assertIsNone(next_page_skip(parse_page({"d": {"results": [{}] * 500, "__next": "x"}}), 0, 500))
        self.assertIsNone(next_page_skip(parse_page({"d": {"results": [{}] * 10}}), 0, 500))
        self.assertEqual(next_page_skip(parse_page({"d": {"results": [{}] * 500}}), 0, 500), 500)
        self.assertIsNone(next_page_skip(parse_page({"d": {"results": [{}] * 500, "__count": "500"}}), 0, 500))
        self.assertEqual(next_page_skip(parse_page({"d": {"results": [{}] * 500, "__count": "1200"}}), 500, 500), 1000)
        del full


class TestTimestampsAndFilters(unittest.TestCase):
    def test_parse_sap_timestamp(self):
        self.assertEqual(parse_sap_timestamp("/Date(1714521600000)/"), 1714521600000)
        self.assertEqual(parse_sap_timestamp("/Date(1714521600000+0120)/"), 1714521600000)
        self.assertEqual(parse_sap_timestamp("2024-05-01T00:00:00Z"), 1714521600000)
        self.assertEqual(parse_sap_timestamp("2024-05-01"), 1714521600000)
        self.assertEqual(parse_sap_timestamp(1714521600000), 1714521600000)
        self.assertIsNone(parse_sap_timestamp("PT10H"))
        self.assertIsNone(parse_sap_timestamp(None))
        self.assertIsNone(parse_sap_timestamp(""))

    def test_literals_per_kind(self):
        ms = 1714521600000
        self.assertEqual(odata_datetime_literal(ms, "datetime"), "datetime'2024-05-01T00:00:00'")
        self.assertEqual(odata_datetime_literal(ms, "datetimeoffset"), "datetimeoffset'2024-05-01T00:00:00Z'")
        self.assertEqual(odata_datetime_literal(ms, "datetimeoffset", 4), "2024-05-01T00:00:00Z")
        self.assertEqual(odata_datetime_literal(ms, "date", 4), "2024-05-01")

    def test_modified_filter_composition(self):
        ms = 1714521600000
        self.assertIsNone(build_modified_filter(SO))
        self.assertEqual(build_modified_filter(SO, since_ms=ms), "LastChangeDateTime gt datetimeoffset'2024-05-01T00:00:00Z'")
        self.assertEqual(build_modified_filter(BP, start_ms=ms), "LastChangeDate ge datetime'2024-05-01T00:00:00'")
        # sync point newer than the user's lower bound -> exclusive gt on the sync point
        self.assertEqual(build_modified_filter(SO, since_ms=ms + 1000, start_ms=ms),
                         "LastChangeDateTime gt datetimeoffset'2024-05-01T00:00:01Z'")
        # user's lower bound newer than the sync point -> inclusive ge
        self.assertEqual(build_modified_filter(SO, since_ms=ms, start_ms=ms + 1000),
                         "LastChangeDateTime ge datetimeoffset'2024-05-01T00:00:01Z'")
        self.assertEqual(build_modified_filter(SO, end_ms=ms), "LastChangeDateTime le datetimeoffset'2024-05-01T00:00:00Z'")
        no_change = ENTITY_SPECS["sales_order"].__class__(**{**SO.__dict__, "change_field": None})
        self.assertIsNone(build_modified_filter(no_change, since_ms=ms))

    def test_scope_filters(self):
        self.assertEqual(build_scope_filter(SO, sales_orgs=["1010", " 1710", "1010"]),
                         "(SalesOrganization eq '1010' or SalesOrganization eq '1710')")
        self.assertEqual(build_scope_filter(SO, company_codes=["1010"]), None)  # SO has no header company code
        self.assertEqual(build_scope_filter(PO, company_codes=["1010"], sales_orgs=["1010"]), "CompanyCode eq '1010'")
        self.assertIsNone(build_scope_filter(PO))
        self.assertEqual(combine_filters(None, "a eq 1", "", "b eq 2"), "a eq 1 and b eq 2")
        self.assertIsNone(combine_filters(None, ""))


class TestRendering(unittest.TestCase):
    def test_titles(self):
        self.assertEqual(record_title(SO, _so_row()), "Sales Order 0000000123 · PO-ACME-77")
        self.assertEqual(record_title(SO, _so_row(PurchaseOrderByCustomer=None)), "Sales Order 0000000123")
        self.assertEqual(record_title(BP, {"BusinessPartner": "1", "BusinessPartnerFullName": "ACME GmbH"}), "ACME GmbH")
        self.assertEqual(record_title(BP, {"BusinessPartner": "1", "FirstName": "Ada", "LastName": "Lovelace"}), "Ada Lovelace")
        self.assertEqual(record_title(BP, {"BusinessPartner": "1"}), "Business Partner 1")
        prod = {"Product": "TG11", "to_Description": {"results": [
            {"Language": "DE", "ProductDescription": "Handelsware"}, {"Language": "EN", "ProductDescription": "Trading good"}]}}
        self.assertEqual(record_title(PROD, prod), "Trading good (TG11)")
        self.assertEqual(record_title(PROD, {"Product": "TG11"}), "Product TG11")
        self.assertEqual(record_title(INV, {"SupplierInvoice": "5105", "FiscalYear": "2024"}), "Supplier Invoice 5105/2024")

    def test_scope_codes_from_header_items_and_nested_bp(self):
        codes = scope_codes(SO, _so_row())
        self.assertEqual(codes, {SCOPE_SALES_ORG: {"1010"}, SCOPE_PLANT: {"1010", "1710"}})
        bp = {
            "BusinessPartner": "1",
            "to_Customer": {"to_CustomerCompany": {"results": [{"CompanyCode": "1010"}]},
                            "to_CustomerSalesArea": {"results": [{"SalesOrganization": "1710"}]}},
            "to_Supplier": {"to_SupplierCompany": {"results": [{"CompanyCode": "2000"}]}},
        }
        self.assertEqual(scope_codes(BP, bp), {SCOPE_COMPANY_CODE: {"1010", "2000"}, SCOPE_SALES_ORG: {"1710"}})
        self.assertEqual(scope_codes(BP, {"BusinessPartner": "1", "to_Customer": {"__deferred": {}}}), {})
        prod = {"Product": "P", "to_Plant": {"results": [{"Plant": "1010"}]}, "to_SalesDelivery": {"results": [{"ProductSalesOrg": "1010"}]}}
        self.assertEqual(scope_codes(PROD, prod), {SCOPE_PLANT: {"1010"}, SCOPE_SALES_ORG: {"1010"}})

    def test_render_sales_order_markdown(self):
        markdown, metadata = render_record_markdown(SO, _so_row(), BASE, FIORI, "100")
        self.assertTrue(markdown.startswith("# Sales Order 0000000123 · PO-ACME-77\n"))
        self.assertIn("**Type:** SAP Sales Order · **Status:** A · **Created by:** CB9980000042", markdown)
        self.assertIn("- **Net amount:** 1500.00", markdown)
        self.assertIn("- **Order date:** 2024-05-01", markdown)
        self.assertIn("## Items (2)", markdown)
        self.assertIn("| 10 | TG11 | Trading \\| Good | 2 | PC | 1000.00 | 1010 |", markdown)
        self.assertIn("- SAP service: API_SALES_ORDER_SRV / A_SalesOrder", markdown)
        self.assertIn("- SalesOrder: 0000000123", markdown)
        self.assertIn("- Plant: 1010, 1710", markdown)
        self.assertIn("- Sales organization: 1010", markdown)
        self.assertIn("- Created: 2024-05-01 · Changed: 2024-05-02", markdown)
        self.assertIn(f"- URL: {FIORI}/ui#SalesOrder-displayFactSheet?SalesOrder=0000000123", markdown)
        self.assertTrue(markdown.endswith("\n"))
        self.assertEqual(metadata["keys"], {"SalesOrder": "0000000123"})
        self.assertEqual(metadata["scopes"], {"plant": ["1010", "1710"], "salesorg": ["1010"]})
        self.assertEqual(metadata["odata_url"], entity_url(BASE, SO, ["0000000123"], "100"))

    def test_render_business_partner_with_addresses_and_roles(self):
        row = {
            "BusinessPartner": "10100001", "BusinessPartnerFullName": "ACME GmbH", "BusinessPartnerCategory": "2",
            "BusinessPartnerIsBlocked": False, "CreatedByUser": "JSMITH",
            "to_BusinessPartnerAddress": {"results": [{"StreetName": "Main St", "HouseNumber": "1", "PostalCode": "10115", "CityName": "Berlin", "Country": "DE"}]},
            "to_BusinessPartnerRole": {"results": [{"BusinessPartnerRole": "FLCU01"}, {"BusinessPartnerRole": "FLCU00"}]},
        }
        markdown, metadata = render_record_markdown(BP, row, BASE)
        self.assertIn("# ACME GmbH", markdown)
        self.assertIn("- **Blocked:** No", markdown)
        self.assertIn("## Addresses\n- Main St, 1, 10115, Berlin, DE", markdown)
        self.assertIn("**Roles:** FLCU00, FLCU01", markdown)
        self.assertEqual(metadata["url"], f"{BASE}/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')")
        self.assertEqual(build_metadata(BP, row, BASE)["created_by"], "JSMITH")


class TestAttachments(unittest.TestCase):
    def test_list_url_and_keys(self):
        url = attachment_list_url(BASE, INV, ["5105600001", "2024"])
        self.assertEqual(
            url,
            f"{BASE}/sap/opu/odata/sap/API_CMS_ATTACHMENT_SRV/GetAllOriginals"
            "?BusinessObjectTypeName='BUS2081'&LinkedSAPObjectKey='51056000012024'&%24format=json",
        )
        entry = {"LogicalDocument": "LD1", "DocumentInfoRecordDocNumber": "N", "DocumentInfoRecordDocType": "SO", "FileName": "a.pdf"}
        self.assertEqual(attachment_key(entry), "LD1")
        self.assertEqual(attachment_key({"ArchiveDocumentID": "ARC9"}), "ARC9")
        self.assertEqual(attachment_key({"DocumentInfoRecordDocNumber": "N1", "DocumentInfoRecordDocVersion": "00", "DocumentInfoRecordDocPart": "000"}), "N1-00-000")
        self.assertIsNone(attachment_key({}))
        content = attachment_content_url(BASE, entry)
        self.assertTrue(content.startswith(f"{BASE}/sap/opu/odata/sap/API_CMS_ATTACHMENT_SRV/AttachmentContentSet(DocumentInfoRecordDocType='SO',DocumentInfoRecordDocNumber='N',"))
        self.assertTrue(content.endswith(",LogicalDocument='LD1',ArchiveDocumentID='',LinkedSAPObjectKey='',BusinessObjectTypeName='')/$value"))


class TestAuthorizationMapping(unittest.TestCase):
    def test_parse_accepts_json_text_and_dict(self):
        import json

        for raw in (MAPPING, json.dumps(MAPPING)):
            mapping = parse_authorization_mapping(raw)
            self.assertEqual(mapping.admins, ["erp-admin@edrak.com"])
            self.assertEqual(mapping.admin_group_refs, ["group:SAP Administrators"])
            self.assertEqual(mapping.scope_principals["sap:companycode:1010"], ["finance@edrak.com", "group:SAP Finance 1010"])
            self.assertEqual(mapping.scope_principals["sap:entity:product"], ["group:Everyone"])
            self.assertEqual(mapping.named_groups["SAP Finance 1010"], ["cfo@edrak.com", "finance@edrak.com"])
            self.assertEqual(mapping.user_emails, {"CB9980000042": "jane.doe@edrak.com"})
            self.assertTrue(any("bogus:1" in w for w in mapping.warnings))
        self.assertEqual(parse_authorization_mapping("").group_ids(), [ADMINS_GROUP_ID])
        self.assertEqual(parse_authorization_mapping(None).admins, [])

    def test_parse_rejects_invalid_json(self):
        with self.assertRaises(ValueError):
            parse_authorization_mapping("{not json")
        with self.assertRaises(ValueError):
            parse_authorization_mapping("[1, 2]")

    def test_scope_keys_and_group_ids(self):
        self.assertEqual(parse_scope_key("CompanyCode:1010"), ("companycode", "1010"))
        self.assertEqual(parse_scope_key("entity:Sales_Order"), ("entity", "sales_order"))
        self.assertIsNone(parse_scope_key("entity:unknown"))
        self.assertIsNone(parse_scope_key("companycode:"))
        self.assertIsNone(parse_scope_key("random"))
        self.assertEqual(scope_group_external_id("plant", " 1010 "), "sap:plant:1010")
        self.assertEqual(named_group_external_id("SAP Finance 1010"), "sap:group:sap-finance-1010")
        self.assertEqual(group_display_name("sap:companycode:1010"), "SAP · Company code 1010")
        self.assertEqual(group_display_name("sap:entity:sales_order"), "SAP · All Sales Orders")
        self.assertEqual(group_display_name(ADMINS_GROUP_ID), "SAP · Administrators")

    def test_expand_without_resolver_uses_inline_groups_only(self):
        mapping = parse_authorization_mapping(MAPPING)
        expanded = mapping.expand(None)
        self.assertEqual(expanded.members[ADMINS_GROUP_ID], ["erp-admin@edrak.com"])
        self.assertEqual(expanded.members["sap:companycode:1010"], ["finance@edrak.com", "cfo@edrak.com"])
        self.assertEqual(expanded.members["sap:salesorg:1010"], [])
        self.assertEqual(expanded.members["sap:entity:product"], ["all@edrak.com"])
        self.assertEqual(sorted(expanded.unresolved_groups),
                         ["0f3f6e1c-3f0e-4d3e-9a8b-3a1a4e0c7b21", "SAP Administrators", "Unknown Plant Group"])

    def test_expand_with_fake_entra_resolver_unions_directory_and_inline(self):
        mapping = parse_authorization_mapping(MAPPING)
        calls = []

        def resolver(name):
            calls.append(name)
            return ENTRA.get(name)

        expanded = mapping.expand(resolver)
        self.assertEqual(sorted(expanded.members[ADMINS_GROUP_ID]), ["admin1@edrak.com", "admin2@edrak.com", "erp-admin@edrak.com"])
        self.assertEqual(expanded.members["sap:companycode:1010"], ["finance@edrak.com", "fin-entra@edrak.com", "cfo@edrak.com"])
        self.assertEqual(expanded.members["sap:salesorg:1010"], ["sales-de@edrak.com"])
        self.assertEqual(expanded.members["sap:entity:product"], ["all@edrak.com"])  # empty Entra group + inline
        self.assertEqual(expanded.members["sap:plant:1010"], [])
        self.assertEqual(expanded.unresolved_groups, ["Unknown Plant Group"])
        self.assertIn("sap:group:sap-finance-1010", expanded.members)
        self.assertIn("SAP Administrators", calls)
        self.assertEqual(sorted(expanded.all_emails())[:2], ["admin1@edrak.com", "admin2@edrak.com"])

    def test_resolve_sap_user(self):
        mapping = parse_authorization_mapping(MAPPING)
        self.assertEqual(mapping.resolve_sap_user("cb9980000042", None), "jane.doe@edrak.com")
        self.assertEqual(mapping.resolve_sap_user("JSMITH", "edrak.com"), "jsmith@edrak.com")
        self.assertEqual(mapping.resolve_sap_user("JSMITH", "@Edrak.com"), "jsmith@edrak.com")
        self.assertIsNone(mapping.resolve_sap_user("JSMITH", None))
        self.assertEqual(mapping.resolve_sap_user("Ada@edrak.com", None), "ada@edrak.com")
        self.assertIsNone(mapping.resolve_sap_user("", "edrak.com"))
        self.assertEqual(mapping.codes_for("companycode"), ["1010"])


class TestDeriveGrants(unittest.TestCase):
    def _ctx(self, **overrides) -> AuthorizationContext:
        ctx = AuthorizationContext(mapping=parse_authorization_mapping(MAPPING), user_email_domain="edrak.com")
        for key, value in overrides.items():
            setattr(ctx, key, value)
        return ctx

    def test_sales_order_grants(self):
        grants = derive_grants(SO, _so_row(), self._ctx())
        by_key = {(g.entity_type, g.external_id or g.email): g.role for g in grants}
        self.assertEqual(by_key[(GrantEntity.USER, "jane.doe@edrak.com")], GrantRole.OWNER)
        self.assertEqual(by_key[(GrantEntity.GROUP, "sap:salesorg:1010")], GrantRole.READER)
        self.assertEqual(by_key[(GrantEntity.GROUP, "sap:plant:1010")], GrantRole.READER)
        self.assertEqual(by_key[(GrantEntity.GROUP, "sap:plant:1710")], GrantRole.READER)
        self.assertEqual(by_key[(GrantEntity.GROUP, "sap:entity:sales_order")], GrantRole.READER)
        self.assertEqual(by_key[(GrantEntity.GROUP, ADMINS_GROUP_ID)], GrantRole.READER)
        self.assertEqual(len(grants), 6)
        self.assertEqual(group_ids_in_grants(grants),
                         ["sap:plant:1010", "sap:plant:1710", "sap:salesorg:1010", ADMINS_GROUP_ID, "sap:entity:sales_order"])

    def test_unresolvable_creator_and_no_scopes(self):
        row = {"PurchaseOrder": "4500000001", "CreatedByUser": "CB123"}
        grants = derive_grants(PO, row, self._ctx(user_email_domain=None))
        self.assertEqual([g.external_id for g in grants], [ADMINS_GROUP_ID, "sap:entity:purchase_order"])
        self.assertTrue(all(g.entity_type == GrantEntity.GROUP for g in grants))

    def test_scope_groups_can_be_disabled(self):
        grants = derive_grants(SO, _so_row(), self._ctx(grant_scope_groups=False))
        self.assertEqual({g.external_id for g in grants if g.entity_type == GrantEntity.GROUP},
                         {ADMINS_GROUP_ID, "sap:entity:sales_order"})

    def test_merge_keeps_strongest_role(self):
        # creator is also listed as a plain reader through a scope group -> OWNER edge stays,
        # and duplicated scope codes collapse to one grant each
        row = _so_row(to_Item={"results": [{"ProductionPlant": "1010"}, {"ProductionPlant": "1010"}]})
        grants = derive_grants(SO, row, self._ctx())
        plant_grants = [g for g in grants if g.external_id == "sap:plant:1010"]
        self.assertEqual(len(plant_grants), 1)
        owner = [g for g in grants if g.entity_type == GrantEntity.USER]
        self.assertEqual([g.role for g in owner], [GrantRole.OWNER])

    def test_entity_grants_for_record_groups(self):
        grants = self._ctx().entity_grants(PROD)
        self.assertEqual([g.external_id for g in grants], [ADMINS_GROUP_ID, "sap:entity:product"])
        self.assertTrue(all(g.role == GrantRole.READER for g in grants))

    def test_permission_grant_reason_is_not_part_of_identity(self):
        a = PermissionGrant(GrantEntity.USER, GrantRole.OWNER, email="x@y.z", reason="one")
        b = PermissionGrant(GrantEntity.USER, GrantRole.OWNER, email="x@y.z", reason="two")
        self.assertEqual(a, b)


AR_FINANCE = "الإدارة المالية"
AR_SALES_RIYADH = "مبيعات الرياض"
AR_SALES_JEDDAH = "مبيعات جدة"


def _product_row(descriptions, **overrides) -> dict:
    """SAP OData v2 shaped product with an inline ``to_Description`` feed."""
    row = {
        "Product": "MAT-100",
        "ProductType": "FERT",
        "ProductGroup": "01",
        "to_Description": {"results": [
            {"Product": "MAT-100", "Language": lang, "ProductDescription": text} for lang, text in descriptions
        ]},
    }
    row.update(overrides)
    return row


class TestSlugifyGroupName(unittest.TestCase):
    def test_ascii_names_are_unchanged_from_legacy_behaviour(self):
        # No hash suffix: existing groups keep their external ids.
        self.assertEqual(slugify_group_name("SAP Finance 1010"), "sap-finance-1010")
        self.assertEqual(slugify_group_name("  Sales__Team (EMEA)!  "), "sales-team-emea")
        self.assertEqual(slugify_group_name("0f3f6e1c-3f0e-4d3e-9a8b-3a1a4e0c7b21"), "0f3f6e1c-3f0e-4d3e-9a8b-3a1a4e0c7b21")
        self.assertEqual(slugify_group_name(""), "group")
        self.assertEqual(slugify_group_name("!!!"), "group")
        self.assertEqual(named_group_external_id("SAP Finance 1010"), "sap:group:sap-finance-1010")

    def test_arabic_names_keep_their_letters_and_do_not_collide(self):
        finance = slugify_group_name(AR_FINANCE)
        riyadh = slugify_group_name(AR_SALES_RIYADH)
        jeddah = slugify_group_name(AR_SALES_JEDDAH)
        for slug in (finance, riyadh, jeddah):
            self.assertNotEqual(slug, "group")
            self.assertNotIn(" ", slug)
        self.assertTrue(finance.startswith("الإدارة-المالية-"), finance)
        self.assertTrue(riyadh.startswith("مبيعات-الرياض-"), riyadh)
        self.assertRegex(finance, r"-[0-9a-f]{8}$")
        self.assertEqual(len({finance, riyadh, jeddah}), 3)
        # Different names sharing every kept character still differ through the hash.
        self.assertNotEqual(slugify_group_name("مبيعات الرياض"), slugify_group_name("مبيعات-الرياض!"))

    def test_slugs_are_stable_across_runs_and_unicode_forms(self):
        self.assertEqual(slugify_group_name(AR_FINANCE), slugify_group_name(AR_FINANCE))
        # Whitespace around the name and NFKC-equivalent code points map to the same group.
        self.assertEqual(slugify_group_name(f"  {AR_FINANCE}  "), slugify_group_name(AR_FINANCE))
        self.assertEqual(slugify_group_name("ﻣﺒﻴﻌﺎﺕ"), slugify_group_name("مبيعات"))  # presentation forms → base letters
        self.assertEqual(named_group_external_id(AR_FINANCE), f"sap:group:{slugify_group_name(AR_FINANCE)}")

    def test_mixed_names_lowercase_latin_and_keep_arabic_and_digits(self):
        slug = slugify_group_name("SAP فريق المالية 1010")
        self.assertTrue(slug.startswith("sap-فريق-المالية-1010-"), slug)
        self.assertRegex(slug, r"^sap-فريق-المالية-1010-[0-9a-f]{8}$")
        digits = slugify_group_name("فرع ٠١٢")  # Arabic-Indic digits are digits, not folded to ASCII
        self.assertTrue(digits.startswith("فرع-٠١٢-"), digits)
        symbols_only = slugify_group_name("★☆")
        self.assertRegex(symbols_only, r"^group-[0-9a-f]{8}$")
        self.assertNotEqual(symbols_only, slugify_group_name("♥"))

    def test_arabic_groups_resolve_in_mapping_and_get_readable_labels(self):
        mapping = parse_authorization_mapping({
            "companycode:1010": [f"group:{AR_FINANCE}"],
            "salesorg:1010": [f"group:{AR_SALES_RIYADH}"],
            "salesorg:1710": [f"group:{AR_SALES_JEDDAH}"],
            "groups": {AR_FINANCE: ["cfo@edrak.com"], AR_SALES_RIYADH: ["riyadh@edrak.com"], AR_SALES_JEDDAH: ["jeddah@edrak.com"]},
        })
        expanded = mapping.expand(None)
        self.assertEqual(expanded.members["sap:companycode:1010"], ["cfo@edrak.com"])
        self.assertEqual(expanded.members["sap:salesorg:1010"], ["riyadh@edrak.com"])
        self.assertEqual(expanded.members["sap:salesorg:1710"], ["jeddah@edrak.com"])
        self.assertEqual(expanded.unresolved_groups, [])
        finance_id = named_group_external_id(AR_FINANCE)
        self.assertIn(finance_id, expanded.members)
        self.assertEqual(len([g for g in mapping.group_ids() if g.startswith("sap:group:")]), 3)
        self.assertEqual(group_display_name(finance_id, mapping), f"SAP · Group {AR_FINANCE}")
        self.assertEqual(mapping.named_group_label(slugify_group_name(AR_FINANCE)), AR_FINANCE)
        self.assertIsNone(mapping.named_group_label("nope"))
        # Referenced-but-undefined groups are still labelled by their original name.
        referenced = parse_authorization_mapping({"plant:1010": [f"group:{AR_SALES_JEDDAH}"]})
        self.assertEqual(group_display_name(named_group_external_id(AR_SALES_JEDDAH), referenced), f"SAP · Group {AR_SALES_JEDDAH}")
        self.assertEqual(group_display_name("sap:group:sap-finance-1010"), "SAP · Group sap-finance-1010")


class TestLanguagePreference(unittest.TestCase):
    AR = "مادة تجارية"
    EN = "Trading good"
    DE = "Handelsware"

    def test_language_codes_normalise_without_touching_text(self):
        self.assertEqual(LANGUAGE_PREFERENCE, ("AR", "EN"))
        for raw in ("AR", "ar", "ar-SA", "ar_SA", "A"):
            self.assertEqual(normalize_language_code(raw), "AR", raw)
        for raw in ("EN", "en-US", "E"):
            self.assertEqual(normalize_language_code(raw), "EN", raw)
        self.assertEqual(normalize_language_code("D"), "DE")
        self.assertEqual(normalize_language_code(None), "")
        self.assertEqual(normalize_language_code("  "), "")

    def test_arabic_wins_then_english_then_any(self):
        row = _product_row([("DE", self.DE), ("EN", self.EN), ("AR", self.AR)])
        self.assertEqual(product_description(row), self.AR)
        self.assertEqual(product_descriptions(row), [("AR", self.AR), ("EN", self.EN), ("DE", self.DE)])
        self.assertEqual(record_title(PROD, row), f"{self.AR} (MAT-100)")
        self.assertEqual(product_description(_product_row([("DE", self.DE), ("EN", self.EN)])), self.EN)
        self.assertEqual(product_description(_product_row([("DE", self.DE)])), self.DE)
        self.assertIsNone(product_description(_product_row([])))
        self.assertIsNone(product_description({"Product": "X", "to_Description": {"__deferred": {"uri": "x"}}}))
        # explicit preference is honoured before the default order
        self.assertEqual(product_description(row, preferred_language="EN"), self.EN)
        self.assertEqual(product_description(row, preferred_language="fr"), self.AR)

    def test_sap_one_letter_language_keys_and_empty_texts(self):
        row = _product_row([("E", self.EN), ("A", self.AR), ("D", "")])
        self.assertEqual(product_description(row), self.AR)
        self.assertEqual(product_descriptions(row), [("AR", self.AR), ("EN", self.EN)])
        self.assertEqual(preferred_text(row["to_Description"], "ProductDescription", preference=("EN",)), self.EN)
        self.assertEqual(localized_texts([{"Language": "AR", "T": " نص "}, {"Language": "AR", "T": "dup"}], "T"), [("AR", "نص")])
        self.assertEqual(localized_texts([{"T": "no lang"}], "T"), [("", "no lang")])

    def test_markdown_keeps_both_languages_and_metadata_lists_them(self):
        row = _product_row([("EN", self.EN), ("AR", self.AR)])
        markdown, metadata = render_record_markdown(PROD, row, BASE)
        self.assertTrue(markdown.startswith(f"# {self.AR} (MAT-100)\n"), markdown.splitlines()[0])
        self.assertIn(f"- **Description (AR):** {self.AR}", markdown)
        self.assertIn(f"- **Description (EN):** {self.EN}", markdown)
        self.assertLess(markdown.index("Description (AR)"), markdown.index("Description (EN)"))
        self.assertLess(markdown.index("Description (EN)"), markdown.index("- **Product type:**"))
        self.assertEqual(metadata["descriptions"], {"AR": self.AR, "EN": self.EN})
        # single language: plain label, no ASCII folding of the Arabic text
        single, meta_single = render_record_markdown(PROD, _product_row([("AR", self.AR)]), BASE)
        self.assertIn(f"- **Description:** {self.AR}", single)
        self.assertNotIn("Description (", single)
        self.assertEqual(meta_single["descriptions"], {"AR": self.AR})
        self.assertEqual(build_metadata(SO, _so_row(), BASE)["descriptions"], {})

    def test_arabic_business_partner_names_are_untouched(self):
        row = {"BusinessPartner": "1", "BusinessPartnerFullName": "شركة أرامكو السعودية", "CreatedByUser": "ABDULLAH"}
        self.assertEqual(record_title(BP, row), "شركة أرامكو السعودية")
        markdown, _ = render_record_markdown(BP, row, BASE)
        self.assertIn("# شركة أرامكو السعودية", markdown)


class TestRetryAfter(unittest.TestCase):
    def test_parse_delay_seconds_and_http_date(self):
        self.assertEqual(parse_retry_after("30"), 30.0)
        self.assertEqual(parse_retry_after(" 2.5 "), 2.5)
        self.assertEqual(parse_retry_after(7), 7.0)
        self.assertEqual(parse_retry_after("-3"), 0.0)
        self.assertIsNone(parse_retry_after(None))
        self.assertIsNone(parse_retry_after(""))
        self.assertIsNone(parse_retry_after("soon"))
        now = 1445412480.0  # Wed, 21 Oct 2015 07:28:00 GMT
        self.assertEqual(parse_retry_after("Wed, 21 Oct 2015 07:28:30 GMT", now_s=now), 30.0)
        self.assertEqual(parse_retry_after("Wed, 21 Oct 2015 07:27:00 GMT", now_s=now), 0.0)

    def test_retry_delay_prefers_server_hint_and_clamps(self):
        self.assertEqual(retry_delay("10", fallback=1.0), 10.0)
        self.assertEqual(retry_delay(None, fallback=4.0), 4.0)
        self.assertEqual(retry_delay("garbage", fallback=4.0), 4.0)
        self.assertEqual(retry_delay("0", fallback=4.0), 0.5)
        self.assertEqual(retry_delay("3600", fallback=1.0), 60.0)
        self.assertEqual(retry_delay("Wed, 21 Oct 2015 07:28:20 GMT", fallback=1.0, now_s=1445412480.0), 20.0)


class TestReconcile(unittest.TestCase):
    def test_interval_parsing_and_due(self):
        self.assertEqual(DEFAULT_RECONCILE_INTERVAL_HOURS, 24.0)
        self.assertEqual(parse_reconcile_interval_hours(None), 24.0)
        self.assertEqual(parse_reconcile_interval_hours(""), 24.0)
        self.assertEqual(parse_reconcile_interval_hours("12"), 12.0)
        self.assertEqual(parse_reconcile_interval_hours(6), 6.0)
        self.assertEqual(parse_reconcile_interval_hours("0"), 0.0)
        self.assertEqual(parse_reconcile_interval_hours("-5"), 0.0)
        self.assertEqual(parse_reconcile_interval_hours("daily"), 24.0)
        self.assertEqual(parse_reconcile_interval_hours(True), 24.0)
        hour = 3_600_000
        self.assertTrue(reconcile_due(None, 10 * hour, 24))
        self.assertFalse(reconcile_due(0, 23 * hour, 24))
        self.assertTrue(reconcile_due(0, 24 * hour, 24))
        self.assertFalse(reconcile_due(None, 10 * hour, 0))
        self.assertTrue(reconcile_due(10 * hour, 10 * hour + hour // 2, 0.5))

    def test_key_page_params_select_keys_only(self):
        params = build_key_page_params(INV, "CompanyCode eq '1010'", skip=5000)
        self.assertEqual(params["$select"], "SupplierInvoice,FiscalYear")
        self.assertEqual(params["$orderby"], "SupplierInvoice asc,FiscalYear asc")
        self.assertEqual(params["$top"], str(RECONCILE_KEY_PAGE_SIZE))
        self.assertEqual(params["$skip"], "5000")
        self.assertEqual(params["$filter"], "CompanyCode eq '1010'")
        self.assertEqual(params["$format"], "json")
        self.assertEqual(params["$inlinecount"], "allpages")
        self.assertNotIn("$expand", params)
        self.assertNotIn("$filter", build_key_page_params(SO, None))

    def test_external_id_entity(self):
        self.assertEqual(external_id_entity("sales_order:123"), "sales_order")
        self.assertEqual(external_id_entity("supplier_invoice:1/2024"), "supplier_invoice")
        self.assertIsNone(external_id_entity(attachment_external_id(SO, ["123"], "LD1")))
        self.assertIsNone(external_id_entity("unknown:1"))
        self.assertIsNone(external_id_entity("garbage"))

    def test_plan_marks_missing_documents_only(self):
        live_payload = {"d": {"results": [{"SalesOrder": "1"}, {"SalesOrder": "3"}, {"SalesOrder": "5"}], "__count": "3"}}
        live_ids = {record_external_id(SO, (r["SalesOrder"],)) for r in parse_page(live_payload).rows}
        known = [
            "sales_order:1", "sales_order:2", "sales_order:3", "sales_order:4",
            attachment_external_id(SO, ["2"], "LD-2"),   # attachment: cascades with its parent, not planned
            "purchase_order:4500000001",                  # other entity: untouched
            "garbage",
        ]
        plan = plan_reconcile(SO, known, live_ids)
        self.assertEqual(plan.entity, "sales_order")
        self.assertEqual(plan.delete_external_ids, ("sales_order:2", "sales_order:4"))
        self.assertEqual((plan.known, plan.live), (4, 3))
        self.assertIsNone(plan.skipped_reason)
        # live ids of other entities do not count
        plan = plan_reconcile(SO, ["sales_order:1"], {"purchase_order:1", "sales_order:1"})
        self.assertEqual(plan.delete_external_ids, ())
        self.assertEqual(plan.live, 1)

    def test_plan_refuses_to_wipe_when_sap_returns_nothing(self):
        plan = plan_reconcile(PO, ["purchase_order:1", "purchase_order:2"], set())
        self.assertEqual(plan.delete_external_ids, ())
        self.assertIsNotNone(plan.skipped_reason)
        self.assertEqual(plan.known, 2)
        # nothing known and nothing live is simply a no-op, not a skip
        empty = plan_reconcile(PO, [], set())
        self.assertEqual(empty.delete_external_ids, ())
        self.assertIsNone(empty.skipped_reason)

    def test_plan_with_composite_keys_and_percent_encoding(self):
        live = {record_external_id(INV, ("5105600001", "2024")), record_external_id(INV, ("A/B", "2024"))}
        known = ["supplier_invoice:5105600001/2024", "supplier_invoice:A%2FB/2024", "supplier_invoice:5105600002/2024"]
        plan = plan_reconcile(INV, known, live)
        self.assertEqual(plan.delete_external_ids, ("supplier_invoice:5105600002/2024",))


if __name__ == "__main__":
    unittest.main(verbosity=1)
