"""Pure helpers for the SAP S/4HANA connector (OData v2/v4).

Everything in this module is deliberately **standard-library only** and free of
I/O, mirroring ``sources/microsoft/dynamics365/mapping.py``: the entity registry,
OData query/paging helpers, markdown rendering, web URLs and the permission
derivation are unit-testable without the connector's runtime dependencies
(``httpx``, pydantic models).  ``connector.py`` converts the plain dataclasses
produced here into ``Record`` / ``Permission`` objects.

Permission model (how SAP authorization becomes CGraph permission edges)
=======================================================================

SAP decides whether a user may display a business document through PFCG roles
(authorization objects such as ``V_VBAK_VKO`` = sales org / distribution
channel / division, ``F_BKPF_BUK`` = company code, ``M_BEST_WRK`` = plant,
``M_BEST_EKO`` = purchasing org ...).  Those checks happen inside the ABAP
stack and are **not** exposed over the public OData APIs of S/4HANA Cloud, and
there is no cheap user directory either.  The connector therefore approximates
them with *authorization groups* that the CGraph admin configures (JSON in the
``authorization_mapping`` sync field) plus the organisational fields carried
by every document:

+--------------------------------------------+---------------------------------------------+---------+
| SAP source                                 | CGraph principal                            | Role    |
+============================================+=============================================+=========+
| ``CreatedByUser`` (SAP user id) resolved   | USER ``<sapuser>@<userEmailDomain>`` or the | OWNER   |
| through ``userEmailDomain`` / ``users`` map| explicit e-mail in ``users``                |         |
| ``SalesOrganization`` on the document      | GROUP ``sap:salesorg:<code>``               | READER  |
| ``CompanyCode`` on the document            | GROUP ``sap:companycode:<code>``            | READER  |
| ``Plant`` on the document or its items     | GROUP ``sap:plant:<code>``                  | READER  |
| ``PurchasingOrganization`` on the document | GROUP ``sap:purchorg:<code>``               | READER  |
| mapping key ``entity:<entity>``            | GROUP ``sap:entity:<entity>`` (everyone who | READER  |
| (e.g. ``entity:sales_order``)              | may read every row of that entity)          |         |
| mapping key ``admins``                     | GROUP ``sap:admins`` (readers of everything,| READER  |
|                                            | also on the record groups)                  |         |
+--------------------------------------------+---------------------------------------------+---------+

Membership of every ``sap:*`` group comes from the same mapping: each key lists
principals that are either user e-mails (optionally prefixed ``user:``) or
``group:<name-or-id>`` references.  A ``group:`` reference is resolved, in order,
against

1. **Microsoft Entra ID** (recommended) — when the optional ``entraTenantId`` /
   ``entraClientId`` / ``entraClientSecret`` fields are configured the connector
   looks the group up by display name or object id and expands its *transitive*
   members via Microsoft Graph (``connector.py::EntraGroupResolver``; app-only,
   ``GroupMember.Read.All`` + ``User.Read.All``).  Resolutions are cached per sync.
2. the inline ``groups`` section of the mapping (``{"groups": {"SAP Finance 1010": ["a@x"]}}``)
   — literal e-mails, useful without Entra or as an override.

A ``group:`` reference that resolves nowhere yields an **empty** group — the safe
default is to under-share — and is logged.  The same resolver seam is the
interface point for a later *SAP authorization sync* job that could feed the
``sap:*`` groups from PFCG role assignments.

Example mapping::

    {
      "admins": ["group:SAP Administrators", "user:erp-admin@edrak.com"],
      "entity:product": ["group:Everyone"],
      "companycode:1010": ["finance@edrak.com", "group:SAP Finance 1010"],
      "salesorg:1010": ["group:0f3f6e1c-3f0e-4d3e-9a8b-3a1a4e0c7b21"],
      "plant:1010": ["group:SAP Plant Berlin"],
      "groups": {"SAP Finance 1010": ["cfo@edrak.com"], "SAP Sales DE": ["sales@edrak.com"]},
      "users": {"CB9980000042": "jane.doe@edrak.com"}
    }

Known approximations (documented gaps):

* **True PFCG evaluation is not performed.**  Exact per-user authorization needs
  ``BAPI_USER_GET_DETAIL`` / table ``AGR_USERS`` (RFC, on-prem only) or the
  Identity Authentication Service group assignments (Cloud) — both out of scope
  for OData.  ``TODO(sap-auth-sync)``: feed the ``sap:*`` groups from such a job.
* Organisational scoping is document-level only; structural authorizations
  (e.g. HR), field-level restrictions and blocked/archived flags are ignored.
* A document with no organisational field and no entity/admin grant is visible
  to its creator only (or to nobody when the creator cannot be resolved).
* ``CreatedByUser`` in S/4HANA Cloud is often a technical id (``CB…``); use the
  ``users`` section of the mapping to pin those to e-mails.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence
from urllib.parse import quote, urlencode

# ---------------------------------------------------------------------------
# SAP / OData constants
# ---------------------------------------------------------------------------

ODATA_V2_ROOT = "/sap/opu/odata/sap"
ODATA_V4_ROOT = "/sap/opu/odata4/sap"
SAP_PAGE_SIZE = 500  # $top per page; S/4HANA Cloud caps most services well above this

AUTH_MODE_BASIC = "BASIC_AUTH"
AUTH_MODE_OAUTH = "OAUTH_ADMIN_CONSENT"  # OAuth2 client credentials (SAP BTP / Cloud)
SUPPORTED_AUTH_MODES: tuple[str, ...] = (AUTH_MODE_BASIC, AUTH_MODE_OAUTH)

ENTITIES_FILTER_KEY = "entities"
COMPANY_CODES_FILTER_KEY = "company_codes"
SALES_ORGS_FILTER_KEY = "sales_orgs"

GROUP_PREFIX = "sap:"
ADMINS_GROUP_ID = "sap:admins"
ADMINS_MAPPING_KEY = "admins"
GROUPS_MAPPING_KEY = "groups"
USERS_MAPPING_KEY = "users"
GROUP_REF_PREFIX = "group:"
USER_REF_PREFIX = "user:"
ATTACHMENT_ID_PREFIX = "attachment"

# Scope kinds: mapping key prefix == group id infix == metadata key
SCOPE_SALES_ORG = "salesorg"
SCOPE_COMPANY_CODE = "companycode"
SCOPE_PLANT = "plant"
SCOPE_PURCH_ORG = "purchorg"
SCOPE_ENTITY = "entity"
SCOPE_KINDS: tuple[str, ...] = (SCOPE_SALES_ORG, SCOPE_COMPANY_CODE, SCOPE_PLANT, SCOPE_PURCH_ORG)

# API_CMS_ATTACHMENT_SRV business object types per entity (GetAllOriginals input)
ATTACHMENT_SERVICE = "API_CMS_ATTACHMENT_SRV"
ATTACHMENT_ENTITY_SET = "AttachmentContentSet"

_SAP_DATE_RE = re.compile(r"^/Date\((-?\d+)([+-]\d{1,4})?\)/$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ---------------------------------------------------------------------------
# Entity registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntitySpec:
    """Static description of one S/4HANA OData entity set the connector indexes."""

    name: str                       # "sales_order" (filter value, external id prefix)
    service: str                    # "API_SALES_ORDER_SRV"
    entity_set: str                 # "A_SalesOrder"
    display_name: str               # "Sales Orders" (record group label)
    singular: str                   # "Sales Order"
    key_fields: tuple[str, ...]     # ("SalesOrder",) or ("SupplierInvoice", "FiscalYear")
    title_fields: tuple[str, ...]   # first non-empty wins; falls back to "<singular> <key>"
    record_type: str                # ``RecordType`` value
    record_group_type: str          # ``RecordGroupType`` value
    odata_version: int = 2
    change_field: Optional[str] = None            # incremental-sync timestamp property
    change_field_kind: str = "datetimeoffset"     # "datetime" | "datetimeoffset" | "date" (v4)
    created_by_field: Optional[str] = "CreatedByUser"
    created_at_field: Optional[str] = "CreationDate"
    select_fields: tuple[str, ...] = ()
    expand: tuple[str, ...] = ()
    # (property, label) pairs rendered in the "Details" section, in order.
    summary_fields: tuple[tuple[str, str], ...] = ()
    # navigation property holding line items + (property, label) columns to render
    items_nav: Optional[str] = None
    item_fields: tuple[tuple[str, str], ...] = ()
    # header properties feeding the scope groups (kind -> property)
    scope_fields: tuple[tuple[str, str], ...] = ()
    # item-level scope properties (kind -> property on each item)
    item_scope_fields: tuple[tuple[str, str], ...] = ()
    # header property used by the company-code / sales-org sync filters
    company_code_field: Optional[str] = None
    sales_org_field: Optional[str] = None
    fiori_intent: Optional[str] = None            # "SalesOrder-displayFactSheet"
    attachment_object_type: Optional[str] = None  # BUS2032 ...
    description: str = ""


ENTITY_SPECS: dict[str, EntitySpec] = {
    "business_partner": EntitySpec(
        name="business_partner",
        service="API_BUSINESS_PARTNER",
        entity_set="A_BusinessPartner",
        display_name="Business Partners",
        singular="Business Partner",
        key_fields=("BusinessPartner",),
        title_fields=("BusinessPartnerFullName", "BusinessPartnerName", "OrganizationBPName1"),
        record_type="OTHERS",
        record_group_type="ERP_ENTITY",
        change_field="LastChangeDate",
        change_field_kind="datetime",
        select_fields=(
            "BusinessPartner", "BusinessPartnerCategory", "BusinessPartnerFullName",
            "BusinessPartnerName", "OrganizationBPName1", "FirstName", "LastName",
            "BusinessPartnerGrouping", "SearchTerm1", "Industry", "Customer", "Supplier",
            "BusinessPartnerIsBlocked", "IsMarkedForArchiving", "CreatedByUser",
            "CreationDate", "LastChangedByUser", "LastChangeDate",
        ),
        expand=(
            "to_BusinessPartnerAddress", "to_BusinessPartnerRole",
            "to_Customer/to_CustomerCompany", "to_Customer/to_CustomerSalesArea",
            "to_Supplier/to_SupplierCompany",
        ),
        summary_fields=(
            ("BusinessPartnerCategory", "Category"),
            ("BusinessPartnerGrouping", "Grouping"),
            ("Customer", "Customer number"),
            ("Supplier", "Supplier number"),
            ("Industry", "Industry"),
            ("SearchTerm1", "Search term"),
            ("BusinessPartnerIsBlocked", "Blocked"),
            ("IsMarkedForArchiving", "Marked for archiving"),
        ),
        fiori_intent="BusinessPartner-displayFactSheet",
        attachment_object_type="BUS1006",
        description="Customers, suppliers and contacts (API_BUSINESS_PARTNER)",
    ),
    "sales_order": EntitySpec(
        name="sales_order",
        service="API_SALES_ORDER_SRV",
        entity_set="A_SalesOrder",
        display_name="Sales Orders",
        singular="Sales Order",
        key_fields=("SalesOrder",),
        title_fields=("PurchaseOrderByCustomer",),
        # DealRecord models a pipeline opportunity (probability, won/lost); an SAP
        # sales order is an already-won, legally binding order, so it is a plain
        # OTHERS record with the commercial fields rendered in the markdown.
        record_type="OTHERS",
        record_group_type="ERP_ENTITY",
        change_field="LastChangeDateTime",
        select_fields=(
            "SalesOrder", "SalesOrderType", "SalesOrganization", "DistributionChannel",
            "OrganizationDivision", "SalesGroup", "SalesOffice", "SoldToParty",
            "PurchaseOrderByCustomer", "CustomerPurchaseOrderDate", "SalesOrderDate",
            "TotalNetAmount", "TransactionCurrency", "RequestedDeliveryDate",
            "OverallSDProcessStatus", "OverallDeliveryStatus", "TotalCreditCheckStatus",
            "CreatedByUser", "CreationDate", "LastChangeDate", "LastChangeDateTime",
        ),
        expand=("to_Item",),
        summary_fields=(
            ("SalesOrderType", "Order type"),
            ("OverallSDProcessStatus", "Processing status"),
            ("OverallDeliveryStatus", "Delivery status"),
            ("SoldToParty", "Sold-to party"),
            ("PurchaseOrderByCustomer", "Customer reference"),
            ("SalesOrderDate", "Order date"),
            ("RequestedDeliveryDate", "Requested delivery"),
            ("TotalNetAmount", "Net amount"),
            ("TransactionCurrency", "Currency"),
            ("SalesOrganization", "Sales organization"),
            ("DistributionChannel", "Distribution channel"),
            ("OrganizationDivision", "Division"),
        ),
        items_nav="to_Item",
        item_fields=(
            ("SalesOrderItem", "Item"),
            ("Material", "Material"),
            ("SalesOrderItemText", "Description"),
            ("RequestedQuantity", "Quantity"),
            ("RequestedQuantityUnit", "Unit"),
            ("NetAmount", "Net amount"),
            ("ProductionPlant", "Plant"),
        ),
        scope_fields=((SCOPE_SALES_ORG, "SalesOrganization"),),
        item_scope_fields=((SCOPE_PLANT, "ProductionPlant"),),
        sales_org_field="SalesOrganization",
        fiori_intent="SalesOrder-displayFactSheet",
        attachment_object_type="BUS2032",
        description="Sales order headers and items (API_SALES_ORDER_SRV)",
    ),
    "purchase_order": EntitySpec(
        name="purchase_order",
        service="API_PURCHASEORDER_PROCESS_SRV",
        entity_set="A_PurchaseOrder",
        display_name="Purchase Orders",
        singular="Purchase Order",
        key_fields=("PurchaseOrder",),
        title_fields=(),
        record_type="OTHERS",
        record_group_type="ERP_ENTITY",
        change_field="LastChangeDateTime",
        select_fields=(
            "PurchaseOrder", "PurchaseOrderType", "CompanyCode", "PurchasingOrganization",
            "PurchasingGroup", "Supplier", "DocumentCurrency", "PurchaseOrderDate",
            "PurchasingProcessingStatus", "PurchasingDocumentDeletionCode",
            "CreatedByUser", "CreationDate", "LastChangeDateTime",
        ),
        expand=("to_PurchaseOrderItem",),
        summary_fields=(
            ("PurchaseOrderType", "Order type"),
            ("PurchasingProcessingStatus", "Processing status"),
            ("Supplier", "Supplier"),
            ("PurchaseOrderDate", "Order date"),
            ("DocumentCurrency", "Currency"),
            ("CompanyCode", "Company code"),
            ("PurchasingOrganization", "Purchasing organization"),
            ("PurchasingGroup", "Purchasing group"),
            ("PurchasingDocumentDeletionCode", "Deletion indicator"),
        ),
        items_nav="to_PurchaseOrderItem",
        item_fields=(
            ("PurchaseOrderItem", "Item"),
            ("Material", "Material"),
            ("PurchaseOrderItemText", "Description"),
            ("OrderQuantity", "Quantity"),
            ("PurchaseOrderQuantityUnit", "Unit"),
            ("NetPriceAmount", "Net price"),
            ("Plant", "Plant"),
        ),
        scope_fields=((SCOPE_COMPANY_CODE, "CompanyCode"), (SCOPE_PURCH_ORG, "PurchasingOrganization")),
        item_scope_fields=((SCOPE_PLANT, "Plant"),),
        company_code_field="CompanyCode",
        fiori_intent="PurchaseOrder-displayFactSheet",
        attachment_object_type="BUS2012",
        description="Purchase order headers and items (API_PURCHASEORDER_PROCESS_SRV)",
    ),
    "product": EntitySpec(
        name="product",
        service="API_PRODUCT_SRV",
        entity_set="A_Product",
        display_name="Products",
        singular="Product",
        key_fields=("Product",),
        title_fields=(),  # title comes from to_Description (see record_title)
        record_type="PRODUCT",
        record_group_type="PRODUCT",
        change_field="LastChangeDateTime",
        select_fields=(
            "Product", "ProductType", "ProductGroup", "BaseUnit", "Division",
            "IndustrySector", "ProductHierarchy", "ProductOldID", "GrossWeight",
            "NetWeight", "WeightUnit", "IsMarkedForDeletion", "CreatedByUser",
            "CreationDate", "LastChangeDateTime",
        ),
        expand=("to_Description", "to_Plant", "to_SalesDelivery"),
        summary_fields=(
            ("ProductType", "Product type"),
            ("ProductGroup", "Product group"),
            ("ProductHierarchy", "Product hierarchy"),
            ("Division", "Division"),
            ("IndustrySector", "Industry sector"),
            ("BaseUnit", "Base unit"),
            ("GrossWeight", "Gross weight"),
            ("NetWeight", "Net weight"),
            ("WeightUnit", "Weight unit"),
            ("ProductOldID", "Old material number"),
            ("IsMarkedForDeletion", "Marked for deletion"),
        ),
        fiori_intent="Product-displayFactSheet",
        attachment_object_type="BUS1001006",
        description="Product master incl. descriptions, plants and sales areas (API_PRODUCT_SRV)",
    ),
    "supplier_invoice": EntitySpec(
        name="supplier_invoice",
        service="API_SUPPLIERINVOICE_PROCESS_SRV",
        entity_set="A_SupplierInvoice",
        display_name="Supplier Invoices",
        singular="Supplier Invoice",
        key_fields=("SupplierInvoice", "FiscalYear"),
        title_fields=("SupplierInvoiceIDByInvcgParty",),
        record_type="OTHERS",
        record_group_type="ERP_ENTITY",
        change_field="LastChangeDateTime",
        select_fields=(
            "SupplierInvoice", "FiscalYear", "CompanyCode", "DocumentDate", "PostingDate",
            "InvoicingParty", "DocumentCurrency", "InvoiceGrossAmount",
            "SupplierInvoiceIDByInvcgParty", "SupplierInvoiceStatus", "PaymentTerms",
            "DueCalculationBaseDate", "CreatedByUser", "CreationDate", "LastChangeDateTime",
        ),
        summary_fields=(
            ("SupplierInvoiceStatus", "Status"),
            ("InvoicingParty", "Invoicing party"),
            ("SupplierInvoiceIDByInvcgParty", "Supplier reference"),
            ("DocumentDate", "Document date"),
            ("PostingDate", "Posting date"),
            ("InvoiceGrossAmount", "Gross amount"),
            ("DocumentCurrency", "Currency"),
            ("PaymentTerms", "Payment terms"),
            ("CompanyCode", "Company code"),
            ("FiscalYear", "Fiscal year"),
        ),
        scope_fields=((SCOPE_COMPANY_CODE, "CompanyCode"),),
        company_code_field="CompanyCode",
        fiori_intent="SupplierInvoice-displayFactSheet",
        attachment_object_type="BUS2081",
        description="Incoming supplier invoices (API_SUPPLIERINVOICE_PROCESS_SRV)",
    ),
}

# Pseudo entity: DMS attachments of the documents above (API_CMS_ATTACHMENT_SRV).
ATTACHMENTS_ENTITY = "attachment"

# Canonical order used for filters and the default sync order.
DEFAULT_ENTITY_ORDER: tuple[str, ...] = (
    "business_partner", "sales_order", "purchase_order", "product", "supplier_invoice",
)
ALL_FILTER_ENTITIES: tuple[str, ...] = DEFAULT_ENTITY_ORDER + (ATTACHMENTS_ENTITY,)
ENTITY_FILTER_LABELS: dict[str, str] = {
    **{name: spec.display_name for name, spec in ENTITY_SPECS.items()},
    ATTACHMENTS_ENTITY: "Attachments (DMS, one extra request per document)",
}


def resolve_selected_entities(values: Optional[Sequence[str]]) -> list[EntitySpec]:
    """Entity specs selected by the ``entities`` sync filter (``attachment`` is
    handled separately, see :func:`attachments_selected`).

    ``None`` / empty means "all supported entities".  Unknown names are ignored
    (never raise on stale filter values), order follows ``DEFAULT_ENTITY_ORDER``.
    """
    if not values:
        return [ENTITY_SPECS[name] for name in DEFAULT_ENTITY_ORDER]
    wanted = {str(v).strip().lower() for v in values if v}
    return [ENTITY_SPECS[name] for name in DEFAULT_ENTITY_ORDER if name in wanted]


def attachments_selected(values: Optional[Sequence[str]]) -> bool:
    """Attachments are opt-in: they cost one ``GetAllOriginals`` call per document."""
    if not values:
        return False
    return ATTACHMENTS_ENTITY in {str(v).strip().lower() for v in values if v}


# ---------------------------------------------------------------------------
# Keys, external ids
# ---------------------------------------------------------------------------


def row_key_values(spec: EntitySpec, row: Mapping[str, Any]) -> Optional[tuple[str, ...]]:
    values = []
    for key_field in spec.key_fields:
        value = row.get(key_field)
        if value in (None, ""):
            return None
        values.append(str(value))
    return tuple(values)


def key_predicate(spec: EntitySpec, key_values: Sequence[str]) -> str:
    """OData key predicate: ``'123'`` or ``SupplierInvoice='1',FiscalYear='2024'``."""
    if len(spec.key_fields) != len(key_values):
        raise ValueError(f"{spec.name} expects {len(spec.key_fields)} key values, got {len(key_values)}")
    if len(key_values) == 1:
        return f"'{_odata_quote(key_values[0])}'"
    return ",".join(f"{name}='{_odata_quote(value)}'" for name, value in zip(spec.key_fields, key_values))


def _odata_quote(value: str) -> str:
    return str(value).replace("'", "''")


def record_external_id(spec: EntitySpec, key_values: Sequence[str]) -> str:
    """``<entity>:<key>[/<key2>]`` — unambiguous across entity sets and easy to split."""
    return f"{spec.name}:{'/'.join(quote(str(v), safe='') for v in key_values)}"


def attachment_external_id(spec: EntitySpec, key_values: Sequence[str], attachment_key: str) -> str:
    return f"{ATTACHMENT_ID_PREFIX}:{record_external_id(spec, key_values)}:{attachment_key}"


def split_external_id(external_id: str) -> tuple[str, tuple[str, ...], Optional[str]]:
    """Inverse of :func:`record_external_id` / :func:`attachment_external_id`.

    Returns ``(entity_name, key_values, attachment_key_or_None)``.
    """
    from urllib.parse import unquote

    kind, _, rest = external_id.partition(":")
    if not rest:
        raise ValueError(f"Not an SAP external id: {external_id!r}")
    if kind == ATTACHMENT_ID_PREFIX:
        entity, _, rest = rest.partition(":")
        keys, _, attachment_key = rest.partition(":")
        if not entity or not keys or not attachment_key:
            raise ValueError(f"Not an SAP attachment id: {external_id!r}")
        return entity, tuple(unquote(k) for k in keys.split("/")), attachment_key
    return kind, tuple(unquote(k) for k in rest.split("/")), None


def record_group_external_id(spec: EntitySpec) -> str:
    return f"sap:{spec.name}"


# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------


def normalize_base_url(url: str) -> str:
    """``https://my300000-api.s4hana.cloud.sap`` — scheme added, trailing slashes removed."""
    value = (url or "").strip().rstrip("/")
    if not value:
        raise ValueError("baseUrl is required")
    if not value.lower().startswith(("https://", "http://")):
        value = f"https://{value}"
    return value


def service_root(base_url: str, spec_or_service: EntitySpec | str, odata_version: int = 2) -> str:
    if isinstance(spec_or_service, EntitySpec):
        service, odata_version = spec_or_service.service, spec_or_service.odata_version
    else:
        service = spec_or_service
    root = ODATA_V4_ROOT if odata_version == 4 else ODATA_V2_ROOT
    return f"{normalize_base_url(base_url)}{root}/{service}"


def metadata_url(base_url: str, spec: EntitySpec) -> str:
    return f"{service_root(base_url, spec)}/$metadata"


def entity_set_url(base_url: str, spec: EntitySpec) -> str:
    return f"{service_root(base_url, spec)}/{spec.entity_set}"


def entity_url(base_url: str, spec: EntitySpec, key_values: Sequence[str], sap_client: Optional[str] = None) -> str:
    url = f"{entity_set_url(base_url, spec)}({key_predicate(spec, key_values)})"
    if sap_client:
        url += f"?sap-client={quote(str(sap_client), safe='')}"
    return url


def fiori_web_url(fiori_base_url: str, spec: EntitySpec, key_values: Sequence[str]) -> Optional[str]:
    """Fiori launchpad object-page deep link, e.g.
    ``https://my300000.s4hana.cloud.sap/ui#SalesOrder-displayFactSheet?SalesOrder=1``."""
    if not fiori_base_url or not spec.fiori_intent:
        return None
    base = normalize_base_url(fiori_base_url)
    if not base.endswith("/ui"):
        base = f"{base}/ui"
    params = urlencode(list(zip(spec.key_fields, key_values)))
    return f"{base}#{spec.fiori_intent}?{params}"


def record_web_url(
    base_url: str,
    spec: EntitySpec,
    key_values: Sequence[str],
    fiori_base_url: Optional[str] = None,
    sap_client: Optional[str] = None,
) -> str:
    return fiori_web_url(fiori_base_url or "", spec, key_values) or entity_url(base_url, spec, key_values, sap_client)


def entity_list_web_url(base_url: str, spec: EntitySpec, fiori_base_url: Optional[str] = None) -> str:
    if fiori_base_url and spec.fiori_intent:
        base = normalize_base_url(fiori_base_url)
        if not base.endswith("/ui"):
            base = f"{base}/ui"
        return f"{base}#{spec.fiori_intent.split('-')[0]}-manage"
    return entity_set_url(base_url, spec)


# ---------------------------------------------------------------------------
# OData payload helpers (v2 ``d``/``results``/``__next`` and v4 ``value``/``@odata.nextLink``)
# ---------------------------------------------------------------------------


def odata_collection(value: Any) -> list[Any]:
    """Unwrap an OData v2 deferred/inline collection (``{"results": [...]}``) or a v4 list."""
    if value is None:
        return []
    if isinstance(value, Mapping):
        inner = value.get("results")
        if isinstance(inner, list):
            return inner
        if "value" in value and isinstance(value["value"], list):
            return value["value"]
        return []
    if isinstance(value, list):
        return value
    return []


@dataclass(frozen=True)
class Page:
    rows: list[dict[str, Any]]
    next_link: Optional[str]   # absolute URL when the service paginates server-side
    total_count: Optional[int]  # $inlinecount / @odata.count when requested


def parse_page(payload: Mapping[str, Any], odata_version: int = 2) -> Page:
    """Normalise a feed response from either protocol version."""
    if odata_version == 4:
        rows = [r for r in odata_collection(payload.get("value")) if isinstance(r, Mapping)]
        count = payload.get("@odata.count")
        return Page(rows=[dict(r) for r in rows], next_link=payload.get("@odata.nextLink"), total_count=_as_int(count))
    body = payload.get("d", payload)
    if isinstance(body, Mapping) and "results" in body:
        rows = [dict(r) for r in odata_collection(body) if isinstance(r, Mapping)]
        return Page(rows=rows, next_link=body.get("__next"), total_count=_as_int(body.get("__count")))
    if isinstance(body, list):  # some Gateway builds return ``{"d": [...]}``
        return Page(rows=[dict(r) for r in body if isinstance(r, Mapping)], next_link=None, total_count=None)
    return Page(rows=[], next_link=None, total_count=None)


def parse_entity(payload: Mapping[str, Any], odata_version: int = 2) -> Optional[dict[str, Any]]:
    """Single-entity response → row dict (``{"d": {...}}`` in v2, bare object in v4)."""
    if odata_version == 4:
        return dict(payload) if payload else None
    body = payload.get("d", payload)
    return dict(body) if isinstance(body, Mapping) and body else None


def build_page_params(
    spec: EntitySpec,
    odata_filter: Optional[str],
    skip: int = 0,
    top: int = SAP_PAGE_SIZE,
) -> dict[str, str]:
    """Query options for one page of ``spec`` (client-side paging with ``$top``/``$skip``)."""
    params: dict[str, str] = {"$top": str(top), "$skip": str(skip)}
    if spec.odata_version == 4:
        params["$count"] = "true"
    else:
        params["$format"] = "json"
        params["$inlinecount"] = "allpages"
    if spec.select_fields:
        params["$select"] = ",".join(spec.select_fields)
    if spec.expand:
        params["$expand"] = ",".join(spec.expand)
    if spec.change_field:
        params["$orderby"] = f"{spec.change_field} asc"
    if odata_filter:
        params["$filter"] = odata_filter
    return params


def next_page_skip(page: Page, skip: int, top: int) -> Optional[int]:
    """Where to continue when the service did not send a next link; ``None`` = done."""
    if page.next_link:
        return None  # follow the link instead
    if len(page.rows) < top:
        return None
    nxt = skip + top
    if page.total_count is not None and nxt >= page.total_count:
        return None
    return nxt


# ---------------------------------------------------------------------------
# Timestamps and filters
# ---------------------------------------------------------------------------


def parse_sap_timestamp(value: Any) -> Optional[int]:
    """SAP OData v2 ``/Date(1714521600000)/`` (optionally ``+0000``), ISO-8601 (v4)
    or ``YYYY-MM-DD`` → epoch ms.  Returns ``None`` for anything else."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    match = _SAP_DATE_RE.match(text)
    if match:
        # ``/Date(ms+0000)/``: the ms value is already UTC; the offset is a display hint.
        return int(match.group(1))
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def format_sap_timestamp(value: Any) -> Optional[str]:
    """Human readable UTC ISO string for the markdown; ``None`` when unparsable."""
    ms = parse_sap_timestamp(value)
    if ms is None:
        return str(value) if value not in (None, "") else None
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    if ms % 86_400_000 == 0:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def odata_datetime_literal(epoch_ms: int, kind: str, odata_version: int = 2) -> str:
    """Literal accepted in ``$filter`` for the given property kind."""
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    iso = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if odata_version == 4:
        return dt.strftime("%Y-%m-%d") if kind == "date" else f"{iso}Z"
    if kind == "datetime":
        return f"datetime'{iso}'"
    return f"datetimeoffset'{iso}Z'"


def build_modified_filter(
    spec: EntitySpec,
    since_ms: Optional[int] = None,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
) -> Optional[str]:
    """``$filter`` clause on the entity's change field from the incremental sync
    point (``since_ms``, exclusive) and the user's modified-date filter (inclusive)."""
    if not spec.change_field:
        return None
    lower = max(v for v in (since_ms, start_ms) if v is not None) if (since_ms or start_ms) else None
    clauses: list[str] = []
    if lower is not None:
        op = "gt" if since_ms is not None and lower == since_ms else "ge"
        clauses.append(f"{spec.change_field} {op} {odata_datetime_literal(lower, spec.change_field_kind, spec.odata_version)}")
    if end_ms is not None:
        clauses.append(f"{spec.change_field} le {odata_datetime_literal(end_ms, spec.change_field_kind, spec.odata_version)}")
    return " and ".join(clauses) if clauses else None


def build_scope_filter(spec: EntitySpec, company_codes: Sequence[str] = (), sales_orgs: Sequence[str] = ()) -> Optional[str]:
    """``$filter`` for the company-code / sales-org sync filters (only where the
    entity carries the property at header level)."""
    parts: list[str] = []
    if company_codes and spec.company_code_field:
        parts.append(_in_clause(spec.company_code_field, company_codes))
    if sales_orgs and spec.sales_org_field:
        parts.append(_in_clause(spec.sales_org_field, sales_orgs))
    return " and ".join(p for p in parts if p) or None


def _in_clause(prop: str, values: Sequence[str]) -> str:
    cleaned = sorted({str(v).strip() for v in values if str(v).strip()})
    if not cleaned:
        return ""
    clause = " or ".join(f"{prop} eq '{_odata_quote(v)}'" for v in cleaned)
    return f"({clause})" if len(cleaned) > 1 else clause


def combine_filters(*clauses: Optional[str]) -> Optional[str]:
    present = [c for c in clauses if c]
    return " and ".join(present) if present else None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def display_value(row: Mapping[str, Any], prop: str) -> Optional[str]:
    raw = row.get(prop)
    if raw in (None, ""):
        return None
    if isinstance(raw, bool):
        return "Yes" if raw else "No"
    if isinstance(raw, str) and raw.startswith("/Date("):
        return format_sap_timestamp(raw)
    if isinstance(raw, float) and raw.is_integer():
        return str(int(raw))
    if isinstance(raw, Mapping):
        return None
    return str(raw)


def product_description(row: Mapping[str, Any], preferred_language: str = "EN") -> Optional[str]:
    descriptions = [d for d in odata_collection(row.get("to_Description")) if isinstance(d, Mapping)]
    for d in descriptions:
        if str(d.get("Language") or "").upper() == preferred_language.upper() and d.get("ProductDescription"):
            return str(d["ProductDescription"])
    for d in descriptions:
        if d.get("ProductDescription"):
            return str(d["ProductDescription"])
    return None


def record_title(spec: EntitySpec, row: Mapping[str, Any]) -> str:
    keys = row_key_values(spec, row) or ()
    key_text = "/".join(keys)
    if spec.name == "product":
        desc = product_description(row)
        return f"{desc} ({key_text})" if desc else f"Product {key_text}".strip()
    if spec.name == "business_partner":
        for prop in spec.title_fields:
            value = display_value(row, prop)
            if value:
                return value
        first, last = display_value(row, "FirstName"), display_value(row, "LastName")
        if first or last:
            return " ".join(p for p in (first, last) if p)
        return f"Business Partner {key_text}".strip()
    for prop in spec.title_fields:
        value = display_value(row, prop)
        if value:
            return f"{spec.singular} {key_text} · {value}"
    return f"{spec.singular} {key_text}".strip()


def scope_codes(spec: EntitySpec, row: Mapping[str, Any]) -> dict[str, set[str]]:
    """Organisational codes carried by the document, per scope kind.

    Header properties come from ``spec.scope_fields``; line items add
    ``spec.item_scope_fields``.  Business partners derive company codes / sales
    orgs from their customer & supplier sub-entities, products from
    ``to_Plant`` / ``to_SalesDelivery``.
    """
    out: dict[str, set[str]] = {}

    def add(kind: str, value: Any) -> None:
        if value not in (None, ""):
            out.setdefault(kind, set()).add(str(value).strip())

    for kind, prop in spec.scope_fields:
        add(kind, row.get(prop))
    if spec.items_nav:
        for item in odata_collection(row.get(spec.items_nav)):
            if isinstance(item, Mapping):
                for kind, prop in spec.item_scope_fields:
                    add(kind, item.get(prop))
    if spec.name == "business_partner":
        customer = row.get("to_Customer") or {}
        supplier = row.get("to_Supplier") or {}
        if isinstance(customer, Mapping):
            for entry in odata_collection(customer.get("to_CustomerCompany")):
                if isinstance(entry, Mapping):
                    add(SCOPE_COMPANY_CODE, entry.get("CompanyCode"))
            for entry in odata_collection(customer.get("to_CustomerSalesArea")):
                if isinstance(entry, Mapping):
                    add(SCOPE_SALES_ORG, entry.get("SalesOrganization"))
        if isinstance(supplier, Mapping):
            for entry in odata_collection(supplier.get("to_SupplierCompany")):
                if isinstance(entry, Mapping):
                    add(SCOPE_COMPANY_CODE, entry.get("CompanyCode"))
    if spec.name == "product":
        for entry in odata_collection(row.get("to_Plant")):
            if isinstance(entry, Mapping):
                add(SCOPE_PLANT, entry.get("Plant"))
        for entry in odata_collection(row.get("to_SalesDelivery")):
            if isinstance(entry, Mapping):
                add(SCOPE_SALES_ORG, entry.get("ProductSalesOrg"))
    return out


def build_metadata(
    spec: EntitySpec,
    row: Mapping[str, Any],
    base_url: str,
    fiori_base_url: Optional[str] = None,
    sap_client: Optional[str] = None,
) -> dict[str, Any]:
    """Raw identifiers kept alongside the rendered content (embedded in the
    markdown footer; ``Record`` has no free-form metadata slot)."""
    keys = row_key_values(spec, row) or ()
    scopes = scope_codes(spec, row)
    return {
        "source": "sap-s4hana",
        "entity": spec.name,
        "service": spec.service,
        "entity_set": spec.entity_set,
        "keys": dict(zip(spec.key_fields, keys)),
        "created_by": row.get(spec.created_by_field) if spec.created_by_field else None,
        "created_at": format_sap_timestamp(row.get(spec.created_at_field)) if spec.created_at_field else None,
        "changed_at": format_sap_timestamp(row.get(spec.change_field)) if spec.change_field else None,
        "scopes": {kind: sorted(values) for kind, values in sorted(scopes.items())},
        "url": record_web_url(base_url, spec, keys, fiori_base_url, sap_client) if keys else None,
        "odata_url": entity_url(base_url, spec, keys, sap_client) if keys else None,
    }


def _markdown_cell(value: Optional[str]) -> str:
    return (value or "").replace("|", "\\|").replace("\n", " ")


def render_record_markdown(
    spec: EntitySpec,
    row: Mapping[str, Any],
    base_url: str,
    fiori_base_url: Optional[str] = None,
    sap_client: Optional[str] = None,
) -> tuple[str, dict[str, Any]]:
    """Readable markdown for chunking/embedding + the raw-id metadata dict."""
    metadata = build_metadata(spec, row, base_url, fiori_base_url, sap_client)
    title = record_title(spec, row)

    lines: list[str] = [f"# {title}", ""]
    header_bits = [f"**Type:** SAP {spec.singular}"]
    for status_prop in ("OverallSDProcessStatus", "PurchasingProcessingStatus", "SupplierInvoiceStatus"):
        status = display_value(row, status_prop)
        if status:
            header_bits.append(f"**Status:** {status}")
            break
    created_by = display_value(row, spec.created_by_field) if spec.created_by_field else None
    if created_by:
        header_bits.append(f"**Created by:** {created_by}")
    lines.append(" · ".join(header_bits))
    lines.append("")

    details: list[str] = []
    for prop, label in spec.summary_fields:
        value = display_value(row, prop)
        if value:
            details.append(f"- **{label}:** {value}")
    if spec.name == "product":
        desc = product_description(row)
        if desc:
            details.insert(0, f"- **Description:** {desc}")
    if details:
        lines.append("## Details")
        lines.extend(details)
        lines.append("")

    if spec.name == "business_partner":
        addresses = [a for a in odata_collection(row.get("to_BusinessPartnerAddress")) if isinstance(a, Mapping)]
        if addresses:
            lines.append("## Addresses")
            for addr in addresses:
                parts = [display_value(addr, p) for p in ("StreetName", "HouseNumber", "PostalCode", "CityName", "Region", "Country")]
                text = ", ".join(p for p in parts if p)
                if text:
                    lines.append(f"- {text}")
            lines.append("")
        roles = [display_value(r, "BusinessPartnerRole") for r in odata_collection(row.get("to_BusinessPartnerRole")) if isinstance(r, Mapping)]
        roles = [r for r in roles if r]
        if roles:
            lines.append(f"**Roles:** {', '.join(sorted(set(roles)))}")
            lines.append("")

    if spec.items_nav and spec.item_fields:
        items = [i for i in odata_collection(row.get(spec.items_nav)) if isinstance(i, Mapping)]
        if items:
            lines.append(f"## Items ({len(items)})")
            lines.append("| " + " | ".join(label for _, label in spec.item_fields) + " |")
            lines.append("|" + "---|" * len(spec.item_fields))
            for item in items:
                lines.append("| " + " | ".join(_markdown_cell(display_value(item, prop)) for prop, _ in spec.item_fields) + " |")
            lines.append("")

    lines.append("## Source metadata")
    lines.append(f"- SAP service: {spec.service} / {spec.entity_set}")
    for key_name, key_value in metadata["keys"].items():
        lines.append(f"- {key_name}: {key_value}")
    for kind, codes in metadata["scopes"].items():
        lines.append(f"- {_scope_label(kind)}: {', '.join(codes)}")
    if metadata["created_by"]:
        lines.append(f"- Created by: {metadata['created_by']}")
    if metadata["created_at"] or metadata["changed_at"]:
        lines.append(f"- Created: {metadata['created_at'] or 'n/a'} · Changed: {metadata['changed_at'] or 'n/a'}")
    if metadata["url"]:
        lines.append(f"- URL: {metadata['url']}")
    return "\n".join(lines).rstrip() + "\n", metadata


def _scope_label(kind: str) -> str:
    return {
        SCOPE_SALES_ORG: "Sales organization",
        SCOPE_COMPANY_CODE: "Company code",
        SCOPE_PLANT: "Plant",
        SCOPE_PURCH_ORG: "Purchasing organization",
    }.get(kind, kind)


# ---------------------------------------------------------------------------
# Attachments (API_CMS_ATTACHMENT_SRV)
# ---------------------------------------------------------------------------

# Key properties of AttachmentContentSet, in the order SAP expects them.
ATTACHMENT_KEY_FIELDS: tuple[str, ...] = (
    "DocumentInfoRecordDocType", "DocumentInfoRecordDocNumber", "DocumentInfoRecordDocVersion",
    "DocumentInfoRecordDocPart", "LogicalDocument", "ArchiveDocumentID",
    "LinkedSAPObjectKey", "BusinessObjectTypeName",
)


def attachment_list_url(base_url: str, spec: EntitySpec, key_values: Sequence[str]) -> Optional[str]:
    """``GetAllOriginals`` function import listing the DMS originals of one document."""
    if not spec.attachment_object_type:
        return None
    linked_key = "".join(key_values)  # SAP concatenates composite keys (e.g. invoice + fiscal year)
    params = urlencode({
        "BusinessObjectTypeName": f"'{spec.attachment_object_type}'",
        "LinkedSAPObjectKey": f"'{linked_key}'",
        "$format": "json",
    }, safe="'")
    return f"{service_root(base_url, ATTACHMENT_SERVICE)}/GetAllOriginals?{params}"


def attachment_key(entry: Mapping[str, Any]) -> Optional[str]:
    """Stable id of one original: ``LogicalDocument`` when present, else ``ArchiveDocumentID``,
    else the DIR number/version/part."""
    for prop in ("LogicalDocument", "ArchiveDocumentID"):
        value = entry.get(prop)
        if value not in (None, ""):
            return str(value)
    doc_number = entry.get("DocumentInfoRecordDocNumber")
    if doc_number in (None, ""):
        return None
    return f"{doc_number}-{entry.get('DocumentInfoRecordDocVersion', '')}-{entry.get('DocumentInfoRecordDocPart', '')}"


def attachment_content_url(base_url: str, entry: Mapping[str, Any]) -> str:
    """``AttachmentContentSet(<all keys>)/$value`` — the binary of one original."""
    predicate = ",".join(f"{name}='{_odata_quote(str(entry.get(name) or ''))}'" for name in ATTACHMENT_KEY_FIELDS)
    return f"{service_root(base_url, ATTACHMENT_SERVICE)}/{ATTACHMENT_ENTITY_SET}({predicate})/$value"


# ---------------------------------------------------------------------------
# Authorization mapping and RBAC derivation
# ---------------------------------------------------------------------------


class GrantRole(str, Enum):
    READER = "READER"
    WRITER = "WRITER"
    OWNER = "OWNER"


_ROLE_RANK = {GrantRole.READER: 1, GrantRole.WRITER: 2, GrantRole.OWNER: 3}


class GrantEntity(str, Enum):
    USER = "USER"
    GROUP = "GROUP"


@dataclass(frozen=True)
class PermissionGrant:
    """Connector-agnostic permission; ``connector.py`` turns it into ``Permission``."""

    entity_type: GrantEntity
    role: GrantRole
    external_id: Optional[str] = None  # GROUP external id (``sap:...``)
    email: Optional[str] = None        # USER
    reason: str = field(default="", compare=False)


def is_email(value: Any) -> bool:
    return isinstance(value, str) and bool(_EMAIL_RE.match(value.strip()))


def normalize_email(value: str) -> str:
    return value.strip().lower()


def slugify_group_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-") or "group"


def scope_group_external_id(kind: str, code: str) -> str:
    """``sap:companycode:1010`` / ``sap:entity:sales_order`` ..."""
    return f"{GROUP_PREFIX}{kind}:{str(code).strip()}"


def named_group_external_id(name: str) -> str:
    """``sap:group:<slug>`` for a ``group:<name>`` reference in the mapping."""
    return f"{GROUP_PREFIX}group:{slugify_group_name(name)}"


def parse_scope_key(key: str) -> Optional[tuple[str, str]]:
    """``companycode:1010`` → ``("companycode", "1010")``; ``entity:sales_order`` → ``("entity", "sales_order")``."""
    kind, _, code = str(key).strip().lower().partition(":")
    code = code.strip()
    if not code or kind not in SCOPE_KINDS + (SCOPE_ENTITY,):
        return None
    if kind == SCOPE_ENTITY and code not in ENTITY_SPECS:
        return None
    return kind, (code if kind == SCOPE_ENTITY else str(key).partition(":")[2].strip())


# ``group:<name-or-id>`` -> member e-mails, or ``None`` when the directory does not
# know the group.  ``connector.py`` pre-resolves asynchronously (Microsoft Graph)
# and hands a plain ``dict.get`` here so this module stays I/O-free.
GroupResolver = Callable[[str], Optional[Sequence[str]]]


@dataclass
class ExpandedMapping:
    """Result of :meth:`AuthorizationMapping.expand`: group id -> member e-mails."""

    members: dict[str, list[str]] = field(default_factory=dict)
    unresolved_groups: list[str] = field(default_factory=list)

    def all_emails(self) -> list[str]:
        out: set[str] = set()
        for emails in self.members.values():
            out.update(emails)
        return sorted(out)


@dataclass
class AuthorizationMapping:
    """Parsed ``authorization_mapping`` sync field (see module docstring)."""

    admins: list[str] = field(default_factory=list)                     # e-mails
    # scope group external id -> principals (e-mails and ``group:<name>`` refs)
    scope_principals: dict[str, list[str]] = field(default_factory=dict)
    named_groups: dict[str, list[str]] = field(default_factory=dict)    # name -> e-mails
    user_emails: dict[str, str] = field(default_factory=dict)           # SAP user id (upper) -> e-mail
    admin_group_refs: list[str] = field(default_factory=list)           # ``group:`` refs under admins
    warnings: list[str] = field(default_factory=list)

    # -- lookups -----------------------------------------------------------

    def group_ids(self) -> list[str]:
        """Every ``sap:*`` group the mapping defines (scopes, named groups, admins)."""
        ids = [ADMINS_GROUP_ID]
        ids.extend(sorted(self.scope_principals))
        ids.extend(named_group_external_id(name) for name in sorted(self.named_groups))
        seen: set[str] = set()
        return [g for g in ids if not (g in seen or seen.add(g))]

    def named_group_members(self, name: str) -> list[str]:
        for known, members in self.named_groups.items():
            if slugify_group_name(known) == slugify_group_name(name):
                return list(members)
        return []

    def principals_of(self, group_external_id: str) -> list[str]:
        """Raw principals (e-mails and ``group:`` refs) configured for a ``sap:*`` group."""
        if group_external_id == ADMINS_GROUP_ID:
            return list(self.admins) + list(self.admin_group_refs)
        if group_external_id.startswith(f"{GROUP_PREFIX}group:"):
            slug = group_external_id[len(f"{GROUP_PREFIX}group:"):]
            for name, members in self.named_groups.items():
                if slugify_group_name(name) == slug:
                    return list(members)
            return []
        return list(self.scope_principals.get(group_external_id, ()))

    def expand_group_ref(self, name_or_id: str, resolver: Optional[GroupResolver] = None) -> tuple[list[str], bool]:
        """Members of one ``group:<name-or-id>`` reference.

        Entra (``resolver``) first, then the inline ``groups`` section; the two
        are unioned so an inline list can extend a directory group.  Returns
        ``(emails, resolved)`` where ``resolved`` is False when neither source
        knew the group.
        """
        emails: list[str] = []
        resolved = False
        if resolver is not None:
            directory = resolver(name_or_id)
            if directory is not None:
                resolved = True
                emails.extend(normalize_email(e) for e in directory if is_email(e))
        inline = self.named_group_members(name_or_id)
        if inline or any(slugify_group_name(n) == slugify_group_name(name_or_id) for n in self.named_groups):
            resolved = True
            emails.extend(inline)
        seen: set[str] = set()
        return [e for e in emails if not (e in seen or seen.add(e))], resolved

    def members_of(self, group_external_id: str, resolver: Optional[GroupResolver] = None) -> list[str]:
        """Flattened member e-mails of a ``sap:*`` group: literal e-mails plus every
        ``group:`` reference expanded through ``resolver`` (Entra) and/or ``groups``."""
        emails: list[str] = []
        for principal in self.principals_of(group_external_id):
            if principal.lower().startswith(GROUP_REF_PREFIX):
                members, _ = self.expand_group_ref(principal[len(GROUP_REF_PREFIX):].strip(), resolver)
                emails.extend(members)
            elif is_email(principal):
                emails.append(normalize_email(principal))
        seen: set[str] = set()
        return [e for e in emails if not (e in seen or seen.add(e))]

    def expand(self, resolver: Optional[GroupResolver] = None) -> "ExpandedMapping":
        """Resolve every ``sap:*`` group to its member e-mails once per sync."""
        members = {gid: self.members_of(gid, resolver) for gid in self.group_ids()}
        unresolved = [
            name for name in self.referenced_named_groups()
            if not self.expand_group_ref(name, resolver)[1]
        ]
        return ExpandedMapping(members=members, unresolved_groups=unresolved)

    def referenced_named_groups(self) -> list[str]:
        """Names referenced via ``group:`` anywhere (defined or not)."""
        names: list[str] = []
        for principals in list(self.scope_principals.values()) + [self.admin_group_refs]:
            for p in principals:
                if p.lower().startswith(GROUP_REF_PREFIX):
                    names.append(p[len(GROUP_REF_PREFIX):].strip())
        seen: set[str] = set()
        return [n for n in names if n and not (n in seen or seen.add(n))]

    def undefined_named_groups(self) -> list[str]:
        defined = {slugify_group_name(n) for n in self.named_groups}
        return [n for n in self.referenced_named_groups() if slugify_group_name(n) not in defined]

    def all_emails(self) -> list[str]:
        emails: set[str] = set(self.admins)
        for members in self.named_groups.values():
            emails.update(members)
        for principals in self.scope_principals.values():
            emails.update(normalize_email(p) for p in principals if is_email(p))
        emails.update(self.user_emails.values())
        return sorted(emails)

    def codes_for(self, kind: str) -> list[str]:
        prefix = f"{GROUP_PREFIX}{kind}:"
        return sorted(g[len(prefix):] for g in self.scope_principals if g.startswith(prefix))

    def resolve_sap_user(self, sap_user: Optional[str], email_domain: Optional[str]) -> Optional[str]:
        """SAP user id → e-mail: explicit ``users`` map first, then ``<user>@<domain>``."""
        if not sap_user:
            return None
        key = str(sap_user).strip()
        if not key:
            return None
        explicit = self.user_emails.get(key.upper())
        if explicit:
            return explicit
        if is_email(key):
            return normalize_email(key)
        domain = (email_domain or "").strip().lstrip("@").lower()
        if not domain:
            return None
        return f"{key.lower()}@{domain}"


def _as_principal_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = re.split(r"[,\n;]+", value)
    elif isinstance(value, (list, tuple, set)):
        items = [str(v) for v in value]
    else:
        return []
    out: list[str] = []
    for item in items:
        text = (item or "").strip()
        if not text:
            continue
        if text.lower().startswith(USER_REF_PREFIX):
            text = text[len(USER_REF_PREFIX):].strip()
        out.append(text)
    return out


def parse_authorization_mapping(raw: Any) -> AuthorizationMapping:
    """Parse the ``authorization_mapping`` sync field (JSON text or already-decoded dict).

    Unknown keys and non-email principals are collected in ``warnings`` rather
    than raising, so a typo never blocks a sync; invalid JSON *does* raise.
    """
    mapping = AuthorizationMapping()
    if raw in (None, ""):
        return mapping
    data = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return mapping
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"authorization_mapping is not valid JSON: {e}") from e
    if not isinstance(data, Mapping):
        raise ValueError("authorization_mapping must be a JSON object")

    for key, value in data.items():
        key_text = str(key).strip()
        lowered = key_text.lower()
        if lowered == ADMINS_MAPPING_KEY:
            for principal in _as_principal_list(value):
                if principal.lower().startswith(GROUP_REF_PREFIX):
                    mapping.admin_group_refs.append(principal)
                elif is_email(principal):
                    mapping.admins.append(normalize_email(principal))
                else:
                    mapping.warnings.append(f"admins: ignoring non-email principal {principal!r}")
            continue
        if lowered == GROUPS_MAPPING_KEY:
            if not isinstance(value, Mapping):
                mapping.warnings.append("groups: expected an object of name -> [emails]")
                continue
            for name, members in value.items():
                emails = []
                for principal in _as_principal_list(members):
                    if is_email(principal):
                        emails.append(normalize_email(principal))
                    else:
                        mapping.warnings.append(f"groups[{name!r}]: ignoring non-email member {principal!r}")
                mapping.named_groups[str(name).strip()] = emails
            continue
        if lowered == USERS_MAPPING_KEY:
            if not isinstance(value, Mapping):
                mapping.warnings.append("users: expected an object of SAP user id -> email")
                continue
            for sap_user, email in value.items():
                if is_email(email):
                    mapping.user_emails[str(sap_user).strip().upper()] = normalize_email(str(email))
                else:
                    mapping.warnings.append(f"users[{sap_user!r}]: {email!r} is not an e-mail")
            continue
        scope = parse_scope_key(key_text)
        if scope is None:
            mapping.warnings.append(f"ignoring unknown mapping key {key_text!r}")
            continue
        kind, code = scope
        group_id = scope_group_external_id(kind, code)
        principals: list[str] = []
        for principal in _as_principal_list(value):
            if principal.lower().startswith(GROUP_REF_PREFIX) or is_email(principal):
                principals.append(principal if principal.lower().startswith(GROUP_REF_PREFIX) else normalize_email(principal))
            else:
                mapping.warnings.append(f"{key_text}: ignoring non-email principal {principal!r}")
        mapping.scope_principals.setdefault(group_id, []).extend(principals)
    return mapping


@dataclass
class AuthorizationContext:
    """Everything the RBAC derivation needs for one sync."""

    mapping: AuthorizationMapping = field(default_factory=AuthorizationMapping)
    user_email_domain: Optional[str] = None
    grant_scope_groups: bool = True  # emit sap:<kind>:<code> READER edges

    def entity_grants(self, spec: EntitySpec) -> list[PermissionGrant]:
        """Grants that apply to every row of ``spec`` (also used for the record group)."""
        return [
            PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id=ADMINS_GROUP_ID, reason="SAP admins"),
            PermissionGrant(
                GrantEntity.GROUP, GrantRole.READER,
                external_id=scope_group_external_id(SCOPE_ENTITY, spec.name),
                reason=f"entity:{spec.name}",
            ),
        ]


def _merge(grants: Iterable[PermissionGrant]) -> list[PermissionGrant]:
    """Dedupe on principal, keeping the strongest role; stable order of first sight."""
    best: dict[tuple[GrantEntity, Optional[str], Optional[str]], PermissionGrant] = {}
    order: list[tuple[GrantEntity, Optional[str], Optional[str]]] = []
    for grant in grants:
        key = (grant.entity_type, grant.external_id, (grant.email or "").lower() or None)
        current = best.get(key)
        if current is None:
            best[key] = grant
            order.append(key)
        elif _ROLE_RANK[grant.role] > _ROLE_RANK[current.role]:
            best[key] = grant
    return [best[k] for k in order]


def derive_grants(spec: EntitySpec, row: Mapping[str, Any], ctx: AuthorizationContext) -> list[PermissionGrant]:
    """Apply the permission model documented in the module docstring to one row."""
    grants: list[PermissionGrant] = []

    # 1. Creator -> OWNER (when resolvable to an e-mail)
    if spec.created_by_field:
        email = ctx.mapping.resolve_sap_user(row.get(spec.created_by_field), ctx.user_email_domain)
        if email:
            grants.append(PermissionGrant(GrantEntity.USER, GrantRole.OWNER, email=email, reason=spec.created_by_field))

    # 2. Organisational scopes -> GROUP READER
    if ctx.grant_scope_groups:
        for kind, codes in sorted(scope_codes(spec, row).items()):
            for code in sorted(codes):
                grants.append(PermissionGrant(
                    GrantEntity.GROUP, GrantRole.READER,
                    external_id=scope_group_external_id(kind, code), reason=f"{kind}:{code}",
                ))

    # 3. Entity-wide readers + SAP admins
    grants.extend(ctx.entity_grants(spec))
    return _merge(grants)


def group_ids_in_grants(grants: Iterable[PermissionGrant]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for grant in grants:
        if grant.entity_type == GrantEntity.GROUP and grant.external_id and grant.external_id not in seen:
            seen.add(grant.external_id)
            out.append(grant.external_id)
    return out


def group_display_name(group_external_id: str) -> str:
    """Human label for a ``sap:*`` group id."""
    body = group_external_id[len(GROUP_PREFIX):] if group_external_id.startswith(GROUP_PREFIX) else group_external_id
    kind, _, code = body.partition(":")
    if kind == "admins":
        return "SAP · Administrators"
    if kind == "group":
        return f"SAP · Group {code}"
    if kind == SCOPE_ENTITY:
        spec = ENTITY_SPECS.get(code)
        return f"SAP · All {spec.display_name if spec else code}"
    return f"SAP · {_scope_label(kind)} {code}"


# ---------------------------------------------------------------------------
# misc
# ---------------------------------------------------------------------------


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def as_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def file_extension(filename: Optional[str]) -> Optional[str]:
    if not filename or "." not in filename:
        return None
    return filename.rsplit(".", 1)[-1].lower() or None
