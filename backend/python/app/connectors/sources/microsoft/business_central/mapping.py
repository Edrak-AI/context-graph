"""Pure helpers for the Microsoft Dynamics 365 Business Central connector.

Everything in this module is I/O free and depends on the standard library only
(plus two tiny pure helpers shared with the SAP connector) so the entity mapping,
markdown rendering, OData query building, company scoping, reconcile planning and
the permission derivation can be unit-tested without the connector's runtime
dependencies (``httpx``, pydantic models, config service).  ``connector.py``
turns the plain dataclasses produced here into ``Record`` / ``Permission`` objects.

Business Central API v2.0 facts the connector relies on
======================================================

* Base URL ``https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environmentName}/api/v2.0/``;
  ``GET companies`` lists the companies the Entra application may see (``id``, ``name``,
  ``displayName``); every other entity set hangs off a company:
  ``companies({id})/customers``, ``.../salesOrders?$expand=salesOrderLines`` ...
* Every row carries ``id`` (GUID) and ``lastModifiedDateTime`` (UTC ISO-8601), so the
  incremental sync is a server-side ``$filter=lastModifiedDateTime gt <last sync>``.
* Server-driven paging: ``$top`` plus ``@odata.nextLink`` (absolute URL carrying a
  ``$skiptoken``).  Responses are OData v4 JSON (``value`` array).
* Deletes are **not** exposed (no delta links, no deleted-entity feed).  Deletions are
  detected by a periodic key-set reconcile — ``$select=id`` pull of each entity set,
  compared with the record external ids the graph holds, missing ones removed through
  the standard cascade delete.  Interval: ``reconcile_interval_hours`` sync filter,
  default 24 h, ``0`` disables it.  A full sync reconciles for free.
* Throttling: HTTP 429 / 503 with ``Retry-After`` (delay-seconds).  The connector honours
  the header through the shared ``retry_delay`` helper.

Permission model (how Business Central access becomes CGraph permission edges)
=============================================================================

Business Central authorises per **company**: a user who may open a company can read
its master data and documents (finer permission sets exist but are not exposed to an
API client, and API v2.0 has no ``users`` / ``userPermissions`` collection an app
could read).  The connector therefore mirrors *company access* only:

+---------------------------------------+-----------------------------------------------+--------+
| Source                                | CGraph principal                              | Role   |
+=======================================+===============================================+========+
| company (always)                      | GROUP ``bc:company:<companyId>`` — one        | READER |
|                                       | AppUserGroup per company, named after it      |        |
| ``companyAccessGroups`` auth field    | members of that group = transitive members of | (same) |
| (``Company = entra-group[, ...]``)    | the listed Entra groups (Microsoft Graph,     |        |
|                                       | resolved by name or object id)                |        |
| company **without** an access-group   | ORG (every member of the Edrak organisation)  | READER |
| entry                                 | — the admin acknowledges this org-wide grant  |        |
|                                       | in the edrak-ai policy dialog                 |        |
+---------------------------------------+-----------------------------------------------+--------+

Deliberately *not* mapped: ``salespersonCode`` / ``salesperson`` / ``purchaser`` are
BC codes (``JR``, ``PS``), not e-mail addresses, so no OWNER edge is derived; BC
permission sets, security filters and record-level restrictions are ignored.  Text
(company, customer, vendor and item names, descriptions) is passed through unchanged;
Arabic and other non-Latin content is never transliterated or trimmed.
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import quote

from app.connectors.sources.sap.mapping import (
    parse_reconcile_interval_hours,
    parse_retry_after,
    reconcile_due,
    retry_delay,
)

__all__ = [
    "parse_reconcile_interval_hours",
    "parse_retry_after",
    "reconcile_due",
    "retry_delay",
]

# ---------------------------------------------------------------------------
# Business Central constants
# ---------------------------------------------------------------------------

BC_API_HOST = "https://api.businesscentral.dynamics.com"
BC_API_VERSION = "v2.0"
BC_WEB_HOST = "https://businesscentral.dynamics.com"
LOGIN_HOST = "https://login.microsoftonline.com"
TOKEN_SCOPE = f"{BC_API_HOST}/.default"
DEFAULT_ENVIRONMENT = "Production"

MODIFIED_FIELD = "lastModifiedDateTime"
DOCUMENT_PAGE_SIZE = 1000   # rows per page when documents (with expanded lines) are pulled
KEY_PAGE_SIZE = 5000        # ``$select=id`` rows per page during the reconcile

ENTITIES_FILTER_KEY = "entities"
RECONCILE_INTERVAL_FILTER_KEY = "reconcile_interval_hours"
DEFAULT_RECONCILE_INTERVAL_HOURS = 24.0
MS_PER_HOUR = 3_600_000

RECORD_ID_PREFIX = "bc"
COMPANY_GROUP_PREFIX = "bc:company:"
ACCESS_MAPPING_WILDCARD = "*"

# Sync-point document fields (camelCase like the other connectors' sync points).
FIELD_LAST_SYNC = "lastSyncTimestamp"
FIELD_LAST_RECONCILE = "lastReconcileTimestamp"

# Business Central renders "empty" dates / GUIDs as these sentinels.
_EMPTY_DATE_PREFIX = "0001-01-01"
_EMPTY_GUID = "00000000-0000-0000-0000-000000000000"
_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_FRACTION_RE = re.compile(r"\.(\d+)")

# Unposted document statuses; anything else on an invoice means it is posted and lives
# on the "Posted ..." card page in the web client.
_UNPOSTED_STATUSES = frozenset({"draft", "in review"})


# ---------------------------------------------------------------------------
# Entity registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntitySpec:
    """Static description of one Business Central API v2.0 entity set the connector indexes."""

    entity_set: str                 # "salesOrders" — path segment under companies({id})
    display_name: str               # "Sales orders" (filter label)
    singular: str                   # "Sales order" (titles / headers)
    record_type: str                # ``RecordType`` value
    card_page: int                  # web client page id of the card / document page
    # (attribute, label) pairs rendered in the "Details" section, in order.
    summary_fields: tuple[tuple[str, str], ...]
    party_field: str | None = None          # customerName / vendorName → appended to document titles
    lines_property: str | None = None       # "salesOrderLines" → $expand + rendered table
    line_columns: tuple[tuple[str, str], ...] = ()
    description_field: str | None = None
    posted_card_page: int | None = None     # invoices: page once posted (status not Draft)
    is_master_data: bool = False


_ADDRESS_FIELDS: tuple[tuple[str, str], ...] = (
    ("addressLine1", "Address"),
    ("addressLine2", "Address 2"),
    ("city", "City"),
    ("state", "State"),
    ("postalCode", "Postal code"),
    ("country", "Country"),
)

_DOCUMENT_TOTALS: tuple[tuple[str, str], ...] = (
    ("currencyCode", "Currency"),
    ("discountAmount", "Discount"),
    ("totalAmountExcludingTax", "Total excl. tax"),
    ("totalTaxAmount", "Tax"),
    ("totalAmountIncludingTax", "Total incl. tax"),
)

_SALES_LINE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("sequence", "#"),
    ("lineType", "Type"),
    ("lineObjectNumber", "No."),
    ("description", "Description"),
    ("quantity", "Quantity"),
    ("unitOfMeasureCode", "Unit"),
    ("unitPrice", "Unit price"),
    ("discountPercent", "Discount %"),
    ("amountExcludingTax", "Amount excl. tax"),
    ("amountIncludingTax", "Amount incl. tax"),
)

_PURCHASE_LINE_COLUMNS: tuple[tuple[str, str], ...] = tuple(
    ("directUnitCost", "Direct unit cost") if attr == "unitPrice" else (attr, label)
    for attr, label in _SALES_LINE_COLUMNS
)

ENTITY_SPECS: dict[str, EntitySpec] = {
    "customers": EntitySpec(
        entity_set="customers",
        display_name="Customers",
        singular="Customer",
        record_type="OTHERS",
        card_page=21,
        is_master_data=True,
        summary_fields=(
            ("number", "No."),
            ("type", "Type"),
            ("email", "Email"),
            ("phoneNumber", "Phone"),
            ("website", "Website"),
            *_ADDRESS_FIELDS,
            ("currencyCode", "Currency"),
            ("taxRegistrationNumber", "Tax registration no."),
            ("salespersonCode", "Salesperson code"),
            ("creditLimit", "Credit limit"),
            ("balanceDue", "Balance due"),
            ("blocked", "Blocked"),
        ),
    ),
    "vendors": EntitySpec(
        entity_set="vendors",
        display_name="Vendors",
        singular="Vendor",
        record_type="OTHERS",
        card_page=26,
        is_master_data=True,
        summary_fields=(
            ("number", "No."),
            ("email", "Email"),
            ("phoneNumber", "Phone"),
            ("website", "Website"),
            *_ADDRESS_FIELDS,
            ("currencyCode", "Currency"),
            ("taxRegistrationNumber", "Tax registration no."),
            ("balance", "Balance"),
            ("blocked", "Blocked"),
        ),
    ),
    "items": EntitySpec(
        entity_set="items",
        display_name="Items",
        singular="Item",
        record_type="PRODUCT",
        card_page=30,
        is_master_data=True,
        description_field="displayName2",
        summary_fields=(
            ("number", "No."),
            ("type", "Type"),
            ("itemCategoryCode", "Item category"),
            ("baseUnitOfMeasureCode", "Base unit of measure"),
            ("gtin", "GTIN"),
            ("inventory", "Inventory"),
            ("unitPrice", "Unit price"),
            ("unitCost", "Unit cost"),
            ("priceIncludesTax", "Price includes tax"),
            ("blocked", "Blocked"),
        ),
    ),
    "salesOrders": EntitySpec(
        entity_set="salesOrders",
        display_name="Sales orders",
        singular="Sales order",
        record_type="OTHERS",
        card_page=42,
        party_field="customerName",
        lines_property="salesOrderLines",
        line_columns=_SALES_LINE_COLUMNS,
        summary_fields=(
            ("number", "No."),
            ("status", "Status"),
            ("orderDate", "Order date"),
            ("postingDate", "Posting date"),
            ("requestedDeliveryDate", "Requested delivery date"),
            ("customerName", "Customer"),
            ("customerNumber", "Customer no."),
            ("externalDocumentNumber", "External document no."),
            ("billToName", "Bill-to"),
            ("shipToName", "Ship-to"),
            ("salesperson", "Salesperson code"),
            *_DOCUMENT_TOTALS,
            ("fullyShipped", "Fully shipped"),
        ),
    ),
    "salesInvoices": EntitySpec(
        entity_set="salesInvoices",
        display_name="Sales invoices",
        singular="Sales invoice",
        record_type="OTHERS",
        card_page=43,
        posted_card_page=132,
        party_field="customerName",
        lines_property="salesInvoiceLines",
        line_columns=_SALES_LINE_COLUMNS,
        summary_fields=(
            ("number", "No."),
            ("status", "Status"),
            ("invoiceDate", "Invoice date"),
            ("postingDate", "Posting date"),
            ("dueDate", "Due date"),
            ("customerName", "Customer"),
            ("customerNumber", "Customer no."),
            ("externalDocumentNumber", "External document no."),
            ("customerPurchaseOrderReference", "Customer PO reference"),
            ("orderNumber", "Order no."),
            ("billToName", "Bill-to"),
            ("shipToName", "Ship-to"),
            ("salesperson", "Salesperson code"),
            *_DOCUMENT_TOTALS,
            ("remainingAmount", "Remaining amount"),
        ),
    ),
    "purchaseOrders": EntitySpec(
        entity_set="purchaseOrders",
        display_name="Purchase orders",
        singular="Purchase order",
        record_type="OTHERS",
        card_page=50,
        party_field="vendorName",
        lines_property="purchaseOrderLines",
        line_columns=_PURCHASE_LINE_COLUMNS,
        summary_fields=(
            ("number", "No."),
            ("status", "Status"),
            ("orderDate", "Order date"),
            ("postingDate", "Posting date"),
            ("requestedReceiptDate", "Requested receipt date"),
            ("vendorName", "Vendor"),
            ("vendorNumber", "Vendor no."),
            ("payToName", "Pay-to"),
            ("shipToName", "Ship-to"),
            ("purchaser", "Purchaser code"),
            *_DOCUMENT_TOTALS,
            ("fullyReceived", "Fully received"),
        ),
    ),
    "purchaseInvoices": EntitySpec(
        entity_set="purchaseInvoices",
        display_name="Purchase invoices",
        singular="Purchase invoice",
        record_type="OTHERS",
        card_page=51,
        posted_card_page=138,
        party_field="vendorName",
        lines_property="purchaseInvoiceLines",
        line_columns=_PURCHASE_LINE_COLUMNS,
        summary_fields=(
            ("number", "No."),
            ("status", "Status"),
            ("invoiceDate", "Invoice date"),
            ("postingDate", "Posting date"),
            ("dueDate", "Due date"),
            ("vendorInvoiceNumber", "Vendor invoice no."),
            ("vendorName", "Vendor"),
            ("vendorNumber", "Vendor no."),
            ("orderNumber", "Order no."),
            ("payToName", "Pay-to"),
            ("shipToName", "Ship-to"),
            ("purchaser", "Purchaser code"),
            *_DOCUMENT_TOTALS,
        ),
    ),
}

# Canonical order used for filters and the default sync order (master data first so
# documents can reference already-indexed customers / vendors / items).
DEFAULT_ENTITY_ORDER: tuple[str, ...] = (
    "customers", "vendors", "items", "salesOrders", "salesInvoices", "purchaseOrders", "purchaseInvoices",
)

_ENTITY_BY_LOWER = {name.lower(): name for name in ENTITY_SPECS}


def resolve_selected_entities(values: Sequence[str] | None) -> list[EntitySpec]:
    """Entity specs selected by the ``entities`` sync filter.

    ``None`` / empty means "all supported entity sets".  Matching is case-insensitive,
    unknown names are ignored (never raise on stale filter values) and the order follows
    ``DEFAULT_ENTITY_ORDER``.
    """
    if not values:
        return [ENTITY_SPECS[name] for name in DEFAULT_ENTITY_ORDER]
    wanted = {_ENTITY_BY_LOWER.get(str(v).strip().lower()) for v in values if v}
    return [ENTITY_SPECS[name] for name in DEFAULT_ENTITY_ORDER if name in wanted]


# ---------------------------------------------------------------------------
# Companies and company scoping
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Company:
    """One row of ``GET companies``."""

    id: str
    name: str                    # technical name — what ``?company=`` in the web client expects
    display_name: str

    @property
    def label(self) -> str:
        return self.display_name or self.name


def parse_company(row: Mapping[str, Any]) -> Company | None:
    company_id = row.get("id")
    name = row.get("name")
    if not company_id or not name:
        return None
    return Company(id=str(company_id), name=str(name), display_name=str(row.get("displayName") or name))


def parse_companies(payload: Mapping[str, Any]) -> list[Company]:
    rows = payload.get("value")
    out: list[Company] = []
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, Mapping):
            company = parse_company(row)
            if company is not None:
                out.append(company)
    return out


def normalize_name(value: str) -> str:
    """Comparison key for company names: NFKC, case-folded, whitespace collapsed.

    Keeps every letter (Arabic included) — only case and spacing are neutralised, so
    ``"شركة  المثال"`` and ``"شركة المثال"`` compare equal while distinct names never merge.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return " ".join(text.split())


def parse_company_names(value: object) -> list[str]:
    """``companies`` auth field → distinct names/ids (comma-, semicolon- or newline-separated)."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        parts = [str(v) for v in value]
    else:
        parts = re.split(r"[,;\n]", str(value))
    return list(dict.fromkeys(p.strip() for p in parts if p and p.strip()))


@dataclass(frozen=True)
class CompanySelection:
    selected: tuple[Company, ...]
    unmatched: tuple[str, ...]  # configured names/ids no company matched (logged as warnings)


def select_companies(companies: Sequence[Company], wanted: Sequence[str]) -> CompanySelection:
    """Restrict the companies the app can see to the ones the admin configured.

    Empty ``wanted`` = every company.  Matching is by technical name, display name or id
    (``normalize_name`` semantics for names, exact for ids).  Order follows ``companies``.
    """
    if not wanted:
        return CompanySelection(tuple(companies), ())
    wanted_keys = {normalize_name(w): w for w in wanted}
    matched_keys: set[str] = set()
    selected: list[Company] = []
    for company in companies:
        keys = {normalize_name(company.name), normalize_name(company.display_name), company.id.lower()}
        hit = keys & wanted_keys.keys()
        if hit:
            matched_keys.update(hit)
            selected.append(company)
    unmatched = tuple(original for key, original in wanted_keys.items() if key not in matched_keys)
    return CompanySelection(tuple(selected), unmatched)


# ---------------------------------------------------------------------------
# Company access mapping (companyAccessGroups auth field)
# ---------------------------------------------------------------------------


@dataclass
class CompanyAccessMapping:
    """``company name (normalised) -> Entra group references`` plus the wildcard default.

    Accepted input shapes (auth field ``companyAccessGroups``):

    * text, one entry per line or ``;``: ``CRONUS SA = BC Readers, 3f2b...guid``
    * JSON object: ``{"CRONUS SA": ["BC Readers"], "*": "All Finance"}``

    ``*`` applies to every company without its own entry.  Companies with no entry (and
    no wildcard) are readable org-wide.
    """

    groups_by_company: dict[str, tuple[str, ...]] = field(default_factory=dict)
    default_groups: tuple[str, ...] = ()
    warnings: list[str] = field(default_factory=list)

    def groups_for(self, company: Company) -> tuple[str, ...]:
        for key in (normalize_name(company.name), normalize_name(company.display_name), company.id.lower()):
            if key in self.groups_by_company:
                return self.groups_by_company[key]
        return self.default_groups

    def referenced_groups(self) -> list[str]:
        refs: list[str] = list(self.default_groups)
        for groups in self.groups_by_company.values():
            refs.extend(groups)
        return list(dict.fromkeys(refs))

    def is_empty(self) -> bool:
        return not self.groups_by_company and not self.default_groups


def _split_group_refs(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set)):
        parts = [str(v) for v in value]
    else:
        parts = str(value).split(",")
    return tuple(dict.fromkeys(p.strip() for p in parts if p and p.strip()))


def parse_company_access_mapping(value: object) -> CompanyAccessMapping:
    """Parse the ``companyAccessGroups`` auth field; never raises (problems land in ``warnings``)."""
    mapping = CompanyAccessMapping()
    if value is None:
        return mapping
    entries: list[tuple[str, object]] = []
    if isinstance(value, Mapping):
        entries = [(str(k), v) for k, v in value.items()]
    else:
        text = str(value).strip()
        if not text:
            return mapping
        if text.startswith("{"):
            try:
                parsed = json.loads(text)
            except ValueError as e:
                mapping.warnings.append(f"companyAccessGroups is not valid JSON ({e}); ignoring it")
                return mapping
            if not isinstance(parsed, Mapping):
                mapping.warnings.append("companyAccessGroups JSON must be an object of company -> groups")
                return mapping
            entries = [(str(k), v) for k, v in parsed.items()]
        else:
            for raw_line in re.split(r"[\n;]", text):
                line = raw_line.strip()
                if not line:
                    continue
                company, sep, groups = line.partition("=")
                if not sep or not company.strip():
                    mapping.warnings.append(f"companyAccessGroups entry {line!r} is not 'Company = group[, group]'")
                    continue
                entries.append((company.strip(), groups))
    for company, groups in entries:
        refs = _split_group_refs(groups)
        if not refs:
            mapping.warnings.append(f"companyAccessGroups entry for {company!r} lists no group; company stays org-wide")
            continue
        if company.strip() == ACCESS_MAPPING_WILDCARD:
            mapping.default_groups = refs
        else:
            mapping.groups_by_company[normalize_name(company)] = refs
    return mapping


# ---------------------------------------------------------------------------
# External ids
# ---------------------------------------------------------------------------


def company_group_external_id(company_id: str) -> str:
    return f"{COMPANY_GROUP_PREFIX}{company_id}"


def record_external_id(company_id: str, spec: EntitySpec, row_id: str) -> str:
    """``bc:<companyId>:<entitySet>:<id>`` — unambiguous across companies and entity sets."""
    return f"{RECORD_ID_PREFIX}:{company_id}:{spec.entity_set}:{row_id}"


def record_id_prefix(company_id: str, spec: EntitySpec) -> str:
    return f"{RECORD_ID_PREFIX}:{company_id}:{spec.entity_set}:"


def split_external_id(external_id: str) -> tuple[str, EntitySpec, str]:
    """Inverse of ``record_external_id``; raises ``ValueError`` for foreign ids."""
    parts = str(external_id or "").split(":")
    if len(parts) != 4 or parts[0] != RECORD_ID_PREFIX or not all(parts[1:]):
        raise ValueError(f"Not a Business Central record id: {external_id!r}")
    spec = ENTITY_SPECS.get(parts[2])
    if spec is None:
        raise ValueError(f"Unknown Business Central entity set in {external_id!r}")
    return parts[1], spec, parts[3]


# ---------------------------------------------------------------------------
# URLs and query options
# ---------------------------------------------------------------------------


def normalize_environment_name(value: object) -> str:
    text = str(value or "").strip().strip("/")
    return text or DEFAULT_ENVIRONMENT


def normalize_tenant_id(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("tenantId is required")
    return text


def api_base_url(tenant_id: str, environment_name: str) -> str:
    return f"{BC_API_HOST}/{BC_API_VERSION}/{quote(tenant_id, safe='')}/{quote(environment_name, safe='')}/api/v2.0/"


def token_url(tenant_id: str) -> str:
    return f"{LOGIN_HOST}/{quote(tenant_id, safe='')}/oauth2/v2.0/token"


def company_path(company_id: str, spec: EntitySpec) -> str:
    return f"companies({company_id})/{spec.entity_set}"


def entity_path(company_id: str, spec: EntitySpec, row_id: str) -> str:
    return f"companies({company_id})/{spec.entity_set}({row_id})"


def build_page_params(spec: EntitySpec, odata_filter: str | None, top: int = DOCUMENT_PAGE_SIZE) -> dict[str, str]:
    """Query options for one document page (lines expanded when the entity has them)."""
    params: dict[str, str] = {"$top": str(top)}
    if spec.lines_property:
        params["$expand"] = spec.lines_property
    if odata_filter:
        params["$filter"] = odata_filter
    return params


def build_key_page_params(top: int = KEY_PAGE_SIZE) -> dict[str, str]:
    """Query options for one *key-only* page (reconcile): ids only, no lines."""
    return {"$select": "id", "$top": str(top)}


def build_single_params(spec: EntitySpec) -> dict[str, str]:
    return {"$expand": spec.lines_property} if spec.lines_property else {}


def company_web_url(tenant_id: str, environment_name: str, company_name: str) -> str:
    return f"{BC_WEB_HOST}/{quote(tenant_id, safe='')}/{quote(environment_name, safe='')}?company={quote(company_name, safe='')}"


def is_posted(spec: EntitySpec, row: Mapping[str, Any]) -> bool:
    if spec.posted_card_page is None:
        return False
    status = str(row.get("status") or "").strip().lower()
    return bool(status) and status not in _UNPOSTED_STATUSES


def record_web_url(tenant_id: str, environment_name: str, company_name: str, spec: EntitySpec, row: Mapping[str, Any]) -> str:
    """Deep link into the web client: company + card page + a ``No.`` filter on the document number."""
    page = spec.posted_card_page if is_posted(spec, row) else spec.card_page
    url = f"{company_web_url(tenant_id, environment_name, company_name)}&page={page}"
    number = row.get("number")
    if number not in (None, ""):
        filter_expr = f"'No.' IS '{str(number).replace(chr(39), chr(39) * 2)}'"
        url += f"&filter={quote(filter_expr, safe='')}"
    return url


# ---------------------------------------------------------------------------
# Timestamps and OData filters
# ---------------------------------------------------------------------------


def parse_bc_timestamp(value: object) -> int | None:
    """``2026-02-03T04:05:06.1234567Z`` (any fraction length) → epoch ms; sentinels → ``None``."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text.startswith(_EMPTY_DATE_PREFIX):
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    text = _FRACTION_RE.sub(lambda m: "." + m.group(1)[:6], text, count=1)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_odata(epoch_ms: int) -> str:
    """``Edm.DateTimeOffset`` literal accepted by Business Central ``$filter`` (second precision)."""
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_modified_filter(
    since_ms: int | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> str | None:
    """``$filter`` on ``lastModifiedDateTime`` from the incremental sync point (``since_ms``,
    exclusive) and the user's modified-date filter (inclusive bounds).  ``None`` = full read."""
    lower = max(v for v in (since_ms, start_ms) if v is not None) if (since_ms or start_ms) else None
    clauses: list[str] = []
    if lower is not None:
        op = "gt" if since_ms is not None and lower == since_ms else "ge"
        clauses.append(f"{MODIFIED_FIELD} {op} {epoch_ms_to_odata(lower)}")
    if end_ms is not None:
        clauses.append(f"{MODIFIED_FIELD} le {epoch_ms_to_odata(end_ms)}")
    return " and ".join(clauses) if clauses else None


# ---------------------------------------------------------------------------
# Paging
# ---------------------------------------------------------------------------


@dataclass
class Page:
    rows: list[dict[str, Any]] = field(default_factory=list)
    next_link: str | None = None


def parse_page(payload: Mapping[str, Any]) -> Page:
    """OData v4 collection response → rows (dicts with an ``id``) and the ``@odata.nextLink``."""
    page = Page()
    rows = payload.get("value")
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, Mapping) and row.get("id"):
            page.rows.append(dict(row))
    next_link = payload.get("@odata.nextLink")
    page.next_link = str(next_link) if next_link else None
    return page


# ---------------------------------------------------------------------------
# Sync state and deletion reconcile
# ---------------------------------------------------------------------------


@dataclass
class EntitySyncState:
    """Per (company, entity set) sync state persisted under ``records/entity/<companyId>/<entitySet>``."""

    last_sync_timestamp: int | None = None
    last_reconcile_timestamp: int | None = None

    @classmethod
    def from_sync_point(cls, data: Mapping[str, object] | None) -> EntitySyncState:
        data = data or {}
        return cls(
            last_sync_timestamp=_as_int(data.get(FIELD_LAST_SYNC)),
            last_reconcile_timestamp=_as_int(data.get(FIELD_LAST_RECONCILE)),
        )

    def to_sync_point(self) -> dict[str, object]:
        return {FIELD_LAST_SYNC: self.last_sync_timestamp, FIELD_LAST_RECONCILE: self.last_reconcile_timestamp}


class ReconcileMode(str, Enum):
    NONE = "none"          # not due (or disabled)
    SEEN = "seen"          # the pull just read the whole entity set: diff against what it saw
    KEY_SET = "key_set"    # pull ``$select=id`` and diff


def plan_reconcile(
    state: EntitySyncState,
    *,
    full_read: bool,
    now_ms: int,
    interval_hours: float,
) -> ReconcileMode:
    """Decide how deletions are detected for one (company, entity set) this run.

    * a complete read (full sync, or an incremental run without a stored sync point and
      no modified window) already knows every live id → ``SEEN`` (free);
    * otherwise a key-set pull when ``reconcile_interval_hours`` elapsed since the last one;
    * ``interval_hours <= 0`` disables the reconcile entirely (deletes are never detected).
    """
    if interval_hours <= 0:
        return ReconcileMode.NONE
    if full_read:
        return ReconcileMode.SEEN
    return ReconcileMode.KEY_SET if reconcile_due(state.last_reconcile_timestamp, now_ms, interval_hours) else ReconcileMode.NONE


@dataclass(frozen=True)
class ReconcilePlan:
    known: int
    live: int
    delete_record_ids: tuple[str, ...]
    skipped_reason: str | None = None


def diff_known_against_live(known: Mapping[str, str], live: Iterable[str]) -> ReconcilePlan:
    """``known`` = ``{external_record_id: graph record id}`` for one (company, entity set);
    ``live`` = external ids Business Central still returns.  Safety guard: an empty live set
    while records are known is treated as a failed pull, not as "everything was deleted"."""
    live_set = set(live)
    if known and not live_set:
        return ReconcilePlan(len(known), 0, (), "Business Central returned no rows; refusing to delete every record")
    missing = tuple(record_id for external_id, record_id in known.items() if external_id not in live_set)
    return ReconcilePlan(len(known), len(live_set), missing)


def seen_external_ids(company_id: str, spec: EntitySpec, rows: Iterable[Mapping[str, Any]]) -> set[str]:
    return {record_external_id(company_id, spec, str(row["id"])) for row in rows if row.get("id")}


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def display_value(row: Mapping[str, Any], attribute: str) -> str | None:
    """Human-readable cell value; BC sentinels (empty dates, null GUIDs) collapse to ``None``."""
    raw = row.get(attribute)
    if raw in (None, ""):
        return None
    if isinstance(raw, bool):
        return "Yes" if raw else "No"
    if isinstance(raw, float):
        return str(int(raw)) if raw.is_integer() else f"{raw:g}" if abs(raw) >= 1e15 else str(raw)
    if isinstance(raw, (int,)):
        return str(raw)
    if isinstance(raw, (list, dict)):
        return None
    text = str(raw).strip()
    if not text or text.startswith(_EMPTY_DATE_PREFIX) or text == _EMPTY_GUID:
        return None
    return text


def record_title(spec: EntitySpec, row: Mapping[str, Any]) -> str:
    """Master data: ``<displayName> (<No.>)``; documents: ``<Singular> <No.> · <party>``."""
    number = display_value(row, "number")
    if spec.is_master_data:
        name = display_value(row, "displayName")
        if name and number:
            return f"{name} ({number})"
        if name:
            return name
        return f"{spec.singular} {number or row.get('id', '')}".strip()
    title = f"{spec.singular} {number}" if number else f"{spec.singular} {row.get('id', '')}".strip()
    party = display_value(row, spec.party_field) if spec.party_field else None
    return f"{title} · {party}" if party else title


def _markdown_cell(value: str | None) -> str:
    if not value:
        return ""
    return value.replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def render_lines_table(spec: EntitySpec, row: Mapping[str, Any]) -> list[str]:
    """Markdown table of the expanded document lines (empty list when there are none)."""
    if not spec.lines_property or not spec.line_columns:
        return []
    lines = row.get(spec.lines_property)
    if not isinstance(lines, list) or not lines:
        return []
    header = "| " + " | ".join(label for _, label in spec.line_columns) + " |"
    separator = "|" + "|".join("---" for _ in spec.line_columns) + "|"
    out = ["## Lines", header, separator]
    for line in lines:
        if not isinstance(line, Mapping):
            continue
        cells = [_markdown_cell(display_value(line, attr)) for attr, _ in spec.line_columns]
        out.append("| " + " | ".join(cells) + " |")
    out.append("")
    return out


def build_metadata(
    company: Company,
    spec: EntitySpec,
    row: Mapping[str, Any],
    tenant_id: str,
    environment_name: str,
) -> dict[str, Any]:
    """Raw identifiers kept alongside the rendered content (embedded in the markdown footer)."""
    row_id = str(row.get("id") or "")
    return {
        "source": "microsoft-business-central",
        "tenant_id": tenant_id,
        "environment": environment_name,
        "company_id": company.id,
        "company_name": company.label,
        "entity_set": spec.entity_set,
        "id": row_id,
        "number": row.get("number"),
        "status": row.get("status"),
        "last_modified": row.get(MODIFIED_FIELD),
        "url": record_web_url(tenant_id, environment_name, company.name, spec, row) if row_id else None,
    }


def render_record_markdown(
    company: Company,
    spec: EntitySpec,
    row: Mapping[str, Any],
    tenant_id: str,
    environment_name: str,
) -> tuple[str, dict[str, Any]]:
    """Readable markdown for chunking/embedding + the raw-id metadata dict."""
    metadata = build_metadata(company, spec, row, tenant_id, environment_name)
    lines: list[str] = [f"# {record_title(spec, row)}", ""]

    header_bits = [f"**Type:** Business Central {spec.singular}", f"**Company:** {company.label}"]
    status = display_value(row, "status")
    if status:
        header_bits.append(f"**Status:** {status}")
    lines.append(" · ".join(header_bits))
    lines.append("")

    details: list[str] = []
    for attribute, label in spec.summary_fields:
        if attribute == "status":
            continue  # already in the header
        value = display_value(row, attribute)
        if value:
            details.append(f"- **{label}:** {value}")
    if details:
        lines.append("## Details")
        lines.extend(details)
        lines.append("")

    if spec.description_field:
        description = display_value(row, spec.description_field)
        if description:
            lines.extend(["## Description", description, ""])

    lines.extend(render_lines_table(spec, row))

    lines.append("## Source metadata")
    lines.append(f"- Business Central company: {company.label} ({company.id})")
    lines.append(f"- Entity set: {spec.entity_set}")
    lines.append(f"- Record id: {metadata['id']}")
    modified = display_value(row, MODIFIED_FIELD)
    if modified:
        lines.append(f"- Modified: {modified}")
    if metadata["url"]:
        lines.append(f"- URL: {metadata['url']}")
    return "\n".join(lines).rstrip() + "\n", metadata


# ---------------------------------------------------------------------------
# Permission derivation
# ---------------------------------------------------------------------------


class GrantRole(str, Enum):
    READER = "READER"


class GrantEntity(str, Enum):
    GROUP = "GROUP"
    ORG = "ORG"


@dataclass(frozen=True)
class PermissionGrant:
    """Connector-agnostic permission; ``connector.py`` turns it into ``Permission``."""

    entity_type: GrantEntity
    role: GrantRole
    external_id: str | None = None
    reason: str = field(default="", compare=False)


@dataclass(frozen=True)
class CompanyAccess:
    """Resolved access of one company: the Entra groups that gate it (empty = org-wide)."""

    company: Company
    group_refs: tuple[str, ...] = ()

    @property
    def org_wide(self) -> bool:
        return not self.group_refs


def resolve_company_access(companies: Sequence[Company], mapping: CompanyAccessMapping) -> list[CompanyAccess]:
    return [CompanyAccess(company=c, group_refs=mapping.groups_for(c)) for c in companies]


def company_grants(access: CompanyAccess) -> list[PermissionGrant]:
    """Grants applied to the company's record group and to every record in it (see module docstring)."""
    grants = [
        PermissionGrant(
            GrantEntity.GROUP, GrantRole.READER,
            external_id=company_group_external_id(access.company.id), reason="company access group",
        )
    ]
    if access.org_wide:
        grants.append(PermissionGrant(GrantEntity.ORG, GrantRole.READER, reason="no companyAccessGroups entry"))
    return grants


def company_group_name(company: Company) -> str:
    return f"Business Central · {company.label}"


def is_guid(value: str) -> bool:
    return bool(_GUID_RE.match(str(value or "").strip()))


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
