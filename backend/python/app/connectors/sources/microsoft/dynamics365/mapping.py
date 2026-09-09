"""Pure helpers for the Microsoft Dynamics 365 (Dataverse) connector.

Everything in this module is deliberately **standard-library only** and free of
I/O so the entity mapping, record rendering and the permission (RBAC) derivation
can be unit-tested without the connector's runtime dependencies
(``azure-identity``, ``httpx``, pydantic models).  ``connector.py`` converts the
plain dataclasses produced here into ``Record`` / ``Permission`` objects.

Permission model (how Dataverse security becomes CGraph permission edges)
=========================================================================

Dataverse decides whether a user can read a row from five sources.  The
connector approximates each with an edge that ``DataSourceEntitiesProcessor``
already understands (``USER`` by email, ``GROUP`` / ``ROLE`` by external id):

+--------------------------------------+----------------------------------------------+---------+
| Dataverse source                     | CGraph principal                             | Role    |
+======================================+==============================================+=========+
| ``_owninguser_value`` (owner = user) | USER (systemuser -> email)                   | OWNER   |
| ``_owningteam_value`` (owner = team) | GROUP ``team:<teamid>``                      | OWNER   |
| ``_owningbusinessunit_value``        | GROUP ``bu:<businessunitid>`` (users in BU)  | READER  |
| security role whose                  | ROLE ``role:<parentrootroleid>`` (all users  | READER  |
| ``prvRead<Entity>`` depth == Global  | holding any BU copy of that role, directly   |         |
|                                      | or through a team)                           |         |
| ``System Administrator`` role        | ROLE (always granted, on every record)       | READER  |
| ``principalobjectaccess`` share      | USER / GROUP ``team:<id>`` per principal     | READER  |
|                                      | (WRITER when the mask has the Write bit)     | /WRITER |
+--------------------------------------+----------------------------------------------+---------+

Known approximations (documented gaps):

* Business-unit *Deep* / *Local* read depth is collapsed to "members of the
  owning BU can read" — child-BU depth (Deep) is not walked.
* *Basic* (user-level) depth is covered by the owner edge only.
* Hierarchy security (manager / position) and field-level security are ignored.
* Access teams are only honoured when they show up as ``principalobjectaccess``
  rows (which is how Dataverse implements them).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Sequence

# ---------------------------------------------------------------------------
# Dataverse constants
# ---------------------------------------------------------------------------

DATAVERSE_API_VERSION = "v9.2"
DATAVERSE_PAGE_SIZE = 5000  # Dataverse hard cap per page (Prefer: odata.maxpagesize)
# ``<pk> eq <guid> or ...`` clauses per request; keeps the URL well under Dataverse's limit.
PRIMARY_ID_FILTER_BATCH_SIZE = 25
SHARE_DIGEST_HEX_CHARS = 12

_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")

FORMATTED_VALUE_SUFFIX = "@OData.Community.Display.V1.FormattedValue"
LOOKUP_LOGICALNAME_SUFFIX = "@Microsoft.Dynamics.CRM.lookuplogicalname"

# principalobjectaccess.principaltypecode
PRINCIPAL_TYPE_SYSTEMUSER = 8
PRINCIPAL_TYPE_TEAM = 9

# Dataverse AccessRights bitmask (principalobjectaccess.accessrightsmask)
ACCESS_READ = 1
ACCESS_WRITE = 2
ACCESS_APPEND = 4
ACCESS_APPEND_TO = 16
ACCESS_CREATE = 32
ACCESS_DELETE = 65536
ACCESS_SHARE = 262144
ACCESS_ASSIGN = 524288

PRIVILEGE_DEPTH_GLOBAL = "Global"
SYSTEM_ADMINISTRATOR_ROLE_NAME = "System Administrator"

# statecode values shared by opportunity / incident / lead
STATE_ACTIVE = 0
STATE_WON_OR_RESOLVED = 1
STATE_LOST_OR_CANCELLED = 2

ENTITIES_FILTER_KEY = "entities"

# Prefixes keep GROUP / ROLE external ids unambiguous and greppable in the graph.
TEAM_GROUP_PREFIX = "team:"
BU_GROUP_PREFIX = "bu:"
ROLE_PREFIX = "role:"
ATTACHMENT_ID_PREFIX = "annotation-file"


# ---------------------------------------------------------------------------
# Entity registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntitySpec:
    """Static description of one Dataverse table the connector indexes."""

    logical_name: str          # "opportunity"  (used in main.aspx?etn=)
    entity_set: str            # "opportunities" (Web API collection)
    display_name: str          # "Opportunities" (record group label)
    singular: str              # "Opportunity" (used in rendered headers/titles)
    primary_id: str            # "opportunityid"
    primary_name: str          # attribute used as the record title
    record_type: str           # ``RecordType`` value
    record_group_type: str     # ``RecordGroupType`` value
    privilege_entity: str      # "Opportunity" -> prvReadOpportunity
    select_fields: tuple[str, ...]
    # (attribute, label) pairs rendered in the "Details" section, in order.
    summary_fields: tuple[tuple[str, str], ...]
    description_field: Optional[str] = "description"


_OWNERSHIP_FIELDS: tuple[str, ...] = (
    "_ownerid_value",
    "_owninguser_value",
    "_owningteam_value",
    "_owningbusinessunit_value",
    "createdon",
    "modifiedon",
    "statecode",
    "statuscode",
)

ENTITY_SPECS: dict[str, EntitySpec] = {
    "account": EntitySpec(
        logical_name="account",
        entity_set="accounts",
        display_name="Accounts",
        singular="Account",
        primary_id="accountid",
        primary_name="name",
        record_type="OTHERS",
        record_group_type="CRM_ENTITY",
        privilege_entity="Account",
        select_fields=(
            "accountid", "name", "accountnumber", "telephone1", "emailaddress1",
            "websiteurl", "address1_composite", "industrycode", "revenue",
            "numberofemployees", "description", "_primarycontactid_value",
            "_parentaccountid_value",
        ) + _OWNERSHIP_FIELDS,
        summary_fields=(
            ("accountnumber", "Account number"),
            ("statuscode", "Status"),
            ("industrycode", "Industry"),
            ("revenue", "Annual revenue"),
            ("numberofemployees", "Employees"),
            ("_primarycontactid_value", "Primary contact"),
            ("_parentaccountid_value", "Parent account"),
            ("telephone1", "Phone"),
            ("emailaddress1", "Email"),
            ("websiteurl", "Website"),
            ("address1_composite", "Address"),
        ),
    ),
    "contact": EntitySpec(
        logical_name="contact",
        entity_set="contacts",
        display_name="Contacts",
        singular="Contact",
        primary_id="contactid",
        primary_name="fullname",
        record_type="OTHERS",
        record_group_type="CRM_ENTITY",
        privilege_entity="Contact",
        select_fields=(
            "contactid", "fullname", "firstname", "lastname", "jobtitle",
            "emailaddress1", "telephone1", "mobilephone", "address1_composite",
            "description", "_parentcustomerid_value",
        ) + _OWNERSHIP_FIELDS,
        summary_fields=(
            ("statuscode", "Status"),
            ("jobtitle", "Job title"),
            ("_parentcustomerid_value", "Company"),
            ("emailaddress1", "Email"),
            ("telephone1", "Phone"),
            ("mobilephone", "Mobile"),
            ("address1_composite", "Address"),
        ),
    ),
    "lead": EntitySpec(
        logical_name="lead",
        entity_set="leads",
        display_name="Leads",
        singular="Lead",
        primary_id="leadid",
        primary_name="subject",
        record_type="OTHERS",
        record_group_type="CRM_ENTITY",
        privilege_entity="Lead",
        select_fields=(
            "leadid", "subject", "fullname", "companyname", "jobtitle",
            "emailaddress1", "telephone1", "leadsourcecode", "leadqualitycode",
            "estimatedvalue", "estimatedclosedate", "description",
            "_parentaccountid_value", "_parentcontactid_value",
        ) + _OWNERSHIP_FIELDS,
        summary_fields=(
            ("statuscode", "Status"),
            ("fullname", "Contact"),
            ("companyname", "Company"),
            ("jobtitle", "Job title"),
            ("leadsourcecode", "Lead source"),
            ("leadqualitycode", "Rating"),
            ("estimatedvalue", "Estimated value"),
            ("estimatedclosedate", "Estimated close date"),
            ("emailaddress1", "Email"),
            ("telephone1", "Phone"),
        ),
    ),
    "opportunity": EntitySpec(
        logical_name="opportunity",
        entity_set="opportunities",
        display_name="Opportunities",
        singular="Opportunity",
        primary_id="opportunityid",
        primary_name="name",
        record_type="DEAL",
        record_group_type="DEAL",
        privilege_entity="Opportunity",
        select_fields=(
            "opportunityid", "name", "description", "estimatedvalue", "actualvalue",
            "budgetamount", "estimatedclosedate", "actualclosedate",
            "closeprobability", "salesstage", "stepname", "_customerid_value",
            "_parentaccountid_value", "_parentcontactid_value",
        ) + _OWNERSHIP_FIELDS,
        summary_fields=(
            ("statuscode", "Status"),
            ("salesstage", "Sales stage"),
            ("stepname", "Pipeline step"),
            ("_customerid_value", "Customer"),
            ("_parentaccountid_value", "Account"),
            ("_parentcontactid_value", "Contact"),
            ("estimatedvalue", "Estimated value"),
            ("actualvalue", "Actual value"),
            ("budgetamount", "Budget"),
            ("closeprobability", "Probability (%)"),
            ("estimatedclosedate", "Estimated close date"),
            ("actualclosedate", "Actual close date"),
        ),
    ),
    "incident": EntitySpec(
        logical_name="incident",
        entity_set="incidents",
        display_name="Cases",
        singular="Case",
        primary_id="incidentid",
        primary_name="title",
        record_type="CASE",
        record_group_type="CASE",
        privilege_entity="Incident",
        select_fields=(
            "incidentid", "title", "ticketnumber", "description", "prioritycode",
            "severitycode", "caseorigincode", "casetypecode", "resolveby",
            "_customerid_value", "_primarycontactid_value", "_subjectid_value",
        ) + _OWNERSHIP_FIELDS,
        summary_fields=(
            ("ticketnumber", "Case number"),
            ("statuscode", "Status"),
            ("prioritycode", "Priority"),
            ("severitycode", "Severity"),
            ("casetypecode", "Case type"),
            ("caseorigincode", "Origin"),
            ("_customerid_value", "Customer"),
            ("_primarycontactid_value", "Contact"),
            ("_subjectid_value", "Subject"),
            ("resolveby", "Resolve by"),
        ),
    ),
    "annotation": EntitySpec(
        logical_name="annotation",
        entity_set="annotations",
        display_name="Notes",
        singular="Note",
        primary_id="annotationid",
        primary_name="subject",
        record_type="OTHERS",
        record_group_type="CRM_ENTITY",
        privilege_entity="Note",  # the privilege is prvReadNote, not prvReadAnnotation
        select_fields=(
            "annotationid", "subject", "notetext", "isdocument", "filename",
            "filesize", "mimetype", "objecttypecode", "_objectid_value",
        ) + _OWNERSHIP_FIELDS[:6],  # annotations have no statecode/statuscode
        summary_fields=(
            ("objecttypecode", "Attached to (entity)"),
            ("_objectid_value", "Attached to"),
            ("filename", "Attachment"),
            ("mimetype", "Attachment type"),
        ),
        description_field="notetext",
    ),
}

# Canonical order used for filters and the default sync order.
DEFAULT_ENTITY_ORDER: tuple[str, ...] = (
    "account", "contact", "lead", "opportunity", "incident", "annotation",
)


def resolve_selected_entities(values: Optional[Sequence[str]]) -> list[EntitySpec]:
    """Return the entity specs selected by the ``entities`` sync filter.

    ``None`` / empty means "all supported entities".  Unknown names are ignored
    (never raise on stale filter values), order follows ``DEFAULT_ENTITY_ORDER``.
    """
    if not values:
        return [ENTITY_SPECS[name] for name in DEFAULT_ENTITY_ORDER]
    wanted = {str(v).strip().lower() for v in values if v}
    return [ENTITY_SPECS[name] for name in DEFAULT_ENTITY_ORDER if name in wanted]


def read_privilege_name(spec: EntitySpec) -> str:
    """``prvRead<Entity>`` — the Dataverse privilege that gates reading rows."""
    return f"prvRead{spec.privilege_entity}"


def role_has_global_read(role_privileges: Iterable[Mapping[str, Any]], spec: EntitySpec) -> bool:
    """True when a ``RetrieveRolePrivilegesRole`` payload grants org-wide read on ``spec``.

    Each item looks like ``{"PrivilegeName": "prvReadOpportunity", "Depth": "Global", ...}``.
    """
    wanted = read_privilege_name(spec)
    for priv in role_privileges:
        if str(priv.get("PrivilegeName") or "") == wanted and str(priv.get("Depth") or "") == PRIVILEGE_DEPTH_GLOBAL:
            return True
    return False


# ---------------------------------------------------------------------------
# External ids
# ---------------------------------------------------------------------------


def team_group_external_id(team_id: str) -> str:
    return f"{TEAM_GROUP_PREFIX}{team_id}"


def bu_group_external_id(business_unit_id: str) -> str:
    return f"{BU_GROUP_PREFIX}{business_unit_id}"


def role_external_id(role_row: Mapping[str, Any]) -> str:
    """Roles exist once per business unit; collapse the copies onto the root role."""
    root = role_row.get("_parentrootroleid_value") or role_row.get("roleid")
    return f"{ROLE_PREFIX}{root}"


def record_external_id(spec: EntitySpec, row_id: str) -> str:
    """``<logical_name>:<guid>`` — unambiguous across tables and easy to split."""
    return f"{spec.logical_name}:{row_id}"


def attachment_external_id(annotation_id: str) -> str:
    return f"{ATTACHMENT_ID_PREFIX}:{annotation_id}"


def split_external_id(external_id: str) -> tuple[str, str]:
    """Inverse of ``record_external_id`` / ``attachment_external_id``."""
    kind, _, guid = external_id.partition(":")
    if not guid:
        raise ValueError(f"Not a Dynamics 365 external id: {external_id!r}")
    return kind, guid


# ---------------------------------------------------------------------------
# URLs, timestamps, OData helpers
# ---------------------------------------------------------------------------


def normalize_environment_url(url: str) -> str:
    """``https://org.crm4.dynamics.com`` — scheme added, trailing slashes removed."""
    value = (url or "").strip().rstrip("/")
    if not value:
        raise ValueError("environmentUrl is required")
    if not value.lower().startswith(("https://", "http://")):
        value = f"https://{value}"
    return value


def token_scope(environment_url: str) -> str:
    return f"{normalize_environment_url(environment_url)}/.default"


def api_base_url(environment_url: str) -> str:
    return f"{normalize_environment_url(environment_url)}/api/data/{DATAVERSE_API_VERSION}/"


def record_web_url(environment_url: str, spec: EntitySpec, row_id: str) -> str:
    return (
        f"{normalize_environment_url(environment_url)}/main.aspx"
        f"?etn={spec.logical_name}&id={row_id}&pagetype=entityrecord"
    )


def entity_list_web_url(environment_url: str, spec: EntitySpec) -> str:
    return f"{normalize_environment_url(environment_url)}/main.aspx?etn={spec.logical_name}&pagetype=entitylist"


def parse_dataverse_timestamp(value: Any) -> Optional[int]:
    """Dataverse returns ISO-8601 UTC (``2024-05-01T10:15:30Z``); return epoch ms."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_odata(epoch_ms: int) -> str:
    """OData ``Edm.DateTimeOffset`` literal accepted by Dataverse ``$filter``."""
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_modified_filter(
    since_ms: Optional[int] = None,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
) -> Optional[str]:
    """Compose the ``$filter`` for ``modifiedon`` from the incremental sync point
    (``since_ms``, exclusive) and the user's modified-date filter (inclusive bounds)."""
    lower = max(v for v in (since_ms, start_ms) if v is not None) if (since_ms or start_ms) else None
    clauses: list[str] = []
    if lower is not None:
        op = "gt" if since_ms is not None and lower == since_ms else "ge"
        clauses.append(f"modifiedon {op} {epoch_ms_to_odata(lower)}")
    if end_ms is not None:
        clauses.append(f"modifiedon le {epoch_ms_to_odata(end_ms)}")
    return " and ".join(clauses) if clauses else None


def build_primary_id_filter(spec: EntitySpec, row_ids: Iterable[str]) -> str | None:
    """``$filter`` selecting rows by primary key (Edm.Guid literals are unquoted).
    Non-GUID values are dropped so nothing but a key can reach the query."""
    guids = [str(v) for v in row_ids if _GUID_RE.match(str(v or ""))]
    if not guids:
        return None
    return " or ".join(f"{spec.primary_id} eq {guid}" for guid in guids)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def display_value(row: Mapping[str, Any], attribute: str) -> Optional[str]:
    """Prefer the OData formatted value (option-set label, lookup name, currency)."""
    formatted = row.get(f"{attribute}{FORMATTED_VALUE_SUFFIX}")
    if formatted not in (None, ""):
        return str(formatted)
    raw = row.get(attribute)
    if raw in (None, ""):
        return None
    if isinstance(raw, bool):
        return "Yes" if raw else "No"
    if isinstance(raw, float) and raw.is_integer():
        return str(int(raw))
    return str(raw)


def record_title(spec: EntitySpec, row: Mapping[str, Any]) -> str:
    title = display_value(row, spec.primary_name)
    if title:
        return title
    if spec.logical_name == "annotation":
        return display_value(row, "filename") or "Note"
    if spec.logical_name == "lead":
        return display_value(row, "fullname") or display_value(row, "companyname") or "Lead"
    return f"{spec.singular} {row.get(spec.primary_id, '')}".strip()


def owner_reference(row: Mapping[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Return ``(owner_id, owner_kind)`` where kind is ``systemuser`` or ``team``."""
    if row.get("_owninguser_value"):
        return str(row["_owninguser_value"]), "systemuser"
    if row.get("_owningteam_value"):
        return str(row["_owningteam_value"]), "team"
    owner_id = row.get("_ownerid_value")
    if not owner_id:
        return None, None
    kind = row.get(f"_ownerid_value{LOOKUP_LOGICALNAME_SUFFIX}")
    return str(owner_id), (str(kind) if kind else None)


def build_metadata(spec: EntitySpec, row: Mapping[str, Any], environment_url: str) -> dict[str, Any]:
    """Raw identifiers kept alongside the rendered content (embedded in the
    markdown footer; ``Record`` has no free-form metadata slot)."""
    row_id = str(row.get(spec.primary_id) or "")
    owner_id, owner_kind = owner_reference(row)
    return {
        "source": "microsoft-dynamics-365",
        "entity": spec.logical_name,
        "entity_set": spec.entity_set,
        "id": row_id,
        "owner_id": owner_id,
        "owner_type": owner_kind,
        "owning_business_unit_id": row.get("_owningbusinessunit_value"),
        "statecode": row.get("statecode"),
        "statuscode": row.get("statuscode"),
        "createdon": row.get("createdon"),
        "modifiedon": row.get("modifiedon"),
        "url": record_web_url(environment_url, spec, row_id) if row_id else None,
    }


def render_record_markdown(spec: EntitySpec, row: Mapping[str, Any], environment_url: str) -> tuple[str, dict[str, Any]]:
    """Readable markdown for chunking/embedding + the raw-id metadata dict."""
    metadata = build_metadata(spec, row, environment_url)
    title = record_title(spec, row)

    lines: list[str] = [f"# {title}", ""]
    header_bits = [f"**Type:** Dynamics 365 {spec.singular}"]
    status = display_value(row, "statuscode") or display_value(row, "statecode")
    if status:
        header_bits.append(f"**Status:** {status}")
    owner = display_value(row, "_ownerid_value")
    if owner:
        header_bits.append(f"**Owner:** {owner}")
    lines.append(" · ".join(header_bits))
    lines.append("")

    details: list[str] = []
    for attribute, label in spec.summary_fields:
        if attribute == "statuscode":
            continue  # already in the header
        value = display_value(row, attribute)
        if value:
            details.append(f"- **{label}:** {value}")
    if details:
        lines.append("## Details")
        lines.extend(details)
        lines.append("")

    if spec.description_field:
        description = row.get(spec.description_field)
        if description:
            lines.append("## Description" if spec.description_field == "description" else "## Note")
            lines.append(str(description).strip())
            lines.append("")

    lines.append("## Source metadata")
    lines.append(f"- Dynamics 365 entity: {spec.logical_name}")
    lines.append(f"- Record id: {metadata['id']}")
    if metadata["owner_id"]:
        lines.append(f"- Owner id: {metadata['owner_id']} ({metadata['owner_type'] or 'unknown'})")
    if metadata["owning_business_unit_id"]:
        bu_name = display_value(row, "_owningbusinessunit_value")
        lines.append(f"- Business unit: {bu_name or metadata['owning_business_unit_id']}")
    created = display_value(row, "createdon")
    modified = display_value(row, "modifiedon")
    if created or modified:
        lines.append(f"- Created: {created or 'n/a'} · Modified: {modified or 'n/a'}")
    if metadata["url"]:
        lines.append(f"- URL: {metadata['url']}")
    return "\n".join(lines).rstrip() + "\n", metadata


# ---------------------------------------------------------------------------
# RBAC derivation
# ---------------------------------------------------------------------------


class GrantRole(str, Enum):
    READER = "READER"
    WRITER = "WRITER"
    OWNER = "OWNER"


_ROLE_RANK = {GrantRole.READER: 1, GrantRole.WRITER: 2, GrantRole.OWNER: 3}


class GrantEntity(str, Enum):
    USER = "USER"
    GROUP = "GROUP"
    ROLE = "ROLE"


@dataclass(frozen=True)
class PermissionGrant:
    """Connector-agnostic permission; ``connector.py`` turns it into ``Permission``."""

    entity_type: GrantEntity
    role: GrantRole
    external_id: Optional[str] = None  # GROUP / ROLE external id
    email: Optional[str] = None        # USER
    reason: str = field(default="", compare=False)


@dataclass(frozen=True)
class ShareEntry:
    """One ``principalobjectaccess`` row."""

    principal_id: str
    principal_type: int
    access_mask: int


@dataclass
class SecurityContext:
    """Everything derived from the security tables at the start of a sync."""

    user_email_by_id: dict[str, str] = field(default_factory=dict)
    known_team_ids: set[str] = field(default_factory=set)
    known_business_unit_ids: set[str] = field(default_factory=set)
    # entity logical name -> role external ids (``role:<root>``) with Global read
    global_read_roles_by_entity: dict[str, set[str]] = field(default_factory=dict)
    system_admin_role_id: Optional[str] = None
    # entity logical name -> object id -> shares
    shares_by_entity: dict[str, dict[str, list[ShareEntry]]] = field(default_factory=dict)

    def entity_role_grants(self, spec: EntitySpec) -> list[PermissionGrant]:
        """ROLE grants that apply to every row of ``spec`` (used for records
        and for the entity's record group)."""
        grants: list[PermissionGrant] = []
        seen: set[str] = set()
        if self.system_admin_role_id:
            seen.add(self.system_admin_role_id)
            grants.append(PermissionGrant(GrantEntity.ROLE, GrantRole.READER, external_id=self.system_admin_role_id, reason="System Administrator"))
        for role_id in sorted(self.global_read_roles_by_entity.get(spec.logical_name, ())):
            if role_id in seen:
                continue
            seen.add(role_id)
            grants.append(PermissionGrant(GrantEntity.ROLE, GrantRole.READER, external_id=role_id, reason=f"{read_privilege_name(spec)} Global"))
        return grants


def share_role(access_mask: int) -> Optional[GrantRole]:
    """Map a Dataverse AccessRights mask to the coarse CGraph role."""
    if access_mask & ACCESS_WRITE:
        return GrantRole.WRITER
    if access_mask & ACCESS_READ:
        return GrantRole.READER
    return None


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


def derive_grants(spec: EntitySpec, row: Mapping[str, Any], ctx: SecurityContext) -> list[PermissionGrant]:
    """Apply the permission model documented in the module docstring to one row."""
    grants: list[PermissionGrant] = []

    # 1. Owner (user or team)
    owner_id, owner_kind = owner_reference(row)
    if owner_id and owner_kind == "team":
        grants.append(PermissionGrant(GrantEntity.GROUP, GrantRole.OWNER, external_id=team_group_external_id(owner_id), reason="ownerid (team)"))
    elif owner_id:
        email = ctx.user_email_by_id.get(owner_id)
        if email:
            grants.append(PermissionGrant(GrantEntity.USER, GrantRole.OWNER, email=email, reason="ownerid (user)"))

    # 2. Owning business unit -> BU group readers
    bu_id = row.get("_owningbusinessunit_value")
    if bu_id:
        grants.append(PermissionGrant(GrantEntity.GROUP, GrantRole.READER, external_id=bu_group_external_id(str(bu_id)), reason="owningbusinessunit"))

    # 3. Roles with Global read on this entity + System Administrator
    grants.extend(ctx.entity_role_grants(spec))

    # 4. Explicit shares
    row_id = str(row.get(spec.primary_id) or "")
    for share in ctx.shares_by_entity.get(spec.logical_name, {}).get(row_id, ()):
        role = share_role(share.access_mask)
        if role is None:
            continue
        if share.principal_type == PRINCIPAL_TYPE_TEAM:
            grants.append(PermissionGrant(GrantEntity.GROUP, role, external_id=team_group_external_id(share.principal_id), reason="principalobjectaccess (team)"))
        elif share.principal_type == PRINCIPAL_TYPE_SYSTEMUSER:
            email = ctx.user_email_by_id.get(share.principal_id)
            if email:
                grants.append(PermissionGrant(GrantEntity.USER, role, email=email, reason="principalobjectaccess (user)"))

    return _merge(grants)


def index_shares(rows: Iterable[Mapping[str, Any]]) -> dict[str, list[ShareEntry]]:
    """Group ``principalobjectaccessset`` rows by object id."""
    out: dict[str, list[ShareEntry]] = {}
    for row in rows:
        object_id = row.get("objectid")
        principal_id = row.get("principalid")
        if not object_id or not principal_id:
            continue
        try:
            principal_type = int(row.get("principaltypecode") or 0)
            mask = int(row.get("accessrightsmask") or 0) | int(row.get("inheritedaccessrightsmask") or 0)
        except (TypeError, ValueError):
            continue
        out.setdefault(str(object_id), []).append(ShareEntry(str(principal_id), principal_type, mask))
    return out


def share_digest(shares: Iterable[ShareEntry]) -> str:
    """Short, order-independent fingerprint of one record's share list."""
    entries = sorted({(s.principal_id, s.principal_type, s.access_mask) for s in shares})
    payload = "\n".join(f"{pid}\t{ptype}\t{mask}" for pid, ptype, mask in entries)
    return hashlib.blake2b(payload.encode("utf-8"), digest_size=SHARE_DIGEST_HEX_CHARS // 2).hexdigest()


def share_digests(shares: Mapping[str, Iterable[ShareEntry]]) -> dict[str, str]:
    """``{object_id: digest}`` for records that have at least one share; the map is
    persisted between syncs so unshared records cost nothing."""
    out: dict[str, str] = {}
    for object_id, shared_with in shares.items():
        entries = list(shared_with)
        if entries:
            out[str(object_id)] = share_digest(entries)
    return out


def changed_share_record_ids(previous: Mapping[str, str], current: Mapping[str, str]) -> set[str]:
    """Record ids whose share digest was added, removed or changed between two syncs."""
    return {rid for rid in set(previous) | set(current) if previous.get(rid) != current.get(rid)}


def systemuser_email(row: Mapping[str, Any]) -> Optional[str]:
    """``internalemailaddress`` first, then the UPN in ``domainname``."""
    for attribute in ("internalemailaddress", "domainname"):
        value = row.get(attribute)
        if value and "@" in str(value):
            return str(value).strip().lower()
    return None


def is_application_user(row: Mapping[str, Any]) -> bool:
    """S2S application users (the connector itself) must not become AppUsers."""
    return bool(row.get("applicationid"))
