"""Tests for app.connectors.sources.microsoft.dynamics365.mapping.

Pure functions only (entity mapping, rendering, RBAC derivation) — no network,
no pydantic, no azure/httpx, so these run in any interpreter.
"""

import pytest

from app.connectors.sources.microsoft.dynamics365.mapping import (
    ACCESS_APPEND,
    ACCESS_READ,
    ACCESS_WRITE,
    DEFAULT_ENTITY_ORDER,
    ENTITY_SPECS,
    PRINCIPAL_TYPE_SYSTEMUSER,
    PRINCIPAL_TYPE_TEAM,
    GrantEntity,
    GrantRole,
    PermissionGrant,
    SecurityContext,
    ShareEntry,
    attachment_external_id,
    build_metadata,
    build_modified_filter,
    bu_group_external_id,
    derive_grants,
    epoch_ms_to_odata,
    index_shares,
    is_application_user,
    normalize_environment_url,
    parse_dataverse_timestamp,
    read_privilege_name,
    record_external_id,
    record_title,
    record_web_url,
    render_record_markdown,
    resolve_selected_entities,
    role_external_id,
    role_has_global_read,
    share_role,
    split_external_id,
    systemuser_email,
    team_group_external_id,
    token_scope,
)

ENV = "https://contoso.crm4.dynamics.com"
OPP = ENTITY_SPECS["opportunity"]
INC = ENTITY_SPECS["incident"]
NOTE = ENTITY_SPECS["annotation"]

USER_A = "11111111-1111-1111-1111-111111111111"
USER_B = "22222222-2222-2222-2222-222222222222"
TEAM_X = "33333333-3333-3333-3333-333333333333"
BU_ROOT = "44444444-4444-4444-4444-444444444444"
OPP_ID = "55555555-5555-5555-5555-555555555555"
ROLE_ADMIN = "role:aaaaaaaa-0000-0000-0000-000000000001"
ROLE_SALES = "role:aaaaaaaa-0000-0000-0000-000000000002"


def _ctx(**overrides) -> SecurityContext:
    ctx = SecurityContext(
        user_email_by_id={USER_A: "alice@contoso.com", USER_B: "bob@contoso.com"},
        known_team_ids={TEAM_X},
        known_business_unit_ids={BU_ROOT},
        global_read_roles_by_entity={"opportunity": {ROLE_SALES}},
        system_admin_role_id=ROLE_ADMIN,
    )
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


def _opp_row(**overrides) -> dict:
    row = {
        "opportunityid": OPP_ID,
        "name": "Contoso renewal",
        "description": "Annual renewal.\nIncludes support.",
        "estimatedvalue": 12000.0,
        "estimatedvalue@OData.Community.Display.V1.FormattedValue": "$12,000.00",
        "closeprobability": 60,
        "estimatedclosedate": "2026-12-31",
        "statecode": 0,
        "statuscode": 1,
        "statuscode@OData.Community.Display.V1.FormattedValue": "In Progress",
        "_ownerid_value": USER_A,
        "_ownerid_value@OData.Community.Display.V1.FormattedValue": "Alice Adams",
        "_ownerid_value@Microsoft.Dynamics.CRM.lookuplogicalname": "systemuser",
        "_owninguser_value": USER_A,
        "_owningteam_value": None,
        "_owningbusinessunit_value": BU_ROOT,
        "_owningbusinessunit_value@OData.Community.Display.V1.FormattedValue": "Contoso",
        "createdon": "2026-01-02T03:04:05Z",
        "modifiedon": "2026-02-03T04:05:06Z",
    }
    row.update(overrides)
    return row


def _grant_keys(grants):
    return {(g.entity_type, g.role, g.external_id, g.email) for g in grants}


# ---------------------------------------------------------------------------
# Entity registry / ids / urls
# ---------------------------------------------------------------------------


class TestEntityRegistry:
    def test_all_entities_have_consistent_specs(self):
        for name, spec in ENTITY_SPECS.items():
            assert spec.logical_name == name
            assert spec.primary_id in spec.select_fields
            assert spec.primary_name in spec.select_fields
            assert "modifiedon" in spec.select_fields
            assert "_ownerid_value" in spec.select_fields
            assert "_owningbusinessunit_value" in spec.select_fields
        assert set(DEFAULT_ENTITY_ORDER) == set(ENTITY_SPECS)

    def test_record_type_mapping(self):
        assert ENTITY_SPECS["opportunity"].record_type == "DEAL"
        assert ENTITY_SPECS["incident"].record_type == "CASE"
        # rows without a dedicated model are stored as plain records only
        for name in ("account", "contact", "lead", "annotation"):
            assert ENTITY_SPECS[name].record_type == "OTHERS"
            assert ENTITY_SPECS[name].record_group_type == "CRM_ENTITY"

    def test_resolve_selected_entities_defaults_and_filters(self):
        assert [s.logical_name for s in resolve_selected_entities(None)] == list(DEFAULT_ENTITY_ORDER)
        assert [s.logical_name for s in resolve_selected_entities([])] == list(DEFAULT_ENTITY_ORDER)
        selected = resolve_selected_entities(["incident", "Opportunity", "bogus"])
        assert [s.logical_name for s in selected] == ["opportunity", "incident"]

    def test_read_privilege_names(self):
        assert read_privilege_name(OPP) == "prvReadOpportunity"
        assert read_privilege_name(INC) == "prvReadIncident"
        assert read_privilege_name(NOTE) == "prvReadNote"

    def test_role_has_global_read(self):
        privs = [
            {"PrivilegeName": "prvReadOpportunity", "Depth": "Deep"},
            {"PrivilegeName": "prvReadIncident", "Depth": "Global"},
        ]
        assert role_has_global_read(privs, INC) is True
        assert role_has_global_read(privs, OPP) is False  # Deep is not Global
        assert role_has_global_read([], OPP) is False

    def test_external_ids_round_trip(self):
        ext = record_external_id(OPP, OPP_ID)
        assert ext == f"opportunity:{OPP_ID}"
        assert split_external_id(ext) == ("opportunity", OPP_ID)
        assert split_external_id(attachment_external_id("abc")) == ("annotation-file", "abc")
        assert team_group_external_id(TEAM_X) == f"team:{TEAM_X}"
        assert bu_group_external_id(BU_ROOT) == f"bu:{BU_ROOT}"
        with pytest.raises(ValueError):
            split_external_id("no-separator")

    def test_role_external_id_collapses_bu_copies(self):
        root = {"roleid": "r1", "_parentrootroleid_value": None}
        copy = {"roleid": "r2", "_parentrootroleid_value": "r1"}
        assert role_external_id(root) == role_external_id(copy) == "role:r1"

    def test_urls(self):
        assert normalize_environment_url("contoso.crm4.dynamics.com/") == ENV
        assert normalize_environment_url(" https://contoso.crm4.dynamics.com// ") == ENV
        assert token_scope(ENV) == f"{ENV}/.default"
        assert record_web_url(ENV, OPP, OPP_ID) == (
            f"{ENV}/main.aspx?etn=opportunity&id={OPP_ID}&pagetype=entityrecord"
        )
        with pytest.raises(ValueError):
            normalize_environment_url("")


# ---------------------------------------------------------------------------
# Timestamps / OData filters
# ---------------------------------------------------------------------------


class TestTimeAndFilters:
    def test_parse_dataverse_timestamp(self):
        assert parse_dataverse_timestamp("1970-01-01T00:00:01Z") == 1000
        assert parse_dataverse_timestamp("1970-01-01T00:00:01.5Z") == 1500
        assert parse_dataverse_timestamp("1970-01-01T01:00:00+01:00") == 0
        assert parse_dataverse_timestamp(None) is None
        assert parse_dataverse_timestamp("not a date") is None

    def test_epoch_to_odata_literal(self):
        assert epoch_ms_to_odata(1000) == "1970-01-01T00:00:01Z"

    def test_build_modified_filter(self):
        assert build_modified_filter() is None
        assert build_modified_filter(since_ms=1000) == "modifiedon gt 1970-01-01T00:00:01Z"
        assert build_modified_filter(start_ms=1000) == "modifiedon ge 1970-01-01T00:00:01Z"
        # user lower bound newer than the sync point wins, and is inclusive
        assert build_modified_filter(since_ms=1000, start_ms=2000) == "modifiedon ge 1970-01-01T00:00:02Z"
        # sync point newer than the user lower bound keeps the exclusive 'gt'
        assert build_modified_filter(since_ms=3000, start_ms=2000) == "modifiedon gt 1970-01-01T00:00:03Z"
        assert build_modified_filter(start_ms=1000, end_ms=5000) == (
            "modifiedon ge 1970-01-01T00:00:01Z and modifiedon le 1970-01-01T00:00:05Z"
        )
        assert build_modified_filter(end_ms=5000) == "modifiedon le 1970-01-01T00:00:05Z"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


class TestRendering:
    def test_render_opportunity_markdown(self):
        markdown, metadata = render_record_markdown(OPP, _opp_row(), ENV)
        assert markdown.startswith("# Contoso renewal\n")
        assert "**Type:** Dynamics 365 Opportunity" in markdown
        assert "**Status:** In Progress" in markdown          # formatted value preferred
        assert "**Owner:** Alice Adams" in markdown
        assert "- **Estimated value:** $12,000.00" in markdown  # currency formatted value
        assert "- **Probability (%):** 60" in markdown
        assert "## Description\nAnnual renewal.\nIncludes support." in markdown
        assert f"- Record id: {OPP_ID}" in markdown
        assert f"- Owner id: {USER_A} (systemuser)" in markdown
        assert "- Business unit: Contoso" in markdown
        assert record_web_url(ENV, OPP, OPP_ID) in markdown
        assert markdown.endswith("\n")

        assert metadata["entity"] == "opportunity"
        assert metadata["id"] == OPP_ID
        assert metadata["owner_id"] == USER_A
        assert metadata["owner_type"] == "systemuser"
        assert metadata["owning_business_unit_id"] == BU_ROOT
        assert metadata["statecode"] == 0 and metadata["statuscode"] == 1
        assert metadata["modifiedon"] == "2026-02-03T04:05:06Z"

    def test_render_note_uses_notetext(self):
        row = {
            "annotationid": "n1",
            "subject": "Call summary",
            "notetext": "Customer asked for a discount.",
            "isdocument": False,
            "objecttypecode": "opportunity",
            "_objectid_value": OPP_ID,
            "_objectid_value@OData.Community.Display.V1.FormattedValue": "Contoso renewal",
            "_ownerid_value": USER_B,
            "_owninguser_value": USER_B,
            "_owningbusinessunit_value": BU_ROOT,
            "createdon": "2026-01-01T00:00:00Z",
            "modifiedon": "2026-01-01T00:00:00Z",
        }
        markdown, metadata = render_record_markdown(NOTE, row, ENV)
        assert markdown.startswith("# Call summary\n")
        assert "## Note\nCustomer asked for a discount." in markdown
        assert "- **Attached to:** Contoso renewal" in markdown
        assert metadata["statecode"] is None

    def test_record_title_fallbacks(self):
        assert record_title(NOTE, {"annotationid": "n1", "filename": "quote.pdf"}) == "quote.pdf"
        assert record_title(ENTITY_SPECS["lead"], {"leadid": "l1", "fullname": "Jane Doe"}) == "Jane Doe"
        assert record_title(ENTITY_SPECS["account"], {"accountid": "a1"}) == "Account a1"

    def test_build_metadata_without_owner(self):
        metadata = build_metadata(OPP, {"opportunityid": OPP_ID}, ENV)
        assert metadata["owner_id"] is None and metadata["owner_type"] is None
        assert metadata["url"].endswith(f"id={OPP_ID}&pagetype=entityrecord")


# ---------------------------------------------------------------------------
# RBAC derivation
# ---------------------------------------------------------------------------


class TestPermissionMapping:
    def test_user_owner_bu_roles(self):
        grants = derive_grants(OPP, _opp_row(), _ctx())
        assert _grant_keys(grants) == {
            (GrantEntity.USER, GrantRole.OWNER, None, "alice@contoso.com"),
            (GrantEntity.GROUP, GrantRole.READER, f"bu:{BU_ROOT}", None),
            (GrantEntity.ROLE, GrantRole.READER, ROLE_ADMIN, None),
            (GrantEntity.ROLE, GrantRole.READER, ROLE_SALES, None),
        }

    def test_team_owner_becomes_group_owner(self):
        row = _opp_row(_owninguser_value=None, _owningteam_value=TEAM_X, _ownerid_value=TEAM_X)
        row["_ownerid_value@Microsoft.Dynamics.CRM.lookuplogicalname"] = "team"
        grants = derive_grants(OPP, row, _ctx())
        assert (GrantEntity.GROUP, GrantRole.OWNER, f"team:{TEAM_X}", None) in _grant_keys(grants)
        assert not any(g.entity_type == GrantEntity.USER for g in grants)

    def test_owner_annotation_fallback_when_owning_fields_missing(self):
        row = _opp_row()
        del row["_owninguser_value"]
        del row["_owningteam_value"]
        grants = derive_grants(OPP, row, _ctx())
        assert (GrantEntity.USER, GrantRole.OWNER, None, "alice@contoso.com") in _grant_keys(grants)

    def test_unknown_owner_is_skipped_not_guessed(self):
        row = _opp_row(_owninguser_value="deadbeef", _ownerid_value="deadbeef")
        grants = derive_grants(OPP, row, _ctx())
        assert not any(g.entity_type == GrantEntity.USER for g in grants)

    def test_global_read_roles_are_per_entity(self):
        ctx = _ctx()
        inc_row = {"incidentid": "i1", "_owninguser_value": USER_B, "_owningbusinessunit_value": BU_ROOT}
        grants = derive_grants(INC, inc_row, ctx)
        role_ids = {g.external_id for g in grants if g.entity_type == GrantEntity.ROLE}
        assert role_ids == {ROLE_ADMIN}  # sales role only has Global read on opportunities

    def test_system_admin_always_granted_even_without_privilege_data(self):
        ctx = _ctx(global_read_roles_by_entity={})
        grants = derive_grants(OPP, _opp_row(), ctx)
        assert (GrantEntity.ROLE, GrantRole.READER, ROLE_ADMIN, None) in _grant_keys(grants)

    def test_no_admin_role_when_not_resolved(self):
        ctx = _ctx(system_admin_role_id=None, global_read_roles_by_entity={})
        grants = derive_grants(OPP, _opp_row(), ctx)
        assert not any(g.entity_type == GrantEntity.ROLE for g in grants)

    def test_shares_map_to_reader_or_writer(self):
        ctx = _ctx()
        ctx.shares_by_entity = {
            "opportunity": {
                OPP_ID: [
                    ShareEntry(USER_B, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ),
                    ShareEntry(TEAM_X, PRINCIPAL_TYPE_TEAM, ACCESS_READ | ACCESS_WRITE),
                    ShareEntry("unknown-user", PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ),
                    ShareEntry(USER_A, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_APPEND),  # no read bit
                ]
            }
        }
        grants = derive_grants(OPP, _opp_row(), ctx)
        keys = _grant_keys(grants)
        assert (GrantEntity.USER, GrantRole.READER, None, "bob@contoso.com") in keys
        assert (GrantEntity.GROUP, GrantRole.WRITER, f"team:{TEAM_X}", None) in keys
        assert not any(g.email is None and g.external_id is None for g in grants)
        # owner keeps OWNER even though a weaker share exists for the same user
        alice = [g for g in grants if g.email == "alice@contoso.com"]
        assert len(alice) == 1 and alice[0].role == GrantRole.OWNER

    def test_strongest_role_wins_on_merge(self):
        ctx = _ctx()
        ctx.shares_by_entity = {
            "opportunity": {OPP_ID: [ShareEntry(USER_B, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_READ), ShareEntry(USER_B, PRINCIPAL_TYPE_SYSTEMUSER, ACCESS_WRITE)]}
        }
        grants = derive_grants(OPP, _opp_row(), ctx)
        bob = [g for g in grants if g.email == "bob@contoso.com"]
        assert len(bob) == 1 and bob[0].role == GrantRole.WRITER

    def test_share_role_bits(self):
        assert share_role(ACCESS_READ) == GrantRole.READER
        assert share_role(ACCESS_READ | ACCESS_WRITE) == GrantRole.WRITER
        assert share_role(ACCESS_WRITE) == GrantRole.WRITER
        assert share_role(ACCESS_APPEND) is None
        assert share_role(0) is None

    def test_index_shares_merges_inherited_mask_and_skips_bad_rows(self):
        rows = [
            {"objectid": OPP_ID, "principalid": USER_B, "principaltypecode": 8, "accessrightsmask": 0, "inheritedaccessrightsmask": ACCESS_READ},
            {"objectid": OPP_ID, "principalid": TEAM_X, "principaltypecode": "9", "accessrightsmask": "3"},
            {"objectid": None, "principalid": USER_A, "principaltypecode": 8, "accessrightsmask": 1},
            {"objectid": OPP_ID, "principalid": USER_A, "principaltypecode": "x", "accessrightsmask": 1},
        ]
        indexed = index_shares(rows)
        assert set(indexed) == {OPP_ID}
        assert indexed[OPP_ID] == [
            ShareEntry(USER_B, 8, ACCESS_READ),
            ShareEntry(TEAM_X, 9, ACCESS_READ | ACCESS_WRITE),
        ]

    def test_entity_role_grants_for_record_groups(self):
        grants = _ctx().entity_role_grants(OPP)
        assert [g.external_id for g in grants] == [ROLE_ADMIN, ROLE_SALES]
        assert all(g.role == GrantRole.READER and g.entity_type == GrantEntity.ROLE for g in grants)
        # admin id is not duplicated when it also has Global read
        ctx = _ctx(global_read_roles_by_entity={"opportunity": {ROLE_ADMIN, ROLE_SALES}})
        assert [g.external_id for g in ctx.entity_role_grants(OPP)] == [ROLE_ADMIN, ROLE_SALES]

    def test_permission_grant_reason_is_not_part_of_identity(self):
        a = PermissionGrant(GrantEntity.USER, GrantRole.OWNER, email="x@y.z", reason="one")
        b = PermissionGrant(GrantEntity.USER, GrantRole.OWNER, email="x@y.z", reason="two")
        assert a == b


class TestUsers:
    def test_systemuser_email_prefers_internal_email(self):
        assert systemuser_email({"internalemailaddress": "A@Contoso.com", "domainname": "a@contoso.onmicrosoft.com"}) == "a@contoso.com"
        assert systemuser_email({"internalemailaddress": "", "domainname": "b@contoso.com"}) == "b@contoso.com"
        assert systemuser_email({"internalemailaddress": None, "domainname": "CONTOSO\\svc"}) is None

    def test_application_users_are_excluded(self):
        assert is_application_user({"applicationid": "app-guid"}) is True
        assert is_application_user({"applicationid": None}) is False
