"""Tests for app.connectors.sources.microsoft.common.personal_scope.

Pure helpers shared by the personal (delegated OAuth) scope of the OneDrive /
Outlook / Microsoft Teams connectors: creator-only grants and delegated-token
claim extraction.  No pydantic / msgraph / httpx.  Written pytest-style but
runnable with plain ``PYTHONPATH=. python3 tests/unit/connectors/sources/test_microsoft_personal_scope.py``.
"""

import base64
import json

from app.connectors.sources.microsoft.common.personal_scope import (
    GRANT_ENTITY_USER,
    GRANT_ROLE_READER,
    PERSONAL_SCOPE,
    TEAM_SCOPE,
    CreatorGrant,
    creator_grants,
    decode_jwt_claims,
    is_personal_scope,
    normalize_email,
    personal_record_group_grants,
    signed_in_account_matches_creator,
    user_email_from_claims,
    user_oid_from_access_token,
)


def _jwt(claims: dict) -> str:
    def b64(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{b64({'alg': 'RS256', 'typ': 'JWT'})}.{b64(claims)}.signature"


class TestScope:
    def test_is_personal_scope(self):
        assert is_personal_scope("personal")
        assert is_personal_scope(" Personal ")
        assert not is_personal_scope("team")
        assert not is_personal_scope(None)
        assert not is_personal_scope("")
        assert PERSONAL_SCOPE == "personal" and TEAM_SCOPE == "team"

    def test_normalize_email(self):
        assert normalize_email(" Alice@Contoso.COM ") == "alice@contoso.com"
        assert normalize_email("no-at-sign") is None
        assert normalize_email(None) is None
        assert normalize_email("") is None


class TestCreatorGrants:
    """OneDrive files / Outlook mails / Teams chats: READER for the creator and nobody else."""

    def test_creator_only_reader(self):
        grants = creator_grants("Owner@Contoso.com")
        assert grants == [CreatorGrant(email="owner@contoso.com")]
        assert grants[0].entity_type == GRANT_ENTITY_USER == "USER"
        assert grants[0].role == GRANT_ROLE_READER == "READER"
        assert len(grants) == 1

    def test_no_owner_or_group_or_org_grants(self):
        grants = creator_grants("owner@contoso.com")
        assert all(g.role == "READER" for g in grants)  # never OWNER / WRITER
        assert all(g.entity_type == "USER" for g in grants)  # never GROUP / ORG / ANYONE
        # the same rule applies to record groups (drive, mail folders, chats container)
        assert personal_record_group_grants("owner@contoso.com") == grants

    def test_fail_closed_without_creator_email(self):
        assert creator_grants(None) == []
        assert creator_grants("") == []
        assert creator_grants("   ") == []
        assert creator_grants("not-an-email") == []
        assert personal_record_group_grants(None) == []

    def test_grant_is_immutable(self):
        grant = creator_grants("owner@contoso.com")[0]
        try:
            grant.email = "someone-else@contoso.com"  # type: ignore[misc]
        except Exception:
            pass
        else:  # pragma: no cover - frozen dataclass must reject the write
            raise AssertionError("CreatorGrant must be frozen")


class TestDelegatedTokenClaims:
    def test_oid_and_email_from_token(self):
        token = _jwt({"oid": "11111111-2222-3333-4444-555555555555", "preferred_username": "Alice@Contoso.com", "tid": "t"})
        assert user_oid_from_access_token(token) == "11111111-2222-3333-4444-555555555555"
        claims = decode_jwt_claims(token)
        assert claims["tid"] == "t"
        assert user_email_from_claims(claims) == "alice@contoso.com"

    def test_email_claim_fallbacks(self):
        assert user_email_from_claims({"upn": "A@b.co"}) == "a@b.co"
        assert user_email_from_claims({"email": "A@b.co"}) == "a@b.co"
        assert user_email_from_claims({"unique_name": "A@b.co"}) == "a@b.co"
        assert user_email_from_claims({"preferred_username": "bogus", "upn": "x@y.z"}) == "x@y.z"
        assert user_email_from_claims({}) is None

    def test_garbage_tokens(self):
        assert decode_jwt_claims(None) == {}
        assert decode_jwt_claims("") == {}
        assert decode_jwt_claims("opaque-token") == {}
        assert decode_jwt_claims("a.b") == {}
        assert decode_jwt_claims("a.!!!.c") == {}
        assert decode_jwt_claims("a." + base64.urlsafe_b64encode(b"[1,2]").decode() + ".c") == {}
        assert user_oid_from_access_token("opaque-token") is None
        assert user_oid_from_access_token(_jwt({"sub": "x"})) is None

    def test_padding_tolerant(self):
        # payload length not a multiple of 4 after stripping '=' padding
        for oid in ("a", "ab", "abc", "abcd", "abcde"):
            assert user_oid_from_access_token(_jwt({"oid": oid})) == oid


class TestSignedInAccountCheck:
    def test_match_is_case_insensitive_and_any_candidate(self):
        assert signed_in_account_matches_creator("Alice@Contoso.com", None, "alice@contoso.com") is True
        assert signed_in_account_matches_creator("alice@contoso.com", "alice@contoso.com", "alice_contoso.com#EXT#@x.onmicrosoft.com") is True

    def test_mismatch(self):
        assert signed_in_account_matches_creator("alice@contoso.com", "bob@contoso.com") is False

    def test_unknown(self):
        assert signed_in_account_matches_creator(None, "bob@contoso.com") is None
        assert signed_in_account_matches_creator("alice@contoso.com") is None
        assert signed_in_account_matches_creator("alice@contoso.com", None, "") is None


if __name__ == "__main__":  # plain-python runner for machines without pytest
    import inspect
    import sys
    import traceback

    failures = 0
    total = 0
    for _name, cls in sorted(globals().items()):
        if not (inspect.isclass(cls) and _name.startswith("Test")):
            continue
        for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not method_name.startswith("test_"):
                continue
            total += 1
            try:
                method(cls())
            except Exception:
                failures += 1
                print(f"FAIL {cls.__name__}.{method_name}")
                traceback.print_exc()
    print(f"{total - failures}/{total} tests passed")
    sys.exit(1 if failures else 0)
