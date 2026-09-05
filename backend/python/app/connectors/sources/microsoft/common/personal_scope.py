"""Pure helpers for the *personal* (delegated OAuth) scope of the Microsoft connectors.

OneDrive, Outlook and Microsoft Teams are dual-scope connectors:

* ``team``     — app-only Entra ID app with admin consent (``OAUTH_ADMIN_CONSENT``);
  the connector mirrors the source ACL into the permission graph.
* ``personal`` — the signed-in user's own account through the delegated OAuth
  flow (``OAUTH``).  Everything indexed under such an instance is readable by
  the connector creator **only**; source ACLs, directory users, groups and
  roles are never synced.

This module holds the part of that behaviour that has no I/O so it can be unit
tested without pydantic / msgraph / httpx: the creator-only grant, the
connector-agnostic grant shape and JWT claim extraction for the delegated
access token (Graph needs the user's ``oid`` for ``/users/{oid}/...`` routes,
which the kiota SDK handles better than ``/me`` when a request adapter is
built by hand).
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional

PERSONAL_SCOPE = "personal"
TEAM_SCOPE = "team"

GRANT_ENTITY_USER = "USER"
GRANT_ROLE_READER = "READER"


def is_personal_scope(scope: Optional[str]) -> bool:
    """``True`` for the ``personal`` connector scope (case-insensitive)."""
    return (scope or "").strip().lower() == PERSONAL_SCOPE


def normalize_email(email: Optional[str]) -> Optional[str]:
    """Lower-cased, trimmed email or ``None`` when there is nothing usable."""
    value = (email or "").strip().lower()
    return value if value and "@" in value else None


@dataclass(frozen=True)
class CreatorGrant:
    """Connector-agnostic permission for the connector creator.

    ``connector.py`` turns it into a graph ``Permission`` (USER / READ by email).
    """

    email: str
    entity_type: str = GRANT_ENTITY_USER
    role: str = GRANT_ROLE_READER


def creator_grants(creator_email: Optional[str]) -> list[CreatorGrant]:
    """READER for the connector creator and nobody else.

    Returns an empty list when no creator email is known so the caller fails
    closed (a record without permissions is visible to no one) instead of
    falling back to team / org wide grants.
    """
    email = normalize_email(creator_email)
    return [CreatorGrant(email=email)] if email else []


def personal_record_group_grants(creator_email: Optional[str]) -> list[CreatorGrant]:
    """Record groups (drives, mail folders, the chats container) use the same
    creator-only rule as records."""
    return creator_grants(creator_email)


# ---------------------------------------------------------------------------
# Delegated access token claims
# ---------------------------------------------------------------------------


def decode_jwt_claims(token: Optional[str]) -> dict[str, Any]:
    """Best-effort decode of a JWT payload **without** signature verification.

    Only used to read non-security-relevant hints (``oid``, ``upn``) from a
    token Microsoft already issued to us; authorization is still enforced by
    Graph on every call.  Returns ``{}`` for anything that is not a 3-part JWT.
    """
    if not token or not isinstance(token, str):
        return {}
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (ValueError, TypeError):
        return {}
    return claims if isinstance(claims, dict) else {}


def user_oid_from_access_token(token: Optional[str]) -> Optional[str]:
    """Entra object id of the signed-in user, or ``None``."""
    oid = decode_jwt_claims(token).get("oid")
    return str(oid) if oid else None


def user_email_from_claims(claims: Mapping[str, Any]) -> Optional[str]:
    """The signed-in user's email as far as the token tells (``preferred_username``,
    ``upn``, ``email``, ``unique_name`` in that order)."""
    for key in ("preferred_username", "upn", "email", "unique_name"):
        email = normalize_email(claims.get(key))
        if email:
            return email
    return None


def signed_in_account_matches_creator(
    creator_email: Optional[str],
    *candidates: Optional[str],
) -> Optional[bool]:
    """Compare the connector creator's Edrak/PipesHub email with the Microsoft
    account that signed in (``mail`` / ``userPrincipalName`` from Graph, or a
    token claim).

    ``None`` when neither side is known; otherwise whether any candidate matches.
    Callers only *log* a mismatch: the data is still granted to the creator
    alone, which is the privacy property the personal scope promises.
    """
    creator = normalize_email(creator_email)
    known = [c for c in (normalize_email(c) for c in candidates) if c]
    if not creator or not known:
        return None
    return creator in known
