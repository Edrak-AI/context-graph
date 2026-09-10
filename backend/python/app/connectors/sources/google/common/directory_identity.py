"""Google Workspace Directory identity helpers shared by the workspace connectors.

A Directory ``users.list`` entry (``projection=full``) carries the account's
``primaryEmail`` plus every other address Google knows for it: ``aliases`` and
``nonEditableAliases`` (domain-alias copies) and ``emails[]`` (which repeats the
primary and lists the rest).  CGraph links the ``AppUser`` to the platform account
by any of these, so people who sign in to Edrak with an alias domain still match.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping


def directory_alternate_emails(user: Mapping[str, Any]) -> list[str]:
    """Every address of a Directory user other than ``primaryEmail`` (lower-cased, deduped, in directory order)."""
    primary = str(user.get("primaryEmail") or "").strip().lower()
    candidates: list[object] = []
    for key in ("aliases", "nonEditableAliases"):
        values = user.get(key)
        if isinstance(values, list):
            candidates.extend(values)
    emails = user.get("emails")
    if isinstance(emails, list):
        candidates.extend(e.get("address") for e in emails if isinstance(e, dict))
    result: list[str] = []
    for candidate in candidates:
        text = str(candidate or "").strip().lower()
        if "@" in text and text != primary and text not in result:
            result.append(text)
    return result
