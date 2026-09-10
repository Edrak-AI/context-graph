"""``directory_alternate_emails`` over Google Workspace Directory ``users.list`` entries (no googleapiclient needed)."""

from app.connectors.sources.google.common.directory_identity import (
    directory_alternate_emails,
)


def test_aliases_and_secondary_emails_without_the_primary() -> None:
    user = {
        "primaryEmail": "Khalid@edrak.com",
        "aliases": ["khalid@favapps.co", "k.hassan@edrak.com"],
        "nonEditableAliases": ["khalid@edrak.com.test-google-a.com", "khalid@favapps.co"],
        "emails": [
            {"address": "khalid@edrak.com", "primary": True},
            {"address": "Khalid@FavApps.co"},
            {"address": "khalid.personal@gmail.com", "type": "home"},
            {"type": "work"},
        ],
    }
    assert directory_alternate_emails(user) == [
        "khalid@favapps.co",
        "k.hassan@edrak.com",
        "khalid@edrak.com.test-google-a.com",
        "khalid.personal@gmail.com",
    ]


def test_tolerates_missing_or_malformed_fields() -> None:
    assert directory_alternate_emails({"primaryEmail": "only@edrak.com"}) == []
    assert directory_alternate_emails({"primaryEmail": "x@edrak.com", "aliases": "x@favapps.co", "emails": "bad"}) == []
    assert directory_alternate_emails({"aliases": [None, "", "no-at-sign", "ok@favapps.co"]}) == ["ok@favapps.co"]
