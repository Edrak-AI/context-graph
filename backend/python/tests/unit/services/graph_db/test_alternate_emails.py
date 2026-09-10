"""Alternate e-mail resolution: a User node's `alternateEmails` (linked sign-ins) and `sourceEmails`
(connector-reported addresses) resolve like its primary e-mail, ranked primary > alternate > source."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames, Connectors
from app.models.entities import AppUser, User, normalize_alternate_emails
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider
from app.services.graph_db.common.utils import alternate_email_matches
from app.services.graph_db.neo4j.neo4j_provider import Neo4jProvider

PRIMARY = "sujit@edrak.com"
ALTERNATE = "sujit@demo.edrak.com"
NEO4J_CLAUSE = (
    "(toLower(u.email) = toLower($email) "
    "OR toLower($email) IN [x IN coalesce(u.alternateEmails, []) | toLower(x)] "
    "OR toLower($email) IN [x IN coalesce(u.sourceEmails, []) | toLower(x)])"
)
NEO4J_RANK = (
    "CASE WHEN toLower(u.email) = toLower($email) THEN 0 "
    "WHEN toLower($email) IN [x IN coalesce(u.alternateEmails, []) | toLower(x)] THEN 1 ELSE 2 END"
)
AQL_CLAUSE = (
    "(LOWER(user.email) == LOWER(@email) "
    "OR LOWER(@email) IN (FOR x IN (user.alternateEmails || []) RETURN LOWER(x)) "
    "OR LOWER(@email) IN (FOR x IN (user.sourceEmails || []) RETURN LOWER(x)))"
)
AQL_RANK = (
    "(LOWER(user.email) == LOWER(@email) ? 0 : "
    "(LOWER(@email) IN (FOR x IN (user.alternateEmails || []) RETURN LOWER(x)) ? 1 : 2))"
)

# The favapps scenario: the tenant's primary addresses are @edrak.onmicrosoft.com / @edrak.com, people sign
# in to Edrak with the alias domain, which exists on their Microsoft and Google accounts as a secondary address.
KHALID_SIGN_IN = "khalid@favapps.co"
KHALID_MS_PRIMARY = "khalid@edrak.onmicrosoft.com"
KHALID_GOOGLE_PRIMARY = "khalid@edrak.com"


def _platform_user_neo4j() -> dict:
    return {"id": "platform-user", "orgId": "org-1", "email": PRIMARY, "alternateEmails": [ALTERNATE]}


def _platform_user_arango() -> dict:
    return {"_key": "platform-user", "orgId": "org-1", "email": PRIMARY, "alternateEmails": [ALTERNATE]}


def _app_user(email: str = ALTERNATE, alternates: list[str] | None = None, connector_id: str = "conn-1") -> AppUser:
    return AppUser(
        app_name=Connectors.ONEDRIVE,
        connector_id=connector_id,
        source_user_id="entra-1",
        org_id="org-1",
        email=email,
        alternate_emails=alternates or [],
        full_name="Sujit",
    )


def _khalid_neo4j(source_emails: list[str] | None = None) -> dict:
    node = {"id": "khalid", "orgId": "org-1", "email": KHALID_SIGN_IN, "alternateEmails": []}
    if source_emails is not None:
        node["sourceEmails"] = source_emails
    return node


class FakeNeo4jUsers:
    """`execute_query` stand-in: e-mail lookups over an in-memory user list, records the sourceEmails merge."""

    def __init__(self, users: list[dict]) -> None:
        self.users = users
        self.merges: list[dict] = []

    async def execute_query(self, query: str, parameters: dict | None = None, txn_id: str | None = None) -> list:
        parameters = parameters or {}
        if "SET u.sourceEmails" in query:
            self.merges.append(parameters)
            return []
        if "MATCH (u:User)" in query:
            wanted = parameters["email"].lower()
            hits = [
                u for u in self.users
                if u.get("orgId") == parameters.get("org_id", u.get("orgId"))
                and (u["email"].lower() == wanted
                     or wanted in [x.lower() for x in u.get("alternateEmails") or []]
                     or wanted in [x.lower() for x in u.get("sourceEmails") or []])
            ]
            hits.sort(key=lambda u: 0 if u["email"].lower() == wanted else 1 if wanted in [x.lower() for x in u.get("alternateEmails") or []] else 2)
            return [{"u": u} for u in hits[:2]]
        return []


@pytest.fixture
def neo4j_provider() -> Neo4jProvider:
    provider = Neo4jProvider(logger=MagicMock(), config_service=MagicMock())
    provider.client = AsyncMock()
    return provider


@pytest.fixture
def arango_provider() -> ArangoHTTPProvider:
    provider = ArangoHTTPProvider(MagicMock(), AsyncMock())
    provider.http_client = AsyncMock()
    return provider


class TestHelpers:
    def test_alternate_email_matches_is_case_insensitive_and_ignores_empty(self) -> None:
        assert alternate_email_matches(["A@X.com", "b@x.com"], ["a@x.com", None, ""]) == ["A@X.com"]
        assert alternate_email_matches(["a@x.com"], None) == []
        assert alternate_email_matches(["a@x.com"], []) == []

    def test_user_model_reads_alternate_emails(self) -> None:
        user = User.from_arango_user(_platform_user_arango() | {"id": "platform-user"})
        assert user.alternate_emails == [ALTERNATE]
        assert User.from_arango_user({"_key": "u", "email": PRIMARY}).alternate_emails == []

    def test_user_model_reads_source_emails(self) -> None:
        user = User.from_arango_user({"_key": "u", "email": KHALID_SIGN_IN, "sourceEmails": [KHALID_MS_PRIMARY]})
        assert user.source_emails == [KHALID_MS_PRIMARY]
        assert User.from_arango_user({"_key": "u", "email": PRIMARY}).source_emails == []

    def test_normalize_alternate_emails(self) -> None:
        assert normalize_alternate_emails("Khalid@favapps.co", [" Khalid@Edrak.com ", "khalid@favapps.co", "khalid@edrak.com", "", None, "not-an-address"]) == ["khalid@edrak.com"]
        assert normalize_alternate_emails(PRIMARY, None) == []

    def test_app_user_normalizes_alternates_and_persists_them(self) -> None:
        app_user = _app_user(KHALID_MS_PRIMARY, ["Khalid@FavApps.co", KHALID_MS_PRIMARY, "khalid@favapps.co"])
        assert app_user.alternate_emails == [KHALID_SIGN_IN]
        assert app_user.to_arango_base_user()["alternateEmails"] == [KHALID_SIGN_IN]
        assert AppUser.from_arango_user(app_user.to_arango_base_user() | {"connectorId": "conn-1"}).alternate_emails == [KHALID_SIGN_IN]
        assert _app_user(PRIMARY).to_arango_base_user()["alternateEmails"] == []


class TestNeo4jQueries:
    async def test_get_user_by_email_matches_alternate_and_scopes_org(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(return_value=[{"u": _platform_user_neo4j()}])

        user = await neo4j_provider.get_user_by_email(ALTERNATE, org_id="org-1")

        kwargs = neo4j_provider.client.execute_query.await_args.kwargs
        query = neo4j_provider.client.execute_query.await_args.args[0]
        assert NEO4J_CLAUSE in query
        assert NEO4J_RANK in query
        assert "AND u.orgId = $org_id" in query
        assert "LIMIT 2" in query
        assert kwargs["parameters"] == {"email": ALTERNATE, "org_id": "org-1"}
        assert user is not None
        assert user.id == "platform-user"
        assert user.email == PRIMARY
        assert user.alternate_emails == [ALTERNATE]
        neo4j_provider.logger.warning.assert_not_called()

    async def test_get_user_by_email_warns_on_multiple_matches(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(
            return_value=[{"u": _platform_user_neo4j()}, {"u": {"id": "stale", "email": ALTERNATE}}]
        )

        user = await neo4j_provider.get_user_by_email(ALTERNATE)

        assert user is not None
        assert user.id == "platform-user"
        neo4j_provider.logger.warning.assert_called_once()

    async def test_get_app_user_by_email_uses_alternate_clause(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(return_value=[])

        assert await neo4j_provider.get_app_user_by_email(ALTERNATE, "conn-1") is None

        query = neo4j_provider.client.execute_query.await_args.args[0]
        assert NEO4J_CLAUSE in query
        assert "MATCH (app:App {id: $connector_id})" in query
        assert neo4j_provider.client.execute_query.await_args.kwargs["parameters"] == {
            "email": ALTERNATE,
            "connector_id": "conn-1",
        }

    async def test_get_entity_id_by_email_uses_alternate_clause(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(return_value=[{"id": "platform-user"}])

        assert await neo4j_provider.get_entity_id_by_email(ALTERNATE) == "platform-user"

        query = neo4j_provider.client.execute_query.await_args.args[0]
        assert NEO4J_CLAUSE.replace("u.", "n.") in query
        assert NEO4J_RANK.replace("u.", "n.") in query
        assert "LIMIT 1" in query

    async def test_bulk_get_entity_ids_maps_alternates_but_prefers_primary(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(
            return_value=[
                {"email": PRIMARY, "alternateEmails": [ALTERNATE, "Shared@x.com"], "id": "platform-user", "labels": ["User"]},
                {"email": "shared@x.com", "alternateEmails": None, "id": "other-user", "labels": ["User"]},
                {"email": "team@x.com", "alternateEmails": None, "id": "grp", "labels": ["Group"]},
            ]
        )

        result = await neo4j_provider.bulk_get_entity_ids_by_email([ALTERNATE, "shared@x.com", "team@x.com"])

        query = neo4j_provider.client.execute_query.await_args.args[0]
        assert "any(x IN coalesce(n.alternateEmails, []) WHERE toLower(x) IN [e IN $emails | toLower(e)])" in query
        assert "any(x IN coalesce(n.sourceEmails, []) WHERE toLower(x) IN [e IN $emails | toLower(e)])" in query
        assert "n.sourceEmails AS sourceEmails" in query
        assert result[ALTERNATE] == ("platform-user", CollectionNames.USERS.value, "USER")
        # shared@x.com is other-user's primary and platform-user's alternate: the primary wins
        assert result["shared@x.com"] == ("other-user", CollectionNames.USERS.value, "USER")
        assert result["team@x.com"] == ("grp", CollectionNames.GROUPS.value, "GROUP")

    async def test_bulk_get_entity_ids_ranks_source_emails_last(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(
            return_value=[
                {"email": KHALID_SIGN_IN, "alternateEmails": [], "sourceEmails": [KHALID_MS_PRIMARY, "Shared@x.com"], "id": "khalid", "labels": ["User"]},
                {"email": "other@x.com", "alternateEmails": ["shared@x.com"], "sourceEmails": None, "id": "other", "labels": ["User"]},
            ]
        )

        result = await neo4j_provider.bulk_get_entity_ids_by_email([KHALID_MS_PRIMARY, "shared@x.com", KHALID_SIGN_IN])

        assert result[KHALID_MS_PRIMARY] == ("khalid", CollectionNames.USERS.value, "USER")
        assert result[KHALID_SIGN_IN] == ("khalid", CollectionNames.USERS.value, "USER")
        # shared@x.com is other's alternate (linked sign-in) and only a source e-mail of khalid: the alternate wins
        assert result["shared@x.com"] == ("other", CollectionNames.USERS.value, "USER")


class TestNeo4jAppUserLinking:
    async def test_app_user_with_alternate_email_links_to_platform_user(self, neo4j_provider: Neo4jProvider) -> None:
        """An AppUser whose e-mail is an alternate reuses the platform User node instead of creating one."""
        async def execute_query(query: str, parameters: dict | None = None, txn_id: str | None = None) -> list:
            if "MATCH (u:User)" in query and parameters and parameters.get("email", "").lower() in {PRIMARY, ALTERNATE}:
                return [{"u": _platform_user_neo4j()}]
            return []

        neo4j_provider.client.execute_query = AsyncMock(side_effect=execute_query)
        neo4j_provider.get_document = AsyncMock(return_value={"id": "conn-1"})  # type: ignore[method-assign]
        neo4j_provider.batch_upsert_nodes = AsyncMock()  # type: ignore[method-assign]
        neo4j_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await neo4j_provider.batch_upsert_app_users([_app_user(ALTERNATE)])

        neo4j_provider.batch_upsert_nodes.assert_not_awaited()
        neo4j_provider.batch_create_edges.assert_awaited_once()
        edges = neo4j_provider.batch_create_edges.await_args.args[0]
        assert neo4j_provider.batch_create_edges.await_args.kwargs["collection"] == CollectionNames.USER_APP_RELATION.value
        assert edges[0]["from_id"] == "platform-user"
        assert edges[0]["to_id"] == "conn-1"
        assert edges[0]["sourceUserId"] == "entra-1"

    async def test_unknown_app_user_still_creates_inactive_user(self, neo4j_provider: Neo4jProvider) -> None:
        created: dict = {}

        async def execute_query(query: str, parameters: dict | None = None, txn_id: str | None = None) -> list:
            if "MATCH (u:User)" in query and created:
                return [{"u": created}]
            return []

        async def upsert(nodes: list, collection: str, transaction: str | None = None) -> None:
            created.update(nodes[0])

        neo4j_provider.client.execute_query = AsyncMock(side_effect=execute_query)
        neo4j_provider.get_document = AsyncMock(return_value={"id": "conn-1"})  # type: ignore[method-assign]
        neo4j_provider.batch_upsert_nodes = AsyncMock(side_effect=upsert)  # type: ignore[method-assign]
        neo4j_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await neo4j_provider.batch_upsert_app_users([_app_user("nobody@demo.edrak.com")])

        neo4j_provider.batch_upsert_nodes.assert_awaited_once()
        assert created["email"] == "nobody@demo.edrak.com"
        assert created["isActive"] is False
        assert neo4j_provider.batch_create_edges.await_count == 2  # BELONGS_TO + USER_APP_RELATION

    async def test_app_user_alternate_links_to_platform_user_and_records_source_emails(self, neo4j_provider: Neo4jProvider) -> None:
        """(a) AppUser(primary=onmicrosoft, alternates=[favapps]) links to User(email=favapps): no placeholder node."""
        fake = FakeNeo4jUsers([_khalid_neo4j()])
        neo4j_provider.client.execute_query = AsyncMock(side_effect=fake.execute_query)
        neo4j_provider.get_document = AsyncMock(return_value={"id": "conn-1"})  # type: ignore[method-assign]
        neo4j_provider.batch_upsert_nodes = AsyncMock()  # type: ignore[method-assign]
        neo4j_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await neo4j_provider.batch_upsert_app_users([_app_user(KHALID_MS_PRIMARY, [KHALID_SIGN_IN])])

        neo4j_provider.batch_upsert_nodes.assert_not_awaited()
        looked_up = [c.kwargs["parameters"]["email"] for c in neo4j_provider.client.execute_query.await_args_list if "MATCH (u:User)" in c.args[0]]
        assert looked_up == [KHALID_MS_PRIMARY, KHALID_SIGN_IN]  # primary first, then the alias hits
        assert fake.merges == [{"user_id": "khalid", "emails": [KHALID_MS_PRIMARY, KHALID_SIGN_IN]}]
        merge_query = next(c.args[0] for c in neo4j_provider.client.execute_query.await_args_list if "SET u.sourceEmails" in c.args[0])
        assert "MATCH (u:User {id: $user_id})" in merge_query
        assert "coalesce(u.sourceEmails, [])" in merge_query  # union with what is already there
        assert "WHERE x <> toLower(u.email)" in merge_query  # never the user's own address
        assert "alternateEmails" not in merge_query  # that property is edrak-ai's
        edges = neo4j_provider.batch_create_edges.await_args.args[0]
        assert edges[0]["from_id"] == "khalid" and edges[0]["to_id"] == "conn-1"

    async def test_source_email_resolves_after_linking(self, neo4j_provider: Neo4jProvider) -> None:
        """(a, continued) once recorded, the source primary resolves to the platform user everywhere."""
        fake = FakeNeo4jUsers([_khalid_neo4j(source_emails=[KHALID_MS_PRIMARY])])
        neo4j_provider.client.execute_query = AsyncMock(side_effect=fake.execute_query)

        user = await neo4j_provider.get_user_by_email(KHALID_MS_PRIMARY, org_id="org-1")
        assert user is not None and user.id == "khalid" and user.source_emails == [KHALID_MS_PRIMARY]

        neo4j_provider.client.execute_query = AsyncMock(
            return_value=[{"email": KHALID_SIGN_IN, "alternateEmails": [], "sourceEmails": [KHALID_MS_PRIMARY], "id": "khalid", "labels": ["User"]}]
        )
        result = await neo4j_provider.bulk_get_entity_ids_by_email([KHALID_MS_PRIMARY])
        assert result[KHALID_MS_PRIMARY] == ("khalid", CollectionNames.USERS.value, "USER")

    async def test_second_connector_unions_into_source_emails(self, neo4j_provider: Neo4jProvider) -> None:
        """(b) a second connector with another primary and the same alias adds to sourceEmails rather than replacing."""
        fake = FakeNeo4jUsers([_khalid_neo4j(source_emails=[KHALID_MS_PRIMARY])])
        neo4j_provider.client.execute_query = AsyncMock(side_effect=fake.execute_query)
        neo4j_provider.get_document = AsyncMock(return_value={"id": "conn-google"})  # type: ignore[method-assign]
        neo4j_provider.batch_upsert_nodes = AsyncMock()  # type: ignore[method-assign]
        neo4j_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await neo4j_provider.batch_upsert_app_users([_app_user(KHALID_GOOGLE_PRIMARY, [KHALID_SIGN_IN], connector_id="conn-google")])

        neo4j_provider.batch_upsert_nodes.assert_not_awaited()
        assert fake.merges == [{"user_id": "khalid", "emails": [KHALID_GOOGLE_PRIMARY, KHALID_SIGN_IN]}]
        merge_query = next(c.args[0] for c in neo4j_provider.client.execute_query.await_args_list if "SET u.sourceEmails" in c.args[0])
        # the merge is a set-union computed in the database, so the earlier connector's address survives
        assert "reduce(acc = [], x IN candidates | CASE WHEN x IN acc THEN acc ELSE acc + x END)" in merge_query

    async def test_app_user_matching_nobody_creates_placeholder_with_alternates(self, neo4j_provider: Neo4jProvider) -> None:
        """(c) no platform user under any address: the inactive placeholder is created as before, carrying the aliases."""
        created: dict = {}
        fake = FakeNeo4jUsers([])

        async def upsert(nodes: list, collection: str, transaction: str | None = None) -> None:
            created.update(nodes[0])
            fake.users.append(nodes[0])

        neo4j_provider.client.execute_query = AsyncMock(side_effect=fake.execute_query)
        neo4j_provider.get_document = AsyncMock(return_value={"id": "conn-1"})  # type: ignore[method-assign]
        neo4j_provider.batch_upsert_nodes = AsyncMock(side_effect=upsert)  # type: ignore[method-assign]
        neo4j_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await neo4j_provider.batch_upsert_app_users([_app_user("nobody@edrak.onmicrosoft.com", ["nobody@favapps.co"])])

        neo4j_provider.batch_upsert_nodes.assert_awaited_once()
        assert created["email"] == "nobody@edrak.onmicrosoft.com"
        assert created["alternateEmails"] == ["nobody@favapps.co"]
        assert created["isActive"] is False
        assert fake.merges == []  # nothing to link to, so no sourceEmails written
        assert neo4j_provider.batch_create_edges.await_count == 2  # BELONGS_TO + USER_APP_RELATION

    async def test_alias_on_two_platform_users_picks_by_ranking_and_warns(self, neo4j_provider: Neo4jProvider) -> None:
        """(d) best-effort sourceEmails: the same address on two users resolves to the higher-ranked one with a warning."""
        fake = FakeNeo4jUsers([
            {"id": "by-source", "orgId": "org-1", "email": "someone@edrak.com", "sourceEmails": [KHALID_SIGN_IN]},
            _khalid_neo4j(),
        ])
        neo4j_provider.client.execute_query = AsyncMock(side_effect=fake.execute_query)

        user = await neo4j_provider.get_user_by_email(KHALID_SIGN_IN, org_id="org-1")

        assert user is not None and user.id == "khalid"  # primary address beats a source-e-mail match
        neo4j_provider.logger.warning.assert_called_once()
        assert "2 users match" in neo4j_provider.logger.warning.call_args.args[0]


class TestArangoQueries:
    async def test_get_user_by_email_matches_alternate_and_scopes_org(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(return_value=[_platform_user_arango()])

        user = await arango_provider.get_user_by_email(ALTERNATE, org_id="org-1")

        query = arango_provider.http_client.execute_aql.await_args.args[0]
        kwargs = arango_provider.http_client.execute_aql.await_args.kwargs
        assert AQL_CLAUSE in query
        assert AQL_RANK in query
        assert "AND user.orgId == @org_id" in query
        assert "LIMIT 2" in query
        assert kwargs["bind_vars"] == {"email": ALTERNATE, "org_id": "org-1"}
        assert user is not None
        assert user.id == "platform-user"
        assert user.alternate_emails == [ALTERNATE]

    async def test_get_user_by_email_warns_on_multiple_matches(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(
            return_value=[_platform_user_arango(), {"_key": "stale", "email": ALTERNATE}]
        )

        user = await arango_provider.get_user_by_email(ALTERNATE)

        assert user is not None
        assert user.id == "platform-user"
        arango_provider.logger.warning.assert_called_once()

    async def test_get_app_user_by_email_uses_alternate_clause(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(return_value=[None])

        assert await arango_provider.get_app_user_by_email(ALTERNATE, "conn-1") is None

        query = arango_provider.http_client.execute_aql.await_args.args[0]
        assert AQL_CLAUSE.replace("user.", "u.") in query

    async def test_get_entity_id_by_email_uses_alternate_clause(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(return_value=["platform-user"])

        assert await arango_provider.get_entity_id_by_email(ALTERNATE) == "platform-user"

        query = arango_provider.http_client.execute_aql.await_args.args[0]
        assert AQL_CLAUSE.replace("user.", "doc.") in query
        assert AQL_RANK.replace("user.", "doc.") in query
        assert f"FOR doc IN {CollectionNames.USERS.value}" in query

    async def test_bulk_get_entity_ids_maps_alternates_but_prefers_primary(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(
            side_effect=[
                [
                    {"email": PRIMARY, "alternateEmails": [ALTERNATE, "shared@x.com"], "id": "platform-user"},
                    {"email": "shared@x.com", "alternateEmails": None, "id": "other-user"},
                ],
                [{"email": "team@x.com", "id": "grp"}],
            ]
        )

        result = await arango_provider.bulk_get_entity_ids_by_email([ALTERNATE, "shared@x.com", "team@x.com"])

        first_call = arango_provider.http_client.execute_aql.await_args_list[0]
        assert "INTERSECTION(" in first_call.args[0]
        assert "doc.alternateEmails" in first_call.args[0]
        assert "doc.sourceEmails" in first_call.args[0]
        assert first_call.kwargs["bind_vars"]["emails_lower"] == [ALTERNATE, "shared@x.com", "team@x.com"] or set(
            first_call.kwargs["bind_vars"]["emails_lower"]
        ) == {ALTERNATE, "shared@x.com", "team@x.com"}
        assert result[ALTERNATE] == ("platform-user", CollectionNames.USERS.value, "USER")
        assert result["shared@x.com"] == ("other-user", CollectionNames.USERS.value, "USER")
        assert result["team@x.com"] == ("grp", CollectionNames.GROUPS.value, "GROUP")
        # the group query only receives the e-mails the users query did not resolve
        second_call = arango_provider.http_client.execute_aql.await_args_list[1]
        assert second_call.kwargs["bind_vars"]["emails"] == ["team@x.com"]


class TestArangoAppUserLinking:
    async def test_app_user_with_alternate_email_links_to_platform_user(self, arango_provider: ArangoHTTPProvider) -> None:
        async def execute_aql(query: str, bind_vars: dict | None = None, txn_id: str | None = None) -> list:
            if f"FOR user IN {CollectionNames.USERS.value}" in query and bind_vars and bind_vars.get("email", "").lower() in {
                PRIMARY,
                ALTERNATE,
            }:
                return [_platform_user_arango()]
            return []

        arango_provider.http_client.execute_aql = AsyncMock(side_effect=execute_aql)
        arango_provider.get_document = AsyncMock(return_value={"_id": f"{CollectionNames.APPS.value}/conn-1"})  # type: ignore[method-assign]
        arango_provider.batch_upsert_nodes = AsyncMock()  # type: ignore[method-assign]
        arango_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await arango_provider.batch_upsert_app_users([_app_user(ALTERNATE)])

        arango_provider.batch_upsert_nodes.assert_not_awaited()
        arango_provider.batch_create_edges.assert_awaited_once()
        edges = arango_provider.batch_create_edges.await_args.args[0]
        assert arango_provider.batch_create_edges.await_args.kwargs["collection"] == CollectionNames.USER_APP_RELATION.value
        assert edges[0]["_from"] == f"{CollectionNames.USERS.value}/platform-user"
        assert edges[0]["_to"] == f"{CollectionNames.APPS.value}/conn-1"
        assert edges[0]["sourceUserId"] == "entra-1"

    async def test_app_user_alternate_links_and_records_source_emails(self, arango_provider: ArangoHTTPProvider) -> None:
        merges: list[dict] = []

        async def execute_aql(query: str, bind_vars: dict | None = None, txn_id: str | None = None) -> list:
            bind_vars = bind_vars or {}
            if "UPDATE u WITH" in query:
                merges.append(bind_vars)
                return [[KHALID_MS_PRIMARY]]
            if f"FOR user IN {CollectionNames.USERS.value}" in query and bind_vars.get("email", "").lower() == KHALID_SIGN_IN:
                return [{"_key": "khalid", "orgId": "org-1", "email": KHALID_SIGN_IN}]
            return []

        arango_provider.http_client.execute_aql = AsyncMock(side_effect=execute_aql)
        arango_provider.get_document = AsyncMock(return_value={"_id": f"{CollectionNames.APPS.value}/conn-1"})  # type: ignore[method-assign]
        arango_provider.batch_upsert_nodes = AsyncMock()  # type: ignore[method-assign]
        arango_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await arango_provider.batch_upsert_app_users([_app_user(KHALID_MS_PRIMARY, [KHALID_SIGN_IN])])

        arango_provider.batch_upsert_nodes.assert_not_awaited()
        looked_up = [c.kwargs["bind_vars"]["email"] for c in arango_provider.http_client.execute_aql.await_args_list if "FOR user IN" in c.args[0]]
        assert looked_up == [KHALID_MS_PRIMARY, KHALID_SIGN_IN]
        assert merges == [{"user_key": "khalid", "emails": [KHALID_MS_PRIMARY, KHALID_SIGN_IN]}]
        merge_query = next(c.args[0] for c in arango_provider.http_client.execute_aql.await_args_list if "UPDATE u WITH" in c.args[0])
        assert "APPEND((FOR x IN (u.sourceEmails || []) RETURN LOWER(x))" in merge_query  # union, not replace
        assert "REMOVE_VALUE(UNIQUE(candidates), LOWER(u.email))" in merge_query
        assert "alternateEmails" not in merge_query
        edges = arango_provider.batch_create_edges.await_args.args[0]
        assert edges[0]["_from"] == f"{CollectionNames.USERS.value}/khalid"

    async def test_placeholder_carries_alternates_when_nobody_matches(self, arango_provider: ArangoHTTPProvider) -> None:
        created: dict = {}

        async def execute_aql(query: str, bind_vars: dict | None = None, txn_id: str | None = None) -> list:
            if f"FOR user IN {CollectionNames.USERS.value}" in query and created:
                return [created]
            return []

        async def upsert(nodes: list, collection: str, transaction: str | None = None) -> None:
            created.update(nodes[0])

        arango_provider.http_client.execute_aql = AsyncMock(side_effect=execute_aql)
        arango_provider.get_document = AsyncMock(return_value={"_id": f"{CollectionNames.APPS.value}/conn-1"})  # type: ignore[method-assign]
        arango_provider.batch_upsert_nodes = AsyncMock(side_effect=upsert)  # type: ignore[method-assign]
        arango_provider.batch_create_edges = AsyncMock()  # type: ignore[method-assign]

        await arango_provider.batch_upsert_app_users([_app_user("nobody@edrak.onmicrosoft.com", ["nobody@favapps.co"])])

        assert created["email"] == "nobody@edrak.onmicrosoft.com"
        assert created["alternateEmails"] == ["nobody@favapps.co"]
        assert created["isActive"] is False
        assert not any("UPDATE u WITH" in c.args[0] for c in arango_provider.http_client.execute_aql.await_args_list)
        assert arango_provider.batch_create_edges.await_count == 2

    async def test_bulk_get_entity_ids_ranks_source_emails_last(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(
            side_effect=[
                [
                    {"email": KHALID_SIGN_IN, "alternateEmails": [], "sourceEmails": [KHALID_MS_PRIMARY, "shared@x.com"], "id": "khalid"},
                    {"email": "other@x.com", "alternateEmails": ["shared@x.com"], "sourceEmails": None, "id": "other"},
                ],
            ]
        )

        result = await arango_provider.bulk_get_entity_ids_by_email([KHALID_MS_PRIMARY, "shared@x.com"])

        assert result[KHALID_MS_PRIMARY] == ("khalid", CollectionNames.USERS.value, "USER")
        assert result["shared@x.com"] == ("other", CollectionNames.USERS.value, "USER")
        assert arango_provider.http_client.execute_aql.await_count == 1  # everything resolved by users; no group query
