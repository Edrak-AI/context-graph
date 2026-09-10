"""Alternate e-mail resolution: a User node's `alternateEmails` resolve like its primary e-mail."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames, Connectors
from app.models.entities import AppUser, User
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider
from app.services.graph_db.common.utils import alternate_email_matches
from app.services.graph_db.neo4j.neo4j_provider import Neo4jProvider

PRIMARY = "sujit@edrak.com"
ALTERNATE = "sujit@demo.edrak.com"
NEO4J_CLAUSE = (
    "(toLower(u.email) = toLower($email) "
    "OR toLower($email) IN [x IN coalesce(u.alternateEmails, []) | toLower(x)])"
)
AQL_CLAUSE = (
    "(LOWER(user.email) == LOWER(@email) "
    "OR LOWER(@email) IN (FOR x IN (user.alternateEmails || []) RETURN LOWER(x)))"
)


def _platform_user_neo4j() -> dict:
    return {"id": "platform-user", "orgId": "org-1", "email": PRIMARY, "alternateEmails": [ALTERNATE]}


def _platform_user_arango() -> dict:
    return {"_key": "platform-user", "orgId": "org-1", "email": PRIMARY, "alternateEmails": [ALTERNATE]}


def _app_user(email: str = ALTERNATE) -> AppUser:
    return AppUser(
        app_name=Connectors.ONEDRIVE,
        connector_id="conn-1",
        source_user_id="entra-1",
        org_id="org-1",
        email=email,
        full_name="Sujit",
    )


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


class TestNeo4jQueries:
    async def test_get_user_by_email_matches_alternate_and_scopes_org(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(return_value=[{"u": _platform_user_neo4j()}])

        user = await neo4j_provider.get_user_by_email(ALTERNATE, org_id="org-1")

        kwargs = neo4j_provider.client.execute_query.await_args.kwargs
        query = neo4j_provider.client.execute_query.await_args.args[0]
        assert NEO4J_CLAUSE in query
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
        assert result[ALTERNATE] == ("platform-user", CollectionNames.USERS.value, "USER")
        # shared@x.com is other-user's primary and platform-user's alternate: the primary wins
        assert result["shared@x.com"] == ("other-user", CollectionNames.USERS.value, "USER")
        assert result["team@x.com"] == ("grp", CollectionNames.GROUPS.value, "GROUP")


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


class TestArangoQueries:
    async def test_get_user_by_email_matches_alternate_and_scopes_org(self, arango_provider: ArangoHTTPProvider) -> None:
        arango_provider.http_client.execute_aql = AsyncMock(return_value=[_platform_user_arango()])

        user = await arango_provider.get_user_by_email(ALTERNATE, org_id="org-1")

        query = arango_provider.http_client.execute_aql.await_args.args[0]
        kwargs = arango_provider.http_client.execute_aql.await_args.kwargs
        assert AQL_CLAUSE in query
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
