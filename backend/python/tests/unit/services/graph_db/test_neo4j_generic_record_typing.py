"""Generic (typeless) records must round-trip out of Neo4j as the base Record.

Connectors such as Dynamics 365, Business Central and SAP write their rows as
bare ``Record`` instances with no IS_OF_TYPE node. ``_create_typed_record_from_neo4j``
used to raise for those, so ``get_record_by_id`` returned None and the internal
stream route answered 404 "Record not found" — the indexer then failed the record.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import Connectors
from app.models.entities import FileRecord, Record, RecordType
from app.services.graph_db.neo4j.neo4j_provider import Neo4jProvider


@pytest.fixture
def neo4j_provider() -> Neo4jProvider:
    provider = Neo4jProvider(logger=MagicMock(), config_service=MagicMock())
    provider.client = AsyncMock()
    return provider


def _dynamics_account_record(**overrides: object) -> dict:
    record = {
        "id": "rec-account-1",
        "orgId": "org-1",
        "recordName": "Contoso Ltd",
        "recordType": RecordType.OTHERS.value,
        "externalRecordId": "account:0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
        "externalRevisionId": "1757500000000",
        "version": 0,
        "origin": "CONNECTOR",
        "connectorName": Connectors.MICROSOFT_DYNAMICS_365.value,
        "connectorId": "conn-d365",
        "mimeType": "text/markdown",
        "virtualRecordId": "vrid-1",
        "webUrl": "https://org.crm.dynamics.com/main.aspx?etn=account",
        "createdAtTimestamp": 1757500000000,
        "updatedAtTimestamp": 1757500000000,
        "indexingStatus": "QUEUED",
    }
    record.update(overrides)
    return record


class TestCreateTypedRecordFromNeo4j:
    def test_no_type_doc_returns_base_record(self, neo4j_provider: Neo4jProvider) -> None:
        record_dict = neo4j_provider._neo4j_to_arango_node(_dynamics_account_record(), "records")

        record = neo4j_provider._create_typed_record_from_neo4j(record_dict, None)

        assert type(record) is Record
        assert record.id == "rec-account-1"
        assert record.org_id == "org-1"
        assert record.record_type == RecordType.OTHERS
        assert record.external_record_id == "account:0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b"
        assert record.connector_name == Connectors.MICROSOFT_DYNAMICS_365
        assert record.connector_id == "conn-d365"
        assert record.mime_type == "text/markdown"
        assert record.virtual_record_id == "vrid-1"
        assert record.version == 0
        assert record.weburl == "https://org.crm.dynamics.com/main.aspx?etn=account"
        # The fields the record-event payload and the stream route read
        payload = record.to_kafka_record()
        assert payload["externalRecordId"] == record.external_record_id
        assert payload["connectorName"] == Connectors.MICROSOFT_DYNAMICS_365.value
        assert payload["recordType"] == RecordType.OTHERS.value

    def test_unmapped_type_with_stray_type_doc_returns_base_record(self, neo4j_provider: Neo4jProvider) -> None:
        # SHAREPOINT_LIST_ITEM has no entry in RECORD_TYPE_COLLECTION_MAPPING
        record_dict = _dynamics_account_record(recordType=RecordType.SHAREPOINT_LIST_ITEM.value)

        record = neo4j_provider._create_typed_record_from_neo4j(record_dict, {"some": "doc"})

        assert type(record) is Record
        assert record.record_type == RecordType.SHAREPOINT_LIST_ITEM

    def test_mapped_type_still_returns_subclass(self, neo4j_provider: Neo4jProvider) -> None:
        record_dict = _dynamics_account_record(
            recordType=RecordType.FILE.value,
            connectorName=Connectors.GOOGLE_DRIVE.value,
            externalRecordId="file-123",
        )
        type_doc = {"isFile": True, "extension": "pdf", "path": "/docs/a.pdf"}

        record = neo4j_provider._create_typed_record_from_neo4j(record_dict, type_doc)

        assert isinstance(record, FileRecord)
        assert record.extension == "pdf"
        assert record.path == "/docs/a.pdf"
        assert record.external_record_id == "file-123"

    def test_malformed_record_dict_still_raises(self, neo4j_provider: Neo4jProvider) -> None:
        record_dict = _dynamics_account_record()
        del record_dict["orgId"]

        with pytest.raises(ValueError, match="Failed to create base record"):
            neo4j_provider._create_typed_record_from_neo4j(record_dict, None)


class TestGetRecordByIdGenericRecord:
    @pytest.mark.asyncio
    async def test_returns_base_record_when_neo4j_has_no_type_doc(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(
            return_value=[{"record": _dynamics_account_record(), "typeDoc": None}]
        )

        record = await neo4j_provider.get_record_by_id("rec-account-1")

        assert record is not None
        assert type(record) is Record
        assert record.id == "rec-account-1"
        assert record.org_id == "org-1"
        assert record.connector_name == Connectors.MICROSOFT_DYNAMICS_365
        assert record.external_record_id == "account:0f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b"
        neo4j_provider.client.execute_query.assert_awaited_once()
        _, kwargs = neo4j_provider.client.execute_query.call_args
        assert kwargs["parameters"] == {"record_id": "rec-account-1"}

    @pytest.mark.asyncio
    async def test_returns_none_when_record_missing(self, neo4j_provider: Neo4jProvider) -> None:
        neo4j_provider.client.execute_query = AsyncMock(return_value=[])

        assert await neo4j_provider.get_record_by_id("nope") is None
