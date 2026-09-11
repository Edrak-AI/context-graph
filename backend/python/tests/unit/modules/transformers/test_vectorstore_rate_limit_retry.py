"""Rate-limit (429 / RESOURCE_EXHAUSTED) retry around the dense embedding call.

Without it a Gemini quota burst failed the batch at once; the consumer's three
quick re-queues all landed inside the same quota window and the record went
terminal. Batch sizes and the per-record semaphore are deliberately untouched.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.documents import Document

from app.modules.transformers import vectorstore as vs_module
from tests.support.vector_db import make_collection_registry

# The exact text seen on US dev (image edrak19), as langchain_google_genai surfaces it.
GEMINI_429_TEXT = (
    "Error embedding content (RESOURCE_EXHAUSTED): 429 RESOURCE_EXHAUSTED. "
    "{'error': {'code': 429, 'message': 'You exceeded your current quota, please check your plan "
    "and billing details. * Quota exceeded for metric: "
    "generativelanguage.googleapis.com/embed_content_paid_tier_requests, limit: 3000, "
    "model: gemini-embedding-001\\nPlease retry in 21.7s.', 'status': 'RESOURCE_EXHAUSTED', "
    "'details': [{'@type': 'type.googleapis.com/google.rpc.RetryInfo', 'retryDelay': '21s'}]}}"
)


class FakeProviderError(Exception):
    """Stands in for GoogleGenerativeAIError: message only, no status attributes."""


class FakeStatusError(Exception):
    def __init__(self, status: str) -> None:
        super().__init__("provider call failed")
        self.status = status


def _make_vectorstore() -> vs_module.VectorStore:
    from app.services.vector_db.models import VectorDBCapabilities

    mock_vdb = AsyncMock()
    mock_vdb.get_capabilities = MagicMock(return_value=VectorDBCapabilities())
    mock_vdb.get_service_name = MagicMock(return_value="mock")
    vs = vs_module.VectorStore(
        logger=MagicMock(),
        config_service=AsyncMock(),
        graph_provider=AsyncMock(),
        collection_registry=make_collection_registry(),
        vector_db_service=mock_vdb,
    )
    vs._is_local_cpu_embedding = MagicMock(return_value=False)
    vs.graph_provider.get_document = AsyncMock(return_value={"_key": "rec-1"})
    vs._compute_sparse_embeddings = AsyncMock(side_effect=lambda texts: [None] * len(texts))
    return vs


@pytest.fixture
def no_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vs_module.random, "uniform", lambda a, b: 0.0)


@pytest.fixture
def fake_sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    sleep = AsyncMock()
    monkeypatch.setattr(vs_module.asyncio, "sleep", sleep)
    return sleep


class TestRateLimitDetection:
    def test_gemini_429_text_is_rate_limit(self) -> None:
        assert vs_module._is_rate_limit_error(FakeProviderError(GEMINI_429_TEXT))

    def test_status_attribute_on_cause_is_rate_limit(self) -> None:
        wrapper = FakeProviderError("Error embedding content")
        wrapper.__cause__ = FakeStatusError("RESOURCE_EXHAUSTED")
        assert vs_module._is_rate_limit_error(wrapper)

    def test_plain_error_is_not_rate_limit(self) -> None:
        assert not vs_module._is_rate_limit_error(FakeProviderError("Error embedding content (INVALID_ARGUMENT): 400 bad request"))
        assert not vs_module._is_rate_limit_error(ValueError("dimension mismatch for record 42942"))

    def test_parse_retry_delay_prefers_structured_hint(self) -> None:
        assert vs_module._parse_retry_delay_s(FakeProviderError(GEMINI_429_TEXT)) == 21.0
        assert vs_module._parse_retry_delay_s(FakeProviderError("429 Too Many Requests. Please retry in 7.5s.")) == 7.5
        assert vs_module._parse_retry_delay_s(FakeProviderError("429 Too Many Requests")) is None

    def test_backoff_without_hint_is_exponential_and_capped(self, no_jitter: None) -> None:
        err = FakeProviderError("429 Too Many Requests")
        assert vs_module._rate_limit_backoff_s(err, 1) == vs_module._EMBEDDING_RATE_LIMIT_BASE_DELAY_S
        assert vs_module._rate_limit_backoff_s(err, 2) == vs_module._EMBEDDING_RATE_LIMIT_BASE_DELAY_S * 2
        assert vs_module._rate_limit_backoff_s(err, 10) == vs_module._EMBEDDING_RATE_LIMIT_MAX_DELAY_S


class TestEmbedAndUpsertRetry:
    @pytest.mark.asyncio
    async def test_429_twice_then_success(self, fake_sleep: AsyncMock, no_jitter: None) -> None:
        vs = _make_vectorstore()
        vs.dense_embeddings = MagicMock()
        vs.dense_embeddings.aembed_documents = AsyncMock(
            side_effect=[FakeProviderError(GEMINI_429_TEXT), FakeProviderError(GEMINI_429_TEXT), [[0.1, 0.2]]]
        )
        docs = [Document(page_content="Contoso Ltd", metadata={"recordId": "rec-1"})]

        await vs._embed_and_upsert_documents(docs, "rec-1", "records")

        assert vs.dense_embeddings.aembed_documents.await_count == 3
        assert fake_sleep.await_count == 2
        assert [call.args[0] for call in fake_sleep.await_args_list] == [21.0, 21.0]
        vs.vector_db_service.upsert_points.assert_awaited_once()
        _, kwargs = vs.vector_db_service.upsert_points.call_args
        assert kwargs["collection_name"] == "records"
        assert len(kwargs["points"]) == 1
        assert kwargs["points"][0].dense_vector == [0.1, 0.2]
        assert vs.logger.info.call_count == 2

    @pytest.mark.asyncio
    async def test_non_429_error_fails_fast(self, fake_sleep: AsyncMock) -> None:
        vs = _make_vectorstore()
        vs.dense_embeddings = MagicMock()
        vs.dense_embeddings.aembed_documents = AsyncMock(
            side_effect=FakeProviderError("Error embedding content (INVALID_ARGUMENT): 400 bad request")
        )
        docs = [Document(page_content="x", metadata={})]

        with pytest.raises(FakeProviderError, match="INVALID_ARGUMENT"):
            await vs._embed_and_upsert_documents(docs, "rec-1", "records")

        assert vs.dense_embeddings.aembed_documents.await_count == 1
        fake_sleep.assert_not_awaited()
        vs.vector_db_service.upsert_points.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_persistent_429_gives_up_after_max_attempts(self, fake_sleep: AsyncMock, no_jitter: None) -> None:
        vs = _make_vectorstore()
        vs.dense_embeddings = MagicMock()
        vs.dense_embeddings.aembed_documents = AsyncMock(side_effect=FakeProviderError(GEMINI_429_TEXT))
        docs = [Document(page_content="x", metadata={})]

        with pytest.raises(FakeProviderError, match="RESOURCE_EXHAUSTED"):
            await vs._embed_and_upsert_documents(docs, "rec-1", "records")

        assert vs.dense_embeddings.aembed_documents.await_count == vs_module._EMBEDDING_RATE_LIMIT_MAX_ATTEMPTS
        assert fake_sleep.await_count == vs_module._EMBEDDING_RATE_LIMIT_MAX_ATTEMPTS - 1
        vs.vector_db_service.upsert_points.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_timeout_is_not_retried(self, fake_sleep: AsyncMock, monkeypatch: pytest.MonkeyPatch) -> None:
        vs = _make_vectorstore()
        vs.dense_embeddings = MagicMock()
        vs.dense_embeddings.aembed_documents = AsyncMock(side_effect=TimeoutError())
        docs = [Document(page_content="x", metadata={})]

        with pytest.raises(vs_module.EmbeddingError, match="timed out"):
            await vs._embed_and_upsert_documents(docs, "rec-1", "records")

        fake_sleep.assert_not_awaited()
