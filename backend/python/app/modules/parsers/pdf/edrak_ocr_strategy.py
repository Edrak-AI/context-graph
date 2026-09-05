"""Edrak OCR strategy: Tesseract (ara+eng) via edrak-ai's OCR worker.

Same contract as VLMOCRStrategy — ``load_document(bytes)`` then ``document_analysis_result`` with
``{"pages": [{"page_number", "markdown", "width", "height"}], "markdown", "total_pages"}`` — so the
indexing pipeline and the OCR parser treat the result exactly like VLM output. Only pages that
``OCRStrategy.needs_ocr`` flags are sent; pages with native text keep pdfplumber's extraction.

Why: Edrak's documents are mostly Arabic scans. The pgvector pipeline already OCRs them with
Tesseract ara+eng on a dedicated Cloud Run worker; reusing it keeps one OCR engine, avoids a vision
LLM call per page, and keeps the bytes inside the same GCP project.

Config (env on the CGraph pod): ``EDRAK_OCR_URL`` = edrak-ai ``POST /api/internal/ocr`` on the OCR
worker; auth is a scoped HS256 JWT (scope ``edrak:ocr``) signed with the shared ``scopedJwtSecret``.
"""

from __future__ import annotations

import asyncio
import base64
import os
import time
from io import BytesIO
from typing import Any, Dict, List, Optional

import httpx
import jwt
import pdfplumber

from app.config.constants.service import config_node_constants
from app.exceptions.indexing_exceptions import DocumentProcessingError
from app.modules.parsers.pdf.ocr_handler import OCRStrategy

_URL_ENV = "EDRAK_OCR_URL"
_SCOPE = "edrak:ocr"
_TIMEOUT_S = float(os.getenv("EDRAK_OCR_TIMEOUT_S", "900"))
_PAGES_PER_REQUEST = int(os.getenv("EDRAK_OCR_PAGES_PER_REQUEST", "25"))


class EdrakOCRStrategy(OCRStrategy):
    def __init__(self, logger, config) -> None:
        super().__init__(logger)
        self.config = config
        self.document_analysis_result: Optional[Dict[str, Any]] = None
        self._url = os.getenv(_URL_ENV, "").strip()

    async def _token(self) -> str:
        secret_keys = await self.config.get_config(config_node_constants.SECRET_KEYS.value)
        secret = (secret_keys or {}).get("scopedJwtSecret") if isinstance(secret_keys, dict) else None
        if not secret:
            raise DocumentProcessingError("edrakOCR: scopedJwtSecret is not configured")
        now = int(time.time())
        return jwt.encode(
            {"scopes": [_SCOPE], "issuer": "cgraph", "iat": now, "exp": now + 900},
            secret,
            algorithm="HS256",
        )

    async def _ocr_pages(self, content: bytes, pages: List[int]) -> Dict[int, str]:
        if not pages:
            return {}
        if not self._url:
            raise DocumentProcessingError(f"edrakOCR: {_URL_ENV} is not set")
        token = await self._token()
        payload_pdf = base64.b64encode(content).decode("ascii")
        out: Dict[int, str] = {}
        async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
            for i in range(0, len(pages), _PAGES_PER_REQUEST):
                chunk = pages[i : i + _PAGES_PER_REQUEST]
                resp = await client.post(
                    self._url,
                    json={"pdfBase64": payload_pdf, "pages": chunk},
                    headers={"Authorization": f"Bearer {token}"},
                )
                if resp.status_code >= 400:
                    raise DocumentProcessingError(
                        f"edrakOCR: HTTP {resp.status_code}", details={"body": resp.text[:300]}
                    )
                for item in resp.json().get("pages", []):
                    out[int(item["page"])] = item.get("text", "") or ""
        return out

    async def process_page(self, page) -> Dict[str, Any]:  # pragma: no cover — batched in load_document
        raise NotImplementedError("EdrakOCRStrategy processes pages in batches via load_document")

    async def load_document(self, content: bytes) -> None:
        self.logger.info("📥 Loading document for Edrak (Tesseract) OCR")
        try:
            with pdfplumber.open(BytesIO(content)) as doc:
                native: Dict[int, str] = {}
                dims: Dict[int, tuple] = {}
                to_ocr: List[int] = []
                for idx, page in enumerate(doc.pages, start=1):
                    dims[idx] = (page.width, page.height)
                    if OCRStrategy.needs_ocr(page, self.logger):
                        to_ocr.append(idx)
                    else:
                        native[idx] = (page.extract_text() or "").strip()
                total = len(doc.pages)

            self.logger.info("🔎 %d/%d pages need OCR", len(to_ocr), total)
            recognized = await self._ocr_pages(content, to_ocr) if to_ocr else {}

            pages_results = []
            for idx in range(1, total + 1):
                text = recognized.get(idx) if idx in recognized else native.get(idx, "")
                w, h = dims[idx]
                pages_results.append(
                    {"page_number": idx, "markdown": text or "", "width": w, "height": h}
                )
            self.document_analysis_result = {
                "pages": pages_results,
                "markdown": "\n\n---\n\n".join(p["markdown"] for p in pages_results),
                "total_pages": total,
            }
            self.logger.info("✅ Edrak OCR completed (%d pages)", total)
        except DocumentProcessingError:
            raise
        except Exception as exc:
            self.logger.error("❌ Edrak OCR failed: %s", exc)
            raise DocumentProcessingError(f"edrakOCR failed: {exc}") from exc
        # Yield so a long batch does not starve the event loop before the caller continues.
        await asyncio.sleep(0)
