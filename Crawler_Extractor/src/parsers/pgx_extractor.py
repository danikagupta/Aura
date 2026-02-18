"""PGX sample-level extraction helpers using page-image chunks."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, List, Optional, Protocol, Tuple

import fitz
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from ..common.errors import ExtractionError

_DEFAULT_PAGES_PER_CHUNK = 3
_DEFAULT_ZOOM = 2.2
_RETRY_ZOOM = 3.0
_MAX_CHUNK_TEXT_CHARS = 12_000


@dataclass(slots=True)
class PgxExtractionRow:
    sample_id: str
    gene: str
    allele: str
    rs_id: str
    medication: str
    outcome: str
    actionability: str
    cpic_recommendation: str
    source_context: str


class PgxExtractor(Protocol):
    """Protocol implemented by PGX extractors."""

    def extract_pdf(
        self, pdf_bytes: bytes, *, pages_per_chunk: int = _DEFAULT_PAGES_PER_CHUNK
    ) -> List[PgxExtractionRow]: ...


class _PgxSampleSchema(BaseModel):
    sample_id: str = Field(..., description="Unique sample/case identifier")
    gene: str = Field(..., description="Pharmacogene, for example CYP2C19")
    allele: str = Field(..., description="Star-allele genotype")
    rs_id: str = Field(..., description="Mapped SNP rs identifier(s) or Whole Gene Seq")
    medication: str = Field(..., description="Medication name")
    outcome: str = Field(..., description="Clinical outcome")
    actionability: str = Field(..., description="Yes or No")
    cpic_recommendation: str = Field(..., description="CPIC guidance for the phenotype")
    source_context: str = Field(..., description="Sample context in paper")


class _PgxExtractionSchema(BaseModel):
    samples: List[_PgxSampleSchema]


class StructuredPgxExtractor:
    """LLM-based extractor that emits structured sample-level PGX observations."""

    def __init__(
        self,
        *,
        prompt: str,
        api_key: Optional[str] = None,
        model_name: str,
        llm: Optional[Any] = None,
    ):
        if not prompt.strip():
            raise ValueError("PGX prompt text is required")
        if not model_name or not model_name.strip():
            raise ValueError("Model name is required for PGX extraction")
        self._prompt = prompt
        if llm is not None:
            self._llm = llm
        else:
            if not api_key:
                raise ValueError("OpenAI API key is required for PGX extraction")
            from langchain_openai import ChatOpenAI

            self._llm = ChatOpenAI(api_key=api_key, model=model_name)

    def extract_pdf(
        self,
        pdf_bytes: bytes,
        *,
        pages_per_chunk: int = _DEFAULT_PAGES_PER_CHUNK,
    ) -> List[PgxExtractionRow]:
        rows = self._extract_with_zoom(
            pdf_bytes,
            pages_per_chunk=max(1, pages_per_chunk),
            zoom=_DEFAULT_ZOOM,
        )
        if rows:
            return rows
        return self._extract_with_zoom(
            pdf_bytes,
            pages_per_chunk=max(1, pages_per_chunk),
            zoom=_RETRY_ZOOM,
        )

    def _extract_with_zoom(
        self,
        pdf_bytes: bytes,
        *,
        pages_per_chunk: int,
        zoom: float,
    ) -> List[PgxExtractionRow]:
        pages = _render_pdf_pages(pdf_bytes, zoom=zoom)
        if not pages:
            raise ExtractionError("PDF contains no readable pages")
        rows: List[PgxExtractionRow] = []
        seen: set[tuple[str, str, str, str]] = set()
        for start in range(0, len(pages), pages_per_chunk):
            chunk = pages[start : start + pages_per_chunk]
            chunk_rows = self._extract_chunk(chunk, start_page=start + 1)
            for row in chunk_rows:
                key = (row.sample_id, row.gene, row.allele, row.medication)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
        return rows

    def _extract_chunk(
        self,
        page_payloads: List[Tuple[str, str]],
        *,
        start_page: int,
    ) -> List[PgxExtractionRow]:
        structured = self._llm.with_structured_output(_PgxExtractionSchema)
        image_blocks = [
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{image_b64}"},
            }
            for image_b64, _text in page_payloads
        ]
        text_context = _build_chunk_text(page_payloads)
        response = structured.invoke(
            [
                SystemMessage(content=self._prompt),
                HumanMessage(
                    content=[
                        {
                            "type": "text",
                            "text": (
                                "Extract PGX sample-level observations from these PDF pages only. "
                                f"The first image corresponds to page {start_page}. "
                                "Use tables/figures and text. One row per sample-medication observation.\n\n"
                                f"OCR-like page text context:\n{text_context}"
                            ),
                        },
                        *image_blocks,
                    ]
                ),
            ]
        )
        rows: List[PgxExtractionRow] = []
        for item in response.samples:
            rows.append(
                PgxExtractionRow(
                    sample_id=item.sample_id.strip(),
                    gene=item.gene.strip(),
                    allele=item.allele.strip(),
                    rs_id=item.rs_id.strip(),
                    medication=item.medication.strip(),
                    outcome=item.outcome.strip(),
                    actionability=_normalize_actionability(item.actionability),
                    cpic_recommendation=item.cpic_recommendation.strip(),
                    source_context=item.source_context.strip(),
                )
            )
        return rows


class StructuredGeminiPgxExtractor(StructuredPgxExtractor):
    """Gemini-backed structured PGX extractor."""

    def __init__(
        self,
        *,
        prompt: str,
        api_key: Optional[str] = None,
        model_name: str,
        llm: Optional[Any] = None,
    ):
        if llm is not None:
            super().__init__(
                prompt=prompt,
                model_name=model_name,
                llm=llm,
            )
            return
        if not api_key:
            raise ValueError("Google API key is required for Gemini PGX extraction")
        from langchain_google_genai import ChatGoogleGenerativeAI

        super().__init__(
            prompt=prompt,
            model_name=model_name,
            llm=ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key),
        )


def _render_pdf_pages(pdf_bytes: bytes, *, zoom: float) -> List[Tuple[str, str]]:
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:
        raise ExtractionError(f"PDF parse error: {exc}") from exc

    pages: List[Tuple[str, str]] = []
    try:
        matrix = fitz.Matrix(zoom, zoom)
        for page in doc:
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            png_bytes = pix.tobytes("png")
            image_b64 = base64.b64encode(png_bytes).decode("ascii")
            page_text = (page.get_text("text") or "").strip()
            pages.append((image_b64, page_text))
    finally:
        doc.close()
    return pages


def pdf_page_count(pdf_bytes: bytes) -> Optional[int]:
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return None
    try:
        return len(doc)
    finally:
        doc.close()


def _build_chunk_text(page_payloads: List[Tuple[str, str]]) -> str:
    chunks: List[str] = []
    for idx, (_image, text) in enumerate(page_payloads, start=1):
        trimmed = text.strip()
        if not trimmed:
            continue
        chunks.append(f"[Chunk page {idx}]\n{trimmed}")
    merged = "\n\n".join(chunks)
    if len(merged) <= _MAX_CHUNK_TEXT_CHARS:
        return merged
    return merged[:_MAX_CHUNK_TEXT_CHARS]


def _normalize_actionability(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"yes", "y", "true", "1"}:
        return "Yes"
    if normalized in {"no", "n", "false", "0"}:
        return "No"
    return "No"
