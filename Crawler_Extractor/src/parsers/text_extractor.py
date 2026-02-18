"""PDF to text extraction pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from typing import List

from pypdf import PdfReader
from pypdf.errors import PdfReadError, PdfStreamError

from ..common.dtos import PaperRecord, StorageRef
from ..common.errors import ExtractionError
from ..ingest.storage_gateway import StorageGateway


@dataclass(slots=True)
class TextExtractionResult:
    """Payload returned after extracting text for a paper."""

    text_ref: StorageRef
    text: str
    links: List["LinkAnnotation"] = field(default_factory=list)


@dataclass(slots=True)
class LinkAnnotation:
    url: str
    text: str
    page: int


class TextExtractor:
    """Extracts clean text from Supabase-stored PDFs."""

    def __init__(self, storage: StorageGateway):
        self._storage = storage

    def extract(self, record: PaperRecord) -> TextExtractionResult:
        if not record.pdf_ref:
            raise ExtractionError("Paper is missing PDF reference")
        pdf_bytes = self._storage.fetch_blob(record.pdf_ref)
        text = self._read_pdf(pdf_bytes)
        links = self._extract_links(pdf_bytes)
        text_ref = self._storage.store_text(record.id, text)
        return TextExtractionResult(text_ref=text_ref, text=text, links=links)

    @staticmethod
    def _read_pdf(data: bytes) -> str:
        try:
            reader = PdfReader(BytesIO(data))
            segments = []
            for page in reader.pages:
                try:
                    segments.append(page.extract_text() or "")
                except Exception:
                    continue
        except (PdfReadError, PdfStreamError) as exc:
            raise ExtractionError(f"PDF parse error: {exc}") from exc
        except Exception as exc:
            raise ExtractionError(f"PDF parse error: {exc}") from exc
        text = "\n".join(segment.strip() for segment in segments if segment.strip())
        if not text:
            raise ExtractionError("PDF parsing produced no text")
        return text

    def _extract_links(self, data: bytes) -> List[LinkAnnotation]:
        try:
            import fitz
        except ImportError:
            return []
        annotations: List[LinkAnnotation] = []
        try:
            with fitz.open(stream=data, filetype="pdf") as doc:
                for page_number, page in enumerate(doc):
                    try:
                        links = page.get_links()
                    except Exception:
                        continue
                    for entry in links:
                        uri = entry.get("uri")
                        rect = entry.get("from")
                        if not uri or rect is None:
                            continue
                        region = fitz.Rect(rect)
                        text = self._text_from_rect(page, region)
                        annotations.append(
                            LinkAnnotation(
                                url=uri,
                                text=text or uri,
                                page=page_number,
                            )
                        )
        except Exception:
            return []
        return annotations

    @staticmethod
    def _text_from_rect(page, rect) -> str:
        try:
            import fitz
        except ImportError:  # pragma: no cover
            return ""
        try:
            words = page.get_text("words")
        except Exception:
            return ""
        captured = [
            word[4]
            for word in words
            if fitz.Rect(word[:4]).intersects(rect)  # type: ignore[attr-defined]
        ]
        return " ".join(captured).strip()
