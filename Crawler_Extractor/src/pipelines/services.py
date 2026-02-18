"""Shared dependency bundle for pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..common.repository import PaperRepository
from ..ingest.pdf_acquisition import PdfFetcher
from ..ingest.storage_gateway import StorageGateway
from ..parsers.citation_extractor import CitationExtractor
from ..parsers.scoring_engine import ScoringModel
from ..parsers.text_extractor import TextExtractor


@dataclass(slots=True)
class PipelineServices:
    """Aggregated services injected into pipeline components."""

    repository: PaperRepository
    storage: StorageGateway
    text_extractor: TextExtractor
    scorer: ScoringModel
    citation_extractor: CitationExtractor
    pdf_fetcher: PdfFetcher
    score_threshold: float
    _cached_level: Optional[int] = field(default=None, init=False, repr=False)

    def current_processing_level(self) -> Optional[int]:
        return self._cached_level

    def set_processing_level(self, level: int) -> None:
        self._cached_level = level
