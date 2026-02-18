"""Batch PGX extraction workflow for P1 rows."""

from __future__ import annotations

from dataclasses import replace
import random
from typing import Callable, List, Optional

from ..common.dtos import PaperRecord, PaperState, StageOutcome
from ..common.repository import PaperRepository
from ..ingest.storage_gateway import StorageGateway
from ..parsers.pgx_extractor import PgxExtractor, pdf_page_count
from .attempts import filter_processable, mark_processing_attempt
from .processing_steps import process_pgx_extraction

_FETCH_LIMIT = 100


def process_pgx_extraction_batch(
    *,
    batch_size: int,
    repository: PaperRepository,
    storage: StorageGateway,
    extractor: PgxExtractor,
    max_workers: int = 1,
    progress_callback: Optional[Callable[[str, PaperRecord], None]] = None,
    completion_callback: Optional[
        Callable[[str, PaperRecord, StageOutcome], None]
    ] = None,
    extraction_table: str = "pgx_extractions",
) -> List[StageOutcome]:
    """Process up to `batch_size` papers with status P1.

    Re-queries the database on every iteration and chooses one random row from
    the current candidate slice.
    """

    def _process(
        paper: PaperRecord,
        *,
        pdf_bytes: bytes | None = None,
        page_count: int | None = None,
    ) -> StageOutcome:
        updated = mark_processing_attempt(repository, paper)
        return process_pgx_extraction(
            updated,
            repository=repository,
            storage=storage,
            extractor=extractor,
            pdf_bytes=pdf_bytes,
            page_count=page_count,
            extraction_table=extraction_table,
        )

    del max_workers  # Processing is intentionally one-at-a-time with re-query.

    outcomes: List[StageOutcome] = []
    while len(outcomes) < batch_size:
        candidates = filter_processable(
            repository.fetch_by_state(PaperState.P1, _FETCH_LIMIT)
        )
        if not candidates:
            break
        selected = random.choice(candidates)
        paper = repository.update_state(selected.id, PaperState.P1_WIP)
        pdf_bytes: bytes | None = None
        page_count: int | None = None
        try:
            if paper.pdf_ref:
                pdf_bytes = storage.fetch_blob(paper.pdf_ref)
                page_count = pdf_page_count(pdf_bytes)
        except Exception:
            pdf_bytes = None
            page_count = None
        progress_paper = replace(
            paper,
            metadata={**paper.metadata, "page_count": page_count},
        )
        if progress_callback:
            progress_callback("pgx_extraction", progress_paper)
        outcome = _process(
            progress_paper,
            pdf_bytes=pdf_bytes,
            page_count=page_count,
        )
        if completion_callback:
            completion_callback("pgx_extraction", progress_paper, outcome)
        outcomes.append(outcome)
    return outcomes
