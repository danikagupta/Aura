"""Batch processing for the text extraction stage."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional

from ..common.dtos import PaperRecord, PaperState, StageOutcome
from ..common.repository import PaperRepository
from ..parsers.text_extractor import TextExtractor
from .attempts import filter_processable, mark_processing_attempt
from .processing_steps import extract_text


def process_text_extraction_batch(
    *,
    batch_size: int,
    repository: PaperRepository,
    extractor: TextExtractor,
    level: int,
    seed_number: Optional[int] = None,
    max_workers: int = 1,
    progress_callback: Optional[Callable[[str, PaperRecord], None]] = None,
    completion_callback: Optional[
        Callable[[str, PaperRecord, StageOutcome], None]
    ] = None,
) -> List[StageOutcome]:
    """Extract text for the next batch of PDF-ready papers."""

    papers = filter_processable(
        repository.fetch_by_state_at_level(
            PaperState.PDF_AVAILABLE,
            level,
            batch_size,
            seed_number=seed_number,
        )
    )
    if not papers:
        return []

    def _process(paper: PaperRecord) -> StageOutcome:
        updated = mark_processing_attempt(repository, paper)
        return extract_text(
            updated,
            repository=repository,
            extractor=extractor,
        )

    if max_workers <= 1 or len(papers) == 1:
        outcomes: List[StageOutcome] = []
        for paper in papers:
            if progress_callback:
                progress_callback("text_extraction", paper)
            outcome = _process(paper)
            if completion_callback:
                completion_callback("text_extraction", paper, outcome)
            outcomes.append(outcome)
        return outcomes

    outcomes: List[Optional[StageOutcome]] = [None] * len(papers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, paper in enumerate(papers):
            if progress_callback:
                progress_callback("text_extraction", paper)
            futures[executor.submit(_process, paper)] = idx
        for future in as_completed(futures):
            idx = futures[future]
            paper = papers[idx]
            outcome = future.result()
            outcomes[idx] = outcome
            if completion_callback:
                completion_callback("text_extraction", paper, outcome)
    return [result for result in outcomes if result is not None]
