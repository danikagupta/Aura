"""Batch scoring workflow for text-ready papers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional

from ..common.dtos import PaperRecord, PaperState, StageOutcome
from ..common.repository import PaperRepository
from ..ingest.storage_gateway import StorageGateway
from ..parsers.scoring_engine import ScoringModel
from .attempts import filter_processable, mark_processing_attempt
from .processing_steps import score_text


def process_scoring_batch(
    *,
    batch_size: int,
    repository: PaperRepository,
    storage: StorageGateway,
    scorer: ScoringModel,
    threshold: float,
    level: int,
    seed_number: Optional[int] = None,
    max_workers: int = 1,
    progress_callback: Optional[Callable[[str, PaperRecord], None]] = None,
    completion_callback: Optional[
        Callable[[str, PaperRecord, StageOutcome], None]
    ] = None,
) -> List[StageOutcome]:
    papers = filter_processable(
        repository.fetch_by_state_at_level(
            PaperState.TEXT_AVAILABLE,
            level,
            batch_size,
            seed_number=seed_number,
        )
    )
    if not papers:
        return []

    def _process(paper: PaperRecord) -> StageOutcome:
        updated = mark_processing_attempt(repository, paper)
        return score_text(
            updated,
            repository=repository,
            storage=storage,
            scorer=scorer,
            threshold=threshold,
        )

    if max_workers <= 1 or len(papers) == 1:
        outcomes: List[StageOutcome] = []
        for paper in papers:
            if progress_callback:
                progress_callback("scoring", paper)
            outcome = _process(paper)
            if completion_callback:
                completion_callback("scoring", paper, outcome)
            outcomes.append(outcome)
        return outcomes

    outcomes: List[Optional[StageOutcome]] = [None] * len(papers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for idx, paper in enumerate(papers):
            if progress_callback:
                progress_callback("scoring", paper)
            futures[executor.submit(_process, paper)] = idx
        for future in as_completed(futures):
            idx = futures[future]
            paper = papers[idx]
            outcome = future.result()
            outcomes[idx] = outcome
            if completion_callback:
                completion_callback("scoring", paper, outcome)
    return [result for result in outcomes if result is not None]
