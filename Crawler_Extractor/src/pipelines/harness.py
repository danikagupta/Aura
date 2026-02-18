"""High-level harness to run targeted pipeline steps with limited parallelism."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from ..common.dtos import StageOutcome
from .attempts import filter_processable
from .paper_processor import (
    PaperProcessor,
    StageName,
    STATE_FOR_STAGE,
)
from .services import PipelineServices


class ProcessingHarness:
    """Runs up to N stage executions using the deterministic PaperProcessor."""

    def __init__(self, services: PipelineServices) -> None:
        self._services = services
        self._processor = PaperProcessor(services)

    def run(
        self,
        *,
        stage: StageName,
        level: int,
        max_steps: int,
        max_workers: int = 1,
    ) -> List[StageOutcome]:
        """Execute up to `max_steps` papers for the provided level/stage combination."""

        if max_steps <= 0:
            return []
        candidates = self._services.repository.fetch_by_state_at_level(
            STATE_FOR_STAGE[stage],
            level,
            max_steps,
        )
        papers = filter_processable(candidates)[:max_steps]
        if not papers:
            return []
        if max_workers <= 1 or len(papers) == 1:
            return [self._processor.run(paper, stage=stage) for paper in papers]
        outcomes: List[Optional[StageOutcome]] = [None] * len(papers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._processor.run, paper, stage=stage): idx
                for idx, paper in enumerate(papers)
            }
            for future in as_completed(futures):
                idx = futures[future]
                outcomes[idx] = future.result()
        return [outcome for outcome in outcomes if outcome is not None]
