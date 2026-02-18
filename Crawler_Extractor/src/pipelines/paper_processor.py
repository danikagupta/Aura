"""Deterministic single-row processor with structured logging."""

from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional

from structlog.stdlib import BoundLogger

from ..common.dtos import PaperRecord, PaperState, StageOutcome
from ..common.logging import get_logger
from .attempts import mark_processing_attempt
from .processing_steps import (
    StageName,
    extract_citations,
    extract_text,
    fetch_pdf,
    score_text,
)
from .services import PipelineServices

StageExecutor = Callable[[PaperRecord], StageOutcome]

STATE_FOR_STAGE: Dict[StageName, PaperState] = {
    "pdf_acquisition": PaperState.PDF_NOT_AVAILABLE,
    "text_extraction": PaperState.PDF_AVAILABLE,
    "scoring": PaperState.TEXT_AVAILABLE,
    "citations": PaperState.SCORED,
}
STAGE_SEQUENCE: List[StageName] = list(STATE_FOR_STAGE.keys())
STATE_TO_STAGE: Dict[PaperState, StageName] = {
    state: stage for stage, state in STATE_FOR_STAGE.items()
}


class PaperProcessor:
    """Executes exactly one pipeline stage for a single PaperRecord."""

    def __init__(
        self,
        services: PipelineServices,
        logger: Optional[BoundLogger] = None,
    ) -> None:
        self._services = services
        self._logger = logger or get_logger(__name__)
        self._executors: Dict[StageName, StageExecutor] = {
            "pdf_acquisition": self._pdf_stage,
            "text_extraction": self._text_stage,
            "scoring": self._scoring_stage,
            "citations": self._citation_stage,
        }

    def stage_for_state(self, state: PaperState) -> Optional[StageName]:
        """Return the stage that can act on the provided state."""

        return STATE_TO_STAGE.get(state)

    def run(
        self,
        paper: PaperRecord,
        stage: Optional[StageName] = None,
    ) -> StageOutcome:
        """Execute the requested stage for the provided paper."""

        target_stage = stage or self.stage_for_state(paper.state)
        if target_stage is None:
            return StageOutcome(
                paper_id=paper.id,
                stage="noop",
                success=False,
                message=f"No actionable stage for {paper.state.value}",
            )
        paper = mark_processing_attempt(self._services.repository, paper)
        log = self._logger.bind(
            paper_id=paper.id,
            stage=target_stage,
            level=paper.level,
        )
        log.info("stage_start", state=paper.state.value)
        executor = self._executors[target_stage]
        started = time.perf_counter()
        try:
            outcome = executor(paper)
        except Exception as exc:  # pragma: no cover - surfaced in UI/logs
            log.exception("stage_crash", error=str(exc))
            outcome = StageOutcome(
                paper_id=paper.id,
                stage=target_stage,
                success=False,
                message="unexpected error",
                metadata={"error": str(exc)},
            )
        duration_ms = int((time.perf_counter() - started) * 1000)
        outcome.metadata.setdefault("duration_ms", duration_ms)
        log.info(
            "stage_complete",
            success=outcome.success,
            message=outcome.message,
            duration_ms=duration_ms,
        )
        return outcome

    def _pdf_stage(self, paper: PaperRecord) -> StageOutcome:
        return fetch_pdf(
            paper,
            repository=self._services.repository,
            storage=self._services.storage,
            fetcher=self._services.pdf_fetcher,
        )

    def _text_stage(self, paper: PaperRecord) -> StageOutcome:
        return extract_text(
            paper,
            repository=self._services.repository,
            extractor=self._services.text_extractor,
        )

    def _scoring_stage(self, paper: PaperRecord) -> StageOutcome:
        return score_text(
            paper,
            repository=self._services.repository,
            storage=self._services.storage,
            scorer=self._services.scorer,
            threshold=self._services.score_threshold,
        )

    def _citation_stage(self, paper: PaperRecord) -> StageOutcome:
        return extract_citations(
            paper,
            repository=self._services.repository,
            storage=self._services.storage,
            extractor=self._services.citation_extractor,
            threshold=self._services.score_threshold,
        )
