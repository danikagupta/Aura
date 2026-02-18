"""High-level batch orchestrator used by the UI and CLI."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

from ..common.dtos import PaperRecord, StageOutcome
from ..common.repository import PaperRepository
from .citation_stage import process_citation_batch
from .pdf_acquisition_stage import process_pdf_acquisition_batch
from .scoring_stage import process_scoring_batch
from .services import PipelineServices
from .text_stage import process_text_extraction_batch


ProgressCallback = Callable[[str, PaperRecord], None]
CompletionCallback = Callable[[str, PaperRecord, StageOutcome], None]
StageCallback = Callable[[str, List[StageOutcome]], None]


def process_pipeline_batch(
    batch_size: int,
    services: PipelineServices,
    max_workers: int = 1,
    *,
    level: Optional[int] = None,
    progress_callback: Optional[ProgressCallback] = None,
    completion_callback: Optional[CompletionCallback] = None,
    stage_callback: Optional[StageCallback] = None,
) -> Dict[str, List[StageOutcome]]:
    """Run one multi-stage batch and return per-stage outcomes."""

    repository = services.repository
    storage = services.storage
    target_level = (
        level
        if level is not None
        else services.current_processing_level()
        or repository.lowest_active_level()
    )
    services.set_processing_level(target_level)
    text_outcomes = process_text_extraction_batch(
        batch_size=batch_size,
        repository=repository,
        extractor=services.text_extractor,
        level=target_level,
        max_workers=max_workers,
        progress_callback=progress_callback,
        completion_callback=completion_callback,
    )
    scoring_outcomes = process_scoring_batch(
        batch_size=batch_size,
        repository=repository,
        storage=storage,
        scorer=services.scorer,
        threshold=services.score_threshold,
        level=target_level,
        max_workers=max_workers,
        progress_callback=progress_callback,
        completion_callback=completion_callback,
    )
    citation_outcomes = process_citation_batch(
        batch_size=batch_size,
        repository=repository,
        storage=storage,
        extractor=services.citation_extractor,
        threshold=services.score_threshold,
        level=target_level,
        max_workers=max_workers,
        progress_callback=progress_callback,
        completion_callback=completion_callback,
    )
    acquisition_outcomes = process_pdf_acquisition_batch(
        batch_size=batch_size,
        repository=repository,
        storage=storage,
        fetcher=services.pdf_fetcher,
        level=target_level,
        max_workers=max_workers,
        progress_callback=progress_callback,
        completion_callback=completion_callback,
    )
    if stage_callback:
        stage_callback("text", text_outcomes)
        stage_callback("scoring", scoring_outcomes)
        stage_callback("citations", citation_outcomes)
        stage_callback("pdf_acquisition", acquisition_outcomes)

    return {
        "text": text_outcomes,
        "scoring": scoring_outcomes,
        "citations": citation_outcomes,
        "pdf_acquisition": acquisition_outcomes,
    }
