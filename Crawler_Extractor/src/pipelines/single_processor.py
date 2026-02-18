"""Interactive helpers for running a single paper through pending pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..common.dtos import PaperState
from ..pipelines.services import PipelineServices
from .paper_processor import PaperProcessor


@dataclass(slots=True)
class OperationLogEntry:
    """Structured message describing one processing step."""

    stage: str
    message: str
    success: Optional[bool] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SingleProcessResult:
    """Summary emitted after running a paper through pending stages."""

    paper_id: str
    started_state: PaperState
    finished_state: PaperState
    stages_run: List[str]
    success: bool
    logs: List[OperationLogEntry]


def process_single_paper(
    paper_id: str, services: PipelineServices
) -> SingleProcessResult:
    """Advance a paper through pending stages, recording an execution log."""

    repository = services.repository
    record = repository.fetch_by_id(paper_id)
    processor = PaperProcessor(services)
    start_state = record.state
    logs: List[OperationLogEntry] = []
    stages_run: List[str] = []
    success = False

    while True:
        stage_name = processor.stage_for_state(record.state)
        if stage_name is None:
            if not stages_run:
                logs.append(
                    OperationLogEntry(
                        stage="noop",
                        message="Paper is not in a processable state",
                        success=False,
                        details={"state": record.state.value},
                    )
                )
            break
        stages_run.append(stage_name)
        logs.append(
            OperationLogEntry(
                stage=stage_name,
                message="Starting stage",
                success=None,
                details={"state": record.state.value, "level": record.level},
            )
        )
        outcome = processor.run(record, stage=stage_name)
        logs.append(
            OperationLogEntry(
                stage=stage_name,
                message=outcome.message,
                success=outcome.success,
                details=outcome.metadata,
            )
        )
        success = outcome.success
        record = repository.fetch_by_id(paper_id)
        if not outcome.success or stage_name == "citations":
            break

    return SingleProcessResult(
        paper_id=paper_id,
        started_state=start_state,
        finished_state=record.state,
        stages_run=stages_run,
        success=success if stages_run else False,
        logs=logs,
    )
