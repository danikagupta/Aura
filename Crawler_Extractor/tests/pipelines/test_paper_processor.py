from __future__ import annotations

from src.common.dtos import PaperRecord, PaperState
from src.pipelines.paper_processor import PaperProcessor
from src.pipelines.services import PipelineServices
from tests.pipelines.fakes import (
    FakeCitationExtractor,
    FakePdfFetcher,
    FakeRepository,
    FakeScorer,
    FakeStorageGateway,
    FakeTextExtractor,
)


def _build_services(state: PaperState = PaperState.PDF_NOT_AVAILABLE) -> PipelineServices:
    record = PaperRecord(
        id="paper-1",
        title="Seed",
        state=state,
        level=1,
        metadata={},
    )
    repository = FakeRepository(record)
    storage = FakeStorageGateway()
    return PipelineServices(
        repository=repository,
        storage=storage,
        text_extractor=FakeTextExtractor(storage),
        scorer=FakeScorer(),
        citation_extractor=FakeCitationExtractor(),
        pdf_fetcher=FakePdfFetcher(),
        score_threshold=7.0,
    )


def test_paper_processor_runs_stage_and_records_duration() -> None:
    services = _build_services(PaperState.PDF_NOT_AVAILABLE)
    processor = PaperProcessor(services)
    paper = services.repository.fetch_by_id("paper-1")

    outcome = processor.run(paper)

    assert outcome.stage == "pdf_acquisition"
    assert outcome.success is True
    assert "duration_ms" in outcome.metadata
    updated = services.repository.fetch_by_id("paper-1")
    assert updated.state == PaperState.PDF_AVAILABLE


def test_paper_processor_handles_unprocessable_state() -> None:
    services = _build_services(PaperState.PROCESSED)
    processor = PaperProcessor(services)
    paper = services.repository.fetch_by_id("paper-1")

    outcome = processor.run(paper)

    assert outcome.stage == "noop"
    assert outcome.success is False
    assert outcome.message.startswith("No actionable stage")
