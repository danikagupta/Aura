from __future__ import annotations

from src.common.dtos import PaperRecord, PaperState
from src.pipelines.harness import ProcessingHarness
from src.pipelines.services import PipelineServices
from tests.pipelines.fakes import (
    FakeCitationExtractor,
    FakePdfFetcher,
    FakeRepository,
    FakeScorer,
    FakeStorageGateway,
    FakeTextExtractor,
)


def _build_services(records: list[PaperRecord]) -> PipelineServices:
    repository = FakeRepository(records[0])
    for record in records[1:]:
        repository.records[record.id] = record
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


def test_processing_harness_limits_steps_and_updates_records() -> None:
    records = [
        PaperRecord(
            id=f"paper-{idx}",
            title=f"Paper {idx}",
            state=PaperState.PDF_NOT_AVAILABLE,
            level=1,
            metadata={},
        )
        for idx in range(1, 4)
    ]
    services = _build_services(records)
    harness = ProcessingHarness(services)

    outcomes = harness.run(stage="pdf_acquisition", level=1, max_steps=2, max_workers=2)

    assert len(outcomes) == 2
    assert all(outcome.success for outcome in outcomes)
    updated_states = [
        services.repository.fetch_by_id(outcome.paper_id).state for outcome in outcomes
    ]
    assert updated_states == [PaperState.PDF_AVAILABLE, PaperState.PDF_AVAILABLE]


def test_processing_harness_returns_empty_when_no_candidates() -> None:
    record = PaperRecord(
        id="paper-1",
        title="Done",
        state=PaperState.PROCESSED,
        level=1,
        metadata={},
    )
    services = _build_services([record])
    harness = ProcessingHarness(services)

    outcomes = harness.run(stage="pdf_acquisition", level=1, max_steps=5)

    assert outcomes == []
