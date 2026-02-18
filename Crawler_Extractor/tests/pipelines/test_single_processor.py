from __future__ import annotations

from src.common.dtos import PaperRecord, PaperState
from src.pipelines.services import PipelineServices
from src.pipelines.single_processor import process_single_paper
from tests.pipelines.fakes import (
    FakeCitationExtractor,
    FakePdfFetcher,
    FakeRepository,
    FakeScorer,
    FakeStorageGateway,
    FakeTextExtractor,
)


def test_process_single_paper_advances_through_all_stages() -> None:
    initial = PaperRecord(
        id="paper-1",
        title="Seed Paper",
        state=PaperState.PDF_NOT_AVAILABLE,
        level=1,
        metadata={"citation": {"title": "Seed Paper"}},
    )
    repository = FakeRepository(initial)
    storage = FakeStorageGateway()
    services = PipelineServices(
        repository=repository,
        storage=storage,
        text_extractor=FakeTextExtractor(storage),
        scorer=FakeScorer(),
        citation_extractor=FakeCitationExtractor(),
        pdf_fetcher=FakePdfFetcher(),
        score_threshold=7.0,
    )

    result = process_single_paper("paper-1", services)

    assert result.success is True
    assert result.stages_run == [
        "pdf_acquisition",
        "text_extraction",
        "scoring",
        "citations",
    ]
    assert repository.fetch_by_id("paper-1").state == PaperState.SCORED
    assert any(
        entry.stage == "citations" and entry.success for entry in result.logs
    ), "citation stage did not log success"
    assert repository.attempts.count("paper-1") == len(result.stages_run)


def test_process_single_paper_handles_missing_text_reference() -> None:
    record = PaperRecord(
        id="paper-2",
        title="Textless",
        state=PaperState.TEXT_AVAILABLE,
        level=1,
    )
    repository = FakeRepository(record)
    storage = FakeStorageGateway()
    services = PipelineServices(
        repository=repository,
        storage=storage,
        text_extractor=FakeTextExtractor(storage),
        scorer=FakeScorer(),
        citation_extractor=FakeCitationExtractor(),
        pdf_fetcher=FakePdfFetcher(),
        score_threshold=7.0,
    )

    result = process_single_paper("paper-2", services)

    assert result.success is False
    assert repository.fetch_by_id("paper-2").state == PaperState.FAILED
    assert any("missing" in entry.message for entry in result.logs)
    assert repository.attempts == ["paper-2"]
