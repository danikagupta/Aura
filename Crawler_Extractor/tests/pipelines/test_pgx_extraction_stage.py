from __future__ import annotations

import random

from src.common.dtos import PaperRecord, PaperState, StorageRef
from src.parsers.pgx_extractor import PgxExtractionRow
from src.pipelines.pgx_extraction_stage import process_pgx_extraction_batch
from tests.pipelines.fakes import FakeRepository, FakeStorageGateway


class _FakeExtractor:
    def __init__(self, rows):
        self._rows = rows

    def extract_pdf(self, data: bytes, *, pages_per_chunk: int = 4):
        assert data
        assert pages_per_chunk
        return self._rows


class _CountingRepository(FakeRepository):
    def __init__(self, record: PaperRecord):
        super().__init__(record)
        self.fetch_by_state_calls = 0
        self.updated_states: list[PaperState] = []

    def fetch_by_state(self, state: PaperState, limit: int):
        self.fetch_by_state_calls += 1
        return super().fetch_by_state(state, limit)

    def update_state(self, paper_id: str, state: PaperState, updates=None):
        self.updated_states.append(state)
        return super().update_state(paper_id, state, updates=updates)


def test_process_pgx_extraction_batch_marks_success_and_persists_rows(monkeypatch) -> None:
    monkeypatch.setattr("src.pipelines.pgx_extraction_stage.pdf_page_count", lambda _b: 8)
    seen_progress = []

    record = PaperRecord(
        id="paper-1",
        title="Paper",
        state=PaperState.P1,
        level=1,
        pdf_ref=StorageRef("storage://papers/paper-1/sample.pdf"),
        metadata={},
    )
    repository = FakeRepository(record)
    storage = FakeStorageGateway()
    storage._objects["storage://papers/paper-1/sample.pdf"] = b"%PDF"
    extractor = _FakeExtractor(
        [
            PgxExtractionRow(
                sample_id="Case-1",
                gene="CYP2C19",
                allele="*1/*2",
                rs_id="rs4244285",
                medication="Clopidogrel",
                outcome="Ineffective",
                actionability="Yes",
                cpic_recommendation="Use alternative",
                source_context="Individual Case Series",
            )
        ]
    )

    outcomes = process_pgx_extraction_batch(
        batch_size=1,
        repository=repository,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        progress_callback=lambda _stage, paper: seen_progress.append(
            paper.metadata.get("page_count")
        ),
    )

    assert len(outcomes) == 1
    assert outcomes[0].success is True
    assert repository.fetch_by_id("paper-1").state == PaperState.P1_SUCCESS
    assert len(repository.pgx_rows["paper-1"]) == 1
    assert seen_progress == [8]


def test_process_pgx_extraction_batch_marks_failure_on_empty_rows() -> None:
    record = PaperRecord(
        id="paper-2",
        title="Paper",
        state=PaperState.P1,
        level=1,
        pdf_ref=StorageRef("storage://papers/paper-2/sample.pdf"),
        metadata={},
    )
    repository = FakeRepository(record)
    storage = FakeStorageGateway()
    storage._objects["storage://papers/paper-2/sample.pdf"] = b"%PDF"
    extractor = _FakeExtractor([])

    outcomes = process_pgx_extraction_batch(
        batch_size=1,
        repository=repository,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
    )

    assert len(outcomes) == 1
    assert outcomes[0].success is False
    assert repository.fetch_by_id("paper-2").state == PaperState.P1_FAILURE


def test_process_pgx_extraction_batch_requeries_and_randomly_selects(monkeypatch) -> None:
    monkeypatch.setattr(random, "choice", lambda rows: rows[-1])
    first = PaperRecord(
        id="paper-1",
        title="Paper 1",
        state=PaperState.P1,
        level=1,
        pdf_ref=StorageRef("storage://papers/paper-1/sample.pdf"),
        metadata={},
    )
    second = PaperRecord(
        id="paper-2",
        title="Paper 2",
        state=PaperState.P1,
        level=1,
        pdf_ref=StorageRef("storage://papers/paper-2/sample.pdf"),
        metadata={},
    )
    repository = _CountingRepository(first)
    repository.records[second.id] = second
    storage = FakeStorageGateway()
    storage._objects["storage://papers/paper-1/sample.pdf"] = b"%PDF"
    storage._objects["storage://papers/paper-2/sample.pdf"] = b"%PDF"
    extractor = _FakeExtractor(
        [
            PgxExtractionRow(
                sample_id="Case-1",
                gene="CYP2C19",
                allele="*1/*2",
                rs_id="rs4244285",
                medication="Clopidogrel",
                outcome="Ineffective",
                actionability="Yes",
                cpic_recommendation="Use alternative",
                source_context="Individual Case Series",
            )
        ]
    )

    outcomes = process_pgx_extraction_batch(
        batch_size=2,
        repository=repository,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        max_workers=8,
    )

    assert len(outcomes) == 2
    assert outcomes[0].paper_id == "paper-2"
    assert repository.fetch_by_state_calls == 2
    assert PaperState.P1_WIP in repository.updated_states


def test_process_pgx_extraction_batch_uses_custom_extraction_table() -> None:
    record = PaperRecord(
        id="paper-3",
        title="Paper 3",
        state=PaperState.P1,
        level=1,
        pdf_ref=StorageRef("storage://papers/paper-3/sample.pdf"),
        metadata={},
    )
    repository = FakeRepository(record)
    storage = FakeStorageGateway()
    storage._objects["storage://papers/paper-3/sample.pdf"] = b"%PDF"
    extractor = _FakeExtractor(
        [
            PgxExtractionRow(
                sample_id="Case-1",
                gene="CYP2C19",
                allele="*1/*2",
                rs_id="rs4244285",
                medication="Clopidogrel",
                outcome="Ineffective",
                actionability="Yes",
                cpic_recommendation="Use alternative",
                source_context="Individual Case Series",
            )
        ]
    )

    outcomes = process_pgx_extraction_batch(
        batch_size=1,
        repository=repository,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        extraction_table="pgx_gextractions",
    )

    assert outcomes[0].success is True
    assert len(repository.pgx_rows_by_table["pgx_gextractions"]["paper-3"]) == 1
