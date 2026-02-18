from __future__ import annotations

from typing import List, Optional

from src.common.dtos import PaperRecord, PaperState, StorageRef
from src.common.errors import ExtractionError
from src.pipelines.text_stage import process_text_extraction_batch


class _FakePaper(PaperRecord):
    def __init__(self, paper_id: str) -> None:
        super().__init__(
            id=paper_id,
            title=f"Paper {paper_id}",
            state=PaperState.PDF_AVAILABLE,
            level=1,
            pdf_ref=StorageRef(f"storage://pdfs/{paper_id}/seed.pdf"),
        )


class _FakeRepository:
    def __init__(self, papers: List[PaperRecord]):
        self._papers = papers
        self.saved: List[str] = []
        self.failed: List[str] = []
        self.attempts: List[str] = []
        self.seed_filters: List[Optional[int]] = []

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        assert state == PaperState.PDF_AVAILABLE
        return self._papers[:limit]

    def fetch_by_state_at_level(
        self,
        state: PaperState,
        level: int,
        limit: int,
        seed_number: Optional[int] = None,
    ) -> List[PaperRecord]:
        assert level == 1
        self.seed_filters.append(seed_number)
        return self._papers[:limit]

    def lowest_active_level(self) -> int:
        return 1

    def save_text_reference(self, paper_id: str, text_uri: str) -> PaperRecord:
        self.saved.append(paper_id)
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.state = PaperState.TEXT_AVAILABLE
        paper.text_ref = StorageRef(text_uri)
        return paper

    def update_state(
        self, paper_id: str, state: PaperState, updates: Optional[dict] = None
    ) -> PaperRecord:
        assert state == PaperState.FAILED
        self.failed.append(paper_id)
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.state = state
        return paper

    def increment_attempts(
        self, paper_id: str, current_attempts: Optional[int] = None
    ) -> PaperRecord:
        self.attempts.append(paper_id)
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.attempts = (paper.attempts or 0) + 1
        return paper


class _FakeExtractor:
    def __init__(self, failures: Optional[set[str]] = None) -> None:
        self.failures = failures or set()

    def extract(self, record: PaperRecord):
        if record.id in self.failures:
            raise ExtractionError("boom")
        return type(
            "Result",
            (),
            {"text_ref": StorageRef("storage://txt/path"), "text": "hello", "links": []},
        )()


def test_process_text_extraction_batch_happy_path() -> None:
    papers = [_FakePaper("1"), _FakePaper("2")]
    repo = _FakeRepository(papers)
    extractor = _FakeExtractor()

    outcomes = process_text_extraction_batch(
        batch_size=2,
        repository=repo,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        level=1,
    )

    assert len(outcomes) == 2
    assert all(outcome.success for outcome in outcomes)
    assert repo.saved == ["1", "2"]
    assert repo.failed == []
    assert repo.attempts == ["1", "2"]


def test_process_text_extraction_batch_handles_extraction_error() -> None:
    papers = [_FakePaper("1")]
    repo = _FakeRepository(papers)
    extractor = _FakeExtractor(failures={"1"})

    outcomes = process_text_extraction_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        level=1,
    )

    assert len(outcomes) == 1
    assert not outcomes[0].success
    assert repo.failed == ["1"]
    assert repo.attempts == ["1"]


def test_process_text_extraction_batch_skips_exhausted_attempts() -> None:
    paper = _FakePaper("1")
    paper.attempts = 6
    repo = _FakeRepository([paper])
    extractor = _FakeExtractor()

    outcomes = process_text_extraction_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        level=1,
    )

    assert outcomes == []
    assert repo.attempts == []


def test_process_text_extraction_batch_filters_by_seed_number() -> None:
    papers = [_FakePaper("1")]
    repo = _FakeRepository(papers)
    extractor = _FakeExtractor()

    process_text_extraction_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        level=1,
        seed_number=55,
    )

    assert repo.seed_filters[-1] == 55
