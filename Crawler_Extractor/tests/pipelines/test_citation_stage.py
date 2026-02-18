from __future__ import annotations

from typing import List

from src.common.dtos import CitationRecord, PaperRecord, PaperState, StorageRef
from src.common.errors import DuplicateTitleError, DuplicateTopicError, RepositoryError
from src.pipelines.citation_stage import process_citation_batch
from src.parsers.citation_extractor import HeuristicCitationExtractor


class _FakeRepository:
    def __init__(self, papers: List[PaperRecord]):
        self._papers = papers
        self.placeholders: List[dict] = []
        self.updated: List[dict] = []
        self.duplicate_titles: set[str] = set()
        self.duplicate_topics: set[str] = set()
        self.pkey_conflicts: set[str] = set()
        self.server_disconnect_titles: set[str] = set()
        self.attempts: List[str] = []

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        assert state == PaperState.SCORED
        return self._papers[:limit]

    def fetch_by_state_at_level(
        self,
        state: PaperState,
        level: int,
        limit: int,
        seed_number: int | None = None,
    ) -> List[PaperRecord]:
        assert level == 1
        return self._papers[:limit]

    def lowest_active_level(self) -> int:
        return 1

    def create_pdf_placeholder(
        self,
        title: str,
        parent_id: str,
        level: int,
        seed_number: int | None = None,
        metadata: dict | None = None,
    ):
        if title in self.duplicate_titles:
            raise DuplicateTitleError("duplicate title")
        if title in self.duplicate_topics:
            raise DuplicateTopicError("duplicate topic")
        if title in self.pkey_conflicts:
            raise RepositoryError('duplicate key value violates unique constraint "papers_pkey"')
        if title in self.server_disconnect_titles:
            raise RepositoryError("Server disconnected")
        inherited_seed = seed_number
        if inherited_seed is None:
            parent = next((p for p in self._papers if p.id == parent_id), None)
            inherited_seed = parent.seed_number if parent else None
        self.placeholders.append(
            {
                "title": title,
                "parent_id": parent_id,
                "level": level,
                "seed_number": inherited_seed,
                "metadata": metadata or {},
            }
        )

    def update_state(
        self, paper_id: str, state: PaperState, updates: dict | None = None
    ) -> None:
        self.updated.append({"paper_id": paper_id, "state": state, "updates": updates})
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.state = state

    def increment_attempts(
        self, paper_id: str, current_attempts: int | None = None
    ) -> PaperRecord:
        self.attempts.append(paper_id)
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.attempts = (paper.attempts or 0) + 1
        return paper


class _FakeStorage:
    def fetch_blob(self, ref: StorageRef) -> bytes:
        return b"1. First Ref\n2. Second Ref"


def _paper(paper_id: str, with_text: bool = True, score: float = 8.0) -> PaperRecord:
    return PaperRecord(
        id=paper_id,
        title="Scored Paper",
        state=PaperState.SCORED,
        level=1,
        score=score,
        seed_number=101,
        text_ref=StorageRef(f"storage://txt/{paper_id}.txt") if with_text else None,
    )


def test_process_citation_batch_creates_placeholders() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert len(repo.placeholders) == 2
    assert outcomes[0].metadata["count"] == 2
    assert outcomes[0].success
    assert {entry["seed_number"] for entry in repo.placeholders} == {101}
    assert repo.updated[0]["state"] == PaperState.PROCESSED
    assert repo.updated[0]["updates"]["metadata"]["citation_count"] == 2
    assert repo.attempts == ["1"]


def test_process_citation_batch_skips_missing_text() -> None:
    paper = _paper("1", with_text=False)
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert not repo.placeholders
    assert not outcomes[0].success
    assert repo.attempts == ["1"]


def test_process_citation_batch_ignores_duplicate_titles() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    repo.duplicate_titles.add("First Ref")
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert len(repo.placeholders) == 1
    assert outcomes[0].metadata["count"] == 1
    assert outcomes[0].success
    assert outcomes[0].success
    assert repo.attempts == ["1"]


def test_process_citation_batch_ignores_duplicate_topics() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    repo.duplicate_topics.add("Second Ref")
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert len(repo.placeholders) == 1
    assert outcomes[0].metadata["count"] == 1
    assert outcomes[0].success
    assert repo.attempts == ["1"]


def test_process_citation_batch_skips_server_disconnect() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    repo.server_disconnect_titles.add("First Ref")
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert outcomes[0].success
    assert len(repo.placeholders) == 1
    assert repo.placeholders[0]["title"] != "First Ref"


class _DuplicateExtractor:
    def extract(self, _text: str) -> List[CitationRecord]:
        return [
            CitationRecord(raw_text="Dup Citation"),
            CitationRecord(raw_text="Dup Citation"),
        ]


def test_process_citation_batch_deduplicates_citations_before_insert() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    extractor = _DuplicateExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,  # type: ignore[arg-type]
        threshold=5.0,
        level=1,
    )

    assert len(repo.placeholders) == 1
    assert outcomes[0].metadata["count"] == 1
    assert outcomes[0].success


def test_process_citation_batch_skips_below_threshold() -> None:
    high = _paper("1", score=7.0)
    low = _paper("2", score=3.0)
    repo = _FakeRepository([high, low])
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=2,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert len(repo.placeholders) == 2  # only from high-scoring paper
    assert all(outcome.paper_id == "1" for outcome in outcomes)


def test_process_citation_batch_skips_primary_key_conflicts() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    repo.pkey_conflicts.add("First Ref")
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert len(repo.placeholders) == 1
    assert outcomes[0].metadata["count"] == 1


def test_process_citation_batch_skips_exhausted_attempts() -> None:
    paper = _paper("1")
    paper.attempts = 9
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    extractor = HeuristicCitationExtractor()

    outcomes = process_citation_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        extractor=extractor,
        threshold=5.0,
        level=1,
    )

    assert outcomes == []
    assert repo.placeholders == []
    assert repo.attempts == []
