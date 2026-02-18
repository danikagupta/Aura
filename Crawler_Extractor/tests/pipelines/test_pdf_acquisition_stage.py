from __future__ import annotations

import hashlib
from typing import List, Optional

from src.common.dtos import PaperRecord, PaperState, StorageRef
from src.ingest.pdf_acquisition import PdfCandidate
from src.pipelines.pdf_acquisition_stage import process_pdf_acquisition_batch


class _FakeRepository:
    def __init__(self, papers: List[PaperRecord]):
        self._papers = papers
        self.updated: List[dict] = []
        self.attempt_calls: List[str] = []

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        assert state == PaperState.PDF_NOT_AVAILABLE
        return self._papers[:limit]

    def fetch_by_state_at_level(
        self,
        state: PaperState,
        level: int,
        limit: int,
        seed_number: Optional[int] = None,
    ) -> List[PaperRecord]:
        assert level == 2
        return self._papers[:limit]

    def lowest_active_level(self) -> int:
        return 2

    def update_state(
        self, paper_id: str, state: PaperState, updates: Optional[dict] = None
    ) -> PaperRecord:
        self.updated.append({"paper_id": paper_id, "state": state, "updates": updates})
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.state = state
        if updates and "metadata" in updates:
            paper.metadata = updates["metadata"]
        if updates and "source_uri" in updates:
            paper.pdf_ref = StorageRef(updates["source_uri"])
        if updates and "pdf_md5" in updates:
            paper.pdf_md5 = str(updates["pdf_md5"])
        return paper

    def increment_attempts(
        self, paper_id: str, current_attempts: Optional[int] = None
    ) -> PaperRecord:
        self.attempt_calls.append(paper_id)
        paper = next(p for p in self._papers if p.id == paper_id)
        paper.attempts = (paper.attempts or 0) + 1
        return paper


class _FakeStorage:
    def __init__(self) -> None:
        self.stored: List[dict] = []

    def store_pdf(self, paper_id: str, filename: str, data: bytes) -> StorageRef:
        self.stored.append(
            {"paper_id": paper_id, "filename": filename, "size": len(data)}
        )
        return StorageRef(f"storage://pdfs/{paper_id}/{filename}")


class _FakeFetcher:
    def __init__(self, candidate: Optional[PdfCandidate]):
        self._candidate = candidate
        self.calls = 0

    def fetch(self, paper: PaperRecord) -> Optional[PdfCandidate]:
        self.calls += 1
        return self._candidate


def _paper(paper_id: str) -> PaperRecord:
    return PaperRecord(
        id=paper_id,
        title="Citation Paper",
        state=PaperState.PDF_NOT_AVAILABLE,
        level=2,
        metadata={},
    )


def test_process_pdf_acquisition_batch_updates_records() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    candidate = PdfCandidate(filename="paper.pdf", content=b"pdf", source_url="http://x")
    fetcher = _FakeFetcher(candidate)

    outcomes = process_pdf_acquisition_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        fetcher=fetcher,  # type: ignore[arg-type]
        level=2,
    )

    assert outcomes[0].success
    assert repo.updated[0]["state"] == PaperState.PDF_AVAILABLE
    expected_hash = hashlib.md5(candidate.content).hexdigest()
    assert repo.updated[0]["updates"]["pdf_md5"] == expected_hash
    assert storage.stored[0]["filename"] == "paper.pdf"
    assert repo.attempt_calls == ["1"]


def test_process_pdf_acquisition_batch_handles_misses() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    fetcher = _FakeFetcher(candidate=None)

    outcomes = process_pdf_acquisition_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        fetcher=fetcher,  # type: ignore[arg-type]
        level=2,
    )

    assert not outcomes[0].success
    assert repo.updated == []


def test_process_pdf_acquisition_batch_skips_exhausted_attempts() -> None:
    paper = _paper("1")
    paper.attempts = 6
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    candidate = PdfCandidate(filename="paper.pdf", content=b"pdf", source_url="http://x")
    fetcher = _FakeFetcher(candidate)

    outcomes = process_pdf_acquisition_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        fetcher=fetcher,  # type: ignore[arg-type]
        level=2,
    )

    assert outcomes == []
    assert repo.attempt_calls == []


def test_process_pdf_acquisition_marks_failed_after_attempt_limit() -> None:
    paper = _paper("1")
    paper.attempts = 2
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    candidate = PdfCandidate(filename="paper.pdf", content=b"pdf", source_url="http://x")
    fetcher = _FakeFetcher(candidate)

    outcomes = process_pdf_acquisition_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        fetcher=fetcher,  # type: ignore[arg-type]
        level=2,
    )

    assert not outcomes[0].success
    assert repo.updated[-1]["state"] == PaperState.FAILED


def test_process_pdf_acquisition_blocked_handle_url_fails_without_fetch() -> None:
    paper = _paper("1")
    paper.title = "url:http://hdl.handle.net/20.500.12708/191953"
    paper.metadata = {"link": {"url": "http://hdl.handle.net/20.500.12708/191953"}}
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    fetcher = _FakeFetcher(
        candidate=PdfCandidate(filename="paper.pdf", content=b"pdf", source_url="http://x")
    )

    outcomes = process_pdf_acquisition_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        fetcher=fetcher,  # type: ignore[arg-type]
        level=2,
    )

    assert fetcher.calls == 0
    assert not outcomes[0].success
    assert outcomes[0].message == "blocked source host"
    assert repo.updated[-1]["state"] == PaperState.FAILED
