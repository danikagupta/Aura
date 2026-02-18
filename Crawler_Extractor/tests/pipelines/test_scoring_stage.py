from __future__ import annotations

from typing import List, Optional

from src.common.dtos import PaperRecord, PaperState, ScoreResult, StageOutcome, StorageRef
from src.pipelines.scoring_stage import process_scoring_batch


class _FakeRepository:
    def __init__(self, papers: List[PaperRecord]):
        self._papers = papers
        self.register_calls: List[dict] = []
        self.failures: List[str] = []
        self.attempts: List[str] = []

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        assert state == PaperState.TEXT_AVAILABLE
        return self._papers[:limit]

    def fetch_by_state_at_level(
        self,
        state: PaperState,
        level: int,
        limit: int,
        seed_number: Optional[int] = None,
    ) -> List[PaperRecord]:
        assert level == 1
        return self._papers[:limit]

    def lowest_active_level(self) -> int:
        return 1

    def register_score(
        self,
        paper_id: str,
        score: float,
        reason: str,
        model_name: str,
        duration_ms: int,
        state: PaperState = PaperState.SCORED,
    ) -> PaperRecord:
        self.register_calls.append(
            {
                "paper_id": paper_id,
                "score": score,
                "reason": reason,
                "model": model_name,
                "duration_ms": duration_ms,
                "state": state,
            }
        )
        return next(p for p in self._papers if p.id == paper_id)

    def update_state(
        self, paper_id: str, state: PaperState, updates: Optional[dict] = None
    ) -> PaperRecord:
        assert state == PaperState.FAILED
        self.failures.append(paper_id)
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


class _FakeStorage:
    def fetch_blob(self, ref: StorageRef) -> bytes:
        return f"text for {ref.uri}".encode("utf-8")


class _FakeScorer:
    def __init__(self, score: float):
        self._score = score

    def score(self, text: str, *, seed_number: Optional[int] = None) -> ScoreResult:
        return ScoreResult(
            score=self._score,
            reason=f"scored {text}",
            model_name="fake",
            duration_ms=1,
        )


def _paper(paper_id: str, with_text: bool = True) -> PaperRecord:
    return PaperRecord(
        id=paper_id,
        title="Paper",
        state=PaperState.TEXT_AVAILABLE,
        level=1,
        text_ref=StorageRef(f"storage://txt/{paper_id}.txt") if with_text else None,
    )


def test_process_scoring_batch_updates_scores() -> None:
    paper = _paper("1")
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    scorer = _FakeScorer(score=9.0)

    outcomes = process_scoring_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        scorer=scorer,  # type: ignore[arg-type]
        threshold=7.0,
        level=1,
    )

    assert repo.register_calls[0]["paper_id"] == "1"
    assert repo.register_calls[0]["score"] == 9.0
    assert repo.register_calls[0]["state"] == PaperState.SCORED
    assert outcomes[0].metadata["eligible_for_citations"]
    assert outcomes[0].success
    assert repo.attempts == ["1"]


def test_process_scoring_batch_handles_missing_text() -> None:
    paper = _paper("1", with_text=False)
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    scorer = _FakeScorer(score=5.0)

    outcomes = process_scoring_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        scorer=scorer,  # type: ignore[arg-type]
        threshold=7.0,
        level=1,
    )

    assert repo.failures == ["1"]
    assert not outcomes[0].success
    assert repo.attempts == ["1"]


def test_process_scoring_batch_skips_exhausted_attempts() -> None:
    paper = _paper("1")
    paper.attempts = 10
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    scorer = _FakeScorer(score=9.0)

    outcomes = process_scoring_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        scorer=scorer,  # type: ignore[arg-type]
        threshold=7.0,
        level=1,
    )

    assert outcomes == []
    assert repo.attempts == []
def test_process_scoring_batch_marks_low_scores_processed() -> None:
    paper = _paper("low")
    repo = _FakeRepository([paper])
    storage = _FakeStorage()
    scorer = _FakeScorer(score=3.5)

    outcomes = process_scoring_batch(
        batch_size=1,
        repository=repo,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        scorer=scorer,  # type: ignore[arg-type]
        threshold=7.0,
        level=1,
    )

    assert repo.register_calls[0]["state"] == PaperState.PROCESSED_LOW_SCORE
    assert outcomes[0].metadata["eligible_for_citations"] is False
    assert outcomes[0].success
