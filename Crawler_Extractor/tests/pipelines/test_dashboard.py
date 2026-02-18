from __future__ import annotations

from typing import Dict, List

from src.common.dtos import PaperRecord, PaperState
from src.pipelines.dashboard import ORDERED_STATES, load_recent_papers


class _FakeRepository:
    def __init__(self) -> None:
        self.calls: Dict[PaperState, int] = {}

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        self.calls[state] = limit
        return []


def test_load_recent_papers_queries_each_state() -> None:
    repo = _FakeRepository()

    load_recent_papers(repo, per_state_limit=5)

    assert set(repo.calls.keys()) == set(ORDERED_STATES)
    assert all(limit == 5 for limit in repo.calls.values())
