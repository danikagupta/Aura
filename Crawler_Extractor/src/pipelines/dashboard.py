"""Read-side helpers for the Streamlit dashboard."""

from __future__ import annotations

from typing import List

from ..common.dtos import PaperRecord, PaperState
from ..common.repository import PaperRepository

ORDERED_STATES = [
    PaperState.PDF_NOT_AVAILABLE,
    PaperState.PDF_AVAILABLE,
    PaperState.TEXT_AVAILABLE,
    PaperState.SCORED,
    PaperState.PROCESSED_LOW_SCORE,
    PaperState.FAILED,
]


def load_recent_papers(
    repository: PaperRepository, per_state_limit: int = 50
) -> List[PaperRecord]:
    """Fetch a slice of papers across states for dashboard display."""

    rows: List[PaperRecord] = []
    for state in ORDERED_STATES:
        rows.extend(repository.fetch_by_state(state, per_state_limit))
    return rows
