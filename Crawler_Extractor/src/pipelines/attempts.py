"""Shared helpers to track per-paper processing attempts."""

from __future__ import annotations

from typing import Iterable, List

from ..common.dtos import MAX_PROCESSING_ATTEMPTS, PaperRecord
from ..common.repository import PaperRepository


def filter_processable(papers: Iterable[PaperRecord]) -> List[PaperRecord]:
    """Return only papers that are still eligible for processing attempts."""

    return [
        paper
        for paper in papers
        if (paper.attempts or 0) <= MAX_PROCESSING_ATTEMPTS
    ]


def mark_processing_attempt(repository: PaperRepository, paper: PaperRecord) -> PaperRecord:
    """Increment the attempt counter before executing a stage."""

    return repository.increment_attempts(paper.id, paper.attempts)
