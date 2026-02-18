"""Helper utilities for UI export workflows."""

from __future__ import annotations

import csv
import io
from typing import Iterable

from src.common.dtos import PaperRecord


def build_scored_papers_csv(papers: Iterable[PaperRecord]) -> bytes:
    """
    Build a CSV snapshot for scored papers.

    Returns a UTF-8 encoded CSV including the seed number and pdf MD5 so exports can be
    joined with the folder structure written to output/<seed>/<level>/<score>.
    """

    buffer = io.StringIO()
    fieldnames = ["id", "seed_number", "title", "level", "score", "state", "reason", "pdf_md5"]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for paper in papers:
        writer.writerow(
            {
                "id": paper.id,
                "seed_number": paper.seed_number if paper.seed_number is not None else "",
                "title": paper.title,
                "level": paper.level,
                "score": paper.score,
                "state": paper.state.value,
                "reason": paper.reason,
                "pdf_md5": paper.pdf_md5 or "",
            }
        )
    return buffer.getvalue().encode("utf-8")
