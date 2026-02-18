import csv
import io

from src.common.dtos import PaperRecord, PaperState
from src.ui.export_helpers import build_scored_papers_csv


def test_build_scored_papers_csv_includes_seed_number():
    papers = [
        PaperRecord(
            id="paper-1",
            title="Sample Paper",
            state=PaperState.SCORED,
            level=3,
            score=8.5,
            seed_number=42,
            reason="Looks promising",
        ),
        PaperRecord(
            id="paper-2",
            title="No Seed",
            state=PaperState.SCORED,
            level=2,
            score=7.0,
            seed_number=None,
            reason=None,
        ),
    ]

    csv_bytes = build_scored_papers_csv(papers)
    rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8"))))

    assert rows[0]["seed_number"] == "42"
    # `None` seed numbers should render as an empty string to keep the CSV tidy.
    assert rows[1]["seed_number"] == ""
