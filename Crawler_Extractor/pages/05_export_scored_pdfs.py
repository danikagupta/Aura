"""Utility page to export scored PDFs into a local folder."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import streamlit as st

from src.common.dtos import PaperRecord
from src.common.logging import configure_logging
from src.ui.export_helpers import build_scored_papers_csv
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Export Scored PDFs", page_icon="📁", layout="wide")
st.title("Export Scored PDFs")
st.caption(
    "Generate an `output/<seed-number>/<level>/<score>/` directory tree containing the PDFs for "
    "all scored papers."
)

services = init_services()
repository = services.repository
storage = services.storage
OUTPUT_ROOT = Path("output")


def _score_bucket(score: float | None) -> str:
    if score is None:
        return "unknown"
    return f"{score:.2f}"


def _seed_bucket(seed_number: Optional[int]) -> str:
    return str(seed_number) if seed_number is not None else "unknown"


def _safe_filename(title: str, fallback: str) -> str:
    truncated = (title or "").strip()[:40]
    cleaned = re.sub(r"[^A-Za-z0-9 _-]+", "", truncated)
    sanitized = cleaned.strip().replace(" ", "_")
    return f"{sanitized or fallback}.pdf"


def _export_paper(paper: PaperRecord) -> Path | None:
    if not paper.pdf_ref:
        return None
    try:
        pdf_bytes = storage.fetch_blob(paper.pdf_ref)
    except Exception:
        return None
    level_value = int(paper.level or 0)
    seed_dir = OUTPUT_ROOT / _seed_bucket(paper.seed_number)
    level_dir = seed_dir / str(level_value)
    score_dir = level_dir / _score_bucket(paper.score)
    score_dir.mkdir(parents=True, exist_ok=True)
    filename = _safe_filename(paper.title, paper.id)
    file_path = score_dir / filename
    if file_path.exists():
        return None
    file_path.write_bytes(pdf_bytes)
    return file_path


papers = repository.fetch_all_scored()
st.write(f"Found **{len(papers)}** papers with stored scores.")
csv_payload = build_scored_papers_csv(papers)
score_threshold = st.number_input(
    "Minimum score to export",
    min_value=0.0,
    max_value=10.0,
    value=0.0,
    step=0.1,
)
eligible_papers = [
    paper for paper in papers if paper.score is not None and paper.score >= score_threshold
]
st.write(f"Eligible papers ≥ {score_threshold:.1f}: **{len(eligible_papers)}**")

if st.button("Export PDFs"):
    csv_path = OUTPUT_ROOT / "scored_papers.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_bytes(csv_payload)
    st.success(f"Wrote scored papers CSV to `{csv_path.resolve()}`.")
    if not eligible_papers:
        st.info("No scored papers meet the selected score threshold.")
    else:
        progress = st.progress(0.0)
        log = st.empty()
        exported = 0
        for idx, paper in enumerate(eligible_papers, start=1):
            destination = (
                OUTPUT_ROOT
                / _seed_bucket(paper.seed_number)
                / str(int(paper.level or 0))
                / _score_bucket(paper.score)
            )
            filename = _safe_filename(paper.title, paper.id)
            dest_path = destination / filename
            if dest_path.exists():
                log.write(
                    f"Skipping {paper.title} (seed {paper.seed_number}, level {paper.level}, score {paper.score}) — already exists"
                )
            else:
                log.write(
                    f"Exporting {paper.title} (seed {paper.seed_number}, level {paper.level}, score {paper.score})"
                )
                if _export_paper(paper):
                    exported += 1
            progress.progress(idx / len(eligible_papers))
        st.success(
            f"Exported {exported} PDF(s) into `{OUTPUT_ROOT.resolve()}` "
            f"(skipped {len(eligible_papers) - exported})."
        )
