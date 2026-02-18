"""Export PGX-related tables as CSV files."""

from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from src.common.logging import configure_logging
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Export PGX CSV", page_icon="📤", layout="wide")
st.title("Export PGX CSV")
st.caption(
    "Download CSV snapshots for the full pgx_extractions table and P1-stage papers."
)

services = init_services()
repository = services.repository

P1_STATUSES = ["P1", "P1Success", "P1Failure", "P1WIP"]
PAPERS_FETCH_LIMIT = 100_000


@st.cache_data(show_spinner=False)
def _load_pgx_rows() -> list[dict]:
    return repository.fetch_all_rows_from_table("pgx_extractions")


@st.cache_data(show_spinner=False)
def _load_p1_papers() -> pd.DataFrame:
    papers = repository.fetch_by_statuses(P1_STATUSES, limit=PAPERS_FETCH_LIMIT)
    return pd.DataFrame(
        [
            {
                "id": paper.id,
                "title": paper.title,
                "status": paper.state.value,
                "level": paper.level,
                "attempts": paper.attempts,
                "source_uri": paper.pdf_ref.uri if paper.pdf_ref else None,
                "text_uri": paper.text_ref.uri if paper.text_ref else None,
                "score": paper.score,
                "reason": paper.reason,
                "seed_number": paper.seed_number,
                "parent_id": paper.parent_id,
                "pdf_md5": paper.pdf_md5,
                "metadata": paper.metadata,
                "created_at": paper.created_at,
                "updated_at": paper.updated_at,
            }
            for paper in papers
        ]
    )


def _to_csv_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


with st.sidebar:
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

pgx_rows = _load_pgx_rows()
pgx_frame = pd.DataFrame(pgx_rows)
st.subheader("1) Complete table: pgx_extractions")
st.write(f"Rows: **{len(pgx_frame)}**")
st.download_button(
    "Download pgx_extractions.csv",
    data=_to_csv_bytes(pgx_frame),
    file_name="pgx_extractions.csv",
    mime="text/csv",
)
if not pgx_frame.empty:
    st.dataframe(pgx_frame.head(200), hide_index=True, use_container_width=True)

papers_frame = _load_p1_papers()
st.subheader("2) papers where status in ('P1','P1Success','P1Failure','P1WIP')")
st.write(f"Rows: **{len(papers_frame)}** (max fetched: {PAPERS_FETCH_LIMIT})")
st.download_button(
    "Download papers_p1_states.csv",
    data=_to_csv_bytes(papers_frame),
    file_name="papers_p1_states.csv",
    mime="text/csv",
)
if not papers_frame.empty:
    st.dataframe(papers_frame.head(200), hide_index=True, use_container_width=True)
