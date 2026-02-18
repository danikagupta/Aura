"""Streamlit page that summarizes paper counts by status and score."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.common.logging import configure_logging
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Status Dashboard", page_icon="📊", layout="wide")
st.title("Pipeline Status")
st.caption("Counts refresh automatically every 60 seconds.")
st.markdown(
    """
    <script>
        setTimeout(function() { window.location.reload(); }, 60000);
    </script>
    """,
    unsafe_allow_html=True,
)

services = init_services()
repository = services.repository

seed_number = st.sidebar.number_input("Seed filter", min_value=1, value=4, step=1)
level = st.sidebar.number_input("Level filter", min_value=1, value=1, step=1)
if st.sidebar.button("Refresh"):
    st.rerun()


def _format_seed(value):
    return value if value is not None else "unassigned"


status_rows = repository.level_status_rows(seed_number=seed_number)
score_rows = repository.level_score_rows(seed_number=seed_number)
filtered_status_rows = [row for row in status_rows if int(row.get("level") or 0) == int(level)]
filtered_score_rows = [row for row in score_rows if int(row.get("level") or 0) == int(level)]

st.subheader("Papers by Level, Status, and Seed")
if not filtered_status_rows:
    st.info("No papers found in Supabase.")
else:
    status_table = pd.DataFrame(
        [
            {
                "seed_number": _format_seed(row.get("seed_number")),
                "level": row.get("level"),
                "status": row.get("status"),
                "count": int(row.get("total") or 0),
            }
            for row in filtered_status_rows
        ],
        columns=["seed_number", "level", "status", "count"],
    ).sort_values(["seed_number", "level", "status"])
    st.dataframe(status_table, hide_index=True)

st.subheader("Papers by Score and Seed")
if not filtered_score_rows:
    st.info("No scored papers yet.")
else:
    score_table = pd.DataFrame(
        [
            {
                "seed_number": _format_seed(row.get("seed_number")),
                "level": row.get("level"),
                "score": row.get("score"),
                "count": int(row.get("total") or 0),
            }
            for row in filtered_score_rows
        ],
        columns=["seed_number", "level", "score", "count"],
    ).sort_values(["seed_number", "level", "score"])
    st.dataframe(score_table, hide_index=True)

st.subheader("Papers by Seed Number and Status (Selected Level)")
if not filtered_status_rows:
    st.info("No seeded papers yet.")
else:
    status_counts: dict[str, int] = {}
    for row in filtered_status_rows:
        status = str(row.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + int(row.get("total") or 0)
    seed_table = pd.DataFrame(
        [
            {
                "seed_number": seed_number,
                "level": int(level),
                "status": status,
                "count": count,
            }
            for status, count in status_counts.items()
        ],
        columns=["seed_number", "level", "status", "count"],
    ).sort_values(["seed_number", "level", "status"])
    st.dataframe(seed_table, hide_index=True)
