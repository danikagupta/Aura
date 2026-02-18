"""Interactive testing harness for stepping through pipeline stages."""

from __future__ import annotations

import json
from typing import List

import pandas as pd
import streamlit as st

from src.common.dtos import PaperRecord
from src.common.logging import configure_logging
from src.pipelines.dashboard import load_recent_papers
from src.pipelines.single_processor import process_single_paper
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Testing Lab", page_icon="🧪", layout="wide")
st.title("Testing Lab")
st.caption(
    "Inspect Supabase rows, pick a paper, and walk it through the deterministic pipeline "
    "with step-by-step logging."
)

services = init_services()
repository = services.repository


def _render_table(records: List[PaperRecord]) -> None:
    table_rows = [
        {
            "id": record.id,
            "title": record.title,
            "state": record.state.value,
            "level": record.level,
            "score": record.score,
            "reason": record.reason,
        }
        for record in records
    ]
    st.dataframe(pd.DataFrame(table_rows))


papers = load_recent_papers(repository, per_state_limit=50)
if not papers:
    st.info("No papers available in Supabase.")
else:
    _render_table(papers)
    selected = st.selectbox(
        "Select a paper to process",
        options=papers,
        format_func=lambda r: f"{r.title} — {r.state.value}" if isinstance(r, PaperRecord) else str(r),
    )
    if st.button("Process Selected Paper", type="primary"):
        with st.spinner("Running pipeline..."):
            result = process_single_paper(selected.id, services)
        st.session_state["testing_result"] = result
        st.success("Processing complete.")

result = st.session_state.get("testing_result")
if result:
    st.subheader("Processing Summary")
    st.write(
        f"Started: **{result.started_state.value}** → Finished: **{result.finished_state.value}**"
    )
    stages_text = ", ".join(result.stages_run) if result.stages_run else "None"
    st.write(f"Stages executed: {stages_text}")
    st.write(f"Outcome: {'✅ Success' if result.success else '❌ Failed'}")

    st.subheader("Detailed Log")
    for entry in result.logs:
        icon = "✅" if entry.success else ("❌" if entry.success is False else "•")
        st.markdown(f"{icon} **[{entry.stage}]** {entry.message}")
        if entry.details:
            st.code(json.dumps(entry.details, indent=2), language="json")
