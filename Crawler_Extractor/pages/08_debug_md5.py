"""Streamlit page to inspect and compare PDFs by MD5 hash."""

from __future__ import annotations

import base64
from typing import List

import pandas as pd
import streamlit as st

from src.common.dtos import PaperRecord, StorageRef
from src.common.errors import StorageError
from src.common.logging import configure_logging
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Debug MD5 Hash", page_icon="🔍", layout="wide")
st.title("Debug PDF MD5s")
st.caption("Look up Supabase rows by `pdf_md5` and inspect their stored PDFs side-by-side.")

services = init_services()
repository = services.repository
storage = services.storage

md5_input = st.sidebar.text_input("MD5 hash", value="", placeholder="e.g. e2c569be17396eca2a2e3c11578123ed")
show_limit = st.sidebar.number_input("Max rows", min_value=1, max_value=200, value=50, step=1)

rows: List[PaperRecord] = []
md5_hash = md5_input.strip().lower()
if md5_hash:
    rows = repository.fetch_by_pdf_md5(md5_hash)[: int(show_limit)]

if not md5_hash:
    st.info("Enter an MD5 hash in the sidebar to begin.")
elif not rows:
    st.warning("No papers found with that md5.")
else:
    table_rows = []
    for paper in rows:
        table_rows.append(
            {
                "paper_id": paper.id,
                "title": paper.title,
                "state": paper.state.value,
                "level": paper.level,
                "attempts": paper.attempts,
                "seed_number": paper.seed_number,
                "score": paper.score,
                "reason": paper.reason,
                "model_name": getattr(paper, "model_name", None),
                "duration_ms": getattr(paper, "duration_ms", None),
                "pdf_uri": paper.pdf_ref.uri if paper.pdf_ref else None,
                "text_uri": paper.text_ref.uri if paper.text_ref else None,
                "pdf_md5": paper.pdf_md5 or "",
                "metadata": paper.metadata,
                "parent_id": paper.parent_id,
                "created_at": paper.created_at,
                "updated_at": paper.updated_at,
            }
        )
    st.dataframe(pd.DataFrame(table_rows), hide_index=True)

    selected = st.multiselect(
        "Select up to two rows to review PDFs",
        options=rows,
        format_func=lambda paper: f"{paper.title[:60]} ({paper.id})",
        max_selections=2,
    )

    if not selected:
        st.info("Select one or two rows above to inspect their PDFs.")
    else:
        col_a, col_b = st.columns(2)

        def _render_pdf(column, paper: PaperRecord) -> None:
            column.subheader(paper.title)
            if not paper.pdf_ref:
                column.warning("No PDF reference stored.")
                return
            try:
                pdf_bytes = storage.fetch_blob(StorageRef(paper.pdf_ref.uri))
            except StorageError as exc:
                column.error(f"Failed to download PDF: {exc}")
                return
            column.download_button(
                label=f"Download PDF ({paper.id})",
                data=pdf_bytes,
                mime="application/pdf",
                file_name=f"{paper.id}.pdf",
            )
            encoded = base64.b64encode(pdf_bytes).decode("ascii")
            iframe = (
                "<iframe "
                "style='width:100%; height:600px;' "
                "frameborder='0' "
                f"src='data:application/pdf;base64,{encoded}#toolbar=1'></iframe>"
            )
            column.markdown(iframe, unsafe_allow_html=True)

        _render_pdf(col_a, selected[0])
        if len(selected) > 1:
            _render_pdf(col_b, selected[1])
