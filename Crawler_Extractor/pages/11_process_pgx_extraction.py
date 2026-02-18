"""Run the PGX extraction stage for rows in P1 state."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.common.config import load_config
from src.common.logging import configure_logging
from src.parsers.pgx_extractor import StructuredPgxExtractor
from src.pipelines.pgx_extraction_stage import process_pgx_extraction_batch
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Process PGX Extraction", page_icon="🧬", layout="wide")
st.title("Process PGX Extraction")
st.caption(
    "Processes rows where status is P1. Successful rows are marked P1Success, failures are marked P1Failure."
)

services = init_services()
config = load_config()

prompt_path = Path("prompts/pgx_extraction_prompt.txt")
default_prompt = "Extract PGX sample-level observations into structured rows."
pgx_prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else default_prompt
extractor = StructuredPgxExtractor(
    prompt=pgx_prompt,
    api_key=config.openai_api_key,
    model_name=config.openai_model,
)

with st.sidebar:
    st.header("PGX Controls")
    file_count = st.number_input(
        "Number of files to process",
        min_value=1,
        max_value=1000,
        value=10,
        step=1,
    )
    workers = st.number_input("Parallel threads", min_value=1, max_value=32, value=1, step=1)
    run = st.button("Run Process PGX Extraction", type="primary")

if run:
    progress_log = st.container()

    def _progress(stage: str, paper) -> None:
        page_count = paper.metadata.get("page_count")
        pages_text = f" | pages: {page_count}" if page_count is not None else ""
        progress_log.write(f"[{stage}] Processing {paper.id} - {paper.title}{pages_text}")

    def _completed(stage: str, paper, outcome) -> None:
        marker = "✅" if outcome.success else "❌"
        page_count = outcome.metadata.get("page_count")
        pages_text = f" | pages: {page_count}" if page_count is not None else ""
        progress_log.write(f"{marker} [{stage}] {paper.id}: {outcome.message}{pages_text}")

    with st.spinner("Running PGX extraction", show_time=True):
        outcomes = process_pgx_extraction_batch(
            batch_size=int(file_count),
            repository=services.repository,
            storage=services.storage,
            extractor=extractor,
            max_workers=int(workers),
            progress_callback=_progress,
            completion_callback=_completed,
        )

    if not outcomes:
        st.info("No rows with status P1 were found.")
    else:
        success_count = sum(1 for item in outcomes if item.success)
        failure_count = len(outcomes) - success_count
        st.success(
            f"Processed {len(outcomes)} file(s). Success: {success_count}. Failure: {failure_count}."
        )
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "paper_id": item.paper_id,
                        "stage": item.stage,
                        "success": item.success,
                        "message": item.message,
                        "page_count": item.metadata.get("page_count"),
                        "metadata": item.metadata,
                    }
                    for item in outcomes
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )
