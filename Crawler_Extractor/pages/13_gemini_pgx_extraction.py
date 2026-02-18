"""Run the PGX extraction stage for gpapers rows using Gemini."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.common.config import load_config
from src.common.logging import configure_logging
from src.common.repository import PaperRepository
from src.common.supabase_client import build_supabase_client
from src.ingest.storage_gateway import StorageGateway
from src.parsers.pgx_extractor import StructuredGeminiPgxExtractor
from src.pipelines.gpapers_sync import sync_gpapers_from_papers
from src.pipelines.pgx_extraction_stage import process_pgx_extraction_batch

configure_logging()
st.set_page_config(page_title="Gemini PGX Extraction", page_icon="🧬", layout="wide")
st.title("Gemini PGX Extraction")
st.caption(
    "Processes `gpapers` rows where `gstatus = P1`. Success moves gstatus to `P1Success`; "
    "failures move gstatus to `P1Failure`."
)

config = load_config()
if not config.google_api_key:
    st.error("Missing GOOGLE_API_KEY or GOOGLE_SEARCH_API_KEY for Gemini extraction.")
    st.stop()

client = build_supabase_client(config)
gpapers_repository = PaperRepository(client, table="gpapers", state_column="gstatus")
storage = StorageGateway(client, config.pdf_bucket, config.text_bucket)

prompt_path = Path("prompts/pgx_extraction_prompt.txt")
default_prompt = "Extract PGX sample-level observations into structured rows."
pgx_prompt = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else default_prompt
extractor = StructuredGeminiPgxExtractor(
    prompt=pgx_prompt,
    api_key=config.google_api_key,
    model_name=config.gemini_model,
)

with st.sidebar:
    st.header("Gemini PGX Controls")
    sync_rows = st.button("Sync gpapers from papers")
    file_count = st.number_input(
        "Number of files to process",
        min_value=1,
        max_value=1000,
        value=10,
        step=1,
    )
    workers = st.number_input("Parallel threads", min_value=1, max_value=32, value=1, step=1)
    run = st.button("Run Gemini PGX Extraction", type="primary")

if sync_rows:
    with st.spinner("Syncing gpapers from papers", show_time=True):
        stats = sync_gpapers_from_papers(client)
    st.success(
        f"Synced gpapers from papers. Fetched: {stats['fetched']}. Upserted: {stats['upserted']}."
    )

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

    with st.spinner("Running Gemini PGX extraction", show_time=True):
        outcomes = process_pgx_extraction_batch(
            batch_size=int(file_count),
            repository=gpapers_repository,
            storage=storage,
            extractor=extractor,
            extraction_table="pgx_gextractions",
            max_workers=int(workers),
            progress_callback=_progress,
            completion_callback=_completed,
        )

    if not outcomes:
        st.info("No rows with gstatus P1 were found in gpapers.")
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
