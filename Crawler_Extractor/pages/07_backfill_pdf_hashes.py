"""Streamlit page to backfill missing pdf_md5 checksums."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import pandas as pd
import streamlit as st

from src.common.dtos import PaperRecord
from src.common.errors import StorageError
from src.common.logging import configure_logging
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Backfill PDF Hashes", page_icon="♻️", layout="wide")
st.title("Backfill PDF Hashes")
st.caption(
    "Scan papers with missing `pdf_md5` values, recompute checksums from Supabase Storage, "
    "and persist the results."
)

services = init_services()
repository = services.repository
storage = services.storage

st.sidebar.header("Filters")
apply_seed_filter = st.sidebar.checkbox("Limit to a specific seed", value=False)
seed_number: Optional[int] = None
if apply_seed_filter:
    seed_number = st.sidebar.number_input("Seed number", min_value=1, value=1, step=1)
missing_only = st.sidebar.checkbox("Only show missing hashes", value=True)
batch_size = st.sidebar.number_input(
    "Batch size",
    min_value=10,
    max_value=10000,
    value=100,
    step=10,
)
threads = st.sidebar.number_input(
    "Parallel threads",
    min_value=1,
    max_value=32,
    value=4,
    step=1,
)

candidates: List[PaperRecord] = repository.fetch_pdf_hash_candidates(
    int(batch_size),
    seed_number=int(seed_number) if seed_number is not None else None,
    missing_only=missing_only,
)

st.write(
    f"Found **{len(candidates)}** paper(s) "
    f"{'without' if missing_only else 'with'} stored hashes."
)

if not candidates:
    st.info("Nothing to backfill with the current filters.")
else:
    table_rows = [
        {
            "paper_id": paper.id,
            "title": paper.title[:80],
            "state": paper.state.value,
            "level": paper.level,
            "seed_number": paper.seed_number,
            "pdf_md5": paper.pdf_md5 or "",
        }
        for paper in candidates
    ]
    st.dataframe(pd.DataFrame(table_rows), hide_index=True)

    if st.button("Backfill Selected Batch", type="primary"):
        progress = st.progress(0.0)
        log = st.container()
        updated = 0
        errors = 0
        skipped = 0

        def _persist_hash(paper: PaperRecord, digest: str) -> None:
            updater = getattr(repository, "update_pdf_md5", None)
            if callable(updater):
                updater(paper.id, digest)
            else:
                repository.update_state(
                    paper.id,
                    paper.state,
                    updates={"pdf_md5": digest},
                )

        def _process(paper: PaperRecord) -> tuple[str, str]:
            if not paper.pdf_ref:
                return paper.id, "⚠️ No PDF reference stored."
            try:
                blob = storage.fetch_blob(paper.pdf_ref)
            except StorageError as exc:
                return paper.id, f"❌ Failed to download PDF: {exc}"
            digest = hashlib.md5(blob).hexdigest()
            if paper.pdf_md5 == digest and missing_only:
                return paper.id, "ℹ️ Up to date; skipping."
            _persist_hash(paper, digest)
            return paper.id, f"✅ Updated hash to {digest}"

        with ThreadPoolExecutor(max_workers=int(threads)) as executor:
            futures = {executor.submit(_process, paper): paper for paper in candidates}
            completed = 0
            for future in as_completed(futures):
                completed += 1
                progress.progress(completed / len(candidates))
                paper = futures[future]
                try:
                    paper_id, message = future.result()
                except Exception as exc:  # pragma: no cover - surfaced in UI
                    errors += 1
                    log.write(f"❌ {paper.id}: unexpected error {exc}")
                    continue
                if message.startswith("✅"):
                    updated += 1
                elif message.startswith("⚠️") or message.startswith("ℹ️"):
                    skipped += 1
                else:
                    errors += 1
                log.write(f"{message} ({paper.title} — {paper_id})")

        progress.empty()
        st.success(f"Updated {updated} paper(s).")
        if skipped:
            st.info(f"{skipped} paper(s) were skipped.")
        if errors:
            st.warning(f"{errors} paper(s) could not be processed. Check the logs above.")
