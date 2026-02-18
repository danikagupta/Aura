"""Streamlit page for uploading new seed PDFs."""

from __future__ import annotations

import streamlit as st

from src.common.errors import DuplicateTopicError, RepositoryError
from src.common.logging import configure_logging, get_logger
from src.pipelines.seed import upload_seed_pdf
from src.ui.services import init_services

configure_logging()
LOGGER = get_logger(__name__)
st.set_page_config(page_title="Seed Upload", page_icon="🌱")
st.title("Seed Upload")
st.caption("Upload a PDF to create a new seed record for downstream processing.")

services = init_services()
repository = services.repository
storage = services.storage

seed_number = st.number_input("Seed number", min_value=1, step=1, value=1, format="%d")
upload = st.file_uploader("Upload PDF", type=["pdf"])

if st.button("Save Seed", type="primary"):
    if not upload:
        st.warning("Please upload a PDF first.")
        st.stop()

    derived_title = f"SEED: {upload.name}"
    try:
        record = upload_seed_pdf(
            file_bytes=upload.getvalue(),
            filename=upload.name,
            title=derived_title,
            level=1,
            seed_number=int(seed_number),
            repository=repository,
            storage=storage,
        )
    except DuplicateTopicError:
        LOGGER.info("Duplicate topic blocked seed upload", exc_info=False)
        st.error("A paper already exists for this topic. Please choose another topic.")
    except RepositoryError as exc:
        LOGGER.exception("Seed upload failed")
        st.error(f"Failed to store seed paper: {exc}")
    else:
        st.success(f"Stored seed paper {record.id}")
