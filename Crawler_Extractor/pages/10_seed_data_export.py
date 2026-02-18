"""Streamlit page for viewing and exporting all rows for a seed number."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.common.logging import configure_logging
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Seed Data Export", page_icon="🧾", layout="wide")
st.title("Seed Data Export")
st.caption("View all `papers` table rows for a seed number and download them as CSV.")

services = init_services()
repository = services.repository

seed_number = st.number_input(
    "Seed number",
    min_value=1,
    value=4,
    step=1,
)

if st.button("Load rows"):
    rows = repository.fetch_rows_by_seed_number(seed_number=int(seed_number))
    st.session_state["seed_export_rows"] = rows
    st.session_state["seed_export_seed"] = int(seed_number)

rows = st.session_state.get("seed_export_rows")
selected_seed = st.session_state.get("seed_export_seed", int(seed_number))

if rows is not None:
    if not rows:
        st.info(f"No rows found for seed {selected_seed}.")
    else:
        dataframe = pd.DataFrame(rows)
        st.write(f"Rows found for seed {selected_seed}: **{len(dataframe)}**")
        st.dataframe(dataframe, use_container_width=True, hide_index=True)

        default_filename = f"papers_seed_{selected_seed}.csv"
        filename = st.text_input("CSV filename", value=default_filename).strip()
        if not filename:
            filename = default_filename
        if not filename.lower().endswith(".csv"):
            filename = f"{filename}.csv"

        csv_payload = dataframe.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download CSV",
            data=csv_payload,
            file_name=filename,
            mime="text/csv",
        )
