"""Home page for the crawler control panel."""

from __future__ import annotations

from textwrap import dedent

import streamlit as st

from src.common.logging import configure_logging

configure_logging()
st.set_page_config(page_title="Crawler Overview", page_icon="🧭")
st.title("Crawler Operations Overview")
st.write(
    "Use the sidebar to jump into the specialized pages. "
    "This overview highlights what each page offers and shows how papers flow through the pipeline."
)

st.subheader("Available Pages")
st.markdown(
    """
- **Seed Upload** — Submit new PDFs and assign their `seed_number`. Every downstream citation inherits this lineage marker.
- **Status Dashboard** — Inspect counts grouped by level, status, score bucket, and seed number for a quick health check.
- **Targeted Processing** — Run a single stage (text extraction, scoring, citations, or PDF acquisition) for a specific level and seed lineage.
- **Extended Processing** — Run the targeted stage cycle repeatedly for fixed seed/level settings with configurable loops.
- **Process PGX Extraction** — Process rows in `P1`, extract PGX sample records, and persist them to the PGX extraction table.
- **Gemini PGX Extraction** — Sync `gpapers` from `papers` P1 statuses, then process `gpapers.gstatus = P1` rows with Gemini.
- **Export PGX CSV** — Download CSV snapshots for `pgx_extractions` and `papers` in P1 workflow statuses.
- **Export Scored PDFs** — Download CSV snapshots or export PDFs to `output/<seed-number>/<level>/<score>/`.
- **Testing Lab** — Execute curated smoke tests and diagnostics in isolation.
"""
)

st.subheader("State Transition Diagram")
st.caption(
    "Papers advance through deterministic stages. Failures reset the attempts counter and surface on the status dashboard."
)

STATE_DIAGRAM = dedent(
    """
    digraph pipeline {
        rankdir=LR;
        node [shape=box, style="rounded,filled", fontname="Helvetica"];
        pdf_available [label="PDF Available", fillcolor="#9ecae1", color="#3182bd"];
        text_available [label="Text Available", fillcolor="#c7e9c0", color="#31a354"];
        scored [label="Scored", fillcolor="#fee391", color="#fec44f"];
        processed_low [label="Processed (Low Score)", fillcolor="#f7f7f7", color="#969696"];
        citations [label="PDF Not Available", fillcolor="#fdd0a2", color="#f16913"];
        processed [label="Processed", fillcolor="#d9d9d9", color="#636363"];
        failed [label="Failed", fillcolor="#fcbba1", color="#cb181d"];

        pdf_available -> text_available [label="Extract Text"];
        text_available -> scored [label="Score"];
        scored -> citations [label="Extract Citations"];
        scored -> processed_low [label="Below threshold"];
        citations -> pdf_available [label="Fetch PDF"];
        scored -> processed [label="High confidence"];
        pdf_available -> failed [label="Extraction error", style="dashed"];
        text_available -> failed [label="Missing text", style="dashed"];
        citations -> failed [label="Acquisition error", style="dashed"];
    }
    """
)
st.graphviz_chart(STATE_DIAGRAM)

st.subheader("Getting Started")
st.write(
    "1. Upload a seed PDF with its identifying number.\n"
    "2. Use the Status Dashboard to confirm it appears.\n"
    "3. Drive targeted execution or exports as needed.\n"
    "Automation-friendly APIs mirror every UI action so you can also script these workflows."
)
