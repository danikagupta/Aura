"""Streamlit page for running a single stage at a specific level/state."""

from __future__ import annotations

import json
import random
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from src.common.dtos import PaperRecord, PaperState, StageOutcome
from src.common.logging import configure_logging
from src.pipelines.attempts import filter_processable, mark_processing_attempt
from src.pipelines.processing_steps import (
    extract_citations,
    extract_text,
    fetch_pdf,
    score_text,
)
from src.ui.services import init_services

configure_logging()
st.set_page_config(page_title="Targeted Processing", page_icon="🎯", layout="wide")
st.title("Targeted Processing")
st.caption(
    "Inspect paper counts by level/state, then run a single pipeline stage for the "
    "combination you choose."
)

services = init_services()
repository = services.repository

FETCH_LIMIT = 100
StageProcessor = Callable[[PaperRecord], StageOutcome]


def _eligible_candidates(
    state: PaperState,
    level: int,
    seed_number: Optional[int],
) -> List[PaperRecord]:
    candidates = repository.fetch_by_state_at_level(
        state,
        level,
        FETCH_LIMIT,
        seed_number=seed_number,
    )
    if state is PaperState.SCORED:
        candidates = [
            paper
            for paper in candidates
            if paper.score is not None and paper.score >= services.score_threshold
        ]
    return filter_processable(candidates)


def _stage_executor(state: PaperState, store_links: bool) -> Tuple[str, StageProcessor]:
    if state == PaperState.PDF_AVAILABLE:
        return "text_extraction", lambda paper: extract_text(
            mark_processing_attempt(repository, paper),
            repository=repository,
            extractor=services.text_extractor,
            store_links=store_links,
        )
    if state == PaperState.TEXT_AVAILABLE:
        return "scoring", lambda paper: score_text(
            mark_processing_attempt(repository, paper),
            repository=repository,
            storage=services.storage,
            scorer=services.scorer,
            threshold=services.score_threshold,
        )
    if state == PaperState.SCORED:
        return "citations", lambda paper: extract_citations(
            mark_processing_attempt(repository, paper),
            repository=repository,
            storage=services.storage,
            extractor=services.citation_extractor,
            threshold=services.score_threshold,
        )
    if state == PaperState.PDF_NOT_AVAILABLE:
        return "pdf_acquisition", lambda paper: fetch_pdf(
            mark_processing_attempt(repository, paper),
            repository=repository,
            storage=services.storage,
            fetcher=services.pdf_fetcher,
        )
    if state == PaperState.PROCESSED_LOW_SCORE:
        raise ValueError("Processed (Low Score) rows are terminal; rerun scoring to re-evaluate.")
    raise ValueError(f"No stage mapped for {state.value}")


def _rows_from_counts(counts: Dict[str, Dict[int, int]]) -> List[Dict[str, int]]:
    rows: List[Dict[str, int]] = []
    for status, level_counts in counts.items():
        for level, count in level_counts.items():
            rows.append({"status": status, "level": level, "count": count})
    rows.sort(key=lambda row: (row["status"], row["level"]))
    return rows


def _levels_from_counts(counts: Dict[str, Dict[int, int]]) -> List[int]:
    levels = {level for level_counts in counts.values() for level in level_counts}
    return sorted(levels)


def _candidate_query_details(
    state: PaperState,
    level: int,
    seed_number: Optional[int],
) -> str:
    seed_clause = (
        f" and seed_number={seed_number}" if seed_number is not None else ""
    )
    return (
        "select * from papers "
        f"where status='{state.value}' and level={level}{seed_clause} "
        f"limit {FETCH_LIMIT}"
    )


def _run_stage(
    state: PaperState,
    level: int,
    batch_size: int,
    progress_callback: Optional[Callable[[str, PaperRecord], None]],
    completion_callback: Optional[
        Callable[[str, PaperRecord, StageOutcome], None]
    ],
    status_callback: Optional[
        Callable[[int, int, str, Optional[float], Optional[float]], None]
    ],
    seed_number: Optional[int],
    threads: int,
    store_links: bool,
) -> Tuple[str, List[StageOutcome]]:
    stage_name, processor = _stage_executor(state, store_links)
    outcomes: List[StageOutcome] = []
    processed = 0
    last_remaining = 0
    max_workers = max(1, threads)
    inflight: Dict[Future[StageOutcome], PaperRecord] = {}
    start_times: Dict[Future[StageOutcome], float] = {}
    total_duration = 0.0
    query_details = _candidate_query_details(state, level, seed_number)
    if status_callback:
        status_callback(processed, last_remaining, query_details, None, None)

    def _drain_completed(completed_futures: List[Future[StageOutcome]]) -> None:
        nonlocal processed, total_duration
        for future in completed_futures:
            paper = inflight.pop(future)
            try:
                outcome = future.result()
            except Exception as exc:  # pragma: no cover - surfaced in UI/logs
                outcome = StageOutcome(
                    paper_id=paper.id,
                    stage=stage_name,
                    success=False,
                    message="unexpected error",
                    metadata={"error": str(exc)},
                )
            outcomes.append(outcome)
            processed += 1
            if completion_callback:
                completion_callback(stage_name, paper, outcome)
            if status_callback:
                started = start_times.pop(future, None)
                duration = (
                    time.perf_counter() - started
                    if started is not None
                    else None
                )
                if duration is not None:
                    total_duration += duration
                    average = total_duration / processed if processed else None
                else:
                    average = None
                status_callback(
                    processed,
                    last_remaining,
                    query_details,
                    duration,
                    average,
                )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while processed < batch_size:
            remaining_slots = batch_size - (processed + len(inflight))
            can_submit = len(inflight) < max_workers and remaining_slots > 0
            if can_submit:
                candidates = _eligible_candidates(state, level, seed_number)
                last_remaining = len(candidates)
                if not candidates:
                    if status_callback:
                        status_callback(processed, last_remaining, query_details, None, None)
                    break
                paper = random.choice(candidates)
                if progress_callback:
                    progress_callback(stage_name, paper)
                future = executor.submit(processor, paper)
                start_times[future] = time.perf_counter()
                inflight[future] = paper
                continue
            if not inflight:
                break
            done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
            _drain_completed(list(done))
        while inflight:
            done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)
            _drain_completed(list(done))
    if processed == 0 and status_callback:
        status_callback(0, last_remaining, query_details, None, None)
    return stage_name, outcomes


st.sidebar.header("Processing Controls")
seed_number = st.sidebar.number_input(
    "Seed number",
    min_value=1,
    value=4,
    step=1,
)

# AG: Commented out
#counts = repository.level_status_counts(seed_number=seed_number)
#if not counts:
#    if seed_number is not None:
#        st.info(f"No papers found for seed {seed_number}.")
#    else:
#        st.info("No papers found in Supabase.")
#    st.stop()

st.subheader(
    "Papers by Level and Status"
    + (f" (Seed {seed_number})" if seed_number is not None else "")
)
# AG
#rows = _rows_from_counts(counts)
#table = pd.DataFrame(rows, columns=["level", "status", "count"])
#st.dataframe(
#    table,
#    hide_index=True,
#)

#levels = _levels_from_counts(counts)
levels=[1,2,3,4,5]
run_stage = False
actionable_states = [
    PaperState.PDF_AVAILABLE,
    PaperState.TEXT_AVAILABLE,
    PaperState.SCORED,
    PaperState.PDF_NOT_AVAILABLE,
]
selected_state = st.sidebar.selectbox(
    "State",
    options=actionable_states,
    format_func=lambda s: s.value,
)
level_options = levels or [0]
default_level_index = level_options.index(2) if 2 in level_options else 0
selected_level = st.sidebar.selectbox(
    "Level",
    options=level_options,
    index=default_level_index,
)
batch_size = st.sidebar.number_input("Batch size", min_value=1, max_value=1250, value=5)
threads = st.sidebar.number_input("Parallel threads", min_value=1, max_value=50, value=1)
store_links = st.sidebar.checkbox(
    "Store extracted links",
    value=True,
    help="When enabled, text extraction creates PDF Not Available placeholders from PDF hyperlinks.",
)
run_stage = st.sidebar.button("Run Stage", type="primary")

if run_stage:
    if selected_state not in {
        PaperState.PDF_AVAILABLE,
        PaperState.TEXT_AVAILABLE,
        PaperState.SCORED,
        PaperState.PDF_NOT_AVAILABLE,
    }:
        st.warning(
            "Only actionable states (PDF Available, Text Available, Scored, PDF Not Available) "
            "can be processed."
        )
    else:
        current_placeholder = st.empty()
        counter_placeholder = st.empty()
        remaining_placeholder = st.empty()
        progress_log = st.container()
        counter_placeholder.write("Records processed this run: 0 (last record: N/A)")
        remaining_placeholder.markdown(
            "Unprocessed count from last query: **0**  \n`awaiting query`"
        )

        def _progress(stage: str, paper: PaperRecord) -> None:
            progress_log.write(f"[{stage}] {paper.title} (level {paper.level})")
            current_placeholder.write(
                f"Processing {paper.title} via {stage} at level {paper.level}"
            )

        def _completed(stage: str, paper: PaperRecord, outcome: StageOutcome) -> None:
            icon = "✅" if outcome.success else "❌"
            progress_log.write(
                f"{icon} [{stage}] Finished {paper.title} — {outcome.message}"
            )
            if outcome.message == "storage error" and outcome.metadata:
                progress_log.code(json.dumps(outcome.metadata, indent=2))

        def _status(
            processed: int,
            unprocessed: int,
            query_details: str,
            duration_seconds: Optional[float],
            average_seconds: Optional[float],
        ) -> None:
            duration_text = (
                f"{max(0, int(round(duration_seconds)))}s"
                if duration_seconds is not None
                else "N/A"
            )
            average_text = (
                f"{max(0, int(round(average_seconds)))}s"
                if average_seconds is not None
                else "N/A"
            )
            counter_placeholder.write(
                f"Records processed this run: {processed} (last: {duration_text}, avg: {average_text})"
            )
            remaining_placeholder.markdown(
                f"Unprocessed count from last query: **{unprocessed}**  \n`{query_details}`"
            )

        spinner_text = f"Processing {selected_state.value} at level {int(selected_level)}"
        try:
            with st.spinner(spinner_text, show_time=True):
                stage_name, stage_outcomes = _run_stage(
                    selected_state,
                    int(selected_level),
                    int(batch_size),
                    _progress,
                    _completed,
                    _status,
                    seed_number,
                    int(threads),
                    bool(store_links),
                )
        except ValueError as exc:
            st.error(str(exc))
        else:
            completed = len(stage_outcomes)
            if not stage_outcomes:
                st.info("No papers matched the chosen state/level.")
            else:
                st.success(
                    f"Stage '{stage_name}' finished with {completed} outcome(s)."
                )
                for outcome in stage_outcomes:
                    status_icon = "✅" if outcome.success else "❌"
                    st.markdown(
                        f"{status_icon} **{outcome.stage}** — {outcome.message} ({outcome.paper_id})"
                    )
                    if outcome.metadata:
                        st.code(json.dumps(outcome.metadata, indent=2), language="json")
