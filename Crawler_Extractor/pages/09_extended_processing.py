"""Run all actionable processing states in parallel for a fixed seed and level."""

from __future__ import annotations

import json
import random
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple

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
st.set_page_config(page_title="Extended Processing", page_icon="🔁", layout="wide")
st.title("Extended Processing")
st.caption(
    "Run one worker per actionable state for a fixed seed and level, up to a target batch size."
)

services = init_services()
repository = services.repository

FETCH_LIMIT = 100
StageProcessor = Callable[[PaperRecord], StageOutcome]
StateProgressCallback = Callable[[str], None]
StateOutcomeCallback = Callable[[StageOutcome], None]


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
    raise ValueError(f"No stage mapped for {state.value}")


def _run_state_worker(
    state: PaperState,
    level: int,
    batch_size: int,
    seed_number: int,
    store_links: bool,
    progress_callback: StateProgressCallback,
    outcome_callback: StateOutcomeCallback,
) -> Tuple[str, List[StageOutcome]]:
    stage_name, processor = _stage_executor(state, store_links)
    outcomes: List[StageOutcome] = []
    progress_callback(
        f"Starting {state.value} -> {stage_name} (seed={seed_number}, level={level}, batch={batch_size})"
    )
    for idx in range(1, batch_size + 1):
        candidates = _eligible_candidates(state, level, seed_number)
        if not candidates:
            progress_callback(f"No remaining eligible candidates after {idx - 1} processed.")
            break
        paper = random.choice(candidates)
        progress_callback(f"[{idx}/{batch_size}] Processing {paper.id}: {paper.title}")
        try:
            outcome = processor(paper)
        except Exception as exc:  # pragma: no cover - surfaced in UI/logs
            outcome = StageOutcome(
                paper_id=paper.id,
                stage=stage_name,
                success=False,
                message="unexpected error",
                metadata={"error": str(exc)},
            )
        outcomes.append(outcome)
        outcome_callback(outcome)
        marker = "OK" if outcome.success else "FAIL"
        progress_callback(f"[{idx}/{batch_size}] {marker} {outcome.paper_id}: {outcome.message}")
        if not outcome.success and outcome.metadata:
            progress_callback(json.dumps(outcome.metadata, sort_keys=True))
    progress_callback(f"Finished with {len(outcomes)} processed rows.")
    return stage_name, outcomes


with st.sidebar:
    st.header("Processing Controls")
    seed_number = st.number_input("Seed number", min_value=1, value=4, step=1)
    level = st.number_input("Level", min_value=1, value=1, step=1)
    batch_size = st.number_input("Batch size", min_value=1, max_value=1250, value=5)
    store_links = st.checkbox(
        "Store extracted links",
        value=True,
        help="When enabled, text extraction creates PDF Not Available placeholders from PDF hyperlinks.",
    )
    run_processing = st.button("Run Extended Processing", type="primary")
    st.divider()
    st.subheader("State Activity")
    activity_indicator = st.empty()

actionable_states = [
    PaperState.PDF_AVAILABLE,
    PaperState.TEXT_AVAILABLE,
    PaperState.SCORED,
    PaperState.PDF_NOT_AVAILABLE,
]

tabs = st.tabs([state.value for state in actionable_states])
tab_views: Dict[PaperState, Dict[str, Any]] = {}
for state, tab in zip(actionable_states, tabs):
    with tab:
        st.subheader(state.value)
        progress = st.progress(0.0)
        status = st.empty()
        log_box = st.empty()
    tab_views[state] = {
        "progress": progress,
        "status": status,
        "log": log_box,
    }

if run_processing:
    run_logs: Dict[PaperState, List[str]] = {state: [] for state in actionable_states}
    run_stats: Dict[PaperState, Dict[str, int]] = {
        state: {"processed": 0, "successes": 0, "failures": 0}
        for state in actionable_states
    }
    run_state_status: Dict[PaperState, str] = {state: "queued" for state in actionable_states}
    run_lock = Lock()
    stage_names: Dict[PaperState, str] = {}
    outcomes_by_state: Dict[PaperState, List[StageOutcome]] = {
        state: [] for state in actionable_states
    }
    summary_rows: List[Dict[str, int | str]] = []
    started_at = time.perf_counter()

    def _append_log(state: PaperState, line: str) -> None:
        with run_lock:
            run_logs[state].append(line)

    def _set_state_status(state: PaperState, status: str) -> None:
        with run_lock:
            run_state_status[state] = status

    def _render_activity_indicator() -> None:
        with run_lock:
            labels = []
            for state in actionable_states:
                status = run_state_status[state]
                processed = run_stats[state]["processed"]
                target = int(batch_size)
                if status == "running":
                    marker = "🟢"
                elif status == "done":
                    marker = "✅"
                else:
                    marker = "⏳"
                labels.append(f"{marker} **{state.value}** `{processed}/{target}`")
        activity_indicator.markdown(" | ".join(labels))

    def _append_outcome(state: PaperState, outcome: StageOutcome) -> None:
        with run_lock:
            run_stats[state]["processed"] += 1
            if outcome.success:
                run_stats[state]["successes"] += 1
            else:
                run_stats[state]["failures"] += 1

    def _render_state(state: PaperState) -> None:
        with run_lock:
            processed = run_stats[state]["processed"]
            successes = run_stats[state]["successes"]
            failures = run_stats[state]["failures"]
            lines = run_logs[state][-200:]
        target = int(batch_size)
        tab_views[state]["progress"].progress(min(processed / target, 1.0))
        tab_views[state]["status"].write(
            f"Processed: **{processed}/{target}** | Success: **{successes}** | Failure: **{failures}**"
        )
        tab_views[state]["log"].code("\n".join(lines) if lines else "Waiting to start...")

    with ThreadPoolExecutor(max_workers=len(actionable_states)) as executor:
        futures: Dict[Future[Tuple[str, List[StageOutcome]]], PaperState] = {}
        for state in actionable_states:
            _set_state_status(state, "running")
            future = executor.submit(
                _run_state_worker,
                state,
                int(level),
                int(batch_size),
                int(seed_number),
                bool(store_links),
                lambda line, current_state=state: _append_log(current_state, line),
                lambda outcome, current_state=state: _append_outcome(current_state, outcome),
            )
            futures[future] = state

        pending = set(futures.keys())
        _render_activity_indicator()
        while pending:
            done, _ = wait(pending, timeout=0.5, return_when=FIRST_COMPLETED)
            if not done:
                _render_activity_indicator()
                for state in actionable_states:
                    _render_state(state)
                continue
            for future in done:
                state = futures[future]
                stage_name, outcomes = future.result()
                stage_names[state] = stage_name
                outcomes_by_state[state] = outcomes
                _set_state_status(state, "done")
                pending.remove(future)
            _render_activity_indicator()
            for state in actionable_states:
                _render_state(state)

    duration_seconds = time.perf_counter() - started_at
    _render_activity_indicator()
    for state in actionable_states:
        outcomes = outcomes_by_state[state]
        success_count = sum(1 for outcome in outcomes if outcome.success)
        failure_count = len(outcomes) - success_count
        summary_rows.append(
            {
                "input_state": state.value,
                "stage": stage_names.get(state, "unknown"),
                "processed": len(outcomes),
                "successes": success_count,
                "failures": failure_count,
            }
        )

    st.subheader("Run Summary")
    st.caption(f"Total runtime: {duration_seconds:.1f}s")
    st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)
