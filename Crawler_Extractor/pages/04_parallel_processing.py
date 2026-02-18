"""Run all actionable pipeline stages in parallel threads."""

from __future__ import annotations

import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
from typing import Dict, List, Optional, Tuple

import streamlit as st

from src.common.dtos import PaperRecord, PaperState, StageOutcome
from src.common.logging import configure_logging, get_logger
from src.pipelines.attempts import filter_processable
from src.pipelines.paper_processor import PaperProcessor, StageName, STATE_FOR_STAGE
from src.ui.services import init_services

configure_logging()
LOGGER = get_logger(__name__)
st.set_page_config(page_title="Parallel Processing", page_icon="🧵", layout="wide")
st.title("Parallel Processing")
st.caption(
    "Launch PDF acquisition, text extraction, scoring, and citation expansion simultaneously "
    "for a specific seed/level. Each stage processes up to the selected batch size."
)

services = init_services()


def _stage_configs() -> List[Tuple[str, StageName]]:
    return [
        ("PDF Acquisition (PDF Not Available)", "pdf_acquisition"),
        ("Text Extraction (PDF Available)", "text_extraction"),
        ("Scoring (Text Available)", "scoring"),
        ("Citation Expansion (Scored)", "citations"),
    ]


def _make_trackers(labels: List[str]) -> Dict[str, Dict[str, object]]:
    trackers: Dict[str, Dict[str, object]] = {}
    for label in labels:
        placeholder = st.empty()
        placeholder.info(f"{label}: waiting to start")
        trackers[label] = {
            "completed": 0,
            "target": 0,
            "lock": threading.Lock(),
            "placeholder": placeholder,
            "last_duration": None,
            "total_duration": 0.0,
        }
    return trackers


def _run_stage(
    *,
    label: str,
    stage_name: StageName,
    level: int,
    seed_number: int,
    batch_size: int,
    tracker: Dict[str, object],
    progress_queue: Queue[Tuple[str, int, int, Optional[float]]],
) -> List[StageOutcome]:
    state: PaperState = STATE_FOR_STAGE[stage_name]
    processor = PaperProcessor(services)
    outcomes: List[StageOutcome] = []
    processed = 0
    remaining = float("inf")
    while True:
        records = services.repository.fetch_by_state_at_level(
            state,
            level,
            batch_size,
            seed_number=seed_number,
        )
        papers = filter_processable(records)
        remaining = len(papers)
        if remaining == 0:
            progress_queue.put((label, processed, remaining, None))
            break
        paper = random.choice(papers)
        started = time.perf_counter()
        outcome = processor.run(paper, stage=stage_name)
        duration = time.perf_counter() - started
        outcomes.append(outcome)
        processed += 1
        progress_queue.put((label, processed, remaining - 1, duration))
        if processed >= batch_size:
            break
    return outcomes


with st.sidebar:
    st.header("Processing Inputs")
    seed_number = st.number_input("Seed number", min_value=1, value=2, step=1)
    level = st.number_input("Level", min_value=1, value=1, step=1)
    batch_size = st.number_input("Batch size", min_value=1, max_value=2000, value=100, step=10)
    trigger = st.button("Submit", type="primary")

if trigger:
    stage_configs = _stage_configs()
    trackers = _make_trackers([label for label, _ in stage_configs])
    status_box = st.empty()
    results = []
    progress_queue: Queue[Tuple[str, int, int, Optional[float]]] = Queue()
    with ThreadPoolExecutor(max_workers=len(stage_configs)) as executor:
        future_map = {}
        for label, stage_name in stage_configs:
            future_map[
                executor.submit(
                    _run_stage,
                    label=label,
                    stage_name=stage_name,
                    level=int(level),
                    seed_number=int(seed_number),
                    batch_size=int(batch_size),
                    tracker=trackers[label],
                    progress_queue=progress_queue,
                )
            ] = label
        pending = set(future_map.keys())
        while pending:
            # update UI if progress messages exist
            try:
                label, count, total, duration = progress_queue.get(timeout=0.1)
                placeholder = trackers[label]["placeholder"]
                if total == 0 and count == 0:
                    placeholder.info(f"{label}: no matching papers found.")
                else:
                    if duration is not None:
                        trackers[label]["last_duration"] = duration
                        trackers[label]["total_duration"] = (
                            trackers[label].get("total_duration", 0.0) + duration
                        )
                    last_duration = trackers[label]["last_duration"]
                    cumulative = trackers[label].get("total_duration", 0.0)
                    average = (
                        cumulative / count if count and cumulative else None
                    )
                    duration_text = (
                        f" ({max(0, int(round(last_duration)))} seconds)"
                        if last_duration is not None
                        else ""
                    )
                    avg_text = (
                        f" (avg {max(0, int(round(average)))} seconds)"
                        if average is not None
                        else ""
                    )
                    placeholder.success(
                        f"{label}: finished processing {count}{duration_text}{avg_text}, "
                        f"unprocessed count {max(total, 0)}"
                    )
            except Empty:
                pass
            done = {future for future in pending if future.done()}
            for future in done:
                label = future_map[future]
                try:
                    outcomes = future.result()
                    results.append((label, outcomes, None))
                except Exception as exc:  # pragma: no cover - surfaced to UI
                    LOGGER.exception("Parallel stage failed", extra={"stage": label})
                    results.append((label, [], str(exc)))
                pending.remove(future)
        # drain any remaining progress updates
        while not progress_queue.empty():
            try:
                label, count, total, duration = progress_queue.get_nowait()
            except Empty:
                break
            placeholder = trackers[label]["placeholder"]
            if total == 0 and count == 0:
                placeholder.info(f"{label}: no matching papers found.")
            else:
                if duration is not None:
                    trackers[label]["last_duration"] = duration
                    trackers[label]["total_duration"] = (
                        trackers[label].get("total_duration", 0.0) + duration
                    )
                last_duration = trackers[label]["last_duration"]
                cumulative = trackers[label].get("total_duration", 0.0)
                average = cumulative / count if count and cumulative else None
                duration_text = (
                    f" ({max(0, int(round(last_duration)))} seconds)"
                    if last_duration is not None
                    else ""
                )
                avg_text = (
                    f" (avg {max(0, int(round(average)))} seconds)"
                    if average is not None
                    else ""
                )
                placeholder.success(
                    f"{label}: finished processing {count}{duration_text}{avg_text}, unprocessed count {max(total, 0)}"
                )

    status_box.success("Parallel processing complete.")
    for label, outcomes, error in results:
        with st.expander(f"{label}", expanded=True):
            if error:
                st.error(f"Stage failed: {error}")
                continue
            if not outcomes:
                st.info("No matching papers were processed.")
                continue
            st.success(f"Processed {len(outcomes)} paper(s).")
            for outcome in outcomes:
                icon = "✅" if outcome.success else "❌"
                st.markdown(f"{icon} **{outcome.stage}** — {outcome.message} ({outcome.paper_id})")
                if outcome.metadata:
                    st.code(json.dumps(outcome.metadata, indent=2), language="json")
