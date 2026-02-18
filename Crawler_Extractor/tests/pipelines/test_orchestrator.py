from __future__ import annotations

from src.pipelines import orchestrator
from src.pipelines.orchestrator import process_pipeline_batch
from src.pipelines.services import PipelineServices


def test_process_pipeline_batch_runs_all_stages(monkeypatch) -> None:
    calls: list[str] = []

    def _make(stage_name: str):
        def _runner(**_: object):
            calls.append(stage_name)
            return [stage_name]

        return _runner

    monkeypatch.setattr(
        orchestrator, "process_text_extraction_batch", _make("text")  # type: ignore[attr-defined]
    )
    monkeypatch.setattr(
        orchestrator, "process_scoring_batch", _make("scoring")  # type: ignore[attr-defined]
    )
    monkeypatch.setattr(
        orchestrator, "process_citation_batch", _make("citations")  # type: ignore[attr-defined]
    )
    monkeypatch.setattr(
        orchestrator,
        "process_pdf_acquisition_batch",
        _make("pdf"),  # type: ignore[attr-defined]
    )

    class _Repo:
        def lowest_active_level(self) -> int:
            return 1

    services = PipelineServices(
        repository=_Repo(),  # type: ignore[arg-type]
        storage=None,  # type: ignore[arg-type]
        text_extractor=None,  # type: ignore[arg-type]
        scorer=None,  # type: ignore[arg-type]
        citation_extractor=None,  # type: ignore[arg-type]
        pdf_fetcher=None,  # type: ignore[arg-type]
        score_threshold=0.7,
    )

    outcomes = process_pipeline_batch(2, services)

    assert calls == ["text", "scoring", "citations", "pdf"]
    assert outcomes["text"] == ["text"]
