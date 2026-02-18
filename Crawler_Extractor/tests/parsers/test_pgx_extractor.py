from __future__ import annotations

import pytest

from src.common.errors import ExtractionError
from src.parsers.pgx_extractor import (
    StructuredGeminiPgxExtractor,
    StructuredPgxExtractor,
    _render_pdf_pages,
)


class _FakeLLM:
    def __init__(self) -> None:
        self.calls = 0

    def with_structured_output(self, schema):
        class _Wrapper:
            def __init__(self, schema, outer):
                self._schema = schema
                self._outer = outer

            def invoke(self, _messages):
                self._outer.calls += 1
                return self._schema(
                    samples=[
                        {
                            "sample_id": f"Case-{self._outer.calls}",
                            "gene": "CYP2C19",
                            "allele": "*1/*2",
                            "rs_id": "rs4244285",
                            "medication": "Clopidogrel",
                            "outcome": "Ineffective",
                            "actionability": "yes",
                            "cpic_recommendation": "Use alternative antiplatelet",
                            "source_context": "Individual Case Series",
                        }
                    ]
                )

        return _Wrapper(schema, self)


def test_structured_pgx_extractor_returns_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.parsers.pgx_extractor._render_pdf_pages",
        lambda _pdf, zoom: [("img1", "txt1"), ("img2", "txt2"), ("img3", "txt3")],
    )
    fake_llm = _FakeLLM()
    extractor = StructuredPgxExtractor(
        prompt="extract pgx",
        model_name="gpt-test",
        llm=fake_llm,  # type: ignore[arg-type]
    )

    rows = extractor.extract_pdf(b"%PDF", pages_per_chunk=2)

    assert len(rows) == 2
    assert rows[0].sample_id == "Case-1"
    assert rows[0].actionability == "Yes"
    assert fake_llm.calls == 2


def test_structured_pgx_extractor_requires_model() -> None:
    with pytest.raises(ValueError):
        StructuredPgxExtractor(
            prompt="extract pgx",
            model_name=" ",
            llm=_FakeLLM(),  # type: ignore[arg-type]
        )


def test_structured_gemini_pgx_extractor_reuses_shared_logic(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.parsers.pgx_extractor._render_pdf_pages",
        lambda _pdf, zoom: [("img1", "txt1")],
    )
    extractor = StructuredGeminiPgxExtractor(
        prompt="extract pgx",
        model_name="gemini-test",
        llm=_FakeLLM(),  # type: ignore[arg-type]
    )

    rows = extractor.extract_pdf(b"%PDF", pages_per_chunk=1)

    assert len(rows) == 1
    assert rows[0].sample_id == "Case-1"


def test_render_pdf_pages_raises_for_invalid_pdf() -> None:
    with pytest.raises(ExtractionError):
        _render_pdf_pages(b"not-a-pdf", zoom=2.0)
