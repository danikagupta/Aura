from __future__ import annotations

import pytest

import src.parsers.citation_extractor as citation_module
from src.parsers.citation_extractor import (
    HeuristicCitationExtractor,
    StructuredCitationExtractor,
)


def test_citation_extractor_parses_numbered_lines() -> None:
    extractor = HeuristicCitationExtractor()
    text = """
1. Title of first citation by A. Researcher
Some other text
2. Second citation entry with details
"""

    citations = extractor.extract(text)

    assert [c.raw_text for c in citations] == [
        "Title of first citation by A. Researcher",
        "Second citation entry with details",
    ]


def test_citation_extractor_stops_at_limit() -> None:
    extractor = HeuristicCitationExtractor()
    text = "1. First entry\n2. Second entry"

    citations = extractor.extract(text)

    assert len(citations) == 2


class _FakeLLM:
    def with_structured_output(self, schema):
        class _Wrapper:
            def __init__(self, schema):
                self._schema = schema

            def invoke(self, messages):
                return self._schema(
                    citations=[
                        {
                            "citation": "Citation One, Author et al., 2023.",
                        }
                    ]
                )

        return _Wrapper(schema)


class _FakeEncoding:
    def __init__(self, token_count: int):
        self._token_count = token_count

    def encode(self, _text: str):
        return [0] * self._token_count


def test_structured_citation_extractor_returns_records() -> None:
    extractor = StructuredCitationExtractor(
        prompt="Extract citations",
        model_name="gpt-test",
        llm=_FakeLLM(),  # type: ignore[arg-type]
    )

    citations = extractor.extract("Some scientific paper text.")

    assert len(citations) == 1
    assert citations[0].raw_text == "Citation One, Author et al., 2023."


def test_structured_citation_extractor_requires_model() -> None:
    with pytest.raises(ValueError):
        StructuredCitationExtractor(
            prompt="Extract citations",
            model_name=" ",
            llm=_FakeLLM(),  # type: ignore[arg-type]
        )


def test_structured_citation_extractor_truncates_long_text() -> None:
    extractor = StructuredCitationExtractor(
        prompt="Extract citations",
        model_name="gpt-test",
        llm=_FakeLLM(),  # type: ignore[arg-type]
    )
    extractor._encoding = _FakeEncoding(citation_module._MAX_TOKEN_BUDGET + 1)  # type: ignore[attr-defined]
    text = "A" * (citation_module._HEAD_TOKEN_BUDGET + citation_module._TAIL_TOKEN_BUDGET + 10)

    truncated = extractor._truncate_text(text)

    assert citation_module._ELLIPSIS in truncated
    assert len(truncated) < len(text) + len(citation_module._ELLIPSIS)
