from __future__ import annotations

import pytest

from src.common.errors import ConfigError
from src.common.prompt_config import SeedPromptConfig
from src.parsers.scoring_engine import LangChainScorer, SeedPromptScorer


class _FakeStructured:
    def __init__(self, schema):
        self._schema = schema

    def invoke(self, messages):
        assert len(messages) == 2
        return self._schema(score=8.0, reason="Highly relevant")


class _FakeLLM:
    model_name = "gpt-test"

    def with_structured_output(self, schema):
        return _FakeStructured(schema)


def test_langchain_scorer_returns_raw_score() -> None:
    scorer = LangChainScorer(
        prompt="Assess",
        model_name="gpt-test",
        llm=_FakeLLM(),  # type: ignore[arg-type]
    )

    result = scorer.score("Sample paper about pharmacogenomics")

    assert result.score == 8.0
    assert result.reason == "Highly relevant"
    assert result.model_name == "gpt-test"


def test_langchain_scorer_requires_prompt() -> None:
    with pytest.raises(ValueError):
        LangChainScorer(
            prompt="   ",
            model_name="gpt-test",
            llm=_FakeLLM(),  # type: ignore[arg-type]
        )


def test_langchain_scorer_requires_text() -> None:
    scorer = LangChainScorer(
        prompt="Assess",
        model_name="gpt-test",
        llm=_FakeLLM(),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError):
        scorer.score("   ")


def test_langchain_scorer_requires_model_name() -> None:
    with pytest.raises(ValueError):
        LangChainScorer(
            prompt="Assess",
            model_name="   ",
            llm=_FakeLLM(),  # type: ignore[arg-type]
        )


def test_seed_prompt_scorer_selects_prompt_from_config(tmp_path) -> None:
    prompt = tmp_path / "custom_prompt.txt"
    prompt.write_text("Assess", encoding="utf-8")
    config = SeedPromptConfig(table={1: prompt.name}, prompt_directory=tmp_path)
    scorer = SeedPromptScorer(
        api_key="test",
        model_name="gpt-test",
        prompt_config=config,
        llm_factory=lambda: _FakeLLM(),  # type: ignore[arg-type]
    )

    result = scorer.score("Seed-level paper", seed_number=1)

    assert result.score == 8.0
    assert result.reason == "Highly relevant"


def test_seed_prompt_scorer_errors_when_level_missing(tmp_path) -> None:
    prompt = tmp_path / "custom_prompt.txt"
    prompt.write_text("Assess", encoding="utf-8")
    config = SeedPromptConfig(table={}, prompt_directory=tmp_path)
    scorer = SeedPromptScorer(
        api_key="test",
        model_name="gpt-test",
        prompt_config=config,
        llm_factory=lambda: _FakeLLM(),  # type: ignore[arg-type]
    )

    with pytest.raises(ConfigError):
        scorer.score("Seed-level paper", seed_number=1)
