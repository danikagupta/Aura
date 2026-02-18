"""Deterministic scoring helpers used during batch processing."""

from __future__ import annotations

from time import perf_counter
from typing import Any, Callable, Dict, Optional, Protocol

import tiktoken

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from ..common.dtos import ScoreResult
from ..common.prompt_config import SeedPromptConfig

_MAX_TOKEN_BUDGET = 30_000
_HEAD_TOKEN_BUDGET = 20_000
_TAIL_TOKEN_BUDGET = 10_000
_ELLIPSIS = "\n\n[...truncated for scoring...]\n\n"


class ScoringModel(Protocol):
    """Interface implemented by scoring engines."""

    def score(self, text: str, *, seed_number: Optional[int] = None) -> ScoreResult: ...


class _ScoreSchema(BaseModel):
    score: float = Field(..., ge=0, le=10, description="Relevance score 1-10")
    reason: str


class LangChainScorer:
    """LLM-backed scorer following the prompt rubric."""

    def __init__(
        self,
        *,
        prompt: str,
        api_key: Optional[str] = None,
        model_name: str,
        llm: Optional[Any] = None,
    ):
        if not prompt.strip():
            raise ValueError("Prompt text is required for LangChainScorer")
        if not model_name or not model_name.strip():
            raise ValueError("Model name is required for LangChainScorer")
        self._prompt = prompt
        self._model_name = model_name
        if llm is not None:
            self._llm = llm
        else:
            if not api_key:
                raise ValueError("OpenAI API key is required for LangChainScorer")
            self._llm = ChatOpenAI(
                api_key=api_key,
                model=model_name,
            )
        try:
            self._encoding = tiktoken.encoding_for_model(model_name)
        except Exception:  # pragma: no cover - depends on model set
            self._encoding = tiktoken.get_encoding("cl100k_base")

    def score(self, text: str, *, seed_number: Optional[int] = None) -> ScoreResult:
        if not text.strip():
            raise ValueError("Paper text must not be empty")
        prepared_text = self._truncate_text(text)
        start = perf_counter()
        structured = self._llm.with_structured_output(_ScoreSchema)
        response = structured.invoke(
            [
                HumanMessage(content=prepared_text),
                SystemMessage(content=self._prompt),
            ]
        )
        payload = response
        if isinstance(payload, dict):
            score_value = float(payload["score"])
            reason_value = str(payload["reason"])
        else:
            score_value = payload.score
            reason_value = payload.reason
        duration_ms = int((perf_counter() - start) * 1000)
        model_name = getattr(self._llm, "model_name", self._model_name)
        return ScoreResult(
            score=score_value,
            reason=reason_value,
            model_name=model_name,
            duration_ms=duration_ms,
        )

    def _truncate_text(self, text: str) -> str:
        tokens = self._encoding.encode(text)
        total_tokens = len(tokens)
        if total_tokens <= _MAX_TOKEN_BUDGET:
            return text
        char_count = len(text)
        if char_count == 0:
            return text
        chars_per_token = max(char_count / total_tokens, 1)
        head_chars = max(1, int(chars_per_token * _HEAD_TOKEN_BUDGET))
        tail_chars = max(1, int(chars_per_token * _TAIL_TOKEN_BUDGET))
        if head_chars + tail_chars >= char_count:
            return text
        tail_slice = text[-tail_chars:] if tail_chars > 0 else ""
        return f"{text[:head_chars]}{_ELLIPSIS}{tail_slice}"


class SeedPromptScorer:
    """Selects the scoring prompt based on the paper's seed number."""

    def __init__(
        self,
        *,
        api_key: Optional[str],
        model_name: str,
        prompt_config: Optional[SeedPromptConfig] = None,
        llm_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        if not model_name or not model_name.strip():
            raise ValueError("Model name is required for SeedPromptScorer")
        if llm_factory is None and (not api_key or not api_key.strip()):
            raise ValueError(
                "OpenAI API key is required for SeedPromptScorer when llm_factory is not provided"
            )
        self._api_key = api_key
        self._model_name = model_name
        self._prompt_config = prompt_config or SeedPromptConfig()
        self._llm_factory = llm_factory
        self._scorers: Dict[int, LangChainScorer] = {}

    def score(self, text: str, *, seed_number: Optional[int] = None) -> ScoreResult:
        if seed_number is None:
            raise ValueError("Seed number is required to select the scoring prompt")
        scorer = self._scorer_for_seed(seed_number)
        return scorer.score(text, seed_number=seed_number)

    def _scorer_for_seed(self, seed_number: int) -> LangChainScorer:
        cached = self._scorers.get(seed_number)
        if cached is not None:
            return cached
        prompt = self._prompt_config.prompt_text_for_seed(seed_number)
        llm = self._llm_factory() if self._llm_factory else None
        scorer = LangChainScorer(
            prompt=prompt,
            api_key=self._api_key if llm is None else None,
            model_name=self._model_name,
            llm=llm,
        )
        self._scorers[seed_number] = scorer
        return scorer
