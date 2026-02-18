"""Citation extraction helpers."""

from __future__ import annotations

from typing import Any, List, Optional, Protocol

import tiktoken
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from ..common.dtos import CitationRecord

_MAX_TOKEN_BUDGET = 30_000
_HEAD_TOKEN_BUDGET = 20_000
_TAIL_TOKEN_BUDGET = 10_000
_ELLIPSIS = "\n\n[...truncated for citation extraction...]\n\n"


class CitationExtractor(Protocol):
    """Protocol implemented by citation extractors."""

    def extract(self, text: str) -> List[CitationRecord]: ...


class HeuristicCitationExtractor:
    """Extract citation-like lines from paper text by regex heuristics."""

    def extract(self, text: str) -> List[CitationRecord]:
        citations: List[CitationRecord] = []
        for line in text.splitlines():
            normalized = line.strip()
            if not normalized or not normalized[0].isdigit():
                continue
            tail = normalized.split(".", 1)
            title = tail[1].strip() if len(tail) > 1 else normalized
            if len(title.split()) < 2:
                continue
            citations.append(
                CitationRecord(raw_text=title)
            )
        return citations


class _CitationSchema(BaseModel):
    citation: str = Field(..., description="Full citation string as it appears")


class _CitationExtraction(BaseModel):
    citations: List[_CitationSchema]


class StructuredCitationExtractor:
    """LLM-based extractor that emits structured citations via LangChain."""

    def __init__(
        self,
        *,
        prompt: str,
        api_key: Optional[str] = None,
        model_name: str,
        llm: Optional[Any] = None,
    ):
        if not prompt.strip():
            raise ValueError("Citation prompt text is required")
        if not model_name or not model_name.strip():
            raise ValueError("Model name is required for citation extraction")
        self._prompt = prompt
        if llm is not None:
            self._llm = llm
        else:
            if not api_key:
                raise ValueError("OpenAI API key is required for citation extraction")
            from langchain_openai import ChatOpenAI

            self._llm = ChatOpenAI(api_key=api_key, model=model_name)
        try:
            self._encoding = tiktoken.encoding_for_model(model_name)
        except Exception:  # pragma: no cover - depends on runtime model choice
            self._encoding = tiktoken.get_encoding("cl100k_base")

    def extract(self, text: str) -> List[CitationRecord]:
        if not text.strip():
            return []
        prepared_text = self._truncate_text(text)
        structured = self._llm.with_structured_output(_CitationExtraction)
        response = structured.invoke(
            [
                HumanMessage(content=prepared_text),
                SystemMessage(content=self._prompt),
            ]
        )
        citations: List[CitationRecord] = []
        for item in response.citations:
            citations.append(CitationRecord(raw_text=item.citation.strip()))
        return citations

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
