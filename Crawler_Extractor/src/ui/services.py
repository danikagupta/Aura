"""Shared helpers for initializing Streamlit services."""

from __future__ import annotations

from pathlib import Path

from src.common.config import load_config
from src.common.prompt_config import SeedPromptConfig
from src.common.repository import PaperRepository
from src.common.supabase_client import build_supabase_client
from src.ingest.pdf_acquisition import GoogleSearchFetcher
from src.ingest.storage_gateway import StorageGateway
from src.parsers.citation_extractor import StructuredCitationExtractor
from src.parsers.scoring_engine import SeedPromptScorer
from src.parsers.text_extractor import TextExtractor
from src.pipelines.services import PipelineServices

_CITATION_PROMPT_FALLBACK = "Extract up to 10 citations mentioned in the paper."


def init_services() -> PipelineServices:
    """Build the dependency graph shared across Streamlit pages."""

    config = load_config()
    client = build_supabase_client(config)
    repository = PaperRepository(client)
    storage = StorageGateway(client, config.pdf_bucket, config.text_bucket)
    prompt_config = SeedPromptConfig()
    citation_prompt = _load_prompt(
        Path("prompts/citation_extraction_prompt.txt"),
        _CITATION_PROMPT_FALLBACK,
    )
    scorer = SeedPromptScorer(
        prompt_config=prompt_config,
        api_key=config.openai_api_key,
        model_name=config.openai_model,
    )
    citation_extractor = StructuredCitationExtractor(
        prompt=citation_prompt,
        api_key=config.openai_api_key,
        model_name=config.openai_model,
    )
    pdf_fetcher = GoogleSearchFetcher(
        api_key=config.google_api_key or "",
        engine_id=config.google_search_engine_id or "",
    )
    return PipelineServices(
        repository=repository,
        storage=storage,
        text_extractor=TextExtractor(storage),
        scorer=scorer,
        citation_extractor=citation_extractor,
        pdf_fetcher=pdf_fetcher,
        score_threshold=config.score_threshold,
    )


def _load_prompt(path: Path, fallback: str) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else fallback
