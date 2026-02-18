"""Configuration loading utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:  # pragma: no cover - Python >=3.11
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for older runtimes
    import tomli as tomllib  # type: ignore

from dotenv import load_dotenv

from .errors import ConfigError

DEFAULT_SCORE_THRESHOLD = 7
_STREAMLIT_SECRETS_CACHE: Optional[Dict[str, Any]] = None


@dataclass(slots=True)
class AppConfig:
    """Runtime configuration loaded from env variables."""

    supabase_url: str
    supabase_key: str
    pdf_bucket: str
    text_bucket: str
    google_api_key: Optional[str]
    google_search_engine_id: Optional[str]
    gemini_model: str
    openai_api_key: str
    openai_model: str
    score_threshold: float = DEFAULT_SCORE_THRESHOLD


def load_config() -> AppConfig:
    """Load and validate configuration from environment variables."""

    load_dotenv()

    def _require(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise ConfigError(f"Missing required environment variable {name}")
        return value

    threshold_value = _get_streamlit_secret("PAPER_THRESHOLD")
    score_threshold = (
        float(threshold_value)
        if threshold_value is not None
        else DEFAULT_SCORE_THRESHOLD
    )

    openai_model = _get_value_from_env_or_secrets("OPENAI_MODEL")
    if not openai_model:
        raise ConfigError(
            "Missing required OPENAI_MODEL environment variable or Streamlit secret"
        )

    google_api_key = _get_value_from_env_or_secrets(
        "GOOGLE_SEARCH_API_KEY", "GOOGLE_API_KEY"
    )
    google_search_engine_id = _get_value_from_env_or_secrets(
        "GOOGLE_SEARCH_ENGINE_ID", "GOOGLE_CSE_ID"
    )
    gemini_model = (
        _get_value_from_env_or_secrets("GEMINI_MODEL") or "gemini-2.5-flash"
    )

    return AppConfig(
        supabase_url=_require("SUPABASE_URL"),
        supabase_key=_require("SUPABASE_KEY"),
        pdf_bucket=_require("SUPABASE_PDF_BUCKET"),
        text_bucket=_require("SUPABASE_TEXT_BUCKET"),
        google_api_key=google_api_key,
        google_search_engine_id=google_search_engine_id,
        gemini_model=gemini_model,
        openai_api_key=_require("OPENAI_API_KEY"),
        openai_model=openai_model,
        score_threshold=score_threshold,
    )


def _get_streamlit_secret(name: str) -> Optional[str]:
    """Read a secret from `.streamlit/secrets.toml` if available."""

    secrets = _load_streamlit_secrets()
    value = secrets.get(name)
    if value is None:
        return None
    return str(value)


def _load_streamlit_secrets() -> Dict[str, Any]:
    """Load and cache Streamlit secrets to avoid repeated disk reads."""

    global _STREAMLIT_SECRETS_CACHE
    if _STREAMLIT_SECRETS_CACHE is not None:
        return _STREAMLIT_SECRETS_CACHE

    project_root = Path(__file__).resolve().parents[2]
    secrets_path = project_root / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        _STREAMLIT_SECRETS_CACHE = {}
        return _STREAMLIT_SECRETS_CACHE

    with secrets_path.open("rb") as handle:
        _STREAMLIT_SECRETS_CACHE = tomllib.load(handle)
    return _STREAMLIT_SECRETS_CACHE


def _get_value_from_env_or_secrets(*names: str) -> Optional[str]:
    """Check env vars first, then Streamlit secrets for any of the provided names."""

    for name in names:
        value = os.getenv(name)
        if value:
            return value
    for name in names:
        secret_value = _get_streamlit_secret(name)
        if secret_value:
            return secret_value
    return None
