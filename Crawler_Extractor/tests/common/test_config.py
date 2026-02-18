from __future__ import annotations

import os

import pytest

from src.common import config
from src.common.errors import ConfigError


def test_load_config_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config, "_STREAMLIT_SECRETS_CACHE", {"PAPER_THRESHOLD": "0.8"}
    )
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "secret")
    monkeypatch.setenv("SUPABASE_PDF_BUCKET", "pdfs")
    monkeypatch.setenv("SUPABASE_TEXT_BUCKET", "texts")
    monkeypatch.setenv("SCORE_THRESHOLD", "0.8")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-test")

    cfg = config.load_config()

    assert cfg.supabase_url == "https://example.supabase.co"
    assert cfg.supabase_key == "secret"
    assert cfg.pdf_bucket == "pdfs"
    assert cfg.text_bucket == "texts"
    assert cfg.score_threshold == 0.8
    assert cfg.google_search_engine_id is None
    assert cfg.gemini_model == "gemini-2.5-flash"
    assert cfg.openai_api_key == "sk-test"
    assert cfg.openai_model == "gpt-test"


def test_load_config_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_STREAMLIT_SECRETS_CACHE", {})
    for key in list(os.environ):
        if key.startswith("SUPABASE") or key in {"SCORE_THRESHOLD", "OPENAI_API_KEY"}:
            monkeypatch.delenv(key, raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)

    with pytest.raises(ConfigError):
        config.load_config()


def test_load_config_uses_streamlit_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_STREAMLIT_SECRETS_CACHE", {"OPENAI_MODEL": "gpt-secret"})
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "secret")
    monkeypatch.setenv("SUPABASE_PDF_BUCKET", "pdfs")
    monkeypatch.setenv("SUPABASE_TEXT_BUCKET", "texts")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("SCORE_THRESHOLD", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)

    cfg = config.load_config()

    assert cfg.openai_model == "gpt-secret"


def test_load_config_uses_google_secret_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "_STREAMLIT_SECRETS_CACHE",
        {
            "OPENAI_MODEL": "gpt-secret",
            "GOOGLE_API_KEY": "api",
            "GOOGLE_CSE_ID": "cse",
            "GEMINI_MODEL": "gemini-secret",
        },
    )
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "secret")
    monkeypatch.setenv("SUPABASE_PDF_BUCKET", "pdfs")
    monkeypatch.setenv("SUPABASE_TEXT_BUCKET", "texts")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    for key in [
        "SCORE_THRESHOLD",
        "OPENAI_MODEL",
        "GOOGLE_SEARCH_API_KEY",
        "GOOGLE_SEARCH_ENGINE_ID",
    ]:
        monkeypatch.delenv(key, raising=False)

    cfg = config.load_config()

    assert cfg.openai_model == "gpt-secret"
    assert cfg.google_api_key == "api"
    assert cfg.google_search_engine_id == "cse"
    assert cfg.gemini_model == "gemini-secret"


def test_load_config_uses_paper_threshold_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "_STREAMLIT_SECRETS_CACHE",
        {"OPENAI_MODEL": "gpt-secret", "PAPER_THRESHOLD": "8.5"},
    )
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "secret")
    monkeypatch.setenv("SUPABASE_PDF_BUCKET", "pdfs")
    monkeypatch.setenv("SUPABASE_TEXT_BUCKET", "texts")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    for key in ["SCORE_THRESHOLD", "OPENAI_MODEL"]:
        monkeypatch.delenv(key, raising=False)

    cfg = config.load_config()

    assert cfg.score_threshold == 8.5
