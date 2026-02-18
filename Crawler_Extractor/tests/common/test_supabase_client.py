from yarl import URL

from src.common.config import AppConfig
from src.common.supabase_client import build_supabase_client


class _DummyClient:
    def __init__(self) -> None:
        self.storage_url = URL("https://example.supabase.co/storage/v1")


def _config() -> AppConfig:
    return AppConfig(
        supabase_url="https://example.supabase.co",
        supabase_key="secret",
        pdf_bucket="pdfs",
        text_bucket="texts",
        google_api_key=None,
        google_search_engine_id=None,
        openai_api_key="sk-test",
        openai_model="gpt-4o",
    )


def test_build_supabase_client_appends_trailing_slash(monkeypatch):
    captured = {}

    def _fake_create_client(url: str, key: str) -> _DummyClient:
        captured["url"] = url
        captured["key"] = key
        return _DummyClient()

    monkeypatch.setattr(
        "src.common.supabase_client.create_client", _fake_create_client
    )

    client = build_supabase_client(_config())

    assert captured["url"] == "https://example.supabase.co"
    assert captured["key"] == "secret"
    assert str(client.storage_url).endswith("/")
