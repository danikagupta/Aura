"""Helpers for creating Supabase clients."""

from __future__ import annotations

from supabase import Client, create_client
from yarl import URL

from .config import AppConfig


def build_supabase_client(config: AppConfig) -> Client:
    """Instantiate a Supabase client using the provided config."""

    client = create_client(config.supabase_url, config.supabase_key)
    storage_url = str(client.storage_url)
    if not storage_url.endswith("/"):
        client.storage_url = URL(f"{storage_url}/")
    return client
