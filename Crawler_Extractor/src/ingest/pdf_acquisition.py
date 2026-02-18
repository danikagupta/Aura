"""Locate and download PDFs for citations without content."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Protocol
from urllib.parse import urlparse

import requests

from ..common.dtos import PaperRecord

BLOCKED_DOWNLOAD_HOSTS = {"hdl.handle.net"}


@dataclass(slots=True)
class PdfCandidate:
    filename: str
    content: bytes
    source_url: str


class PdfFetcher(Protocol):
    def fetch(self, paper: PaperRecord) -> Optional[PdfCandidate]: ...


class GoogleSearchFetcher:
    """Fetch PDFs via Google Custom Search."""

    def __init__(
        self,
        api_key: str,
        engine_id: str,
        session: Optional[requests.Session] = None,
    ):
        self._api_key = api_key
        self._engine_id = engine_id
        self._session = session or requests.Session()

    def fetch(self, paper: PaperRecord) -> Optional[PdfCandidate]:
        if not self._api_key or not self._engine_id:
            return None
        query = _build_query(paper)
        links = self._search(query)
        for link in links:
            blob = self._download(link)
            if blob:
                filename = _sanitize_filename(paper.title or "paper") + ".pdf"
                return PdfCandidate(filename=filename, content=blob, source_url=link)
        return None

    def _search(self, query: str) -> Iterable[str]:
        response = self._session.get(
            "https://www.googleapis.com/customsearch/v1",
            params={
                "key": self._api_key,
                "cx": self._engine_id,
                "q": f"{query} filetype:pdf",
            },
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        return [item["link"] for item in payload.get("items", []) if item.get("link")]

    def _download(self, url: str) -> Optional[bytes]:
        if _is_blocked_download_host(url):
            return None
        response = self._session.get(url, timeout=20)
        if response.status_code != 200:
            return None
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            return None
        return response.content


def _build_query(paper: PaperRecord) -> str:
    citation = paper.metadata.get("citation", {})
    title = citation.get("title") or paper.title
    return title


def _sanitize_filename(title: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "-", title.strip().lower())
    return safe.strip("-") or "paper"


def _is_blocked_download_host(url: str) -> bool:
    hostname = (urlparse(url).hostname or "").lower()
    return hostname in BLOCKED_DOWNLOAD_HOSTS
