from __future__ import annotations

from src.common.dtos import PaperRecord, PaperState
from src.ingest.pdf_acquisition import GoogleSearchFetcher, PdfCandidate, _sanitize_filename


class _DummyFetcher(GoogleSearchFetcher):
    def __init__(self) -> None:
        super().__init__(api_key="key", engine_id="engine")

    def _search(self, query: str):
        return ["http://example.com/file.pdf"]

    def _download(self, url: str):
        return b"pdf-binary"


def test_google_fetcher_returns_candidate(monkeypatch) -> None:
    fetcher = _DummyFetcher()
    paper = PaperRecord(
        id="1",
        title="Great Paper",
        state=PaperState.PDF_NOT_AVAILABLE,
        level=1,
        metadata={},
    )

    candidate = fetcher.fetch(paper)

    assert isinstance(candidate, PdfCandidate)
    assert candidate.filename.endswith(".pdf")


def test_sanitize_filename_handles_symbols() -> None:
    assert _sanitize_filename("A Study on DNA/ RNA!") == "a-study-on-dna-rna"


def test_download_skips_blocked_handle_host_without_request() -> None:
    class _Session:
        def get(self, *_args, **_kwargs):
            raise AssertionError("request should not be made for blocked hosts")

    fetcher = GoogleSearchFetcher(api_key="key", engine_id="engine", session=_Session())

    result = fetcher._download("https://hdl.handle.net/1234/5678")

    assert result is None


def test_download_skips_blocked_handle_host_http_scheme() -> None:
    class _Session:
        def get(self, *_args, **_kwargs):
            raise AssertionError("request should not be made for blocked hosts")

    fetcher = GoogleSearchFetcher(api_key="key", engine_id="engine", session=_Session())

    result = fetcher._download("http://hdl.handle.net/1234/5678")

    assert result is None
