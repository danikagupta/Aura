from __future__ import annotations

import pytest

from src.common.dtos import PaperRecord, PaperState, StorageRef
from src.common.errors import ExtractionError
from src.parsers.text_extractor import TextExtractor


class _FakeStorage:
    def __init__(self) -> None:
        self.saved_text: str | None = None

    def fetch_blob(self, ref: StorageRef) -> bytes:
        return b"ignored-pdf-bytes"

    def store_text(self, paper_id: str, text: str) -> StorageRef:
        self.saved_text = text
        return StorageRef(f"storage://texts/{paper_id}/extracted.txt")


def test_text_extractor_persists_text(monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _FakeStorage()
    extractor = TextExtractor(storage=storage)  # type: ignore[arg-type]
    monkeypatch.setattr(
        TextExtractor, "_read_pdf", staticmethod(lambda _: "hello world")
    )
    record = PaperRecord(
        id="paper-1",
        title="Title",
        state=PaperState.PDF_AVAILABLE,
        level=1,
        pdf_ref=StorageRef("storage://pdfs/paper-1/seed.pdf"),
    )

    result = extractor.extract(record)

    assert storage.saved_text == "hello world"
    assert result.text == "hello world"
    assert result.text_ref.uri.endswith("extracted.txt")


def test_text_extractor_requires_pdf_reference() -> None:
    extractor = TextExtractor(storage=_FakeStorage())  # type: ignore[arg-type]
    record = PaperRecord(
        id="paper-2",
        title="Missing PDF",
        state=PaperState.PDF_AVAILABLE,
        level=1,
    )

    with pytest.raises(ExtractionError):
        extractor.extract(record)
