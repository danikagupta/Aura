from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

from src.common.dtos import PaperRecord, PaperState, StorageRef
from src.pipelines.seed import upload_seed_pdf


@dataclass
class _FakeStorage:
    bucket: str = "pdfs"
    last_paper_id: Optional[str] = None
    last_filename: Optional[str] = None

    def store_pdf(self, paper_id: str, filename: str, data: bytes) -> StorageRef:
        self.last_paper_id = paper_id
        self.last_filename = filename
        return StorageRef(f"storage://{self.bucket}/{paper_id}/{filename}")


class _FakeRepository:
    def __init__(self) -> None:
        self.created: Optional[PaperRecord] = None

    def create_pdf_available(
        self,
        *,
        title: str,
        pdf_uri: str,
        level: int,
        parent_id: Optional[str],
        paper_id: Optional[str],
        seed_number: Optional[int],
        metadata: Optional[dict] = None,
        pdf_md5: Optional[str] = None,
    ) -> PaperRecord:
        self.created = PaperRecord(
            id=paper_id or "generated",
            title=title,
            state=PaperState.PDF_AVAILABLE,
            level=level,
            pdf_ref=StorageRef(pdf_uri),
            parent_id=parent_id,
            seed_number=seed_number,
            metadata=metadata or {},
            pdf_md5=pdf_md5,
        )
        return self.created


def test_upload_seed_pdf_persists_file_and_record() -> None:
    storage = _FakeStorage()
    repository = _FakeRepository()

    record = upload_seed_pdf(
        file_bytes=b"fake",
        filename="seed.pdf",
        title="Seed Paper",
        level=1,
        seed_number=42,
        repository=repository,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
    )

    assert repository.created is not None
    assert storage.last_paper_id == repository.created.id
    assert record.pdf_ref is not None
    assert record.pdf_ref.uri.endswith("seed.pdf")
    assert repository.created.title == "Seed Paper"
    assert repository.created.seed_number == 42
    assert repository.created.pdf_md5 == hashlib.md5(b"fake").hexdigest()
