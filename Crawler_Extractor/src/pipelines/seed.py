"""Seed upload workflow that the UI can trigger."""

from __future__ import annotations

import hashlib
from typing import Optional
from uuid import uuid4

from ..common.dtos import PaperRecord
from ..common.repository import PaperRepository
from ..ingest.storage_gateway import StorageGateway


def upload_seed_pdf(
    *,
    file_bytes: bytes,
    filename: str,
    title: str,
    level: int,
    seed_number: Optional[int],
    repository: PaperRepository,
    storage: StorageGateway,
    parent_id: Optional[str] = None,
) -> PaperRecord:
    """Persist a user-uploaded PDF and create the Supabase row."""

    paper_id = str(uuid4())
    pdf_hash = hashlib.md5(file_bytes).hexdigest()
    pdf_ref = storage.store_pdf(paper_id, filename, file_bytes)
    return repository.create_pdf_available(
        title=title,
        pdf_uri=pdf_ref.uri,
        level=level,
        parent_id=parent_id,
        seed_number=seed_number,
        paper_id=paper_id,
        pdf_md5=pdf_hash,
    )
