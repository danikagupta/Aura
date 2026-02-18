from __future__ import annotations

from dataclasses import replace
from typing import Dict, List, Optional

from src.common.dtos import (
    CitationRecord,
    PaperRecord,
    PaperState,
    ScoreResult,
    StorageRef,
)
from src.ingest.pdf_acquisition import PdfCandidate
from src.parsers.text_extractor import TextExtractionResult


class FakeRepository:
    def __init__(self, record: PaperRecord):
        self.records: Dict[str, PaperRecord] = {record.id: record}
        self.attempts: List[str] = []
        self.pgx_rows: Dict[str, List[Dict[str, object]]] = {}
        self.pgx_rows_by_table: Dict[str, Dict[str, List[Dict[str, object]]]] = {}

    def fetch_by_id(self, paper_id: str) -> PaperRecord:
        return self.records[paper_id]

    def fetch_by_state_at_level(
        self,
        state: PaperState,
        level: int,
        limit: int,
        seed_number: Optional[int] = None,
    ) -> List[PaperRecord]:
        rows = [
            record
            for record in self.records.values()
            if record.state == state and record.level == level
        ]
        return rows[:limit]

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        rows = [record for record in self.records.values() if record.state == state]
        return rows[:limit]

    def save_text_reference(self, paper_id: str, text_uri: str) -> PaperRecord:
        record = self.records[paper_id]
        updated = replace(
            record,
            state=PaperState.TEXT_AVAILABLE,
            text_ref=StorageRef(text_uri),
            attempts=0,
        )
        self.records[paper_id] = updated
        return updated

    def register_score(
        self,
        paper_id: str,
        score: float,
        reason: str,
        model_name: str,
        duration_ms: int,
        state: PaperState = PaperState.SCORED,
    ) -> PaperRecord:
        record = self.records[paper_id]
        updated = replace(
            record,
            state=state,
            score=score,
            reason=reason,
            attempts=0,
        )
        self.records[paper_id] = updated
        return updated

    def update_state(
        self,
        paper_id: str,
        state: PaperState,
        updates: Optional[Dict[str, object]] = None,
    ) -> PaperRecord:
        record = self.records[paper_id]
        pdf_ref = record.pdf_ref
        text_ref = record.text_ref
        metadata = record.metadata
        reason = record.reason
        pdf_md5 = record.pdf_md5
        if updates:
            if "source_uri" in updates:
                pdf_ref = StorageRef(str(updates["source_uri"]))
            if "text_uri" in updates:
                text_ref = StorageRef(str(updates["text_uri"]))
            if "metadata" in updates:
                metadata = updates["metadata"]  # type: ignore[assignment]
            if "reason" in updates:
                reason = str(updates["reason"])
            if "pdf_md5" in updates:
                pdf_md5 = str(updates["pdf_md5"]) if updates["pdf_md5"] is not None else None
        updated = replace(
            record,
            state=state,
            pdf_ref=pdf_ref,
            text_ref=text_ref,
            metadata=metadata,
            reason=reason,
            pdf_md5=pdf_md5,
            attempts=0,
        )
        self.records[paper_id] = updated
        return updated

    def create_pdf_placeholder(
        self,
        title: str,
        parent_id: str,
        level: int,
        seed_number: Optional[int] = None,
        metadata: Optional[Dict[str, object]] = None,
    ) -> PaperRecord:
        placeholder_id = f"{parent_id}-child-{len(self.records)}"
        inherited_seed = seed_number
        if inherited_seed is None:
            parent = self.records.get(parent_id)
            inherited_seed = parent.seed_number if parent else None
        record = PaperRecord(
            id=placeholder_id,
            title=title,
            state=PaperState.PDF_NOT_AVAILABLE,
            level=level,
            pdf_ref=None,
            parent_id=parent_id,
            seed_number=inherited_seed,
            metadata=metadata or {},
            attempts=0,
        )
        self.records[placeholder_id] = record
        return record

    def increment_attempts(
        self, paper_id: str, current_attempts: Optional[int] = None
    ) -> PaperRecord:
        record = self.records[paper_id]
        next_attempts = (current_attempts or record.attempts) + 1
        updated = replace(record, attempts=next_attempts)
        self.records[paper_id] = updated
        self.attempts.append(paper_id)
        return updated

    def insert_pgx_extractions(
        self,
        paper_id: str,
        rows: List[Dict[str, object]],
        table: str = "pgx_extractions",
    ) -> int:
        table_rows = self.pgx_rows_by_table.setdefault(table, {})
        existing_for_table = table_rows.setdefault(paper_id, [])
        existing_for_table.extend(rows)
        existing = self.pgx_rows.setdefault(paper_id, [])
        existing.extend(rows)
        return len(rows)


class FakeStorageGateway:
    def __init__(self):
        self._objects: Dict[str, bytes] = {}

    def store_pdf(self, paper_id: str, filename: str, data: bytes) -> StorageRef:
        uri = f"storage://pdfs/{paper_id}/{filename}"
        self._objects[uri] = data
        return StorageRef(uri)

    def store_text(self, paper_id: str, text: str) -> StorageRef:
        uri = f"storage://texts/{paper_id}/extracted.txt"
        self._objects[uri] = text.encode("utf-8")
        return StorageRef(uri)

    def fetch_blob(self, ref: StorageRef) -> bytes:
        return self._objects[ref.uri]


class FakeTextExtractor:
    def __init__(self, storage: FakeStorageGateway):
        self._storage = storage

    def extract(self, record: PaperRecord) -> TextExtractionResult:
        text = f"text for {record.id}"
        text_ref = self._storage.store_text(record.id, text)
        return TextExtractionResult(text_ref=text_ref, text=text)


class FakeScorer:
    def __init__(self, score: float = 9.0):
        self._score = score

    def score(self, text: str, *, seed_number: Optional[int] = None) -> ScoreResult:
        return ScoreResult(score=self._score, reason="relevant", model_name="fake", duration_ms=50)


class FakeCitationExtractor:
    def extract(self, text: str):
        return [CitationRecord(raw_text="Citation One")]


class FakePdfFetcher:
    def fetch(self, paper: PaperRecord) -> PdfCandidate:
        return PdfCandidate(
            filename="paper.pdf",
            content=b"pdf-bytes",
            source_url="https://example.com/paper.pdf",
        )
