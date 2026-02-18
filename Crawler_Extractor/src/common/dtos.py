"""Shared DTOs used across crawler modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

MAX_PROCESSING_ATTEMPTS = 2


class PaperState(str, Enum):
    """Lifecycle states persisted in Supabase."""

    PDF_AVAILABLE = "PDF Available"
    TEXT_AVAILABLE = "Text Available"
    SCORED = "Scored"
    PROCESSED = "Processed"
    PROCESSED_LOW_SCORE = "Processed (Low Score)"
    PDF_NOT_AVAILABLE = "PDF Not Available"
    FAILED = "Failed"
    P1 = "P1"
    P1_WIP = "P1WIP"
    P1_SUCCESS = "P1Success"
    P1_FAILURE = "P1Failure"


@dataclass(slots=True)
class StorageRef:
    """Pointer to an object stored in Supabase Storage."""

    uri: str

    def bucket(self) -> Optional[str]:
        segments = self.uri.split("/", 3)
        return segments[2] if len(segments) > 2 else None

    def path(self) -> Optional[str]:
        segments = self.uri.split("/", 3)
        return segments[3] if len(segments) > 3 else None


@dataclass(slots=True)
class PaperRecord:
    """Represents one paper row in Supabase."""

    id: str
    title: str
    state: PaperState
    level: int = 1
    attempts: int = 0
    pdf_ref: Optional[StorageRef] = None
    text_ref: Optional[StorageRef] = None
    score: Optional[float] = None
    reason: Optional[str] = None
    pdf_md5: Optional[str] = None
    parent_id: Optional[str] = None
    seed_number: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass(slots=True)
class CitationRecord:
    """Citation captured from a paper."""

    raw_text: str


@dataclass(slots=True)
class ScoreResult:
    """Structured response from the scoring stage."""

    score: float
    reason: str
    model_name: str
    duration_ms: int


@dataclass(slots=True)
class StageOutcome:
    """Result emitted by a pipeline stage."""

    paper_id: str
    stage: str
    success: bool
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)
