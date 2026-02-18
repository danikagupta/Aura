"""Single-step processing helpers used across pipeline runners."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from typing import Iterable, List, Literal
from urllib.parse import urlparse

from ..common.dtos import (
    MAX_PROCESSING_ATTEMPTS,
    CitationRecord,
    PaperRecord,
    PaperState,
    StageOutcome,
    StorageRef,
)
from ..common.errors import (
    DuplicateTitleError,
    DuplicateTopicError,
    ExtractionError,
    RepositoryError,
    StorageError,
)
from ..common.repository import PaperRepository
from ..ingest.pdf_acquisition import PdfFetcher
from ..ingest.storage_gateway import StorageGateway
from ..parsers.citation_extractor import CitationExtractor
from ..parsers.pgx_extractor import (
    PgxExtractor,
    pdf_page_count,
)
from ..parsers.scoring_engine import ScoringModel
from ..parsers.text_extractor import LinkAnnotation, TextExtractor

StageName = Literal[
    "pdf_acquisition",
    "text_extraction",
    "scoring",
    "citations",
    "pgx_extraction",
]
BLOCKED_SOURCE_HOSTS = {"hdl.handle.net"}


def fetch_pdf(
    paper: PaperRecord,
    repository: PaperRepository,
    storage: StorageGateway,
    fetcher: PdfFetcher,
) -> StageOutcome:
    """Download a PDF candidate and persist it to storage."""

    blocked_url = _blocked_source_url(paper)
    if blocked_url:
        repository.update_state(
            paper.id,
            PaperState.FAILED,
            updates={"reason": f"blocked source host: {blocked_url}"},
        )
        return StageOutcome(
            paper_id=paper.id,
            stage="pdf_acquisition",
            success=False,
            message="blocked source host",
            metadata={"source_url": blocked_url},
        )

    if (paper.attempts or 0) > MAX_PROCESSING_ATTEMPTS:
        repository.update_state(
            paper.id,
            PaperState.FAILED,
            updates={"reason": "exceeded PDF acquisition attempts"},
        )
        return StageOutcome(
            paper_id=paper.id,
            stage="pdf_acquisition",
            success=False,
            message="exceeded attempt limit",
        )

    try:
        candidate = fetcher.fetch(paper)
    except Exception as exc:  # pragma: no cover - surfaced in UI/logs
        return StageOutcome(
            paper_id=paper.id,
            stage="pdf_acquisition",
            success=False,
            message="fetch error",
            metadata={"error": str(exc)},
        )
    if not candidate:
        return StageOutcome(
            paper_id=paper.id,
            stage="pdf_acquisition",
            success=False,
            message="pdf not found",
            metadata={"reason": "No PDF found"},
        )
    pdf_hash = hashlib.md5(candidate.content).hexdigest()
    try:
        pdf_ref = storage.store_pdf(paper.id, candidate.filename, candidate.content)
    except StorageError as exc:
        return StageOutcome(
            paper_id=paper.id,
            stage="pdf_acquisition",
            success=False,
            message="storage error",
            metadata={"error": str(exc)},
        )
    metadata = dict(paper.metadata)
    metadata["acquisition"] = {"source_url": candidate.source_url}
    repository.update_state(
        paper.id,
        PaperState.PDF_AVAILABLE,
        updates={"source_uri": pdf_ref.uri, "metadata": metadata, "pdf_md5": pdf_hash},
    )
    return StageOutcome(
        paper_id=paper.id,
        stage="pdf_acquisition",
        success=True,
        message="pdf stored",
        metadata={"source_url": candidate.source_url, "pdf_md5": pdf_hash},
    )


def extract_text(
    paper: PaperRecord,
    repository: PaperRepository,
    extractor: TextExtractor,
    store_links: bool = True,
) -> StageOutcome:
    """Convert a PDF to text and persist references."""

    try:
        result = extractor.extract(paper)
    except ExtractionError as exc:
        repository.update_state(
            paper.id, PaperState.FAILED, updates={"reason": str(exc)}
        )
        return StageOutcome(
            paper_id=paper.id,
            stage="text_extraction",
            success=False,
            message=str(exc),
        )
    repository.save_text_reference(paper.id, result.text_ref.uri)
    if store_links and result.links:
        _insert_link_placeholders(repository, paper, result.links)
    return StageOutcome(
        paper_id=paper.id,
        stage="text_extraction",
        success=True,
        message="extracted",
        metadata={"characters": len(result.text)},
    )


def score_text(
    paper: PaperRecord,
    repository: PaperRepository,
    storage: StorageGateway,
    scorer: ScoringModel,
    threshold: float,
) -> StageOutcome:
    """Score extracted text and persist outcomes."""

    if not paper.text_ref:
        repository.update_state(
            paper.id,
            PaperState.FAILED,
            updates={"reason": "Paper missing text reference"},
        )
        return StageOutcome(
            paper_id=paper.id,
            stage="scoring",
            success=False,
            message="paper missing text",
    )
    text = storage.fetch_blob(paper.text_ref).decode("utf-8")
    result = scorer.score(text, seed_number=paper.seed_number)
    eligible = result.score >= threshold
    next_state = (
        PaperState.SCORED if eligible else PaperState.PROCESSED_LOW_SCORE
    )
    repository.register_score(
        paper.id,
        result.score,
        result.reason,
        result.model_name,
        result.duration_ms,
        state=next_state,
    )
    return StageOutcome(
        paper_id=paper.id,
        stage="scoring",
        success=True,
        message="scored",
        metadata={
            "score": result.score,
            "eligible_for_citations": eligible,
        },
    )


def extract_citations(
    paper: PaperRecord,
    repository: PaperRepository,
    storage: StorageGateway,
    extractor: CitationExtractor,
    threshold: float,
) -> StageOutcome:
    """Extract citations from scored papers and create placeholders."""

    if paper.score is None or paper.score < threshold:
        return StageOutcome(
            paper_id=paper.id,
            stage="citations",
            success=False,
            message="paper below threshold",
        )
    if not paper.text_ref:
        return StageOutcome(
            paper_id=paper.id,
            stage="citations",
            success=False,
            message="paper missing text",
        )
    text = storage.fetch_blob(paper.text_ref).decode("utf-8")
    citations = extractor.extract(text)
    inserted = _insert_citations(repository, paper, citations)
    repository.update_state(
        paper.id,
        PaperState.PROCESSED,
        updates={"metadata": {**paper.metadata, "citation_count": inserted}},
    )
    return StageOutcome(
        paper_id=paper.id,
        stage="citations",
        success=True,
        message="citations created",
        metadata={"count": inserted},
    )


def process_pgx_extraction(
    paper: PaperRecord,
    repository: PaperRepository,
    storage: StorageGateway,
    extractor: PgxExtractor,
    *,
    pdf_bytes: bytes | None = None,
    page_count: int | None = None,
    extraction_table: str = "pgx_extractions",
) -> StageOutcome:
    """Extract PGX sample rows from a paper and persist them to pgx_extractions."""

    source_ref = _pgx_source_ref(paper)
    if source_ref is None:
        repository.update_state(
            paper.id,
            PaperState.P1_FAILURE,
            updates={"reason": "Paper missing source_uri"},
        )
        return StageOutcome(
            paper_id=paper.id,
            stage="pgx_extraction",
            success=False,
            message="paper missing source_uri",
        )

    try:
        current_pdf_bytes = pdf_bytes
        if current_pdf_bytes is None:
            current_pdf_bytes = storage.fetch_blob(source_ref)
        if page_count is None:
            page_count = pdf_page_count(current_pdf_bytes)
        samples = extractor.extract_pdf(current_pdf_bytes)
        if not samples:
            repository.update_state(
                paper.id,
                PaperState.P1_FAILURE,
                updates={"reason": "No PGX samples extracted"},
            )
            return StageOutcome(
                paper_id=paper.id,
                stage="pgx_extraction",
                success=False,
                message="no pgx samples extracted",
                metadata={
                    "page_count": page_count,
                    "hint": "No structured rows were returned from page-chunk extraction. Verify the model supports image input and the PDF contains readable tables/figures/text."
                },
            )
        inserted = repository.insert_pgx_extractions(
            paper.id,
            [
                {
                    "sample_id": row.sample_id,
                    "gene": row.gene,
                    "allele": row.allele,
                    "rs_id": row.rs_id,
                    "medication": row.medication,
                    "outcome": row.outcome,
                    "actionability": row.actionability,
                    "cpic_recommendation": row.cpic_recommendation,
                    "source_context": row.source_context,
                }
                for row in samples
            ],
            table=extraction_table,
        )
    except Exception as exc:
        repository.update_state(
            paper.id,
            PaperState.P1_FAILURE,
            updates={"reason": str(exc)},
        )
        return StageOutcome(
            paper_id=paper.id,
            stage="pgx_extraction",
            success=False,
            message="pgx extraction failed",
            metadata={"error": str(exc), "page_count": page_count},
        )

    repository.update_state(
        paper.id,
        PaperState.P1_SUCCESS,
        updates={
            "reason": None,
            "metadata": {**paper.metadata, "pgx_extraction_count": inserted},
        },
    )
    return StageOutcome(
        paper_id=paper.id,
        stage="pgx_extraction",
        success=True,
        message="pgx extraction completed",
        metadata={"count": inserted, "page_count": page_count},
    )


def _insert_link_placeholders(
    repository: PaperRepository,
    paper: PaperRecord,
    links: Iterable[LinkAnnotation],
) -> None:
    level = paper.level + 1
    for link in links:
        title = f"url:{link.url}"[:255]
        metadata = {"link": {"url": link.url, "text": link.text, "page": link.page}}
        try:
            repository.create_pdf_placeholder(
                title=title,
                parent_id=paper.id,
                level=level,
                seed_number=paper.seed_number,
                metadata=metadata,
            )
        except (DuplicateTitleError, DuplicateTopicError):
            continue
        except RepositoryError as exc:
            message = str(exc).lower()
            if (
                "server disconnected" in message
                or "papers_pkey" in message
                or "duplicate key value violates unique constraint \"papers_pkey\"" in message
            ):
                continue
            raise


def _insert_citations(
    repository: PaperRepository,
    paper: PaperRecord,
    citations: List[CitationRecord],
) -> int:
    level = paper.level + 1
    inserted = 0
    seen_titles: set[str] = set()
    for citation in citations:
        raw_title = (citation.raw_text or "").strip()
        title = raw_title[:255] or "Citation"
        normalized = title.casefold()
        if normalized in seen_titles:
            continue
        seen_titles.add(normalized)
        try:
            repository.create_pdf_placeholder(
                title=title,
                parent_id=paper.id,
                level=level,
                seed_number=paper.seed_number,
                metadata={"citation": asdict(citation)},
            )
            inserted += 1
        except (DuplicateTitleError, DuplicateTopicError):
            continue
        except RepositoryError as exc:
            message = str(exc).lower()
            if "papers_pkey" in message or "server disconnected" in message:
                continue
            raise
    return inserted


def _blocked_source_url(paper: PaperRecord) -> str | None:
    candidates: List[str] = []
    title = (paper.title or "").strip()
    if title.lower().startswith("url:"):
        candidates.append(title[4:].strip())
    link_url = str((paper.metadata.get("link") or {}).get("url") or "").strip()
    if link_url:
        candidates.append(link_url)
    for candidate in candidates:
        if _is_blocked_host(candidate):
            return candidate
    return None


def _is_blocked_host(url: str) -> bool:
    hostname = (urlparse(url).hostname or "").lower()
    return hostname in BLOCKED_SOURCE_HOSTS


def _pgx_source_ref(paper: PaperRecord) -> StorageRef | None:
    if not paper.pdf_ref:
        return None
    return paper.pdf_ref
