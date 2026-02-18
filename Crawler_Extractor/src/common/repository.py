"""Supabase-backed repository for paper metadata."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

try:  # pragma: no cover - Supabase dependency available at runtime
    from postgrest.exceptions import APIError as PostgrestAPIError
except Exception:  # pragma: no cover - fallback during tests
    class PostgrestAPIError(Exception):
        """Fallback API error used when postgrest is not installed."""

        code: Optional[str] = None
        details: Optional[str] = None
        hint: Optional[str] = None

try:  # pragma: no cover - httpx available at runtime
    from httpx import (
        ConnectError,
        ReadError,
        ReadTimeout,
        RemoteProtocolError,
        WriteError,
        WriteTimeout,
    )

    _TRANSIENT_HTTP_ERRORS = (
        ConnectError,
        ReadError,
        ReadTimeout,
        RemoteProtocolError,
        WriteError,
        WriteTimeout,
    )
except Exception:  # pragma: no cover - fallback during tests
    _TRANSIENT_HTTP_ERRORS = tuple()


from .dtos import PaperRecord, PaperState, StorageRef
from .errors import DuplicateTitleError, DuplicateTopicError, RepositoryError

MAX_TRANSIENT_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 0.2


class PaperRepository:
    """High-level CRUD helper that wraps the Supabase client."""

    def __init__(
        self,
        client: Any,
        table: str = "papers",
        *,
        state_column: str = "status",
    ):
        self._client = client
        self._table = table
        self._state_column = state_column

    def create_pdf_available(
        self,
        title: str,
        pdf_uri: str,
        level: int,
        parent_id: Optional[str] = None,
        seed_number: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        paper_id: Optional[str] = None,
        pdf_md5: Optional[str] = None,
    ) -> PaperRecord:
        paper_id = paper_id or str(uuid4())
        payload = {
            "id": paper_id,
            "title": title,
            "source_uri": pdf_uri,
            "pdf_md5": pdf_md5,
            self._state_column: PaperState.PDF_AVAILABLE.value,
            "level": level,
            "attempts": 0,
            "parent_id": parent_id,
            "seed_number": seed_number,
            "metadata": metadata or {},
        }
        response = self._execute(self._client.table(self._table).insert(payload))
        return self._to_record(self._single(response.data))

    def create_pdf_placeholder(
        self,
        title: str,
        parent_id: str,
        level: int,
        seed_number: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> PaperRecord:
        paper_id = str(uuid4())
        payload = {
            "id": paper_id,
            "title": title,
            "source_uri": f"pending://{paper_id}",
            "pdf_md5": None,
            self._state_column: PaperState.PDF_NOT_AVAILABLE.value,
            "level": level,
            "attempts": 0,
            "parent_id": parent_id,
            "seed_number": seed_number,
            "metadata": metadata or {},
        }
        response = self._execute(self._client.table(self._table).insert(payload))
        return self._to_record(self._single(response.data))

    def fetch_by_state(self, state: PaperState, limit: int) -> List[PaperRecord]:
        response = self._execute(
            self._client.table(self._table)
            .select("*")
            .eq(self._state_column, state.value)
            .order("level", desc=False)
            .order("created_at", desc=False)
            .limit(limit)
        )
        return [self._to_record(row) for row in response.data]

    def insert_pgx_extractions(
        self,
        paper_id: str,
        rows: List[Dict[str, Any]],
        table: str = "pgx_extractions",
    ) -> int:
        if not rows:
            return 0
        payload = [{"paper_id": paper_id, **row} for row in rows]
        self._execute(self._client.table(table).insert(payload))
        return len(payload)

    def fetch_by_state_at_level(
        self,
        state: PaperState,
        level: int,
        limit: int,
        seed_number: Optional[int] = None,
    ) -> List[PaperRecord]:
        query = (
            self._client.table(self._table)
            .select("*")
            .eq(self._state_column, state.value)
            .eq("level", level)
            .order("created_at", desc=False)
        )
        if seed_number is not None:
            query = query.eq("seed_number", seed_number)
        response = self._execute(query.limit(limit))
        return [self._to_record(row) for row in response.data]

    def fetch_pdf_hash_candidates(
        self,
        limit: int,
        *,
        seed_number: Optional[int] = None,
        missing_only: bool = True,
    ) -> List[PaperRecord]:
        query = (
            self._client.table(self._table)
            .select("*")
            .in_("status", list(HASHABLE_STATES))
            .order("updated_at", desc=False)
        )
        if seed_number is not None:
            query = query.eq("seed_number", seed_number)
        if missing_only:
            query = query.is_("pdf_md5", "null")
        else:
            query = query.not_.is_("pdf_md5", "null")
        response = self._execute(query.limit(limit))
        return [self._to_record(row) for row in response.data]

    def fetch_all_scored(self) -> List[PaperRecord]:
        rows: List[Dict[str, Any]] = []
        page_size = 1000
        offset = 0
        while True:
            response = self._execute(
                self._client.table(self._table)
                .select("*")
                .not_.is_("score", "null")
                .order("level", desc=False)
                .order("created_at", desc=False)
                .range(offset, offset + page_size - 1)
            )
            batch = response.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return [self._to_record(row) for row in rows]

    def fetch_rows_by_seed_number(self, seed_number: int) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        page_size = 1000
        offset = 0
        while True:
            response = self._execute(
                self._client.table(self._table)
                .select("*")
                .eq("seed_number", seed_number)
                .order("created_at", desc=False)
                .range(offset, offset + page_size - 1)
            )
            batch = response.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return rows

    def fetch_by_statuses(
        self,
        statuses: List[str],
        *,
        limit: int,
    ) -> List[PaperRecord]:
        if not statuses:
            return []
        response = self._execute(
            self._client.table(self._table)
            .select("*")
            .in_(self._state_column, statuses)
            .order("created_at", desc=False)
            .limit(limit)
        )
        return [self._to_record(row) for row in response.data]

    def fetch_all_rows_from_table(
        self,
        table: str,
        *,
        order_by: str = "created_at",
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        page_size = 1000
        offset = 0
        while True:
            response = self._execute(
                self._client.table(table)
                .select("*")
                .order(order_by, desc=False)
                .range(offset, offset + page_size - 1)
            )
            batch = response.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return rows

    def level_status_rows(
        self, seed_number: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._execute_rpc(
            "stats_level_status",
            {"seed_filter": seed_number} if seed_number is not None else None,
        )

    def level_status_counts(
        self, seed_number: Optional[int] = None
    ) -> Dict[str, Dict[int, int]]:
        rows = self.level_status_rows(seed_number)
        counts: Dict[str, Dict[int, int]] = {}
        for row in rows:
            status = row.get("status")
            level = row.get("level")
            row_count = int(row.get("total") or 0)
            if status is None or level is None:
                continue
            level_counts = counts.setdefault(status, {})
            level_counts[int(level)] = level_counts.get(int(level), 0) + row_count
        return counts

    def fetch_by_pdf_md5(self, md5_hash: str) -> List[PaperRecord]:
        response = (
            self._client.table(self._table)
            .select("*")
            .eq("pdf_md5", md5_hash)
            .order("updated_at", desc=False)
            .execute()
        )
        return [self._to_record(row) for row in response.data]

    def seed_status_rows(self) -> List[Dict[str, Any]]:
        return self._execute_rpc("stats_seed_status")

    def seed_status_counts(self) -> Dict[Optional[int], Dict[str, int]]:
        rows = self.seed_status_rows()
        counts: Dict[Optional[int], Dict[str, int]] = {}
        for row in rows:
            seed_value = row.get("seed_number")
            status = row.get("status")
            row_count = int(row.get("total") or 0)
            if status is None:
                continue
            seed_counts = counts.setdefault(seed_value, {})
            seed_counts[status] = seed_counts.get(status, 0) + row_count
        return counts

    def level_score_rows(
        self, seed_number: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._execute_rpc(
            "stats_level_score",
            {"seed_filter": seed_number} if seed_number is not None else None,
        )

    def level_score_counts(
        self, seed_number: Optional[int] = None
    ) -> Dict[int, Dict[float, int]]:
        rows = self.level_score_rows(seed_number)
        counts: Dict[int, Dict[float, int]] = {}
        for row in rows:
            level = row.get("level")
            score = row.get("score")
            row_count = int(row.get("total") or 0)
            if level is None or score is None:
                continue
            try:
                score_value = float(score)
            except (TypeError, ValueError):
                continue
            level_counts = counts.setdefault(int(level), {})
            level_counts[score_value] = level_counts.get(score_value, 0) + row_count
        return counts

    def lowest_active_level(self) -> int:
        active_statuses = [
            PaperState.PDF_AVAILABLE.value,
            PaperState.TEXT_AVAILABLE.value,
            PaperState.SCORED.value,
        ]
        try:
            response = self._execute(
                self._client.table(self._table)
                .select("level")
                .in_(self._state_column, active_statuses)
                .order("level", desc=False)
                .limit(1)
            )
        except Exception:  # pragma: no cover
            return 0
        rows = response.data or [{"level": 0}]
        return rows[0]["level"]

    def fetch_by_id(self, paper_id: str) -> PaperRecord:
        response = self._execute(
            self._client.table(self._table).select("*").eq("id", paper_id).limit(1)
        )
        return self._to_record(self._single(response.data))

    def _execute_rpc(
        self, fn: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        builder = self._client.rpc(fn, params or {})
        response = self._execute(builder)
        return response.data or []

    def increment_attempts(
        self, paper_id: str, current_attempts: Optional[int] = None
    ) -> PaperRecord:
        attempts = (current_attempts or 0) + 1
        now_iso = datetime.now(timezone.utc).isoformat()
        response = self._execute(
            self._client.table(self._table)
            .update({"attempts": attempts, "updated_at": now_iso})
            .eq("id", paper_id)
        )
        return self._to_record(self._single(response.data))

    def update_state(
        self, paper_id: str, state: PaperState, updates: Optional[Dict[str, Any]] = None
    ) -> PaperRecord:
        values = {
            self._state_column: state.value,
            "attempts": 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if updates:
            sanitized_updates = {
                key: value for key, value in updates.items() if key != "attempts"
            }
            values.update(sanitized_updates)
        response = self._execute(
            self._client.table(self._table).update(values).eq("id", paper_id)
        )
        return self._to_record(self._single(response.data))

    def _execute(self, builder: Any) -> Any:
        attempts = 0
        while True:
            try:
                return builder.execute()
            except PostgrestAPIError as exc:
                raise _map_postgrest_error(exc) from exc
            except Exception as exc:  # pragma: no cover - unexpected client failure
                if attempts >= MAX_TRANSIENT_RETRIES or not _is_transient_transport_error(
                    exc
                ):
                    raise RepositoryError(str(exc)) from exc
                attempts += 1
                time.sleep(RETRY_BASE_DELAY_SECONDS * attempts)

    def save_text_reference(self, paper_id: str, text_uri: str) -> PaperRecord:
        return self.update_state(
            paper_id,
            PaperState.TEXT_AVAILABLE,
            updates={"text_uri": text_uri},
        )

    def register_score(
        self,
        paper_id: str,
        score: float,
        reason: str,
        model_name: str,
        duration_ms: int,
        state: PaperState = PaperState.SCORED,
    ) -> PaperRecord:
        updates = {
            "score": score,
            "reason": reason,
            "model_name": model_name,
            "duration_ms": duration_ms,
        }
        return self.update_state(paper_id, state, updates=updates)

    @staticmethod
    def _single(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        rows = list(rows)
        if not rows:
            raise RepositoryError("Supabase returned no rows for mutation")
        return rows[0]

    @staticmethod
    def _maybe_ref(uri: Optional[str]) -> Optional[StorageRef]:
        return StorageRef(uri) if uri else None

    def _to_record(self, payload: Dict[str, Any]) -> PaperRecord:
        state_value = payload.get(self._state_column)
        if state_value is None and self._state_column != "status":
            state_value = payload.get("status")
        return PaperRecord(
            id=payload["id"],
            title=payload["title"],
            state=PaperState(state_value),
            level=payload.get("level", 1),
            pdf_ref=self._maybe_ref(payload.get("source_uri")),
            text_ref=self._maybe_ref(payload.get("text_uri")),
            score=payload.get("score"),
            reason=payload.get("reason"),
            pdf_md5=payload.get("pdf_md5"),
            parent_id=payload.get("parent_id"),
            seed_number=payload.get("seed_number"),
            metadata=payload.get("metadata") or {},
            attempts=int(payload.get("attempts") or 0),
            created_at=payload.get("created_at"),
            updated_at=payload.get("updated_at"),
        )


def _map_postgrest_error(error: PostgrestAPIError) -> RepositoryError:
    if _is_topic_unique_error(error):
        return DuplicateTopicError("A paper already exists for this topic.")
    if _is_title_unique_error(error):
        return DuplicateTitleError("A paper already exists with this title.")
    message = getattr(error, "message", None) or getattr(error, "details", None)
    return RepositoryError(str(message or error))


def _is_topic_unique_error(error: PostgrestAPIError) -> bool:
    code = getattr(error, "code", None)
    if code is not None and str(code) != "23505":
        return False
    text = " ".join(
        str(getattr(error, attr, "") or "").lower()
        for attr in ("message", "details", "hint")
    )
    return "topic" in text and ("duplicate" in text or "already exists" in text)


def _is_title_unique_error(error: PostgrestAPIError) -> bool:
    code = getattr(error, "code", None)
    if code is not None and str(code) != "23505":
        return False
    text = " ".join(
        str(getattr(error, attr, "") or "").lower()
        for attr in ("message", "details", "hint")
    )
    return "papers_title_unique" in text or ("title" in text and "duplicate" in text)


def _is_server_disconnect_error(exc: Exception) -> bool:
    return _is_transient_transport_error(exc)


def _is_transient_transport_error(exc: Exception) -> bool:
    for current in _exception_chain(exc):
        if _TRANSIENT_HTTP_ERRORS and isinstance(current, _TRANSIENT_HTTP_ERRORS):
            return True
        message = str(current).lower()
        if (
            "server disconnected" in message
            or "connection terminated" in message
            or "connection reset" in message
            or "remote protocol error" in message
            or "goaway" in message
            or "broken pipe" in message
        ):
            return True
    return False


def _exception_chain(exc: Exception) -> List[BaseException]:
    chain: List[BaseException] = []
    current: Optional[BaseException] = exc
    for _ in range(6):
        if current is None or current in chain:
            break
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


HASHABLE_STATES = {
    PaperState.PDF_AVAILABLE.value,
    PaperState.TEXT_AVAILABLE.value,
    PaperState.SCORED.value,
    PaperState.PROCESSED.value,
    PaperState.PROCESSED_LOW_SCORE.value,
}
