from __future__ import annotations

from types import SimpleNamespace
from typing import Optional

import pytest

from src.common import repository as repo_module
from src.common.errors import DuplicateTopicError, RepositoryError
from src.common.repository import PaperRepository
from src.common.dtos import PaperState


class FakeTable:
    def __init__(self, error: Optional[Exception] = None):
        self._error = error
        self.payload = None

    def insert(self, payload):
        self.payload = payload
        return self

    def execute(self):
        if self._error:
            raise self._error
        return SimpleNamespace(data=[self.payload])


class FakeUpdateTable:
    def __init__(self):
        self.update_payload = None
        self.filters = []

    def update(self, payload):
        self.update_payload = payload
        return self

    def eq(self, column, value):
        self.filters.append((column, value))
        return self

    def execute(self):
        paper_id = self.filters[-1][1] if self.filters else "paper-id"
        row = {
            "id": paper_id,
            "title": "Paper",
            "status": self.update_payload.get("status", "PDF Available"),
            "level": 1,
            "metadata": self.update_payload.get("metadata") or {},
            "attempts": self.update_payload.get("attempts", 0),
        }
        return SimpleNamespace(data=[row])


class FlakyTable(FakeTable):
    def __init__(self, errors):
        super().__init__()
        self._errors = errors
        self.calls = 0

    def execute(self):
        if self.calls < len(self._errors):
            exc = self._errors[self.calls]
            self.calls += 1
            raise exc
        self.calls += 1
        return SimpleNamespace(data=[self.payload])


class FakeRPCResponse:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return SimpleNamespace(data=self._data)


class FakeRpcHandler:
    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    def build(self, function: str, params: Optional[dict]) -> FakeRPCResponse:
        self.calls.append({"fn": function, "params": params or {}})
        data = self._responses.get(function, [])
        return FakeRPCResponse(data)


class FakeClient:
    def __init__(self, table: FakeTable, rpc_handler: Optional[FakeRpcHandler] = None):
        self._table = table
        self._rpc_handler = rpc_handler

    def table(self, name: str):
        return self._table

    def rpc(self, function: str, params: Optional[dict] = None):
        if not self._rpc_handler:
            raise AssertionError("RPC handler not configured")
        return self._rpc_handler.build(function, params)


class FakeSelectTable(FakeTable):
    def __init__(self, batches):
        super().__init__()
        self._batches = batches
        self._calls = 0

    def select(self, *_args, **_kwargs):
        return self

    @property
    def not_(self):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def is_(self, *_args, **_kwargs):
        return self

    def range(self, *_args, **_kwargs):
        return self

    def execute(self):
        if self._calls >= len(self._batches):
            data = []
        else:
            data = self._batches[self._calls]
        self._calls += 1
        return SimpleNamespace(data=data)


class FakeSeedRowsTable(FakeTable):
    def __init__(self, batches):
        super().__init__()
        self._batches = batches
        self._calls = 0
        self.filters = []
        self.ranges = []

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, column, value):
        self.filters.append((column, value))
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self.ranges.append((start, end))
        return self

    def execute(self):
        if self._calls >= len(self._batches):
            data = []
        else:
            data = self._batches[self._calls]
        self._calls += 1
        return SimpleNamespace(data=data)


class FakeStatusFilterTable(FakeTable):
    def __init__(self, rows):
        super().__init__()
        self._rows = rows
        self.status_column = "status"
        self.statuses = []
        self.limit_value = None

    def select(self, *_args, **_kwargs):
        return self

    def in_(self, column, values):
        self.status_column = column
        self.statuses = list(values)
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def execute(self):
        rows = [
            row for row in self._rows if row.get(self.status_column) in set(self.statuses)
        ]
        if self.limit_value is not None:
            rows = rows[: self.limit_value]
        return SimpleNamespace(data=rows)


class FakePagedTable(FakeTable):
    def __init__(self, batches):
        super().__init__()
        self._batches = batches
        self._calls = 0
        self.ranges = []

    def select(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self.ranges.append((start, end))
        return self

    def execute(self):
        if self._calls >= len(self._batches):
            data = []
        else:
            data = self._batches[self._calls]
        self._calls += 1
        return SimpleNamespace(data=data)


class _DummyPostgrestError(Exception):
    def __init__(
        self,
        *,
        code: Optional[str] = None,
        message: str = "",
        details: str = "",
        hint: Optional[str] = None,
    ):
        super().__init__(message or details)
        self.code = code
        self.message = message
        self.details = details
        self.hint = hint


def test_create_pdf_available_raises_duplicate_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo_module, "PostgrestAPIError", _DummyPostgrestError)
    table = FakeTable(
        error=_DummyPostgrestError(
            code="23505",
            message='duplicate key value violates unique constraint "papers_topic_topic_key"',
            details="Key (topic)=(genomics) already exists.",
        )
    )
    repository = PaperRepository(FakeClient(table))

    with pytest.raises(DuplicateTopicError):
        repository.create_pdf_available(
            title="Seed",
            pdf_uri="storage://pdfs/seed.pdf",
            level=1,
        )


def test_level_score_counts_groups_by_level_and_score() -> None:
    rpc = FakeRpcHandler(
        {
            "stats_level_score": [
                {"level": 1, "score": 0.5, "seed_number": 1, "total": 2},
                {"level": 2, "score": "0.75", "seed_number": 2, "total": 1},
                {"level": 2, "score": None, "seed_number": 3, "total": 1},
            ]
        }
    )
    repository = PaperRepository(FakeClient(FakeTable(), rpc_handler=rpc))

    counts = repository.level_score_counts()

    assert counts == {1: {0.5: 2}, 2: {0.75: 1}}


def test_level_status_counts_optionally_filters_by_seed_number() -> None:
    rpc = FakeRpcHandler(
        {
            "stats_level_status": [
                {"level": 1, "status": "PDF Available", "seed_number": 9, "total": 2},
                {"level": 2, "status": "PDF Available", "seed_number": 9, "total": 1},
            ]
        }
    )
    repository = PaperRepository(FakeClient(FakeTable(), rpc_handler=rpc))

    counts = repository.level_status_counts(seed_number=9)

    assert counts["PDF Available"] == {1: 2, 2: 1}


def test_create_pdf_available_wraps_other_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo_module, "PostgrestAPIError", _DummyPostgrestError)
    table = FakeTable(error=_DummyPostgrestError(message="unexpected failure"))
    repository = PaperRepository(FakeClient(table))

    with pytest.raises(RepositoryError):
        repository.create_pdf_available(
            title="Seed",
            pdf_uri="storage://pdfs/seed.pdf",
            level=1,
        )


def test_create_pdf_available_persists_seed_number() -> None:
    table = FakeTable()
    repository = PaperRepository(FakeClient(table))

    repository.create_pdf_available(
        title="Seed",
        pdf_uri="storage://pdfs/seed.pdf",
        level=1,
        seed_number=7,
    )

    assert table.payload["seed_number"] == 7


def test_create_pdf_available_persists_pdf_md5() -> None:
    table = FakeTable()
    repository = PaperRepository(FakeClient(table))

    repository.create_pdf_available(
        title="Seed",
        pdf_uri="storage://pdfs/seed.pdf",
        level=1,
        pdf_md5="abc123",
    )

    assert table.payload["pdf_md5"] == "abc123"


def test_insert_pgx_extractions_persists_rows() -> None:
    table = FakeTable()
    repository = PaperRepository(FakeClient(table))

    inserted = repository.insert_pgx_extractions(
        "paper-1",
        [
            {
                "sample_id": "Case-1",
                "gene": "CYP2C19",
                "allele": "*1/*2",
                "rs_id": "rs4244285",
                "medication": "Clopidogrel",
                "outcome": "Ineffective",
                "actionability": "Yes",
                "cpic_recommendation": "Use alternative",
                "source_context": "Individual Case Series",
            }
        ],
    )

    assert inserted == 1
    assert table.payload[0]["paper_id"] == "paper-1"


def test_create_pdf_placeholder_retries_server_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repo_module, "PostgrestAPIError", _DummyPostgrestError)
    table = FlakyTable([Exception("Server disconnected")])
    repository = PaperRepository(FakeClient(table))

    record = repository.create_pdf_placeholder(title="Paper", parent_id="parent", level=2)

    assert record.state.value == "PDF Not Available"
    assert table.calls == 2


def test_create_pdf_placeholder_inherits_seed_number() -> None:
    table = FakeTable()
    repository = PaperRepository(FakeClient(table))

    repository.create_pdf_placeholder(
        title="Paper",
        parent_id="parent",
        level=2,
        seed_number=33,
    )

    assert table.payload["seed_number"] == 33


def test_seed_status_counts_groups_by_seed_and_status() -> None:
    rpc = FakeRpcHandler(
        {
            "stats_seed_status": [
                {"seed_number": 11, "status": "PDF Available", "total": 2},
                {"seed_number": None, "status": "Scored", "total": 1},
            ]
        }
    )
    repository = PaperRepository(FakeClient(FakeTable(), rpc_handler=rpc))

    counts = repository.seed_status_counts()

    assert counts[11]["PDF Available"] == 2
    assert counts[None]["Scored"] == 1


def test_level_score_counts_includes_filter_params() -> None:
    rpc = FakeRpcHandler({"stats_level_score": []})
    repository = PaperRepository(FakeClient(FakeTable(), rpc_handler=rpc))

    repository.level_score_counts(seed_number=7)

    assert rpc.calls[-1] == {"fn": "stats_level_score", "params": {"seed_filter": 7}}


def test_update_state_resets_attempts_field() -> None:
    table = FakeUpdateTable()
    repository = PaperRepository(FakeClient(table))

    repository.update_state(
        "paper-1",
        PaperState.SCORED,
        updates={"attempts": 5, "metadata": {"foo": "bar"}},
    )

    assert table.update_payload["attempts"] == 0
    assert table.update_payload["metadata"] == {"foo": "bar"}
    assert "updated_at" in table.update_payload


def test_increment_attempts_sets_updated_at() -> None:
    table = FakeUpdateTable()
    repository = PaperRepository(FakeClient(table))

    repository.increment_attempts("paper-1", current_attempts=2)

    assert table.update_payload["attempts"] == 3
    assert "updated_at" in table.update_payload


def test_fetch_rows_by_seed_number_paginates() -> None:
    first_page = [{"id": f"paper-{idx}", "seed_number": 9} for idx in range(1000)]
    second_page = [{"id": "paper-1000", "seed_number": 9}]
    table = FakeSeedRowsTable([first_page, second_page])
    repository = PaperRepository(FakeClient(table))

    rows = repository.fetch_rows_by_seed_number(seed_number=9)

    assert len(rows) == 1001
    assert table.filters[0] == ("seed_number", 9)
    assert table.ranges == [(0, 999), (1000, 1999)]


def test_fetch_by_statuses_returns_filtered_rows() -> None:
    rows = [
        {
            "id": "a",
            "title": "A",
            "status": "P1",
            "level": 1,
            "attempts": 0,
            "metadata": {},
        },
        {
            "id": "b",
            "title": "B",
            "status": "Scored",
            "level": 1,
            "attempts": 0,
            "metadata": {},
        },
    ]
    table = FakeStatusFilterTable(rows)
    repository = PaperRepository(FakeClient(table))

    papers = repository.fetch_by_statuses(["P1", "P1WIP"], limit=50)

    assert len(papers) == 1
    assert papers[0].id == "a"


def test_fetch_by_statuses_uses_custom_state_column() -> None:
    rows = [
        {
            "id": "a",
            "title": "A",
            "gstatus": "P1",
            "level": 1,
            "attempts": 0,
            "metadata": {},
        },
        {
            "id": "b",
            "title": "B",
            "gstatus": "P1Success",
            "level": 1,
            "attempts": 0,
            "metadata": {},
        },
    ]
    table = FakeStatusFilterTable(rows)
    repository = PaperRepository(FakeClient(table), table="gpapers", state_column="gstatus")

    papers = repository.fetch_by_statuses(["P1"], limit=50)

    assert len(papers) == 1
    assert papers[0].id == "a"


def test_fetch_all_rows_from_table_paginates() -> None:
    first_page = [{"id": idx} for idx in range(1000)]
    second_page = [{"id": 1001}]
    table = FakePagedTable([first_page, second_page])
    repository = PaperRepository(FakeClient(table))

    rows = repository.fetch_all_rows_from_table("pgx_extractions")

    assert len(rows) == 1001
    assert table.ranges == [(0, 999), (1000, 1999)]
