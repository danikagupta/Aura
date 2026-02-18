from __future__ import annotations

from types import SimpleNamespace

from src.pipelines.gpapers_sync import sync_gpapers_from_papers


class _FakePapersQuery:
    def __init__(self, batches):
        self._batches = batches
        self._calls = 0
        self.statuses = None
        self.ranges = []

    def select(self, *_args, **_kwargs):
        return self

    def in_(self, _column, values):
        self.statuses = list(values)
        return self

    def order(self, *_args, **_kwargs):
        return self

    def range(self, start, end):
        self.ranges.append((start, end))
        return self

    def execute(self):
        if self._calls >= len(self._batches):
            rows = []
        else:
            rows = self._batches[self._calls]
        self._calls += 1
        return SimpleNamespace(data=rows)


class _FakeGpapersQuery:
    def __init__(self):
        self.payloads = []
        self.conflicts = []

    def upsert(self, payload, on_conflict):
        self.payloads.append(payload)
        self.conflicts.append(on_conflict)
        return self

    def execute(self):
        return SimpleNamespace(data=[])


class _FakeClient:
    def __init__(self, papers_batches):
        self.papers_query = _FakePapersQuery(papers_batches)
        self.gpapers_query = _FakeGpapersQuery()

    def table(self, name: str):
        if name == "papers":
            return self.papers_query
        if name == "gpapers":
            return self.gpapers_query
        raise AssertionError(f"Unexpected table: {name}")


def test_sync_gpapers_from_papers_upserts_with_gstatus_and_paginates() -> None:
    first = [{"id": "a", "status": "P1"}, {"id": "b", "status": "P1Success"}]
    second = [{"id": "c", "status": "P1Failure"}]
    client = _FakeClient([first, second])

    result = sync_gpapers_from_papers(client, page_size=2)

    assert result == {"fetched": 3, "upserted": 3}
    assert client.papers_query.ranges == [(0, 1), (2, 3)]
    assert client.gpapers_query.conflicts == ["id", "id"]
    first_payload = client.gpapers_query.payloads[0]
    assert first_payload[0]["gstatus"] == "P1"
    assert first_payload[1]["gstatus"] == "P1"
