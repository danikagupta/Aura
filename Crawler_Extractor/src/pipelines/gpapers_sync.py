"""Utilities for syncing PGX-focused rows from papers into gpapers."""

from __future__ import annotations

from typing import Any, Dict, Sequence


P1_SOURCE_STATUSES = ("P1", "P1Success", "P1Failure", "P1WIP")


def sync_gpapers_from_papers(
    client: Any,
    *,
    statuses: Sequence[str] = P1_SOURCE_STATUSES,
    page_size: int = 500,
) -> Dict[str, int]:
    """Copy P1 workflow rows from papers into gpapers and reset gstatus to P1."""

    offset = 0
    fetched = 0
    upserted = 0
    while True:
        response = (
            client.table("papers")
            .select("*")
            .in_("status", list(statuses))
            .order("created_at", desc=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = response.data or []
        if not rows:
            break
        payload = [{**row, "gstatus": "P1"} for row in rows]
        client.table("gpapers").upsert(payload, on_conflict="id").execute()
        fetched += len(rows)
        upserted += len(payload)
        if len(rows) < page_size:
            break
        offset += page_size
    return {"fetched": fetched, "upserted": upserted}
