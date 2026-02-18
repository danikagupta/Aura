"""Upload and download artifacts from Supabase Storage."""

from __future__ import annotations

import hashlib
from typing import Any, Optional

from storage3.exceptions import StorageApiError

from ..common.dtos import StorageRef
from ..common.errors import StorageError


class StorageGateway:
    """Encapsulates Supabase Storage operations for PDFs and extracted text."""

    def __init__(self, client: Any, pdf_bucket: str, text_bucket: str):
        self._client = client
        self._pdf_bucket = pdf_bucket
        self._text_bucket = text_bucket

    def store_pdf(self, paper_id: str, filename: str, data: bytes) -> StorageRef:
        key = self._build_key(paper_id, filename, data)
        bucket = self._client.storage.from_(self._pdf_bucket)
        options = {"content-type": "application/pdf", "cache-control": "3600"}
        try:
            response = bucket.upload(key, data, options)
        except StorageApiError as exc:  # pragma: no cover - depends on runtime state
            if _is_duplicate_storage_error(exc):
                return StorageRef(self._uri(self._pdf_bucket, key))
            raise StorageError(str(exc)) from exc
        except Exception as exc:  # pragma: no cover - network/runtime failures
            raise StorageError(str(exc)) from exc
        if not response:
            raise StorageError("Supabase did not acknowledge PDF upload")
        return StorageRef(self._uri(self._pdf_bucket, key))

    def store_text(self, paper_id: str, text: str) -> StorageRef:
        data = text.encode("utf-8")
        key = self._build_key(paper_id, "extracted.txt", data)
        bucket = self._client.storage.from_(self._text_bucket)
        options = {"content-type": "text/plain", "cache-control": "3600"}
        try:
            response = bucket.upload(key, data, options)
        except StorageApiError as exc:  # pragma: no cover
            if _is_duplicate_storage_error(exc):
                return StorageRef(self._uri(self._text_bucket, key))
            raise StorageError(str(exc)) from exc
        except Exception as exc:  # pragma: no cover
            raise StorageError(str(exc)) from exc
        if not response:
            raise StorageError("Supabase did not acknowledge text upload")
        return StorageRef(self._uri(self._text_bucket, key))

    def fetch_blob(self, ref: StorageRef) -> bytes:
        bucket_name, path = self._parse_uri(ref.uri)
        bucket = self._client.storage.from_(bucket_name)
        data = bucket.download(path)
        if data is None:
            raise StorageError(f"Supabase returned empty blob for {ref.uri}")
        return data

    def _build_key(self, paper_id: str, filename: str, data: bytes) -> str:
        checksum = hashlib.sha1(data).hexdigest()  # noqa: S324
        return f"{paper_id}/{checksum}-{filename}"

    @staticmethod
    def _uri(bucket: str, path: str) -> str:
        return f"storage://{bucket}/{path}"

    @staticmethod
    def _parse_uri(uri: str) -> tuple[str, str]:
        segments = uri.split("/", 3)
        if len(segments) < 4:
            raise StorageError(f"Malformed storage URI: {uri}")
        return segments[2], segments[3]


def _is_duplicate_storage_error(error: StorageApiError) -> bool:
    payload = error.args[0] if error.args else {}
    if isinstance(payload, dict):
        status = payload.get("statusCode")
        message = str(payload.get("message", ""))
        if status == 409 or "already exists" in message or "Duplicate" in message:
            return True
    return "already exists" in str(error)
