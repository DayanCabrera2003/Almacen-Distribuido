# almacen/api/deps.py
"""FastAPI dependency providers and shared request-handling helpers."""
from __future__ import annotations

import uuid

from fastapi import HTTPException, Request

from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


def get_blob_store(request: Request) -> BlobStore:
    return request.app.state.blob_store


def get_metadata_store(request: Request) -> MetadataStore:
    return request.app.state.metadata_store


def get_tag_index(request: Request) -> TagIndex:
    return request.app.state.tag_index


def get_live_record(metadata_store: MetadataStore, file_id: uuid.UUID) -> FileRecord:
    """Look up a file, raising 404 if it's missing or tombstoned (spec §10: a
    tombstoned file is indistinguishable from an unknown one via the API)."""
    record = metadata_store.get(file_id)
    if record is None or record.tombstone:
        raise HTTPException(status_code=404, detail="file not found")
    return record
