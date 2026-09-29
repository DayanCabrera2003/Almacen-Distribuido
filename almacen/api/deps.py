# almacen/api/deps.py
"""FastAPI dependency providers and shared request-handling helpers."""
from __future__ import annotations

import uuid
from collections.abc import Callable

from fastapi import HTTPException, Request

from almacen.cluster.replication_client import ReplicationClient
from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.protocols import MetadataStoreLike
from almacen.storage.tag_index import TagIndex


def get_blob_store(request: Request) -> BlobStore:
    return request.app.state.blob_store


def get_metadata_store(request: Request) -> MetadataStoreLike:
    return request.app.state.metadata_store


def get_node_id(request: Request) -> str:
    """This node's identity, which every CRDT mutation is attributed to."""
    return request.app.state.settings.node_id


def get_tag_index(request: Request) -> TagIndex:
    return request.app.state.tag_index


def get_replication_client(request: Request) -> ReplicationClient:
    return request.app.state.replication_client


def get_live_record(
    metadata_store: MetadataStoreLike, file_id: uuid.UUID
) -> FileRecord:
    """Look up a file, raising 404 if it's missing or tombstoned (spec §10: a
    tombstoned file is indistinguishable from an unknown one via the API)."""
    record = metadata_store.get(file_id)
    if record is None or record.tombstone:
        raise HTTPException(status_code=404, detail="file not found")
    return record


class _RecordGone(Exception):
    """Raised inside a mutator when the record turned out not to be live."""


def mutate_live_record(
    metadata_store: MetadataStoreLike,
    file_id: uuid.UUID,
    mutator: Callable[[FileRecord], None],
) -> FileRecord:
    """Apply `mutator` to a live record atomically, or raise 404.

    Reading a record and writing it back in two separate calls loses concurrent
    updates, because the write replaces the whole stored value from a snapshot
    that may already be stale. Routing every read-modify-write through the
    store's `mutate` keeps the sequence under one lock.

    The liveness check happens *inside* the mutation, not before it, so a file
    deleted between the check and the write cannot be resurrected.
    """

    def guarded(record: FileRecord) -> None:
        if record.tombstone:
            raise _RecordGone
        mutator(record)

    try:
        updated = metadata_store.mutate(file_id, guarded)
    except _RecordGone:
        raise HTTPException(status_code=404, detail="file not found") from None

    if updated is None:
        raise HTTPException(status_code=404, detail="file not found")
    return updated
