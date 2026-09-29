# almacen/rpc/record_codec.py
"""Conversion between FileRecord and its gRPC wire representation.

Kept separate from the servicers because both directions are needed on both
sides of a call, and because this is the single place Phase 3 has to touch when
FileRecord gains a vector clock and CRDT-valued fields.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb


def record_to_message(record: FileRecord) -> pb.FileRecordMsg:
    return pb.FileRecordMsg(
        file_id=str(record.file_id),
        name=record.name,
        content_hash=record.content_hash,
        # Sorted so the encoding of a record is deterministic, which makes
        # messages comparable in tests and diffable in logs.
        tags=sorted(record.tags),
        tombstone=record.tombstone,
        # proto3 scalars have no null; empty string is the absent value.
        tombstone_at=record.tombstone_at.isoformat() if record.tombstone_at else "",
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
    )


def message_to_record(message: pb.FileRecordMsg) -> FileRecord:
    created_at = _parse(message.created_at)
    updated_at = _parse(message.updated_at)
    # `files.created_at` and `files.updated_at` are NOT NULL in the schema. Left
    # unchecked, a truncated or hand-built message would reach `upsert` and fail
    # there as a sqlite3.IntegrityError, surfacing to the caller as an opaque
    # gRPC UNKNOWN. Rejecting here turns it into INVALID_ARGUMENT, which is what
    # it actually is.
    if created_at is None or updated_at is None:
        raise ValueError("created_at and updated_at are required")

    return FileRecord(
        file_id=uuid.UUID(message.file_id),
        name=message.name,
        content_hash=message.content_hash,
        tags=set(message.tags),
        tombstone=message.tombstone,
        tombstone_at=_parse(message.tombstone_at),
        created_at=created_at,
        updated_at=updated_at,
    )


def _parse(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
