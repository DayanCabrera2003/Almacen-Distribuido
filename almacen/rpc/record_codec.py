# almacen/rpc/record_codec.py
"""Conversion between FileRecord and its gRPC wire representation.

Kept separate from the servicers because both directions are needed on both
sides of a call, and because this is the single place that has to change when
the record's CRDT shape changes.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from almacen.crdt.lww_register import LWWRegister
from almacen.crdt.or_set import Element, OrSet
from almacen.crdt.vector_clock import VectorClock
from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb


def record_to_message(record: FileRecord) -> pb.FileRecordMsg:
    return pb.FileRecordMsg(
        file_id=str(record.file_id),
        name=record.name,
        name_ts=record.name_register.timestamp.isoformat(),
        name_node=record.name_register.node_id,
        content_hash=record.content_hash,
        content_hash_ts=record.content_hash_register.timestamp.isoformat(),
        content_hash_node=record.content_hash_register.node_id,
        tombstone=record.tombstone,
        tombstone_ts=record.tombstone_register.timestamp.isoformat(),
        tombstone_node=record.tombstone_register.node_id,
        created_at=record.created_at.isoformat(),
        updated_at=record.updated_at.isoformat(),
        vector_clock=dict(record.vector_clock.counters),
        tag_adds=_encode_occurrences(record.tag_set.adds),
        tag_removes=_encode_occurrences(record.tag_set.removes),
    )


def message_to_record(message: pb.FileRecordMsg) -> FileRecord:
    created_at = _parse(message.created_at)
    updated_at = _parse(message.updated_at)
    # `files.created_at` and `files.updated_at` are NOT NULL in the schema, and
    # the LWW timestamps are what the merge compares. Left unchecked, a
    # truncated message would fail deep inside SQLite or silently compare as
    # None; rejecting here turns it into INVALID_ARGUMENT, which is what it is.
    if created_at is None or updated_at is None:
        raise ValueError("created_at and updated_at are required")

    name_ts = _parse(message.name_ts)
    content_hash_ts = _parse(message.content_hash_ts)
    tombstone_ts = _parse(message.tombstone_ts)
    if name_ts is None or content_hash_ts is None or tombstone_ts is None:
        raise ValueError("every last-writer-wins field needs a timestamp")

    # Naive timestamps would be accepted, stored, and then raise
    # `TypeError: can't compare offset-naive and offset-aware datetimes` out of
    # every subsequent merge on this file — an opaque gRPC UNKNOWN forever
    # after, unrecoverable without editing the database. The codec is the trust
    # boundary between this node and a peer that may be buggy or older, so the
    # check belongs here rather than deeper in.
    for label, value in (
        ("created_at", created_at),
        ("updated_at", updated_at),
        ("name_ts", name_ts),
        ("content_hash_ts", content_hash_ts),
        ("tombstone_ts", tombstone_ts),
    ):
        if value.tzinfo is None:
            raise ValueError(f"{label} must be timezone-aware, got {value!r}")

    return FileRecord(
        file_id=uuid.UUID(message.file_id),
        name_register=LWWRegister(message.name, name_ts, message.name_node),
        content_hash_register=LWWRegister(
            message.content_hash, content_hash_ts, message.content_hash_node
        ),
        tombstone_register=LWWRegister(
            message.tombstone, tombstone_ts, message.tombstone_node
        ),
        tag_set=OrSet(
            adds=_decode_occurrences(message.tag_adds),
            removes=_decode_occurrences(message.tag_removes),
        ),
        vector_clock=VectorClock(dict(message.vector_clock)),
        created_at=created_at,
        updated_at=updated_at,
    )


def _encode_occurrences(occurrences: set[Element]) -> list[pb.TagOccurrence]:
    # Sorted so a record always encodes identically, which makes messages
    # comparable in tests and diffable in logs.
    return [
        pb.TagOccurrence(tag=tag, node_id=node_id, counter=counter)
        for tag, node_id, counter in sorted(occurrences)
    ]


def _decode_occurrences(occurrences) -> set[Element]:
    return {(occ.tag, occ.node_id, occ.counter) for occ in occurrences}


def _parse(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
