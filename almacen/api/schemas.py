# almacen/api/schemas.py
"""Pydantic request/response models for the REST API."""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, field_validator

from almacen.domain.file_record import FileRecord
from almacen.domain.tags import normalize_tag


class FileMetadata(BaseModel):
    file_id: uuid.UUID
    name: str
    content_hash: str
    tags: list[str]
    created_at: datetime
    updated_at: datetime


class TagsResponse(BaseModel):
    tags: list[str]


class AddTagsRequest(BaseModel):
    tags: list[str]

    @field_validator("tags")
    @classmethod
    def _normalize(cls, tags: list[str]) -> list[str]:
        # InvalidTag is a ValueError, which FastAPI reports as a 422.
        return [normalize_tag(tag) for tag in tags]


class MemberStatus(BaseModel):
    node_id: str
    # None for a peer learned through gossip but absent from ALMACEN_PEERS.
    address: str | None
    state: str
    incarnation: int
    # When this node last saw the state change; None if it never has.
    since: datetime | None
    is_self: bool


class ReplicationSettings(BaseModel):
    replication_factor: int
    write_quorum: int
    rpc_timeout_seconds: float


class GossipSettings(BaseModel):
    enabled: bool
    interval_seconds: float
    fanout: int
    suspicion_timeout_seconds: float
    blob_catchup_budget: int


class GCSettings(BaseModel):
    grace_ttl_seconds: float
    budget: int


class StorageCounts(BaseModel):
    live_records: int
    tombstones: int
    blobs: int


class ClusterStatus(BaseModel):
    node_id: str
    members: list[MemberStatus]
    replication: ReplicationSettings
    gossip: GossipSettings
    gc: GCSettings
    storage: StorageCounts


def to_file_metadata(record: FileRecord) -> FileMetadata:
    return FileMetadata(
        file_id=record.file_id,
        name=record.name,
        content_hash=record.content_hash,
        tags=sorted(record.tags),
        created_at=record.created_at,
        updated_at=record.updated_at,
    )
