# almacen/api/schemas.py
"""Pydantic request/response models for the REST API."""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel

from almacen.domain.file_record import FileRecord


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


def to_file_metadata(record: FileRecord) -> FileMetadata:
    return FileMetadata(
        file_id=record.file_id,
        name=record.name,
        content_hash=record.content_hash,
        tags=sorted(record.tags),
        created_at=record.created_at,
        updated_at=record.updated_at,
    )
