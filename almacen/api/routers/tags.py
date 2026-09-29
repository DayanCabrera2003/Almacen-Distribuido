# almacen/api/routers/tags.py
"""REST endpoints for adding, removing, and listing a file's tags."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends

from almacen.api.deps import get_live_record, get_metadata_store, mutate_live_record
from almacen.api.schemas import AddTagsRequest, TagsResponse
from almacen.domain.file_record import FileRecord
from almacen.storage.protocols import MetadataStoreLike

router = APIRouter(prefix="/files/{file_id}/tags", tags=["tags"])


@router.get("", response_model=TagsResponse)
def list_tags(
    file_id: uuid.UUID,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
) -> TagsResponse:
    record = get_live_record(metadata_store, file_id)
    return TagsResponse(tags=sorted(record.tags))


@router.post("", response_model=TagsResponse)
def add_tags(
    file_id: uuid.UUID,
    body: AddTagsRequest,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
) -> TagsResponse:
    def apply(record: FileRecord) -> None:
        for tag in body.tags:
            record.add_tag(tag)

    updated = mutate_live_record(metadata_store, file_id, apply)
    return TagsResponse(tags=sorted(updated.tags))


@router.delete("/{tag}", response_model=TagsResponse)
def remove_tag(
    file_id: uuid.UUID,
    tag: str,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
) -> TagsResponse:
    updated = mutate_live_record(
        metadata_store, file_id, lambda record: record.remove_tag(tag)
    )
    return TagsResponse(tags=sorted(updated.tags))
