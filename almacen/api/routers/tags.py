# almacen/api/routers/tags.py
"""REST endpoints for adding, removing, and listing a file's tags."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends
from starlette.convertors import PathConvertor, register_url_convertor

from almacen.api.deps import (
    get_live_record,
    get_metadata_store,
    get_node_id,
    mutate_live_record,
    tag_or_422,
)
from almacen.api.schemas import AddTagsRequest, TagsResponse
from almacen.domain.file_record import FileRecord
from almacen.storage.protocols import MetadataStoreLike

router = APIRouter(prefix="/files/{file_id}/tags", tags=["tags"])


class _AnyTagConvertor(PathConvertor):
    """Matches any non-empty path remainder, newlines included.

    The handler, not the router, decides what a valid tag is: with the default
    `str` convertor a percent-encoded "/" or newline would fail to route and
    answer 404 instead of the tag rule's 422. The router matches against the
    decoded path, so "/" must be allowed, and `.` alone skips newlines.
    """

    regex = "(?s:.+)"


register_url_convertor("any_tag", _AnyTagConvertor())


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
    node_id: str = Depends(get_node_id),
) -> TagsResponse:
    def apply(record: FileRecord) -> None:
        for tag in body.tags:
            record.add_tag(tag, node_id)

    updated = mutate_live_record(metadata_store, file_id, apply)
    return TagsResponse(tags=sorted(updated.tags))


@router.delete("/{tag:any_tag}", response_model=TagsResponse)
def remove_tag(
    file_id: uuid.UUID,
    tag: str,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    node_id: str = Depends(get_node_id),
) -> TagsResponse:
    normalized = tag_or_422(tag)
    updated = mutate_live_record(
        metadata_store, file_id, lambda record: record.remove_tag(normalized, node_id)
    )
    return TagsResponse(tags=sorted(updated.tags))
