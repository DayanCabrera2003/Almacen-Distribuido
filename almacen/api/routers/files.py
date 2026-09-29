# almacen/api/routers/files.py
"""REST endpoints for file upload, download, update, and delete."""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, UploadFile
from fastapi.responses import Response

from almacen.api.deps import get_blob_store, get_live_record, get_metadata_store
from almacen.api.schemas import FileMetadata, to_file_metadata
from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

router = APIRouter(prefix="/files", tags=["files"])


def _parse_tags(tags: str | None) -> set[str]:
    if not tags:
        return set()
    return {tag.strip() for tag in tags.split(",") if tag.strip()}


@router.post("", response_model=FileMetadata, status_code=201)
async def upload_file(
    file: UploadFile,
    name: Annotated[str | None, Form()] = None,
    tags: Annotated[str | None, Form()] = None,
    blob_store: BlobStore = Depends(get_blob_store),
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> FileMetadata:
    content = await file.read()
    content_hash = blob_store.put(content)
    record = FileRecord.new(
        name=name or file.filename or "untitled",
        content_hash=content_hash,
        tags=_parse_tags(tags),
    )
    metadata_store.insert(record)
    return to_file_metadata(record)


@router.get("/{file_id}")
def download_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStore = Depends(get_metadata_store),
    blob_store: BlobStore = Depends(get_blob_store),
) -> Response:
    record = get_live_record(metadata_store, file_id)
    content = blob_store.get(record.content_hash)
    if content is None:
        # Would indicate local storage corruption/bug, not a client error.
        raise RuntimeError(f"blob {record.content_hash} missing for known file {file_id}")
    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{record.name}"'},
    )


@router.patch("/{file_id}", response_model=FileMetadata)
async def update_file(
    file_id: uuid.UUID,
    file: Annotated[UploadFile | None, File()] = None,
    name: Annotated[str | None, Form()] = None,
    metadata_store: MetadataStore = Depends(get_metadata_store),
    blob_store: BlobStore = Depends(get_blob_store),
) -> FileMetadata:
    record = get_live_record(metadata_store, file_id)
    if name is not None:
        record.rename(name)
    if file is not None:
        content = await file.read()
        content_hash = blob_store.put(content)
        record.update_content(content_hash)
    metadata_store.update(record)
    return to_file_metadata(record)


@router.delete("/{file_id}", status_code=204)
def delete_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> None:
    record = get_live_record(metadata_store, file_id)
    record.mark_deleted()
    metadata_store.update(record)
