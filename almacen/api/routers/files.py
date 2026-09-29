# almacen/api/routers/files.py
"""REST endpoints for file upload, download, listing, update, and delete."""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool

from almacen.api.deps import (
    get_live_record,
    get_metadata_store,
    get_node_id,
    get_replication_client,
    get_tag_index,
    mutate_live_record,
)
from almacen.api.schemas import FileMetadata, to_file_metadata
from almacen.cluster.replication_client import QuorumNotReached, ReplicationClient
from almacen.domain.file_record import FileRecord
from almacen.storage.protocols import MetadataStoreLike
from almacen.storage.tag_index import TagIndex

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
    replication_client: ReplicationClient = Depends(get_replication_client),
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    node_id: str = Depends(get_node_id),
) -> FileMetadata:
    content = await file.read()
    # These handlers must stay `async def` to await UploadFile.read(), but
    # put_blob and the metadata push are synchronous and can each take up to the
    # gRPC deadline. Run them in a worker thread, or they would block the event
    # loop and stall every other request this node is serving.
    try:
        content_hash = await run_in_threadpool(replication_client.put_blob, content)
    except QuorumNotReached as error:
        # Recording metadata now would leave a file pointing at content that is
        # not durable, so nothing is written.
        raise HTTPException(status_code=503, detail=str(error)) from error
    record = FileRecord.new(
        name=name or file.filename or "untitled",
        content_hash=content_hash,
        node_id=node_id,
        tags=_parse_tags(tags),
    )
    await run_in_threadpool(metadata_store.insert, record)
    return to_file_metadata(record)


@router.get("", response_model=list[FileMetadata])
def list_files(
    tags: str | None = None,
    mode: str = "and",
    tag_index: TagIndex = Depends(get_tag_index),
) -> list[FileMetadata]:
    tag_list = list(_parse_tags(tags)) if tags else None
    # mode only applies when tags are given, so a tags-less request with a bogus
    # `mode` still succeeds instead of failing on an argument it never uses.
    if tag_list and mode not in ("and", "or"):
        raise HTTPException(status_code=400, detail="mode must be 'and' or 'or'")
    records = tag_index.query(tag_list, mode=mode)  # type: ignore[arg-type]
    return [to_file_metadata(record) for record in records]


@router.get("/{file_id}")
def download_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    replication_client: ReplicationClient = Depends(get_replication_client),
) -> Response:
    record = get_live_record(metadata_store, file_id)
    content = replication_client.get_blob(record.content_hash)
    if content is None:
        # The file exists; its bytes are temporarily unreachable. 404 would deny
        # the file, 500 would blame this node.
        raise HTTPException(
            status_code=503,
            detail=f"no replica currently holds content for file {file_id}",
        )
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
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    replication_client: ReplicationClient = Depends(get_replication_client),
    node_id: str = Depends(get_node_id),
) -> FileMetadata:
    # Check the file exists before spending a replication round on its content.
    get_live_record(metadata_store, file_id)

    content_hash: str | None = None
    if file is not None:
        content = await file.read()
        # Replicate before mutating: this is network I/O, and it must not run
        # while the metadata store's lock is held.
        try:
            content_hash = await run_in_threadpool(
                replication_client.put_blob, content
            )
        except QuorumNotReached as error:
            raise HTTPException(status_code=503, detail=str(error)) from error

    def apply(record: FileRecord) -> None:
        if name is not None:
            record.rename(name, node_id)
        if content_hash is not None:
            record.update_content(content_hash, node_id)

    updated = await run_in_threadpool(
        mutate_live_record, metadata_store, file_id, apply
    )
    return to_file_metadata(updated)


@router.delete("/{file_id}", status_code=204)
def delete_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStoreLike = Depends(get_metadata_store),
    node_id: str = Depends(get_node_id),
) -> None:
    mutate_live_record(
        metadata_store, file_id, lambda record: record.mark_deleted(node_id)
    )
