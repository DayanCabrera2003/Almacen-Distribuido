# almacen/rpc/replication_servicer.py
"""Serves blob content to peer nodes over gRPC."""
from __future__ import annotations

import hashlib
from collections.abc import Iterator

import grpc

from almacen.rpc import replication_pb2 as pb
from almacen.rpc import replication_pb2_grpc as pb_grpc
from almacen.storage.blob_store import BlobStore

# 1 MiB: comfortably under gRPC's 4 MiB default per-message limit, and large
# enough that per-message overhead is irrelevant.
CHUNK_SIZE = 1 << 20


class ReplicationServicer(pb_grpc.ReplicationServicer):
    def __init__(self, blob_store: BlobStore) -> None:
        self._blob_store = blob_store

    def PutBlob(
        self, request_iterator: Iterator[pb.BlobChunk], context: grpc.ServicerContext
    ) -> pb.PutBlobAck:
        declared_hash = ""
        buffer = bytearray()
        received_any = False

        for chunk in request_iterator:
            received_any = True
            if chunk.content_hash:
                declared_hash = chunk.content_hash
            buffer.extend(chunk.data)

        if not received_any or not declared_hash:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "PutBlob requires at least one chunk carrying a content_hash",
            )

        # Verify before storing, never after: a blob is addressed by its hash, so
        # storing content whose hash does not match would poison every future
        # read of that address across the whole cluster.
        actual_hash = hashlib.sha256(bytes(buffer)).hexdigest()
        if actual_hash != declared_hash:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"content hash mismatch: declared {declared_hash}, got {actual_hash}",
            )

        stored_hash = self._blob_store.put(bytes(buffer))
        return pb.PutBlobAck(content_hash=stored_hash, stored=True)

    def GetBlob(
        self, request: pb.BlobRequest, context: grpc.ServicerContext
    ) -> Iterator[pb.BlobChunk]:
        content = self._blob_store.get(request.content_hash)
        if content is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"blob {request.content_hash} not held by this node",
            )

        for offset in range(0, len(content), CHUNK_SIZE):
            yield pb.BlobChunk(
                content_hash=request.content_hash,
                data=content[offset : offset + CHUNK_SIZE],
            )
