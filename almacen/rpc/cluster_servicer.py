# almacen/rpc/cluster_servicer.py
"""Receives metadata replicated from peer nodes."""
from __future__ import annotations

import logging

import grpc

from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.record_codec import message_to_record
from almacen.storage.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


class ClusterServicer(pb_grpc.ClusterServicer):
    def __init__(self, metadata_store: MetadataStore, node_id: str) -> None:
        # Deliberately the *local* store, never the replicating wrapper: applying
        # a record received from a peer must not push it back out, or every
        # write would ping-pong around the cluster forever.
        self._metadata_store = metadata_store
        self._node_id = node_id

    def ReplicateRecord(
        self, request: pb.FileRecordMsg, context: grpc.ServicerContext
    ) -> pb.ReplicateAck:
        try:
            record = message_to_record(request)
        except ValueError as error:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"undecodable record: {error}"
            )

        # Phase 2 applies the incoming record wholesale (last write to arrive
        # wins). That is genuinely wrong under concurrency: two nodes editing the
        # same file during a partition will clobber each other depending on
        # arrival order. Phase 3 replaces this line with a CRDT merge, which is
        # the whole reason that phase exists.
        self._metadata_store.upsert(record)
        logger.debug("applied replicated record %s", record.file_id)
        return pb.ReplicateAck(applied=True)

    def Ping(
        self, request: pb.PingRequest, context: grpc.ServicerContext
    ) -> pb.PingResponse:
        return pb.PingResponse(node_id=self._node_id)
