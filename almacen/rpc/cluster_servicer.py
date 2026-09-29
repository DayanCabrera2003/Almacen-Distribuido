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

        local = self._metadata_store.get(record.file_id)
        if local is not None and local.is_concurrent_with(record):
            # Spec §9: a genuine conflict is resolved by last-writer-wins, but
            # never silently. Logging both clocks is what makes the resolution
            # auditable — LWW discards one of two concurrent writes, and that
            # loss should be visible rather than inferred.
            logger.warning(
                "concurrent update to %s from %s: local clock %s, incoming %s "
                "- resolving by last-writer-wins",
                record.file_id,
                context.peer() if context is not None else "unknown",
                local.vector_clock.counters,
                record.vector_clock.counters,
            )

        # Merge, never overwrite. An incoming record is one replica's view, not
        # the truth: the local copy may hold edits the sender never saw.
        self._metadata_store.merge_remote(record)
        logger.debug("merged replicated record %s", record.file_id)
        return pb.ReplicateAck(applied=True)

    def Ping(
        self, request: pb.PingRequest, context: grpc.ServicerContext
    ) -> pb.PingResponse:
        return pb.PingResponse(node_id=self._node_id)
