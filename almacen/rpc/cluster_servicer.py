# almacen/rpc/cluster_servicer.py
"""Receives metadata replicated from peer nodes."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import grpc

from almacen.cluster.membership import Membership
from almacen.cluster.reconciliation import build_digest, plan_exchange
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.gossip_codec import (
    decode_digest,
    decode_membership,
    encode_membership,
)
from almacen.rpc.record_codec import message_to_record, record_to_message
from almacen.storage.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ClusterServicer(pb_grpc.ClusterServicer):
    def __init__(
        self,
        metadata_store: MetadataStore,
        node_id: str,
        membership: Membership | None = None,
    ) -> None:
        # Deliberately the *local* store, never the replicating wrapper: applying
        # a record received from a peer must not push it back out, or every
        # write would ping-pong around the cluster forever.
        self._metadata_store = metadata_store
        self._node_id = node_id
        self._membership = membership

    def ReplicateRecord(
        self, request: pb.FileRecordMsg, context: grpc.ServicerContext
    ) -> pb.ReplicateAck:
        try:
            record = message_to_record(request)
        except ValueError as error:
            # `abort` raises, but it is not typed NoReturn, so without the
            # explicit raise the reader (and the type checker) cannot tell that
            # `record` below is always bound. The guard also keeps the two
            # calling conventions consistent: tests may pass context=None.
            if context is not None:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, f"undecodable record: {error}"
                )
            raise

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

    def Gossip(
        self, request: pb.GossipDigest, context: grpc.ServicerContext
    ) -> pb.GossipDelta:
        """Answer a peer's digest: what we should send, and what we want back."""
        now = _utcnow()
        local_records = self._metadata_store.list_all()
        plan = plan_exchange(local_records, decode_digest(request.entries))

        membership: list[pb.MemberStatus] = []
        if self._membership is not None:
            # The message itself proves the caller is up, which makes failure
            # detection bidirectional for nothing.
            if request.from_node_id:
                self._membership.record_reachable(request.from_node_id, now)
            try:
                self._membership.merge(decode_membership(request.membership), now)
            except ValueError as error:
                # A malformed membership entry must not cost us the data half of
                # the exchange, which is the part that actually reconciles.
                logger.warning(
                    "ignoring malformed membership from %s: %s",
                    request.from_node_id or "unknown",
                    error,
                )
            membership = encode_membership(self._membership.snapshot(now))

        return pb.GossipDelta(
            records=[record_to_message(record) for record in plan.push],
            wanted_file_ids=[str(file_id) for file_id in plan.wanted],
            membership=membership,
        )
