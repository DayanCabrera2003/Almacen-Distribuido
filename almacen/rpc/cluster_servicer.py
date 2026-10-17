# almacen/rpc/cluster_servicer.py
"""Receives metadata replicated from peer nodes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import grpc
import structlog

from almacen.cluster.conflicts import log_if_concurrent
from almacen.cluster.gc import is_expired_tombstone
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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ClusterServicer(pb_grpc.ClusterServicer):
    def __init__(
        self,
        metadata_store: MetadataStore,
        node_id: str,
        membership: Membership | None = None,
        grace_ttl: timedelta | None = None,
    ) -> None:
        # Deliberately the *local* store, never the replicating wrapper: applying
        # a record received from a peer must not push it back out, or every
        # write would ping-pong around the cluster forever.
        self._metadata_store = metadata_store
        self._node_id = node_id
        self._membership = membership
        # The node's GC grace TTL. When set, a tombstone already past it is not
        # adopted by a store that does not hold the record: a node gossips
        # before it collects, so without this a peer that has not purged yet
        # hands the record straight back to one that has, and it bounces round
        # the cluster indefinitely. None (tests of other concerns) adopts all.
        self._grace_ttl = grace_ttl
        self._log = structlog.get_logger(__name__, node_id=node_id)

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

        log_if_concurrent(
            self._log,
            self._metadata_store.get(record.file_id),
            record,
            sender=context.peer() if context is not None else "unknown",
        )

        # Merge, never overwrite. An incoming record is one replica's view, not
        # the truth: the local copy may hold edits the sender never saw.
        self._metadata_store.merge_remote(record, refuse_unknown=self._refuse_unknown)
        self._log.debug("record_merged", file_id=str(record.file_id))
        return pb.ReplicateAck(applied=True)

    def _refuse_unknown(self, record) -> bool:
        if self._grace_ttl is None:
            return False
        return is_expired_tombstone(record, _utcnow(), self._grace_ttl)

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
                self._log.warning(
                    "malformed_membership_ignored",
                    sender=request.from_node_id or "unknown",
                    error=str(error),
                )
            membership = encode_membership(self._membership.snapshot(now))

        return pb.GossipDelta(
            records=[record_to_message(record) for record in plan.push],
            wanted_file_ids=[str(file_id) for file_id in plan.wanted],
            membership=membership,
        )
