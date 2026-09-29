# almacen/rpc/gossip_codec.py
"""Wire conversions for the gossip exchange.

Both the servicer and the gossip client need all four of these, so they live in
one place rather than being written twice with a chance of disagreeing.
"""
from __future__ import annotations

import uuid
from collections.abc import Iterable
from datetime import datetime

from almacen.cluster.membership import NodeState, NodeStatus
from almacen.cluster.reconciliation import Digest
from almacen.crdt.vector_clock import VectorClock
from almacen.rpc import cluster_pb2 as pb


def encode_digest(digest: Digest) -> list[pb.DigestEntry]:
    # Sorted so a given state always encodes identically, which makes messages
    # comparable in tests and diffable in logs.
    return [
        pb.DigestEntry(file_id=str(file_id), vector_clock=dict(clock.counters))
        for file_id, clock in sorted(digest.items(), key=lambda item: str(item[0]))
    ]


def decode_digest(entries: Iterable[pb.DigestEntry]) -> Digest:
    return {
        uuid.UUID(entry.file_id): VectorClock(dict(entry.vector_clock))
        for entry in entries
    }


def encode_membership(view: dict[str, NodeStatus]) -> list[pb.MemberStatus]:
    return [
        pb.MemberStatus(
            node_id=node_id,
            state=int(status.state),
            incarnation=status.incarnation,
            since=status.since.isoformat(),
        )
        for node_id, status in sorted(view.items())
    ]


def decode_membership(statuses: Iterable[pb.MemberStatus]) -> dict[str, NodeStatus]:
    view: dict[str, NodeStatus] = {}
    for status in statuses:
        # proto3 defaults an unset string to "", and a NodeStatus holding None
        # for `since` raises inside the subtraction in `state_of`. The codec is
        # the trust boundary, so a malformed peer is rejected here rather than
        # crashing a gossip round later.
        if not status.since:
            raise ValueError(f"membership entry for {status.node_id!r} has no 'since'")
        since = datetime.fromisoformat(status.since)
        if since.tzinfo is None:
            raise ValueError(
                f"membership entry for {status.node_id!r} has a naive 'since'"
            )
        view[status.node_id] = NodeStatus(
            state=NodeState(status.state),
            incarnation=status.incarnation,
            since=since,
        )
    return view
