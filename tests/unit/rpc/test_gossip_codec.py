from datetime import datetime, timezone

import pytest

from almacen.cluster.membership import NodeState, NodeStatus
from almacen.crdt.vector_clock import VectorClock
from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc.gossip_codec import (
    decode_digest,
    decode_membership,
    encode_digest,
    encode_membership,
)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_a_digest_round_trips():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    record.rename("b.txt", "node2")
    digest = {record.file_id: record.vector_clock}

    assert decode_digest(encode_digest(digest)) == digest


def test_an_empty_digest_round_trips():
    assert decode_digest(encode_digest({})) == {}


def test_a_membership_view_round_trips():
    view = {
        "node1": NodeStatus(NodeState.ALIVE, 0, T0),
        "node2": NodeStatus(NodeState.SUSPECT, 3, T0),
        "node3": NodeStatus(NodeState.DEAD, 7, T0),
    }
    assert decode_membership(encode_membership(view)) == view


def test_encoding_is_deterministic():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    other = FileRecord.new(name="b.txt", content_hash="h", node_id="node1")
    digest = {record.file_id: record.vector_clock, other.file_id: other.vector_clock}

    assert encode_digest(digest) == encode_digest(digest)


def test_a_membership_entry_without_a_since_is_rejected():
    # proto3 defaults it to "", and a NodeStatus holding None for `since` raises
    # inside state_of's subtraction — long after the malformed peer is gone.
    with pytest.raises(ValueError, match="since"):
        decode_membership([pb.MemberStatus(node_id="node2", state=1, incarnation=0)])


def test_a_naive_since_is_rejected():
    with pytest.raises(ValueError, match="naive"):
        decode_membership(
            [
                pb.MemberStatus(
                    node_id="node2", state=0, incarnation=0, since="2026-01-01T00:00:00"
                )
            ]
        )


def test_a_clock_with_several_components_survives():
    clock = VectorClock({"node1": 3, "node2": 1, "node3": 9})
    import uuid

    file_id = uuid.uuid4()
    assert decode_digest(encode_digest({file_id: clock}))[file_id] == clock
