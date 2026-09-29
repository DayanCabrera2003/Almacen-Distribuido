"""The responder's half of a push-pull exchange, over a real channel."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
import pytest

from almacen.cluster.membership import Membership, NodeState
from almacen.cluster.reconciliation import build_digest
from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.cluster_servicer import ClusterServicer
from almacen.rpc.gossip_codec import decode_membership, encode_digest, encode_membership
from almacen.storage.metadata_store import MetadataStore

PEERS = ("node1", "node2", "node3")
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def metadata_store(tmp_path: Path) -> MetadataStore:
    return MetadataStore(tmp_path / "metadata.db")


@pytest.fixture
def membership() -> Membership:
    return Membership("node2", PEERS, timedelta(seconds=5))


@pytest.fixture
def stub(metadata_store, membership, grpc_server_factory):
    address = grpc_server_factory(
        lambda server: pb_grpc.add_ClusterServicer_to_server(
            ClusterServicer(metadata_store, node_id="node2", membership=membership),
            server,
        )
    )
    channel = grpc.insecure_channel(address)
    yield pb_grpc.ClusterStub(channel)
    channel.close()


def digest_request(records=(), view=None, from_node_id="node1") -> pb.GossipDigest:
    return pb.GossipDigest(
        from_node_id=from_node_id,
        entries=encode_digest(build_digest(list(records))),
        membership=encode_membership(view or {}),
    )


def test_a_peer_that_knows_nothing_is_sent_everything(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    metadata_store.insert(record)

    delta = stub.Gossip(digest_request())

    assert [m.file_id for m in delta.records] == [str(record.file_id)]
    assert list(delta.wanted_file_ids) == []


def test_a_file_the_responder_lacks_is_requested(stub):
    theirs = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")

    delta = stub.Gossip(digest_request([theirs]))

    assert list(delta.records) == []
    assert list(delta.wanted_file_ids) == [str(theirs.file_id)]


def test_identical_state_produces_an_empty_delta(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    metadata_store.insert(record)

    delta = stub.Gossip(digest_request([record]))

    assert list(delta.records) == [] and list(delta.wanted_file_ids) == []


def test_a_newer_remote_version_is_requested_not_pushed(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    metadata_store.insert(record)
    ahead = record.copy()
    ahead.rename("newer.txt", "node1")

    delta = stub.Gossip(digest_request([ahead]))

    assert list(delta.records) == []
    assert list(delta.wanted_file_ids) == [str(record.file_id)]


def test_concurrent_versions_travel_both_ways(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    metadata_store.insert(record)
    local = metadata_store.mutate(record.file_id, lambda r: r.rename("here", "node2"))
    elsewhere = record.copy()
    elsewhere.rename("there", "node1")

    delta = stub.Gossip(digest_request([elsewhere]))

    assert [m.file_id for m in delta.records] == [str(local.file_id)]
    assert list(delta.wanted_file_ids) == [str(local.file_id)]


def test_tombstoned_records_are_advertised(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    record.mark_deleted("node2")
    metadata_store.insert(record)

    delta = stub.Gossip(digest_request())

    assert [m.file_id for m in delta.records] == [str(record.file_id)]


def test_the_caller_is_recorded_as_reachable(stub, membership):
    membership.record_unreachable("node1", T0)
    assert membership.state_of("node1", T0) is NodeState.SUSPECT

    stub.Gossip(digest_request(from_node_id="node1"))

    assert membership.state_of("node1", datetime.now(timezone.utc)) is NodeState.ALIVE


def test_membership_travels_in_both_directions(stub, membership):
    accuser = Membership("node1", PEERS, timedelta(seconds=5))
    accuser.record_unreachable("node3", T0)

    delta = stub.Gossip(digest_request(view=accuser.snapshot(T0)))

    now = datetime.now(timezone.utc)
    assert membership.state_of("node3", now) is NodeState.SUSPECT
    assert "node2" in decode_membership(delta.membership)


def test_a_malformed_membership_entry_does_not_lose_the_data_exchange(
    stub, metadata_store
):
    # The data half is the part that actually reconciles; a bad membership entry
    # must not cost it.
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node2")
    metadata_store.insert(record)

    request = digest_request()
    request.membership.append(pb.MemberStatus(node_id="ghost", state=1, incarnation=0))

    delta = stub.Gossip(request)

    assert [m.file_id for m in delta.records] == [str(record.file_id)]
