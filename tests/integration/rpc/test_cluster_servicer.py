from pathlib import Path

import grpc
import pytest
from structlog.testing import capture_logs

from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.cluster_servicer import ClusterServicer
from almacen.rpc.record_codec import record_to_message
from almacen.storage.metadata_store import MetadataStore


@pytest.fixture
def metadata_store(tmp_path: Path) -> MetadataStore:
    return MetadataStore(tmp_path / "metadata.db")


@pytest.fixture
def stub(metadata_store: MetadataStore, grpc_server_factory):
    address = grpc_server_factory(
        lambda server: pb_grpc.add_ClusterServicer_to_server(
            ClusterServicer(metadata_store, node_id="node2"), server
        )
    )
    channel = grpc.insecure_channel(address)
    yield pb_grpc.ClusterStub(channel)
    channel.close()


def test_replicate_record_applies_a_new_record(stub, metadata_store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x"})

    ack = stub.ReplicateRecord(record_to_message(record))

    assert ack.applied is True
    stored = metadata_store.get(record.file_id)
    assert stored is not None
    assert stored.name == "a.txt"
    assert stored.tags == {"x"}


def test_replicate_record_merges_into_a_known_record(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x"})
    metadata_store.insert(record)

    record.rename("renamed.txt", "node1")
    stub.ReplicateRecord(record_to_message(record))

    stored = metadata_store.get(record.file_id)
    assert stored is not None and stored.name == "renamed.txt"


def test_replicate_record_propagates_a_tombstone(stub, metadata_store):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    metadata_store.insert(record)

    record.mark_deleted("node1")
    stub.ReplicateRecord(record_to_message(record))

    assert metadata_store.list_live() == []


def test_replicate_record_rejects_a_malformed_file_id(stub):
    with pytest.raises(grpc.RpcError) as error:
        stub.ReplicateRecord(pb.FileRecordMsg(file_id="not-a-uuid", name="a.txt"))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_ping_identifies_the_responding_node(stub):
    response = stub.Ping(pb.PingRequest(from_node_id="node1"))
    assert response.node_id == "node2"


def test_a_genuine_conflict_is_logged_with_both_vector_clocks(stub, metadata_store):
    """Spec §9 requires conflicts be surfaced, not silently resolved.

    Last-writer-wins discards one of two concurrent writes. The vector clock is
    what tells a genuine conflict from a routine causally-newer update, and the
    warning is the only place that distinction becomes visible — so it needs a
    test, or the clock could be dropped on the wire with nothing noticing.
    """
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    metadata_store.insert(base)

    local = metadata_store.mutate(base.file_id, lambda r: r.rename("local", "node1"))
    assert local is not None

    from_peer = base.copy()
    from_peer.rename("remote", "node2")

    with capture_logs() as logs:
        stub.ReplicateRecord(record_to_message(from_peer))

    conflicts = [e for e in logs if e["event"] == "concurrent_update_resolved"]
    assert conflicts, "a concurrent update must be logged"
    [conflict] = conflicts
    assert conflict["log_level"] == "warning"
    assert conflict["node_id"] == "node2"
    assert conflict["file_id"] == str(base.file_id)
    # Both vector clocks belong in the event, or the record is unauditable.
    assert conflict["local_clock"] == local.vector_clock.counters
    assert conflict["incoming_clock"] == from_peer.vector_clock.counters
    assert conflict["resolution"] == "last_writer_wins"


def test_a_causally_newer_update_is_not_logged_as_a_conflict(stub, metadata_store):
    # False positives would make the conflict log useless. An update that
    # descends from the local copy is routine, not a conflict.
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    metadata_store.insert(base)

    newer = base.copy()
    newer.rename("newer", "node1")

    with capture_logs() as logs:
        stub.ReplicateRecord(record_to_message(newer))

    assert not [
        e for e in logs if e["event"] == "concurrent_update_resolved"
    ], "a causally newer update is not a conflict"
