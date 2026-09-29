import uuid

import pytest

from almacen.domain.file_record import FileRecord
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc.record_codec import message_to_record, record_to_message


def test_round_trips_a_live_record():
    original = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x", "y"})

    restored = message_to_record(record_to_message(original))

    assert restored.file_id == original.file_id
    assert restored.name == original.name
    assert restored.content_hash == original.content_hash
    assert restored.tags == original.tags
    assert restored.tombstone is False
    assert restored.tombstone_at is None
    assert restored.created_at == original.created_at
    assert restored.updated_at == original.updated_at


def test_round_trips_a_tombstoned_record():
    original = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    original.mark_deleted("node1")

    restored = message_to_record(record_to_message(original))

    assert restored.tombstone is True
    assert restored.tombstone_at == original.tombstone_at


def test_round_trips_a_record_with_no_tags():
    original = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    restored = message_to_record(record_to_message(original))
    assert restored.tags == set()


def test_timestamps_survive_as_timezone_aware_values():
    original = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    restored = message_to_record(record_to_message(original))
    assert restored.created_at.tzinfo is not None
    assert restored.created_at == original.created_at


def test_rejects_a_message_without_timestamps():
    # A record with no created_at would violate the NOT NULL schema on upsert, so
    # it has to be rejected at the edge instead.
    incomplete = pb.FileRecordMsg(file_id=str(uuid.uuid4()), name="a.txt")
    with pytest.raises(ValueError):
        message_to_record(incomplete)


def crdt_state(record: FileRecord) -> tuple:
    """Everything that affects a future merge."""
    return (
        record.file_id,
        record.name_register,
        record.content_hash_register,
        record.tombstone_register,
        frozenset(record.tag_set.adds),
        frozenset(record.tag_set.removes),
        tuple(sorted(record.vector_clock.counters.items())),
        record.created_at,
        record.updated_at,
    )


def test_the_wire_format_preserves_the_whole_crdt_state():
    """Comparing only the derived values leaves the merge metadata unchecked.

    Verified by mutation: blanking `name_node` or the vector clock in
    `record_to_message` passed the entire suite before this test existed,
    even though either one destroys merging across the network.
    """
    record = FileRecord.new(
        name="a.txt", content_hash="h1", node_id="node1", tags={"draft", "keep"}
    )
    record.add_tag("review", "node2")
    record.remove_tag("draft", "node3")
    record.rename("b.txt", "node3")

    assert crdt_state(message_to_record(record_to_message(record))) == crdt_state(
        record
    )


def test_a_decoded_record_merges_identically_to_the_original():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    record.add_tag("x", "node2")

    from_peer = record.copy()
    from_peer.add_tag("y", "node3")

    decoded = message_to_record(record_to_message(record))
    assert crdt_state(decoded.merged(from_peer)) == crdt_state(
        record.merged(from_peer)
    )
