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
