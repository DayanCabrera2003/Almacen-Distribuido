"""Write timestamps must never go backwards, whatever the wall clock says.

The single-value fields resolve by last-writer-wins on `(timestamp, node_id)`.
If a write can be stamped at or before something the record already carries,
three things break, and none of them raise an error:

- two writes by the same node in one clock tick get identical ranks, so merge
  stops being commutative;
- a delete issued on a node whose clock is behind loses to the record's own
  creation timestamp, and the file comes back to life;
- a write made *after* observing a future-dated remote write loses to it, so
  the user's change silently disappears.

The fix is to derive each write's timestamp from the newest one the record has
already seen, so it is monotonic per record rather than per machine.
"""
from datetime import datetime, timedelta, timezone

from almacen.crdt.lww_register import LWWRegister
from almacen.domain.file_record import FileRecord


def test_two_writes_by_one_node_get_distinct_ranks():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")

    record.rename("first.txt", "node1")
    first = record.name_register
    record.rename("second.txt", "node1")
    second = record.name_register

    assert second.timestamp > first.timestamp
    # Same rank would make merge order-dependent: a.merged(b) != b.merged(a).
    assert first.merged(second).value == second.merged(first).value == "second.txt"


def test_a_delete_outranks_the_record_it_deletes_even_with_a_lagging_clock():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    # Simulate a record created by a node whose clock is well ahead of ours.
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    record.tombstone_register = LWWRegister(False, future, "node1")
    record.updated_at = future

    record.mark_deleted("node2")

    assert record.tombstone_register.timestamp > future
    assert record.tombstone is True


def test_a_delete_survives_merging_with_the_version_it_deleted():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    record.tombstone_register = LWWRegister(False, future, "node1")
    record.updated_at = future
    before_delete = record.copy()

    record.mark_deleted("node2")

    assert record.merged(before_delete).tombstone is True
    assert before_delete.merged(record).tombstone is True


def test_a_write_outranks_a_future_dated_remote_write_it_observed():
    # node2's clock is fast. node1 merges node2's rename, then the user renames
    # again on node1. The user's change must win: it happened after.
    local = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    from_fast_peer = local.copy()
    from_fast_peer.name_register = LWWRegister(
        "from-fast-peer.txt", datetime.now(timezone.utc) + timedelta(seconds=30), "node2"
    )
    from_fast_peer.vector_clock.increment("node2")

    after_merge = local.merged(from_fast_peer)
    after_merge.rename("what-the-user-typed.txt", "node1")

    assert after_merge.name == "what-the-user-typed.txt"
    # And it stays that way once the peer's version comes round again.
    assert after_merge.merged(from_fast_peer).name == "what-the-user-typed.txt"


def test_updated_at_never_moves_backwards():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    future = datetime.now(timezone.utc) + timedelta(minutes=5)
    record.updated_at = future

    record.add_tag("x", "node1")

    assert record.updated_at > future


def test_normal_writes_still_track_the_wall_clock():
    # The monotonic floor must not detach timestamps from real time when
    # nothing is skewed, or every record would drift into the future.
    before = datetime.now(timezone.utc)
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    record.rename("b.txt", "node1")
    after = datetime.now(timezone.utc)

    assert before <= record.name_register.timestamp <= after
