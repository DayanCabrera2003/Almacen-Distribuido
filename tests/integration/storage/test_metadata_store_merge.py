"""`merge_remote` combines a peer's record with the local one.

Phase 2 overwrote the local record with whatever arrived, so whichever update
landed last won and two nodes could disagree permanently. Merging instead means
the outcome does not depend on arrival order, which is the point of the CRDTs.
"""
from pathlib import Path

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


def test_a_record_for_an_unknown_file_is_stored_as_is(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(
        name="a.txt", content_hash="h1", node_id="node9", tags={"x"}
    )

    store.merge_remote(record)

    stored = store.get(record.file_id)
    assert stored is not None
    assert stored.name == "a.txt"
    assert stored.tags == {"x"}


def test_local_edits_survive_an_incoming_record_that_never_saw_them(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    base = FileRecord.new(
        name="a.txt", content_hash="h1", node_id="node1", tags={"shared"}
    )
    store.insert(base)

    # This node adds a tag; a peer, which never saw it, adds a different one.
    local = store.get(base.file_id)
    local.add_tag("local-only", "node1")
    store.update(local)

    from_peer = base.copy()
    from_peer.add_tag("remote-only", "node2")

    store.merge_remote(from_peer)

    assert store.get(base.file_id).tags == {"shared", "local-only", "remote-only"}


def test_a_causally_stale_record_does_not_undo_newer_local_work(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    base = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    store.insert(base)
    stale = base.copy()

    local = store.get(base.file_id)
    local.rename("newer.txt", "node1")
    store.update(local)

    store.merge_remote(stale)

    assert store.get(base.file_id).name == "newer.txt"


def test_merging_the_same_record_twice_changes_nothing(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = FileRecord.new(
        name="a.txt", content_hash="h1", node_id="node1", tags={"x"}
    )

    store.merge_remote(record)
    first = store.get(record.file_id)
    store.merge_remote(record)
    second = store.get(record.file_id)

    assert first.tags == second.tags
    assert first.vector_clock.counters == second.vector_clock.counters
    assert first.name == second.name


def test_a_tombstone_from_a_peer_applies_locally(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    base = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    store.insert(base)

    deleted_elsewhere = base.copy()
    deleted_elsewhere.mark_deleted("node2")

    store.merge_remote(deleted_elsewhere)

    assert store.get(base.file_id).tombstone is True
    assert store.list_live() == []


def test_a_delete_outlasts_a_concurrent_local_edit(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    base = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    store.insert(base)

    deleted_elsewhere = base.copy()
    deleted_elsewhere.mark_deleted("node2")

    local = store.get(base.file_id)
    local.add_tag("late-edit", "node1")
    store.update(local)

    store.merge_remote(deleted_elsewhere)

    merged = store.get(base.file_id)
    assert merged.tombstone is True
    # The edit is not lost, only invisible through the API while tombstoned.
    assert "late-edit" in merged.tags


def test_merge_is_order_independent(tmp_path: Path):
    """Two stores that receive the same two updates in opposite orders agree."""
    base = FileRecord.new(
        name="a.txt", content_hash="h1", node_id="node1", tags={"base"}
    )
    from_node2 = base.copy()
    from_node2.add_tag("two", "node2")
    from_node3 = base.copy()
    from_node3.add_tag("three", "node3")

    forward = MetadataStore(tmp_path / "forward.db")
    forward.insert(base.copy())
    forward.merge_remote(from_node2)
    forward.merge_remote(from_node3)

    backward = MetadataStore(tmp_path / "backward.db")
    backward.insert(base.copy())
    backward.merge_remote(from_node3)
    backward.merge_remote(from_node2)

    assert forward.get(base.file_id).tags == backward.get(base.file_id).tags
    assert forward.get(base.file_id).tags == {"base", "two", "three"}
