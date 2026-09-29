"""Every byte of CRDT state must survive a store/load cycle.

Asserting only the derived values (name, tags, tombstone) leaves the metadata
that drives merging — the LWW timestamps and node ids, the vector clock, the
removed occurrences — completely unchecked. Verified by mutation: persisting an
empty vector clock, or dropping the LWW node id, both passed the whole suite
before these tests existed. A node restarted mid-partition would then merge to
the wrong answer, with nothing to indicate why.
"""
from datetime import timedelta
from pathlib import Path

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


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


def _busy_record() -> FileRecord:
    """A record with history on three nodes: removals, re-adds, a rename."""
    record = FileRecord.new(
        name="a.txt", content_hash="h1", node_id="node1", tags={"draft", "keep"}
    )
    record.add_tag("review", "node2")
    record.remove_tag("draft", "node3")
    record.add_tag("draft", "node2")
    record.rename("b.txt", "node3")
    record.update_content("h2", "node1")
    return record


def test_insert_then_get_preserves_the_whole_crdt_state(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = _busy_record()

    store.insert(record)

    assert crdt_state(store.get(record.file_id)) == crdt_state(record)


def test_update_then_get_preserves_the_whole_crdt_state(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = _busy_record()
    store.insert(record)

    record.add_tag("later", "node1")
    record.remove_tag("keep", "node2")
    store.update(record)

    assert crdt_state(store.get(record.file_id)) == crdt_state(record)


def test_list_live_preserves_the_whole_crdt_state(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = _busy_record()
    store.insert(record)

    (restored,) = store.list_live()

    assert crdt_state(restored) == crdt_state(record)


def test_merge_remote_persists_exactly_what_it_returned(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    local = _busy_record()
    store.insert(local)

    from_peer = local.copy()
    from_peer.add_tag("from-peer", "node4")
    # A differing created_at is the one field where the merge result can
    # legitimately differ from what is already stored, so it is the case that
    # actually exercises whether the persist writes back the merge.
    from_peer.created_at = local.created_at - timedelta(days=1)

    returned = store.merge_remote(from_peer)

    assert crdt_state(store.get(local.file_id)) == crdt_state(returned), (
        "the persisted record must match the merge result, or the next merge "
        "starts from a different state than the one just computed"
    )


def test_a_reloaded_record_merges_identically_to_the_original(tmp_path: Path):
    store = MetadataStore(tmp_path / "m.db")
    record = _busy_record()
    store.insert(record)

    from_peer = record.copy()
    from_peer.add_tag("concurrent", "node5")
    from_peer.remove_tag("review", "node5")

    in_memory = record.merged(from_peer)
    reloaded = store.get(record.file_id).merged(from_peer)

    assert crdt_state(reloaded) == crdt_state(in_memory)
