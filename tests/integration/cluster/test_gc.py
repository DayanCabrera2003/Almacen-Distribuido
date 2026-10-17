"""The collector against real stores, driven by an explicit `now` — no sleeping."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from almacen.cluster.gc import collect
from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.gc_state import BlobGCState
from almacen.storage.metadata_store import MetadataStore

GRACE = timedelta(hours=1)
JUST = timedelta(seconds=1)


class Node:
    """One node's local state: the three stores the collector works on."""

    def __init__(self, root: Path) -> None:
        self.store = MetadataStore(root / "metadata.db")
        self.blobs = BlobStore(root / "blobs")
        self.gc_state = BlobGCState(root / "gc_state.db")

    def collect(self, now: datetime, budget: int = 100, should_stop=lambda: False):
        return collect(
            store=self.store,
            blob_store=self.blobs,
            gc_state=self.gc_state,
            now=now,
            grace=GRACE,
            budget=budget,
            should_stop=should_stop,
        )

    def upload(self, content: bytes, name: str = "a.txt") -> FileRecord:
        record = FileRecord.new(
            name=name, content_hash=self.blobs.put(content), node_id="node1"
        )
        self.store.insert(record)
        return record

    def delete(self, record: FileRecord) -> datetime:
        deleted = self.store.mutate(record.file_id, lambda r: r.mark_deleted("node1"))
        return deleted.tombstone_at

    def close(self) -> None:
        self.store.close()
        self.gc_state.close()


@pytest.fixture
def node(tmp_path: Path):
    n = Node(tmp_path)
    yield n
    n.close()


def test_a_live_file_and_its_blob_are_left_alone(node: Node):
    record = node.upload(b"keep me")

    node.collect(datetime.now(timezone.utc) + 10 * GRACE)

    assert node.store.get(record.file_id) is not None
    assert node.blobs.exists(record.content_hash)
    assert node.gc_state.candidates() == {}


def test_a_young_tombstone_is_kept_and_still_holds_its_blob(node: Node):
    record = node.upload(b"recently deleted")
    deleted_at = node.delete(record)

    node.collect(deleted_at + GRACE - JUST)

    assert node.store.get(record.file_id) is not None
    # Still a referrer, so the blob's own clock has not even started.
    assert node.gc_state.candidates() == {}


def test_a_deleted_files_blob_outlives_its_record_by_one_more_ttl(node: Node):
    record = node.upload(b"deleted content")
    deleted_at = node.delete(record)

    # First TTL: the record is purged, and the blob becomes a candidate in the
    # same round — but is not deleted yet.
    purged_at = deleted_at + GRACE
    result = node.collect(purged_at)
    assert result.purged == 1
    assert node.store.get(record.file_id) is None
    assert node.gc_state.candidates() == {record.content_hash: purged_at}
    assert node.blobs.exists(record.content_hash)

    # Just short of the second TTL: still there.
    node.collect(purged_at + GRACE - JUST)
    assert node.blobs.exists(record.content_hash)

    # Second TTL: gone, and so is its candidacy.
    result = node.collect(purged_at + GRACE)
    assert result.blobs_deleted == 1
    assert not node.blobs.exists(record.content_hash)
    assert node.gc_state.candidates() == {}


def test_a_patch_orphaned_blob_is_collected_after_one_ttl(node: Node):
    record = node.upload(b"version one")
    old_hash = record.content_hash
    new_hash = node.blobs.put(b"version two")
    node.store.mutate(record.file_id, lambda r: r.update_content(new_hash, "node1"))

    repointed_at = datetime.now(timezone.utc)
    node.collect(repointed_at)
    assert node.gc_state.candidates() == {old_hash: repointed_at}
    assert node.blobs.exists(old_hash)

    node.collect(repointed_at + GRACE)

    assert not node.blobs.exists(old_hash)
    # The record and its current content are untouched.
    assert node.store.get(record.file_id).content_hash == new_hash
    assert node.blobs.exists(new_hash)


def test_a_blob_that_regains_a_referrer_survives(node: Node):
    record = node.upload(b"shared bytes")
    deleted_at = node.delete(record)
    purged_at = deleted_at + GRACE
    node.collect(purged_at)
    assert record.content_hash in node.gc_state.candidates()

    # Identical bytes uploaded again: same content hash, a new referrer.
    again = node.upload(b"shared bytes", name="again.txt")
    assert again.content_hash == record.content_hash

    node.collect(purged_at + GRACE)

    assert node.blobs.exists(record.content_hash)
    assert node.gc_state.candidates() == {}


def test_a_stray_blob_with_no_record_is_collected_after_one_ttl(node: Node):
    stray = node.blobs.put(b"metadata never landed")
    first_seen = datetime.now(timezone.utc)

    node.collect(first_seen)
    assert node.blobs.exists(stray)

    node.collect(first_seen + GRACE)
    assert not node.blobs.exists(stray)


def test_the_budget_caps_one_round(node: Node):
    records = [node.upload(f"file {i}".encode(), name=f"{i}.txt") for i in range(3)]
    latest = max(node.delete(record) for record in records)

    first = node.collect(latest + GRACE, budget=2)
    assert first.purged == 2
    assert len(node.store.list_all()) == 1

    # The remainder is picked up next round: GC is a loop, not a one-shot.
    second = node.collect(latest + GRACE, budget=2)
    assert second.purged == 1
    assert node.store.list_all() == []


def test_the_budget_also_caps_blob_deletions(node: Node):
    strays = {node.blobs.put(f"stray {i}".encode()) for i in range(3)}
    first_seen = datetime.now(timezone.utc)
    node.collect(first_seen)

    result = node.collect(first_seen + GRACE, budget=2)

    assert result.blobs_deleted == 2
    assert len(node.blobs.list_hashes() & strays) == 1


def test_a_stop_signal_interrupts_the_round(node: Node):
    records = [node.upload(f"file {i}".encode(), name=f"{i}.txt") for i in range(3)]
    latest = max(node.delete(record) for record in records)
    calls = []

    def stop_after_first() -> bool:
        calls.append(None)
        return len(calls) > 1

    result = node.collect(latest + GRACE, should_stop=stop_after_first)

    assert result.purged == 1
    assert len(node.store.list_all()) == 2


def test_a_stop_signal_before_the_round_does_nothing(node: Node):
    record = node.upload(b"x")
    deleted_at = node.delete(record)

    result = node.collect(deleted_at + GRACE, should_stop=lambda: True)

    assert (result.purged, result.blobs_deleted) == (0, 0)
    assert node.store.get(record.file_id) is not None
