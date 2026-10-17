import sqlite3
import threading
import uuid
from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


@pytest.fixture
def store(tmp_path: Path) -> MetadataStore:
    return MetadataStore(tmp_path / "metadata.db")


def test_insert_then_get_returns_equivalent_record(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x", "y"})

    store.insert(record)
    fetched = store.get(record.file_id)

    assert fetched is not None
    assert fetched.file_id == record.file_id
    assert fetched.name == "a.txt"
    assert fetched.content_hash == "h1"
    assert fetched.tags == {"x", "y"}
    assert fetched.tombstone is False


def test_get_returns_none_for_unknown_id(store: MetadataStore):
    assert store.get(uuid.uuid4()) is None


def test_update_persists_renamed_and_retagged_record(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x"})
    store.insert(record)

    record.rename("b.txt", "node1")
    record.add_tag("y", "node1")
    record.remove_tag("x", "node1")
    store.update(record)

    fetched = store.get(record.file_id)
    assert fetched.name == "b.txt"
    assert fetched.tags == {"y"}


def test_update_persists_tombstone(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    store.insert(record)

    record.mark_deleted("node1")
    store.update(record)

    fetched = store.get(record.file_id)
    assert fetched.tombstone is True
    assert fetched.tombstone_at is not None


def test_data_survives_reopening_the_same_db_file(tmp_path: Path):
    db_path = tmp_path / "metadata.db"
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x"})

    MetadataStore(db_path).insert(record)
    reopened = MetadataStore(db_path)

    fetched = reopened.get(record.file_id)
    assert fetched is not None
    assert fetched.tags == {"x"}


def test_update_on_unknown_file_id_raises_instead_of_orphaning_tags(store: MetadataStore):
    ghost = FileRecord.new(name="ghost.txt", content_hash="h1", node_id="node1", tags={"orphan"})

    with pytest.raises(sqlite3.IntegrityError):
        store.update(ghost)


def test_concurrent_inserts_from_multiple_threads_all_persist_correctly(store: MetadataStore):
    thread_count = 20
    records = [
        FileRecord.new(name=f"file-{i}.txt", content_hash=f"h{i}", node_id="node1", tags={f"tag-{i}", "shared"})
        for i in range(thread_count)
    ]
    errors: list[BaseException] = []

    def _insert(record: FileRecord) -> None:
        try:
            store.insert(record)
        except BaseException as exc:  # noqa: BLE001 - capture for assertion in main thread
            errors.append(exc)

    threads = [threading.Thread(target=_insert, args=(record,)) for record in records]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    for i, record in enumerate(records):
        fetched = store.get(record.file_id)
        assert fetched is not None
        assert fetched.tags == {f"tag-{i}", "shared"}


def test_list_live_excludes_tombstoned_records(store: MetadataStore):
    live = FileRecord.new(name="live.txt", content_hash="h1", node_id="node1")
    deleted = FileRecord.new(name="deleted.txt", content_hash="h2", node_id="node1")
    deleted.mark_deleted("node1")
    store.insert(live)
    store.insert(deleted)

    results = store.list_live()

    assert {r.file_id for r in results} == {live.file_id}


def test_list_all_includes_tombstoned_records(tmp_path: Path):
    # Anti-entropy needs them: a tombstone that never propagates leaves a peer
    # serving a file the rest of the cluster considers deleted.
    store = MetadataStore(tmp_path / "m.db")
    live = FileRecord.new(name="live.txt", content_hash="h1", node_id="node1")
    deleted = FileRecord.new(name="gone.txt", content_hash="h2", node_id="node1")
    deleted.mark_deleted("node1")
    store.insert(live)
    store.insert(deleted)

    assert {r.file_id for r in store.list_all()} == {live.file_id, deleted.file_id}
    assert [r.file_id for r in store.list_live()] == [live.file_id]


def test_purge_removes_the_record_and_its_tag_state(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"x"})
    record.remove_tag("x", "node1")
    record.add_tag("y", "node1")
    store.insert(record)

    assert store.purge(record.file_id) is True

    assert store.get(record.file_id) is None
    assert store.list_all() == []
    # The OR-Set rows go too: left behind, they would be inherited by nothing
    # and leak forever.
    for table in ("file_tag_adds", "file_tag_removes"):
        count = store._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count == 0, table


def test_purge_of_an_unknown_record_reports_false(store: MetadataStore):
    assert store.purge(uuid.uuid4()) is False


def test_purge_leaves_other_records_alone(store: MetadataStore):
    kept = FileRecord.new(name="kept.txt", content_hash="h1", node_id="node1", tags={"t"})
    gone = FileRecord.new(name="gone.txt", content_hash="h2", node_id="node1", tags={"t"})
    store.insert(kept)
    store.insert(gone)

    store.purge(gone.file_id)

    assert [r.file_id for r in store.list_all()] == [kept.file_id]
    assert store.get(kept.file_id).tags == {"t"}


def test_reference_counts_include_tombstoned_records(store: MetadataStore):
    # A tombstoned record still points at its content until it is purged; that
    # is what makes a deleted file's blob wait two grace TTLs, not one.
    live = FileRecord.new(name="a.txt", content_hash="shared", node_id="node1")
    twin = FileRecord.new(name="b.txt", content_hash="shared", node_id="node1")
    deleted = FileRecord.new(name="c.txt", content_hash="only", node_id="node1")
    deleted.mark_deleted("node1")
    for record in (live, twin, deleted):
        store.insert(record)

    assert store.reference_counts() == {"shared": 2, "only": 1}


def test_reference_counts_follow_a_content_repoint(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="old", node_id="node1")
    store.insert(record)

    store.mutate(record.file_id, lambda r: r.update_content("new", "node1"))

    assert store.reference_counts() == {"new": 1}
