"""Read-modify-write on a record must not lose a concurrent update.

`update()` is a blind write: it replaces a file's whole tag set from whatever
snapshot the caller read. Two callers that read the same snapshot and then write
will each write their own version, and the second silently erases the first's
change — a write this node already acknowledged with a 200.

This is not the divergence CRDTs address in Phase 3: it happens on one node,
with one database, and no replication involved. `mutate()` closes it by holding
the store's lock across the whole read-modify-write.
"""
import threading
import uuid
from pathlib import Path

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore

# Enough concurrent writers that the lost-update window is hit reliably: with a
# blind get()/update() pair this loses tags on essentially every run.
WRITER_COUNT = 16


def _run_concurrently(targets) -> None:
    threads = [threading.Thread(target=t) for t in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive(), "a writer thread deadlocked"


def test_concurrent_tag_additions_do_not_lose_each_other(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"base"})
    store.insert(record)

    def add(tag: str):
        return lambda: store.mutate(record.file_id, lambda stored: stored.add_tag(tag))

    added = {f"tag{i}" for i in range(WRITER_COUNT)}
    _run_concurrently([add(tag) for tag in sorted(added)])

    assert store.get(record.file_id).tags == {"base"} | added


def test_a_concurrent_rename_and_tag_changes_all_survive(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="before.txt", content_hash="h1", tags={"base"})
    store.insert(record)

    def rename():
        store.mutate(record.file_id, lambda stored: stored.rename("after.txt"))

    def add(tag: str):
        return lambda: store.mutate(record.file_id, lambda stored: stored.add_tag(tag))

    added = {f"tag{i}" for i in range(WRITER_COUNT)}
    _run_concurrently([rename] + [add(tag) for tag in sorted(added)])

    stored = store.get(record.file_id)
    assert stored.name == "after.txt", "the rename was clobbered by a tag write"
    assert stored.tags == {"base"} | added


def test_concurrent_tag_removals_do_not_resurrect_each_other(tmp_path: Path):
    initial = {f"tag{i}" for i in range(WRITER_COUNT)}
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"base"} | initial)
    store.insert(record)

    def remove(tag: str):
        return lambda: store.mutate(
            record.file_id, lambda stored: stored.remove_tag(tag)
        )

    _run_concurrently([remove(tag) for tag in sorted(initial)])

    assert store.get(record.file_id).tags == {"base"}


def test_mutate_returns_none_for_an_unknown_file(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    assert store.mutate(uuid.uuid4(), lambda record: record.rename("x")) is None


def test_mutate_returns_the_updated_record(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="a.txt", content_hash="h1")
    store.insert(record)

    returned = store.mutate(record.file_id, lambda stored: stored.rename("b.txt"))

    assert returned is not None and returned.name == "b.txt"
    assert store.get(record.file_id).name == "b.txt"
