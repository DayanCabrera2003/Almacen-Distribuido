import uuid
from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


@pytest.fixture
def store(tmp_path: Path) -> MetadataStore:
    return MetadataStore(tmp_path / "metadata.db")


def test_insert_then_get_returns_equivalent_record(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x", "y"})

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
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})
    store.insert(record)

    record.rename("b.txt")
    record.add_tag("y")
    record.remove_tag("x")
    store.update(record)

    fetched = store.get(record.file_id)
    assert fetched.name == "b.txt"
    assert fetched.tags == {"y"}


def test_update_persists_tombstone(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1")
    store.insert(record)

    record.mark_deleted()
    store.update(record)

    fetched = store.get(record.file_id)
    assert fetched.tombstone is True
    assert fetched.tombstone_at is not None


def test_data_survives_reopening_the_same_db_file(tmp_path: Path):
    db_path = tmp_path / "metadata.db"
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})

    MetadataStore(db_path).insert(record)
    reopened = MetadataStore(db_path)

    fetched = reopened.get(record.file_id)
    assert fetched is not None
    assert fetched.tags == {"x"}


def test_list_live_excludes_tombstoned_records(store: MetadataStore):
    live = FileRecord.new(name="live.txt", content_hash="h1")
    deleted = FileRecord.new(name="deleted.txt", content_hash="h2")
    deleted.mark_deleted()
    store.insert(live)
    store.insert(deleted)

    results = store.list_live()

    assert {r.file_id for r in results} == {live.file_id}
