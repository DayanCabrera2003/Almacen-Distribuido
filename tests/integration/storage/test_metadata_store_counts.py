from pathlib import Path

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


def test_an_empty_store_counts_nothing(tmp_path: Path):
    assert MetadataStore(tmp_path / "metadata.db").record_counts() == (0, 0)


def test_counts_separate_live_records_from_tombstones(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    for name in ("a.txt", "b.txt"):
        store.insert(FileRecord.new(name=name, content_hash="h", node_id="node1"))
    deleted = FileRecord.new(name="c.txt", content_hash="h", node_id="node1")
    deleted.mark_deleted("node1")
    store.insert(deleted)

    assert store.record_counts() == (2, 1)
