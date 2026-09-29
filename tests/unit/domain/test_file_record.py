import uuid

from almacen.domain.file_record import FileRecord


def test_new_creates_record_with_generated_id_and_empty_tags_by_default():
    record = FileRecord.new(name="report.txt", content_hash="abc123", node_id="node1")

    assert isinstance(record.file_id, uuid.UUID)
    assert record.name == "report.txt"
    assert record.content_hash == "abc123"
    assert record.tags == set()
    assert record.tombstone is False
    assert record.tombstone_at is None


def test_new_copies_tags_without_aliasing_caller_set():
    original_tags = {"invoice"}
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags=original_tags)

    record.add_tag("draft", "node1")

    assert original_tags == {"invoice"}


def test_add_tag_is_idempotent():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")

    record.add_tag("invoice", "node1")
    record.add_tag("invoice", "node1")

    assert record.tags == {"invoice"}


def test_remove_tag_on_absent_tag_is_a_noop():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")

    record.remove_tag("nonexistent", "node1")

    assert record.tags == set()


def test_rename_updates_name_and_updated_at():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")
    original_updated_at = record.updated_at

    record.rename("b.txt", "node1")

    assert record.name == "b.txt"
    assert record.updated_at >= original_updated_at


def test_update_content_replaces_content_hash():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")

    record.update_content("h2", "node1")

    assert record.content_hash == "h2"


def test_mark_deleted_sets_tombstone_and_timestamp():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1")

    record.mark_deleted("node1")

    assert record.tombstone is True
    assert record.tombstone_at is not None
