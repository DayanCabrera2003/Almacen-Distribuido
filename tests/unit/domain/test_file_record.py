import uuid
from datetime import datetime, timezone

from almacen.domain.file_record import FileRecord

NODE = "node1"


def test_new_exposes_plain_values_through_its_read_interface():
    record = FileRecord.new(
        name="a.txt", content_hash="h1", node_id=NODE, tags={"x", "y"}
    )

    assert isinstance(record.file_id, uuid.UUID)
    assert record.name == "a.txt"
    assert record.content_hash == "h1"
    assert record.tags == {"x", "y"}
    assert record.tombstone is False
    assert record.tombstone_at is None
    assert record.created_at.tzinfo is not None
    assert record.updated_at >= record.created_at


def test_new_records_get_distinct_ids():
    first = FileRecord.new(name="a", content_hash="h", node_id=NODE)
    second = FileRecord.new(name="a", content_hash="h", node_id=NODE)
    assert first.file_id != second.file_id


def test_new_without_tags_starts_empty():
    assert FileRecord.new(name="a", content_hash="h", node_id=NODE).tags == set()


def test_rename_changes_the_name_and_bumps_updated_at():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    before = record.updated_at

    record.rename("b.txt", NODE)

    assert record.name == "b.txt"
    assert record.updated_at >= before


def test_update_content_repoints_the_hash():
    record = FileRecord.new(name="a.txt", content_hash="h1", node_id=NODE)
    record.update_content("h2", NODE)
    assert record.content_hash == "h2"


def test_add_and_remove_tag():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)

    record.add_tag("invoice", NODE)
    assert record.tags == {"invoice"}

    record.remove_tag("invoice", NODE)
    assert record.tags == set()


def test_removing_a_tag_the_file_does_not_have_is_a_no_op():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE, tags={"x"})
    record.remove_tag("absent", NODE)
    assert record.tags == {"x"}


def test_a_tag_change_bumps_updated_at():
    # The OR-Set carries no timestamps, so without a dedicated field a tag edit
    # would leave updated_at stale in the API response.
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    before = record.updated_at
    record.add_tag("x", NODE)
    assert record.updated_at >= before


def test_mark_deleted_sets_the_tombstone_and_its_timestamp():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)

    record.mark_deleted(NODE)

    assert record.tombstone is True
    assert record.tombstone_at is not None
    assert record.updated_at == record.tombstone_at


def test_every_mutation_advances_this_node_s_vector_clock():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    start = record.vector_clock.get(NODE)

    record.rename("b.txt", NODE)
    record.update_content("h2", NODE)
    record.add_tag("x", NODE)
    record.remove_tag("x", NODE)
    record.mark_deleted(NODE)

    assert record.vector_clock.get(NODE) == start + 5


def test_a_mutation_from_another_node_advances_only_that_component():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    record.rename("b.txt", "node2")
    assert record.vector_clock.get("node2") == 1
    assert record.vector_clock.get("node1") == 1  # only the creation


def test_tag_occurrences_use_the_vector_clock_as_their_counter():
    # Spec §6: one counter source, not two. Two adds of the same tag from the
    # same node must still be distinguishable occurrences.
    record = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE)
    record.add_tag("x", NODE)
    record.remove_tag("x", NODE)
    record.add_tag("x", NODE)
    assert record.tags == {"x"}
    assert len(record.tag_set.adds) == 2


def test_merge_of_concurrent_edits_keeps_both_tag_changes():
    base = FileRecord.new(
        name="a.txt", content_hash="h", node_id="node1", tags={"draft"}
    )
    on_node1, on_node2 = base.copy(), base.copy()

    on_node1.add_tag("urgent", "node1")
    on_node2.add_tag("reviewed", "node2")
    on_node2.remove_tag("draft", "node2")

    merged = on_node1.merged(on_node2)
    assert merged.tags == {"urgent", "reviewed"}


def test_merge_resolves_a_concurrent_rename_by_lww():
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    on_node1, on_node2 = base.copy(), base.copy()

    on_node1.rename("from-node1.txt", "node1")
    on_node2.rename("from-node2.txt", "node2")

    assert on_node1.merged(on_node2).name == on_node2.merged(on_node1).name, (
        "both nodes must pick the same winner"
    )


def test_merge_keeps_the_causally_newer_value():
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    stale = record.copy()
    record.rename("newer.txt", "node1")

    assert record.merged(stale).name == "newer.txt"
    assert stale.merged(record).name == "newer.txt"


def test_a_delete_outlasts_a_concurrent_edit():
    # Spec §6: a non-delete edit never writes the tombstone field, so it cannot
    # produce a competing timestamp for it.
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    deleting, editing = base.copy(), base.copy()

    deleting.mark_deleted("node1")
    editing.add_tag("late", "node2")

    merged = deleting.merged(editing)
    assert merged.tombstone is True
    # The edit is not lost, just invisible through the API while tombstoned.
    assert "late" in merged.tags


def test_merge_combines_the_vector_clocks():
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    left, right = base.copy(), base.copy()
    left.rename("l", "node1")
    right.rename("r", "node2")

    merged = left.merged(right)
    assert merged.vector_clock.get("node1") == 2
    assert merged.vector_clock.get("node2") == 1


def test_merge_keeps_the_earlier_creation_time():
    left = FileRecord.new(name="a", content_hash="h", node_id="node1")
    right = left.copy()
    right.created_at = datetime(2030, 1, 1, tzinfo=timezone.utc)

    # Both directions: asserting only the case where `self` is already the
    # earlier one would pass against `created_at=self.created_at`.
    assert left.merged(right).created_at == left.created_at
    assert right.merged(left).created_at == left.created_at


def test_merge_keeps_the_later_update_time():
    left = FileRecord.new(name="a", content_hash="h", node_id="node1")
    right = left.copy()
    right.rename("b", "node2")

    assert left.merged(right).updated_at == right.updated_at
    assert right.merged(left).updated_at == right.updated_at


def test_merging_different_files_is_a_programming_error():
    import pytest

    left = FileRecord.new(name="a", content_hash="h", node_id="node1")
    right = FileRecord.new(name="b", content_hash="h", node_id="node1")
    with pytest.raises(ValueError, match="different files"):
        left.merged(right)


def test_concurrency_detection_distinguishes_stale_from_conflicting():
    base = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")

    stale = base.copy()
    ahead = base.copy()
    ahead.rename("newer", "node1")
    assert not ahead.is_concurrent_with(stale), "one descends from the other"

    left, right = base.copy(), base.copy()
    left.rename("l", "node1")
    right.rename("r", "node2")
    assert left.is_concurrent_with(right), "neither saw the other's edit"


def test_copy_is_independent_of_the_original():
    original = FileRecord.new(name="a.txt", content_hash="h", node_id=NODE, tags={"x"})
    duplicate = original.copy()

    duplicate.add_tag("y", NODE)
    duplicate.rename("b.txt", NODE)

    assert original.tags == {"x"}
    assert original.name == "a.txt"


def test_merge_does_not_mutate_either_input():
    left = FileRecord.new(name="a.txt", content_hash="h", node_id="node1")
    right = left.copy()
    right.rename("b.txt", "node2")

    left.merged(right)

    assert left.name == "a.txt"
    assert right.name == "b.txt"
