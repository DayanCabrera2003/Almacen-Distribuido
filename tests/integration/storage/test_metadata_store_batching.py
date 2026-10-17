"""Decoding many records must cost a constant number of statements, not N+1."""
from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


def _store_with(tmp_path: Path, count: int) -> MetadataStore:
    store = MetadataStore(tmp_path / "metadata.db")
    for i in range(count):
        record = FileRecord.new(
            name=f"f{i}.txt", content_hash="h", node_id="node1", tags={"a", f"t{i}"}
        )
        record.remove_tag("a", "node1")
        store.insert(record)
    return store


def _count_selects(store: MetadataStore, action) -> tuple[object, int]:
    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)
    try:
        result = action()
    finally:
        store._conn.set_trace_callback(None)
    return result, sum(1 for s in statements if s.lstrip().upper().startswith("SELECT"))


@pytest.mark.parametrize("method", ["list_live", "list_all"])
def test_listing_issues_the_same_number_of_selects_for_any_corpus_size(
    tmp_path: Path, method: str
):
    small = _store_with(tmp_path / "small", 2)
    large = _store_with(tmp_path / "large", 40)

    _, small_selects = _count_selects(small, getattr(small, method))
    _, large_selects = _count_selects(large, getattr(large, method))

    assert large_selects == small_selects


def test_batched_decoding_keeps_each_records_own_tag_state(tmp_path: Path):
    store = _store_with(tmp_path, 5)

    for record in store.list_live():
        index = record.name.removeprefix("f").removesuffix(".txt")
        # The removed tag's add and remove both survive the round trip, and
        # no other record's occurrences leak in.
        assert record.tags == {f"t{index}"}
        assert {occ[0] for occ in record.tag_set.adds} == {"a", f"t{index}"}
        assert {occ[0] for occ in record.tag_set.removes} == {"a"}
