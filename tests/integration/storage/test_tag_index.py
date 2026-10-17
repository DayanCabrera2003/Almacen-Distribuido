from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


@pytest.fixture
def index(tmp_path: Path) -> TagIndex:
    metadata_store = MetadataStore(tmp_path / "metadata.db")
    invoice_only = FileRecord.new(name="a.txt", content_hash="h1", node_id="node1", tags={"invoice"})
    invoice_and_draft = FileRecord.new(
        name="b.txt", content_hash="h2", node_id="node1", tags={"invoice", "draft"}
    )
    draft_only = FileRecord.new(name="c.txt", content_hash="h3", node_id="node1", tags={"draft"})
    deleted_invoice = FileRecord.new(name="d.txt", content_hash="h4", node_id="node1", tags={"invoice"})
    deleted_invoice.mark_deleted("node1")
    for record in (invoice_only, invoice_and_draft, draft_only, deleted_invoice):
        metadata_store.insert(record)
    return TagIndex(metadata_store)


def test_and_mode_returns_files_with_all_tags(index: TagIndex):
    results = index.query(["invoice", "draft"], mode="and")
    assert [r.name for r in results] == ["b.txt"]


def test_or_mode_returns_files_with_any_tag(index: TagIndex):
    results = index.query(["invoice", "draft"], mode="or")
    assert {r.name for r in results} == {"a.txt", "b.txt", "c.txt"}


def test_no_tags_returns_all_live_files(index: TagIndex):
    results = index.query(tags=None)
    assert {r.name for r in results} == {"a.txt", "b.txt", "c.txt"}


def test_query_excludes_tombstoned_files(index: TagIndex):
    results = index.query(["invoice"], mode="or")
    assert "d.txt" not in {r.name for r in results}


def test_no_tags_in_or_mode_still_returns_all_live_files(index: TagIndex):
    # Guards the early return in query(): without it the "or" predicate would
    # intersect against an empty tag set and match nothing.
    results = index.query(tags=None, mode="or")
    assert {r.name for r in results} == {"a.txt", "b.txt", "c.txt"}


def test_empty_tag_list_in_or_mode_still_returns_all_live_files(index: TagIndex):
    results = index.query(tags=[], mode="or")
    assert {r.name for r in results} == {"a.txt", "b.txt", "c.txt"}


# ---- the query is answered from the tag index, not a scan of live records ----


def _store_with_noise(tmp_path: Path, noise: int) -> MetadataStore:
    """One `needle`+`hay` file among `noise` live files tagged only `hay`."""
    store = MetadataStore(tmp_path / "metadata.db")
    store.insert(
        FileRecord.new(
            name="needle.txt", content_hash="h", node_id="node1", tags={"needle", "hay"}
        )
    )
    for i in range(noise):
        store.insert(
            FileRecord.new(name=f"hay{i}.txt", content_hash="h", node_id="node1", tags={"hay"})
        )
    return store


def _traced(store: MetadataStore, action) -> tuple[object, list[str]]:
    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)
    try:
        result = action()
    finally:
        store._conn.set_trace_callback(None)
    return result, [s for s in statements if s.lstrip().upper().startswith("SELECT")]


@pytest.mark.parametrize("mode", ["and", "or"])
def test_query_cost_in_statements_does_not_grow_with_the_corpus(tmp_path: Path, mode: str):
    small = _store_with_noise(tmp_path / "small", 2)
    large = _store_with_noise(tmp_path / "large", 60)

    _, small_selects = _traced(small, lambda: TagIndex(small).query(["needle"], mode))
    large_result, large_selects = _traced(large, lambda: TagIndex(large).query(["needle"], mode))

    assert [r.name for r in large_result] == ["needle.txt"]
    assert len(large_selects) == len(small_selects)


@pytest.mark.parametrize("mode", ["and", "or"])
def test_query_decodes_only_the_matching_records(tmp_path: Path, mode: str):
    store = _store_with_noise(tmp_path, 10)
    hay_ids = {str(r.file_id) for r in store.list_live() if r.name != "needle.txt"}

    _, selects = _traced(store, lambda: TagIndex(store).query(["needle"], mode))

    # The trace shows bound values, so a non-matching file whose tag state was
    # read (or whose row was fetched by id) would show up here.
    assert not any(file_id in statement for statement in selects for file_id in hay_ids)


@pytest.mark.parametrize("mode", ["and", "or"])
def test_query_seeks_the_tag_index_instead_of_scanning_files(tmp_path: Path, mode: str):
    store = _store_with_noise(tmp_path, 20)

    _, selects = _traced(store, lambda: TagIndex(store).query(["needle"], mode))
    plan = " | ".join(
        row[-1]
        for statement in selects
        for row in store._conn.execute(f"EXPLAIN QUERY PLAN {statement}").fetchall()
    )

    assert "idx_file_tag_adds_tag" in plan
    # A full pass over `files` is exactly the linear scan this replaces.
    assert "SCAN files" not in plan


def test_a_concurrent_add_survives_a_remove_and_stays_queryable(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1", tags={"invoice"})
    store.insert(record)

    # node2 re-adds the tag without having seen node1's remove: add wins.
    concurrent = record.copy()
    concurrent.add_tag("invoice", "node2")
    store.mutate(record.file_id, lambda r: r.remove_tag("invoice", "node1"))
    assert TagIndex(store).query(["invoice"]) == []

    store.merge_remote(concurrent)

    assert [r.name for r in TagIndex(store).query(["invoice"])] == ["a.txt"]


def test_an_unknown_mode_is_rejected_rather_than_read_as_or(index: TagIndex):
    with pytest.raises(ValueError):
        index.query(["invoice"], mode="xor")  # type: ignore[arg-type]


def test_a_tag_removed_on_every_observed_add_is_not_matched(tmp_path: Path):
    store = MetadataStore(tmp_path / "metadata.db")
    record = FileRecord.new(name="a.txt", content_hash="h", node_id="node1", tags={"invoice"})
    record.add_tag("invoice", "node2")
    store.insert(record)

    store.mutate(record.file_id, lambda r: r.remove_tag("invoice", "node1"))

    assert TagIndex(store).query(["invoice"], mode="or") == []
