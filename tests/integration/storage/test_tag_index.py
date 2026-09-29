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
