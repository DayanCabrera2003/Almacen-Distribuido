"""Human-readable rendering of API responses."""
from __future__ import annotations

from almacen.cli.output import render_files, render_mapping, render_table

FILE = {
    "file_id": "8f1c0000-0000-0000-0000-000000000001",
    "name": "report.txt",
    "content_hash": "ab" * 32,
    "tags": ["draft", "invoice"],
    "created_at": "2026-10-17T10:00:00Z",
    "updated_at": "2026-10-17T11:30:00Z",
}


def test_render_table_aligns_columns():
    lines = render_table(["A", "LONG"], [["xyz", "1"], ["q", "22"]]).splitlines()

    assert lines == ["A    LONG", "xyz  1", "q    22"]


def test_render_files_shows_one_row_per_file():
    lines = render_files([FILE]).splitlines()

    assert lines[0].split() == ["FILE_ID", "NAME", "TAGS", "UPDATED"]
    assert lines[1].split() == [
        FILE["file_id"],
        "report.txt",
        "draft,invoice",
        "2026-10-17T11:30:00Z",
    ]


def test_render_files_says_so_when_empty():
    assert render_files([]) == "no files"


def test_render_mapping_handles_scalars_nested_mappings_and_row_lists():
    text = render_mapping(
        {
            "node_id": "node1",
            "counts": {"files": 3},
            "members": [
                {"node_id": "node1", "state": "alive"},
                {"node_id": "node2", "state": "suspect"},
            ],
            "peers": ["node2", "node3"],
        }
    )

    assert text.splitlines() == [
        "node_id: node1",
        "counts:",
        "  files: 3",
        "members:",
        "  NODE_ID  STATE",
        "  node1    alive",
        "  node2    suspect",
        "peers: node2, node3",
    ]
