"""Plain-text rendering of API responses for the CLI's default output.

Kept dependency-free on purpose: the tables are simple enough that a rendering
library would add weight without adding anything the demo needs.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

_COLUMN_GAP = "  "


def render_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    """Left-aligned columns, each as wide as its widest cell."""
    cells = [list(map(str, headers))] + [[str(cell) for cell in row] for row in rows]
    widths = [max(len(row[col]) for row in cells) for col in range(len(headers))]
    return "\n".join(
        _COLUMN_GAP.join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in cells
    )


def render_files(files: Sequence[Mapping[str, Any]]) -> str:
    if not files:
        return "no files"
    return render_table(
        ["FILE_ID", "NAME", "TAGS", "UPDATED"],
        [
            [f["file_id"], f["name"], ",".join(f["tags"]) or "-", f["updated_at"]]
            for f in files
        ],
    )


def render_file(file: Mapping[str, Any]) -> str:
    return render_mapping(
        {
            "file_id": file["file_id"],
            "name": file["name"],
            "tags": ", ".join(file["tags"]) or "-",
            "content_hash": file["content_hash"],
            "created_at": file["created_at"],
            "updated_at": file["updated_at"],
        }
    )


def render_tags(tags: Sequence[str]) -> str:
    return "\n".join(tags) if tags else "no tags"


def render_mapping(data: Mapping[str, Any], indent: str = "") -> str:
    """Render an arbitrary JSON object as indented `key: value` lines.

    Used for `/cluster/status`, whose shape is owned by the server: rendering it
    generically means the CLI does not break when a field is added. A list of
    objects becomes a table, a list of scalars a comma-separated value.
    """
    lines: list[str] = []
    for key, value in data.items():
        if isinstance(value, Mapping):
            lines.append(f"{indent}{key}:")
            lines.append(render_mapping(value, indent + "  "))
        elif _is_row_list(value):
            lines.append(f"{indent}{key}:")
            headers = list(dict.fromkeys(k for row in value for k in row))
            table = render_table(
                [h.upper() for h in headers],
                [[_scalar(row.get(h)) for h in headers] for row in value],
            )
            lines.extend(indent + "  " + line for line in table.splitlines())
        elif isinstance(value, list):
            lines.append(f"{indent}{key}: {', '.join(map(_scalar, value)) or '-'}")
        else:
            lines.append(f"{indent}{key}: {_scalar(value)}")
    return "\n".join(lines)


def _is_row_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, Mapping) for item in value)
    )


def _scalar(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, list):
        return ",".join(map(_scalar, value)) or "-"
    return str(value)
