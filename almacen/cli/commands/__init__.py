"""CLI command groups, one module per REST resource."""
from __future__ import annotations

from collections.abc import Iterable


def split_tags(values: Iterable[str] | None) -> list[str]:
    """Flatten repeated and comma-separated tag arguments, dropping duplicates."""
    parts = (part.strip() for value in values or [] for part in value.split(","))
    return list(dict.fromkeys(part for part in parts if part))
