"""Queries over tag combinations against the metadata store."""
from __future__ import annotations

from typing import Literal

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore

Mode = Literal["and", "or"]


class TagIndex:
    """AND/OR tag queries, answered from the inverted index on tag occurrences.

    The index is `idx_file_tag_adds_tag` over the OR-Set's add occurrences;
    `MetadataStore.find_live_by_tags` turns it into the visible-tag predicate.
    Nothing extra is maintained on writes: the occurrence tables every write
    path already keeps are the postings lists.
    """

    def __init__(self, metadata_store: MetadataStore) -> None:
        self._metadata_store = metadata_store

    def query(self, tags: list[str] | None, mode: Mode = "and") -> list[FileRecord]:
        # Required for correctness, not just a shortcut: with no query tags an
        # "or" match would be empty for every record (nothing intersects the
        # empty set), so an unfiltered request would return nothing instead of
        # every live file. ("and" would survive, since the empty set is a
        # subset of every tag set.)
        if not tags:
            return self._metadata_store.list_live()

        if mode not in ("and", "or"):
            raise ValueError(f"mode must be 'and' or 'or', not {mode!r}")
        # A set, so duplicates cannot inflate k in the AND count.
        return self._metadata_store.find_live_by_tags(
            set(tags), match_all=mode == "and"
        )
