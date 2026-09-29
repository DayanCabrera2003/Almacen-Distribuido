"""Queries over tag combinations against the metadata store."""
from __future__ import annotations

from typing import Literal

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore

Mode = Literal["and", "or"]


class TagIndex:
    def __init__(self, metadata_store: MetadataStore) -> None:
        self._metadata_store = metadata_store

    def query(self, tags: list[str] | None, mode: Mode = "and") -> list[FileRecord]:
        live = self._metadata_store.list_live()
        # Required for correctness, not just a shortcut: with no query tags the
        # "or" predicate below would be `set() & record.tags`, i.e. empty for
        # every record, so an unfiltered request would return nothing instead of
        # every live file. ("and" would survive, since `set() <= record.tags`
        # holds vacuously.)
        if not tags:
            return live

        wanted = set(tags)
        if mode == "and":
            return [record for record in live if wanted.issubset(record.tags)]
        return [record for record in live if wanted & record.tags]
