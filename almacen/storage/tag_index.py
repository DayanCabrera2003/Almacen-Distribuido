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
        if not tags:
            return live

        wanted = set(tags)
        if mode == "and":
            return [record for record in live if wanted.issubset(record.tags)]
        return [record for record in live if wanted & record.tags]
