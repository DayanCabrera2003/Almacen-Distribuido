# almacen/cluster/replicated_metadata_store.py
"""A MetadataStore that also pushes its writes to the rest of the cluster."""
from __future__ import annotations

import uuid

from almacen.cluster.metadata_replicator import MetadataReplicator
from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


class ReplicatedMetadataStore:
    """Writes locally, then replicates; reads are always local.

    Implemented as a decorator rather than as calls sprinkled through the
    routers: there are five mutation sites, and wrapping the store means none of
    them can forget to replicate. Reads need no cluster involvement because
    every node holds a full metadata replica (spec §4).
    """

    def __init__(self, local: MetadataStore, replicator: MetadataReplicator) -> None:
        self._local = local
        self._replicator = replicator

    @property
    def local(self) -> MetadataStore:
        """The underlying local store, for components that must not replicate."""
        return self._local

    def insert(self, record: FileRecord) -> None:
        self._local.insert(record)
        self._replicator.replicate(record)

    def update(self, record: FileRecord) -> None:
        self._local.update(record)
        self._replicator.replicate(record)

    def get(self, file_id: uuid.UUID) -> FileRecord | None:
        return self._local.get(file_id)

    def list_live(self) -> list[FileRecord]:
        return self._local.list_live()
