# almacen/storage/protocols.py
"""Structural types for the storage layer."""
from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Protocol

from almacen.domain.file_record import FileRecord


class MetadataStoreLike(Protocol):
    """What the API layer needs from a metadata store.

    Satisfied by both the local `MetadataStore` and the cluster's
    `ReplicatedMetadataStore`, which lets the routers stay unaware of whether
    they are running in a cluster.
    """

    def insert(self, record: FileRecord) -> None: ...

    def get(self, file_id: uuid.UUID) -> FileRecord | None: ...

    def update(self, record: FileRecord) -> None: ...

    def mutate(
        self, file_id: uuid.UUID, mutator: Callable[[FileRecord], None]
    ) -> FileRecord | None: ...

    def list_live(self) -> list[FileRecord]: ...
