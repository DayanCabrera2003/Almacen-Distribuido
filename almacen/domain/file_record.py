"""Domain entity for a stored file's identity and metadata."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FileRecord:
    file_id: uuid.UUID
    name: str
    content_hash: str
    tags: set[str]
    tombstone: bool = False
    tombstone_at: datetime | None = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    @classmethod
    def new(cls, name: str, content_hash: str, tags: set[str] | None = None) -> "FileRecord":
        now = _utcnow()
        return cls(
            file_id=uuid.uuid4(),
            name=name,
            content_hash=content_hash,
            tags=set(tags) if tags else set(),
            created_at=now,
            updated_at=now,
        )

    def add_tag(self, tag: str) -> None:
        self.tags.add(tag)
        self.updated_at = _utcnow()

    def remove_tag(self, tag: str) -> None:
        self.tags.discard(tag)
        self.updated_at = _utcnow()

    def rename(self, name: str) -> None:
        self.name = name
        self.updated_at = _utcnow()

    def update_content(self, content_hash: str) -> None:
        self.content_hash = content_hash
        self.updated_at = _utcnow()

    def mark_deleted(self) -> None:
        self.tombstone = True
        self.tombstone_at = _utcnow()
        self.updated_at = self.tombstone_at
