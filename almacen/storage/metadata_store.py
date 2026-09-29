"""SQLite-backed persistence for FileRecord metadata."""
from __future__ import annotations

import sqlite3
import threading
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from almacen.domain.file_record import FileRecord

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    file_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    tombstone INTEGER NOT NULL DEFAULT 0,
    tombstone_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_tags (
    file_id TEXT NOT NULL REFERENCES files(file_id),
    tag TEXT NOT NULL,
    PRIMARY KEY (file_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_file_tags_tag ON file_tags(tag);
"""


def _dt_to_str(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _str_to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class MetadataStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.Lock()

    def insert(self, record: FileRecord) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO files
                    (file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(record.file_id),
                    record.name,
                    record.content_hash,
                    int(record.tombstone),
                    _dt_to_str(record.tombstone_at),
                    _dt_to_str(record.created_at),
                    _dt_to_str(record.updated_at),
                ),
            )
            self._insert_tags(record.file_id, record.tags)

    def get(self, file_id: uuid.UUID) -> FileRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at "
                "FROM files WHERE file_id = ?",
                (str(file_id),),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_record(row)

    def update(self, record: FileRecord) -> None:
        """Write the record as given, replacing whatever is stored.

        This is a blind write: it overwrites the stored tag set with the
        caller's, so anything another writer changed since the caller read the
        record is lost. Use `mutate` for read-modify-write on a live record;
        this method is for callers that legitimately own the whole value, such
        as applying a record received from a peer.
        """
        with self._lock, self._conn:
            self._write(record)

    def mutate(
        self, file_id: uuid.UUID, mutator: Callable[[FileRecord], None]
    ) -> FileRecord | None:
        """Read, apply `mutator`, and write back, all under one lock.

        Read-modify-write split across two calls loses updates: two callers read
        the same tag set, each adds one tag, and the second write erases the
        first's — a change this node already acknowledged. Holding the lock
        across the whole sequence makes the operation atomic.

        Returns the updated record, or None if the file does not exist. The lock
        is not reentrant, so `mutator` must not call back into this store.
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at "
                "FROM files WHERE file_id = ?",
                (str(file_id),),
            ).fetchone()
            if row is None:
                return None

            record = self._row_to_record(row)
            mutator(record)
            self._write(record)
            return record

    def _write(self, record: FileRecord) -> None:
        """Persist a record's mutable fields. Caller must hold the lock."""
        self._conn.execute(
            """
            UPDATE files
            SET name = ?, content_hash = ?, tombstone = ?, tombstone_at = ?, updated_at = ?
            WHERE file_id = ?
            """,
            (
                record.name,
                record.content_hash,
                int(record.tombstone),
                _dt_to_str(record.tombstone_at),
                _dt_to_str(record.updated_at),
                str(record.file_id),
            ),
        )
        self._conn.execute(
            "DELETE FROM file_tags WHERE file_id = ?", (str(record.file_id),)
        )
        self._insert_tags(record.file_id, record.tags)

    def upsert(self, record: FileRecord) -> None:
        """Insert the record, or overwrite the stored one if the file is known.

        Every mutable field is replaced; `created_at` is not, because a file's
        creation time never changes and the local copy is at least as
        authoritative as an incoming one.

        Used when applying a record replicated from another node, where this node
        may or may not already know the file.
        """
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO files
                    (file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_id) DO UPDATE SET
                    name = excluded.name,
                    content_hash = excluded.content_hash,
                    tombstone = excluded.tombstone,
                    tombstone_at = excluded.tombstone_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(record.file_id),
                    record.name,
                    record.content_hash,
                    int(record.tombstone),
                    _dt_to_str(record.tombstone_at),
                    _dt_to_str(record.created_at),
                    _dt_to_str(record.updated_at),
                ),
            )
            self._conn.execute(
                "DELETE FROM file_tags WHERE file_id = ?", (str(record.file_id),)
            )
            self._insert_tags(record.file_id, record.tags)

    def list_live(self) -> list[FileRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at "
                "FROM files WHERE tombstone = 0"
            ).fetchall()
            return [self._row_to_record(row) for row in rows]

    def _insert_tags(self, file_id: uuid.UUID, tags: set[str]) -> None:
        self._conn.executemany(
            "INSERT INTO file_tags (file_id, tag) VALUES (?, ?)",
            [(str(file_id), tag) for tag in tags],
        )

    def _row_to_record(self, row: tuple) -> FileRecord:
        file_id_str, name, content_hash, tombstone, tombstone_at, created_at, updated_at = row
        file_id = uuid.UUID(file_id_str)
        tags = {
            tag_row[0]
            for tag_row in self._conn.execute(
                "SELECT tag FROM file_tags WHERE file_id = ?", (file_id_str,)
            ).fetchall()
        }
        return FileRecord(
            file_id=file_id,
            name=name,
            content_hash=content_hash,
            tags=tags,
            tombstone=bool(tombstone),
            tombstone_at=_str_to_dt(tombstone_at),
            created_at=_str_to_dt(created_at),
            updated_at=_str_to_dt(updated_at),
        )
