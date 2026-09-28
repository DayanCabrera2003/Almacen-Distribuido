"""SQLite-backed persistence for FileRecord metadata."""
from __future__ import annotations

import sqlite3
import uuid
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
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def insert(self, record: FileRecord) -> None:
        with self._conn:
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
        row = self._conn.execute(
            "SELECT file_id, name, content_hash, tombstone, tombstone_at, created_at, updated_at "
            "FROM files WHERE file_id = ?",
            (str(file_id),),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def update(self, record: FileRecord) -> None:
        with self._conn:
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

    def list_live(self) -> list[FileRecord]:
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
