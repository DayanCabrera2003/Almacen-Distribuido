"""SQLite-backed persistence for FileRecord metadata, including CRDT state."""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from almacen.crdt.lww_register import LWWRegister
from almacen.crdt.or_set import OrSet
from almacen.crdt.vector_clock import VectorClock
from almacen.domain.file_record import FileRecord

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    file_id TEXT PRIMARY KEY,
    -- Each single-value field stores its last-writer-wins value alongside the
    -- (timestamp, node_id) that wrote it. The value keeps its own column so
    -- queries and manual inspection stay readable.
    name TEXT NOT NULL,
    name_ts TEXT NOT NULL,
    name_node TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    content_hash_ts TEXT NOT NULL,
    content_hash_node TEXT NOT NULL,
    tombstone INTEGER NOT NULL DEFAULT 0,
    tombstone_ts TEXT NOT NULL,
    tombstone_node TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    -- Small, and always read and written whole, so a column beats a join.
    vector_clock TEXT NOT NULL
);

-- The OR-Set's occurrences. Both sets are needed: the removes are not
-- derivable from the visible tag set, and losing them would resurrect tags on
-- the next merge.
CREATE TABLE IF NOT EXISTS file_tag_adds (
    file_id TEXT NOT NULL REFERENCES files(file_id),
    tag TEXT NOT NULL,
    node_id TEXT NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag, node_id, counter)
);

CREATE TABLE IF NOT EXISTS file_tag_removes (
    file_id TEXT NOT NULL REFERENCES files(file_id),
    tag TEXT NOT NULL,
    node_id TEXT NOT NULL,
    counter INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag, node_id, counter)
);

CREATE INDEX IF NOT EXISTS idx_file_tag_adds_tag ON file_tag_adds(tag);
"""

_COLUMNS = (
    "file_id, name, name_ts, name_node, content_hash, content_hash_ts, "
    "content_hash_node, tombstone, tombstone_ts, tombstone_node, "
    "created_at, updated_at, vector_clock"
)
_SELECT_ONE = f"SELECT {_COLUMNS} FROM files WHERE file_id = ?"


def _dt_to_str(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _str_to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class MetadataStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # A metadata write is acknowledged once it commits here; the push to
        # peers is best effort. FULL fsyncs the WAL on every commit, so an
        # acknowledged write survives a power loss. NORMAL would keep the
        # database consistent but could roll back the last commits, losing a
        # write no peer has yet received. Set explicitly because the default
        # is a compile-time option of the SQLite build.
        self._conn.execute("PRAGMA synchronous = FULL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.Lock()

    def insert(self, record: FileRecord) -> None:
        with self._lock, self._conn:
            self._insert_locked(record)

    def get(self, file_id: uuid.UUID) -> FileRecord | None:
        with self._lock:
            row = self._conn.execute(_SELECT_ONE, (str(file_id),)).fetchone()
            return self._row_to_record(row) if row else None

    def update(self, record: FileRecord) -> None:
        """Write the record as given, replacing whatever is stored.

        This is a blind write: it overwrites the stored CRDT state with the
        caller's, so anything another writer changed since the caller read the
        record is lost. Use `mutate` for read-modify-write on a live record,
        and `merge_remote` for a record arriving from a peer.
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
            row = self._conn.execute(_SELECT_ONE, (str(file_id),)).fetchone()
            if row is None:
                return None

            record = self._row_to_record(row)
            mutator(record)
            self._write(record)
            return record

    def merge_remote(
        self,
        incoming: FileRecord,
        *,
        refuse_unknown: Callable[[FileRecord], bool] | None = None,
    ) -> FileRecord | None:
        """Merge a record received from a peer into the local copy.

        Read-merge-write happens under the store's lock, so two records arriving
        for the same file at once cannot interleave and lose one side's changes.

        `refuse_unknown` is consulted only when this store does not hold the
        record; if it returns True the record is not adopted and None is
        returned. GC uses it to keep a purged, expired tombstone from being
        taught straight back (see `almacen.cluster.gc.is_expired_tombstone`).
        It is checked under the same lock as the insert, so a record cannot
        arrive in between and be skipped by mistake.
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                _SELECT_ONE, (str(incoming.file_id),)
            ).fetchone()
            if row is None:
                if refuse_unknown is not None and refuse_unknown(incoming):
                    return None
                self._insert_locked(incoming)
                return incoming

            merged = self._row_to_record(row).merged(incoming)
            self._write(merged)
            return merged

    def list_live(self) -> list[FileRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_COLUMNS} FROM files WHERE tombstone = 0"
            ).fetchall()
            return [self._row_to_record(row) for row in rows]

    def record_counts(self) -> tuple[int, int]:
        """`(live, tombstoned)` record counts, without decoding any record.

        For status reporting, which only needs the numbers: `list_all` would
        rebuild every record's CRDT state to count them.
        """
        with self._lock:
            counts = dict(
                self._conn.execute(
                    "SELECT tombstone, COUNT(*) FROM files GROUP BY tombstone"
                ).fetchall()
            )
            return counts.get(0, 0), counts.get(1, 0)

    def list_all(self) -> list[FileRecord]:
        """Every record, tombstoned or not.

        Anti-entropy uses this rather than `list_live`: a tombstone is an update
        like any other, and a peer that never receives it goes on serving a file
        the rest of the cluster considers deleted.
        """
        with self._lock:
            rows = self._conn.execute(f"SELECT {_COLUMNS} FROM files").fetchall()
            return [self._row_to_record(row) for row in rows]

    def purge(self, file_id: uuid.UUID) -> bool:
        """Delete a record and its tag occurrences outright.

        The one irreversible operation in this store. A purged record is
        indistinguishable from one this node never saw, so a peer that still
        holds it live will teach it back on the next anti-entropy round — as a
        *live* file, undoing the deletion. The caller must have checked the
        grace TTL first; see `almacen.cluster.gc.records_to_purge`.
        """
        with self._lock, self._conn:
            for table in ("file_tag_adds", "file_tag_removes"):
                self._conn.execute(
                    f"DELETE FROM {table} WHERE file_id = ?", (str(file_id),)
                )
            cursor = self._conn.execute(
                "DELETE FROM files WHERE file_id = ?", (str(file_id),)
            )
            return cursor.rowcount > 0

    def reference_counts(self) -> dict[str, int]:
        """How many records point at each content hash.

        Counts tombstoned records too: they still reference their content until
        they are purged, which is what makes a blob's retention two grace TTLs
        rather than one (spec §10).
        """
        with self._lock:
            return {
                content_hash: count
                for content_hash, count in self._conn.execute(
                    "SELECT content_hash, COUNT(*) FROM files GROUP BY content_hash"
                ).fetchall()
            }

    def close(self) -> None:
        """Release the SQLite connection.

        Each `create_app` opens one; without this they accumulate for the life
        of the process, which the in-process multi-node tests do five at a time.
        """
        with self._lock:
            self._conn.close()

    # ---- internals; every one of these requires the lock to be held ----

    def _insert_locked(self, record: FileRecord) -> None:
        self._conn.execute(
            f"INSERT INTO files ({_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self._to_row(record),
        )
        self._write_tag_state(record)

    def _write(self, record: FileRecord) -> None:
        self._conn.execute(
            """
            UPDATE files SET
                name = ?, name_ts = ?, name_node = ?,
                content_hash = ?, content_hash_ts = ?, content_hash_node = ?,
                tombstone = ?, tombstone_ts = ?, tombstone_node = ?,
                created_at = ?, updated_at = ?, vector_clock = ?
            WHERE file_id = ?
            """,
            (
                record.name,
                _dt_to_str(record.name_register.timestamp),
                record.name_register.node_id,
                record.content_hash,
                _dt_to_str(record.content_hash_register.timestamp),
                record.content_hash_register.node_id,
                int(record.tombstone),
                _dt_to_str(record.tombstone_register.timestamp),
                record.tombstone_register.node_id,
                # `merged` takes the earlier created_at, so a merge can
                # legitimately change it. Omitting it here would silently
                # discard that, leaving the stored record different from the
                # one the merge just computed.
                _dt_to_str(record.created_at),
                _dt_to_str(record.updated_at),
                json.dumps(record.vector_clock.counters, sort_keys=True),
                str(record.file_id),
            ),
        )
        self._write_tag_state(record)

    def _to_row(self, record: FileRecord) -> tuple:
        return (
            str(record.file_id),
            record.name,
            _dt_to_str(record.name_register.timestamp),
            record.name_register.node_id,
            record.content_hash,
            _dt_to_str(record.content_hash_register.timestamp),
            record.content_hash_register.node_id,
            int(record.tombstone),
            _dt_to_str(record.tombstone_register.timestamp),
            record.tombstone_register.node_id,
            _dt_to_str(record.created_at),
            _dt_to_str(record.updated_at),
            json.dumps(record.vector_clock.counters, sort_keys=True),
        )

    def _write_tag_state(self, record: FileRecord) -> None:
        file_id = str(record.file_id)
        for table, occurrences in (
            ("file_tag_adds", record.tag_set.adds),
            ("file_tag_removes", record.tag_set.removes),
        ):
            self._conn.execute(f"DELETE FROM {table} WHERE file_id = ?", (file_id,))
            self._conn.executemany(
                f"INSERT INTO {table} (file_id, tag, node_id, counter) "
                "VALUES (?, ?, ?, ?)",
                [(file_id, tag, node_id, counter) for tag, node_id, counter in occurrences],
            )

    def _row_to_record(self, row: tuple) -> FileRecord:
        (
            file_id_str,
            name,
            name_ts,
            name_node,
            content_hash,
            content_hash_ts,
            content_hash_node,
            tombstone,
            tombstone_ts,
            tombstone_node,
            created_at,
            updated_at,
            vector_clock,
        ) = row

        return FileRecord(
            file_id=uuid.UUID(file_id_str),
            name_register=LWWRegister(name, _str_to_dt(name_ts), name_node),
            content_hash_register=LWWRegister(
                content_hash, _str_to_dt(content_hash_ts), content_hash_node
            ),
            tombstone_register=LWWRegister(
                bool(tombstone), _str_to_dt(tombstone_ts), tombstone_node
            ),
            tag_set=OrSet(
                adds=self._read_occurrences("file_tag_adds", file_id_str),
                removes=self._read_occurrences("file_tag_removes", file_id_str),
            ),
            vector_clock=VectorClock(json.loads(vector_clock)),
            created_at=_str_to_dt(created_at),
            updated_at=_str_to_dt(updated_at),
        )

    def _read_occurrences(self, table: str, file_id: str) -> set[tuple[str, str, int]]:
        return {
            (tag, node_id, counter)
            for tag, node_id, counter in self._conn.execute(
                f"SELECT tag, node_id, counter FROM {table} WHERE file_id = ?",
                (file_id,),
            ).fetchall()
        }
