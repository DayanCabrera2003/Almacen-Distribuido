"""SQLite-backed record of when each orphaned blob became a GC candidate."""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime
from pathlib import Path

# Keyed by content hash rather than living as columns on `files`: the row has to
# outlive every record that ever pointed at the blob, including purged ones.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS blob_candidates (
    content_hash TEXT PRIMARY KEY,
    unreferenced_since TEXT NOT NULL
);
"""


class BlobGCState:
    """Tracks when each orphaned blob became a candidate for deletion.

    Persisted because it cannot be recomputed: once the last referrer is gone,
    nothing else remembers when that happened. Forgetting it on restart would be
    safe (every timer restarts, so blobs are kept longer, never deleted early),
    but a node that restarts often would then never collect anything.
    """

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.Lock()

    def mark_candidates(self, hashes: set[str], now: datetime) -> None:
        """Record newly unreferenced blobs, leaving existing timers alone.

        `INSERT OR IGNORE` is the point: re-marking a blob that is already a
        candidate must not restart its clock, or a blob seen as orphaned on every
        round would never reach its deadline.
        """
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO blob_candidates "
                "(content_hash, unreferenced_since) VALUES (?, ?)",
                [(content_hash, now.isoformat()) for content_hash in hashes],
            )

    def clear(self, hashes: set[str]) -> None:
        """Forget these candidates: they regained a referrer, or were deleted."""
        with self._lock, self._conn:
            self._conn.executemany(
                "DELETE FROM blob_candidates WHERE content_hash = ?",
                [(content_hash,) for content_hash in hashes],
            )

    def candidates(self) -> dict[str, datetime]:
        """Every candidate, with when it became unreferenced."""
        with self._lock:
            return {
                content_hash: datetime.fromisoformat(since)
                for content_hash, since in self._conn.execute(
                    "SELECT content_hash, unreferenced_since FROM blob_candidates"
                ).fetchall()
            }

    def candidates_since(self, deadline: datetime) -> set[str]:
        """Blobs that became candidates at or before `deadline`."""
        return {
            content_hash
            for content_hash, since in self.candidates().items()
            if since <= deadline
        }

    def close(self) -> None:
        with self._lock:
            self._conn.close()
