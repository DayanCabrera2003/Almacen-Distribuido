# Mutual exclusion for a shared SQLite connection under concurrent writers

## Problem

`MetadataStore` (`almacen/storage/metadata_store.py`) wraps a single
`sqlite3.Connection` opened with `check_same_thread=False`, so it can be called
from multiple threads — which is exactly what happens once this store is called
from FastAPI's synchronous route handlers, since FastAPI dispatches sync
handlers to a thread pool. Each write method (`insert`, `update`) executes more
than one SQL statement inside a single logical operation (e.g. `update()` does
an `UPDATE files ...`, then a `DELETE FROM file_tags ...`, then an `INSERT INTO
file_tags ...`), wrapped in an implicit transaction via `with self._conn:`.

SQLite's per-connection transaction state is exactly that — per *connection*,
not per *thread*. When two threads call methods on the same `MetadataStore`
instance concurrently, their individual statements can interleave into what
SQLite sees as a single connection's activity, even though the two callers
believe they're each executing an atomic, isolated operation. This is the
classic **critical section problem**: multiple concurrent "processes" (here,
threads) each execute a sequence of operations that must appear atomic with
respect to each other, over a piece of shared, mutable state (the connection's
transaction/statement execution).

Without synchronization, this produces genuinely observable corruption, not
just a theoretical race. An empirical test (20-50 concurrent threads each
calling `insert()` once) reliably reproduced:
- `sqlite3.InterfaceError` ("bad parameter or other API misuse") from
  interleaved statement execution on the shared connection object.
- Missing records: fewer rows in the database than the number of successful
  `insert()` calls.
- Records with an incomplete/wrong set of associated tags (partial writes from
  one thread's multi-statement operation visible mixed with another's).

## Solution

Guard every public method that touches `self._conn` with a single
`threading.Lock`, held for the *entire* operation (not just the SQL
execution), so at most one thread is inside a connection-touching method at
any time:

```python
def __init__(self, db_path: Path) -> None:
    ...
    self._lock = threading.Lock()

def insert(self, record: FileRecord) -> None:
    with self._lock, self._conn:
        ...

def get(self, file_id: uuid.UUID) -> FileRecord | None:
    with self._lock:
        ...

def update(self, record: FileRecord) -> None:
    with self._lock, self._conn:
        ...

def list_live(self) -> list[FileRecord]:
    with self._lock:
        ...
```

This reduces the critical section to "the whole method body," which is
coarser-grained than necessary for correctness (a reader-writer lock would let
concurrent `get`/`list_live` calls proceed in parallel, since SQLite's WAL
mode already supports concurrent readers), but is the simplest correct
solution and appropriate at this project's scale: a single-node store handling
a small number of concurrent requests in a demo/portfolio-scale deployment,
not a high-throughput production database.

**Why not just rely on WAL mode?** WAL (Write-Ahead Logging) mode gives
SQLite's *multi-reader/single-writer* concurrency model — but that model
assumes one OS-level connection per concurrent actor (or a connection pool),
each managing its own transaction boundary correctly. It does not make a
*single connection object*, mutated by multiple threads with no
synchronization of their own, safe. WAL solves cross-connection concurrency;
it does not solve cross-thread misuse of one connection.

**Correctness constraints checked for this fix:**
- No deadlock risk: `threading.Lock` is non-reentrant, but no locked method
  calls another locked public method on `self` — each acquires the lock
  exactly once per call.
- The lock must be held for the full transaction, not released between the
  SQL statements and the transaction's commit — `with self._lock, self._conn:`
  achieves this because `self._conn.__exit__` (which commits or rolls back)
  runs before `self._lock` releases (context managers exit in reverse
  acquisition order).
- Private helpers (`_insert_tags`, `_row_to_record`) are correctly *not*
  separately locked — they are only ever called from within an
  already-locked public method, so adding a second lock acquisition there
  would only add overhead (and, if the lock were reentrant-unsafe in some
  other design, risk deadlock).

**Verified empirically, not just by inspection:** the regression test
(`test_concurrent_inserts_from_multiple_threads_all_persist_correctly`,
20 threads) was confirmed to fail reliably (10/10 runs) with the lock removed,
and pass reliably (3/3 runs) with it restored — establishing the test is a
genuine regression guard rather than one that passes regardless of the fix.

**Deferred, not solved here:** a reader-writer lock (allowing concurrent
`get`/`list_live` reads to proceed in parallel while writes remain
exclusive) would better exploit WAL mode's read concurrency. Not implemented
now — YAGNI at this project's scale — but worth revisiting if `MetadataStore`
becomes a throughput bottleneck in a later phase (e.g. once it's exercised by
concurrent replication/anti-entropy traffic in Phase 4).
