# Phase 1 — Single-Node Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fully working single-node tag-based file store: upload/download files, rename/update content, add/remove tags, query by tag combination, and delete (tombstone) — all through a REST API, with no networking/clustering yet.

**Architecture:** Layered per the approved design spec (`docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md`, §14): a pure `domain` entity (`FileRecord`, no I/O), a `storage` layer (filesystem-backed `BlobStore` for content, SQLite-backed `MetadataStore` for records, `TagIndex` for tag-combination queries built on top of `MetadataStore`), and an `api` layer (FastAPI routers translating HTTP to storage/domain calls). No CRDTs, vector clocks, or gossip yet — those are Phase 3/4 per the roadmap; tags here are a plain Python `set[str]` and conflicts cannot occur because there is exactly one node.

**Tech Stack:** Python 3.11+, FastAPI + uvicorn, `python-multipart` (file uploads), stdlib `sqlite3` (WAL mode), pytest + httpx (FastAPI `TestClient`).

---

## Decisions made while planning (not fully pinned down by the spec)

The spec review flagged two REST shape details as "to be settled when the phase-1 API plan is written" (see spec, final review notes). Resolved here:

- **`GET /files` (list/query) response shape:** returns an array of full file metadata objects (`file_id`, `name`, `content_hash`, `tags`, `created_at`, `updated_at`) — not just IDs, and not the raw content bytes (those come from `GET /files/{file_id}`, which is content-only, per spec §12).
- **No separate "get one file's metadata" endpoint.** The spec's API surface (§12) lists exactly nine endpoints; none of them is "get metadata for a single file" without downloading content or listing. Adding one would be scope creep beyond the approved spec (YAGNI) — skip it for Phase 1.
- **`GET /files` with no `tags` param:** lists all live (non-tombstoned) files, ignoring `mode`. This matches ordinary REST "list all" conventions.
- **`mode` default:** `"and"` when `tags` is given but `mode` is omitted.
- **`remove_tag` on a tag the file doesn't have:** no-op, not an error (consistent with the OR-Set semantics Phase 3 will introduce — removing an unseen element is harmless).

---

## File Structure

```
pyproject.toml
almacen/
  __init__.py
  config.py                  # Settings: data_dir, db_path
  main.py                    # FastAPI app factory + module-level `app`
  domain/
    __init__.py
    file_record.py           # FileRecord entity (no I/O)
  storage/
    __init__.py
    blob_store.py             # filesystem content-addressed blob storage
    metadata_store.py         # SQLite CRUD for FileRecord
    tag_index.py               # tag-combination queries
  api/
    __init__.py
    deps.py                    # FastAPI dependency providers + get_live_record helper
    schemas.py                 # Pydantic models + FileRecord -> FileMetadata mapping
    routers/
      __init__.py
      files.py                 # POST/GET/PATCH/DELETE /files, GET /files (list)
      tags.py                  # GET/POST /files/{id}/tags, DELETE /files/{id}/tags/{tag}
tests/
  unit/
    domain/
      test_file_record.py
  integration/
    storage/
      test_blob_store.py
      test_metadata_store.py
      test_tag_index.py
    api/
      test_files_lifecycle.py
README.md                    # setup / run / test instructions
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `almacen/__init__.py`
- Create: `almacen/domain/__init__.py`
- Create: `almacen/storage/__init__.py`
- Create: `almacen/api/__init__.py`
- Create: `almacen/api/routers/__init__.py`
- Create: `tests/unit/domain/__init__.py`
- Create: `tests/integration/storage/__init__.py`
- Create: `tests/integration/api/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "almacen"
version = "0.1.0"
description = "Tag-based distributed file store"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.32",
    "python-multipart>=0.0.12",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "httpx>=0.27",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["almacen*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create empty `__init__.py` files**

Create each of the `__init__.py` files listed above as empty files (they just mark the directories as packages).

- [ ] **Step 3: Create a venv, install the project in editable/dev mode, and verify pytest runs (with nothing to collect yet)**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Expected: `pip install` succeeds; `pytest` reports `no tests ran` (no test files exist yet) — this confirms the package installs and pytest is wired to `tests/`.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml almacen tests
git commit -m "Add project scaffolding for single-node core"
```

---

### Task 2: Domain — `FileRecord` entity

**Files:**
- Create: `almacen/domain/file_record.py`
- Test: `tests/unit/domain/test_file_record.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/domain/test_file_record.py
import uuid

from almacen.domain.file_record import FileRecord


def test_new_creates_record_with_generated_id_and_empty_tags_by_default():
    record = FileRecord.new(name="report.txt", content_hash="abc123")

    assert isinstance(record.file_id, uuid.UUID)
    assert record.name == "report.txt"
    assert record.content_hash == "abc123"
    assert record.tags == set()
    assert record.tombstone is False
    assert record.tombstone_at is None


def test_new_copies_tags_without_aliasing_caller_set():
    original_tags = {"invoice"}
    record = FileRecord.new(name="a.txt", content_hash="h1", tags=original_tags)

    record.add_tag("draft")

    assert original_tags == {"invoice"}


def test_add_tag_is_idempotent():
    record = FileRecord.new(name="a.txt", content_hash="h1")

    record.add_tag("invoice")
    record.add_tag("invoice")

    assert record.tags == {"invoice"}


def test_remove_tag_on_absent_tag_is_a_noop():
    record = FileRecord.new(name="a.txt", content_hash="h1")

    record.remove_tag("nonexistent")

    assert record.tags == set()


def test_rename_updates_name_and_updated_at():
    record = FileRecord.new(name="a.txt", content_hash="h1")
    original_updated_at = record.updated_at

    record.rename("b.txt")

    assert record.name == "b.txt"
    assert record.updated_at >= original_updated_at


def test_update_content_replaces_content_hash():
    record = FileRecord.new(name="a.txt", content_hash="h1")

    record.update_content("h2")

    assert record.content_hash == "h2"


def test_mark_deleted_sets_tombstone_and_timestamp():
    record = FileRecord.new(name="a.txt", content_hash="h1")

    record.mark_deleted()

    assert record.tombstone is True
    assert record.tombstone_at is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/domain/test_file_record.py -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'almacen.domain.file_record'`

- [ ] **Step 3: Write the implementation**

```python
# almacen/domain/file_record.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/domain/test_file_record.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/domain/file_record.py tests/unit/domain/test_file_record.py
git commit -m "Add FileRecord domain entity"
```

---

### Task 3: Storage — `BlobStore`

**Files:**
- Create: `almacen/storage/blob_store.py`
- Test: `tests/integration/storage/test_blob_store.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/storage/test_blob_store.py
import hashlib
from pathlib import Path

import pytest

from almacen.storage.blob_store import BlobStore


@pytest.fixture
def blob_store(tmp_path: Path) -> BlobStore:
    return BlobStore(tmp_path / "blobs")


def test_put_returns_sha256_hex_digest(blob_store: BlobStore):
    content = b"hello world"
    expected_hash = hashlib.sha256(content).hexdigest()

    result = blob_store.put(content)

    assert result == expected_hash


def test_get_returns_previously_put_content(blob_store: BlobStore):
    content = b"hello world"
    content_hash = blob_store.put(content)

    assert blob_store.get(content_hash) == content


def test_get_returns_none_for_unknown_hash(blob_store: BlobStore):
    assert blob_store.get("deadbeef" * 8) is None


def test_put_is_idempotent_for_identical_content(blob_store: BlobStore):
    content = b"duplicate"

    first_hash = blob_store.put(content)
    second_hash = blob_store.put(content)

    assert first_hash == second_hash
    assert blob_store.get(first_hash) == content


def test_exists_reflects_stored_content(blob_store: BlobStore):
    content_hash = blob_store.put(b"present")

    assert blob_store.exists(content_hash) is True
    assert blob_store.exists("0" * 64) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/storage/test_blob_store.py -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'almacen.storage.blob_store'`

- [ ] **Step 3: Write the implementation**

```python
# almacen/storage/blob_store.py
"""Content-addressed blob storage on the local filesystem."""
from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path


class BlobStore:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def put(self, content: bytes) -> str:
        content_hash = hashlib.sha256(content).hexdigest()
        target = self._path_for(content_hash)
        if target.exists():
            return content_hash

        tmp_path = self._root / f".tmp-{uuid.uuid4().hex}"
        tmp_path.write_bytes(content)
        os.replace(tmp_path, target)
        return content_hash

    def get(self, content_hash: str) -> bytes | None:
        path = self._path_for(content_hash)
        if not path.exists():
            return None
        return path.read_bytes()

    def exists(self, content_hash: str) -> bool:
        return self._path_for(content_hash).exists()

    def _path_for(self, content_hash: str) -> Path:
        return self._root / content_hash
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/integration/storage/test_blob_store.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/storage/blob_store.py tests/integration/storage/test_blob_store.py
git commit -m "Add filesystem-backed BlobStore"
```

---

### Task 4: Storage — `MetadataStore`

**Files:**
- Create: `almacen/storage/metadata_store.py`
- Test: `tests/integration/storage/test_metadata_store.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/storage/test_metadata_store.py
import uuid
from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore


@pytest.fixture
def store(tmp_path: Path) -> MetadataStore:
    return MetadataStore(tmp_path / "metadata.db")


def test_insert_then_get_returns_equivalent_record(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x", "y"})

    store.insert(record)
    fetched = store.get(record.file_id)

    assert fetched is not None
    assert fetched.file_id == record.file_id
    assert fetched.name == "a.txt"
    assert fetched.content_hash == "h1"
    assert fetched.tags == {"x", "y"}
    assert fetched.tombstone is False


def test_get_returns_none_for_unknown_id(store: MetadataStore):
    assert store.get(uuid.uuid4()) is None


def test_update_persists_renamed_and_retagged_record(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})
    store.insert(record)

    record.rename("b.txt")
    record.add_tag("y")
    record.remove_tag("x")
    store.update(record)

    fetched = store.get(record.file_id)
    assert fetched.name == "b.txt"
    assert fetched.tags == {"y"}


def test_update_persists_tombstone(store: MetadataStore):
    record = FileRecord.new(name="a.txt", content_hash="h1")
    store.insert(record)

    record.mark_deleted()
    store.update(record)

    fetched = store.get(record.file_id)
    assert fetched.tombstone is True
    assert fetched.tombstone_at is not None


def test_data_survives_reopening_the_same_db_file(tmp_path: Path):
    db_path = tmp_path / "metadata.db"
    record = FileRecord.new(name="a.txt", content_hash="h1", tags={"x"})

    MetadataStore(db_path).insert(record)
    reopened = MetadataStore(db_path)

    fetched = reopened.get(record.file_id)
    assert fetched is not None
    assert fetched.tags == {"x"}


def test_list_live_excludes_tombstoned_records(store: MetadataStore):
    live = FileRecord.new(name="live.txt", content_hash="h1")
    deleted = FileRecord.new(name="deleted.txt", content_hash="h2")
    deleted.mark_deleted()
    store.insert(live)
    store.insert(deleted)

    results = store.list_live()

    assert {r.file_id for r in results} == {live.file_id}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/storage/test_metadata_store.py -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'almacen.storage.metadata_store'`

- [ ] **Step 3: Write the implementation**

```python
# almacen/storage/metadata_store.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/integration/storage/test_metadata_store.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/storage/metadata_store.py tests/integration/storage/test_metadata_store.py
git commit -m "Add SQLite-backed MetadataStore"
```

---

### Task 5: Storage — `TagIndex`

**Files:**
- Create: `almacen/storage/tag_index.py`
- Test: `tests/integration/storage/test_tag_index.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/storage/test_tag_index.py
from pathlib import Path

import pytest

from almacen.domain.file_record import FileRecord
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


@pytest.fixture
def index(tmp_path: Path) -> TagIndex:
    metadata_store = MetadataStore(tmp_path / "metadata.db")
    invoice_only = FileRecord.new(name="a.txt", content_hash="h1", tags={"invoice"})
    invoice_and_draft = FileRecord.new(
        name="b.txt", content_hash="h2", tags={"invoice", "draft"}
    )
    draft_only = FileRecord.new(name="c.txt", content_hash="h3", tags={"draft"})
    deleted_invoice = FileRecord.new(name="d.txt", content_hash="h4", tags={"invoice"})
    deleted_invoice.mark_deleted()
    for record in (invoice_only, invoice_and_draft, draft_only, deleted_invoice):
        metadata_store.insert(record)
    return TagIndex(metadata_store)


def test_and_mode_returns_files_with_all_tags(index: TagIndex):
    results = index.query(["invoice", "draft"], mode="and")
    assert [r.name for r in results] == ["b.txt"]


def test_or_mode_returns_files_with_any_tag(index: TagIndex):
    results = index.query(["invoice", "draft"], mode="or")
    assert {r.name for r in results} == {"a.txt", "b.txt", "c.txt"}


def test_no_tags_returns_all_live_files(index: TagIndex):
    results = index.query(tags=None)
    assert {r.name for r in results} == {"a.txt", "b.txt", "c.txt"}


def test_query_excludes_tombstoned_files(index: TagIndex):
    results = index.query(["invoice"], mode="or")
    assert "d.txt" not in {r.name for r in results}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/storage/test_tag_index.py -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'almacen.storage.tag_index'`

- [ ] **Step 3: Write the implementation**

```python
# almacen/storage/tag_index.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/integration/storage/test_tag_index.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/storage/tag_index.py tests/integration/storage/test_tag_index.py
git commit -m "Add TagIndex for tag-combination queries"
```

---

### Task 6: `config.py` — node settings

**Files:**
- Create: `almacen/config.py`

No dedicated test file: this is a thin dataclass with no branching logic worth a unit test on its own; it is exercised indirectly by Task 9's integration tests, which construct `Settings` directly.

- [ ] **Step 1: Write the implementation**

```python
# almacen/config.py
"""Node configuration for the single-node core."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path

    @classmethod
    def from_env(cls) -> "Settings":
        base_dir = Path(os.environ.get("ALMACEN_DATA_DIR", "./data"))
        return cls(
            data_dir=base_dir / "blobs",
            db_path=base_dir / "metadata.db",
        )
```

- [ ] **Step 2: Sanity check it imports cleanly**

Run: `python -c "from almacen.config import Settings; print(Settings.from_env())"`
Expected: prints a `Settings(data_dir=..., db_path=...)` line, no error.

- [ ] **Step 3: Commit**

```bash
git add almacen/config.py
git commit -m "Add node Settings configuration"
```

---

### Task 7: API — schemas and dependency providers

**Files:**
- Create: `almacen/api/schemas.py`
- Create: `almacen/api/deps.py`

No dedicated test file: these are thin translation/wiring layers exercised end-to-end by Task 9 and Task 10's API tests. Testing them in isolation would just re-test FastAPI/Pydantic itself.

- [ ] **Step 1: Write `schemas.py`**

```python
# almacen/api/schemas.py
"""Pydantic request/response models for the REST API."""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel

from almacen.domain.file_record import FileRecord


class FileMetadata(BaseModel):
    file_id: uuid.UUID
    name: str
    content_hash: str
    tags: list[str]
    created_at: datetime
    updated_at: datetime


class TagsResponse(BaseModel):
    tags: list[str]


class AddTagsRequest(BaseModel):
    tags: list[str]


def to_file_metadata(record: FileRecord) -> FileMetadata:
    return FileMetadata(
        file_id=record.file_id,
        name=record.name,
        content_hash=record.content_hash,
        tags=sorted(record.tags),
        created_at=record.created_at,
        updated_at=record.updated_at,
    )
```

- [ ] **Step 2: Write `deps.py`**

```python
# almacen/api/deps.py
"""FastAPI dependency providers and shared request-handling helpers."""
from __future__ import annotations

import uuid

from fastapi import HTTPException, Request

from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


def get_blob_store(request: Request) -> BlobStore:
    return request.app.state.blob_store


def get_metadata_store(request: Request) -> MetadataStore:
    return request.app.state.metadata_store


def get_tag_index(request: Request) -> TagIndex:
    return request.app.state.tag_index


def get_live_record(metadata_store: MetadataStore, file_id: uuid.UUID) -> FileRecord:
    """Look up a file, raising 404 if it's missing or tombstoned (spec §10: a
    tombstoned file is indistinguishable from an unknown one via the API)."""
    record = metadata_store.get(file_id)
    if record is None or record.tombstone:
        raise HTTPException(status_code=404, detail="file not found")
    return record
```

- [ ] **Step 3: Sanity check both modules import cleanly**

Run: `python -c "import almacen.api.schemas, almacen.api.deps"`
Expected: no error.

- [ ] **Step 4: Commit**

```bash
git add almacen/api/schemas.py almacen/api/deps.py
git commit -m "Add API schemas and dependency providers"
```

---

### Task 8: `main.py` — FastAPI app factory (routers wired in Tasks 9-10)

**Files:**
- Create: `almacen/main.py`

This task creates the app factory with empty router wiring so Tasks 9-10 can add
routers incrementally and test against a real app each time.

- [ ] **Step 1: Write `main.py`**

```python
# almacen/main.py
"""FastAPI application bootstrap for a single node."""
from __future__ import annotations

from fastapi import FastAPI

from almacen.config import Settings
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Almacen Distribuido")

    app.state.blob_store = BlobStore(settings.data_dir)
    app.state.metadata_store = MetadataStore(settings.db_path)
    app.state.tag_index = TagIndex(app.state.metadata_store)

    return app


app = create_app()
```

- [ ] **Step 2: Sanity check the app starts**

Run: `python -c "from almacen.main import create_app; create_app()"`
Expected: no error (creates `./data/blobs` and `./data/metadata.db` in the current
directory as a side effect — that's expected for the default `from_env()` config;
delete `./data` afterward if you don't want it lingering: `rm -rf data`).

- [ ] **Step 3: Commit**

```bash
git add almacen/main.py
git commit -m "Add FastAPI app factory"
```

---

### Task 9: API — files router: upload, download, update, delete

**Files:**
- Create: `almacen/api/routers/files.py`
- Modify: `almacen/main.py` (wire the router)
- Test: `tests/integration/api/test_files_lifecycle.py` (created here, extended in Task 10)

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/api/test_files_lifecycle.py
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from almacen.config import Settings
from almacen.main import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    app = create_app(settings)
    return TestClient(app)


def test_upload_then_download_roundtrips_content(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("report.txt", b"hello world", "text/plain")},
        data={"name": "report.txt", "tags": "invoice,draft"},
    )
    assert upload.status_code == 201
    body = upload.json()
    assert sorted(body["tags"]) == ["draft", "invoice"]
    file_id = body["file_id"]

    download = client.get(f"/files/{file_id}")
    assert download.status_code == 200
    assert download.content == b"hello world"


def test_download_unknown_file_returns_404(client: TestClient):
    response = client.get("/files/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


def test_patch_renames_and_replaces_content(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"v1", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    rename = client.patch(f"/files/{file_id}", data={"name": "b.txt"})
    assert rename.status_code == 200
    assert rename.json()["name"] == "b.txt"

    update_content = client.patch(
        f"/files/{file_id}",
        files={"file": ("b.txt", b"v2", "text/plain")},
    )
    assert update_content.status_code == 200
    assert client.get(f"/files/{file_id}").content == b"v2"


def test_delete_tombstones_and_all_endpoints_then_404(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"v1", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    delete = client.delete(f"/files/{file_id}")
    assert delete.status_code == 204

    assert client.get(f"/files/{file_id}").status_code == 404
    assert client.patch(f"/files/{file_id}", data={"name": "x"}).status_code == 404
    assert client.delete(f"/files/{file_id}").status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/api/test_files_lifecycle.py -v`
Expected: FAIL — 404s become connection/route errors (no `/files` route registered
yet), since `files.py` doesn't exist and isn't wired into `main.py`.

- [ ] **Step 3: Write `files.py`**

```python
# almacen/api/routers/files.py
"""REST endpoints for file upload, download, update, and delete."""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, UploadFile
from fastapi.responses import Response

from almacen.api.deps import get_blob_store, get_live_record, get_metadata_store
from almacen.api.schemas import FileMetadata, to_file_metadata
from almacen.domain.file_record import FileRecord
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

router = APIRouter(prefix="/files", tags=["files"])


def _parse_tags(tags: str | None) -> set[str]:
    if not tags:
        return set()
    return {tag.strip() for tag in tags.split(",") if tag.strip()}


@router.post("", response_model=FileMetadata, status_code=201)
async def upload_file(
    file: UploadFile,
    name: Annotated[str | None, Form()] = None,
    tags: Annotated[str | None, Form()] = None,
    blob_store: BlobStore = Depends(get_blob_store),
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> FileMetadata:
    content = await file.read()
    content_hash = blob_store.put(content)
    record = FileRecord.new(
        name=name or file.filename or "untitled",
        content_hash=content_hash,
        tags=_parse_tags(tags),
    )
    metadata_store.insert(record)
    return to_file_metadata(record)


@router.get("/{file_id}")
def download_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStore = Depends(get_metadata_store),
    blob_store: BlobStore = Depends(get_blob_store),
) -> Response:
    record = get_live_record(metadata_store, file_id)
    content = blob_store.get(record.content_hash)
    if content is None:
        # Would indicate local storage corruption/bug, not a client error.
        raise RuntimeError(f"blob {record.content_hash} missing for known file {file_id}")
    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{record.name}"'},
    )


@router.patch("/{file_id}", response_model=FileMetadata)
async def update_file(
    file_id: uuid.UUID,
    file: Annotated[UploadFile | None, File()] = None,
    name: Annotated[str | None, Form()] = None,
    metadata_store: MetadataStore = Depends(get_metadata_store),
    blob_store: BlobStore = Depends(get_blob_store),
) -> FileMetadata:
    record = get_live_record(metadata_store, file_id)
    if name is not None:
        record.rename(name)
    if file is not None:
        content = await file.read()
        content_hash = blob_store.put(content)
        record.update_content(content_hash)
    metadata_store.update(record)
    return to_file_metadata(record)


@router.delete("/{file_id}", status_code=204)
def delete_file(
    file_id: uuid.UUID,
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> None:
    record = get_live_record(metadata_store, file_id)
    record.mark_deleted()
    metadata_store.update(record)
```

- [ ] **Step 4: Wire the router into `main.py`**

```python
# almacen/main.py
"""FastAPI application bootstrap for a single node."""
from __future__ import annotations

from fastapi import FastAPI

from almacen.api.routers import files as files_router
from almacen.config import Settings
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from almacen.storage.tag_index import TagIndex


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Almacen Distribuido")

    app.state.blob_store = BlobStore(settings.data_dir)
    app.state.metadata_store = MetadataStore(settings.db_path)
    app.state.tag_index = TagIndex(app.state.metadata_store)

    app.include_router(files_router.router)

    return app


app = create_app()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/integration/api/test_files_lifecycle.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add almacen/api/routers/files.py almacen/main.py tests/integration/api/test_files_lifecycle.py
git commit -m "Add files router: upload, download, update, delete"
```

---

### Task 10: API — files router: list/query by tags

**Files:**
- Modify: `almacen/api/routers/files.py`
- Modify: `tests/integration/api/test_files_lifecycle.py`

- [ ] **Step 1: Add the failing tests**

Append to `tests/integration/api/test_files_lifecycle.py`:

```python
def test_list_with_and_mode_returns_intersection(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "invoice,draft"},
    )
    client.post(
        "/files",
        files={"file": ("b.txt", b"b", "text/plain")},
        data={"name": "b.txt", "tags": "invoice"},
    )

    response = client.get("/files", params={"tags": "invoice,draft", "mode": "and"})

    assert response.status_code == 200
    assert [f["name"] for f in response.json()] == ["a.txt"]


def test_list_with_or_mode_returns_union(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "x"},
    )
    client.post(
        "/files",
        files={"file": ("b.txt", b"b", "text/plain")},
        data={"name": "b.txt", "tags": "y"},
    )

    response = client.get("/files", params={"tags": "x,y", "mode": "or"})

    assert {f["name"] for f in response.json()} == {"a.txt", "b.txt"}


def test_list_with_no_tags_returns_all_live_files(client: TestClient):
    client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )

    response = client.get("/files")

    assert response.status_code == 200
    assert [f["name"] for f in response.json()] == ["a.txt"]


def test_list_excludes_deleted_files(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "x"},
    )
    file_id = upload.json()["file_id"]
    client.delete(f"/files/{file_id}")

    response = client.get("/files", params={"tags": "x"})

    assert response.json() == []


def test_list_rejects_invalid_mode(client: TestClient):
    response = client.get("/files", params={"tags": "x", "mode": "xor"})
    assert response.status_code == 400
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/api/test_files_lifecycle.py -v`
Expected: FAIL — `GET /files` (no path param) isn't a registered route, so these
requests 404/405 instead of returning the expected filtered results.

- [ ] **Step 3: Add the `list_files` endpoint to `files.py`**

Add this import to the top of `almacen/api/routers/files.py`:

```python
from fastapi import HTTPException
```

and this import:

```python
from almacen.api.deps import get_tag_index
from almacen.storage.tag_index import TagIndex
```

Then add the endpoint (order matters: FastAPI needs `""` registered — it does
not conflict with `"/{file_id}"` since they're different path shapes, but keep
this defined before `download_file` for readability):

```python
@router.get("", response_model=list[FileMetadata])
def list_files(
    tags: str | None = None,
    mode: str = "and",
    tag_index: TagIndex = Depends(get_tag_index),
) -> list[FileMetadata]:
    if mode not in ("and", "or"):
        raise HTTPException(status_code=400, detail="mode must be 'and' or 'or'")
    tag_list = list(_parse_tags(tags)) if tags else None
    records = tag_index.query(tag_list, mode=mode)  # type: ignore[arg-type]
    return [to_file_metadata(r) for r in records]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/integration/api/test_files_lifecycle.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add almacen/api/routers/files.py tests/integration/api/test_files_lifecycle.py
git commit -m "Add tag-combination list/query endpoint"
```

---

### Task 11: API — tags router

**Files:**
- Create: `almacen/api/routers/tags.py`
- Modify: `almacen/main.py` (wire the router)
- Modify: `tests/integration/api/test_files_lifecycle.py`

- [ ] **Step 1: Add the failing tests**

Append to `tests/integration/api/test_files_lifecycle.py`:

```python
def test_add_list_and_remove_tags(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt", "tags": "invoice"},
    )
    file_id = upload.json()["file_id"]

    list_tags = client.get(f"/files/{file_id}/tags")
    assert list_tags.status_code == 200
    assert list_tags.json()["tags"] == ["invoice"]

    add_tags = client.post(f"/files/{file_id}/tags", json={"tags": ["draft", "final"]})
    assert add_tags.status_code == 200
    assert sorted(add_tags.json()["tags"]) == ["draft", "final", "invoice"]

    remove_tag = client.delete(f"/files/{file_id}/tags/draft")
    assert remove_tag.status_code == 200
    assert sorted(remove_tag.json()["tags"]) == ["final", "invoice"]


def test_remove_absent_tag_is_a_noop_not_an_error(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]

    response = client.delete(f"/files/{file_id}/tags/nonexistent")

    assert response.status_code == 200
    assert response.json()["tags"] == []


def test_tag_endpoints_404_on_deleted_file(client: TestClient):
    upload = client.post(
        "/files",
        files={"file": ("a.txt", b"a", "text/plain")},
        data={"name": "a.txt"},
    )
    file_id = upload.json()["file_id"]
    client.delete(f"/files/{file_id}")

    assert client.get(f"/files/{file_id}/tags").status_code == 404
    assert client.post(f"/files/{file_id}/tags", json={"tags": ["x"]}).status_code == 404
    assert client.delete(f"/files/{file_id}/tags/x").status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/api/test_files_lifecycle.py -v`
Expected: FAIL — no `/files/{file_id}/tags` routes registered yet.

- [ ] **Step 3: Write `tags.py`**

```python
# almacen/api/routers/tags.py
"""REST endpoints for adding, removing, and listing a file's tags."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends

from almacen.api.deps import get_live_record, get_metadata_store
from almacen.api.schemas import AddTagsRequest, TagsResponse
from almacen.storage.metadata_store import MetadataStore

router = APIRouter(prefix="/files/{file_id}/tags", tags=["tags"])


@router.get("", response_model=TagsResponse)
def list_tags(
    file_id: uuid.UUID,
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> TagsResponse:
    record = get_live_record(metadata_store, file_id)
    return TagsResponse(tags=sorted(record.tags))


@router.post("", response_model=TagsResponse)
def add_tags(
    file_id: uuid.UUID,
    body: AddTagsRequest,
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> TagsResponse:
    record = get_live_record(metadata_store, file_id)
    for tag in body.tags:
        record.add_tag(tag)
    metadata_store.update(record)
    return TagsResponse(tags=sorted(record.tags))


@router.delete("/{tag}", response_model=TagsResponse)
def remove_tag(
    file_id: uuid.UUID,
    tag: str,
    metadata_store: MetadataStore = Depends(get_metadata_store),
) -> TagsResponse:
    record = get_live_record(metadata_store, file_id)
    record.remove_tag(tag)
    metadata_store.update(record)
    return TagsResponse(tags=sorted(record.tags))
```

- [ ] **Step 4: Wire the router into `main.py`**

```python
# almacen/main.py — update the import and include_router lines
from almacen.api.routers import files as files_router
from almacen.api.routers import tags as tags_router
```

```python
    app.include_router(files_router.router)
    app.include_router(tags_router.router)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/integration/api/test_files_lifecycle.py -v`
Expected: 12 passed

- [ ] **Step 6: Commit**

```bash
git add almacen/api/routers/tags.py almacen/main.py tests/integration/api/test_files_lifecycle.py
git commit -m "Add tags router: list, add, remove"
```

---

### Task 12: Full test suite pass and README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Run the entire test suite**

Run: `pytest -v`
Expected: all tests across `tests/unit/` and `tests/integration/` pass (28 tests:
7 domain + 5 blob_store + 6 metadata_store + 4 tag_index + 12 files/tags API — the
API file accumulated tests across Tasks 9-11, but a couple of shared-fixture
assertions may bring the exact count off by one or two; the point is 0 failures).

- [ ] **Step 2: Write `README.md`**

```markdown
# Almacén Distribuido de Archivos Basado en Etiquetas

Tag-based distributed file store. See `docs/superpowers/specs/2026-09-28-tag-based-distributed-store-design.md`
for the full architecture, and `docs/project-context.md` for the original
assignment brief.

## Status

Phase 1 (single-node core) — no networking/clustering yet. A single node
exposes a REST API to upload, download, tag, query, and delete files, backed by
local filesystem blob storage and a local SQLite metadata store.

## Setup

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e ".[dev]"

## Run

    uvicorn almacen.main:app --reload

By default, data is stored under `./data` (override with the `ALMACEN_DATA_DIR`
environment variable). Interactive API docs: http://127.0.0.1:8000/docs

## Test

    pytest -v
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "Add project README with setup, run, and test instructions"
```
