import hashlib
import os
import stat
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


def test_list_hashes_returns_every_stored_blob(blob_store: BlobStore):
    first = blob_store.put(b"one")
    second = blob_store.put(b"two")

    assert blob_store.list_hashes() == {first, second}


def test_list_hashes_ignores_in_flight_temporary_files(
    blob_store: BlobStore, tmp_path: Path
):
    stored = blob_store.put(b"real")
    # What a crash in the middle of `put` leaves behind.
    (tmp_path / "blobs" / ".tmp-abc123").write_bytes(b"partial")

    assert blob_store.list_hashes() == {stored}


def test_delete_removes_a_blob_and_reports_it(blob_store: BlobStore):
    content_hash = blob_store.put(b"doomed")

    assert blob_store.delete(content_hash) is True
    assert blob_store.exists(content_hash) is False
    assert blob_store.get(content_hash) is None


def test_delete_is_idempotent(blob_store: BlobStore):
    content_hash = blob_store.put(b"doomed")
    blob_store.delete(content_hash)

    assert blob_store.delete(content_hash) is False
    assert blob_store.delete("0" * 64) is False


def test_put_flushes_the_file_before_renaming_and_the_directory_after(
    blob_store: BlobStore, monkeypatch: pytest.MonkeyPatch
):
    # A blob acknowledged to a quorum must survive a power loss, not only a
    # process crash: its bytes have to reach the disk before the rename makes
    # them visible, and the rename itself (a directory entry) has to reach the
    # disk before `put` returns.
    events: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def recording_fsync(fd: int) -> None:
        kind = "dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
        events.append(f"fsync-{kind}")
        real_fsync(fd)

    def recording_replace(src, dst) -> None:
        events.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    monkeypatch.setattr(os, "replace", recording_replace)

    content_hash = blob_store.put(b"durable")

    assert events == ["fsync-file", "replace", "fsync-dir"]
    assert blob_store.get(content_hash) == b"durable"


def test_put_of_a_blob_already_held_writes_nothing(
    blob_store: BlobStore, monkeypatch: pytest.MonkeyPatch
):
    content_hash = blob_store.put(b"already here")
    calls: list[int] = []
    monkeypatch.setattr(os, "fsync", calls.append)

    assert blob_store.put(b"already here") == content_hash
    assert calls == []


def test_put_leaves_no_temporary_file_behind(blob_store: BlobStore, tmp_path: Path):
    content_hash = blob_store.put(b"clean")

    assert [p.name for p in (tmp_path / "blobs").iterdir()] == [content_hash]
