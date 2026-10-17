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
