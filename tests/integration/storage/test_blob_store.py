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
