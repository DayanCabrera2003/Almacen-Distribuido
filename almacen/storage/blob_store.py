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
        """Store `content` durably and return its hash.

        When this returns, the blob survives a power loss, not only a process
        crash: an acknowledgement from this replica counts towards the upload's
        write quorum, so it must mean the bytes are on disk. The bytes are
        flushed before the rename, so the blob's name never points at
        unwritten data, and the directory is flushed after it, so the name
        itself is not lost.
        """
        content_hash = hashlib.sha256(content).hexdigest()
        target = self._path_for(content_hash)
        if target.exists():
            return content_hash

        tmp_path = self._root / f".tmp-{uuid.uuid4().hex}"
        with open(tmp_path, "wb") as tmp_file:
            tmp_file.write(content)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, target)
        self._fsync_root()
        return content_hash

    def get(self, content_hash: str) -> bytes | None:
        path = self._path_for(content_hash)
        if not path.exists():
            return None
        return path.read_bytes()

    def exists(self, content_hash: str) -> bool:
        return self._path_for(content_hash).exists()

    def list_hashes(self) -> set[str]:
        """Every blob held locally.

        Skips the `.tmp-*` files `put` uses for its atomic write: treating one
        as a blob would mean reference-counting a name that is not a hash.
        """
        return {
            path.name
            for path in self._root.iterdir()
            if path.is_file() and not path.name.startswith(".tmp-")
        }

    def delete(self, content_hash: str) -> bool:
        """Remove a blob. Returns whether it was there; safe to repeat."""
        path = self._path_for(content_hash)
        if not path.exists():
            return False
        path.unlink()
        return True

    def _fsync_root(self) -> None:
        # A rename is a change to the directory, not to the file, so only
        # flushing the directory makes it durable (POSIX; Linux in practice).
        fd = os.open(self._root, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _path_for(self, content_hash: str) -> Path:
        return self._root / content_hash
