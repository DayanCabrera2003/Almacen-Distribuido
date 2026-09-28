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
