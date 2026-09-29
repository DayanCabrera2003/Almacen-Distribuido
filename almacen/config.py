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
