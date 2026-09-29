# almacen/config.py
"""Node configuration: local storage plus static cluster membership."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_GRPC_PORT = 50051
DEFAULT_NODE_ID = "node1"


@dataclass(frozen=True)
class Peer:
    """One node in the cluster, addressable over gRPC."""

    node_id: str
    address: str  # "host:port"

    @classmethod
    def parse(cls, raw: str) -> "Peer":
        """Parse a `node_id@host:port` entry from ALMACEN_PEERS."""
        node_id, _, address = raw.strip().partition("@")
        if not node_id or not address:
            raise ValueError(
                f"malformed peer entry {raw!r}: expected 'node_id@host:port'"
            )
        return cls(node_id=node_id, address=address)


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    node_id: str = DEFAULT_NODE_ID
    grpc_port: int = DEFAULT_GRPC_PORT
    # Every node in the cluster, including this one. Empty means "just me",
    # which is how single-node development and the Phase 1 tests run.
    peers: tuple[Peer, ...] = field(default_factory=tuple)
    replication_factor: int = 3
    write_quorum: int = 2

    def __post_init__(self) -> None:
        if self.replication_factor < 1:
            raise ValueError(
                f"replication_factor must be >= 1, got {self.replication_factor}"
            )
        if self.write_quorum < 1:
            raise ValueError(f"write_quorum must be >= 1, got {self.write_quorum}")
        if self.write_quorum > self.replication_factor:
            raise ValueError(
                f"write_quorum ({self.write_quorum}) cannot exceed "
                f"replication_factor ({self.replication_factor})"
            )
        if self.peers and self.node_id not in {p.node_id for p in self.peers}:
            raise ValueError(
                f"node_id {self.node_id!r} does not appear in the peer list "
                f"{[p.node_id for p in self.peers]}"
            )

    @property
    def node_ids(self) -> list[str]:
        """Every node id in the cluster, for placement decisions."""
        return [p.node_id for p in self.peers] if self.peers else [self.node_id]

    @property
    def remote_peers(self) -> tuple[Peer, ...]:
        """Every peer except this node."""
        return tuple(p for p in self.peers if p.node_id != self.node_id)

    def peer_address(self, node_id: str) -> str | None:
        for peer in self.peers:
            if peer.node_id == node_id:
                return peer.address
        return None

    @classmethod
    def from_env(cls) -> "Settings":
        base_dir = Path(os.environ.get("ALMACEN_DATA_DIR", "./data"))
        raw_peers = os.environ.get("ALMACEN_PEERS", "").strip()
        peers = tuple(
            Peer.parse(entry) for entry in raw_peers.split(",") if entry.strip()
        )
        return cls(
            data_dir=base_dir / "blobs",
            db_path=base_dir / "metadata.db",
            node_id=os.environ.get("ALMACEN_NODE_ID", DEFAULT_NODE_ID),
            grpc_port=int(os.environ.get("ALMACEN_GRPC_PORT", DEFAULT_GRPC_PORT)),
            peers=peers,
            replication_factor=int(os.environ.get("ALMACEN_REPLICATION_FACTOR", 3)),
            write_quorum=int(os.environ.get("ALMACEN_WRITE_QUORUM", 2)),
        )
