"""An in-process five-node cluster under a virtual clock and a faulty network.

Each node is the real application — `create_app`, its gRPC server, its SQLite
store and blob directory — reached over HTTP through a `TestClient`. What the
harness replaces is only what makes a test non-deterministic:

- the clock: record timestamps, the gRPC servicer and every gossip tick read a
  `VirtualClock`, so grace TTLs and suspicion timeouts elapse in simulated
  seconds;
- the network: every outbound channel is a `FaultyChannelPool` (see
  `network.py`);
- the gossip loop: it is disabled in the apps and driven here one tick at a
  time, with peers chosen from a seeded random generator.
"""
from __future__ import annotations

import random
import socket
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

import almacen.main
from almacen.cluster.gossip import GossipLoop
from almacen.config import Peer, Settings
from almacen.domain.file_record import FileRecord
from almacen.main import create_app
from almacen.storage.blob_store import BlobStore
from almacen.storage.gc_state import BlobGCState
from almacen.storage.metadata_store import MetadataStore
from tests.chaos.network import FaultyChannelPool, Network
from tests.helpers import free_port

NODE_IDS = tuple(f"node{i}" for i in range(1, 6))
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


class VirtualClock:
    """Simulated UTC time, advanced only by the scenario."""

    def __init__(self, start: datetime = EPOCH) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


@dataclass(frozen=True)
class ClusterConfig:
    grace_ttl_seconds: float = 60.0
    suspicion_timeout_seconds: float = 20.0
    gossip_fanout: int = 2
    blob_catchup_budget: int = 8
    gc_budget: int = 32


@dataclass
class ChaosNode:
    node_id: str
    settings: Settings
    app: object = None
    client: TestClient | None = None
    gc_state: BlobGCState | None = None
    stack: ExitStack = field(default_factory=ExitStack)

    @property
    def is_up(self) -> bool:
        return self.client is not None

    @property
    def store(self) -> MetadataStore:
        return self.app.state.local_metadata_store

    @property
    def blob_store(self) -> BlobStore:
        return self.app.state.blob_store

    @property
    def channels(self) -> FaultyChannelPool:
        return self.app.state.channels


class ChaosCluster:
    def __init__(
        self, root: Path, clock: VirtualClock, config: ClusterConfig, seed: int
    ) -> None:
        self.clock = clock
        self.config = config
        self.network = Network(NODE_IDS)
        # Peer choice gets its own generator, so that changing how many random
        # draws the operation generator makes does not reshuffle gossip.
        self._gossip_rng = random.Random(f"gossip-{seed}")
        ports = {n: free_port() for n in NODE_IDS}
        self.peers = tuple(Peer(n, f"127.0.0.1:{p}") for n, p in ports.items())
        self._node_of_address = {p.address: p.node_id for p in self.peers}
        self.nodes = {
            n: ChaosNode(n, self._settings(root / n, n, ports[n])) for n in NODE_IDS
        }
        # Last view of a crashed node's metadata. Its store is closed while it
        # is down, but its records still count: they come back on restart.
        self._down_records: dict[str, list[FileRecord]] = {}
        for node_id in NODE_IDS:
            self.start(node_id)

    def _settings(self, root: Path, node_id: str, port: int) -> Settings:
        return Settings(
            data_dir=root / "blobs",
            db_path=root / "metadata.db",
            node_id=node_id,
            grpc_port=port,
            peers=self.peers,
            replication_factor=3,
            write_quorum=2,
            # Never reached: cut links fail at once. Bounded anyway so that a
            # harness bug shows up as a failure rather than as a hang.
            rpc_timeout_seconds=5.0,
            # The apps' own gossip thread is off; ticks are driven by `tick`.
            gossip_interval_seconds=0,
            suspicion_timeout_seconds=self.config.suspicion_timeout_seconds,
            gossip_fanout=self.config.gossip_fanout,
            blob_catchup_budget=self.config.blob_catchup_budget,
            grace_ttl_seconds=self.config.grace_ttl_seconds,
            gc_budget=self.config.gc_budget,
        )

    # ---- lifecycle ----

    def start(self, node_id: str) -> None:
        """Start (or restart) a node on its existing storage and port."""
        node = self.nodes[node_id]
        assert not node.is_up, f"{node_id} is already up"

        def pool() -> FaultyChannelPool:
            return FaultyChannelPool(node_id, self.network, self._node_of_address)

        with mock.patch.object(almacen.main, "ChannelPool", pool):
            node.app = create_app(node.settings)
        node.client = node.stack.enter_context(TestClient(node.app))
        # The production loop owns this file; with the loop disabled the harness
        # opens it at the same path, so it survives restarts the same way.
        node.gc_state = BlobGCState(node.settings.db_path.with_name("gc_state.db"))
        node.stack.callback(node.gc_state.close)
        _assert_listening(node.settings.grpc_port)

        self.network.mark_up(node_id)
        self._down_records.pop(node_id, None)
        # Peers drop their channels to the node, so the first call after the
        # restart opens a fresh connection rather than reusing a dead one.
        for other in self.up_nodes():
            if other.node_id != node_id:
                other.channels.reset()

    def crash(self, node_id: str) -> None:
        """Stop a node; its blobs, metadata and GC state stay on disk."""
        node = self.nodes[node_id]
        self._down_records[node_id] = node.store.list_all()
        self.network.mark_down(node_id)
        node.stack.close()
        node.client = None
        node.app = None
        node.gc_state = None

    def close(self) -> None:
        for node in self.nodes.values():
            node.stack.close()

    def up_nodes(self) -> list[ChaosNode]:
        return [n for n in self.nodes.values() if n.is_up]

    # ---- gossip ----

    def tick(self, *, all_pairs: bool) -> dict[str, set[str]]:
        """One gossip tick on every running node, at the current virtual time.

        `all_pairs` makes every node gossip with every other node — after which,
        on a healthy network, every node holds every update. Otherwise each node
        picks `gossip_fanout` random peers, as production does, including peers
        that are down or cut off: probing them is how they get suspected.

        Returns the blobs each node deleted during the tick, for the GC safety
        check.
        """
        deleted: dict[str, set[str]] = {}
        for node in self.up_nodes():
            others = [p for p in self.peers if p.node_id != node.node_id]
            chosen = (
                others
                if all_pairs
                else self._gossip_rng.sample(others, self.config.gossip_fanout)
            )
            before = node.blob_store.list_hashes()
            GossipLoop(
                settings=node.settings,
                store=node.store,
                blob_store=node.blob_store,
                gc_state=node.gc_state,
                membership=node.app.state.membership,
                channels=node.channels,
                replication_client=node.app.state.replication_client,
                choose_peers=lambda _now, chosen=chosen: chosen,
                now=self.clock.now,
            ).run_once()
            # Catch-up only adds blobs, so anything missing was collected.
            deleted[node.node_id] = before - node.blob_store.list_hashes()
        return deleted

    # ---- observation ----

    def records(self, node_id: str) -> list[FileRecord]:
        """Every record a node holds, from its store or, if down, its last view."""
        node = self.nodes[node_id]
        if node.is_up:
            return node.store.list_all()
        return self._down_records[node_id]


def _assert_listening(port: int) -> None:
    # `add_insecure_port` reports a failed bind by returning 0, not by raising,
    # so a restart that lost its port would otherwise look like a partition. A
    # plain TCP connect checks the bind without gRPC's 200 ms readiness polling.
    with socket.create_connection(("127.0.0.1", port), timeout=5):
        pass
