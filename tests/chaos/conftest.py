"""Fixtures for the chaos suite: seeds, the virtual clock, the cluster."""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from almacen.cluster import metadata_replicator
from almacen.domain import file_record
from almacen.rpc import cluster_servicer
from tests.chaos.cluster import ChaosCluster, ClusterConfig, VirtualClock

DEFAULT_SEED_COUNT = 10


def _seeds() -> list[int]:
    """`CHAOS_SEEDS` overrides the default seeds: `7`, `3,8,13` or `100-199`."""
    raw = os.environ.get("CHAOS_SEEDS", "").strip()
    if not raw:
        return list(range(DEFAULT_SEED_COUNT))
    seeds: list[int] = []
    for part in raw.split(","):
        first, _, last = part.strip().partition("-")
        seeds.extend(range(int(first), int(last or first) + 1))
    return seeds


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "seed" in metafunc.fixturenames:
        seeds = _seeds()
        metafunc.parametrize("seed", seeds, ids=[f"seed{s}" for s in seeds])


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> VirtualClock:
    """Simulated time for record timestamps, the gRPC servicer and the clients.

    Gossip ticks read it too (the harness passes it to each loop), so every
    timestamp a grace TTL or a suspicion timeout is measured against is virtual.
    """
    clock = VirtualClock()
    monkeypatch.setattr(file_record, "_utcnow", clock.now)
    monkeypatch.setattr(cluster_servicer, "_utcnow", clock.now)
    # The metadata replicator consults membership, whose timestamps are virtual.
    monkeypatch.setattr(metadata_replicator, "_utcnow", clock.now)
    return clock


@pytest.fixture
def chaos_cluster(
    tmp_path: Path, clock: VirtualClock, seed: int
) -> Iterator[ChaosCluster]:
    cluster = ChaosCluster(tmp_path, clock, ClusterConfig(), seed)
    yield cluster
    cluster.close()
