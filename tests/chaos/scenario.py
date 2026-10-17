"""One chaos run: fault episodes with random operations, then healing.

Time is split into *episodes*. During an episode faults come and go at random
and nodes gossip with random peers; each episode ends with a healing tick —
every node restarted, every link restored, every pair of nodes gossiping. An
episode is bounded to less than one grace TTL of simulated time, which is the
condition under which the design promises no resurrection: every tombstone
reaches every node before any node may purge it. Runs span several TTLs, so
purges and blob collection happen in the middle of the chaos, not only after.
"""
from __future__ import annotations

import random
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from tests.chaos.cluster import ChaosCluster
from tests.chaos.faults import FaultInjector
from tests.chaos.history import History
from tests.chaos.invariants import (
    InvariantChecker,
    assert_everyone_alive,
    assert_fully_collected,
    assert_live_content_readable_everywhere,
    assert_matches_history,
    assert_metadata_converged,
    blobs_fully_placed,
    metadata_converged,
)
from tests.chaos.operations import OperationGenerator, OperationMix

# Healthy all-pairs rounds allowed for metadata to converge. One suffices on a
# healthy network (see `ChaosCluster.tick`); the second absorbs a purge that
# lands mid-round.
CONVERGENCE_ROUNDS = 2
# Blob catch-up is budgeted per tick, so placement may take a few more.
PLACEMENT_ROUNDS = 10


@dataclass(frozen=True)
class Profile:
    episodes: int = 8
    max_fault_ticks: int = 5
    max_tick_seconds: float = 8.0
    max_ops_per_tick: int = 4
    fault_change_probability: float = 0.6
    mix: OperationMix = field(default_factory=OperationMix)

    def longest_episode_seconds(self) -> float:
        # Fault ticks plus the healing tick.
        return (self.max_fault_ticks + 1) * self.max_tick_seconds


class ChaosRun:
    def __init__(self, cluster: ChaosCluster, seed: int, profile: Profile) -> None:
        assert profile.longest_episode_seconds() < cluster.config.grace_ttl_seconds, (
            "an episode may outlast the grace TTL, so resurrection would be "
            "allowed by design and the no-resurrection check would be unsound"
        )
        self.cluster = cluster
        self.profile = profile
        self.seed = seed
        self.history = History(seed)
        self.checker = InvariantChecker(cluster, self.history)
        self._rng = random.Random(seed)
        self._operations = OperationGenerator(
            cluster, self.history, self._rng, profile.mix
        )
        self._faults = FaultInjector(cluster, self.history, self._rng)

    @contextmanager
    def reporting(self, test_id: str) -> Iterator[None]:
        """Re-raise any failure with the seed and the schedule that led to it."""
        try:
            yield
        except Exception as error:
            raise AssertionError(
                f"chaos run failed: {error}\n"
                f"seed={self.seed}; reproduce with\n"
                f"    CHAOS_SEEDS={self.seed} pytest '{test_id}'\n"
                f"schedule:\n{self.history.schedule()}"
            ) from error

    # ---- phases ----

    def run_chaos(self) -> None:
        for episode in range(self.profile.episodes):
            self._log(f"episode {episode}")
            for _ in range(self._rng.randint(1, self.profile.max_fault_ticks)):
                if self._rng.random() < self.profile.fault_change_probability:
                    self._faults.random_fault()
                for _ in range(self._rng.randint(0, self.profile.max_ops_per_tick)):
                    self._operations.random_operation()
                    self.checker.after_step()
                self._tick(all_pairs=False)
            self._faults.heal_all()
            self._tick(all_pairs=True)
            self.checker.after_heal()

    def heal_and_converge(self) -> None:
        """Heal everything; metadata must converge and every blob be placed."""
        self._faults.heal_all()
        for _ in range(CONVERGENCE_ROUNDS):
            self._tick(all_pairs=True, seconds=1.0)
            if metadata_converged(self.cluster):
                break
        assert_metadata_converged(self.cluster)
        assert_matches_history(self.cluster, self.history)
        assert_everyone_alive(self.cluster)

        for _ in range(PLACEMENT_ROUNDS):
            if blobs_fully_placed(self.cluster):
                break
            self._tick(all_pairs=True, seconds=1.0)
        assert blobs_fully_placed(self.cluster), "blob catch-up never completed"
        assert_live_content_readable_everywhere(self.cluster, self.history)

    def collect_everything(self) -> None:
        """Let three grace TTLs pass: every tombstone and orphan must be gone.

        A deleted file's blob is retained for about two TTLs (one until the
        tombstone is purged, one as a candidate), so three is enough with room
        for the tick granularity.
        """
        ttl = self.cluster.config.grace_ttl_seconds
        for _ in range(7):
            self._tick(all_pairs=True, seconds=ttl / 2)
        assert_metadata_converged(self.cluster)
        assert_fully_collected(self.cluster)
        assert_live_content_readable_everywhere(self.cluster, self.history)

    # ---- helpers ----

    def _tick(self, *, all_pairs: bool, seconds: float | None = None) -> None:
        if seconds is None:
            seconds = self._rng.uniform(1.0, self.profile.max_tick_seconds)
        self.cluster.clock.advance(seconds)
        deleted = self.cluster.tick(all_pairs=all_pairs)
        collected = {n: h for n, h in deleted.items() if h}
        self._log(
            f"  tick +{seconds:.1f}s {'all-pairs' if all_pairs else 'random peers'}"
            + (f" collected {_short(collected)}" if collected else "")
        )
        self.checker.after_step(deleted)

    def _log(self, line: str) -> None:
        self.history.record(line)


def _short(collected: dict[str, set[str]]) -> str:
    return ", ".join(
        f"{node}:{sorted(h[:8] for h in hashes)}" for node, hashes in collected.items()
    )
