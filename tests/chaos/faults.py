"""Random fault schedules: partitions, one-way cuts, crashes and restarts."""
from __future__ import annotations

import itertools
import random

from tests.chaos.cluster import NODE_IDS, ChaosCluster
from tests.chaos.history import History


class FaultInjector:
    def __init__(
        self, cluster: ChaosCluster, history: History, rng: random.Random
    ) -> None:
        self._cluster = cluster
        self._history = history
        self._rng = rng

    def random_fault(self) -> None:
        """Change the fault state at random; faults accumulate until healed."""
        kind = self._rng.choice(
            ("split", "split", "isolate", "asymmetric", "crash", "restart", "heal_links")
        )
        getattr(self, f"_{kind}")()
        self._history.record(f"  fault {kind}: {self._cluster.network.describe()}")

    def heal_all(self) -> None:
        for node_id in sorted(self._cluster.network.down):
            self._cluster.start(node_id)
        self._cluster.network.heal()
        self._history.record("  heal: every node up, every link restored")

    def _split(self) -> None:
        """An arbitrary split into two or three non-empty groups."""
        nodes = list(NODE_IDS)
        self._rng.shuffle(nodes)
        count = self._rng.choice((2, 2, 3))
        cuts = sorted(self._rng.sample(range(1, len(nodes)), count - 1))
        groups = [nodes[a:b] for a, b in zip([0, *cuts], [*cuts, len(nodes)])]
        self._cluster.network.partition(groups)

    def _isolate(self) -> None:
        lonely = self._rng.choice(NODE_IDS)
        self._cluster.network.partition([[lonely], [n for n in NODE_IDS if n != lonely]])

    def _asymmetric(self) -> None:
        """A few one-way cuts: the source cannot call the target, not vice versa."""
        links = list(itertools.permutations(NODE_IDS, 2))
        self._cluster.network.cut(self._rng.sample(links, self._rng.randint(1, 5)))

    def _crash(self) -> None:
        # At least two nodes stay up, so there is still a cluster to talk to.
        up = [n.node_id for n in self._cluster.up_nodes()]
        if len(up) > 2:
            self._cluster.crash(self._rng.choice(up))

    def _restart(self) -> None:
        down = sorted(self._cluster.network.down)
        if down:
            self._cluster.start(self._rng.choice(down))

    def _heal_links(self) -> None:
        self._cluster.network.heal()
