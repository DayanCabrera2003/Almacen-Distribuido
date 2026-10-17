"""Fault injection on the node-to-node plane: cut links and crashed nodes.

Every node's outbound gRPC traffic goes through a `FaultyChannelPool`, whose
channels consult the shared `Network` before each call. A call over a cut link
fails at once with UNAVAILABLE, exactly as the production code sees an
unreachable peer, but without a socket timeout — so a partition costs nothing
and its effect does not depend on how fast the machine running the tests is.

Links are directed. Cutting only `a -> b` is an asymmetric partition: `a`
cannot open calls to `b`, while `b` can still call `a` and read the answer,
which is how a one-way firewall rule behaves on a request/response protocol.
"""
from __future__ import annotations

import itertools
from collections.abc import Iterable

import grpc

from almacen.cluster.channels import ChannelPool


class LinkDown(grpc.RpcError):
    """What a call over a cut link raises: an ordinary UNAVAILABLE RpcError."""

    def __init__(self, source: str, target: str) -> None:
        super().__init__(f"chaos: link {source} -> {target} is down")

    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.UNAVAILABLE

    def details(self) -> str:
        return str(self)


class Network:
    """The cluster's connectivity: which directed links are cut, who is down."""

    def __init__(self, node_ids: Iterable[str]) -> None:
        self.node_ids = tuple(node_ids)
        self._cut: set[tuple[str, str]] = set()
        self._down: set[str] = set()

    def can_reach(self, source: str, target: str) -> bool:
        if source == target:
            return True
        if source in self._down or target in self._down:
            return False
        return (source, target) not in self._cut

    # ---- faults ----

    def partition(self, groups: Iterable[Iterable[str]]) -> None:
        """Replace the current cuts with a split into `groups`, both directions."""
        group_of = {n: i for i, group in enumerate(groups) for n in group}
        self._cut = {
            (a, b)
            for a, b in itertools.permutations(self.node_ids, 2)
            if group_of.get(a) != group_of.get(b)
        }

    def cut(self, links: Iterable[tuple[str, str]]) -> None:
        """Replace the current cuts with these directed links."""
        self._cut = set(links)

    def heal(self) -> None:
        self._cut.clear()

    def mark_down(self, node_id: str) -> None:
        self._down.add(node_id)

    def mark_up(self, node_id: str) -> None:
        self._down.discard(node_id)

    @property
    def down(self) -> frozenset[str]:
        return frozenset(self._down)

    @property
    def is_healthy(self) -> bool:
        return not self._cut and not self._down

    def describe(self) -> str:
        cuts = ",".join(f"{a}->{b}" for a, b in sorted(self._cut)) or "none"
        down = ",".join(sorted(self._down)) or "none"
        return f"cut=[{cuts}] down=[{down}]"


class _LinkGuard(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Fails a call before it touches the real channel if its link is cut.

    Failing *before* the real channel is used matters: a gRPC channel that
    tried to connect to a stopped server backs off for a second or more, and
    would go on failing after the server returned — a partition that outlives
    its heal by an amount of wall-clock time.
    """

    def __init__(self, network: Network, source: str, target: str) -> None:
        self._network = network
        self._source = source
        self._target = target

    def _check(self) -> None:
        if not self._network.can_reach(self._source, self._target):
            raise LinkDown(self._source, self._target)

    def intercept_unary_unary(self, continuation, details, request):
        self._check()
        return continuation(details, request)

    def intercept_unary_stream(self, continuation, details, request):
        self._check()
        return continuation(details, request)

    def intercept_stream_unary(self, continuation, details, request_iterator):
        self._check()
        return continuation(details, request_iterator)

    def intercept_stream_stream(self, continuation, details, request_iterator):
        self._check()
        return continuation(details, request_iterator)


class FaultyChannelPool(ChannelPool):
    """A node's channel pool whose every channel is guarded by the `Network`."""

    def __init__(
        self, owner: str, network: Network, node_of_address: dict[str, str]
    ) -> None:
        super().__init__()
        self._owner = owner
        self._network = network
        self._node_of_address = node_of_address
        self._guarded: dict[str, grpc.Channel] = {}

    def channel(self, address: str) -> grpc.Channel:
        real = super().channel(address)
        with self._lock:
            guarded = self._guarded.get(address)
            if guarded is None:
                guard = _LinkGuard(
                    self._network, self._owner, self._node_of_address[address]
                )
                guarded = grpc.intercept_channel(real, guard)
                self._guarded[address] = guarded
            return guarded

    def reset(self) -> None:
        """Drop every channel, so the next call to a restarted peer reconnects
        immediately instead of inheriting a stale connection's state."""
        self.close()
        with self._lock:
            self._guarded.clear()
