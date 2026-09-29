# almacen/cluster/gossip.py
"""One push-pull gossip round, and the loop that drives it (spec §11)."""
from __future__ import annotations

import logging
import random
import threading
import uuid
from collections.abc import Callable
from datetime import datetime, timezone

import grpc

from almacen.cluster.channels import ChannelPool
from almacen.cluster.membership import Membership
from almacen.cluster.placement import replica_set
from almacen.cluster.reconciliation import build_digest
from almacen.cluster.replication_client import ReplicationClient
from almacen.config import Peer, Settings
from almacen.rpc import cluster_pb2 as pb
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.gossip_codec import (
    decode_membership,
    encode_digest,
    encode_membership,
)
from almacen.rpc.record_codec import message_to_record, record_to_message
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

logger = logging.getLogger(__name__)


def gossip_once(
    peer: Peer,
    *,
    settings: Settings,
    store: MetadataStore,
    membership: Membership,
    channels: ChannelPool,
    now: datetime,
) -> bool:
    """Run one push-pull exchange with `peer`. Returns whether it succeeded.

    Failure is reported, never raised. An unreachable peer is the normal case
    this phase exists to handle, and the caller is a background thread: an
    exception escaping here would stop that node gossiping for good, silently.
    """
    local_records = store.list_all()
    request = pb.GossipDigest(
        from_node_id=settings.node_id,
        entries=encode_digest(build_digest(local_records)),
        membership=encode_membership(membership.snapshot(now)),
    )

    try:
        stub = pb_grpc.ClusterStub(channels.channel(peer.address))
        delta = stub.Gossip(request, timeout=settings.rpc_timeout_seconds)
    except grpc.RpcError as error:
        logger.info("gossip with %s failed: %s", peer.node_id, error)
        membership.record_unreachable(peer.node_id, now)
        return False

    membership.record_reachable(peer.node_id, now)

    # Everything below can fail too, and all of it runs on the gossip thread.
    # A decode error, or a peer that dies between answering the digest and
    # receiving the pushes — the exact scenario this phase exists for — would
    # otherwise propagate out of the thread target.
    try:
        membership.merge(decode_membership(delta.membership), now)

        # Pull: merge what the peer is ahead on.
        for message in delta.records:
            store.merge_remote(message_to_record(message))

        # Push: send back what the peer said it is behind on. ReplicateRecord
        # already merges on the far side, so no new transfer path is needed.
        wanted = {uuid.UUID(raw) for raw in delta.wanted_file_ids}
        for record in local_records:
            if record.file_id in wanted:
                stub.ReplicateRecord(
                    record_to_message(record), timeout=settings.rpc_timeout_seconds
                )
    except (grpc.RpcError, ValueError) as error:
        logger.warning("gossip with %s failed mid-exchange: %s", peer.node_id, error)
        return False

    return True


def catch_up_blobs(
    *,
    settings: Settings,
    store: MetadataStore,
    blob_store: BlobStore,
    replication_client: ReplicationClient,
    budget: int,
    should_stop: Callable[[], bool] = lambda: False,
) -> int:
    """Fetch up to `budget` blobs this node should hold but does not.

    Driven by placement, not by "fetch whatever I am missing": a node pulls only
    the content rendezvous hashing makes it a replica for. Fetching everything
    would make every node store the whole corpus and quietly undo the sharding.

    Bounded on purpose. This runs on the gossip thread and each fetch is a
    blocking transfer, so a node healing from a long partition could otherwise
    spend minutes inside a single tick — which is what would actually defeat
    the loop's shutdown join. Whatever is left over is picked up next tick:
    anti-entropy is a loop, not a one-shot.
    """
    fetched = 0
    for record in store.list_live():
        if fetched >= budget or should_stop():
            break
        replicas = replica_set(
            record.content_hash, settings.node_ids, settings.replication_factor
        )
        if settings.node_id not in replicas:
            continue
        if blob_store.exists(record.content_hash):
            continue
        content = replication_client.get_blob(record.content_hash)
        if content is not None:
            blob_store.put(content)
            fetched += 1
            logger.info("caught up blob %s", record.content_hash[:12])
    return fetched


class GossipLoop:
    """Runs gossip rounds on a timer until stopped.

    Deliberately thin: it owns the clock and the peer choice and nothing else.
    All the protocol lives in `membership.py` and `reconciliation.py`, which take
    time and peers as parameters — which is why none of this phase's tests need
    to sleep.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        store: MetadataStore,
        blob_store: BlobStore,
        membership: Membership,
        channels: ChannelPool,
        replication_client: ReplicationClient,
        choose_peers: Callable[[datetime], list[Peer]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._blob_store = blob_store
        self._membership = membership
        self._channels = channels
        self._replication_client = replication_client
        # Injected in tests. Random selection needs up to 23 rounds to converge
        # five nodes, so a test using it would be flaky at any round count fast
        # enough to be worth running.
        self._choose_peers = choose_peers or self._random_live_peers
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _random_live_peers(self, now: datetime) -> list[Peer]:
        live = set(self._membership.live_peers(now))
        candidates = [p for p in self._settings.remote_peers if p.node_id in live]
        if not candidates:
            return []
        return random.sample(candidates, min(self._settings.gossip_fanout, len(candidates)))

    def run_once(self) -> None:
        """One tick: gossip with the chosen peers, then catch up on blobs."""
        now = self._now()
        for peer in self._choose_peers(now):
            if self._stop.is_set():
                # Re-checked between peers so a shutdown during a fanout does
                # not have to wait out every peer's timeout.
                return
            try:
                gossip_once(
                    peer,
                    settings=self._settings,
                    store=self._store,
                    membership=self._membership,
                    channels=self._channels,
                    now=now,
                )
            except Exception:  # noqa: BLE001 - nothing may escape the thread
                # gossip_once already handles the expected failures; this is the
                # backstop for the unexpected. One bad peer must not end the
                # loop, and an escaped exception would do exactly that.
                logger.exception("unexpected failure gossiping with %s", peer.node_id)

        if self._stop.is_set():
            return
        try:
            catch_up_blobs(
                settings=self._settings,
                store=self._store,
                blob_store=self._blob_store,
                replication_client=self._replication_client,
                budget=self._settings.blob_catchup_budget,
                should_stop=self._stop.is_set,
            )
        except Exception:  # noqa: BLE001
            logger.exception("unexpected failure catching up blobs")

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("gossip loop already started")
        self._thread = threading.Thread(
            target=self._run, name="gossip", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        # Waits before the first tick rather than after. Ticking at startup
        # would make every app construction do a round of network I/O before
        # anything asked it to.
        while not self._stop.wait(self._settings.gossip_interval_seconds):
            self.run_once()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the thread and wait for it, raising if it does not finish.

        Returning silently on a failed join is how a leaked thread becomes a
        failure attributed to whatever test runs next.
        """
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is None:
            return
        thread.join(timeout=timeout)
        if thread.is_alive():
            raise RuntimeError(f"gossip loop did not stop within {timeout}s")
