# almacen/cluster/replication_client.py
"""Orchestrates content writes and reads across the cluster (spec §8)."""
from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import grpc
import structlog

from almacen.cluster.channels import ChannelPool
from almacen.cluster.membership import Membership, NodeState
from almacen.cluster.placement import replica_set
from almacen.config import Settings
from almacen.observability import describe_error
from almacen.rpc import replication_pb2 as pb
from almacen.rpc import replication_pb2_grpc as pb_grpc
from almacen.rpc.replication_servicer import CHUNK_SIZE
from almacen.storage.blob_store import BlobStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class QuorumNotReached(RuntimeError):
    """Fewer than W replicas accepted a write, so it is not durable enough."""


class ReplicationClient:
    def __init__(
        self,
        settings: Settings,
        blob_store: BlobStore,
        channels: ChannelPool | None = None,
        membership: Membership | None = None,
    ) -> None:
        self._settings = settings
        self._blob_store = blob_store
        self._channels = channels or ChannelPool()
        # Optional so a client can run without failure detection; without it
        # every peer is treated as alive.
        self._membership = membership
        self._log = structlog.get_logger(__name__, node_id=settings.node_id)

    def put_blob(self, content: bytes) -> str:
        """Write content to its replica set, requiring W acknowledgements.

        Returns the content hash. Raises QuorumNotReached if fewer than W
        replicas stored it, in which case the caller must not record metadata
        pointing at this hash.
        """
        content_hash = hashlib.sha256(content).hexdigest()
        replicas = replica_set(
            content_hash, self._settings.node_ids, self._settings.replication_factor
        )
        # A cluster smaller than R cannot produce R acks, so the requirement
        # shrinks with it. Without this, a single-node deployment could never
        # complete a write.
        required = min(self._settings.write_quorum, len(replicas))

        acknowledged = 0
        if self._settings.node_id in replicas:
            # The local write is just another replica, done without the network.
            self._blob_store.put(content)
            acknowledged += 1

        # Every replica is attempted and waited for, and only then is W
        # evaluated — rather than acknowledging the client as soon as the W-th
        # ack arrives. Early acknowledgement is faster and is the Dynamo
        # behaviour, but it is only correct once something finishes or repairs
        # the replicas still in flight, which is Phase 4's anti-entropy. Doing it
        # now would mean accepting writes that nothing ever completes, so a blob
        # could sit below R replicas forever with nothing noticing.
        #
        # Replicas declared dead are held back: attempting them costs a full RPC
        # timeout each and usually ends as a missing acknowledgement anyway. They
        # are tried only if the others cannot reach W, because a death can be
        # stale — right after a partition heals, membership lags the network by
        # a few gossip ticks, and refusing the write then would turn a recovered
        # cluster into 503s. A replica skipped this way gets the blob from
        # anti-entropy's blob catch-up once it is back.
        remotes = [n for n in replicas if n != self._settings.node_id]
        presumed_dead = [n for n in remotes if self._is_dead(n)]
        acknowledged += self._put_remotes(
            [n for n in remotes if n not in presumed_dead], content, content_hash
        )
        if acknowledged < required:
            acknowledged += self._put_remotes(presumed_dead, content, content_hash)

        if acknowledged < required:
            self._log.warning(
                "write_quorum_not_reached",
                content_hash=content_hash[:12],
                acknowledged=acknowledged,
                required=required,
                replicas=replicas,
            )
            # The replicas that did accept the write keep their copy, and no
            # metadata will ever point at it. Those bytes are unreferenced until
            # Phase 5's reference-counting GC collects them; rolling them back
            # here would need its own quorum to be reliable, and would fail in
            # exactly the situation that triggered this path.
            raise QuorumNotReached(
                f"only {acknowledged} of {len(replicas)} replicas stored "
                f"{content_hash[:12]}, need {required}"
            )
        return content_hash

    def get_blob(self, content_hash: str) -> bytes | None:
        """Return the content, from this node if it has it or from a replica.

        Returns None when no replica could serve it — a transient availability
        problem, which the API layer surfaces as 503.
        """
        local = self._blob_store.get(content_hash)
        # Verify the local copy too, not just copies fetched from peers. Content
        # is addressed by its hash, so the address doubles as a checksum — but
        # only if it is actually checked. Skipping it here would mean corruption
        # on this node's disk is served to the client while the same corruption
        # on a peer is caught, so whether a read can be trusted would depend on
        # which node answered it. Costs one SHA-256 over data already in memory.
        if local is not None and _matches(local, content_hash):
            return local
        if local is not None:
            self._log.error("blob_local_copy_corrupt", content_hash=content_hash[:12])

        # HRW order doubles as a preference order: every node tries the replicas
        # in the same sequence, so reads concentrate on the same node and benefit
        # from its page cache. Replicas declared dead go last rather than being
        # skipped: a death can be false, and a slow read beats a 503 when the
        # "dead" node is the only one holding the blob.
        replicas = replica_set(
            content_hash, self._settings.node_ids, self._settings.replication_factor
        )
        for node_id in sorted(replicas, key=self._is_dead):
            if node_id == self._settings.node_id:
                continue
            content = self._get_remote(node_id, content_hash)
            if content is not None:
                return content
        return None

    def _put_remotes(
        self, node_ids: list[str], content: bytes, content_hash: str
    ) -> int:
        """Write to these replicas in parallel; return how many acknowledged."""
        if not node_ids:
            return 0
        with ThreadPoolExecutor(max_workers=len(node_ids)) as pool:
            results = pool.map(
                lambda node_id: self._put_remote(node_id, content, content_hash),
                node_ids,
            )
            return sum(1 for succeeded in results if succeeded)

    def _is_dead(self, node_id: str) -> bool:
        return (
            self._membership is not None
            and self._membership.state_of(node_id, _utcnow()) is NodeState.DEAD
        )

    def _put_remote(self, node_id: str, content: bytes, content_hash: str) -> bool:
        address = self._settings.peer_address(node_id)
        if address is None:
            self._log.warning("blob_replica_unknown", replica=node_id)
            return False
        try:
            stub = pb_grpc.ReplicationStub(self._channels.channel(address))
            stub.PutBlob(_chunk(content, content_hash), timeout=self._settings.rpc_timeout_seconds)
            return True
        except grpc.RpcError as error:
            # Swallowed on purpose: a single failed replica is not a failed
            # write, it is one fewer acknowledgement. The quorum check decides.
            self._log.warning(
                "blob_replication_failed",
                replica=node_id,
                content_hash=content_hash[:12],
                error=describe_error(error),
            )
            return False

    def _get_remote(self, node_id: str, content_hash: str) -> bytes | None:
        address = self._settings.peer_address(node_id)
        if address is None:
            return None
        try:
            stub = pb_grpc.ReplicationStub(self._channels.channel(address))
            received = b"".join(
                chunk.data
                for chunk in stub.GetBlob(
                    pb.BlobRequest(content_hash=content_hash),
                    timeout=self._settings.rpc_timeout_seconds,
                )
            )
        except grpc.RpcError as error:
            self._log.info(
                "blob_fetch_failed",
                replica=node_id,
                content_hash=content_hash[:12],
                error=describe_error(error),
            )
            return None

        # Same check as on the local path: turns silent corruption into a miss,
        # so the caller falls through to the next replica.
        if not _matches(received, content_hash):
            self._log.error(
                "blob_replica_corrupt", replica=node_id, content_hash=content_hash[:12]
            )
            return None
        return received

def _matches(content: bytes, content_hash: str) -> bool:
    """Whether content actually hashes to the address it was stored under."""
    return hashlib.sha256(content).hexdigest() == content_hash


def _chunk(content: bytes, content_hash: str):
    if not content:
        # An empty blob is still a blob. Without this, range() yields nothing and
        # the receiver sees a stream with no chunks, which it cannot distinguish
        # from a malformed request and therefore rejects — so every remote
        # replica would refuse an empty file and the write quorum would fail.
        yield pb.BlobChunk(content_hash=content_hash, data=b"")
        return

    for offset in range(0, len(content), CHUNK_SIZE):
        yield pb.BlobChunk(
            content_hash=content_hash, data=content[offset : offset + CHUNK_SIZE]
        )
