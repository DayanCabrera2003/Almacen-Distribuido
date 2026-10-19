# almacen/cluster/metadata_replicator.py
"""Pushes metadata records to every peer (spec §4: full metadata replication)."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import grpc
import structlog

from almacen.cluster.channels import ChannelPool
from almacen.cluster.membership import Membership, NodeState
from almacen.config import Settings
from almacen.domain.file_record import FileRecord
from almacen.observability import describe_error
from almacen.rpc import cluster_pb2_grpc as pb_grpc
from almacen.rpc.record_codec import record_to_message


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class MetadataReplicator:
    """Best-effort push of a record to all peers.

    Deliberately best-effort: unlike content (where W acks are a durability
    guarantee from spec §3), metadata replication failing must not turn a
    successful local write into a client-visible error. A peer that misses a push
    diverges, and nothing here repairs it — that is exactly the gap Phase 4's
    push-pull anti-entropy closes.

    Peers the failure detector has declared dead are skipped. The push is
    awaited, so a write would otherwise wait out a full RPC timeout on every
    unreachable peer, on every write, for as long as the partition lasts.
    Suspects are still pushed to: suspicion is not death. A peer that is back
    but still believed dead misses the push until gossip revives it, and
    anti-entropy delivers the record in the meantime.
    """

    def __init__(
        self,
        settings: Settings,
        channels: ChannelPool | None = None,
        membership: Membership | None = None,
    ) -> None:
        self._settings = settings
        self._channels = channels or ChannelPool()
        self._membership = membership
        self._log = structlog.get_logger(__name__, node_id=settings.node_id)

    def replicate(self, record: FileRecord) -> None:
        peers = [
            peer for peer in self._settings.remote_peers if not self._is_dead(peer.node_id)
        ]
        if not peers:
            return

        message = record_to_message(record)
        with ThreadPoolExecutor(max_workers=len(peers)) as pool:
            # list() forces every push to complete; exceptions are handled inside
            # _push, so nothing escapes to the caller.
            list(pool.map(lambda peer: self._push(peer, message), peers))

    def _is_dead(self, node_id: str) -> bool:
        return (
            self._membership is not None
            and self._membership.state_of(node_id, _utcnow()) is NodeState.DEAD
        )

    def _push(self, peer, message) -> None:
        try:
            stub = pb_grpc.ClusterStub(self._channels.channel(peer.address))
            stub.ReplicateRecord(message, timeout=self._settings.rpc_timeout_seconds)
        except grpc.RpcError as error:
            self._log.warning(
                "record_replication_failed",
                peer=peer.node_id,
                file_id=message.file_id,
                error=describe_error(error),
            )

    def close(self) -> None:
        """Close the channel pool.

        Only safe when this replicator owns its pool. When a pool is shared with
        other clients — as it is in `create_app` — the owner closes it instead,
        or this would disconnect the others too.
        """
        self._channels.close()
