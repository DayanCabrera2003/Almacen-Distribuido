# almacen/rpc/server.py
"""Assembly of the node's gRPC server (internal, node-to-node plane)."""
from __future__ import annotations

from concurrent import futures

import grpc

from almacen.cluster.membership import Membership
from almacen.rpc import cluster_pb2_grpc, replication_pb2_grpc
from almacen.rpc.cluster_servicer import ClusterServicer
from almacen.rpc.replication_servicer import ReplicationServicer
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore

DEFAULT_MAX_WORKERS = 8


def build_server(
    blob_store: BlobStore,
    metadata_store: MetadataStore,
    node_id: str,
    port: int,
    membership: Membership | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> grpc.Server:
    """Build the node's gRPC server, bound but not started.

    The caller starts and stops it, so server lifetime is tied to whatever owns
    it (the FastAPI lifespan in production, a fixture in tests) instead of being
    an invisible side effect of construction.

    `metadata_store` must be the plain local store, not the replicating wrapper:
    records arriving from peers are applied locally and must not be pushed back
    out (see ClusterServicer).

    `membership` is optional so that tests which only exercise data replication
    need not build one. When present it must be the *same instance* the gossip
    loop uses: two instances would each hold half the picture and neither would
    ever see the other's observations.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    replication_pb2_grpc.add_ReplicationServicer_to_server(
        ReplicationServicer(blob_store), server
    )
    cluster_pb2_grpc.add_ClusterServicer_to_server(
        ClusterServicer(metadata_store, node_id=node_id, membership=membership),
        server,
    )
    # Binding to all interfaces is required inside a container, where peers reach
    # this node by its service name rather than loopback.
    server.add_insecure_port(f"[::]:{port}")
    return server
