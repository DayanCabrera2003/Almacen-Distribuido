from pathlib import Path

import grpc

from almacen.rpc import cluster_pb2 as cluster_pb
from almacen.rpc import cluster_pb2_grpc as cluster_grpc
from almacen.rpc import replication_pb2 as repl_pb
from almacen.rpc import replication_pb2_grpc as repl_grpc
from almacen.rpc.server import build_server
from almacen.storage.blob_store import BlobStore
from almacen.storage.metadata_store import MetadataStore
from tests.helpers import free_port


def test_build_server_exposes_both_services(tmp_path: Path):
    blob_store = BlobStore(tmp_path / "blobs")
    metadata_store = MetadataStore(tmp_path / "metadata.db")
    port = free_port()

    server = build_server(
        blob_store=blob_store,
        metadata_store=metadata_store,
        node_id="node1",
        port=port,
    )
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            # Cluster service answers.
            ping = cluster_grpc.ClusterStub(channel).Ping(
                cluster_pb.PingRequest(from_node_id="tester")
            )
            assert ping.node_id == "node1"

            # Replication service answers on the same port.
            content_hash = blob_store.put(b"present")
            received = b"".join(
                chunk.data
                for chunk in repl_grpc.ReplicationStub(channel).GetBlob(
                    repl_pb.BlobRequest(content_hash=content_hash)
                )
            )
            assert received == b"present"
    finally:
        server.stop(0).wait()
