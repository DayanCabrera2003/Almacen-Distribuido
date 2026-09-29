from collections.abc import Iterator
from concurrent import futures

import grpc
import pytest

from tests.helpers import free_port


@pytest.fixture
def grpc_server_factory() -> Iterator[object]:
    """Start gRPC servers that are always torn down, even if a test fails."""
    servers: list[grpc.Server] = []

    def start(register) -> str:
        """`register(server)` wires servicers in; returns the server address."""
        port = free_port()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        register(server)
        server.add_insecure_port(f"127.0.0.1:{port}")
        server.start()
        servers.append(server)
        return f"127.0.0.1:{port}"

    yield start

    for server in servers:
        server.stop(0).wait()
