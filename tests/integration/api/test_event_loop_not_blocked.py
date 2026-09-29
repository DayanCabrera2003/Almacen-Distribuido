"""A slow replication round must not freeze the node's whole REST plane.

`put_blob` and the metadata push are synchronous and can take up to the gRPC
timeout. If they run on the event loop instead of a worker thread, every other
request to the node — downloads, queries, deletes — stalls with them.
"""
import asyncio
import time
from pathlib import Path

import httpx
import pytest

from almacen.config import Peer, Settings
from almacen.main import create_app

# Unroutable addresses: connections hang until the RPC deadline rather than
# failing fast, which is what makes the stall measurable.
BLACKHOLE_A = "10.255.255.1:50051"
BLACKHOLE_B = "10.255.255.2:50051"

HEARTBEAT_SECONDS = 0.2
# Generous compared to the heartbeat, but far below the ~5s RPC deadline that a
# blocked loop would produce, so the test is decisive without being flaky.
MAX_ACCEPTABLE_LAG = 1.0


@pytest.fixture
def app_with_black_holed_peers(tmp_path: Path):
    settings = Settings(
        data_dir=tmp_path / "blobs",
        db_path=tmp_path / "metadata.db",
        node_id="node1",
        peers=(
            Peer("node1", "127.0.0.1:59001"),
            Peer("node2", BLACKHOLE_A),
            Peer("node3", BLACKHOLE_B),
        ),
        replication_factor=3,
        write_quorum=2,
    )
    return create_app(settings)


def test_the_event_loop_keeps_running_during_a_slow_upload(app_with_black_holed_peers):
    async def scenario() -> tuple[int, float]:
        transport = httpx.ASGITransport(app=app_with_black_holed_peers)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://node"
        ) as client:
            lag = 0.0

            async def heartbeat() -> None:
                nonlocal lag
                started = time.monotonic()
                await asyncio.sleep(HEARTBEAT_SECONDS)
                lag = time.monotonic() - started

            upload, _ = await asyncio.gather(
                client.post(
                    "/files",
                    files={"file": ("a.txt", b"payload", "text/plain")},
                    data={"name": "a.txt"},
                ),
                heartbeat(),
            )
            return upload.status_code, lag

    status_code, lag = asyncio.run(scenario())

    # The upload itself is expected to fail its quorum against dead peers.
    assert status_code == 503

    # The point of the test: a short timer scheduled on the loop must fire on
    # time. If the handler blocks the loop it fires only after the RPC deadline.
    assert lag < MAX_ACCEPTABLE_LAG, (
        f"event loop was blocked: a {HEARTBEAT_SECONDS}s sleep took {lag:.2f}s, so "
        "every other request to this node stalled for the whole replication round"
    )
