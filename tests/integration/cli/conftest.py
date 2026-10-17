"""Run the CLI against a real node app, in process, with no sockets.

The CLI's HTTP client is given an httpx transport that hands every request to a
Starlette TestClient wrapping the app, so commands exercise the real routers and
storage without binding a port. Node URLs whose host is listed in `dead_hosts`
raise a connection error instead, which is how failover is tested.
"""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
from click.testing import Result
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from almacen.cli.app import app as cli_app
from almacen.config import Settings
from almacen.main import create_app

NODE = "http://node1:8001"


class Cluster:
    """The in-process node plus a record of which hosts the CLI contacted."""

    def __init__(self, test_client: TestClient) -> None:
        self.test_client = test_client
        self.dead_hosts: set[str] = set()
        self.contacted: list[str] = []
        self.transport = httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.contacted.append(request.url.host)
        if request.url.host in self.dead_hosts:
            raise httpx.ConnectError("connection refused", request=request)
        response = self.test_client.request(
            request.method,
            request.url.raw_path.decode(),
            content=request.content,
            headers={
                k: v for k, v in request.headers.items() if k.lower() != "host"
            },
        )
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            content=response.content,
        )


@pytest.fixture
def cluster(tmp_path: Path) -> Cluster:
    settings = Settings(data_dir=tmp_path / "blobs", db_path=tmp_path / "metadata.db")
    return Cluster(TestClient(create_app(settings)))


@pytest.fixture
def cli(cluster: Cluster) -> Callable[..., Result]:
    """Invoke the CLI against `NODE`, or against no --node at all if None."""
    runner = CliRunner()

    def invoke(
        *args: str, node: str | None = NODE, env: dict[str, str] | None = None
    ) -> Result:
        node_args = ["--node", node] if node is not None else []
        return runner.invoke(
            cli_app, [*node_args, *args], obj=cluster.transport, env=env
        )

    return invoke
