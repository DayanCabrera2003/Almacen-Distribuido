"""status: rendering /cluster/status, and nodes that do not serve it.

These use a stub transport rather than the node app: the response shape is owned
by the status endpoint, and the CLI must render whatever it returns.
"""
from __future__ import annotations

import json

import httpx
from typer.testing import CliRunner

from almacen.cli.app import app as cli_app

STATUS = {
    "node_id": "node1",
    "members": [
        {"node_id": "node1", "state": "alive"},
        {"node_id": "node2", "state": "suspect"},
    ],
}


def _invoke(handler, *args: str):
    return CliRunner().invoke(
        cli_app,
        ["--node", "http://node1:8001", *args, "status"],
        obj=httpx.MockTransport(handler),
    )


def test_status_renders_the_node_view():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/cluster/status"
        return httpx.Response(200, json=STATUS)

    result = _invoke(handler)

    assert result.exit_code == 0, result.output
    assert "node_id: node1" in result.stdout
    assert "node2    suspect" in result.stdout


def test_status_json_is_the_raw_response():
    result = _invoke(lambda request: httpx.Response(200, json=STATUS), "--json")

    assert json.loads(result.stdout) == STATUS


def test_status_on_a_node_without_the_endpoint_explains_why():
    result = _invoke(lambda request: httpx.Response(404, json={"detail": "Not Found"}))

    assert result.exit_code == 1
    assert "does not serve /cluster/status" in result.stderr
