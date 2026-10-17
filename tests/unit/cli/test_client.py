"""NodeClient: node-list parsing, failover, and error mapping."""
from __future__ import annotations

import httpx
import pytest

from almacen.cli.client import ApiError, NodeClient, NoNodeReachable, parse_node_urls


def test_parse_node_urls_splits_on_commas_and_strips_blanks():
    assert parse_node_urls(" http://a:1/ , ,http://b:2") == ["http://a:1", "http://b:2"]


def test_parse_node_urls_rejects_an_empty_list():
    with pytest.raises(ValueError):
        parse_node_urls(" , ")


def _transport(dead_hosts: set[str], seen: list[str]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host)
        if request.url.host in dead_hosts:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"host": request.url.host})

    return httpx.MockTransport(handler)


def test_fails_over_to_the_next_node_on_connection_error():
    seen: list[str] = []
    skipped: list[str] = []
    client = NodeClient(
        ["http://a:1", "http://b:2"],
        transport=_transport({"a"}, seen),
        on_unreachable=lambda url, error: skipped.append(url),
    )

    response = client.request("GET", "/files")

    assert response.json() == {"host": "b"}
    assert seen == ["a", "b"]
    assert skipped == ["http://a:1"]


def test_keeps_using_the_node_that_answered():
    seen: list[str] = []
    client = NodeClient(["http://a:1", "http://b:2"], transport=_transport({"a"}, seen))

    client.request("GET", "/files")
    client.request("GET", "/files")

    # The dead node is not retried first on every call.
    assert seen == ["a", "b", "b"]


def test_raises_when_no_node_is_reachable():
    client = NodeClient(
        ["http://a:1", "http://b:2"], transport=_transport({"a", "b"}, [])
    )

    with pytest.raises(NoNodeReachable) as excinfo:
        client.request("GET", "/files")

    assert excinfo.value.node_urls == ["http://a:1", "http://b:2"]


def test_http_errors_do_not_fail_over():
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host)
        return httpx.Response(404, json={"detail": "file not found"})

    client = NodeClient(
        ["http://a:1", "http://b:2"], transport=httpx.MockTransport(handler)
    )

    with pytest.raises(ApiError) as excinfo:
        client.request("GET", "/files/x")

    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "file not found"
    # A node that answered is authoritative; asking another would only hide it.
    assert seen == ["a"]


def test_api_error_detail_falls_back_to_the_body_text():
    client = NodeClient(
        ["http://a:1"],
        transport=httpx.MockTransport(lambda request: httpx.Response(500, text="boom")),
    )

    with pytest.raises(ApiError) as excinfo:
        client.request("GET", "/files")

    assert excinfo.value.detail == "boom"
