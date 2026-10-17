"""--node, ALMACEN_NODE_URL, and failover between the listed nodes."""
from __future__ import annotations

from almacen.cli.client import DEFAULT_NODE_URL


def test_default_node_is_used_when_nothing_is_configured(cli, cluster):
    result = cli("list", node=None, env={"ALMACEN_NODE_URL": None})

    assert result.exit_code == 0, result.output
    assert cluster.contacted == [DEFAULT_NODE_URL.split("//")[1].split(":")[0]]


def test_environment_variable_selects_the_node(cli, cluster):
    result = cli("list", node=None, env={"ALMACEN_NODE_URL": "http://from-env:9000"})

    assert result.exit_code == 0, result.output
    assert cluster.contacted == ["from-env"]


def test_fails_over_to_the_next_node_and_says_so(cli, cluster, tmp_path):
    cluster.dead_hosts.add("dead")
    source = tmp_path / "a.txt"
    source.write_bytes(b"x")

    result = cli("upload", str(source), node="http://dead:8001,http://alive:8002")

    assert result.exit_code == 0, result.output
    assert cluster.contacted == ["dead", "alive"]
    assert "http://dead:8001" in result.stderr
    # The upload went through once, on the live node.
    assert "a.txt" in cli("list", node="http://alive:8002").stdout


def test_reports_when_no_node_is_reachable(cli, cluster):
    cluster.dead_hosts.update({"a", "b"})

    result = cli("list", node="http://a:1,http://b:2")

    assert result.exit_code == 3
    assert "no node reachable" in result.stderr


def test_an_empty_node_list_is_a_usage_error(cli):
    result = cli("list", node=" , ")

    assert result.exit_code == 2
