import io
import json
import logging

import pytest
import structlog

from almacen.observability import configure_logging, configure_logging_from_env


@pytest.fixture(autouse=True)
def restore_logging():
    """Undo each test's configuration, which is process-global."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    structlog.reset_defaults()
    root.handlers[:] = handlers
    root.setLevel(level)


def test_json_format_emits_one_object_per_line_with_the_node_id():
    stream = io.StringIO()
    configure_logging(level="INFO", fmt="json", node_id="node3", stream=stream)

    structlog.get_logger("almacen.test").info("gossip_round_completed", peer="node2")

    entry = json.loads(stream.getvalue().strip())
    assert entry["event"] == "gossip_round_completed"
    assert entry["peer"] == "node2"
    assert entry["level"] == "info"
    assert entry["node_id"] == "node3"
    assert "timestamp" in entry


def test_a_node_id_bound_on_the_logger_wins_over_the_process_default():
    # In-process multi-node tests run several nodes under one configuration, so
    # a component's own binding must not be overwritten by the default.
    stream = io.StringIO()
    configure_logging(level="INFO", fmt="json", node_id="node1", stream=stream)

    structlog.get_logger("almacen.test", node_id="node4").info("member_state_changed")

    assert json.loads(stream.getvalue())["node_id"] == "node4"


def test_console_format_is_not_json():
    stream = io.StringIO()
    configure_logging(level="INFO", fmt="console", node_id="node1", stream=stream)

    structlog.get_logger("almacen.test").info("node_started")

    output = stream.getvalue()
    assert "node_started" in output
    with pytest.raises(json.JSONDecodeError):
        json.loads(output)


def test_events_below_the_level_are_dropped():
    stream = io.StringIO()
    configure_logging(level="WARNING", fmt="json", node_id="node1", stream=stream)

    log = structlog.get_logger("almacen.test")
    log.info("too_quiet")
    log.warning("loud_enough")

    events = [json.loads(line)["event"] for line in stream.getvalue().splitlines()]
    assert events == ["loud_enough"]


def test_stdlib_records_share_the_same_renderer():
    # Library logs (uvicorn, grpc) must not break the one-JSON-object-per-line
    # contract that a log collector relies on.
    stream = io.StringIO()
    configure_logging(level="INFO", fmt="json", node_id="node2", stream=stream)

    logging.getLogger("uvicorn.error").info("Application startup complete.")

    entry = json.loads(stream.getvalue())
    assert entry["event"] == "Application startup complete."
    assert entry["node_id"] == "node2"


def test_reconfiguring_does_not_duplicate_output():
    stream = io.StringIO()
    configure_logging(level="INFO", fmt="json", node_id="node1", stream=stream)
    configure_logging(level="INFO", fmt="json", node_id="node1", stream=stream)

    structlog.get_logger("almacen.test").info("once")

    assert len(stream.getvalue().splitlines()) == 1


@pytest.mark.parametrize("fmt", ["xml", ""])
def test_an_unknown_format_is_rejected(fmt):
    with pytest.raises(ValueError, match="log format"):
        configure_logging(level="INFO", fmt=fmt, node_id="node1")


def test_an_unknown_level_is_rejected():
    with pytest.raises(ValueError, match="log level"):
        configure_logging(level="LOUD", fmt="json", node_id="node1")


def test_from_env_reads_format_level_and_node_id(monkeypatch):
    monkeypatch.setenv("ALMACEN_LOG_FORMAT", "json")
    monkeypatch.setenv("ALMACEN_LOG_LEVEL", "debug")
    monkeypatch.setenv("ALMACEN_NODE_ID", "node5")
    stream = io.StringIO()

    configure_logging_from_env(stream=stream)
    structlog.get_logger("almacen.test").debug("verbose")

    entry = json.loads(stream.getvalue())
    assert entry["event"] == "verbose"
    assert entry["node_id"] == "node5"
