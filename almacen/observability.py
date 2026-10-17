"""Structured logging: one configuration for structlog and stdlib alike.

Cluster code logs named events (`gossip_round_completed`, `member_state_changed`)
with key/value context rather than formatted sentences, so a demo can be
followed by filtering on `event` and `node_id` instead of reading prose.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, TextIO

import structlog

from almacen.config import DEFAULT_NODE_ID

LOG_FORMATS = ("console", "json")
DEFAULT_LOG_FORMAT = "console"
DEFAULT_LOG_LEVEL = "INFO"

# Uvicorn installs its own handlers on these and stops propagation, which would
# leave its lines in plain text between our JSON ones.
_LIBRARY_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


class _OurHandler(logging.StreamHandler):
    """Marks the handler this module installs, so reconfiguring replaces it."""


def configure_logging(
    *, level: str, fmt: str, node_id: str, stream: TextIO | None = None
) -> None:
    """Route structlog and stdlib logging through one renderer.

    `node_id` is a default, not an override: components bind their own, which
    is what keeps several in-process nodes distinguishable in the test suite.
    """
    if fmt not in LOG_FORMATS:
        raise ValueError(f"unknown log format {fmt!r}: expected one of {LOG_FORMATS}")
    numeric_level = logging.getLevelName(level.upper())
    if not isinstance(numeric_level, int):
        raise ValueError(f"unknown log level {level!r}")

    def default_node_id(_logger: Any, _method: str, event_dict: dict) -> dict:
        event_dict.setdefault("node_id", node_id)
        return event_dict

    # Applied to structlog events and stdlib records alike.
    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        default_node_id,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    renderer = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared,
            structlog.processors.StackInfoRenderer(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # Off on purpose: a cached logger keeps the processors it first saw, so
        # `structlog.testing.capture_logs` and any later reconfiguration would
        # silently miss it.
        cache_logger_on_first_use=False,
    )

    final = [structlog.stdlib.ProcessorFormatter.remove_processors_meta]
    if fmt == "json":
        # The console renderer prints tracebacks itself; JSON needs them as a
        # field, or an exception would span several lines.
        final.append(structlog.processors.format_exc_info)
    final.append(renderer)

    handler = _OurHandler(stream or sys.stderr)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(foreign_pre_chain=shared, processors=final)
    )
    root = logging.getLogger()
    root.handlers[:] = [h for h in root.handlers if not isinstance(h, _OurHandler)]
    root.addHandler(handler)
    root.setLevel(numeric_level)

    for name in _LIBRARY_LOGGERS:
        library_logger = logging.getLogger(name)
        library_logger.handlers.clear()
        library_logger.propagate = True


def configure_logging_from_env(stream: TextIO | None = None) -> None:
    """Configure from `ALMACEN_LOG_FORMAT`, `ALMACEN_LOG_LEVEL` and the node id."""
    configure_logging(
        level=os.environ.get("ALMACEN_LOG_LEVEL", DEFAULT_LOG_LEVEL),
        fmt=os.environ.get("ALMACEN_LOG_FORMAT", DEFAULT_LOG_FORMAT).lower(),
        node_id=os.environ.get("ALMACEN_NODE_ID", DEFAULT_NODE_ID),
        stream=stream,
    )


def describe_error(error: BaseException) -> str:
    """A short, stable value for an `error` field.

    A gRPC error's string form spans several lines of debug detail; its status
    code is what a reader filters on. Anything else falls back to its message.
    """
    code = getattr(error, "code", None)
    if callable(code):
        try:
            return code().name
        except Exception:  # noqa: BLE001 - some RpcErrors carry no status
            pass
    return str(error)
