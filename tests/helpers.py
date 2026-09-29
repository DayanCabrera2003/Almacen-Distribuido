"""Helpers shared across test packages."""
from __future__ import annotations

import socket


def free_port() -> int:
    """Ask the OS for an unused loopback port.

    Binding to port 0 and reading back the assignment avoids the hard-coded-port
    collisions that make multi-node tests flaky when a previous run has not yet
    released its sockets.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
