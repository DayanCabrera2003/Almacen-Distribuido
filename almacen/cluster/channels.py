# almacen/cluster/channels.py
"""Shared, reusable gRPC channels to peer nodes."""
from __future__ import annotations

import threading

import grpc


class ChannelPool:
    """Caches one long-lived gRPC channel per peer address.

    gRPC channels are designed to be created once and reused: each one manages
    its own connection state and reconnects on its own. Creating a channel per
    request would add a TCP/HTTP2 handshake to every replicated write.
    """

    def __init__(self) -> None:
        self._channels: dict[str, grpc.Channel] = {}
        # Guards the dict: FastAPI serves sync handlers from a thread pool, so
        # two requests can ask for the same peer's channel at the same time and
        # would otherwise each create one, leaking all but the last.
        self._lock = threading.Lock()

    def channel(self, address: str) -> grpc.Channel:
        with self._lock:
            channel = self._channels.get(address)
            if channel is None:
                channel = grpc.insecure_channel(address)
                self._channels[address] = channel
            return channel

    def close(self) -> None:
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
