"""HTTP access to the cluster for the CLI, failing over between nodes.

Any node answers any request (metadata is fully replicated and content is
fetched from replicas on demand), so the CLI can be pointed at several nodes and
use whichever one it can reach. That is what keeps it usable during a partition
demo, when the node it was talking to has just been cut off.
"""
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import httpx

DEFAULT_NODE_URL = "http://127.0.0.1:8001"
DEFAULT_TIMEOUT_SECONDS = 30.0

# Only errors raised before the request reached the node trigger failover. Once
# a node may have received the request, retrying elsewhere could apply a
# non-idempotent write (an upload, a delete) twice, so a read or write timeout
# is reported instead of retried.
_FAILOVER_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)


class NoNodeReachable(Exception):
    """Every configured node refused or timed out the connection."""

    def __init__(self, node_urls: Sequence[str]) -> None:
        self.node_urls = list(node_urls)
        super().__init__(f"no node reachable (tried {', '.join(self.node_urls)})")


class ApiError(Exception):
    """A node answered with an HTTP error status."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")

    @classmethod
    def from_response(cls, response: httpx.Response) -> ApiError:
        try:
            detail: Any = response.json().get("detail", response.text)
        except (ValueError, AttributeError):
            detail = response.text
        return cls(response.status_code, str(detail))


def parse_node_urls(value: str) -> list[str]:
    """Split a comma-separated node list, dropping blanks and trailing slashes."""
    urls = [part.strip().rstrip("/") for part in value.split(",")]
    urls = [url for url in urls if url]
    if not urls:
        raise ValueError("at least one node URL is required")
    return urls


class NodeClient:
    """Sends each request to the first reachable node in a list.

    The node that answered moves to the front, so a command that issues several
    requests does not pay the connection timeout of a dead node on each one.
    """

    def __init__(
        self,
        node_urls: Sequence[str],
        transport: httpx.BaseTransport | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        on_unreachable: Callable[[str, Exception], None] | None = None,
    ) -> None:
        if not node_urls:
            raise ValueError("at least one node URL is required")
        self._node_urls = list(node_urls)
        self._http = httpx.Client(transport=transport, timeout=timeout)
        self._on_unreachable = on_unreachable

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Send a request, failing over on connection errors.

        Raises ApiError for an HTTP error status, and NoNodeReachable when no
        node accepted the connection. Request bodies must be bytes, not streams,
        so that they can be sent again to the next node.
        """
        for index, url in enumerate(self._node_urls):
            try:
                response = self._http.request(method, url + path, **kwargs)
            except _FAILOVER_ERRORS as error:
                if self._on_unreachable is not None:
                    self._on_unreachable(url, error)
                continue
            if index:
                self._node_urls.insert(0, self._node_urls.pop(index))
            if response.is_error:
                raise ApiError.from_response(response)
            return response
        raise NoNodeReachable(self._node_urls)

    def close(self) -> None:
        self._http.close()
