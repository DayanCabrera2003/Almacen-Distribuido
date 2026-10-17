"""State shared by every command, and the CLI's error-to-exit-code mapping."""
from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import typer

from almacen.cli.client import ApiError, NodeClient, NoNodeReachable

# Exit status 2 is click's own, for usage errors.
EXIT_API_ERROR = 1
EXIT_NO_NODE = 3


@dataclass
class CliState:
    client: NodeClient
    json_output: bool

    def emit(self, data: Any, render: Callable[[Any], str]) -> None:
        """Print `data` as JSON with --json, otherwise as `render` formats it."""
        if self.json_output:
            typer.echo(json.dumps(data, indent=2))
        else:
            typer.echo(render(data))


def state_of(ctx: typer.Context) -> CliState:
    return ctx.obj


@contextmanager
def reporting_errors() -> Iterator[None]:
    """Turn client errors into a one-line message on stderr and an exit code."""
    try:
        yield
    except ApiError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(EXIT_API_ERROR) from None
    except NoNodeReachable as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(EXIT_NO_NODE) from None
