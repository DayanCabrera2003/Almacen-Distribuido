"""Cluster command: status (`/cluster/status`)."""
from __future__ import annotations

import typer

from almacen.cli.client import ApiError
from almacen.cli.context import EXIT_API_ERROR, reporting_errors, state_of
from almacen.cli.output import render_mapping


def status(ctx: typer.Context) -> None:
    """Show the answering node's view of the cluster (membership and health)."""
    state = state_of(ctx)
    with reporting_errors():
        try:
            response = state.client.request("GET", "/cluster/status")
        except ApiError as error:
            if error.status_code != 404:
                raise
            # A 404 here means the route itself is missing, not some resource.
            typer.echo(
                "error: this node does not serve /cluster/status; it is running "
                "a version older than the status endpoint",
                err=True,
            )
            raise typer.Exit(EXIT_API_ERROR) from None
    state.emit(response.json(), render_mapping)


def register(app: typer.Typer) -> None:
    app.command()(status)
