"""Entry point of `almacen-cli`: global options and command registration."""
from __future__ import annotations

import httpx
import typer

from almacen.cli.client import DEFAULT_NODE_URL, NodeClient, parse_node_urls
from almacen.cli.commands import cluster, files, tags
from almacen.cli.context import CliState

app = typer.Typer(
    help="Operate an Almacen Distribuido cluster over its REST API.",
    no_args_is_help=True,
    add_completion=False,
)


def _warn_unreachable(url: str, error: Exception) -> None:
    typer.echo(f"warning: {url} unreachable ({error})", err=True)


@app.callback()
def main(
    ctx: typer.Context,
    node: str = typer.Option(
        DEFAULT_NODE_URL,
        "--node",
        envvar="ALMACEN_NODE_URL",
        help="Node base URL, or a comma-separated list tried in order when a "
        "node cannot be reached.",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Print raw JSON responses instead of tables."
    ),
) -> None:
    try:
        node_urls = parse_node_urls(node)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint="--node") from None
    # Tests pass an httpx transport as the context object, to route requests to
    # an in-process app instead of the network.
    transport = ctx.obj if isinstance(ctx.obj, httpx.BaseTransport) else None
    client = NodeClient(node_urls, transport=transport, on_unreachable=_warn_unreachable)
    ctx.call_on_close(client.close)
    ctx.obj = CliState(client=client, json_output=json_output)


files.register(app)
tags.register(app)
cluster.register(app)


def run() -> None:
    """Console-script entry point."""
    app()
