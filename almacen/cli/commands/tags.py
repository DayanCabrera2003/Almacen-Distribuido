"""Tag commands: list, add, remove (`/files/{file_id}/tags`)."""
from __future__ import annotations

from urllib.parse import quote

import typer

from almacen.cli.commands import split_tags
from almacen.cli.context import reporting_errors, state_of
from almacen.cli.output import render_tags

tag_app = typer.Typer(help="List, add or remove a file's tags.", no_args_is_help=True)

TagsArgument = typer.Argument(..., help="Tags; separate arguments or commas.")


def _required_tags(values: list[str]) -> list[str]:
    tags = split_tags(values)
    if not tags:
        raise typer.BadParameter("at least one non-empty tag is required")
    return tags


def _emit_tags(ctx: typer.Context, body: dict) -> None:
    state_of(ctx).emit(body, lambda b: render_tags(b["tags"]))


@tag_app.command("list")
def list_tags(ctx: typer.Context, file_id: str = typer.Argument(...)) -> None:
    """List a file's tags."""
    with reporting_errors():
        response = state_of(ctx).client.request("GET", f"/files/{file_id}/tags")
    _emit_tags(ctx, response.json())


@tag_app.command("add")
def add_tags(
    ctx: typer.Context,
    file_id: str = typer.Argument(...),
    tags: list[str] = TagsArgument,
) -> None:
    """Add tags to a file."""
    with reporting_errors():
        response = state_of(ctx).client.request(
            "POST", f"/files/{file_id}/tags", json={"tags": _required_tags(tags)}
        )
    _emit_tags(ctx, response.json())


@tag_app.command("remove")
def remove_tags(
    ctx: typer.Context,
    file_id: str = typer.Argument(...),
    tags: list[str] = TagsArgument,
) -> None:
    """Remove tags from a file (one request per tag; absent tags are no-ops)."""
    client = state_of(ctx).client
    with reporting_errors():
        for tag in _required_tags(tags):
            # The tag is a path segment, so spaces and reserved characters
            # must be percent-encoded to reach the server intact.
            response = client.request(
                "DELETE", f"/files/{file_id}/tags/{quote(tag, safe='')}"
            )
    # Each response carries the full tag set, so the last one is current.
    _emit_tags(ctx, response.json())


def register(app: typer.Typer) -> None:
    app.add_typer(tag_app, name="tag")
