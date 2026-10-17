"""File commands: upload, list, info, download, update, delete (`/files`)."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import typer

from almacen.cli.commands import split_tags
from almacen.cli.context import reporting_errors, state_of
from almacen.cli.output import render_file, render_files

_FILENAME = re.compile(r'filename="([^"]*)"')

TagOption = typer.Option(
    None,
    "--tag",
    "-t",
    help="Tag; repeat the option or separate tags with commas.",
)


def upload(
    ctx: typer.Context,
    path: Path = typer.Argument(..., exists=True, dir_okay=False, readable=True),
    name: Optional[str] = typer.Option(
        None, "--name", "-n", help="Name to store; defaults to the file's name."
    ),
    tags: Optional[list[str]] = TagOption,
) -> None:
    """Upload a file."""
    state = state_of(ctx)
    # Read into memory rather than streaming, so the body can be re-sent to
    # another node on failover.
    content = path.read_bytes()
    data = {"name": name or path.name}
    if joined := ",".join(split_tags(tags)):
        data["tags"] = joined
    with reporting_errors():
        response = state.client.request(
            "POST", "/files", files={"file": (path.name, content)}, data=data
        )
    state.emit(response.json(), render_file)


def list_files(
    ctx: typer.Context,
    tags: Optional[list[str]] = TagOption,
    any_tag: bool = typer.Option(
        False, "--any", help="Match files with any of the tags instead of all."
    ),
) -> None:
    """List live files, optionally only those carrying the given tags."""
    state = state_of(ctx)
    params = {}
    if joined := ",".join(split_tags(tags)):
        params = {"tags": joined, "mode": "or" if any_tag else "and"}
    with reporting_errors():
        response = state.client.request("GET", "/files", params=params)
    state.emit(response.json(), render_files)


def info(ctx: typer.Context, file_id: str = typer.Argument(...)) -> None:
    """Show a file's metadata without downloading its content."""
    state = state_of(ctx)
    with reporting_errors():
        response = state.client.request("GET", f"/files/{file_id}/metadata")
    state.emit(response.json(), render_file)


def download(
    ctx: typer.Context,
    file_id: str = typer.Argument(...),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Destination path, or '-' for stdout. Defaults to the stored name "
        "in the current directory.",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite an existing file."),
) -> None:
    """Download a file's content."""
    state = state_of(ctx)
    with reporting_errors():
        response = state.client.request("GET", f"/files/{file_id}")
    if output == "-":
        typer.echo(response.content, nl=False)
        return
    target = Path(output) if output else _default_target(response, file_id)
    if target.exists() and not force:
        raise typer.BadParameter(
            f"{target} already exists (use --force to overwrite)", param_hint="--output"
        )
    target.write_bytes(response.content)
    result = {"file_id": file_id, "path": str(target), "bytes": len(response.content)}
    state.emit(result, lambda r: f"saved {r['bytes']} bytes to {r['path']}")


def _default_target(response, file_id: str) -> Path:
    match = _FILENAME.search(response.headers.get("content-disposition", ""))
    # Only the final component: a stored name must not choose the directory.
    name = Path(match.group(1)).name if match else ""
    return Path(name or file_id)


def update(
    ctx: typer.Context,
    file_id: str = typer.Argument(...),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="New name."),
    file: Optional[Path] = typer.Option(
        None, "--file", exists=True, dir_okay=False, readable=True,
        help="Replace the content with this file.",
    ),
) -> None:
    """Rename a file and/or replace its content."""
    if name is None and file is None:
        raise typer.BadParameter("nothing to update: pass --name and/or --file")
    state = state_of(ctx)
    data = {"name": name} if name is not None else None
    files = {"file": (file.name, file.read_bytes())} if file is not None else None
    with reporting_errors():
        response = state.client.request(
            "PATCH", f"/files/{file_id}", data=data, files=files
        )
    state.emit(response.json(), render_file)


def delete(ctx: typer.Context, file_id: str = typer.Argument(...)) -> None:
    """Delete a file."""
    state = state_of(ctx)
    with reporting_errors():
        state.client.request("DELETE", f"/files/{file_id}")
    state.emit({"file_id": file_id, "deleted": True}, lambda r: f"deleted {r['file_id']}")


def register(app: typer.Typer) -> None:
    app.command()(upload)
    app.command("list")(list_files)
    app.command()(info)
    app.command()(download)
    app.command("get", help="Alias of download.")(download)
    app.command()(update)
    app.command()(delete)
