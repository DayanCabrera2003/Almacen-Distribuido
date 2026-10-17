"""upload, list, download, update and delete, end to end against a node app."""
from __future__ import annotations

import json
from pathlib import Path


def _upload(cli, path: Path, *extra: str) -> dict:
    result = cli("--json", "upload", str(path), *extra)
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


def test_upload_reports_metadata_and_list_shows_the_file(cli, tmp_path):
    source = tmp_path / "report.txt"
    source.write_bytes(b"hello")

    upload = cli("upload", str(source), "--tag", "invoice", "--tag", "draft")
    assert upload.exit_code == 0, upload.output
    assert "report.txt" in upload.stdout
    assert "draft, invoice" in upload.stdout

    listing = cli("list")
    assert listing.exit_code == 0, listing.output
    assert "report.txt" in listing.stdout
    assert "draft,invoice" in listing.stdout


def test_upload_name_overrides_the_file_name_and_tags_accept_commas(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"x")

    body = _upload(cli, source, "--name", "renamed.txt", "--tag", "a,b")

    assert body["name"] == "renamed.txt"
    assert body["tags"] == ["a", "b"]


def test_list_filters_by_tags_with_and_by_default_and_or_on_request(cli, tmp_path):
    source = tmp_path / "f.txt"
    source.write_bytes(b"x")
    _upload(cli, source, "--name", "both", "--tag", "a,b")
    _upload(cli, source, "--name", "only-a", "--tag", "a")

    both = json.loads(cli("--json", "list", "--tag", "a", "--tag", "b").stdout)
    either = json.loads(cli("--json", "list", "--tag", "a,b", "--any").stdout)

    assert [f["name"] for f in both] == ["both"]
    assert sorted(f["name"] for f in either) == ["both", "only-a"]


def test_list_says_so_when_nothing_matches(cli):
    result = cli("list", "--tag", "nothing")

    assert result.exit_code == 0
    assert result.stdout.strip() == "no files"


def test_download_writes_the_server_file_name_by_default(cli, tmp_path, monkeypatch):
    source = tmp_path / "report.txt"
    source.write_bytes(b"payload")
    file_id = _upload(cli, source, "--name", "server-name.txt")["file_id"]
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    monkeypatch.chdir(out_dir)

    result = cli("download", file_id)

    assert result.exit_code == 0, result.output
    assert (out_dir / "server-name.txt").read_bytes() == b"payload"


def test_download_to_a_path_and_to_stdout(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"payload")
    file_id = _upload(cli, source)["file_id"]
    target = tmp_path / "copy.bin"

    to_file = cli("get", file_id, "--output", str(target))
    to_stdout = cli("get", file_id, "--output", "-")

    assert to_file.exit_code == 0, to_file.output
    assert target.read_bytes() == b"payload"
    assert to_stdout.stdout_bytes == b"payload"


def test_download_refuses_to_overwrite_without_force(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"new")
    file_id = _upload(cli, source)["file_id"]
    target = tmp_path / "existing"
    target.write_bytes(b"old")

    refused = cli("download", file_id, "-o", str(target))
    assert refused.exit_code != 0
    assert target.read_bytes() == b"old"

    forced = cli("download", file_id, "-o", str(target), "--force")
    assert forced.exit_code == 0, forced.output
    assert target.read_bytes() == b"new"


def test_update_renames_and_replaces_content(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"v1")
    file_id = _upload(cli, source)["file_id"]
    replacement = tmp_path / "v2.txt"
    replacement.write_bytes(b"v2")

    result = cli(
        "--json", "update", file_id, "--name", "b.txt", "--file", str(replacement)
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["name"] == "b.txt"
    assert cli("get", file_id, "-o", "-").stdout_bytes == b"v2"


def test_update_with_nothing_to_change_is_a_usage_error(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"v1")
    file_id = _upload(cli, source)["file_id"]

    result = cli("update", file_id)

    assert result.exit_code == 2
    assert "--name" in result.output


def test_delete_then_every_command_reports_not_found(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"v1")
    file_id = _upload(cli, source)["file_id"]

    deleted = cli("delete", file_id)
    assert deleted.exit_code == 0, deleted.output
    assert file_id in deleted.stdout

    again = cli("delete", file_id)
    assert again.exit_code == 1
    assert "404" in again.stderr
    assert "file not found" in again.stderr


def test_upload_of_a_missing_local_file_is_a_usage_error(cli, tmp_path):
    result = cli("upload", str(tmp_path / "does-not-exist"))

    assert result.exit_code == 2


def test_info_shows_a_files_metadata(cli, tmp_path):
    source = tmp_path / "report.txt"
    source.write_bytes(b"hello")
    uploaded = _upload(cli, source, "--tag", "invoice,draft")

    result = cli("info", uploaded["file_id"])

    assert result.exit_code == 0, result.output
    assert uploaded["file_id"] in result.stdout
    assert "report.txt" in result.stdout
    assert "draft, invoice" in result.stdout
    assert uploaded["content_hash"] in result.stdout


def test_info_json_is_the_same_object_as_the_upload_response(cli, tmp_path):
    source = tmp_path / "report.txt"
    source.write_bytes(b"hello")
    uploaded = _upload(cli, source, "--tag", "invoice")

    result = cli("--json", "info", uploaded["file_id"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == uploaded


def test_info_on_a_deleted_file_reports_not_found(cli, tmp_path):
    source = tmp_path / "a.txt"
    source.write_bytes(b"v1")
    file_id = _upload(cli, source)["file_id"]
    cli("delete", file_id)

    result = cli("info", file_id)

    assert result.exit_code == 1
    assert "404" in result.stderr
    assert "file not found" in result.stderr
