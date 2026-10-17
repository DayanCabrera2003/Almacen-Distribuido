"""tag list / add / remove, end to end against a node app."""
from __future__ import annotations

import json

import pytest


@pytest.fixture
def file_id(cli, tmp_path) -> str:
    source = tmp_path / "a.txt"
    source.write_bytes(b"x")
    result = cli("--json", "upload", str(source), "--tag", "keep")
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)["file_id"]


def test_tag_list_prints_one_tag_per_line(cli, file_id):
    result = cli("tag", "list", file_id)

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["keep"]


def test_tag_add_accepts_several_tags(cli, file_id):
    result = cli("--json", "tag", "add", file_id, "b", "a,c")

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"tags": ["a", "b", "c", "keep"]}


def test_tag_remove_removes_each_given_tag(cli, file_id):
    cli("tag", "add", file_id, "a", "b")

    result = cli("tag", "remove", file_id, "a", "keep")

    assert result.exit_code == 0, result.output
    assert result.stdout.splitlines() == ["b"]


def test_removing_the_last_tag_says_so(cli, file_id):
    result = cli("tag", "remove", file_id, "keep")

    assert result.stdout.strip() == "no tags"


def test_tag_commands_on_an_unknown_file_report_not_found(cli):
    unknown = "00000000-0000-0000-0000-000000000000"

    result = cli("tag", "add", unknown, "x")

    assert result.exit_code == 1
    assert "file not found" in result.stderr


def test_blank_tags_are_a_usage_error(cli, file_id):
    assert cli("tag", "remove", file_id, ",").exit_code == 2
    assert cli("tag", "add", file_id, " ").exit_code == 2
