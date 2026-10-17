import pytest

from almacen.domain.tags import MAX_TAG_LENGTH, InvalidTag, normalize_tag, parse_tag_list


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("invoice", "invoice"),
        ("  invoice  ", "invoice"),
        ("\tinvoice\n", "invoice"),
        ("two words", "two words"),
        ("2026/q1", "2026/q1"),
        ("café", "café"),
        ("x" * MAX_TAG_LENGTH, "x" * MAX_TAG_LENGTH),
    ],
)
def test_normalize_tag_strips_surrounding_whitespace_only(raw, expected):
    assert normalize_tag(raw) == expected


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("", "empty"),
        ("   ", "empty"),
        ("a,b", "','"),
        (",", "','"),
        ("a\nb", "control character"),
        ("a\x00b", "control character"),
        ("a\tb", "control character"),
        ("x" * (MAX_TAG_LENGTH + 1), "longer than"),
    ],
)
def test_normalize_tag_rejects_tags_that_could_never_be_queried(raw, reason):
    with pytest.raises(InvalidTag, match=reason):
        normalize_tag(raw)


def test_invalid_tag_is_a_value_error():
    # Pydantic turns a ValueError raised in a validator into a 422.
    assert issubclass(InvalidTag, ValueError)


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_tag_list_of_nothing_is_empty(raw):
    assert parse_tag_list(raw) == set()


def test_parse_tag_list_splits_on_commas_and_normalizes_each_tag():
    assert parse_tag_list(" invoice , draft,two words ") == {
        "invoice",
        "draft",
        "two words",
    }


@pytest.mark.parametrize("raw", ["a,,b", "a,", ",a", "a, ,b"])
def test_parse_tag_list_rejects_empty_entries(raw):
    with pytest.raises(InvalidTag, match="empty"):
        parse_tag_list(raw)


def test_parse_tag_list_rejects_an_invalid_entry():
    with pytest.raises(InvalidTag, match="control character"):
        parse_tag_list("ok,bad\x07tag")
