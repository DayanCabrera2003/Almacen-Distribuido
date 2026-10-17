"""The single rule for what a tag may be, shared by every API input path.

A tag is only useful if it can be found again: `GET /files?tags=...` takes a
comma-separated list and strips each entry, so a tag stored with a comma,
surrounding whitespace or no characters at all could be written but never
queried. Every path that accepts a tag (upload form, add, remove, query)
normalizes it here, so what is stored is exactly what a query can name.

The rule:

- surrounding whitespace is stripped (inner spaces are kept: "two words");
- the result must not be empty;
- it must not contain a comma, the list separator of the query and form syntax;
- it must not contain control characters (newlines, tabs, NUL, ...), which are
  invisible in listings and unsafe in headers and logs;
- it must be at most MAX_TAG_LENGTH characters long.

Anything else, including "/" and non-ASCII letters, is allowed.
"""
from __future__ import annotations

import unicodedata

MAX_TAG_LENGTH = 128
TAG_SEPARATOR = ","


class InvalidTag(ValueError):
    """A tag that breaks the rule above. A ValueError, so Pydantic reports it."""


def normalize_tag(raw: str) -> str:
    """Return the canonical form of one tag, or raise InvalidTag."""
    tag = raw.strip()
    if not tag:
        raise InvalidTag("tag must not be empty")
    if TAG_SEPARATOR in tag:
        raise InvalidTag(f"tag {tag!r} must not contain {TAG_SEPARATOR!r}")
    if any(unicodedata.category(char) == "Cc" for char in tag):
        raise InvalidTag(f"tag {tag!r} must not contain a control character")
    if len(tag) > MAX_TAG_LENGTH:
        raise InvalidTag(
            f"tag {tag[:16]!r}... is longer than {MAX_TAG_LENGTH} characters"
        )
    return tag


def parse_tag_list(raw: str | None) -> set[str]:
    """Parse a comma-separated tag list, as sent in a form field or query.

    A missing or blank value means no tags. Otherwise every entry must be a
    valid tag: "a,,b" is rejected rather than silently read as "a,b".
    """
    if raw is None or not raw.strip():
        return set()
    return {normalize_tag(part) for part in raw.split(TAG_SEPARATOR)}
