"""Fail-closed media contract for CAO-readable verified artifacts."""

from __future__ import annotations

import re


class ArtifactContentTypeError(ValueError):
    """A stable, path-free artifact content type failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


_TOKEN = re.compile(r"^[A-Za-z0-9!#$&^_.+-]+$")
_STRUCTURED_TEXT_TYPES = frozenset(
    {
        "application/json",
        "application/xml",
        "application/yaml",
        "application/x-yaml",
        "application/toml",
        "application/javascript",
    }
)


def canonical_utf8_text_media_type(value: object) -> str:
    """Return one canonical textual MIME type or reject it.

    Only a UTF-8 charset parameter is accepted.  Treating arbitrary Worker
    metadata as executable or binary content would expand the review surface,
    so every other parameter and every non-text type fails closed.
    """

    if (
        not isinstance(value, str)
        or not value
        or len(value) > 200
        or value != value.strip()
        or any(ord(character) < 0x20 or ord(character) > 0x7E for character in value)
    ):
        raise ArtifactContentTypeError("artifact_content_media_type_invalid")
    parts = [part.strip() for part in value.split(";")]
    type_parts = parts[0].split("/")
    if len(type_parts) != 2 or not all(_TOKEN.fullmatch(part) for part in type_parts):
        raise ArtifactContentTypeError("artifact_content_media_type_invalid")
    base = "/".join(part.lower() for part in type_parts)
    parameters: list[str] = []
    for raw in parts[1:]:
        name, separator, parameter_value = raw.partition("=")
        if (
            not separator
            or name.strip().lower() != "charset"
            or parameter_value.strip().lower() != "utf-8"
            or parameters
        ):
            raise ArtifactContentTypeError("artifact_content_media_type_unsupported")
        parameters.append("charset=utf-8")
    if not (
        base.startswith("text/")
        or base in _STRUCTURED_TEXT_TYPES
        or (base.startswith("application/") and base.endswith(("+json", "+xml")))
    ):
        raise ArtifactContentTypeError("artifact_content_media_type_unsupported")
    return ";".join([base, *parameters])
