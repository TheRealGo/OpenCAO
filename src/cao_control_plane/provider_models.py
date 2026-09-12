"""Model selector transport, independent of any provider's model catalog."""

from __future__ import annotations

import re
from typing import Annotated

from pydantic import AfterValidator, Field

from .security import contains_control_plane_secret, contains_generic_credential_text

MODEL_IDENTIFIER_MAX_LENGTH = 512
MODEL_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:@+/-]*(?:\[[A-Za-z0-9._:+-]+\])?$"
_MODEL_IDENTIFIER = re.compile(MODEL_IDENTIFIER_PATTERN)


def model_identifier(value: object) -> str | None:
    """Check a bounded selector, never whether a provider offers that model.

    Namespaces, version suffixes, and context-window aliases are provider
    syntax. Options, URLs, filesystem traversal, and control text are not
    model selectors and cannot enter launch arguments or public labels.
    """

    if (
        not isinstance(value, str)
        or len(value) > MODEL_IDENTIFIER_MAX_LENGTH
        or _MODEL_IDENTIFIER.fullmatch(value) is None
        or re.match(r"^[A-Za-z]:/", value) is not None
        or "://" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or contains_control_plane_secret(value)
        or contains_generic_credential_text(value)
    ):
        return None
    return value


def _validate_model_identifier(value: str) -> str:
    if model_identifier(value) is None:
        raise ValueError("model must be a bounded provider identifier or alias")
    return value


ProviderModel = Annotated[
    str,
    Field(min_length=1, max_length=MODEL_IDENTIFIER_MAX_LENGTH, pattern=MODEL_IDENTIFIER_PATTERN),
    AfterValidator(_validate_model_identifier),
]
