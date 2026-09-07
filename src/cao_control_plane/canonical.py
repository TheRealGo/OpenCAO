"""Stable canonical JSON encoding shared by immutable CAO identities.

This encoding is a persisted protocol contract.  Keep it separate from the
human-readable and security-policy JSON encoders, which intentionally use
different Unicode behavior.
"""

from __future__ import annotations

import hashlib
import json


def canonical_json(value: object) -> str:
    """Return CAO's ASCII, key-sorted, whitespace-free JSON representation."""

    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def canonical_json_bytes(value: object) -> bytes:
    """Return the UTF-8 bytes used by persisted CAO identity digests."""

    return canonical_json(value).encode("utf-8")


def canonical_sha256(value: object) -> str:
    """Return the SHA-256 digest of CAO's canonical JSON representation."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
