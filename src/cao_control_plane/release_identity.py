"""Deterministic identity for the exact local CAO code and MCP surface."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_RELEASE_FORMAT = "cao-release-identity/v1"
_TRACKED_SUFFIXES = frozenset({".py", ".html", ".css", ".js"})


@dataclass(frozen=True, slots=True)
class ReleaseIdentity:
    release_id: str
    schema_version: int
    mcp_catalog_digest: str

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)


def _release_files(package_root: Path) -> tuple[Path, ...]:
    files: list[Path] = []
    for candidate in package_root.rglob("*"):
        relative = candidate.relative_to(package_root)
        if "__pycache__" in relative.parts:
            continue
        if candidate.name != "py.typed" and candidate.suffix not in _TRACKED_SUFFIXES:
            continue
        info = candidate.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise RuntimeError("release content contains an unsupported file identity")
        files.append(candidate)
    return tuple(sorted(files, key=lambda item: item.relative_to(package_root).as_posix()))


def _read_release_file(path: Path) -> bytes:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise RuntimeError("release content must be a regular file")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        finished = os.fstat(descriptor)
        current = path.lstat()

        def identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if identity(opened) != identity(finished) or identity(finished) != identity(current):
            raise RuntimeError("release content changed while it was read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def package_content_digest(package_root: Path | None = None) -> str:
    """Hash executable package source and static assets, excluding caches."""

    root = (package_root or Path(__file__).resolve().parent).resolve(strict=True)
    digest = hashlib.sha256(b"cao-release-content/v1\0")
    for path in _release_files(root):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = _read_release_file(path)
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def catalog_digest(tools: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        tools,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"cao-conversation-mcp-catalog/v1\0" + canonical).hexdigest()


def current_release_identity() -> ReleaseIdentity:
    """Return the identity advertised by this exact running package."""

    from .database import SCHEMA_VERSION
    from .mcp import conversation_proxy_tools

    return ReleaseIdentity(
        release_id=package_content_digest(),
        schema_version=SCHEMA_VERSION,
        mcp_catalog_digest=catalog_digest(conversation_proxy_tools()),
    )


def release_identity_format() -> str:
    return _RELEASE_FORMAT
