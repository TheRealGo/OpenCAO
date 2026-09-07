"""Strict owner-private credentials shared by Dashboard runtime boundaries."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from .cloudflare_access import CloudflareAccessSettings

_OWNER_ONLY_FILE_MAX_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class DashboardCredentials:
    upstream_base_url: str
    dashboard_bearer: str
    public_origin: str | None
    allowed_private_upstream_hosts: tuple[str, ...]
    cloudflare_access: CloudflareAccessSettings | None


def load_dashboard_credentials(path: Path) -> DashboardCredentials:
    """Load the edge bearer only from an owner-only, non-symlinked file."""

    value = _read_owner_only_file(path)
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError("dashboard credential file must contain JSON") from error
    if not isinstance(payload, dict):
        raise ValueError("dashboard credential file must contain an object")
    allowed = {
        "upstream_base_url",
        "dashboard_bearer",
        "public_origin",
        "allowed_private_upstream_hosts",
        "cloudflare_access_team_domain",
        "cloudflare_access_audience",
        "cloudflare_access_allowed_email",
    }
    if set(payload) - allowed:
        raise ValueError("dashboard credential file contains unsupported fields")
    upstream = payload.get("upstream_base_url")
    bearer = payload.get("dashboard_bearer")
    origin = payload.get("public_origin")
    hosts = payload.get("allowed_private_upstream_hosts", [])
    access_team_domain = payload.get("cloudflare_access_team_domain")
    access_audience = payload.get("cloudflare_access_audience")
    access_email = payload.get("cloudflare_access_allowed_email")
    if not isinstance(upstream, str) or not isinstance(bearer, str):
        raise ValueError("dashboard credential file is missing required fields")
    if origin is not None and not isinstance(origin, str):
        raise ValueError("dashboard public_origin must be a string")
    if not isinstance(hosts, list) or not all(isinstance(item, str) for item in hosts):
        raise ValueError("dashboard allowed_private_upstream_hosts must be a string list")
    access_values = (access_team_domain, access_audience, access_email)
    if any(item is not None for item in access_values) and not all(
        isinstance(item, str) for item in access_values
    ):
        raise ValueError("dashboard Cloudflare Access fields must be configured together")
    access: CloudflareAccessSettings | None = None
    if (
        isinstance(access_team_domain, str)
        and isinstance(access_audience, str)
        and isinstance(access_email, str)
    ):
        access = CloudflareAccessSettings(access_team_domain, access_audience, access_email)
    return DashboardCredentials(upstream, bearer, origin, tuple(hosts), access)


def _read_owner_only_file(path: Path) -> str:
    """Read exactly one regular 0600 file without following a symlink."""

    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError("dashboard credential file must be a regular file")
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError("dashboard credential file must be owner-only (0600)")
    if info.st_size > _OWNER_ONLY_FILE_MAX_BYTES:
        raise ValueError("owner-only file exceeds the maximum supported size")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (
            info.st_dev,
            info.st_ino,
        ):
            raise ValueError("dashboard credential file changed while opening")
        chunks: list[bytes] = []
        remaining = _OWNER_ONLY_FILE_MAX_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
        try:
            current = path.lstat()
        except OSError as error:
            raise ValueError("dashboard credential file changed while reading") from error
        if len(payload) > _OWNER_ONLY_FILE_MAX_BYTES or after.st_size > _OWNER_ONLY_FILE_MAX_BYTES:
            raise ValueError("owner-only file exceeds the maximum supported size")
        if (
            (after.st_dev, after.st_ino) != (info.st_dev, info.st_ino)
            or after.st_size != info.st_size
            or after.st_mtime_ns != info.st_mtime_ns
            or after.st_ctime_ns != info.st_ctime_ns
            or (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
            or current.st_size != info.st_size
            or current.st_mtime_ns != info.st_mtime_ns
            or current.st_ctime_ns != info.st_ctime_ns
        ):
            raise ValueError("dashboard credential file changed while reading")
        return payload.decode("utf-8")
    finally:
        os.close(descriptor)
