"""Persistent directory identity, separate from live device/inode race fences."""

from __future__ import annotations

import ctypes
import fcntl
import hashlib
import os
import stat
import sys
from pathlib import Path

from .canonical import canonical_json_bytes


def object_generation(info: os.stat_result) -> str:
    birthtime = getattr(info, "st_birthtime", None)
    if isinstance(birthtime, (int, float)) and not isinstance(birthtime, bool):
        return "birthtime:" + float(birthtime).hex()
    generation = getattr(info, "st_gen", None)
    if isinstance(generation, int) and not isinstance(generation, bool) and generation > 0:
        return f"stat-generation:{int(generation)}"
    return "stable-generation-unavailable"


class _AttrList(ctypes.Structure):
    _fields_ = [
        ("bitmapcount", ctypes.c_uint16),
        ("reserved", ctypes.c_uint16),
        ("commonattr", ctypes.c_uint32),
        ("volattr", ctypes.c_uint32),
        ("dirattr", ctypes.c_uint32),
        ("fileattr", ctypes.c_uint32),
        ("forkattr", ctypes.c_uint32),
    ]


def _volume_identity(descriptor: int) -> str:
    if sys.platform != "darwin":
        # The portable fallback binds canonical location, inode and available
        # generation. It never treats a mount's transient device as durable.
        return "canonical-location"
    attributes = _AttrList(bitmapcount=5, volattr=0x80040000)
    result = ctypes.create_string_buffer(20)
    function = ctypes.CDLL(None, use_errno=True).fgetattrlist
    function.argtypes = [
        ctypes.c_int, ctypes.POINTER(_AttrList), ctypes.c_void_p,
        ctypes.c_size_t, ctypes.c_ulong,
    ]
    function.restype = ctypes.c_int
    if function(descriptor, ctypes.byref(attributes), result, len(result), 0) != 0:
        raise OSError("persistent volume identity is unavailable")
    size = int.from_bytes(result.raw[:4], sys.byteorder)
    volume = result.raw[4:20]
    if size != 20 or not any(volume):
        raise OSError("persistent volume identity is unavailable")
    return "volume-uuid:" + volume.hex()


def _descriptor_path(descriptor: int, resolved: Path) -> Path:
    if sys.platform != "darwin":
        return resolved
    # F_GETPATH returns the kernel spelling, including case normalization.
    # Path.resolve() alone preserves caller spelling on case-insensitive APFS.
    raw = fcntl.fcntl(descriptor, 50, b"\0" * 1024)
    canonical = Path(os.fsdecode(raw.split(b"\0", 1)[0]))
    if not canonical.is_absolute():
        raise OSError("canonical directory identity is unavailable")
    return canonical


def canonical_directory_path(path: Path) -> Path:
    resolved = path.resolve(strict=True)
    descriptor = os.open(resolved, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        canonical = _descriptor_path(descriptor, resolved)
        opened, current = os.fstat(descriptor), canonical.stat()
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            raise OSError("canonical directory identity changed")
        return canonical
    finally:
        os.close(descriptor)


def directory_identity(path: Path, *, expected: os.stat_result | None = None) -> str:
    """Hash the canonical location and persistent filesystem object identity.

    Paths and volume IDs remain local. Device/inode pairs are used only to
    prove that resolution and the open descriptor observe the same object.
    """
    canonical = path.resolve(strict=True)
    descriptor = os.open(canonical, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        canonical = _descriptor_path(descriptor, canonical)
        volume = _volume_identity(descriptor)
        after = canonical.stat()
        if (
            not stat.S_ISDIR(before.st_mode)
            or (expected is not None and (
                (before.st_dev, before.st_ino) != (expected.st_dev, expected.st_ino)
                or object_generation(before) != object_generation(expected)
            ))
            or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            or object_generation(before) != object_generation(after)
            or canonical.resolve(strict=True) != canonical
        ):
            raise OSError("persistent directory identity changed")
        return hashlib.sha256(canonical_json_bytes({
            "format": "cao-directory-identity/v2",
            "path": os.fspath(canonical),
            "volume": volume,
            "inode": int(before.st_ino),
            "generation": object_generation(before),
        })).hexdigest()
    finally:
        os.close(descriptor)
