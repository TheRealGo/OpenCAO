"""Owner-local OS change notifications, with no filesystem polling fallback."""
from __future__ import annotations

import asyncio
import ctypes
import os
import select
import struct
import sys
from collections.abc import Callable, Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any


class FilesystemEvents:
    def __init__(self, changed: Callable[[list[str]], None]) -> None:
        self.changed = changed
        self.loop = asyncio.get_running_loop()
        self.targets: set[Path] = set()
        self.fingerprints: dict[Path, tuple[int, int, int, int] | None] = {}
        self.watches: dict[int, Path] = {}
        self.failure: asyncio.Future[None] = self.loop.create_future()
        self._queue: Any = None
        self._libc: Any = None
        if sys.platform == "darwin":
            self._queue = select.kqueue()
            self.descriptor = self._queue.fileno()
        elif sys.platform.startswith("linux"):
            self._libc = ctypes.CDLL(None, use_errno=True)
            self._libc.inotify_init1.argtypes = [ctypes.c_int]
            self._libc.inotify_init1.restype = ctypes.c_int
            self._libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            self._libc.inotify_add_watch.restype = ctypes.c_int
            self._libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
            self.descriptor = int(self._libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC))
            if self.descriptor < 0:
                raise OSError("Native filesystem event source unavailable")
        else:
            raise OSError("Native filesystem events are unavailable on this platform")
        self.loop.add_reader(self.descriptor, self._ready)

    @staticmethod
    def _fingerprint(path: Path) -> tuple[int, int, int, int] | None:
        try:
            info = path.stat()
            return info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size
        except FileNotFoundError:
            return None

    def set_paths(self, paths: Iterable[Path]) -> None:
        targets = {path.resolve() for path in paths}
        for path in targets - self.targets:
            self.fingerprints[path] = self._fingerprint(path)
        for path in self.targets - targets:
            self.fingerprints.pop(path, None)
        self.targets = targets
        self._refresh()

    def _refresh(self) -> None:
        # Parent subscriptions admit WAL creation and atomic file replacement.
        needed = {path.parent for path in self.targets}
        if self._queue is not None:
            needed |= {path for path in self.targets if path.exists()}
        for identity, path in list(self.watches.items()):
            if path not in needed or (self._queue is not None and not path.exists()):
                self._remove(identity)
        existing = set(self.watches.values())
        for path in needed - existing:
            if self._queue is not None:
                identity = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
                self._queue.control([select.kevent(
                    identity, filter=select.KQ_FILTER_VNODE,
                    flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR,
                    fflags=select.KQ_NOTE_WRITE | select.KQ_NOTE_EXTEND | select.KQ_NOTE_ATTRIB
                    | select.KQ_NOTE_RENAME | select.KQ_NOTE_DELETE,
                )], 0, 0)
            else:
                # Linux inotify ABI: modify/attribute/close-write, directory
                # entry creation/removal/rename and watched-directory removal.
                mask = 0x2 | 0x4 | 0x8 | 0x40 | 0x80 | 0x100 | 0x200 | 0x400 | 0x800
                identity = int(self._libc.inotify_add_watch(self.descriptor, os.fsencode(path), mask))
                if identity < 0:
                    raise OSError("Native filesystem subscription failed")
            self.watches[identity] = path

    def _remove(self, identity: int) -> None:
        self.watches.pop(identity, None)
        if self._queue is not None:
            with suppress(OSError):
                self._queue.control([select.kevent(identity, filter=select.KQ_FILTER_VNODE, flags=select.KQ_EV_DELETE)], 0, 0)
            os.close(identity)
        else:
            self._libc.inotify_rm_watch(self.descriptor, identity)

    def _ready(self) -> None:
        try:
            self._drain()
        except Exception:
            self.loop.remove_reader(self.descriptor)
            if not self.failure.done():
                self.failure.set_exception(OSError("Native filesystem event subscription failed"))

    def _drain(self) -> None:
        touched: set[Path] = set()
        if self._queue is not None:
            for event in self._queue.control(None, max(1, len(self.watches)), 0):
                path = self.watches.get(event.ident)
                if path is not None:
                    touched.add(path)
                if event.fflags & (select.KQ_NOTE_RENAME | select.KQ_NOTE_DELETE):
                    self._remove(event.ident)
        else:
            header = struct.Struct("iIII")
            while True:
                try:
                    data = os.read(self.descriptor, 65_536)
                except BlockingIOError:
                    break
                if not data:
                    break
                offset = 0
                while offset + header.size <= len(data):
                    identity, mask, _, length = header.unpack_from(data, offset)
                    offset += header.size
                    name = os.fsdecode(data[offset:offset + length].split(b"\0", 1)[0])
                    offset += length
                    if mask & 0x4000:  # IN_Q_OVERFLOW is a real lost-change hint.
                        touched.update(self.targets)
                    root = self.watches.get(identity)
                    if root is not None:
                        touched.add(root / name if name else root)
                    if mask & 0x8000:  # IN_IGNORED follows watch removal.
                        self.watches.pop(identity, None)
        changed = []
        for path in self.targets:
            if path not in touched and path.parent not in touched:
                continue
            fingerprint = self._fingerprint(path)
            if fingerprint != self.fingerprints.get(path):
                self.fingerprints[path] = fingerprint
                changed.append(str(path))
        self._refresh()
        if changed:
            self.changed(changed)

    def close(self) -> None:
        if not self.failure.done():
            self.failure.cancel()
        elif not self.failure.cancelled():
            self.failure.exception()
        self.loop.remove_reader(self.descriptor)
        for identity in list(self.watches):
            self._remove(identity)
        if self._queue is not None:
            self._queue.close()
        else:
            os.close(self.descriptor)
