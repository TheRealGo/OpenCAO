"""Process-bound enrollment capabilities for managed Workers.

No raw launch ticket is written to disk or exposed through argv, environment,
HTTP, or durable state.  The CAO process keeps it in memory and exchanges it
exactly once. It may redeliver the resulting in-memory runtime credential only
while the exact kernel-bound launch root remains alive and only to its current
descendants. This accommodates an app-server recreating its short-lived stdio
MCP child without reusing the launch ticket.
"""

from __future__ import annotations

import asyncio
import ctypes
import hashlib
import json
import os
import re
import socket
import stat
import struct
import sys
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MAX_ENROLLMENT_CAPABILITY_BYTES = 16 * 1024
MAX_PROCESS_ANCESTRY_DEPTH = 128
_TICKET_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_DARWIN_SOL_LOCAL = 0
_DARWIN_LOCAL_PEERPID = 2
_DARWIN_PROC_PIDTBSDINFO = 3
_UNIX_PATH_LIMIT = 103


class EnrollmentCapabilityError(RuntimeError):
    """A managed launch capability could not be established safely."""


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    pid: int
    parent_pid: int
    start_signature: str


class _DarwinProcBSDInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


def _process_identity(pid: int) -> ProcessIdentity:
    """Read a PID generation and parent directly from the local kernel."""

    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise EnrollmentCapabilityError("managed runner PID is invalid")
    if sys.platform == "darwin":
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        proc_pidinfo = library.proc_pidinfo
        proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        proc_pidinfo.restype = ctypes.c_int
        info = _DarwinProcBSDInfo()
        size = ctypes.sizeof(info)
        if proc_pidinfo(pid, _DARWIN_PROC_PIDTBSDINFO, 0, ctypes.byref(info), size) != size:
            raise EnrollmentCapabilityError("managed runner process identity is unavailable")
        if int(info.pbi_pid) != pid:
            raise EnrollmentCapabilityError("managed runner process identity changed")
        return ProcessIdentity(
            pid=pid,
            parent_pid=int(info.pbi_ppid),
            start_signature=f"{int(info.pbi_start_tvsec)}:{int(info.pbi_start_tvusec)}",
        )
    if sys.platform.startswith("linux"):
        try:
            raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
            close = raw.rfind(")")
            fields = raw[close + 2 :].split()
            if close < 0 or len(fields) < 20:
                raise ValueError
            return ProcessIdentity(
                pid=pid,
                parent_pid=int(fields[1]),
                start_signature=fields[19],
            )
        except (OSError, UnicodeError, ValueError) as error:
            raise EnrollmentCapabilityError(
                "managed runner process identity is unavailable"
            ) from error
    raise EnrollmentCapabilityError("managed Worker process binding is unsupported")


def _peer_pid(peer_socket: Any) -> int:
    """Return the kernel-authenticated PID at the other end of a Unix socket."""

    try:
        if sys.platform == "darwin":
            raw = peer_socket.getsockopt(
                _DARWIN_SOL_LOCAL, _DARWIN_LOCAL_PEERPID, struct.calcsize("i")
            )
            return int(struct.unpack("i", raw)[0])
        if sys.platform.startswith("linux") and hasattr(socket, "SO_PEERCRED"):
            raw = peer_socket.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
            )
            return int(struct.unpack("3i", raw)[0])
    except (OSError, TypeError, ValueError, struct.error) as error:
        raise EnrollmentCapabilityError("managed Worker peer identity is unavailable") from error
    raise EnrollmentCapabilityError("managed Worker peer identity is unsupported")


def _is_bound_process(peer_pid: int, root: ProcessIdentity) -> bool:
    """Require the peer to be the exact launch root or a current descendant."""

    current = peer_pid
    seen: set[int] = set()
    for _ in range(MAX_PROCESS_ANCESTRY_DEPTH):
        if current <= 0 or current in seen:
            return False
        seen.add(current)
        try:
            identity = _process_identity(current)
        except EnrollmentCapabilityError:
            return False
        if current == root.pid:
            return identity.start_signature == root.start_signature
        if identity.parent_pid <= 1:
            return False
        current = identity.parent_pid
    return False


def _effective_socket_root(configured_root: Path, socket_name: str) -> Path:
    configured = Path(configured_root).expanduser()
    if not configured.is_absolute():
        raise EnrollmentCapabilityError("managed enrollment socket root must be absolute")
    if sys.platform == "darwin":
        temporary_alias = Path("/tmp")
        try:
            relative = configured.relative_to(temporary_alias)
        except ValueError:
            pass
        else:
            try:
                canonical_temporary = temporary_alias.resolve(strict=True)
            except OSError as error:
                raise EnrollmentCapabilityError(
                    "managed enrollment socket root is unavailable"
                ) from error
            if canonical_temporary != Path("/private/tmp"):
                raise EnrollmentCapabilityError(
                    "managed enrollment temporary alias is unsafe"
                )
            configured = canonical_temporary / relative
    if len(os.fsencode(configured / socket_name)) <= _UNIX_PATH_LIMIT:
        return configured
    namespace = hashlib.sha256(os.fsencode(configured)).hexdigest()[:16]
    temporary_root = Path("/private/tmp") if sys.platform == "darwin" else Path("/tmp")
    return temporary_root / f"cao-a2a-{os.geteuid()}" / namespace


def _prepare_private_directory(path: Path) -> Path:
    try:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or path.resolve(strict=True) != path
        ):
            raise EnrollmentCapabilityError("managed enrollment socket root is unsafe")
        os.chmod(path, 0o700)
        if stat.S_IMODE(path.stat().st_mode) != 0o700:
            raise EnrollmentCapabilityError("managed enrollment socket root is not private")
    except EnrollmentCapabilityError:
        raise
    except OSError as error:
        raise EnrollmentCapabilityError("managed enrollment socket root is unavailable") from error
    return path


def enrollment_capability_path(configured_root: Path, ticket_id: str) -> Path:
    if not isinstance(ticket_id, str) or _TICKET_ID.fullmatch(ticket_id) is None:
        raise EnrollmentCapabilityError("managed enrollment ticket ID is invalid")
    socket_name = f"e-{hashlib.sha256(ticket_id.encode('ascii')).hexdigest()[:20]}.sock"
    root = _effective_socket_root(Path(configured_root), socket_name)
    path = root / socket_name
    if len(os.fsencode(path)) > _UNIX_PATH_LIMIT:
        raise EnrollmentCapabilityError("managed enrollment socket path is too long")
    return path


def cleanup_enrollment_capability(configured_root: Path, ticket_id: str) -> bool:
    """Remove only the exact socket node derived from a durable ticket ID."""

    path = enrollment_capability_path(configured_root, ticket_id)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise EnrollmentCapabilityError("managed enrollment socket cannot be inspected") from error
    if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.geteuid():
        raise EnrollmentCapabilityError("managed enrollment artifact is not the expected socket")
    try:
        path.unlink()
    except OSError as error:
        raise EnrollmentCapabilityError("managed enrollment socket cannot be cleaned") from error
    return True


class EnrollmentCapabilityBroker:
    """Exchange a ticket once and deliver its credential to one launch tree."""

    def __init__(
        self,
        *,
        configured_root: Path,
        ticket_id: str,
        raw_ticket: str,
        exchange: Callable[[str], Mapping[str, Any]],
        delivery_failed: Callable[[str], None],
    ) -> None:
        raw = raw_ticket.encode("utf-8")
        if not raw or len(raw) > MAX_ENROLLMENT_CAPABILITY_BYTES:
            raise EnrollmentCapabilityError("managed enrollment ticket has an invalid size")
        self._configured_root = Path(configured_root)
        self.path = enrollment_capability_path(configured_root, ticket_id)
        self.ticket_id = ticket_id
        self._ticket = bytearray(raw)
        self._exchange = exchange
        self._delivery_failed = delivery_failed
        self._server: asyncio.AbstractServer | None = None
        self._root: ProcessIdentity | None = None
        self._root_bound = asyncio.Event()
        self._claim_lock = asyncio.Lock()
        self._credential_payload = bytearray()
        self._closed = False

    async def start(self) -> None:
        if self._server is not None or self._closed:
            raise EnrollmentCapabilityError("managed enrollment broker state is invalid")
        root = _prepare_private_directory(self.path.parent)
        if self.path.exists() or self.path.is_symlink():
            raise EnrollmentCapabilityError("managed enrollment socket already exists")
        try:
            self._server = await asyncio.start_unix_server(
                self._handle_peer,
                path=str(self.path),
                limit=MAX_ENROLLMENT_CAPABILITY_BYTES,
                backlog=16,
            )
            os.chmod(self.path, 0o600)
            metadata = self.path.lstat()
            if (
                self.path.parent != root
                or not stat.S_ISSOCK(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise EnrollmentCapabilityError("managed enrollment socket is unsafe")
        except Exception:
            await self.close()
            raise

    def bind_runner_pid(self, pid: int) -> ProcessIdentity:
        identity = _process_identity(pid)
        if self._root is not None and self._root != identity:
            raise EnrollmentCapabilityError("managed enrollment runner binding changed")
        self._root = identity
        self._root_bound.set()
        return identity

    async def _handle_peer(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        del reader
        delivery_started = False
        try:
            await asyncio.wait_for(self._root_bound.wait(), timeout=10.0)
            root = self._root
            peer_socket = writer.get_extra_info("socket")
            if root is None or peer_socket is None or not _is_bound_process(
                _peer_pid(peer_socket), root
            ):
                return
            async with self._claim_lock:
                if self._closed:
                    return
                if not self._credential_payload:
                    if not self._ticket:
                        return
                    try:
                        ticket = bytes(self._ticket).decode("utf-8")
                        response = dict(self._exchange(ticket))
                    finally:
                        self._zero_ticket()
                    encoded = json.dumps(
                        response, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                    if not encoded or len(encoded) >= MAX_ENROLLMENT_CAPABILITY_BYTES:
                        raise EnrollmentCapabilityError(
                            "managed enrollment broker response is invalid"
                        )
                    self._credential_payload.extend(encoded)
                delivery_started = True
                writer.write(bytes(self._credential_payload) + b"\n")
                await writer.drain()
        except (TimeoutError, OSError, UnicodeError, ValueError, TypeError):
            self._fail_credential_delivery(delivery_started)
        except EnrollmentCapabilityError:
            self._fail_credential_delivery(delivery_started)
        except Exception:
            self._fail_credential_delivery(delivery_started)
        finally:
            writer.close()
            with suppress(ConnectionError, OSError):
                await writer.wait_closed()

    def _zero_ticket(self) -> None:
        for index in range(len(self._ticket)):
            self._ticket[index] = 0
        self._ticket.clear()

    def _zero_credential_payload(self) -> None:
        for index in range(len(self._credential_payload)):
            self._credential_payload[index] = 0
        self._credential_payload.clear()

    def _fail_credential_delivery(self, delivery_started: bool) -> None:
        if not delivery_started:
            return
        self._zero_credential_payload()
        self._delivery_failed("managed enrollment credential delivery failed")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._zero_ticket()
        self._zero_credential_payload()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        try:
            cleanup_enrollment_capability(self._configured_root, self.ticket_id)
        except EnrollmentCapabilityError:
            # The configured root may differ when the Unix path-length fallback
            # was selected; unlink only the already-verified exact live path.
            try:
                metadata = self.path.lstat()
                if stat.S_ISSOCK(metadata.st_mode) and metadata.st_uid == os.geteuid():
                    self.path.unlink()
            except FileNotFoundError:
                pass


async def receive_enrollment_capability(
    socket_path: str | os.PathLike[str],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Receive a root-bound enrollment result without exposing its ticket."""

    path = Path(socket_path)
    if not path.is_absolute() or "\x00" in str(path):
        raise EnrollmentCapabilityError("managed enrollment socket path is invalid")
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(str(path), limit=MAX_ENROLLMENT_CAPABILITY_BYTES),
            timeout=timeout_seconds,
        )
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout_seconds)
            if not raw or len(raw) >= MAX_ENROLLMENT_CAPABILITY_BYTES or not raw.endswith(b"\n"):
                raise EnrollmentCapabilityError("managed enrollment capability was rejected")
            value = json.loads(raw)
            if not isinstance(value, Mapping):
                raise EnrollmentCapabilityError("managed enrollment response is invalid")
            return dict(value)
        finally:
            writer.close()
            await writer.wait_closed()
    except EnrollmentCapabilityError:
        raise
    except (OSError, TimeoutError, UnicodeError, json.JSONDecodeError) as error:
        raise EnrollmentCapabilityError("managed enrollment capability is unavailable") from error
