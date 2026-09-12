"""Owner-local issuer for one-use CAO attachment bootstrap capabilities.

The authority boundary is the daemon owner's real private directory, its exact
``0600`` Unix socket, and the kernel-reported peer UID. The peer PID and start
identity are audit/routing bindings, not a signed-host admission policy. The
bridge also supplies the catalog digest and proxy ABI loaded in that process;
the one-use CAB and later connection-bound CSC preserve the remaining fences.

Processes running as the daemon's OS user are inside this application's trust
boundary. This module therefore never inspects executable paths, argv,
ancestry, code signatures, application bundles, or launch-service state.
"""

from __future__ import annotations

import asyncio
import ctypes
import hashlib
import json
import os
import socket
import stat
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path

from .connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from .runtime_enrollment import (
    EnrollmentCapabilityError,
    ProcessIdentity,
    _effective_socket_root,
    _peer_pid,
    _prepare_private_directory,
    _process_identity,
)

_RETRYABLE_ISSUER_ERRORS = frozenset({"peer_unavailable"})
_PUBLIC_ISSUER_FAILURES: dict[str, str] = {
    "peer_unavailable": "attachment_peer_unavailable",
    "context_invalid": "attachment_context_invalid",
    "catalog_refresh": "attachment_catalog_refresh_required",
}
_MAX_PROXY_ABI_VERSION = 2_147_483_647


AttachmentBootstrapIssuer = Callable[
    [
        ProcessIdentity,
        str,
        str,
        str,
        int,
    ],
    str,
]


class AttachmentIssuerError(RuntimeError):
    pass


class AttachmentIssuerRetryableError(AttachmentIssuerError):
    """A pre-issuance peer race may be retried on one fresh UDS connection."""


class AttachmentPeerUnavailable(AttachmentIssuerRetryableError):
    """The kernel-authenticated peer identity could not be captured."""


class AttachmentCatalogRefreshRequired(AttachmentIssuerError):
    """The connecting bridge did not load the current proxy contract."""


class AttachmentIssuerRemoteError(AttachmentIssuerError):
    """One allowlisted, path-free issuer outcome safe for the MCP bridge."""

    def __init__(self, reason_code: str, *, retryable: bool) -> None:
        super().__init__("attachment issuer rejected the bootstrap request")
        self.reason_code = reason_code
        self.retryable = retryable


AttachmentPeerIdentityProvider = Callable[[int], ProcessIdentity]


def resolve_owner_peer_identity(peer_pid: int) -> ProcessIdentity:
    """Capture one exact peer generation without inspecting its executable."""

    try:
        return _process_identity(peer_pid)
    except EnrollmentCapabilityError as error:
        raise AttachmentPeerUnavailable("attachment peer identity is unavailable") from error


def _attachment_context(raw: bytes) -> tuple[str, str, str, int]:
    """Parse the exact bridge-loaded conversation and proxy contract."""

    try:
        value = json.loads(raw)
    except (TypeError, UnicodeError, json.JSONDecodeError) as error:
        raise AttachmentIssuerError("attachment context is invalid") from error
    if not isinstance(value, dict) or set(value) != {
        "native_thread_id",
        "project_digest",
        "proxy_catalog_digest",
        "proxy_abi_version",
    }:
        raise AttachmentIssuerError("attachment context is invalid")
    thread_id = value.get("native_thread_id")
    project_digest = value.get("project_digest")
    proxy_catalog_digest = value.get("proxy_catalog_digest")
    proxy_abi_version = value.get("proxy_abi_version")
    if (
        not isinstance(thread_id, str)
        or not thread_id
        or len(thread_id) > 256
        or "\x00" in thread_id
        or not isinstance(project_digest, str)
        or len(project_digest) != 64
        or any(char not in "0123456789abcdef" for char in project_digest)
        or not isinstance(proxy_catalog_digest, str)
        or len(proxy_catalog_digest) != 64
        or any(char not in "0123456789abcdef" for char in proxy_catalog_digest)
        or not isinstance(proxy_abi_version, int)
        or isinstance(proxy_abi_version, bool)
        or proxy_abi_version <= 0
        or proxy_abi_version > _MAX_PROXY_ABI_VERSION
    ):
        raise AttachmentIssuerError("attachment context is invalid")
    return thread_id, project_digest, proxy_catalog_digest, proxy_abi_version


def attachment_issuer_path(state_dir: Path) -> Path:
    root = Path(state_dir).expanduser()
    name = f"i-{hashlib.sha256(os.fsencode(root)).hexdigest()[:20]}.sock"
    return _effective_socket_root(root, name) / name


def _peer_uid(peer: socket.socket) -> int:
    getpeereid = getattr(peer, "getpeereid", None)
    if callable(getpeereid):
        uid, _ = getpeereid()
        return int(uid)
    if hasattr(socket, "SO_PEERCRED"):
        import struct

        raw = peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        _, uid, _ = struct.unpack("3i", raw)
        return int(uid)
    if os.uname().sysname == "Darwin":
        uid = ctypes.c_uint()
        gid = ctypes.c_int()
        getpeereid = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True).getpeereid
        getpeereid.argtypes = [
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint),
            ctypes.POINTER(ctypes.c_int),
        ]
        getpeereid.restype = ctypes.c_int
        if getpeereid(peer.fileno(), ctypes.byref(uid), ctypes.byref(gid)) == 0:
            return int(uid.value)
    raise AttachmentPeerUnavailable("attachment peer identity is unavailable")


def _assert_private_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or path.resolve(strict=True) != path
        ):
            raise AttachmentIssuerError("attachment issuer directory is unsafe")
    except AttachmentIssuerError:
        raise
    except (OSError, UnicodeError) as error:
        raise AttachmentIssuerError("attachment issuer directory is unavailable") from error


def _assert_private_socket(path: Path) -> None:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISSOCK(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AttachmentIssuerError("attachment issuer socket is unsafe")
    except AttachmentIssuerError:
        raise
    except (OSError, UnicodeError) as error:
        raise AttachmentIssuerError("attachment issuer socket is unavailable") from error


async def _write_issuer_failure(
    writer: asyncio.StreamWriter, error_code: str, *, retryable: bool
) -> None:
    """Return only a fixed pre-issuance outcome over the owner-local socket."""

    try:
        writer.write(
            json.dumps(
                {"error": error_code, "retryable": retryable}, separators=(",", ":")
            ).encode()
            + b"\n"
        )
        await writer.drain()
    except (ConnectionError, OSError):
        return


class AttachmentCapabilityIssuer:
    def __init__(
        self,
        state_dir: Path,
        issue: AttachmentBootstrapIssuer,
        *,
        peer_identity_provider: AttachmentPeerIdentityProvider | None = None,
        prepare_project: Callable[[str, str, str, int], Awaitable[None]] | None = None,
    ) -> None:
        self.path = attachment_issuer_path(state_dir)
        self._issue = issue
        self._peer_identity_provider = peer_identity_provider
        self._prepare_project = prepare_project
        self._server: asyncio.AbstractServer | None = None
        self._owns_socket = False

    def _peer_identity(self, peer: socket.socket) -> ProcessIdentity:
        try:
            peer_pid = _peer_pid(peer)
            provider = self._peer_identity_provider or resolve_owner_peer_identity
            value = provider(peer_pid)
        except (AttachmentIssuerError, EnrollmentCapabilityError) as error:
            raise AttachmentPeerUnavailable("attachment peer identity is unavailable") from error
        if not isinstance(value, ProcessIdentity):
            raise AttachmentPeerUnavailable("attachment peer identity is unavailable")
        return value

    async def start(self) -> None:
        try:
            _prepare_private_directory(self.path.parent)
        except EnrollmentCapabilityError as error:
            raise AttachmentIssuerError("attachment issuer directory is unsafe") from error
        _assert_private_directory(self.path.parent)
        if self.path.exists() or self.path.is_symlink():
            _assert_private_socket(self.path)
            # The socket is also the owner-local single-daemon fence. Never
            # unlink a live daemon's endpoint.
            try:
                _reader, writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(str(self.path), limit=4096),
                    timeout=0.5,
                )
            except (ConnectionRefusedError, FileNotFoundError):
                self.path.unlink()
            except OSError as error:
                raise AttachmentIssuerError("attachment issuer socket state is unknown") from error
            else:
                writer.close()
                with suppress(ConnectionError, OSError):
                    await writer.wait_closed()
                raise AttachmentIssuerError("attachment issuer is already active")
        self._server = await asyncio.start_unix_server(
            self._handle, path=str(self.path), limit=4096
        )
        try:
            os.chmod(self.path, 0o600)
            _assert_private_socket(self.path)
        except Exception:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            with suppress(FileNotFoundError):
                self.path.unlink()
            raise
        self._owns_socket = True

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            peer = writer.get_extra_info("socket")
            if peer is None:
                await _write_issuer_failure(writer, "peer_unavailable", retryable=True)
                return
            try:
                peer_uid = _peer_uid(peer)
            except (AttachmentIssuerError, OSError, TypeError, ValueError):
                await _write_issuer_failure(writer, "peer_unavailable", retryable=True)
                return
            if peer_uid != os.geteuid():
                await _write_issuer_failure(writer, "peer_unavailable", retryable=False)
                return
            try:
                identity = self._peer_identity(peer)
            except AttachmentPeerUnavailable:
                await _write_issuer_failure(writer, "peer_unavailable", retryable=True)
                return
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
                (
                    native_thread_id,
                    project_digest,
                    proxy_catalog_digest,
                    proxy_abi_version,
                ) = _attachment_context(raw)
            except (AttachmentIssuerError, ValueError, TimeoutError):
                await _write_issuer_failure(writer, "context_invalid", retryable=False)
                return
            if proxy_abi_version != CAO_CONVERSATION_PROXY_ABI_VERSION:
                await _write_issuer_failure(writer, "catalog_refresh", retryable=False)
                return
            try:
                if self._prepare_project is not None:
                    await self._prepare_project(
                        native_thread_id, project_digest, proxy_catalog_digest, proxy_abi_version
                    )
                token = self._issue(
                    identity,
                    native_thread_id,
                    project_digest,
                    proxy_catalog_digest,
                    proxy_abi_version,
                )
            except AttachmentCatalogRefreshRequired:
                await _write_issuer_failure(writer, "catalog_refresh", retryable=False)
                return
            except AttachmentIssuerError:
                await _write_issuer_failure(writer, "context_invalid", retryable=False)
                return
            writer.write(json.dumps({"token": token}, separators=(",", ":")).encode() + b"\n")
            await writer.drain()
        except (
            AttachmentIssuerError,
            EnrollmentCapabilityError,
            ValueError,
            OSError,
            TimeoutError,
        ):
            return
        finally:
            writer.close()
            with suppress(ConnectionError, OSError):
                await writer.wait_closed()

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._owns_socket:
            with suppress(FileNotFoundError, AttachmentIssuerError):
                _assert_private_socket(self.path)
                self.path.unlink()
            self._owns_socket = False


async def receive_attachment_bootstrap(
    socket_path: str | os.PathLike[str],
    *,
    native_thread_id: str,
    project_digest: str,
    proxy_catalog_digest: str,
    proxy_abi_version: int,
    timeout_seconds: float,
) -> str:
    path = Path(socket_path)
    if not path.is_absolute() or "\x00" in str(path):
        raise AttachmentIssuerError("attachment issuer socket is invalid")
    _assert_private_directory(path.parent)
    _assert_private_socket(path)
    context = json.dumps(
        {
            "native_thread_id": native_thread_id,
            "project_digest": project_digest,
            "proxy_catalog_digest": proxy_catalog_digest,
            "proxy_abi_version": proxy_abi_version,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    # Validate locally before sending so malformed caller input cannot be
    # reflected through a peer-visible issuer diagnostic.
    _attachment_context(context)
    for attempt in range(2):
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(str(path), limit=4096), timeout=timeout_seconds
        )
        try:
            writer.write(context + b"\n")
            await writer.drain()
            raw = await asyncio.wait_for(reader.readline(), timeout=timeout_seconds)
        finally:
            writer.close()
            with suppress(ConnectionError, OSError):
                await writer.wait_closed()
        try:
            value = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise AttachmentIssuerError("attachment issuer response is invalid") from error
        token = value.get("token") if isinstance(value, dict) else None
        if isinstance(token, str) and token.startswith("cao.cab_"):
            return token
        error_code = value.get("error") if isinstance(value, dict) else None
        retryable = value.get("retryable") if isinstance(value, dict) else None
        public_reason = (
            _PUBLIC_ISSUER_FAILURES.get(error_code) if isinstance(error_code, str) else None
        )
        if (
            attempt == 0
            and retryable is True
            and error_code in _RETRYABLE_ISSUER_ERRORS
            and public_reason is not None
        ):
            continue
        if public_reason is not None and isinstance(retryable, bool):
            raise AttachmentIssuerRemoteError(public_reason, retryable=retryable)
        raise AttachmentIssuerError("attachment issuer response is invalid")
    raise AttachmentIssuerError("attachment issuer response is invalid")
