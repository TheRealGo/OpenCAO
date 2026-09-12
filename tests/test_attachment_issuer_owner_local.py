from __future__ import annotations

import json
import os
import socket
import sqlite3
import stat
from pathlib import Path
from typing import Any

import pytest
from conftest import current_cao_session_attachment
from pydantic import ValidationError as PydanticValidationError

import cao_control_plane.attachment_issuer as issuer_module
from cao_control_plane.attachment_issuer import (
    AttachmentCapabilityIssuer,
    AttachmentCatalogRefreshRequired,
    AttachmentIssuerError,
    AttachmentIssuerRemoteError,
    _attachment_context,
    attachment_issuer_path,
    receive_attachment_bootstrap,
    resolve_owner_peer_identity,
)
from cao_control_plane.config import Settings
from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.database import (
    SCHEMA_VERSION,
    Database,
    _normalize_cao_attachment_peer_schema_v35,
)
from cao_control_plane.errors import AuthenticationError, AuthorizationError
from cao_control_plane.models import CAOSessionAttachment
from cao_control_plane.runtime_enrollment import EnrollmentCapabilityError, ProcessIdentity

_CATALOG_DIGEST = "a" * 64
_PROJECT_DIGEST = "b" * 64


def _context(**overrides: object) -> bytes:
    value: dict[str, object] = {
        "native_thread_id": "thread",
        "project_digest": _PROJECT_DIGEST,
        "proxy_catalog_digest": _CATALOG_DIGEST,
        "proxy_abi_version": CAO_CONVERSATION_PROXY_ABI_VERSION,
    }
    value.update(overrides)
    return json.dumps(value, separators=(",", ":")).encode()


def _identity(pid: int = 4401) -> ProcessIdentity:
    return ProcessIdentity(pid=pid, parent_pid=1, start_signature="peer-start")


def test_fresh_v36_schema_exposes_one_peer_identity_and_no_host_aliases(
    settings: Settings,
) -> None:
    database = Database(settings)
    database.initialize()
    assert SCHEMA_VERSION == 45
    expected = {
        "cao_session_attachments": set(),
        "cao_attachment_connections": {"peer_pid", "peer_start_signature"},
        "cao_attachment_bootstrap_credentials": {
            "attachment_generation",
            "peer_pid",
            "peer_start_signature",
        },
    }
    forbidden = {
        "host_root_pid",
        "host_root_start_signature",
        "bridge_pid",
        "bridge_start_signature",
        "host_attestation_receipt_id",
    }
    with database.connect() as connection:
        for table, required in expected.items():
            columns = {
                str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")
            }
            assert required <= columns
            assert not columns & forbidden
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'cao_host_attestation_receipts'"
            ).fetchone()
            is None
        )


def test_service_attach_has_no_implicit_process_derived_fallback(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    before = {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")["count"])
        for table in (
            "cao_session_attachments",
            "cao_attachment_connections",
            "cao_attachment_bootstrap_credentials",
        )
    }
    with pytest.raises(AuthorizationError, match="explicit one-use bootstrap"):
        service.attach_cao_session(
            system["cao"],
            current_cao_session_attachment(
                native_thread_id="implicit-fallback-must-not-exist",
                project_digest="d" * 64,
            ),
        )
    assert {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")["count"])
        for table in before
    } == before


@pytest.mark.parametrize(
    ("proxy_catalog_digest", "proxy_abi_version"),
    (("", CAO_CONVERSATION_PROXY_ABI_VERSION), (_CATALOG_DIGEST, 0)),
)
def test_blank_proxy_contract_is_rejected_before_cab_mutation(
    system: dict[str, Any],
    proxy_catalog_digest: str,
    proxy_abi_version: int,
) -> None:
    service = system["service"]
    before = {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")["count"])
        for table in ("cao_attachment_bootstrap_credentials", "events")
    }

    with pytest.raises(PydanticValidationError):
        CAOSessionAttachment(
            native_thread_id="blank-proxy-contract",
            project_digest=_PROJECT_DIGEST,
            proxy_catalog_digest=proxy_catalog_digest,
            proxy_abi_version=proxy_abi_version,
        )
    with pytest.raises(AuthenticationError):
        service.issue_owner_local_attachment_bootstrap(
            _identity(),
            "blank-proxy-contract",
            _PROJECT_DIGEST,
            proxy_catalog_digest,
            proxy_abi_version,
        )

    assert {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")["count"])
        for table in before
    } == before


def test_stale_nonempty_proxy_contract_is_rejected_before_cab_mutation(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    current = service.db.fetchone(
        "SELECT value FROM metadata WHERE key = 'conversation_mcp_catalog_digest'"
    )
    assert current is not None and str(current["value"]) != _CATALOG_DIGEST
    before = {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")["count"])
        for table in ("cao_attachment_bootstrap_credentials", "events")
    }

    with pytest.raises(AttachmentCatalogRefreshRequired):
        service.issue_owner_local_attachment_bootstrap(
            _identity(),
            "stale-proxy-contract",
            _PROJECT_DIGEST,
            _CATALOG_DIGEST,
            CAO_CONVERSATION_PROXY_ABI_VERSION,
        )

    assert {
        table: int(service.db.fetchone(f"SELECT COUNT(*) AS count FROM {table}")["count"])
        for table in before
    } == before


def test_v35_peer_normalization_rejects_ambiguous_legacy_authority() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE cao_session_attachments (
            id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            host_root_pid INTEGER NOT NULL DEFAULT 0,
            host_root_start_signature TEXT NOT NULL DEFAULT '',
            bridge_pid INTEGER NOT NULL DEFAULT 0,
            bridge_start_signature TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE cao_attachment_bootstrap_credentials (
            id TEXT PRIMARY KEY,
            attachment_id TEXT,
            peer_pid INTEGER NOT NULL DEFAULT 0,
            peer_start_signature TEXT NOT NULL DEFAULT '',
            proxy_catalog_digest TEXT NOT NULL DEFAULT '',
            proxy_abi_version INTEGER NOT NULL DEFAULT 0,
            attachment_generation INTEGER,
            host_root_pid INTEGER NOT NULL DEFAULT 0,
            host_root_start_signature TEXT NOT NULL DEFAULT '',
            bridge_pid INTEGER NOT NULL DEFAULT 0,
            bridge_start_signature TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            revoked_at TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE cao_attachment_connections (
            id TEXT PRIMARY KEY,
            attachment_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            peer_pid INTEGER NOT NULL DEFAULT 0,
            peer_start_signature TEXT NOT NULL DEFAULT '',
            proxy_catalog_digest TEXT NOT NULL DEFAULT '',
            proxy_abi_version INTEGER NOT NULL DEFAULT 0,
            host_root_pid INTEGER NOT NULL DEFAULT 0,
            host_root_start_signature TEXT NOT NULL DEFAULT '',
            bridge_pid INTEGER NOT NULL DEFAULT 0,
            bridge_start_signature TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL,
            revoked_at TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX cao_attachment_connections_one_active_bridge
        ON cao_attachment_connections(
            principal_id, host_root_pid, host_root_start_signature,
            bridge_pid, bridge_start_signature
        ) WHERE state = 'active';
        CREATE TABLE cao_conversation_credentials (
            id TEXT PRIMARY KEY,
            connection_id TEXT,
            state TEXT NOT NULL,
            revoked_at TEXT,
            updated_at TEXT NOT NULL
        );
        INSERT INTO cao_session_attachments
        VALUES('cat_legacy', 'cao', 1, 'root-one', 70, 'same-peer');
        INSERT INTO cao_attachment_bootstrap_credentials
        VALUES(
            'cab_legacy', 'cat_legacy', 0, '', '', 0, NULL,
            1, 'root-one', 70, 'same-peer', 'active', NULL, 'old'
        );
        INSERT INTO cao_attachment_connections
        VALUES(
            'cac_one', 'cat_legacy', 'cao', 0, '', '', 0,
            1, 'root-one', 70, 'same-peer', 'active', NULL, 'old'
        );
        INSERT INTO cao_attachment_connections
        VALUES(
            'cac_two', 'cat_legacy', 'cao', 0, '', '', 0,
            2, 'root-two', 70, 'same-peer', 'active', NULL, 'old'
        );
        INSERT INTO cao_attachment_connections
        VALUES(
            'cac_zero', 'cat_legacy', 'cao', 0, '', '', 0,
            0, '', 0, '', 'active', NULL, 'old'
        );
        INSERT INTO cao_conversation_credentials
        VALUES('csc_one', 'cac_one', 'active', NULL, 'old');
        INSERT INTO cao_conversation_credentials
        VALUES('csc_two', 'cac_two', 'active', NULL, 'old');
        INSERT INTO cao_conversation_credentials
        VALUES('csc_zero', 'cac_zero', 'active', NULL, 'old');
        """
    )

    _normalize_cao_attachment_peer_schema_v35(connection, existing_schema_version=34)

    assert (
        connection.execute("SELECT state FROM cao_attachment_bootstrap_credentials").fetchone()[
            "state"
        ]
        == "revoked"
    )
    assert {
        str(row["state"])
        for row in connection.execute("SELECT state FROM cao_attachment_connections")
    } == {"stale"}
    assert {
        str(row["state"])
        for row in connection.execute("SELECT state FROM cao_conversation_credentials")
    } == {"revoked"}
    for table in (
        "cao_session_attachments",
        "cao_attachment_bootstrap_credentials",
        "cao_attachment_connections",
    ):
        columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}
        assert not columns & {
            "host_root_pid",
            "host_root_start_signature",
            "bridge_pid",
            "bridge_start_signature",
        }
    assert {
        str(row["name"])
        for row in connection.execute("PRAGMA index_list(cao_attachment_connections)")
    } >= {"cao_attachment_connections_one_active_peer"}
    assert {
        str(row["name"])
        for row in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'trigger' AND tbl_name = 'cao_attachment_connections'"
        )
    } >= {
        "cao_attachment_connections_active_peer_insert",
        "cao_attachment_connections_active_peer_update",
    }
    assert {
        str(row["name"])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND tbl_name = 'cao_attachment_bootstrap_credentials'"
        )
    } >= {
        "cao_attachment_bootstrap_credentials_active_contract_insert",
        "cao_attachment_bootstrap_credentials_active_contract_update",
    }

    connection.execute(
        """
        INSERT INTO cao_attachment_bootstrap_credentials(
            id, attachment_id, peer_pid, peer_start_signature,
            proxy_catalog_digest, proxy_abi_version,
            attachment_generation, state, revoked_at, updated_at
        ) VALUES('cab_current', 'cat_legacy', 71, 'current-peer', ?, ?, 1,
                 'active', NULL, 'current')
        """,
        (_CATALOG_DIGEST, CAO_CONVERSATION_PROXY_ABI_VERSION),
    )
    _normalize_cao_attachment_peer_schema_v35(connection, existing_schema_version=35)
    assert (
        connection.execute(
            "SELECT state FROM cao_attachment_bootstrap_credentials WHERE id = 'cab_current'"
        ).fetchone()["state"]
        == "active"
    )
    connection.close()


async def _receive(path: Path, **overrides: Any) -> str:
    values: dict[str, Any] = {
        "native_thread_id": "owner-local-thread",
        "project_digest": _PROJECT_DIGEST,
        "proxy_catalog_digest": _CATALOG_DIGEST,
        "proxy_abi_version": CAO_CONVERSATION_PROXY_ABI_VERSION,
        "timeout_seconds": 2,
    }
    values.update(overrides)
    return await receive_attachment_bootstrap(path, **values)


async def _exchange_raw(path: Path, raw: bytes) -> dict[str, object]:
    reader, writer = await __import__("asyncio").open_unix_connection(str(path))
    try:
        writer.write(raw + b"\n")
        await writer.drain()
        response = await reader.readline()
    finally:
        writer.close()
        await writer.wait_closed()
    value = json.loads(response)
    assert isinstance(value, dict)
    return value


def test_attachment_context_requires_exact_loaded_proxy_contract() -> None:
    assert _attachment_context(_context()) == (
        "thread",
        _PROJECT_DIGEST,
        _CATALOG_DIGEST,
        CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    for invalid in (
        _context(proxy_catalog_digest=""),
        _context(proxy_abi_version=0),
        _context(proxy_abi_version=True),
        _context(extra="not-allowed"),
    ):
        with pytest.raises(AttachmentIssuerError, match="context is invalid"):
            _attachment_context(invalid)


def test_owner_peer_capture_does_not_read_path_argv_ancestry_or_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[int] = []
    identity = _identity()

    def capture(pid: int) -> ProcessIdentity:
        observed.append(pid)
        return identity

    monkeypatch.setattr(issuer_module, "_process_identity", capture)
    assert resolve_owner_peer_identity(9123) == identity
    assert observed == [9123]

    source = Path(issuer_module.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "DarwinRunningCodeVerifier",
        "Security.framework",
        "mapped_executable",
        "codesign",
        "subprocess",
        "LaunchAgent",
    ):
        assert forbidden not in source
    assert not (Path(issuer_module.__file__).with_name("darwin_identity.py")).exists()


@pytest.mark.asyncio
async def test_same_uid_peer_issues_exact_contract_without_executable_gate(
    tmp_path: Path,
) -> None:
    identity = _identity()
    issued: list[tuple[object, ...]] = []

    def issue(*values: object) -> str:
        issued.append(values)
        return "cao.cab_owner-peer-contract"

    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        issue,
        peer_identity_provider=lambda _pid: identity,
    )
    await issuer.start()
    try:
        assert stat.S_IMODE(issuer.path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(issuer.path.stat().st_mode) == 0o600
        token = await _receive(issuer.path)
    finally:
        await issuer.close()

    assert token == "cao.cab_owner-peer-contract"
    assert issued == [
        (
            identity,
            "owner-local-thread",
            _PROJECT_DIGEST,
            _CATALOG_DIGEST,
            CAO_CONVERSATION_PROXY_ABI_VERSION,
        )
    ]


@pytest.mark.asyncio
async def test_peer_admission_survives_application_source_path_disappearance(
    tmp_path: Path,
) -> None:
    removed_source = tmp_path / "prior-app-version" / "bridge"
    removed_source.parent.mkdir()
    removed_source.write_text("old app source", encoding="utf-8")
    removed_source.unlink()
    issued = 0

    def issue(
        _peer: ProcessIdentity,
        _thread_id: str,
        _project_digest: str,
        _catalog_digest: str,
        _abi_version: int,
    ) -> str:
        nonlocal issued
        issued += 1
        return "cao.cab_path-independent"

    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        issue,
        peer_identity_provider=lambda _pid: _identity(4402),
    )
    await issuer.start()
    try:
        assert await _receive(issuer.path) == "cao.cab_path-independent"
    finally:
        await issuer.close()
    assert not removed_source.exists()
    assert issued == 1


@pytest.mark.asyncio
async def test_foreign_uid_fails_closed_without_issuing_capability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issued = False

    def issue(*_values: object) -> str:
        nonlocal issued
        issued = True
        return "cao.cab_must-not-issue"

    monkeypatch.setattr(issuer_module, "_peer_uid", lambda _peer: os.geteuid() + 1)
    issuer = AttachmentCapabilityIssuer(tmp_path / "state", issue)
    await issuer.start()
    try:
        with pytest.raises(AttachmentIssuerRemoteError) as stopped:
            await _receive(issuer.path)
    finally:
        await issuer.close()

    assert stopped.value.reason_code == "attachment_peer_unavailable"
    assert stopped.value.retryable is False
    assert issued is False


@pytest.mark.asyncio
async def test_peer_identity_race_retries_once_then_stops_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def unavailable(_peer: socket.socket) -> int:
        nonlocal calls
        calls += 1
        raise EnrollmentCapabilityError("simulated peer exit")

    monkeypatch.setattr(issuer_module, "_peer_pid", unavailable)
    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        lambda *_values: "cao.cab_must-not-issue",
    )
    await issuer.start()
    try:
        with pytest.raises(AttachmentIssuerRemoteError) as stopped:
            await _receive(issuer.path)
    finally:
        await issuer.close()

    assert calls == 2
    assert stopped.value.reason_code == "attachment_peer_unavailable"
    assert stopped.value.retryable is True


@pytest.mark.asyncio
async def test_stale_proxy_abi_returns_catalog_refresh_without_issue(
    tmp_path: Path,
) -> None:
    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        lambda *_values: pytest.fail("stale ABI issued a capability"),
        peer_identity_provider=lambda _pid: _identity(),
    )
    await issuer.start()
    try:
        with pytest.raises(AttachmentIssuerRemoteError) as stopped:
            await _receive(
                issuer.path,
                proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION + 1,
            )
    finally:
        await issuer.close()

    assert stopped.value.reason_code == "attachment_catalog_refresh_required"
    assert stopped.value.retryable is False


@pytest.mark.asyncio
async def test_current_catalog_rejection_is_one_bounded_refresh_result(
    tmp_path: Path,
) -> None:
    def stale(*_values: object) -> str:
        raise AttachmentCatalogRefreshRequired("loaded catalog is stale")

    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        stale,
        peer_identity_provider=lambda _pid: _identity(),
    )
    await issuer.start()
    try:
        with pytest.raises(AttachmentIssuerRemoteError) as stopped:
            await _receive(issuer.path)
    finally:
        await issuer.close()

    assert stopped.value.reason_code == "attachment_catalog_refresh_required"
    assert stopped.value.retryable is False


@pytest.mark.asyncio
async def test_malformed_context_is_rejected_before_capability_issue(tmp_path: Path) -> None:
    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        lambda *_values: pytest.fail("malformed context issued a capability"),
        peer_identity_provider=lambda _pid: _identity(),
    )
    await issuer.start()
    try:
        response = await _exchange_raw(
            issuer.path,
            json.dumps(
                {
                    "native_thread_id": "thread",
                    "project_digest": _PROJECT_DIGEST,
                    "proxy_catalog_digest": "not-a-digest",
                    "proxy_abi_version": CAO_CONVERSATION_PROXY_ABI_VERSION,
                }
            ).encode(),
        )
    finally:
        await issuer.close()

    assert response == {"error": "context_invalid", "retryable": False}


@pytest.mark.asyncio
async def test_existing_non_0600_socket_is_not_reused_or_unlinked(tmp_path: Path) -> None:
    issuer = AttachmentCapabilityIssuer(
        tmp_path / "state",
        lambda *_values: "cao.cab_must-not-issue",
    )
    issuer.path.parent.mkdir(parents=True, mode=0o700)
    os.chmod(issuer.path.parent, 0o700)
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(attachment_issuer_path(tmp_path / "state")))
    os.chmod(issuer.path, 0o660)
    try:
        with pytest.raises(AttachmentIssuerError, match="socket is unsafe"):
            await issuer.start()
        assert issuer.path.exists()
    finally:
        stale.close()
        issuer.path.unlink()


def test_legacy_host_receipt_schema_is_removed_by_v35_migration(settings: Any) -> None:
    database = Database(settings)
    database.initialize()
    with database.connection_scope() as connection:
        connection.execute(
            "CREATE TABLE cao_host_attestation_receipts("
            "id TEXT PRIMARY KEY, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "ALTER TABLE cao_attachment_bootstrap_credentials "
            "ADD COLUMN host_attestation_receipt_id TEXT "
            "REFERENCES cao_host_attestation_receipts(id)"
        )
        connection.execute("UPDATE metadata SET value = '33' WHERE key = 'schema_version'")
        connection.execute("DELETE FROM schema_migrations WHERE version >= 34")
        connection.execute("PRAGMA user_version = 33")

    database.initialize()

    version = database.fetchone("SELECT value FROM metadata WHERE key = 'schema_version'")
    tables = {
        str(row["name"])
        for row in database.fetchall("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    columns = {
        str(row["name"])
        for row in database.fetchall("PRAGMA table_info(cao_attachment_bootstrap_credentials)")
    }
    assert version is not None and version["value"] == str(SCHEMA_VERSION)
    assert "cao_host_attestation_receipts" not in tables
    assert "host_attestation_receipt_id" not in columns
