from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import threading
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from types import TracebackType
from typing import Any, Literal

from .close_contract import (
    ArtifactPreservation,
    CleanupAction,
    CleanupOutcome,
    CleanupRecord,
    CleanupTargetKind,
    ClosePlan,
)
from .config import Settings
from .connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from .errors import AuthorityModeError
from .event_scope import backfill_event_operator_scopes_tx
from .goal_packets import (
    build_goal_packet,
    build_task_packet,
    canonical_digest,
    canonical_json,
    goal_packet_digest,
    task_packet_digest,
)
from .security import canonical_json as security_canonical_json
from .security import contains_control_plane_secret

SCHEMA_VERSION = 45
APPLICATION_ID = 0x43414F32  # "CAO2"


class _ClosingConnection(sqlite3.Connection):
    """Commit or roll back, then close when used as a context manager."""

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        try:
            return super().__exit__(exception_type, exception, traceback)
        finally:
            self.close()


@dataclass(frozen=True, slots=True)
class SQLiteDatabaseIdentity:
    """Migration-free identity and integrity evidence for one CAO database."""

    application_id: int
    user_version: int
    schema_version: int
    integrity: tuple[str, ...]
    foreign_key_error_count: int

    @property
    def ok(self) -> bool:
        return (
            self.application_id == APPLICATION_ID
            and self.user_version == self.schema_version
            and self.integrity == ("ok",)
            and self.foreign_key_error_count == 0
        )


@dataclass(frozen=True, slots=True)
class SQLiteBackupResult:
    """Durable evidence returned by a migration-free SQLite backup."""

    path: Path
    source_identity: SQLiteDatabaseIdentity
    backup_identity: SQLiteDatabaseIdentity
    sha256: str


# These values describe the old dispatcher persistence contract, not a
# redaction vocabulary.  Upgrade code must be value-blind: adapter/model
# output may already contain a prompt, response, path, credential, or another
# opaque conversation-bearing value, so it is never safe to inspect and carry
# forward a subset of it.
_PRE_V18_RUNTIME_EVENT_OUTCOMES = {
    "runtime.message_delivered": "delivered",
    "runtime.message_retry_scheduled": "retry_scheduled",
    "runtime.message_dead": "dead",
    "runtime.message_delivery_unknown": "unknown",
}
_LEGACY_RUNTIME_DIAGNOSTIC_CODE = "legacy_runtime_diagnostic_redacted"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_after(seconds: float) -> str:
    return (
        (datetime.now(UTC) + timedelta(seconds=seconds))
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _service_json_digest_sql(value: object) -> str:
    """Return the service JSON digest for one value used by SQL fences."""

    if not isinstance(value, str):
        return ""
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return ""
    return hashlib.sha256(security_canonical_json(parsed).encode("utf-8")).hexdigest()


def _safe_json_document_sql(document: str) -> str:
    """Return a fail-soft SQL document expression for one static column reference.

    Historical JSON evidence is immutable even when an interrupted legacy
    writer left malformed bytes behind.  Read-side derivations must therefore
    treat that one optional document as unavailable without rewriting it or
    allowing SQLite's ``malformed JSON`` exception to abort unrelated work.

    ``document`` is always a source-owned SQL identifier, never request data.
    Keep the small validation here so a future caller cannot accidentally turn
    this helper into a general SQL string interpolation surface.
    """

    if not document or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_."
        for character in document
    ):
        raise ValueError("safe JSON SQL requires one static column reference")
    return f"(CASE WHEN json_valid({document}) THEN {document} ELSE '{{}}' END)"


def _checked_owner_sqlite_file(path: Path) -> tuple[Path, os.stat_result]:
    candidate = path.expanduser().absolute()
    try:
        info = candidate.lstat()
    except FileNotFoundError as error:
        raise ValueError("SQLite database does not exist") from error
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise ValueError("SQLite database must be an owner-only regular file")
    return candidate.resolve(strict=True), info


def _read_only_sqlite(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def inspect_sqlite_database(path: Path) -> SQLiteDatabaseIdentity:
    """Inspect a CAO SQLite file without creating or migrating it."""

    checked, before = _checked_owner_sqlite_file(path)
    try:
        with closing(_read_only_sqlite(checked)) as connection:
            application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
            user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            row = connection.execute(
                "SELECT value FROM metadata WHERE key = 'schema_version'"
            ).fetchone()
            schema_version = int(row["value"]) if row is not None else -1
            integrity = tuple(str(item[0]) for item in connection.execute("PRAGMA integrity_check"))
            foreign_key_error_count = sum(
                1 for _item in connection.execute("PRAGMA foreign_key_check")
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as error:
        raise ValueError("SQLite database identity is invalid") from error
    after = checked.lstat()
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise ValueError("SQLite database changed identity while it was inspected")
    return SQLiteDatabaseIdentity(
        application_id=application_id,
        user_version=user_version,
        schema_version=schema_version,
        integrity=integrity,
        foreign_key_error_count=foreign_key_error_count,
    )


def backup_sqlite_database(
    source: Path,
    destination: Path,
    *,
    replace: bool = True,
) -> SQLiteBackupResult:
    """Create and verify an atomic backup without initializing the source DB."""

    source_path, source_before = _checked_owner_sqlite_file(source)
    destination_path = destination.expanduser().absolute()
    if destination_path.exists() or destination_path.is_symlink():
        if not replace:
            raise FileExistsError("backup destination already exists")
        destination_info = destination_path.lstat()
        if stat.S_ISLNK(destination_info.st_mode) or not stat.S_ISREG(destination_info.st_mode):
            raise ValueError("backup destination must be a regular file")
    destination_path = destination_path.resolve(strict=False)
    if destination_path == source_path:
        raise ValueError("backup destination must differ from the live database")
    destination_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent_info = destination_path.parent.lstat()
    if (
        stat.S_ISLNK(parent_info.st_mode)
        or not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.getuid()
    ):
        raise ValueError("backup directory must be an owner directory")
    if stat.S_IMODE(parent_info.st_mode) != 0o700:
        raise ValueError("backup directory must be owner-only (0700)")
    source_identity = inspect_sqlite_database(source_path)
    if not source_identity.ok:
        raise ValueError("source database is not a healthy CAO database")

    temporary = destination_path.with_name(f".{destination_path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    try:
        with (
            closing(_read_only_sqlite(source_path)) as source_connection,
            closing(sqlite3.connect(temporary)) as target_connection,
        ):
            source_connection.backup(target_connection)
            target_connection.execute("PRAGMA journal_mode = DELETE")
            target_connection.commit()
        os.chmod(temporary, 0o600)
        backup_identity = inspect_sqlite_database(temporary)
        if not backup_identity.ok or backup_identity != source_identity:
            raise RuntimeError("backup database verification failed")
        digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
        descriptor = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        source_after = source_path.lstat()
        if (source_before.st_dev, source_before.st_ino) != (
            source_after.st_dev,
            source_after.st_ino,
        ):
            raise RuntimeError("source database changed identity during backup")
        if replace:
            os.replace(temporary, destination_path)
        else:
            # Publish without replacing a destination that appeared after the
            # initial check.  Linking the already-synced private inode is the
            # portable atomic no-clobber operation available on every
            # supported platform; the temporary name is removed in ``finally``.
            os.link(temporary, destination_path, follow_symlinks=False)
        os.chmod(destination_path, 0o600)
        directory_descriptor = os.open(destination_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        return SQLiteBackupResult(
            path=destination_path,
            source_identity=source_identity,
            backup_identity=backup_identity,
            sha256=digest,
        )
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_authority (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    mode TEXT NOT NULL CHECK(mode = 'canonical'),
    generation INTEGER NOT NULL,
    activated_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS principals (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    operator_scope TEXT NOT NULL DEFAULT 'unclassified'
        CHECK(operator_scope IN ('production', 'acceptance-test', 'system', 'unclassified')),
    operator_label TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS principals_name_role_unique
    ON principals(name, role);

CREATE TABLE IF NOT EXISTS runtime_sessions (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    adapter TEXT NOT NULL,
    endpoint TEXT NOT NULL DEFAULT '',
    native_session_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runtime_sessions_principal_idx
    ON runtime_sessions(principal_id, state);

-- A managed Worker profile is a typed, immutable server-side launch contract.
-- It deliberately contains no command, concrete workspace path, or credential.
CREATE TABLE IF NOT EXISTS managed_worker_specs (
    id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL REFERENCES cao_session_attachments(id) ON DELETE RESTRICT,
    attachment_generation INTEGER NOT NULL CHECK(attachment_generation >= 0),
    principal_id TEXT NOT NULL UNIQUE REFERENCES principals(id) ON DELETE RESTRICT,
    runtime_session_id TEXT NOT NULL UNIQUE REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
    enrollment_id TEXT NOT NULL UNIQUE REFERENCES worker_enrollments(id) ON DELETE RESTRICT,
    worker_profile_id TEXT NOT NULL,
    adapter TEXT NOT NULL CHECK(adapter IN ('codex-app-server', 'claude')),
    workspace_ref TEXT NOT NULL,
    requested_model TEXT NOT NULL,
    effective_model TEXT NOT NULL,
    requested_reasoning_effort TEXT NOT NULL,
    effective_reasoning_effort TEXT NOT NULL,
    provider_scope_digest TEXT NOT NULL DEFAULT '',
    catalog_target_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK(state IN ('enabled', 'stopped', 'revoked')),
    policy_binding_digest TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    stopped_at TEXT,
    UNIQUE(attachment_id, attachment_generation, input_digest),
    UNIQUE(attachment_id, attachment_generation, idempotency_key)
);
CREATE INDEX IF NOT EXISTS managed_worker_specs_attachment_idx
    ON managed_worker_specs(attachment_id, attachment_generation, state);

-- A managed Worker thread is the stable CAO-owned logical lifecycle.  Its
-- provider session is intentionally stored on an epoch so finish/delete can
-- remain Control-Plane-only operations: no provider-native thread is deleted.
CREATE TABLE IF NOT EXISTS managed_worker_threads (
    id TEXT PRIMARY KEY,
    managed_spec_id TEXT NOT NULL UNIQUE
        REFERENCES managed_worker_specs(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('active', 'archived', 'legacy_stopped')),
    generation INTEGER NOT NULL CHECK(generation >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived_at TEXT
);
CREATE INDEX IF NOT EXISTS managed_worker_threads_state_idx
    ON managed_worker_threads(state, updated_at DESC);

-- Provider throttling is shared across Worker names/workspaces when the
-- adapter, model, and owner-local authentication scope are the same.  Only the
-- opaque scope digest and fixed operational state are durable.
CREATE TABLE IF NOT EXISTS provider_runtime_circuits (
    scope_digest TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('open', 'half_open', 'blocked', 'closed')),
    failure_code TEXT NOT NULL CHECK(failure_code = 'runtime_provider_rate_limited'),
    cooldown_until TEXT NOT NULL,
    restart_authorized INTEGER NOT NULL DEFAULT 0 CHECK(restart_authorized IN (0, 1)),
    source_runtime_session_id TEXT REFERENCES runtime_sessions(id) ON DELETE SET NULL,
    source_boundary_id TEXT REFERENCES boundaries(id) ON DELETE SET NULL,
    source_work_item_id TEXT REFERENCES work_items(id) ON DELETE SET NULL,
    source_boundary_sequence INTEGER NOT NULL DEFAULT 0
        CHECK(source_boundary_sequence >= 0),
    probe_restart_override INTEGER NOT NULL DEFAULT 0
        CHECK(probe_restart_override IN (0, 1)),
    probe_runtime_session_id TEXT REFERENCES runtime_sessions(id) ON DELETE SET NULL,
    probe_attempt_id TEXT REFERENCES attempts(id) ON DELETE SET NULL,
    probe_message_id TEXT NOT NULL DEFAULT '',
    probe_instruction_sequence INTEGER NOT NULL DEFAULT 0
        CHECK(probe_instruction_sequence >= 0),
    probe_outcome_state TEXT NOT NULL DEFAULT 'none'
        CHECK(probe_outcome_state IN ('none', 'active', 'unknown')),
    opened_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_runtime_circuits_state_idx
    ON provider_runtime_circuits(state, cooldown_until);

-- Managed MCP enrollment is a separate credential lifecycle from the
-- principal bootstrap token.  Only verifier hashes are durable; ticket and
-- runtime-credential plaintext must never be stored in this database.
CREATE TABLE IF NOT EXISTS worker_enrollments (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    runtime_session_id TEXT NOT NULL UNIQUE
        REFERENCES runtime_sessions(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK(state IN (
        'awaiting_handshake', 'ready', 'stale', 'revoked', 'failed'
    )),
    generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    managed INTEGER NOT NULL DEFAULT 0 CHECK(managed IN (0, 1)),
    required_tools_digest TEXT NOT NULL DEFAULT '',
    discovered_tools_digest TEXT NOT NULL DEFAULT '',
    protocol_version TEXT NOT NULL DEFAULT '',
    heartbeat_sequence INTEGER NOT NULL DEFAULT 0 CHECK(heartbeat_sequence >= 0),
    discovered_at TEXT,
    heartbeat_at TEXT,
    lease_expires_at TEXT,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, principal_id)
);
-- awaiting_handshake, ready, and stale remain recoverable enrollment states.
-- revoked and failed rows are historical terminal records, so they do not
-- prevent a fresh enrollment for the same Worker principal.
CREATE UNIQUE INDEX IF NOT EXISTS worker_enrollments_one_nonterminal_principal
    ON worker_enrollments(principal_id)
    WHERE state IN ('awaiting_handshake', 'ready', 'stale');
CREATE INDEX IF NOT EXISTS worker_enrollments_principal_state_idx
    ON worker_enrollments(principal_id, state, updated_at DESC);
CREATE INDEX IF NOT EXISTS worker_enrollments_lease_idx
    ON worker_enrollments(state, lease_expires_at);

-- Runtime/enrollment rows are durable history.  A logical resume appends a
-- fresh epoch and repoints the stable managed spec; it never reactivates an
-- older enrollment or credential.
CREATE TABLE IF NOT EXISTS managed_worker_thread_epochs (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL
        REFERENCES managed_worker_threads(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL CHECK(generation >= 1),
    connection_generation INTEGER NOT NULL DEFAULT 1
        CHECK(connection_generation >= 1),
    runtime_session_id TEXT NOT NULL UNIQUE
        REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
    enrollment_id TEXT NOT NULL UNIQUE
        REFERENCES worker_enrollments(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    retired_at TEXT,
    UNIQUE(thread_id, generation, connection_generation)
);
CREATE UNIQUE INDEX IF NOT EXISTS managed_worker_thread_epochs_one_current
    ON managed_worker_thread_epochs(thread_id) WHERE retired_at IS NULL;

CREATE TABLE IF NOT EXISTS runtime_enrollment_tickets (
    id TEXT PRIMARY KEY,
    enrollment_id TEXT NOT NULL REFERENCES worker_enrollments(id) ON DELETE CASCADE,
    attempt_id TEXT REFERENCES attempts(id),
    generation INTEGER NOT NULL CHECK(generation >= 0),
    ticket_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('pending', 'consumed', 'expired', 'revoked')),
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- A generation may issue a replacement ticket after revoking an unconsumed
-- one, so tickets are intentionally not unique on (enrollment, generation).
CREATE INDEX IF NOT EXISTS runtime_enrollment_tickets_enrollment_generation_idx
    ON runtime_enrollment_tickets(enrollment_id, generation);
CREATE INDEX IF NOT EXISTS runtime_enrollment_tickets_state_expiry_idx
    ON runtime_enrollment_tickets(state, expires_at);

CREATE TABLE IF NOT EXISTS runtime_credentials (
    id TEXT PRIMARY KEY,
    enrollment_id TEXT NOT NULL,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL CHECK(generation >= 0),
    token_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('active', 'revoked', 'expired')),
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(enrollment_id, principal_id)
        REFERENCES worker_enrollments(id, principal_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS runtime_credentials_one_active_enrollment
    ON runtime_credentials(enrollment_id)
    WHERE state = 'active';
CREATE INDEX IF NOT EXISTS runtime_credentials_principal_state_expiry_idx
    ON runtime_credentials(principal_id, state, expires_at);

-- The local owner-private placement provider is never copied into SQLite.
-- This table carries only PlacementDecision.as_durable() plus opaque/current
-- control-plane bindings for diagnosis and replay fencing.
CREATE TABLE IF NOT EXISTS owner_private_placement_decisions (
    id TEXT PRIMARY KEY,
    runtime_session_id TEXT NOT NULL REFERENCES runtime_sessions(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    recipient_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    work_item_id TEXT REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT REFERENCES attempts(id) ON DELETE CASCADE,
    delivery_generation INTEGER NOT NULL CHECK(delivery_generation >= 0),
    launch_generation INTEGER NOT NULL CHECK(launch_generation >= 0),
    binding_digest TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    policy_digest TEXT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('allow', 'deny')),
    runner_adapter TEXT NOT NULL CHECK(runner_adapter IN ('claude', 'codex')),
    workspace_identity_digest TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER,
    created_at TEXT NOT NULL,
    UNIQUE(message_id, recipient_id, delivery_generation, runtime_session_id, launch_generation)
);
CREATE INDEX IF NOT EXISTS owner_private_placement_runtime_generation_idx
    ON owner_private_placement_decisions(runtime_session_id, launch_generation);

-- A CAO attachment is intentionally a different authority from a managed
-- Worker enrollment.  It binds an already-existing Codex conversation thread
-- to one exact control-plane runtime.  Like Worker credentials, only ticket
-- and credential verifiers are durable; plaintext never enters SQLite.
CREATE TABLE IF NOT EXISTS cao_session_attachments (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    runtime_session_id TEXT NOT NULL UNIQUE
        REFERENCES runtime_sessions(id) ON DELETE CASCADE,
    native_thread_id TEXT NOT NULL,
    project_digest TEXT NOT NULL,
    project_scope_digest TEXT NOT NULL DEFAULT '',
    project_identity_version INTEGER NOT NULL DEFAULT 1 CHECK(project_identity_version IN (1, 2)),
    model TEXT NOT NULL,
    sandbox TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active', 'stale', 'revoked', 'failed')),
    generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    lease_expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, principal_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS cao_session_attachments_one_active_thread
    ON cao_session_attachments(principal_id, native_thread_id)
    WHERE state = 'active';
CREATE INDEX IF NOT EXISTS cao_session_attachments_runtime_state_idx
    ON cao_session_attachments(runtime_session_id, state, updated_at DESC);

-- The durable conversation attachment owns requester authority.  Every MCP
-- stdio bridge is instead a replaceable, independently leased connection to
-- that attachment.  Multiple authentic bridges may be active concurrently;
-- transport/catalog failure therefore fences only the affected connection.
CREATE TABLE IF NOT EXISTS cao_attachment_connections (
    id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL CHECK(generation >= 0),
    connection_generation INTEGER NOT NULL CHECK(connection_generation >= 1),
    peer_pid INTEGER NOT NULL,
    peer_start_signature TEXT NOT NULL,
    proxy_catalog_digest TEXT NOT NULL,
    proxy_abi_version INTEGER NOT NULL CHECK(proxy_abi_version >= 0),
    state TEXT NOT NULL CHECK(state IN ('active', 'stale', 'revoked', 'expired', 'failed')),
    lease_expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, attachment_id, principal_id, generation),
    UNIQUE(attachment_id, connection_generation),
    FOREIGN KEY(attachment_id, principal_id)
        REFERENCES cao_session_attachments(id, principal_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS cao_attachment_connections_attachment_state_expiry_idx
    ON cao_attachment_connections(attachment_id, state, lease_expires_at);

CREATE TABLE IF NOT EXISTS cao_runtime_tickets (
    id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL REFERENCES cao_session_attachments(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL CHECK(generation >= 0),
    ticket_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('pending', 'consumed', 'expired', 'revoked')),
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cao_runtime_tickets_attachment_generation_idx
    ON cao_runtime_tickets(attachment_id, generation);
CREATE INDEX IF NOT EXISTS cao_runtime_tickets_state_expiry_idx
    ON cao_runtime_tickets(state, expires_at);

CREATE TABLE IF NOT EXISTS cao_runtime_credentials (
    id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL CHECK(generation >= 0),
    token_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('active', 'revoked', 'expired')),
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(attachment_id, principal_id)
        REFERENCES cao_session_attachments(id, principal_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS cao_runtime_credentials_one_active_attachment
    ON cao_runtime_credentials(attachment_id)
    WHERE state = 'active';
CREATE INDEX IF NOT EXISTS cao_runtime_credentials_principal_state_expiry_idx
    ON cao_runtime_credentials(principal_id, state, expires_at);

-- A conversation client credential binds the already-running MCP stdio proxy
-- to one immutable CAO attachment.  It is separate from the short-lived
-- resume-runtime credential so waking the same conversation cannot revoke the
-- original operator connection.  Only a verifier is durable.
CREATE TABLE IF NOT EXISTS cao_conversation_credentials (
    id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL,
    connection_id TEXT REFERENCES cao_attachment_connections(id) ON DELETE CASCADE,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL CHECK(generation >= 0),
    token_hash TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('active', 'revoked', 'expired')),
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(attachment_id, principal_id)
        REFERENCES cao_session_attachments(id, principal_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS cao_conversation_credentials_attachment_state_expiry_idx
    ON cao_conversation_credentials(attachment_id, state, expires_at);

-- This is a deliberately narrow bootstrap capability for the MCP stdio
-- bridge.  Its first successful use binds it to exactly one attachment; it
-- is not a CAO principal bearer and cannot enter generic control-plane APIs.
CREATE TABLE IF NOT EXISTS cao_attachment_bootstrap_credentials (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    attachment_id TEXT REFERENCES cao_session_attachments(id) ON DELETE RESTRICT,
    attachment_generation INTEGER CHECK(attachment_generation >= 0),
    token_hash TEXT NOT NULL UNIQUE,
    one_time INTEGER NOT NULL DEFAULT 0 CHECK(one_time IN (0, 1)),
    peer_pid INTEGER NOT NULL,
    peer_start_signature TEXT NOT NULL,
    native_thread_id TEXT NOT NULL DEFAULT '',
    project_digest TEXT NOT NULL DEFAULT '',
    proxy_catalog_digest TEXT NOT NULL,
    proxy_abi_version INTEGER NOT NULL CHECK(proxy_abi_version >= 0),
    state TEXT NOT NULL CHECK(state IN ('active', 'revoked', 'expired')),
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cao_attachment_bootstrap_credentials_principal_idx
    ON cao_attachment_bootstrap_credentials(principal_id, state, expires_at);

CREATE TABLE IF NOT EXISTS work_items (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    goal_version INTEGER NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL,
    created_by TEXT NOT NULL REFERENCES principals(id),
    requester_id TEXT REFERENCES principals(id),
    supervisor_id TEXT REFERENCES principals(id),
    supervisor_attachment_id TEXT REFERENCES cao_session_attachments(id),
    assigned_worker_id TEXT NOT NULL REFERENCES principals(id),
    managed_worker_thread_id TEXT,
    managed_worker_thread_generation INTEGER
        CHECK(managed_worker_thread_generation >= 1),
    user_needed_boundary_id TEXT
        REFERENCES boundaries(id) ON DELETE RESTRICT,
    paused_boundary_id TEXT
        REFERENCES boundaries(id) ON DELETE RESTRICT,
    operator_scope TEXT NOT NULL DEFAULT 'unclassified'
        CHECK(operator_scope IN ('production', 'acceptance-test', 'system', 'unclassified')),
    attention_owner TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    suspended_by_work_item_id TEXT REFERENCES work_items(id),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(id, goal_version)
        REFERENCES goal_revisions(work_item_id, version)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS work_items_worker_state_idx
    ON work_items(assigned_worker_id, state, priority DESC);
CREATE INDEX IF NOT EXISTS work_items_attention_idx
    ON work_items(attention_owner, state);

CREATE TABLE IF NOT EXISTS source_receipts (
    id TEXT PRIMARY KEY,
    source_principal_id TEXT NOT NULL REFERENCES principals(id),
    source_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE(source_principal_id, source_id)
);

CREATE TABLE IF NOT EXISTS submitted_intents (
    id TEXT PRIMARY KEY,
    source_receipt_id TEXT NOT NULL REFERENCES source_receipts(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    submitter_id TEXT NOT NULL REFERENCES principals(id),
    content_json TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(source_receipt_id, ordinal)
);

CREATE TABLE IF NOT EXISTS intent_dispositions (
    id TEXT PRIMARY KEY,
    submitted_intent_id TEXT NOT NULL UNIQUE
        REFERENCES submitted_intents(id) ON DELETE CASCADE,
    source_receipt_id TEXT NOT NULL REFERENCES source_receipts(id) ON DELETE CASCADE,
    decided_by TEXT NOT NULL REFERENCES principals(id),
    kind TEXT NOT NULL,
    relation TEXT,
    target_work_item_id TEXT REFERENCES work_items(id),
    result_work_item_id TEXT REFERENCES work_items(id),
    result_directive_id TEXT REFERENCES directives(id),
    reason TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS goal_revisions (
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    maturity TEXT NOT NULL,
    acceptance_json TEXT NOT NULL,
    non_goals_json TEXT NOT NULL,
    priority INTEGER NOT NULL,
    requester_id TEXT REFERENCES principals(id),
    supervisor_id TEXT REFERENCES principals(id),
    metadata_json TEXT NOT NULL,
    packet_json TEXT NOT NULL,
    packet_digest TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL REFERENCES principals(id),
    source_intent_id TEXT REFERENCES submitted_intents(id),
    source_directive_id TEXT,
    correlation_id TEXT NOT NULL DEFAULT '',
    prior_version INTEGER,
    supervisor_attachment_generation INTEGER CHECK(supervisor_attachment_generation >= 0),
    created_at TEXT NOT NULL,
    supervisor_runtime_session_id TEXT,
    PRIMARY KEY(work_item_id, version)
);

CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL REFERENCES principals(id),
    runtime_session_id TEXT REFERENCES runtime_sessions(id) ON DELETE SET NULL,
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    trajectory TEXT NOT NULL,
    evidence_confidence TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT '',
    next_boundary TEXT NOT NULL DEFAULT '',
    completion_claim_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(work_item_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS attempts_worker_state_idx
    ON attempts(worker_id, state, updated_at DESC);

CREATE TABLE IF NOT EXISTS directives (
    id TEXT PRIMARY KEY,
    submitted_intent_id TEXT NOT NULL UNIQUE
        REFERENCES submitted_intents(id) ON DELETE CASCADE,
    source_receipt_id TEXT NOT NULL REFERENCES source_receipts(id) ON DELETE CASCADE,
    issuer_id TEXT NOT NULL REFERENCES principals(id),
    target_work_item_id TEXT REFERENCES work_items(id),
    created_work_item_id TEXT REFERENCES work_items(id),
    relation TEXT NOT NULL,
    expected_goal_version INTEGER,
    expected_goal_packet_digest TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    content TEXT NOT NULL,
    reason TEXT NOT NULL,
    handled_by TEXT REFERENCES principals(id),
    handled_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS directives_work_state_idx
    ON directives(target_work_item_id, state, created_at);
CREATE INDEX IF NOT EXISTS directives_created_work_idx
    ON directives(created_work_item_id, relation);

CREATE TABLE IF NOT EXISTS messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    work_item_id TEXT REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT REFERENCES attempts(id) ON DELETE CASCADE,
    sender_id TEXT NOT NULL REFERENCES principals(id),
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    message_digest TEXT NOT NULL,
    correlation_id TEXT NOT NULL DEFAULT '',
    causation_id TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    goal_version INTEGER,
    goal_packet_digest TEXT NOT NULL DEFAULT '',
    task_packet_digest TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS messages_sender_idempotency_unique
    ON messages(sender_id, idempotency_key)
    WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS messages_work_sequence_idx
    ON messages(work_item_id, sequence);

CREATE TABLE IF NOT EXISTS message_deliveries (
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    recipient_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    recipient_attachment_id TEXT REFERENCES cao_session_attachments(id) ON DELETE RESTRICT,
    state TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    runtime_session_id TEXT REFERENCES runtime_sessions(id) ON DELETE SET NULL,
    lease_until TEXT,
    owner_token TEXT NOT NULL DEFAULT '',
    delivered_at TEXT,
    acknowledged_at TEXT,
    handled_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    reactivation_policy TEXT NOT NULL DEFAULT 'terminal'
        CHECK(reactivation_policy IN ('terminal', 'retryable')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(message_id, recipient_id)
);
CREATE INDEX IF NOT EXISTS message_deliveries_recipient_state_idx
    ON message_deliveries(recipient_id, state, updated_at);

-- Provider events are independent of model-authored cao_report calls. Their
-- text lives only in the owner-private content store, never in diagnostics,
-- wake prompts, events, or Dashboard projections. One receipt and its outbox
-- notification commit together; duplicates cannot create another wake.
CREATE TABLE IF NOT EXISTS worker_output_streams (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    runtime_session_id TEXT NOT NULL REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
    source_message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE RESTRICT,
    delivery_generation INTEGER NOT NULL,
    enrollment_generation INTEGER NOT NULL,
    work_generation INTEGER NOT NULL,
    owner_token_digest TEXT NOT NULL,
    source_thread_digest TEXT NOT NULL DEFAULT '',
    source_turn_digest TEXT NOT NULL DEFAULT '',
    terminal_output_id TEXT NOT NULL DEFAULT '',
    settled_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(source_message_id, delivery_generation)
);
CREATE TABLE IF NOT EXISTS worker_output_receipts (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    stream_id TEXT NOT NULL REFERENCES worker_output_streams(id) ON DELETE CASCADE,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    runtime_session_id TEXT NOT NULL REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
    source_message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE RESTRICT,
    delivery_generation INTEGER NOT NULL,
    enrollment_generation INTEGER NOT NULL,
    work_generation INTEGER NOT NULL,
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    source_thread_digest TEXT NOT NULL,
    source_turn_digest TEXT NOT NULL,
    source_item_digest TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK(event_kind IN ('message', 'turn_end')),
    phase TEXT NOT NULL CHECK(phase IN ('commentary', 'final', 'unspecified')),
    turn_status TEXT NOT NULL CHECK(turn_status IN ('running', 'completed', 'failed', 'interrupted')),
    capture_state TEXT NOT NULL CHECK(capture_state IN ('available', 'empty', 'partial', 'withheld', 'unavailable')),
    content_digest TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK(byte_count >= 0),
    complete INTEGER NOT NULL CHECK(complete IN (0, 1)),
    event_digest TEXT NOT NULL,
    artifact_manifest_json TEXT NOT NULL DEFAULT '[]',
    notification_message_id TEXT REFERENCES messages(id) ON DELETE RESTRICT,
    boundary_id TEXT REFERENCES boundaries(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_output_attempt_idx
    ON worker_output_receipts(attempt_id, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS worker_output_terminal_delivery_idx
    ON worker_output_receipts(source_message_id, delivery_generation)
    WHERE event_kind = 'turn_end';

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    producer_id TEXT NOT NULL REFERENCES principals(id),
    name TEXT NOT NULL,
    uri TEXT NOT NULL,
    media_type TEXT NOT NULL,
    digest TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS artifacts_work_idx ON artifacts(work_item_id, created_at);

CREATE TABLE IF NOT EXISTS reviews (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    boundary_id TEXT REFERENCES boundaries(id) ON DELETE SET NULL,
    reviewer_id TEXT NOT NULL REFERENCES principals(id),
    reviewer_role TEXT NOT NULL,
    supervisor_attachment_id TEXT REFERENCES cao_session_attachments(id),
    supervisor_attachment_generation INTEGER,
    work_generation INTEGER NOT NULL DEFAULT 1,
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    verdict TEXT NOT NULL,
    summary TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reviews_work_idx ON reviews(work_item_id, created_at);

CREATE TABLE IF NOT EXISTS requester_decisions (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    review_id TEXT NOT NULL UNIQUE REFERENCES reviews(id) ON DELETE CASCADE,
    requester_id TEXT NOT NULL REFERENCES principals(id),
    recorded_by TEXT NOT NULL REFERENCES principals(id),
    supervisor_attachment_id TEXT NOT NULL REFERENCES cao_session_attachments(id),
    supervisor_attachment_generation INTEGER NOT NULL CHECK(supervisor_attachment_generation >= 0),
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    work_generation INTEGER NOT NULL CHECK(work_generation >= 1),
    verdict TEXT NOT NULL CHECK(verdict IN ('accepted', 'rejected')),
    summary TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    conversation_evidence_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS requester_decisions_work_idx
    ON requester_decisions(work_item_id, created_at);

CREATE TABLE IF NOT EXISTS work_close_receipts (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL UNIQUE REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    review_id TEXT NOT NULL REFERENCES reviews(id) ON DELETE CASCADE,
    requester_decision_id TEXT NOT NULL UNIQUE REFERENCES requester_decisions(id) ON DELETE CASCADE,
    closed_by TEXT NOT NULL REFERENCES principals(id),
    supervisor_attachment_id TEXT NOT NULL REFERENCES cao_session_attachments(id),
    supervisor_attachment_generation INTEGER NOT NULL CHECK(supervisor_attachment_generation >= 0),
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    work_generation INTEGER NOT NULL CHECK(work_generation >= 1),
    plan_digest TEXT NOT NULL,
    retention_policy_evidence_id TEXT NOT NULL,
    artifact_manifest_evidence_id TEXT NOT NULL,
    cleanup_inventory_evidence_id TEXT NOT NULL,
    close_preparation_id TEXT REFERENCES work_close_preparations(id),
    artifacts_json TEXT NOT NULL,
    cleanup_json TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS work_close_receipts_created_idx
    ON work_close_receipts(created_at);

-- A close preparation is the server-created, generation-fenced inventory of
-- resources that a WorkItem owns.  The opaque target fingerprints are derived
-- using an owner-local key; raw runtime/workspace locators never enter the
-- receipt or an operator projection.
CREATE TABLE IF NOT EXISTS work_close_preparations (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    final_attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    work_generation INTEGER NOT NULL CHECK(work_generation >= 1),
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    supervisor_attachment_id TEXT NOT NULL REFERENCES cao_session_attachments(id),
    supervisor_attachment_generation INTEGER NOT NULL CHECK(supervisor_attachment_generation >= 0),
    retention_policy_evidence_id TEXT NOT NULL,
    artifact_manifest_evidence_id TEXT NOT NULL,
    artifact_manifest_scope TEXT NOT NULL DEFAULT 'work_history_v1'
        CHECK(artifact_manifest_scope IN ('work_history_v1', 'final_completion_claim_v2')),
    artifacts_json TEXT NOT NULL,
    artifact_preservations_json TEXT NOT NULL DEFAULT '[]',
    artifact_preservations_digest TEXT NOT NULL DEFAULT '',
    inventory_json TEXT NOT NULL,
    inventory_digest TEXT NOT NULL,
    cleanup_execution_request_digest TEXT NOT NULL DEFAULT '',
    cleanup_execution_result_json TEXT NOT NULL DEFAULT '{}',
    cleanup_executed_at TEXT,
    created_by TEXT NOT NULL REFERENCES principals(id),
    created_at TEXT NOT NULL,
    UNIQUE(work_item_id, work_generation)
);
CREATE INDEX IF NOT EXISTS work_close_preparations_work_idx
    ON work_close_preparations(work_item_id, work_generation);

-- SQLite foreign keys protect each referenced row, but a close record also
-- spans several rows.  Keep that composite identity in the database boundary:
-- an application bug (or a second writer) must not be able to manufacture a
-- decision/receipt by combining individually valid rows from different work.
CREATE TRIGGER IF NOT EXISTS requester_decisions_exact_binding_insert
BEFORE INSERT ON requester_decisions
FOR EACH ROW BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM work_items w
        JOIN attempts a ON a.id = NEW.attempt_id AND a.work_item_id = w.id
        JOIN reviews r ON r.id = NEW.review_id
                       AND r.work_item_id = w.id
                       AND r.attempt_id = a.id
        JOIN cao_session_attachments attachment
          ON attachment.id = NEW.supervisor_attachment_id
        WHERE w.id = NEW.work_item_id
          AND w.requester_id = NEW.requester_id
          AND w.supervisor_id = NEW.recorded_by
          AND w.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND w.generation = NEW.work_generation
          AND attachment.principal_id = NEW.recorded_by
          AND attachment.generation = NEW.supervisor_attachment_generation
          AND r.reviewer_role = 'cao'
          AND r.verdict = 'ok'
          AND r.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND r.supervisor_attachment_generation <= NEW.supervisor_attachment_generation
          AND r.work_generation <= NEW.work_generation
          AND a.goal_version = NEW.goal_version
          AND a.goal_packet_digest = NEW.goal_packet_digest
          AND a.task_packet_digest = NEW.task_packet_digest
          AND r.goal_version = NEW.goal_version
          AND r.goal_packet_digest = NEW.goal_packet_digest
          AND r.task_packet_digest = NEW.task_packet_digest
    ) THEN RAISE(ABORT, 'requester decision exact binding violation') END;
END;

CREATE TRIGGER IF NOT EXISTS requester_decisions_exact_binding_update
BEFORE UPDATE ON requester_decisions
FOR EACH ROW BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM work_items w
        JOIN attempts a ON a.id = NEW.attempt_id AND a.work_item_id = w.id
        JOIN reviews r ON r.id = NEW.review_id
                       AND r.work_item_id = w.id
                       AND r.attempt_id = a.id
        JOIN cao_session_attachments attachment
          ON attachment.id = NEW.supervisor_attachment_id
        WHERE w.id = NEW.work_item_id
          AND w.requester_id = NEW.requester_id
          AND w.supervisor_id = NEW.recorded_by
          AND w.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND w.generation = NEW.work_generation
          AND attachment.principal_id = NEW.recorded_by
          AND attachment.generation = NEW.supervisor_attachment_generation
          AND r.reviewer_role = 'cao'
          AND r.verdict = 'ok'
          AND r.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND r.supervisor_attachment_generation <= NEW.supervisor_attachment_generation
          AND r.work_generation <= NEW.work_generation
          AND a.goal_version = NEW.goal_version
          AND a.goal_packet_digest = NEW.goal_packet_digest
          AND a.task_packet_digest = NEW.task_packet_digest
          AND r.goal_version = NEW.goal_version
          AND r.goal_packet_digest = NEW.goal_packet_digest
          AND r.task_packet_digest = NEW.task_packet_digest
    ) THEN RAISE(ABORT, 'requester decision exact binding violation') END;
END;

CREATE TRIGGER IF NOT EXISTS work_close_receipts_exact_binding_insert
BEFORE INSERT ON work_close_receipts
FOR EACH ROW BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM work_items w
        JOIN attempts a ON a.id = NEW.attempt_id AND a.work_item_id = w.id
        JOIN reviews r ON r.id = NEW.review_id
                       AND r.work_item_id = w.id
                       AND r.attempt_id = a.id
        JOIN requester_decisions d ON d.id = NEW.requester_decision_id
                                  AND d.work_item_id = w.id
                                  AND d.attempt_id = a.id
                                  AND d.review_id = r.id
        JOIN cao_session_attachments attachment
          ON attachment.id = NEW.supervisor_attachment_id
        WHERE w.id = NEW.work_item_id
          AND w.state = 'completed'
          AND NEW.cleanup_inventory_evidence_id <> ''
          AND w.supervisor_id = NEW.closed_by
          AND w.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND w.generation = NEW.work_generation
          AND attachment.principal_id = NEW.closed_by
          AND attachment.generation >= NEW.supervisor_attachment_generation
          AND d.verdict = 'accepted'
          AND d.recorded_by = NEW.closed_by
          AND d.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND d.supervisor_attachment_generation <= NEW.supervisor_attachment_generation
          AND d.work_generation = NEW.work_generation
          AND a.goal_version = NEW.goal_version
          AND a.goal_packet_digest = NEW.goal_packet_digest
          AND a.task_packet_digest = NEW.task_packet_digest
          AND r.goal_version = NEW.goal_version
          AND r.goal_packet_digest = NEW.goal_packet_digest
          AND r.task_packet_digest = NEW.task_packet_digest
          AND d.goal_version = NEW.goal_version
          AND d.goal_packet_digest = NEW.goal_packet_digest
          AND d.task_packet_digest = NEW.task_packet_digest
    ) THEN RAISE(ABORT, 'work close receipt exact binding violation') END;
END;

CREATE TRIGGER IF NOT EXISTS work_close_receipts_exact_binding_update
BEFORE UPDATE ON work_close_receipts
FOR EACH ROW BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM work_items w
        JOIN attempts a ON a.id = NEW.attempt_id AND a.work_item_id = w.id
        JOIN reviews r ON r.id = NEW.review_id
                       AND r.work_item_id = w.id
                       AND r.attempt_id = a.id
        JOIN requester_decisions d ON d.id = NEW.requester_decision_id
                                  AND d.work_item_id = w.id
                                  AND d.attempt_id = a.id
                                  AND d.review_id = r.id
        JOIN cao_session_attachments attachment
          ON attachment.id = NEW.supervisor_attachment_id
        WHERE w.id = NEW.work_item_id
          AND w.state = 'completed'
          AND NEW.cleanup_inventory_evidence_id <> ''
          AND w.supervisor_id = NEW.closed_by
          AND w.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND w.generation = NEW.work_generation
          AND attachment.principal_id = NEW.closed_by
          AND attachment.generation >= NEW.supervisor_attachment_generation
          AND d.verdict = 'accepted'
          AND d.recorded_by = NEW.closed_by
          AND d.supervisor_attachment_id = NEW.supervisor_attachment_id
          AND d.supervisor_attachment_generation <= NEW.supervisor_attachment_generation
          AND d.work_generation = NEW.work_generation
          AND a.goal_version = NEW.goal_version
          AND a.goal_packet_digest = NEW.goal_packet_digest
          AND a.task_packet_digest = NEW.task_packet_digest
          AND r.goal_version = NEW.goal_version
          AND r.goal_packet_digest = NEW.goal_packet_digest
          AND r.task_packet_digest = NEW.task_packet_digest
          AND d.goal_version = NEW.goal_version
          AND d.goal_packet_digest = NEW.goal_packet_digest
          AND d.task_packet_digest = NEW.task_packet_digest
    ) THEN RAISE(ABORT, 'work close receipt exact binding violation') END;
END;

CREATE TABLE IF NOT EXISTS reasoner_turns (
    id TEXT PRIMARY KEY,
    supervisor_id TEXT NOT NULL REFERENCES principals(id),
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    boundary_id TEXT NOT NULL REFERENCES boundaries(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    goal_version INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    result_digest TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    lease_token_digest TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    idempotency_key TEXT NOT NULL DEFAULT '',
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS reasoner_turns_subject_leased_unique
    ON reasoner_turns(supervisor_id, work_item_id) WHERE state = 'leased';
CREATE UNIQUE INDEX IF NOT EXISTS reasoner_turns_boundary_leased_unique
    ON reasoner_turns(boundary_id) WHERE state = 'leased';
CREATE UNIQUE INDEX IF NOT EXISTS reasoner_turns_idempotency_unique
    ON reasoner_turns(supervisor_id, idempotency_key)
    WHERE idempotency_key <> '';

CREATE TABLE IF NOT EXISTS boundaries (
    id TEXT PRIMARY KEY,
    source_principal_id TEXT NOT NULL REFERENCES principals(id),
    source_event_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    goal_version INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    goal_packet_digest TEXT NOT NULL,
    task_packet_digest TEXT NOT NULL,
    kind TEXT NOT NULL,
    summary TEXT NOT NULL,
    runtime_state TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    recovery_action TEXT NOT NULL DEFAULT '',
    recovery_target TEXT NOT NULL DEFAULT '',
    recovery_model TEXT NOT NULL DEFAULT '',
    recovery_reasoning_effort TEXT NOT NULL DEFAULT '',
    input_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(source_principal_id, source_event_id)
);
CREATE INDEX IF NOT EXISTS boundaries_work_created_idx
    ON boundaries(work_item_id, created_at);

CREATE TABLE IF NOT EXISTS boundary_dispositions (
    id TEXT PRIMARY KEY,
    boundary_id TEXT NOT NULL UNIQUE REFERENCES boundaries(id) ON DELETE CASCADE,
    reasoner_turn_id TEXT NOT NULL REFERENCES reasoner_turns(id),
    decided_by TEXT NOT NULL REFERENCES principals(id),
    generation INTEGER NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    instruction TEXT NOT NULL DEFAULT '',
    resume_condition TEXT NOT NULL DEFAULT '',
    request_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS work_pauses (
    boundary_id TEXT NOT NULL PRIMARY KEY REFERENCES boundaries(id) ON DELETE RESTRICT,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE RESTRICT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    source_generation INTEGER NOT NULL CHECK(typeof(source_generation) = 'integer' AND source_generation >= 1),
    pause_generation INTEGER NOT NULL CHECK(typeof(pause_generation) = 'integer' AND pause_generation = source_generation + 1),
    paused_by TEXT NOT NULL REFERENCES principals(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    UNIQUE(work_item_id, pause_generation)
);

CREATE TABLE IF NOT EXISTS work_pause_resumptions (
    boundary_id TEXT NOT NULL PRIMARY KEY REFERENCES work_pauses(boundary_id) ON DELETE RESTRICT,
    work_item_id TEXT NOT NULL REFERENCES work_items(id) ON DELETE RESTRICT,
    previous_attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    successor_attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(id) ON DELETE RESTRICT,
    expected_generation INTEGER NOT NULL CHECK(typeof(expected_generation) = 'integer' AND expected_generation >= 1),
    successor_generation INTEGER NOT NULL CHECK(typeof(successor_generation) = 'integer' AND successor_generation = expected_generation + 1),
    message_id TEXT NOT NULL UNIQUE REFERENCES messages(id) ON DELETE RESTRICT,
    resumed_by TEXT NOT NULL REFERENCES principals(id) ON DELETE RESTRICT,
    reason TEXT NOT NULL CHECK(length(trim(reason)) > 0),
    instruction TEXT NOT NULL CHECK(length(trim(instruction)) > 0),
    resume_evidence TEXT NOT NULL CHECK(length(trim(resume_evidence)) > 0),
    created_at TEXT NOT NULL
);

-- A WAIT_USER disposition says what input is required.  The Work pointer says
-- which exact disposition is current, while this append-only relation proves
-- which exact requester continuation consumed it.  Neither authority depends
-- on optional JSON payloads, events, or a live Worker runtime.
CREATE TABLE IF NOT EXISTS boundary_continuations (
    boundary_id TEXT PRIMARY KEY
        REFERENCES boundaries(id) ON DELETE RESTRICT,
    work_item_id TEXT NOT NULL
        REFERENCES work_items(id) ON DELETE RESTRICT,
    source_attempt_id TEXT NOT NULL
        REFERENCES attempts(id) ON DELETE RESTRICT,
    source_generation INTEGER NOT NULL CHECK(source_generation >= 1),
    successor_attempt_id TEXT NOT NULL
        REFERENCES attempts(id) ON DELETE RESTRICT,
    successor_generation INTEGER NOT NULL CHECK(successor_generation >= 1),
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE RESTRICT,
    decided_by TEXT NOT NULL REFERENCES principals(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    operator_scope TEXT NOT NULL DEFAULT 'unclassified'
        CHECK(operator_scope IN ('production', 'acceptance-test', 'system', 'unclassified')),
    actor_id TEXT NOT NULL DEFAULT '',
    data_json TEXT NOT NULL,
    correlation_id TEXT NOT NULL DEFAULT '',
    causation_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_aggregate_idx
    ON events(aggregate_type, aggregate_id, sequence);
CREATE INDEX IF NOT EXISTS events_created_idx
    ON events(created_at, sequence);

-- A Work cancellation supersedes every still-open Boundary on that Work, but
-- it is not a CAO review disposition.  Keep the distinction durable: callers
-- can no longer act on the Boundary, while the exact later cancellation event
-- remains the authority and audit source for the supersession.
CREATE TABLE IF NOT EXISTS boundary_supersessions (
    boundary_id TEXT PRIMARY KEY REFERENCES boundaries(id) ON DELETE CASCADE,
    boundary_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
    superseding_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
    reason TEXT NOT NULL CHECK(
        reason IN (
          'work_canceled', 'recovery_boundary_replaced', 'goal_replaced'
        )
    ),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS boundary_supersessions_event_idx
    ON boundary_supersessions(superseding_event_sequence);
CREATE INDEX IF NOT EXISTS boundary_supersessions_boundary_event_idx
    ON boundary_supersessions(boundary_event_sequence);

CREATE TRIGGER IF NOT EXISTS boundary_supersessions_exact_binding_insert
BEFORE INSERT ON boundary_supersessions
FOR EACH ROW BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM boundaries AS boundary
        JOIN work_items AS work ON work.id = boundary.work_item_id
        JOIN events AS boundary_event
          ON boundary_event.sequence = NEW.boundary_event_sequence
         AND boundary_event.event_type = 'boundary.recorded'
         AND boundary_event.aggregate_type = 'work_item'
         AND boundary_event.aggregate_id = boundary.work_item_id
         AND json_extract(boundary_event.data_json, '$.boundary_id') = boundary.id
        JOIN events AS superseding_event
          ON superseding_event.sequence = NEW.superseding_event_sequence
         AND superseding_event.event_type = 'work.canceled'
         AND superseding_event.aggregate_type = 'work_item'
         AND superseding_event.aggregate_id = boundary.work_item_id
         AND superseding_event.sequence > boundary_event.sequence
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        WHERE boundary.id = NEW.boundary_id
          AND work.state = 'canceled'
          AND boundary.generation < work.generation
          AND disposition.id IS NULL
          AND NEW.reason = 'work_canceled'
          AND (
              SELECT COUNT(*) FROM events AS exact_boundary_event
              WHERE exact_boundary_event.event_type = 'boundary.recorded'
                AND exact_boundary_event.aggregate_type = 'work_item'
                AND exact_boundary_event.aggregate_id = boundary.work_item_id
                AND json_extract(
                      exact_boundary_event.data_json, '$.boundary_id'
                    ) = boundary.id
          ) = 1
    ) THEN RAISE(ABORT, 'boundary supersession exact binding violation') END;
END;

CREATE TRIGGER IF NOT EXISTS boundary_supersessions_immutable_update
BEFORE UPDATE ON boundary_supersessions
FOR EACH ROW BEGIN
    SELECT RAISE(ABORT, 'boundary supersession is immutable');
END;

CREATE TRIGGER IF NOT EXISTS boundary_dispositions_reject_supersession_insert
BEFORE INSERT ON boundary_dispositions
FOR EACH ROW BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM boundary_supersessions
        WHERE boundary_id = NEW.boundary_id
    ) THEN RAISE(ABORT, 'superseded boundary cannot receive a disposition') END;
END;

CREATE TRIGGER IF NOT EXISTS boundary_dispositions_reject_supersession_update
BEFORE UPDATE ON boundary_dispositions
FOR EACH ROW BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM boundary_supersessions
        WHERE boundary_id = NEW.boundary_id
    ) THEN RAISE(ABORT, 'superseded boundary cannot receive a disposition') END;
END;

CREATE TABLE IF NOT EXISTS event_consumers (
    name TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS effect_grants (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    target_pattern TEXT NOT NULL,
    action_pattern TEXT NOT NULL,
    content_digest TEXT NOT NULL DEFAULT '',
    argv_digest TEXT NOT NULL DEFAULT '',
    workdir_digest TEXT NOT NULL DEFAULT '',
    expires_at TEXT,
    standing INTEGER NOT NULL DEFAULT 0,
    consumed_at TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS effect_operations (
    id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id),
    kind TEXT NOT NULL,
    target TEXT NOT NULL,
    action TEXT NOT NULL,
    content_digest TEXT NOT NULL DEFAULT '',
    argv_digest TEXT NOT NULL DEFAULT '',
    workdir_digest TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    evidence TEXT NOT NULL DEFAULT '',
    grant_id TEXT REFERENCES effect_grants(id),
    cleanup_work_item_id TEXT REFERENCES work_items(id) ON DELETE CASCADE,
    cleanup_generation INTEGER,
    cleanup_preparation_id TEXT NOT NULL DEFAULT '',
    cleanup_target_kind TEXT NOT NULL DEFAULT '',
    cleanup_target_fingerprint TEXT NOT NULL DEFAULT '',
    cleanup_execution_digest TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS effect_operations_target_idx
    ON effect_operations(target, action, status);

CREATE TABLE IF NOT EXISTS a2a_task_map (
    task_id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL UNIQUE REFERENCES work_items(id) ON DELETE CASCADE,
    context_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS a2a_push_configs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES a2a_task_map(task_id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    token_encrypted TEXT NOT NULL DEFAULT '',
    authentication_scheme TEXT NOT NULL DEFAULT 'Bearer',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS a2a_push_deliveries (
    id TEXT PRIMARY KEY,
    config_id TEXT NOT NULL REFERENCES a2a_push_configs(id) ON DELETE CASCADE,
    event_sequence INTEGER NOT NULL REFERENCES events(sequence) ON DELETE CASCADE,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    lease_until TEXT,
    owner_token TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(config_id, event_sequence)
);

CREATE INDEX IF NOT EXISTS a2a_push_deliveries_due_idx
    ON a2a_push_deliveries(state, next_attempt_at);

CREATE TABLE IF NOT EXISTS idempotency_results (
    actor_id TEXT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(actor_id, operation, idempotency_key)
);
"""


def _install_requester_decision_binding_triggers(
    connection: sqlite3.Connection,
) -> None:
    """Bind one historic review to a later epoch of the same conversation.

    CAO runtime credentials rotate on every autonomous resume.  The immutable
    attachment ID is the conversation identity; its generation fences the
    currently authenticated process epoch.  A later requester decision may
    therefore consume a review from an earlier generation of that same
    attachment, but never one from a future generation or another attachment.
    """

    for suffix, event in (("insert", "INSERT"), ("update", "UPDATE")):
        trigger = f"requester_decisions_exact_binding_{suffix}"
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute(
            f"""
            CREATE TRIGGER {trigger}
            BEFORE {event} ON requester_decisions
            FOR EACH ROW BEGIN
                SELECT CASE WHEN NOT EXISTS (
                    SELECT 1
                    FROM work_items w
                    JOIN attempts a ON a.id = NEW.attempt_id AND a.work_item_id = w.id
                    JOIN reviews r ON r.id = NEW.review_id
                                   AND r.work_item_id = w.id
                                   AND r.attempt_id = a.id
                    JOIN cao_session_attachments attachment
                      ON attachment.id = NEW.supervisor_attachment_id
                    WHERE w.id = NEW.work_item_id
                      AND w.requester_id = NEW.requester_id
                      AND w.supervisor_id = NEW.recorded_by
                      AND w.supervisor_attachment_id = NEW.supervisor_attachment_id
                      AND w.generation = NEW.work_generation
                      AND attachment.principal_id = NEW.recorded_by
                      AND attachment.generation = NEW.supervisor_attachment_generation
                      AND r.reviewer_role = 'cao'
                      AND r.verdict = 'ok'
                      AND r.supervisor_attachment_id = NEW.supervisor_attachment_id
                      AND r.supervisor_attachment_generation
                          <= NEW.supervisor_attachment_generation
                      AND r.work_generation <= NEW.work_generation
                      AND a.goal_version = NEW.goal_version
                      AND a.goal_packet_digest = NEW.goal_packet_digest
                      AND a.task_packet_digest = NEW.task_packet_digest
                      AND r.goal_version = NEW.goal_version
                      AND r.goal_packet_digest = NEW.goal_packet_digest
                      AND r.task_packet_digest = NEW.task_packet_digest
                ) THEN RAISE(ABORT, 'requester decision exact binding violation') END;
            END
            """
        )


def _install_work_close_receipt_binding_triggers(
    connection: sqlite3.Connection,
) -> None:
    """Allow one sealed close contract across later epochs of its attachment."""

    for suffix, event in (("insert", "INSERT"), ("update", "UPDATE")):
        trigger = f"work_close_receipts_exact_binding_{suffix}"
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute(
            f"""
            CREATE TRIGGER {trigger}
            BEFORE {event} ON work_close_receipts
            FOR EACH ROW BEGIN
                SELECT CASE WHEN NOT EXISTS (
                    SELECT 1
                    FROM work_items w
                    JOIN attempts a ON a.id = NEW.attempt_id AND a.work_item_id = w.id
                    JOIN reviews r ON r.id = NEW.review_id
                                   AND r.work_item_id = w.id
                                   AND r.attempt_id = a.id
                    JOIN requester_decisions d ON d.id = NEW.requester_decision_id
                                              AND d.work_item_id = w.id
                                              AND d.attempt_id = a.id
                                              AND d.review_id = r.id
                    JOIN cao_session_attachments attachment
                      ON attachment.id = NEW.supervisor_attachment_id
                    WHERE w.id = NEW.work_item_id
                      AND w.state = 'completed'
                      AND NEW.cleanup_inventory_evidence_id <> ''
                      AND w.supervisor_id = NEW.closed_by
                      AND w.supervisor_attachment_id = NEW.supervisor_attachment_id
                      AND w.generation = NEW.work_generation
                      AND attachment.principal_id = NEW.closed_by
                      AND attachment.generation
                          >= NEW.supervisor_attachment_generation
                      AND d.verdict = 'accepted'
                      AND d.recorded_by = NEW.closed_by
                      AND d.supervisor_attachment_id = NEW.supervisor_attachment_id
                      AND d.supervisor_attachment_generation
                          <= NEW.supervisor_attachment_generation
                      AND d.work_generation = NEW.work_generation
                      AND a.goal_version = NEW.goal_version
                      AND a.goal_packet_digest = NEW.goal_packet_digest
                      AND a.task_packet_digest = NEW.task_packet_digest
                      AND r.goal_version = NEW.goal_version
                      AND r.goal_packet_digest = NEW.goal_packet_digest
                      AND r.task_packet_digest = NEW.task_packet_digest
                      AND d.goal_version = NEW.goal_version
                      AND d.goal_packet_digest = NEW.goal_packet_digest
                      AND d.task_packet_digest = NEW.task_packet_digest
                ) THEN RAISE(ABORT, 'work close receipt exact binding violation') END;
            END
            """
        )


def _install_boundary_supersession_binding_triggers(
    connection: sqlite3.Connection,
) -> None:
    """Bind supersession to one exact later cancellation or recovery event."""

    boundary_event_json = _safe_json_document_sql("boundary_event.data_json")
    superseding_event_json = _safe_json_document_sql("superseding_event.data_json")
    exact_boundary_event_json = _safe_json_document_sql("exact_boundary_event.data_json")
    exact_replacement_event_json = _safe_json_document_sql("exact_replacement_event.data_json")
    boundary_metadata_json = _safe_json_document_sql("boundary.metadata_json")
    replacement_metadata_json = _safe_json_document_sql("replacement.metadata_json")

    connection.execute("DROP TRIGGER IF EXISTS boundary_supersessions_exact_binding_insert")
    connection.execute("DROP TRIGGER IF EXISTS boundary_supersessions_immutable_update")
    connection.execute("DROP TRIGGER IF EXISTS boundary_dispositions_reject_supersession_insert")
    connection.execute("DROP TRIGGER IF EXISTS boundary_dispositions_reject_supersession_update")
    connection.execute(
        f"""
        CREATE TRIGGER boundary_supersessions_exact_binding_insert
        BEFORE INSERT ON boundary_supersessions
        FOR EACH ROW BEGIN
            SELECT CASE WHEN NOT (
              EXISTS (
                SELECT 1
                FROM boundaries AS boundary
                JOIN work_items AS work ON work.id = boundary.work_item_id
                JOIN events AS boundary_event
                  ON boundary_event.sequence = NEW.boundary_event_sequence
                 AND boundary_event.event_type = 'boundary.recorded'
                 AND boundary_event.aggregate_type = 'work_item'
                 AND boundary_event.aggregate_id = boundary.work_item_id
                 AND json_extract(
                       {boundary_event_json}, '$.boundary_id'
                     ) = boundary.id
                JOIN events AS superseding_event
                  ON superseding_event.sequence = NEW.superseding_event_sequence
                 AND superseding_event.event_type = 'work.canceled'
                 AND superseding_event.aggregate_type = 'work_item'
                 AND superseding_event.aggregate_id = boundary.work_item_id
                 AND superseding_event.sequence > boundary_event.sequence
                LEFT JOIN boundary_dispositions AS disposition
                  ON disposition.boundary_id = boundary.id
                WHERE boundary.id = NEW.boundary_id
                  AND work.state = 'canceled'
                  AND boundary.generation < work.generation
                  AND disposition.id IS NULL
                  AND NEW.reason = 'work_canceled'
                  AND (
                      SELECT COUNT(*) FROM events AS exact_boundary_event
                      WHERE exact_boundary_event.event_type = 'boundary.recorded'
                        AND exact_boundary_event.aggregate_type = 'work_item'
                        AND exact_boundary_event.aggregate_id = boundary.work_item_id
                        AND json_extract(
                              {exact_boundary_event_json}, '$.boundary_id'
                            ) = boundary.id
                  ) = 1
              ) OR EXISTS (
                SELECT 1
                FROM boundaries AS boundary
                JOIN events AS boundary_event
                  ON boundary_event.sequence = NEW.boundary_event_sequence
                 AND boundary_event.event_type = 'boundary.recorded'
                 AND boundary_event.aggregate_type = 'work_item'
                 AND boundary_event.aggregate_id = boundary.work_item_id
                 AND json_extract(
                       {boundary_event_json}, '$.boundary_id'
                     ) = boundary.id
                JOIN events AS superseding_event
                  ON superseding_event.sequence = NEW.superseding_event_sequence
                 AND superseding_event.event_type = 'boundary.recorded'
                 AND superseding_event.aggregate_type = 'work_item'
                 AND superseding_event.aggregate_id = boundary.work_item_id
                 AND superseding_event.sequence > boundary_event.sequence
                JOIN boundaries AS replacement
                  ON replacement.id = json_extract(
                       {superseding_event_json}, '$.boundary_id'
                     )
                 AND replacement.work_item_id = boundary.work_item_id
                 AND replacement.attempt_id = boundary.attempt_id
                 AND replacement.generation = boundary.generation
                LEFT JOIN boundary_dispositions AS disposition
                  ON disposition.boundary_id = boundary.id
                WHERE boundary.id = NEW.boundary_id
                  AND disposition.id IS NULL
                  AND NEW.reason = 'recovery_boundary_replaced'
                  AND (
                    json_extract({boundary_metadata_json}, '$.runtime_recovery') = 1
                    OR json_extract({boundary_metadata_json}, '$.system_recovery') = 1
                  )
                  AND (
                    json_extract({replacement_metadata_json}, '$.runtime_recovery') = 1
                    OR json_extract({replacement_metadata_json}, '$.system_recovery') = 1
                  )
                  AND (
                      SELECT COUNT(*) FROM events AS exact_boundary_event
                      WHERE exact_boundary_event.event_type = 'boundary.recorded'
                        AND exact_boundary_event.aggregate_type = 'work_item'
                        AND exact_boundary_event.aggregate_id = boundary.work_item_id
                        AND json_extract(
                              {exact_boundary_event_json}, '$.boundary_id'
                            ) = boundary.id
                  ) = 1
                  AND (
                      SELECT COUNT(*) FROM events AS exact_replacement_event
                      WHERE exact_replacement_event.event_type = 'boundary.recorded'
                        AND exact_replacement_event.aggregate_type = 'work_item'
                        AND exact_replacement_event.aggregate_id = replacement.work_item_id
                        AND json_extract(
                              {exact_replacement_event_json}, '$.boundary_id'
                            ) = replacement.id
                  ) = 1
              ) OR EXISTS (
                SELECT 1
                FROM boundaries AS boundary
                JOIN work_items AS work ON work.id = boundary.work_item_id
                JOIN events AS boundary_event
                  ON boundary_event.sequence = NEW.boundary_event_sequence
                 AND boundary_event.event_type = 'boundary.recorded'
                 AND boundary_event.aggregate_type = 'work_item'
                 AND boundary_event.aggregate_id = boundary.work_item_id
                 AND json_extract(
                       {boundary_event_json}, '$.boundary_id'
                     ) = boundary.id
                JOIN events AS superseding_event
                  ON superseding_event.sequence = NEW.superseding_event_sequence
                 AND superseding_event.event_type = 'work.goal_replaced'
                 AND superseding_event.aggregate_type = 'work_item'
                 AND superseding_event.aggregate_id = boundary.work_item_id
                 AND superseding_event.sequence > boundary_event.sequence
                JOIN goal_revisions AS replacement_goal
                  ON replacement_goal.work_item_id = boundary.work_item_id
                 AND replacement_goal.version = CAST(json_extract(
                       {superseding_event_json}, '$.version'
                     ) AS INTEGER)
                 AND replacement_goal.source_directive_id = json_extract(
                       {superseding_event_json}, '$.directive_id'
                     )
                JOIN attempts AS successor_attempt
                  ON successor_attempt.id = json_extract(
                       {superseding_event_json}, '$.attempt_id'
                     )
                 AND successor_attempt.work_item_id = boundary.work_item_id
                 AND successor_attempt.goal_version = replacement_goal.version
                LEFT JOIN boundary_dispositions AS disposition
                  ON disposition.boundary_id = boundary.id
                WHERE boundary.id = NEW.boundary_id
                  AND disposition.id IS NULL
                  AND NEW.reason = 'goal_replaced'
                  AND replacement_goal.version = boundary.goal_version + 1
                  AND CAST(json_extract(
                        {superseding_event_json}, '$.generation'
                      ) AS INTEGER) = boundary.generation + 1
                  AND work.goal_version >= replacement_goal.version
                  AND work.generation >= CAST(json_extract(
                        {superseding_event_json}, '$.generation'
                      ) AS INTEGER)
                  AND (
                      SELECT COUNT(*) FROM events AS exact_boundary_event
                      WHERE exact_boundary_event.event_type = 'boundary.recorded'
                        AND exact_boundary_event.aggregate_type = 'work_item'
                        AND exact_boundary_event.aggregate_id = boundary.work_item_id
                        AND json_extract(
                              {exact_boundary_event_json}, '$.boundary_id'
                            ) = boundary.id
                  ) = 1
                  AND (
                      SELECT COUNT(*) FROM events AS exact_replacement_event
                      WHERE exact_replacement_event.event_type = 'work.goal_replaced'
                        AND exact_replacement_event.aggregate_type = 'work_item'
                        AND exact_replacement_event.aggregate_id = boundary.work_item_id
                        AND CAST(json_extract(
                              {exact_replacement_event_json}, '$.version'
                            ) AS INTEGER) = replacement_goal.version
                        AND CAST(json_extract(
                              {exact_replacement_event_json}, '$.generation'
                            ) AS INTEGER) = boundary.generation + 1
                        AND json_extract(
                              {exact_replacement_event_json}, '$.attempt_id'
                            ) = successor_attempt.id
                        AND json_extract(
                              {exact_replacement_event_json}, '$.directive_id'
                            ) = replacement_goal.source_directive_id
                  ) = 1
              )
            ) THEN RAISE(
                ABORT, 'boundary supersession exact binding violation'
            ) END;
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_supersessions_immutable_update
        BEFORE UPDATE ON boundary_supersessions
        FOR EACH ROW BEGIN
            SELECT RAISE(ABORT, 'boundary supersession is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_dispositions_reject_supersession_insert
        BEFORE INSERT ON boundary_dispositions
        FOR EACH ROW BEGIN
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM boundary_supersessions
                WHERE boundary_id = NEW.boundary_id
            ) THEN RAISE(
                ABORT, 'superseded boundary cannot receive a disposition'
            ) END;
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_dispositions_reject_supersession_update
        BEFORE UPDATE ON boundary_dispositions
        FOR EACH ROW BEGIN
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM boundary_supersessions
                WHERE boundary_id = NEW.boundary_id
            ) THEN RAISE(
                ABORT, 'superseded boundary cannot receive a disposition'
            ) END;
        END
        """
    )


def _install_cao_connection_credential_triggers(
    connection: sqlite3.Connection,
) -> None:
    """Fence every active CSC to one exact attachment connection tuple."""

    connection.executescript(
        """
        DROP TRIGGER IF EXISTS cao_conversation_credentials_connection_insert;
        DROP TRIGGER IF EXISTS cao_conversation_credentials_connection_update;
        CREATE TRIGGER cao_conversation_credentials_connection_insert
        BEFORE INSERT ON cao_conversation_credentials
        FOR EACH ROW WHEN NEW.state = 'active' BEGIN
            SELECT CASE WHEN NEW.connection_id IS NULL OR NOT EXISTS (
                SELECT 1
                FROM cao_attachment_connections AS connection
                WHERE connection.id = NEW.connection_id
                  AND connection.attachment_id = NEW.attachment_id
                  AND connection.principal_id = NEW.principal_id
                  AND connection.generation = NEW.generation
                  AND connection.state = 'active'
            ) THEN RAISE(ABORT, 'active CSC connection binding violation') END;
        END;
        CREATE TRIGGER cao_conversation_credentials_connection_update
        BEFORE UPDATE ON cao_conversation_credentials
        FOR EACH ROW WHEN NEW.state = 'active' BEGIN
            SELECT CASE WHEN NEW.connection_id IS NULL OR NOT EXISTS (
                SELECT 1
                FROM cao_attachment_connections AS connection
                WHERE connection.id = NEW.connection_id
                  AND connection.attachment_id = NEW.attachment_id
                  AND connection.principal_id = NEW.principal_id
                  AND connection.generation = NEW.generation
                  AND connection.state = 'active'
            ) THEN RAISE(ABORT, 'active CSC connection binding violation') END;
        END;
        """
    )


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    """Return one trusted schema table's column names."""

    return {str(column["name"]) for column in connection.execute(f"PRAGMA table_info({table})")}


def _drop_column_if_present(
    connection: sqlite3.Connection,
    table: str,
    column: str,
) -> None:
    if column in _table_columns(connection, table):
        connection.execute(f"ALTER TABLE {table} DROP COLUMN {column}")


def _normalize_cao_attachment_peer_schema_v35(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Collapse legacy root/bridge bindings to one kernel peer identity.

    The caller adds the v35 peer columns before this helper runs.  Legacy
    connections preserve the exact bridge generation as their peer.  If the
    old root+bridge uniqueness rule admitted several active rows for that same
    peer, no row is guessed to be authoritative: every ambiguous connection
    is made stale and every active CSC bound to it is revoked.
    """

    now = utc_now()
    bootstrap_columns = _table_columns(connection, "cao_attachment_bootstrap_credentials")
    if {"bridge_pid", "bridge_start_signature"} <= bootstrap_columns:
        connection.execute(
            """
            UPDATE cao_attachment_bootstrap_credentials
            SET peer_pid = bridge_pid,
                peer_start_signature = bridge_start_signature
            WHERE peer_pid <= 0 AND peer_start_signature = ''
              AND bridge_pid > 0 AND bridge_start_signature <> ''
            """
        )
    # A pre-v35 CAB has no trustworthy observation of the logical attachment
    # generation.  Preserve it as history, but never let it cross the new CAS.
    if existing_schema_version < 35:
        connection.execute(
            """
            UPDATE cao_attachment_bootstrap_credentials
            SET state = 'revoked', revoked_at = COALESCE(revoked_at, ?),
                updated_at = ?
            WHERE state = 'active'
            """,
            (now, now),
        )
    connection.execute(
        f"""
        UPDATE cao_attachment_bootstrap_credentials
        SET state = 'revoked', revoked_at = COALESCE(revoked_at, ?),
            updated_at = ?
        WHERE state = 'active'
          AND (
                peer_pid <= 0
             OR peer_start_signature = ''
             OR length(proxy_catalog_digest) <> 64
             OR proxy_catalog_digest GLOB '*[^0-9a-f]*'
             OR proxy_abi_version <> {CAO_CONVERSATION_PROXY_ABI_VERSION}
          )
        """,
        (now, now),
    )

    connection_columns = _table_columns(connection, "cao_attachment_connections")
    if {"bridge_pid", "bridge_start_signature"} <= connection_columns:
        connection.execute(
            """
            UPDATE cao_attachment_connections
            SET peer_pid = bridge_pid,
                peer_start_signature = bridge_start_signature
            WHERE peer_pid <= 0 AND peer_start_signature = ''
              AND bridge_pid > 0 AND bridge_start_signature <> ''
            """
        )

    invalid_or_ambiguous_connection_ids = {
        str(row["id"])
        for row in connection.execute(
            f"""
            SELECT id
            FROM cao_attachment_connections
            WHERE state = 'active'
              AND (
                    peer_pid <= 0
                 OR peer_start_signature = ''
                 OR length(proxy_catalog_digest) <> 64
                 OR proxy_catalog_digest GLOB '*[^0-9a-f]*'
                 OR proxy_abi_version <> {CAO_CONVERSATION_PROXY_ABI_VERSION}
              )
            """
        )
    }
    ambiguous_peers = connection.execute(
        """
        SELECT principal_id, peer_pid, peer_start_signature
        FROM cao_attachment_connections
        WHERE state = 'active'
          AND peer_pid > 0 AND peer_start_signature <> ''
        GROUP BY principal_id, peer_pid, peer_start_signature
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for peer in ambiguous_peers:
        invalid_or_ambiguous_connection_ids.update(
            str(row["id"])
            for row in connection.execute(
                """
                SELECT id
                FROM cao_attachment_connections
                WHERE state = 'active' AND principal_id = ?
                  AND peer_pid = ? AND peer_start_signature = ?
                """,
                (
                    peer["principal_id"],
                    peer["peer_pid"],
                    peer["peer_start_signature"],
                ),
            )
        )
    if invalid_or_ambiguous_connection_ids:
        placeholders = ",".join("?" for _ in invalid_or_ambiguous_connection_ids)
        ordered_ids = tuple(sorted(invalid_or_ambiguous_connection_ids))
        connection.execute(
            f"""
            UPDATE cao_conversation_credentials
            SET state = 'revoked', revoked_at = ?, updated_at = ?
            WHERE state = 'active' AND connection_id IN ({placeholders})
            """,
            (now, now, *ordered_ids),
        )
        connection.execute(
            f"""
            UPDATE cao_attachment_connections
            SET state = 'stale', revoked_at = ?, updated_at = ?
            WHERE state = 'active' AND id IN ({placeholders})
            """,
            (now, now, *ordered_ids),
        )

    connection.execute("DROP INDEX IF EXISTS cao_attachment_connections_one_active_bridge")
    connection.execute("DROP INDEX IF EXISTS cao_attachment_connections_one_active_peer")
    connection.execute("DROP INDEX IF EXISTS cao_attachment_connections_one_active_legacy")
    connection.execute("DROP TRIGGER IF EXISTS cao_attachment_connections_active_peer_insert")
    connection.execute("DROP TRIGGER IF EXISTS cao_attachment_connections_active_peer_update")
    connection.execute(
        "DROP TRIGGER IF EXISTS cao_attachment_bootstrap_credentials_active_contract_insert"
    )
    connection.execute(
        "DROP TRIGGER IF EXISTS cao_attachment_bootstrap_credentials_active_contract_update"
    )
    for column in (
        "host_root_pid",
        "host_root_start_signature",
        "bridge_pid",
        "bridge_start_signature",
    ):
        _drop_column_if_present(connection, "cao_attachment_bootstrap_credentials", column)
        _drop_column_if_present(connection, "cao_attachment_connections", column)
        _drop_column_if_present(connection, "cao_session_attachments", column)

    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            cao_attachment_connections_one_active_peer
        ON cao_attachment_connections(
            principal_id, peer_pid, peer_start_signature
        )
        WHERE state = 'active'
          AND peer_pid > 0 AND peer_start_signature <> ''
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER cao_attachment_connections_active_peer_insert
        BEFORE INSERT ON cao_attachment_connections
        FOR EACH ROW WHEN NEW.state = 'active'
          AND (
                NEW.peer_pid <= 0
             OR NEW.peer_start_signature = ''
             OR length(NEW.proxy_catalog_digest) <> 64
             OR NEW.proxy_catalog_digest GLOB '*[^0-9a-f]*'
             OR NEW.proxy_abi_version <> {CAO_CONVERSATION_PROXY_ABI_VERSION}
          )
        BEGIN
            SELECT RAISE(ABORT, 'active CAO connection contract violation');
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER cao_attachment_connections_active_peer_update
        BEFORE UPDATE ON cao_attachment_connections
        FOR EACH ROW WHEN NEW.state = 'active'
          AND (
                NEW.peer_pid <= 0
             OR NEW.peer_start_signature = ''
             OR length(NEW.proxy_catalog_digest) <> 64
             OR NEW.proxy_catalog_digest GLOB '*[^0-9a-f]*'
             OR NEW.proxy_abi_version <> {CAO_CONVERSATION_PROXY_ABI_VERSION}
          )
        BEGIN
            SELECT RAISE(ABORT, 'active CAO connection contract violation');
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER cao_attachment_bootstrap_credentials_active_contract_insert
        BEFORE INSERT ON cao_attachment_bootstrap_credentials
        FOR EACH ROW WHEN NEW.state = 'active'
          AND (
                NEW.peer_pid <= 0
             OR NEW.peer_start_signature = ''
             OR length(NEW.proxy_catalog_digest) <> 64
             OR NEW.proxy_catalog_digest GLOB '*[^0-9a-f]*'
             OR NEW.proxy_abi_version <> {CAO_CONVERSATION_PROXY_ABI_VERSION}
          )
        BEGIN
            SELECT RAISE(ABORT, 'active CAO bootstrap contract violation');
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER cao_attachment_bootstrap_credentials_active_contract_update
        BEFORE UPDATE ON cao_attachment_bootstrap_credentials
        FOR EACH ROW WHEN NEW.state = 'active'
          AND (
                NEW.peer_pid <= 0
             OR NEW.peer_start_signature = ''
             OR length(NEW.proxy_catalog_digest) <> 64
             OR NEW.proxy_catalog_digest GLOB '*[^0-9a-f]*'
             OR NEW.proxy_abi_version <> {CAO_CONVERSATION_PROXY_ABI_VERSION}
          )
        BEGIN
            SELECT RAISE(ABORT, 'active CAO bootstrap contract violation');
        END
        """
    )


def _ensure_recovery_boundary_supersession_schema(
    connection: sqlite3.Connection,
) -> None:
    """Install every exact, append-only Boundary supersession reason."""

    table = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'boundary_supersessions'"
    ).fetchone()
    if table is None:
        return
    table_sql = str(table["sql"] or "")
    if "recovery_boundary_replaced" not in table_sql or "goal_replaced" not in table_sql:
        for trigger in (
            "boundary_supersessions_exact_binding_insert",
            "boundary_supersessions_immutable_update",
            "boundary_dispositions_reject_supersession_insert",
            "boundary_dispositions_reject_supersession_update",
        ):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute(
            """
            CREATE TABLE boundary_supersessions_v41 (
                boundary_id TEXT PRIMARY KEY
                    REFERENCES boundaries(id) ON DELETE CASCADE,
                boundary_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                superseding_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                reason TEXT NOT NULL CHECK(
                    reason IN (
                      'work_canceled', 'recovery_boundary_replaced',
                      'goal_replaced'
                    )
                ),
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO boundary_supersessions_v41 SELECT * FROM boundary_supersessions"
        )
        connection.execute("DROP TABLE boundary_supersessions")
        connection.execute(
            "ALTER TABLE boundary_supersessions_v41 RENAME TO boundary_supersessions"
        )
        connection.execute(
            "CREATE INDEX boundary_supersessions_event_idx "
            "ON boundary_supersessions(superseding_event_sequence)"
        )
        connection.execute(
            "CREATE INDEX boundary_supersessions_boundary_event_idx "
            "ON boundary_supersessions(boundary_event_sequence)"
        )
    _install_boundary_supersession_binding_triggers(connection)


_OPERATOR_SCOPE_TRIGGERS = (
    "principals_operator_scope_allowed_insert",
    "principals_operator_scope_allowed_update",
    "principals_operator_scope_immutable",
    "principals_operator_label_immutable",
    "principals_operator_identity_valid_insert",
    "principals_operator_identity_valid_update",
    "work_items_operator_scope_allowed_insert",
    "work_items_operator_scope_allowed_update",
    "work_items_worker_scope_valid_insert",
    "work_items_operator_scope_immutable",
    "work_items_worker_scope_immutable",
)


def _drop_operator_scope_triggers(connection: sqlite3.Connection) -> None:
    for trigger in _OPERATOR_SCOPE_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")


def _safe_historical_operator_label(value: object) -> str | None:
    label = str(value).strip()
    if (
        not label
        or len(label) > 128
        or "/" in label
        or "\\" in label
        or "\x00" in label
        or "\n" in label
        or "\r" in label
        or contains_control_plane_secret(label)
    ):
        return None
    return label


def _install_operator_scope_triggers(connection: sqlite3.Connection) -> None:
    """Enforce v23 scope values on upgraded tables and freeze Work provenance."""

    _drop_operator_scope_triggers(connection)
    allowed = "'production', 'acceptance-test', 'system', 'unclassified'"
    for table in ("principals", "work_items"):
        for suffix, event in (("insert", "INSERT"), ("update", "UPDATE OF operator_scope")):
            trigger = f"{table}_operator_scope_allowed_{suffix}"
            connection.execute(
                f"""
                CREATE TRIGGER {trigger}
                BEFORE {event} ON {table}
                FOR EACH ROW
                WHEN NEW.operator_scope NOT IN ({allowed})
                BEGIN
                    SELECT RAISE(ABORT, 'invalid operator scope');
                END
                """
            )

    connection.execute(
        """
        CREATE TRIGGER principals_operator_scope_immutable
        BEFORE UPDATE OF operator_scope ON principals
        FOR EACH ROW
        WHEN NEW.operator_scope <> OLD.operator_scope
         AND NOT (
             OLD.operator_scope = 'unclassified'
             AND NEW.operator_scope IN ('production', 'acceptance-test', 'system')
             AND NOT EXISTS (
                 SELECT 1 FROM work_items
                 WHERE assigned_worker_id = OLD.id
             )
         )
         AND NOT (
             OLD.role = 'worker' AND NEW.role = 'worker'
             AND OLD.enabled = 1 AND NEW.enabled = 0
             AND OLD.operator_scope IN ('production', 'acceptance-test')
             AND NEW.operator_scope = 'unclassified'
             AND NEW.operator_label = ''
             AND EXISTS (
                 SELECT 1
                 FROM managed_worker_specs AS spec
                 JOIN managed_worker_threads AS thread
                   ON thread.managed_spec_id = spec.id
                 WHERE spec.principal_id = OLD.id
                   AND spec.state = 'revoked'
                   AND thread.state = 'archived'
             )
         )
        BEGIN
            SELECT RAISE(ABORT, 'principal operator scope is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER principals_operator_label_immutable
        BEFORE UPDATE OF operator_label ON principals
        FOR EACH ROW
        WHEN NEW.operator_label <> OLD.operator_label
         AND OLD.operator_scope <> 'unclassified'
         AND NOT (
             OLD.role = 'worker' AND NEW.role = 'worker'
             AND OLD.enabled = 1 AND NEW.enabled = 0
             AND OLD.operator_scope IN ('production', 'acceptance-test')
             AND NEW.operator_scope = 'unclassified'
             AND NEW.operator_label = ''
             AND EXISTS (
                 SELECT 1
                 FROM managed_worker_specs AS spec
                 JOIN managed_worker_threads AS thread
                   ON thread.managed_spec_id = spec.id
                 WHERE spec.principal_id = OLD.id
                   AND spec.state = 'revoked'
                   AND thread.state = 'archived'
             )
         )
        BEGIN
            SELECT RAISE(ABORT, 'principal operator label is immutable');
        END
        """
    )
    secret_shaped = " OR ".join(
        f"NEW.operator_label GLOB '*cao.{prefix}_*.*'"
        for prefix in ("prn", "rtc", "ent", "crc", "catk", "csc", "cab")
    )
    invalid_identity = f"""
        NEW.role <> 'worker' AND (
            NEW.operator_scope <> 'unclassified' OR NEW.operator_label <> ''
        )
        OR NEW.role = 'worker' AND NEW.operator_scope IN (
            'production', 'acceptance-test'
        ) AND (
            trim(NEW.operator_label) = ''
            OR instr(NEW.operator_label, '/') > 0
            OR instr(NEW.operator_label, CAST(X'5C' AS TEXT)) > 0
            OR instr(NEW.operator_label, CAST(X'00' AS TEXT)) > 0
            OR instr(NEW.operator_label, CAST(X'0A' AS TEXT)) > 0
            OR instr(NEW.operator_label, CAST(X'0D' AS TEXT)) > 0
            OR {secret_shaped}
        )
        OR NEW.role = 'worker'
           AND NEW.operator_scope IN ('system', 'unclassified')
           AND NEW.operator_label <> ''
    """
    for suffix, event in (
        ("insert", "INSERT"),
        ("update", "UPDATE OF role, operator_scope, operator_label"),
    ):
        connection.execute(
            f"""
            CREATE TRIGGER principals_operator_identity_valid_{suffix}
            BEFORE {event} ON principals
            FOR EACH ROW
            WHEN {invalid_identity}
            BEGIN
                SELECT RAISE(ABORT, 'invalid principal operator identity');
            END
            """
        )
    connection.execute(
        """
        CREATE TRIGGER work_items_worker_scope_valid_insert
        BEFORE INSERT ON work_items
        FOR EACH ROW
        WHEN COALESCE((
            SELECT operator_scope FROM principals WHERE id = NEW.assigned_worker_id
        ), '') <> NEW.operator_scope
        BEGIN
            SELECT RAISE(ABORT, 'WorkItem scope does not match assigned Worker');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_operator_scope_immutable
        BEFORE UPDATE OF operator_scope ON work_items
        FOR EACH ROW
        WHEN NEW.operator_scope <> OLD.operator_scope
        BEGIN
            SELECT RAISE(ABORT, 'WorkItem operator scope is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_worker_scope_immutable
        BEFORE UPDATE OF assigned_worker_id ON work_items
        FOR EACH ROW
        WHEN COALESCE((
            SELECT operator_scope FROM principals WHERE id = NEW.assigned_worker_id
        ), '') <> OLD.operator_scope
        BEGIN
            SELECT RAISE(ABORT, 'cross-scope WorkItem reassignment');
        END
        """
    )


def _json_value(raw: object, default: Any) -> Any:
    try:
        return json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return default


def _backfill_goal_attachment_epochs(connection: sqlite3.Connection) -> None:
    """Freeze the exact conversation epoch sealed by each immutable Goal.

    A logical attachment can reopen on a different CAO wake runtime.  The
    attachment row is therefore current state, not authority for reconstructing
    an older Goal or Task packet.  A sealed legacy packet may supply its runtime
    only after its digest and stable attachment identity are verified.  An
    unsealed legacy packet may use current state only when its already-stored
    attachment generation proves that it was created in the current epoch.

    This helper runs in the same exclusive migration transaction as
    ``_backfill_goal_and_task_packets``.  That later pass verifies the complete
    canonical Goal semantics before any value populated here can commit.
    """

    rows = connection.execute(
        """
        SELECT g.work_item_id, g.version, g.packet_json, g.packet_digest,
               g.supervisor_id AS goal_supervisor_id,
               g.supervisor_attachment_generation,
               g.supervisor_runtime_session_id,
               w.supervisor_attachment_id, w.supervisor_id AS work_supervisor_id,
               a.principal_id,
               a.runtime_session_id AS current_attachment_runtime_session_id,
               a.native_thread_id,
               a.project_digest, a.generation AS current_attachment_generation
        FROM goal_revisions AS g
        JOIN work_items AS w ON w.id = g.work_item_id
        LEFT JOIN cao_session_attachments AS a
          ON a.id = w.supervisor_attachment_id
        ORDER BY g.work_item_id, g.version
        """
    ).fetchall()
    for row in rows:
        attachment_id = row["supervisor_attachment_id"]
        stored_generation = row["supervisor_attachment_generation"]
        stored_runtime_id = row["supervisor_runtime_session_id"]
        packet_raw = str(row["packet_json"] or "")
        packet_digest = str(row["packet_digest"] or "")
        packet = _json_value(packet_raw, None)
        packet_is_default = packet_raw in {"", "{}"}
        if attachment_id is None:
            if stored_generation is not None or stored_runtime_id is not None:
                raise RuntimeError("unbound goal has an attachment epoch")
            if not packet_is_default:
                if not isinstance(packet, dict) or not packet_digest:
                    raise RuntimeError("unbound goal packet binding is partial")
                try:
                    computed_digest = goal_packet_digest(packet)
                except (TypeError, ValueError) as error:
                    raise RuntimeError("unbound goal packet binding is invalid") from error
                if computed_digest != packet_digest or "supervisor_attachment" in packet:
                    raise RuntimeError("unbound goal packet binding is invalid")
            elif packet_digest:
                raise RuntimeError("unbound goal packet binding is partial")
            continue
        if row["current_attachment_generation"] is None:
            raise RuntimeError("goal attachment binding cannot be reconstructed")
        if str(row["goal_supervisor_id"] or "") != str(row["principal_id"]) or str(
            row["work_supervisor_id"] or ""
        ) != str(row["principal_id"]):
            raise RuntimeError("goal attachment supervisor binding is invalid")

        packet_attachment = (
            packet.get("supervisor_attachment") if isinstance(packet, dict) else None
        )
        if packet_is_default:
            if packet_digest:
                raise RuntimeError("legacy unsealed goal packet binding is partial")
            if (
                stored_generation is None
                or isinstance(stored_generation, bool)
                or int(stored_generation) != int(row["current_attachment_generation"])
            ):
                raise RuntimeError("legacy unsealed goal attachment epoch is ambiguous")
            candidate_generation = int(stored_generation)
            candidate_runtime_id = str(row["current_attachment_runtime_session_id"] or "")
        else:
            if not isinstance(packet, dict) or not packet_digest:
                raise RuntimeError("sealed goal packet binding is partial")
            try:
                computed_digest = goal_packet_digest(packet)
            except (TypeError, ValueError) as error:
                raise RuntimeError("sealed goal packet binding is invalid") from error
            if computed_digest != packet_digest or not isinstance(packet_attachment, dict):
                raise RuntimeError("sealed goal packet binding is invalid")
            if set(packet_attachment) != {
                "attachment_id",
                "supervisor_id",
                "runtime_session_id",
                "native_thread_id",
                "project_digest",
                "generation",
            }:
                raise RuntimeError("goal attachment binding is invalid")
            candidate = packet_attachment.get("generation")
            if isinstance(candidate, bool) or not isinstance(candidate, int):
                raise RuntimeError("goal attachment generation is invalid")
            candidate_generation = candidate
            candidate_runtime_id = str(packet_attachment.get("runtime_session_id") or "")
            if (
                str(packet_attachment.get("attachment_id") or "") != str(attachment_id)
                or str(packet_attachment.get("supervisor_id") or "") != str(row["principal_id"])
                or str(packet_attachment.get("native_thread_id") or "")
                != str(row["native_thread_id"])
                or str(packet_attachment.get("project_digest") or "") != str(row["project_digest"])
            ):
                raise RuntimeError("goal attachment binding is invalid")

        if (
            candidate_generation < 0
            or candidate_generation > int(row["current_attachment_generation"])
            or (stored_generation is not None and int(stored_generation) != candidate_generation)
        ):
            raise RuntimeError("goal attachment generation is invalid")
        if not candidate_runtime_id:
            raise RuntimeError("goal attachment runtime is invalid")
        runtime = connection.execute(
            "SELECT 1 FROM runtime_sessions WHERE id = ? AND principal_id = ?",
            (candidate_runtime_id, row["principal_id"]),
        ).fetchone()
        if runtime is None:
            raise RuntimeError("goal attachment runtime is invalid")
        if stored_runtime_id is not None and str(stored_runtime_id) != candidate_runtime_id:
            raise RuntimeError("goal attachment runtime is invalid")

        if stored_generation is None or stored_runtime_id is None:
            connection.execute(
                "UPDATE goal_revisions "
                "SET supervisor_attachment_generation = ?, "
                "supervisor_runtime_session_id = ? "
                "WHERE work_item_id = ? AND version = ?",
                (
                    candidate_generation,
                    candidate_runtime_id,
                    row["work_item_id"],
                    row["version"],
                ),
            )


def _scrub_pre_v18_runtime_diagnostics(connection: sqlite3.Connection) -> None:
    """Remove pre-release adapter transcripts before this DB can be served.

    Versions before v18 retained ``last_output`` and the entire adapter result
    in runtime metadata, and wrote equivalent payloads to runtime events.
    Those fields are telemetry only; delivery state and the durable work
    protocol are stored elsewhere.  This migration deliberately selects no
    value from a diagnostic payload into any new durable field.
    """

    runtime_rows = connection.execute("SELECT id, metadata_json FROM runtime_sessions").fetchall()
    for row in runtime_rows:
        metadata = _json_value(row["metadata_json"], None)
        if not isinstance(metadata, dict):
            # A legacy runtime metadata value is required to be an object.  A
            # malformed/non-object value cannot be safely separated from a
            # transcript, so fail closed for that one field rather than
            # copying or reporting any part of it.
            connection.execute(
                "UPDATE runtime_sessions SET metadata_json = '{}' WHERE id = ?",
                (row["id"],),
            )
            continue
        if "last_output" not in metadata and "last_dispatch" not in metadata:
            continue
        # Do not retain a safe-looking subfield: a pre-v18 result was an
        # arbitrary adapter object, and structural selection is the privacy
        # boundary.  Launch metadata unrelated to dispatch diagnostics stays.
        metadata.pop("last_output", None)
        metadata.pop("last_dispatch", None)
        connection.execute(
            "UPDATE runtime_sessions SET metadata_json = ? WHERE id = ?",
            (
                json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                row["id"],
            ),
        )

    # The event ledger is otherwise immutable.  Runtime adapter events were
    # the one historical exception: their result/error payloads duplicate the
    # transcript.  Preserve only the event's semantic outcome; message and
    # delivery tables remain the source of truth for identity and lifecycle.
    for event_type, outcome in _PRE_V18_RUNTIME_EVENT_OUTCOMES.items():
        connection.execute(
            "UPDATE events SET data_json = ? WHERE event_type = ?",
            (json.dumps({"outcome": outcome}, separators=(",", ":")), event_type),
        )

    # Pre-v18 retry/unknown paths also stored arbitrary exception text in the
    # delivery row.  It has no control-flow meaning after state/generation are
    # durable, so use one fixed code without parsing or echoing the old text.
    connection.execute(
        "UPDATE message_deliveries SET last_error = ? WHERE last_error <> ''",
        (_LEGACY_RUNTIME_DIAGNOSTIC_CODE,),
    )


def _checkpoint_and_compact_runtime_diagnostic_storage(connection: sqlite3.Connection) -> None:
    """Make a completed v18 scrub absent from SQLite's main/WAL files too."""

    def require_truncated_wal() -> None:
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None or int(row[0]) != 0:
            raise RuntimeError("runtime diagnostic scrub WAL checkpoint did not complete")

    # The first checkpoint incorporates the redacted logical rows, VACUUM
    # rewrites the main database without their former cells, and the second
    # checkpoint prevents a new WAL from retaining an earlier page image.
    require_truncated_wal()
    connection.execute("VACUUM")
    require_truncated_wal()


def _backfill_goal_and_task_packets(connection: sqlite3.Connection) -> None:
    """Migrate only packet identities that can be reconstructed exactly.

    A historical Attempt in a database with multiple Goal revisions cannot be
    assigned to a revision by guessing. Such an upgrade fails closed and needs
    an explicit evidence-backed operator repair with the older release.
    """

    def packet_values(row: sqlite3.Row) -> tuple[str, str]:
        return (
            str(row["goal_packet_digest"] or ""),
            str(row["task_packet_digest"] or ""),
        )

    def packet_dependencies(raw_packet: object) -> list[str]:
        try:
            packet = json.loads(str(raw_packet or "{}"))
        except json.JSONDecodeError as error:
            raise RuntimeError("goal packet dependencies are invalid") from error
        dependencies = packet.get("dependencies", []) if isinstance(packet, dict) else []
        if dependencies not in ([], ["docker_api_ping"]):
            raise RuntimeError("goal packet dependencies are invalid")
        return list(dependencies)

    def packet_completion_contract(raw_packet: object) -> str:
        try:
            packet = json.loads(str(raw_packet or "{}"))
        except json.JSONDecodeError as error:
            raise RuntimeError("goal packet completion contract is invalid") from error
        if not isinstance(packet, dict):
            raise RuntimeError("goal packet completion contract is invalid")
        value = str(packet.get("completion_contract", "legacy_unclassified"))
        if value not in {
            "legacy_unclassified",
            "completion_required",
            "no_artifact_expected",
        }:
            raise RuntimeError("goal packet completion contract is invalid")
        return value

    def packet_managed_task_policy(
        raw_packet: object,
    ) -> dict[str, Any] | None:
        try:
            packet = json.loads(str(raw_packet or "{}"))
        except json.JSONDecodeError as error:
            raise RuntimeError("goal packet task policy is invalid") from error
        if not isinstance(packet, dict):
            raise RuntimeError("goal packet task policy is invalid")
        value = packet.get("managed_task_policy")
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise RuntimeError("goal packet task policy is invalid")
        binding = dict(value)
        task_policy = binding.get("task_policy")
        task_policy_valid = task_policy is None or (
            isinstance(task_policy, Mapping)
            and set(task_policy)
            == {
                "scope",
                "external_writes",
                "allowed_without_approval",
                "instruction_language",
                "do_not_generalize",
            }
            and all(
                isinstance(task_policy.get(field), str)
                for field in (
                    "scope",
                    "external_writes",
                    "allowed_without_approval",
                    "instruction_language",
                )
            )
            and isinstance(task_policy.get("do_not_generalize"), bool)
        )
        expected_policy_digest = f"sha256:{canonical_digest(task_policy)}"
        if (
            set(binding)
            != {
                "format",
                "catalog_target_id",
                "task_policy",
                "task_policy_digest",
            }
            or binding.get("format") != "cao-managed-worker-task-policy-binding/v1"
            or not isinstance(binding.get("catalog_target_id"), str)
            or not binding.get("catalog_target_id")
            or not task_policy_valid
            or binding.get("task_policy_digest") != expected_policy_digest
        ):
            raise RuntimeError("goal packet task policy is invalid")
        return binding

    def supervisor_attachment(work_item_id: str, goal_version: int) -> dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT a.id, a.principal_id, a.native_thread_id,
                   a.project_digest, g.supervisor_attachment_generation,
                   g.supervisor_runtime_session_id
            FROM work_items AS w
            JOIN cao_session_attachments AS a ON a.id = w.supervisor_attachment_id
            JOIN goal_revisions AS g
              ON g.work_item_id = w.id AND g.version = ?
            JOIN runtime_sessions AS supervisor_runtime
              ON supervisor_runtime.id = g.supervisor_runtime_session_id
             AND supervisor_runtime.principal_id = a.principal_id
            WHERE w.id = ?
              AND g.supervisor_attachment_generation IS NOT NULL
              AND g.supervisor_runtime_session_id IS NOT NULL
            """,
            (goal_version, work_item_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "attachment_id": str(row["id"]),
            "supervisor_id": str(row["principal_id"]),
            "runtime_session_id": str(row["supervisor_runtime_session_id"]),
            "native_thread_id": str(row["native_thread_id"]),
            "project_digest": str(row["project_digest"]),
            "generation": int(row["supervisor_attachment_generation"]),
        }

    def current_supervisor_attachment(work_item_id: str) -> dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT attachment.id, attachment.principal_id,
                   attachment.runtime_session_id, attachment.native_thread_id,
                   attachment.project_digest, attachment.generation
            FROM work_items AS work
            JOIN cao_session_attachments AS attachment
              ON attachment.id = work.supervisor_attachment_id
             AND attachment.principal_id = work.supervisor_id
            WHERE work.id = ?
            """,
            (work_item_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "attachment_id": str(row["id"]),
            "supervisor_id": str(row["principal_id"]),
            "runtime_session_id": str(row["runtime_session_id"]),
            "native_thread_id": str(row["native_thread_id"]),
            "project_digest": str(row["project_digest"]),
            "generation": int(row["generation"]),
        }

    def repair_unsubmitted_retry_attachment_drift(
        row: sqlite3.Row,
        *,
        goal: sqlite3.Row,
        canonical_binding: tuple[int, str, str],
    ) -> bool:
        """Repair the one exact pre-fix retry packet construction defect.

        Older retries used the replaceable CAO bridge runtime from the live
        attachment instead of the immutable attachment packet sealed into the
        Goal revision.  Only a latest, unsubmitted, CAO-owned failure with no
        Worker evidence or review lineage is eligible.  Every packet-bearing
        row is re-bound atomically and the repair is recorded as an event.
        """

        def service_digest(value: object) -> str:
            return hashlib.sha256(security_canonical_json(value).encode("utf-8")).hexdigest()

        actual_goal_digest, actual_task_digest = packet_values(row)
        if (
            int(row["attempt_number"]) < 2
            or str(row["state"]) != "waiting_supervisor"
            or actual_goal_digest != canonical_binding[1]
            or actual_task_digest == canonical_binding[2]
        ):
            return False
        try:
            goal_packet = json.loads(str(goal["packet_json"] or "{}"))
        except (TypeError, ValueError):
            return False
        sealed_attachment = (
            goal_packet.get("supervisor_attachment") if isinstance(goal_packet, Mapping) else None
        )
        live_attachment = current_supervisor_attachment(str(row["work_item_id"]))
        if (
            not isinstance(sealed_attachment, Mapping)
            or live_attachment is None
            or dict(sealed_attachment) == live_attachment
            or str(sealed_attachment.get("attachment_id") or "")
            != str(live_attachment["attachment_id"])
        ):
            return False
        buggy_packet = build_task_packet(
            goal_packet_digest_value=canonical_binding[1],
            work_item_id=str(row["work_item_id"]),
            goal_version=canonical_binding[0],
            attempt_id=str(row["id"]),
            attempt_number=int(row["attempt_number"]),
            worker_id=str(row["worker_id"]),
            runtime_session_id=(
                str(row["runtime_session_id"]) if row["runtime_session_id"] else None
            ),
            supervisor_attachment=live_attachment,
            dependencies=packet_dependencies(goal["packet_json"]),
        )
        if task_packet_digest(buggy_packet) != actual_task_digest:
            return False
        # ``work_items`` intentionally has no mutable current-attempt pointer;
        # prove latest ownership with the Attempt sequence instead.
        work = connection.execute(
            "SELECT state, attention_owner, supervisor_id, operator_scope "
            "FROM work_items WHERE id = ?",
            (row["work_item_id"],),
        ).fetchone()
        latest = connection.execute(
            "SELECT id FROM attempts WHERE work_item_id = ? ORDER BY attempt_number DESC LIMIT 1",
            (row["work_item_id"],),
        ).fetchone()
        assignment_proof = dict(row)
        assignment_proof["attempt_id"] = str(row["id"])
        if (
            work is None
            or latest is None
            or str(latest["id"]) != str(row["id"])
            or str(work["state"]) != "waiting_supervisor"
            or str(work["attention_owner"]) != "cao"
            or not _exact_assignment_not_submitted_is_proven(
                connection,
                assignment_proof,
            )
        ):
            return False
        boundary_rows = connection.execute(
            """
            SELECT boundary.*
            FROM boundaries AS boundary
            LEFT JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
            LEFT JOIN boundary_supersessions AS supersession
              ON supersession.boundary_id = boundary.id
            WHERE boundary.attempt_id = ?
              AND disposition.id IS NULL
              AND supersession.boundary_id IS NULL
            """,
            (row["id"],),
        ).fetchall()
        if (
            len(boundary_rows) != 1
            or str(boundary_rows[0]["kind"]) != "failure"
            or str(boundary_rows[0]["task_packet_digest"]) != actual_task_digest
            or int(
                connection.execute(
                    "SELECT COUNT(*) FROM boundaries WHERE attempt_id = ?",
                    (row["id"],),
                ).fetchone()[0]
            )
            != 1
        ):
            return False
        boundary = boundary_rows[0]
        unsafe = connection.execute(
            """
            SELECT 1
            WHERE EXISTS (
                SELECT 1 FROM messages
                WHERE attempt_id = ? AND kind NOT IN ('assignment', 'system')
            ) OR EXISTS (
                SELECT 1 FROM artifacts WHERE attempt_id = ?
            ) OR EXISTS (
                SELECT 1 FROM reviews WHERE attempt_id = ?
            ) OR EXISTS (
                SELECT 1 FROM reasoner_turns WHERE boundary_id = ?
            )
            """,
            (row["id"], row["id"], row["id"], boundary["id"]),
        ).fetchone()
        if unsafe is not None:
            return False
        messages = connection.execute(
            "SELECT * FROM messages WHERE attempt_id = ? ORDER BY sequence",
            (row["id"],),
        ).fetchall()
        if not messages or sum(str(message["kind"]) == "assignment" for message in messages) != 1:
            return False
        repaired_messages: list[tuple[str, str, str, str]] = []
        for message in messages:
            try:
                payload = json.loads(str(message["payload_json"] or "{}"))
            except (TypeError, ValueError):
                return False
            if (
                not isinstance(payload, dict)
                or service_digest(payload) != str(message["payload_digest"])
                or str(message["goal_packet_digest"]) != canonical_binding[1]
                or str(message["task_packet_digest"]) != actual_task_digest
                or payload.get("goal_packet_digest") != canonical_binding[1]
                or payload.get("task_packet_digest") != actual_task_digest
            ):
                return False
            recipients = connection.execute(
                "SELECT recipient_id FROM message_deliveries WHERE message_id = ? ORDER BY rowid",
                (message["id"],),
            ).fetchall()
            if len(recipients) != 1:
                return False
            recipient_ids = [str(recipients[0]["recipient_id"])]
            correlation_input = (
                ""
                if str(message["correlation_id"]) == str(message["id"])
                else str(message["correlation_id"])
            )

            message_digest_input = {
                "sender_id": str(message["sender_id"]),
                "recipient_ids": recipient_ids,
                "kind": str(message["kind"]),
                "payload": dict(payload),
                "work_item_id": str(message["work_item_id"]),
                "attempt_id": str(message["attempt_id"]),
                "correlation_id": correlation_input,
                "causation_id": str(message["causation_id"]),
                "goal_version": int(message["goal_version"]),
                "goal_packet_digest": canonical_binding[1],
                "task_packet_digest": actual_task_digest,
                "runtime_session_id": None,
            }
            if service_digest(message_digest_input) != str(message["message_digest"]):
                return False
            payload["task_packet_digest"] = canonical_binding[2]
            message_digest_input["payload"] = dict(payload)
            message_digest_input["task_packet_digest"] = canonical_binding[2]
            repaired_messages.append(
                (
                    security_canonical_json(payload),
                    service_digest(payload),
                    service_digest(message_digest_input),
                    str(message["id"]),
                )
            )
        try:
            boundary_metadata = json.loads(str(boundary["metadata_json"] or "{}"))
        except (TypeError, ValueError):
            return False
        if not isinstance(boundary_metadata, dict):
            return False
        boundary_input = {
            "source_event_id": str(boundary["source_event_id"]),
            "work_item_id": str(boundary["work_item_id"]),
            "attempt_id": str(boundary["attempt_id"]),
            "expected_goal_version": int(boundary["goal_version"]),
            "expected_goal_packet_digest": str(boundary["goal_packet_digest"]),
            "expected_task_packet_digest": canonical_binding[2],
            "expected_generation": int(boundary["generation"]),
            "kind": str(boundary["kind"]),
            "summary": str(boundary["summary"]),
            "runtime_state": str(boundary["runtime_state"]),
            "metadata": boundary_metadata,
        }
        connection.execute(
            "UPDATE attempts SET task_packet_digest = ? WHERE id = ?",
            (canonical_binding[2], row["id"]),
        )
        for payload_json, payload_digest, message_digest_value, message_id in repaired_messages:
            connection.execute(
                "UPDATE messages SET task_packet_digest = ?, payload_json = ?, "
                "payload_digest = ?, message_digest = ? WHERE id = ?",
                (
                    canonical_binding[2],
                    payload_json,
                    payload_digest,
                    message_digest_value,
                    message_id,
                ),
            )
        connection.execute(
            "UPDATE boundaries SET task_packet_digest = ?, input_digest = ? WHERE id = ?",
            (canonical_binding[2], service_digest(boundary_input), boundary["id"]),
        )
        connection.execute(
            """
            INSERT INTO events(
                id, event_type, aggregate_type, aggregate_id, operator_scope,
                actor_id, data_json, correlation_id, causation_id, created_at
            ) VALUES(?, 'attempt.task_packet_binding_repaired', 'attempt', ?, ?, ?, ?, '', ?, ?)
            """,
            (
                f"evt_{uuid.uuid4().hex}",
                row["id"],
                str(work["operator_scope"]),
                str(work["supervisor_id"]),
                security_canonical_json(
                    {
                        "reason": "retry_used_replaceable_supervisor_runtime",
                        "prior_task_packet_digest": actual_task_digest,
                        "task_packet_digest": canonical_binding[2],
                    }
                ),
                boundary["id"],
                utc_now(),
            ),
        )
        return True

    def has_partial_task_binding(row: sqlite3.Row) -> bool:
        goal_digest_value, task_digest_value = packet_values(row)
        return bool(goal_digest_value) != bool(task_digest_value)

    def require_task_binding(
        *,
        table: str,
        row: sqlite3.Row,
        expected: tuple[int, str, str],
        allow_missing_goal_version: bool = False,
    ) -> bool:
        """Return whether a complete, verified binding was already stored."""

        if has_partial_task_binding(row):
            raise RuntimeError(f"{table} has a partial packet binding")
        actual_goal_digest, actual_task_digest = packet_values(row)
        if not actual_goal_digest:
            if row["goal_version"] is None and allow_missing_goal_version:
                return False
            if row["goal_version"] is None or int(row["goal_version"]) != expected[0]:
                raise RuntimeError(f"{table} has an invalid unbound goal version")
            return False
        actual = (int(row["goal_version"]), actual_goal_digest, actual_task_digest)
        if actual != expected:
            raise RuntimeError(f"stored {table} packet binding is invalid")
        return True

    def canonical_attempt_binding(row: sqlite3.Row) -> tuple[int, str, str]:
        goal_version = int(row["goal_version"])
        goal = connection.execute(
            "SELECT packet_digest, packet_json FROM goal_revisions "
            "WHERE work_item_id = ? AND version = ?",
            (row["work_item_id"], goal_version),
        ).fetchone()
        if goal is None:
            raise RuntimeError("attempt goal packet binding cannot be reconstructed")
        goal_digest_value = str(goal["packet_digest"])
        task_packet = build_task_packet(
            goal_packet_digest_value=goal_digest_value,
            work_item_id=str(row["work_item_id"]),
            goal_version=goal_version,
            attempt_id=str(row["id"]),
            attempt_number=int(row["attempt_number"]),
            worker_id=str(row["worker_id"]),
            runtime_session_id=(
                str(row["runtime_session_id"]) if row["runtime_session_id"] else None
            ),
            supervisor_attachment=supervisor_attachment(str(row["work_item_id"]), goal_version),
            dependencies=packet_dependencies(goal["packet_json"]),
        )
        return goal_version, goal_digest_value, task_packet_digest(task_packet)

    goals = connection.execute(
        "SELECT * FROM goal_revisions ORDER BY work_item_id, version"
    ).fetchall()
    for row in goals:
        version = int(row["version"])
        expected_prior_version = version - 1 if version > 1 else None
        packet = build_goal_packet(
            work_item_id=str(row["work_item_id"]),
            version=version,
            title=str(row["title"]),
            objective=str(row["objective"]),
            maturity=str(row["maturity"]),
            acceptance=_json_value(row["acceptance_json"], []),
            non_goals=_json_value(row["non_goals_json"], []),
            priority=int(row["priority"]),
            requester_id=(str(row["requester_id"]) if row["requester_id"] else None),
            supervisor_id=(str(row["supervisor_id"]) if row["supervisor_id"] else None),
            metadata=_json_value(row["metadata_json"], {}),
            reason=str(row["reason"]),
            created_by=str(row["created_by"]),
            source_intent_id=(str(row["source_intent_id"]) if row["source_intent_id"] else None),
            source_directive_id=(
                str(row["source_directive_id"]) if row["source_directive_id"] else None
            ),
            correlation_id=str(row["correlation_id"] or ""),
            prior_version=expected_prior_version,
            completion_contract=packet_completion_contract(row["packet_json"]),
            supervisor_attachment=supervisor_attachment(str(row["work_item_id"]), version),
            dependencies=packet_dependencies(row["packet_json"]),
            managed_task_policy=packet_managed_task_policy(row["packet_json"]),
        )
        computed_digest = goal_packet_digest(packet)
        stored_digest = str(row["packet_digest"] or "")
        stored_packet_raw = str(row["packet_json"] or "")
        stored_packet = _json_value(stored_packet_raw, None)
        packet_is_default = stored_packet_raw in {"", "{}"}
        if stored_digest or not packet_is_default:
            if not stored_digest or packet_is_default:
                raise RuntimeError("stored goal packet binding is partial")
            stored_packet = _json_value(row["packet_json"], None)
            if (
                not isinstance(stored_packet, dict)
                or stored_packet != packet
                or computed_digest != stored_digest
                or row["prior_version"] != expected_prior_version
            ):
                raise RuntimeError("stored goal packet digest is invalid")
            continue
        connection.execute(
            """
            UPDATE goal_revisions
            SET packet_json = ?, packet_digest = ?, prior_version = ?
            WHERE work_item_id = ? AND version = ?
            """,
            (
                canonical_json(packet),
                computed_digest,
                expected_prior_version,
                row["work_item_id"],
                version,
            ),
        )

    attempts = connection.execute(
        "SELECT * FROM attempts ORDER BY work_item_id, attempt_number"
    ).fetchall()
    for row in attempts:
        if has_partial_task_binding(row):
            raise RuntimeError("attempt has a partial packet binding")
        if row["goal_packet_digest"]:
            expected = canonical_attempt_binding(row)
            goal = connection.execute(
                "SELECT packet_digest, packet_json FROM goal_revisions "
                "WHERE work_item_id = ? AND version = ?",
                (row["work_item_id"], row["goal_version"]),
            ).fetchone()
            assert goal is not None
            stored_attempt_binding = (int(row["goal_version"]), *packet_values(row))
            if stored_attempt_binding != expected and repair_unsubmitted_retry_attachment_drift(
                row,
                goal=goal,
                canonical_binding=expected,
            ):
                row = connection.execute(
                    "SELECT * FROM attempts WHERE id = ?",
                    (row["id"],),
                ).fetchone()
                assert row is not None
            require_task_binding(table="attempt", row=row, expected=expected)
            continue
        versions = connection.execute(
            "SELECT version, packet_digest, packet_json FROM goal_revisions WHERE work_item_id = ?",
            (row["work_item_id"],),
        ).fetchall()
        if len(versions) != 1:
            raise RuntimeError(
                "historical attempt goal packet is ambiguous; operator repair is required"
            )
        goal_version = int(versions[0]["version"])
        goal_digest_value = str(versions[0]["packet_digest"])
        task_packet = build_task_packet(
            goal_packet_digest_value=goal_digest_value,
            work_item_id=str(row["work_item_id"]),
            goal_version=goal_version,
            attempt_id=str(row["id"]),
            attempt_number=int(row["attempt_number"]),
            worker_id=str(row["worker_id"]),
            runtime_session_id=(
                str(row["runtime_session_id"]) if row["runtime_session_id"] else None
            ),
            supervisor_attachment=supervisor_attachment(str(row["work_item_id"]), goal_version),
            dependencies=packet_dependencies(versions[0]["packet_json"]),
        )
        expected = (goal_version, goal_digest_value, task_packet_digest(task_packet))
        require_task_binding(table="attempt", row=row, expected=expected)
        connection.execute(
            """
            UPDATE attempts
            SET goal_version = ?, goal_packet_digest = ?, task_packet_digest = ?
            WHERE id = ?
            """,
            (
                goal_version,
                goal_digest_value,
                expected[2],
                row["id"],
            ),
        )

    for table in ("messages", "boundaries", "reviews"):
        rows = connection.execute(f"SELECT rowid AS packet_rowid, * FROM {table}").fetchall()
        for row in rows:
            attempt = None
            if row["attempt_id"]:
                attempt = connection.execute(
                    "SELECT * FROM attempts WHERE id = ?", (row["attempt_id"],)
                ).fetchone()
                if attempt is None:
                    raise RuntimeError(f"{table} references a missing attempt")
            if attempt is not None:
                expected = (
                    int(attempt["goal_version"]),
                    str(attempt["goal_packet_digest"]),
                    str(attempt["task_packet_digest"]),
                )
            elif table != "messages":
                raise RuntimeError(f"{table} packet binding requires an attempt")
            elif row["work_item_id"] is None:
                if row["goal_version"] is not None or any(packet_values(row)):
                    raise RuntimeError("unscoped message has a packet binding")
                continue
            else:
                if row["goal_version"] is not None:
                    goal_version = int(row["goal_version"])
                else:
                    versions = connection.execute(
                        "SELECT version FROM goal_revisions WHERE work_item_id = ?",
                        (row["work_item_id"],),
                    ).fetchall()
                    if len(versions) != 1:
                        raise RuntimeError(
                            "historical message goal packet is ambiguous; operator repair is required"
                        )
                    goal_version = int(versions[0]["version"])
                goal = connection.execute(
                    "SELECT packet_digest FROM goal_revisions WHERE work_item_id = ? AND version = ?",
                    (row["work_item_id"], goal_version),
                ).fetchone()
                if goal is None:
                    raise RuntimeError(f"{table} packet binding cannot be reconstructed")
                expected = (goal_version, str(goal["packet_digest"]), "")
            if attempt is not None and row["work_item_id"] != attempt["work_item_id"]:
                raise RuntimeError(f"{table} references an attempt from another work item")
            if require_task_binding(
                table=table,
                row=row,
                expected=expected,
                allow_missing_goal_version=(table == "messages" and attempt is None),
            ):
                continue
            connection.execute(
                f"UPDATE {table} SET goal_version = ?, goal_packet_digest = ?, "
                "task_packet_digest = ? WHERE rowid = ?",
                (
                    expected[0],
                    expected[1],
                    expected[2],
                    row["packet_rowid"],
                ),
            )

    reasoners = connection.execute("SELECT * FROM reasoner_turns").fetchall()
    for row in reasoners:
        boundary = connection.execute(
            "SELECT * FROM boundaries WHERE id = ?", (row["boundary_id"],)
        ).fetchone()
        if boundary is None:
            raise RuntimeError("reasoner turn packet binding cannot be reconstructed")
        expected = (
            int(boundary["goal_version"]),
            str(boundary["goal_packet_digest"]),
            str(boundary["task_packet_digest"]),
        )
        input_digest = canonical_digest(
            {
                "boundary_id": boundary["id"],
                "boundary_input_digest": boundary["input_digest"],
                "goal_packet_digest": boundary["goal_packet_digest"],
                "task_packet_digest": boundary["task_packet_digest"],
                "generation": row["generation"],
            }
        )
        stored_binding = require_task_binding(table="reasoner turn", row=row, expected=expected)
        stored_input_digest = str(row["input_digest"] or "")
        if stored_input_digest and stored_input_digest != input_digest:
            raise RuntimeError("stored reasoner turn input digest is invalid")
        result_digest = str(row["result_digest"] or "")
        if row["state"] == "completed":
            disposition = connection.execute(
                "SELECT id, request_digest FROM boundary_dispositions WHERE reasoner_turn_id = ?",
                (row["id"],),
            ).fetchone()
            if disposition is None:
                raise RuntimeError("completed reasoner turn has no disposition")
            expected_result_digest = canonical_digest(
                {
                    "boundary_id": boundary["id"],
                    "disposition_id": disposition["id"],
                    "request_digest": disposition["request_digest"],
                    "goal_packet_digest": boundary["goal_packet_digest"],
                    "task_packet_digest": boundary["task_packet_digest"],
                }
            )
            if result_digest and result_digest != expected_result_digest:
                raise RuntimeError("stored reasoner result digest is invalid")
            result_digest = expected_result_digest
        elif result_digest:
            raise RuntimeError("unfinished reasoner turn has a result digest")
        if stored_binding != bool(stored_input_digest):
            # Packet identity and its derived input evidence form one binding.
            raise RuntimeError("stored reasoner turn packet binding is partial")
        if not stored_binding and result_digest:
            raise RuntimeError("stored reasoner turn packet binding is partial")
        connection.execute(
            """
            UPDATE reasoner_turns SET goal_packet_digest = ?,
                task_packet_digest = ?, input_digest = ?, result_digest = ?
            WHERE id = ?
            """,
            (
                expected[1],
                expected[2],
                input_digest,
                result_digest,
                row["id"],
            ),
        )

    directives = connection.execute("SELECT * FROM directives").fetchall()
    for row in directives:
        expected_version = row["expected_goal_version"]
        stored_digest = str(row["expected_goal_packet_digest"] or "")
        if expected_version is None:
            if stored_digest:
                raise RuntimeError("unversioned directive has a goal packet binding")
            continue
        work_id = row["target_work_item_id"] or row["created_work_item_id"]
        if work_id is None:
            raise RuntimeError("directive goal packet binding cannot be reconstructed")
        goal = connection.execute(
            "SELECT packet_digest FROM goal_revisions WHERE work_item_id = ? AND version = ?",
            (work_id, expected_version),
        ).fetchone()
        if goal is None:
            raise RuntimeError("directive goal packet binding cannot be reconstructed")
        if stored_digest:
            if stored_digest != str(goal["packet_digest"]):
                raise RuntimeError("stored directive goal packet binding is invalid")
            continue
        connection.execute(
            "UPDATE directives SET expected_goal_packet_digest = ? WHERE id = ?",
            (goal["packet_digest"], row["id"]),
        )


def _validate_close_lifecycle_bindings(connection: sqlite3.Connection) -> None:
    """Fail upgrades closed unless every close row matches its canonical source.

    Requester decisions and close receipts are immutable evidence, not a cache
    of values that migration may silently trust. A missing or mismatched
    binding therefore requires explicit operator repair rather than a guessed
    backfill.
    """

    invalid_decision = connection.execute(
        """
        SELECT d.id
        FROM requester_decisions d
        WHERE NOT EXISTS (
            SELECT 1
            FROM work_items w
            JOIN attempts a ON a.id = d.attempt_id AND a.work_item_id = w.id
            JOIN reviews r ON r.id = d.review_id
                           AND r.work_item_id = w.id
                           AND r.attempt_id = a.id
            JOIN cao_session_attachments attachment
              ON attachment.id = d.supervisor_attachment_id
            WHERE w.id = d.work_item_id
              AND w.requester_id = d.requester_id
              AND w.supervisor_id = d.recorded_by
              AND w.supervisor_attachment_id = d.supervisor_attachment_id
              AND w.generation = d.work_generation
              AND attachment.principal_id = d.recorded_by
              -- The attachment row holds only the *current* conversation
              -- epoch.  A durable requester decision seals its own earlier
              -- epoch, and the same conversation may be renewed afterwards.
              -- Require a monotonic successor here; requiring equality would
              -- make a valid historic acceptance prevent daemon startup.
              AND attachment.generation >= d.supervisor_attachment_generation
              AND r.reviewer_role = 'cao'
              AND r.verdict = 'ok'
              AND r.supervisor_attachment_id = d.supervisor_attachment_id
              AND r.supervisor_attachment_generation <= d.supervisor_attachment_generation
              AND r.work_generation <= d.work_generation
              AND a.goal_version = d.goal_version
              AND a.goal_packet_digest = d.goal_packet_digest
              AND a.task_packet_digest = d.task_packet_digest
              AND r.goal_version = d.goal_version
              AND r.goal_packet_digest = d.goal_packet_digest
              AND r.task_packet_digest = d.task_packet_digest
        )
        LIMIT 1
        """
    ).fetchone()
    if invalid_decision is not None:
        raise RuntimeError("requester decision binding cannot be reconstructed from canonical rows")

    invalid_receipt = connection.execute(
        """
        SELECT c.id
        FROM work_close_receipts c
        WHERE c.cleanup_inventory_evidence_id = ''
           OR NOT EXISTS (
                SELECT 1
                FROM work_items w
                JOIN attempts a ON a.id = c.attempt_id AND a.work_item_id = w.id
                JOIN reviews r ON r.id = c.review_id
                               AND r.work_item_id = w.id
                               AND r.attempt_id = a.id
                JOIN requester_decisions d ON d.id = c.requester_decision_id
                                          AND d.work_item_id = w.id
                                          AND d.attempt_id = a.id
                                          AND d.review_id = r.id
                JOIN cao_session_attachments attachment
                  ON attachment.id = c.supervisor_attachment_id
                WHERE w.id = c.work_item_id
                  AND w.state = 'completed'
                  AND w.supervisor_id = c.closed_by
                  AND w.supervisor_attachment_id = c.supervisor_attachment_id
                  AND w.generation = c.work_generation
                  AND attachment.principal_id = c.closed_by
                  -- As for requester decisions above, a close receipt remains
                  -- valid after the same attached conversation later renews.
                  AND attachment.generation >= c.supervisor_attachment_generation
                  AND d.verdict = 'accepted'
                  AND d.recorded_by = c.closed_by
                  AND d.supervisor_attachment_id = c.supervisor_attachment_id
                  AND d.supervisor_attachment_generation <= c.supervisor_attachment_generation
                  AND d.work_generation = c.work_generation
                  AND a.goal_version = c.goal_version
                  AND a.goal_packet_digest = c.goal_packet_digest
                  AND a.task_packet_digest = c.task_packet_digest
                  AND r.goal_version = c.goal_version
                  AND r.goal_packet_digest = c.goal_packet_digest
                  AND r.task_packet_digest = c.task_packet_digest
                  AND d.goal_version = c.goal_version
                  AND d.goal_packet_digest = c.goal_packet_digest
                  AND d.task_packet_digest = c.task_packet_digest
           )
        LIMIT 1
        """
    ).fetchone()
    if invalid_receipt is not None:
        raise RuntimeError("work close receipt binding cannot be reconstructed from canonical rows")
    for receipt in connection.execute("SELECT * FROM work_close_receipts").fetchall():
        preparation_id = str(receipt["close_preparation_id"] or "")
        preparation = connection.execute(
            "SELECT * FROM work_close_preparations WHERE id = ? AND work_item_id = ?",
            (preparation_id, receipt["work_item_id"]),
        ).fetchone()
        try:
            artifacts = json.loads(str(receipt["artifacts_json"]))
            cleanup = json.loads(str(receipt["cleanup_json"]))
            prepared_artifacts = (
                json.loads(str(preparation["artifacts_json"])) if preparation else None
            )
            inventory = json.loads(str(preparation["inventory_json"])) if preparation else None
            if not isinstance(artifacts, list) or not isinstance(cleanup, list):
                raise ValueError("receipt JSON is not an array")
            if not isinstance(prepared_artifacts, list) or not isinstance(inventory, list):
                raise ValueError("preparation JSON is not an array")
            inventory_digest = hashlib.sha256(
                json.dumps(
                    inventory, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest()
            if inventory_digest != str(preparation["inventory_digest"]):
                raise ValueError("inventory digest mismatch")
            expected_cleanup = {
                (str(item["target_kind"]), str(item["target_fingerprint"]), str(item["action"]))
                for item in inventory
                if isinstance(item, dict) and item.get("coverage", "enumerated") == "enumerated"
            }
            actual_cleanup = {
                (str(item["target_kind"]), str(item["target_fingerprint"]), str(item["action"]))
                for item in cleanup
            }
            if len(actual_cleanup) != len(cleanup) or actual_cleanup != expected_cleanup:
                raise ValueError("cleanup inventory mismatch")
            if {str(item["artifact_id"]): str(item["digest"]) for item in artifacts} != {
                str(item["id"]): str(item["digest"]) for item in prepared_artifacts
            }:
                raise ValueError("artifact inventory mismatch")
            if receipt["cleanup_inventory_evidence_id"] != preparation_id:
                raise ValueError("receipt does not bind its preparation id")
            if str(preparation["supervisor_attachment_id"] or "") != str(
                receipt["supervisor_attachment_id"]
            ) or int(preparation["supervisor_attachment_generation"] or 0) != int(
                receipt["supervisor_attachment_generation"]
            ):
                raise ValueError("preparation attachment binding mismatch")
            plan = ClosePlan(
                work_item_id=str(receipt["work_item_id"]),
                attempt_id=str(receipt["attempt_id"]),
                review_id=str(receipt["review_id"]),
                requester_decision_id=str(receipt["requester_decision_id"]),
                expected_goal_version=int(receipt["goal_version"]),
                expected_goal_packet_digest=str(receipt["goal_packet_digest"]),
                expected_task_packet_digest=str(receipt["task_packet_digest"]),
                expected_generation=int(receipt["work_generation"]),
                retention_policy_evidence_id=str(receipt["retention_policy_evidence_id"]),
                artifact_manifest_evidence_id=str(receipt["artifact_manifest_evidence_id"]),
                cleanup_inventory_evidence_id=str(receipt["cleanup_inventory_evidence_id"]),
                artifacts=tuple(
                    ArtifactPreservation(
                        str(item["artifact_id"]), str(item["digest"]), str(item["evidence_id"])
                    )
                    for item in artifacts
                ),
                cleanup=tuple(
                    CleanupRecord(
                        CleanupTargetKind(item["target_kind"]),
                        str(item["target_fingerprint"]),
                        CleanupAction(item["action"]),
                        CleanupOutcome(item["outcome"]),
                        str(item["evidence_id"]),
                        str(item.get("effect_operation_id", "")),
                        str(item.get("destructive_authority_evidence_id", "")),
                    )
                    for item in cleanup
                ),
                close_preparation_id=preparation_id,
            )
            if plan.digest() != str(receipt["plan_digest"]):
                raise ValueError("plan digest mismatch")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise RuntimeError(
                "work close receipt semantic integrity cannot be reconstructed"
            ) from error


def _backfill_managed_worker_threads(connection: sqlite3.Connection) -> None:
    """Give every pre-v31 managed spec one stable logical thread and epoch.

    The spec state is the only migration authority.  Runtime and enrollment
    health are deliberately ignored: an enabled spec remains logically active,
    while every already stopped/revoked spec is retained as legacy history.
    """

    specs = connection.execute(
        "SELECT * FROM managed_worker_specs ORDER BY created_at, id"
    ).fetchall()
    for spec in specs:
        expected_state = "active" if str(spec["state"]) == "enabled" else "legacy_stopped"
        thread = connection.execute(
            "SELECT * FROM managed_worker_threads WHERE managed_spec_id = ?",
            (spec["id"],),
        ).fetchone()
        if thread is None:
            thread_id = f"mwt_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO managed_worker_threads(
                    id, managed_spec_id, state, generation,
                    created_at, updated_at, archived_at
                ) VALUES(?, ?, ?, 1, ?, ?, NULL)
                """,
                (
                    thread_id,
                    spec["id"],
                    expected_state,
                    spec["created_at"],
                    spec["updated_at"],
                ),
            )
            thread = connection.execute(
                "SELECT * FROM managed_worker_threads WHERE id = ?", (thread_id,)
            ).fetchone()
        if thread is None or int(thread["generation"]) != 1:
            raise RuntimeError("managed Worker thread backfill is inconsistent")
        if str(thread["state"]) != expected_state:
            raise RuntimeError("managed Worker thread backfill state is inconsistent")

        epochs = connection.execute(
            "SELECT * FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (thread["id"],),
        ).fetchall()
        if not epochs:
            retired_at = (
                None
                if expected_state == "active"
                else str(spec["stopped_at"] or spec["updated_at"])
            )
            connection.execute(
                """
                INSERT INTO managed_worker_thread_epochs(
                    id, thread_id, generation, runtime_session_id,
                    enrollment_id, created_at, retired_at
                ) VALUES(?, ?, 1, ?, ?, ?, ?)
                """,
                (
                    f"mwe_{uuid.uuid4().hex}",
                    thread["id"],
                    spec["runtime_session_id"],
                    spec["enrollment_id"],
                    spec["created_at"],
                    retired_at,
                ),
            )
            continue
        if len(epochs) != 1:
            raise RuntimeError("managed Worker thread backfill has multiple epochs")
        epoch = epochs[0]
        expected_retired = expected_state != "active"
        if (
            int(epoch["generation"]) != 1
            or str(epoch["runtime_session_id"]) != str(spec["runtime_session_id"])
            or str(epoch["enrollment_id"]) != str(spec["enrollment_id"])
            or (epoch["retired_at"] is not None) != expected_retired
        ):
            raise RuntimeError("managed Worker thread epoch backfill is inconsistent")


_WORK_THREAD_BINDING_COLUMNS = {
    "managed_worker_thread_id",
    "managed_worker_thread_generation",
}
_WORK_THREAD_BINDING_TRIGGERS = (
    "work_items_managed_thread_pair_insert",
    "work_items_managed_thread_pair_update",
    "work_items_managed_thread_exact_insert",
    "work_items_managed_thread_exact_update",
    "work_items_managed_thread_rebind_update",
    "attempts_managed_thread_exact_insert",
    "attempts_managed_thread_identity_immutable",
    "managed_worker_threads_nonterminal_work_update",
    "managed_worker_threads_nonterminal_work_delete",
)


def _work_thread_binding_candidates_sql() -> str:
    """Return the exact, latest-Attempt-only v35 Work binding relation."""

    return """
        latest_attempt_numbers AS (
            SELECT work_item_id, MAX(attempt_number) AS attempt_number
            FROM attempts
            GROUP BY work_item_id
        ),
        latest_attempts AS (
            SELECT attempt.*
            FROM attempts AS attempt
            JOIN latest_attempt_numbers AS latest
              ON latest.work_item_id = attempt.work_item_id
             AND latest.attempt_number = attempt.attempt_number
        ),
        candidate_rows AS (
            SELECT work.id AS work_item_id,
                   epoch.thread_id AS managed_worker_thread_id,
                   epoch.generation AS managed_worker_thread_generation
            FROM work_items AS work
            JOIN latest_attempts AS attempt
              ON attempt.work_item_id = work.id
             AND attempt.worker_id = work.assigned_worker_id
             AND attempt.runtime_session_id IS NOT NULL
            JOIN managed_worker_thread_epochs AS epoch
              ON epoch.runtime_session_id = attempt.runtime_session_id
            JOIN managed_worker_threads AS thread
              ON thread.id = epoch.thread_id
            JOIN managed_worker_specs AS spec
              ON spec.id = thread.managed_spec_id
             AND spec.principal_id = work.assigned_worker_id
            JOIN cao_session_attachments AS source_attachment
              ON source_attachment.id = spec.attachment_id
            JOIN cao_session_attachments AS supervisor_attachment
              ON supervisor_attachment.id = work.supervisor_attachment_id
             AND supervisor_attachment.principal_id = source_attachment.principal_id
             AND supervisor_attachment.project_scope_digest = source_attachment.project_scope_digest
        ),
        exact_bindings AS (
            SELECT work_item_id,
                   MIN(managed_worker_thread_id) AS managed_worker_thread_id,
                   MIN(managed_worker_thread_generation)
                       AS managed_worker_thread_generation
            FROM candidate_rows
            GROUP BY work_item_id
            HAVING COUNT(*) = 1
        )
    """


def _validate_and_backfill_work_thread_bindings(
    connection: sqlite3.Connection,
    *,
    backfill_missing: bool,
) -> None:
    """Validate every binding and optionally perform the one-time v35 backfill."""

    partial = connection.execute(
        """
        SELECT id FROM work_items
        WHERE (managed_worker_thread_id IS NULL)
              <> (managed_worker_thread_generation IS NULL)
        LIMIT 1
        """
    ).fetchone()
    if partial is not None:
        raise RuntimeError("work item has a partial managed Worker thread binding")

    invalid_generation = connection.execute(
        """
        SELECT id FROM work_items
        WHERE managed_worker_thread_generation IS NOT NULL
          AND managed_worker_thread_generation < 1
        LIMIT 1
        """
    ).fetchone()
    if invalid_generation is not None:
        raise RuntimeError("work item has an invalid managed Worker thread generation")

    candidates = _work_thread_binding_candidates_sql()
    conflict = connection.execute(
        f"""
        WITH {candidates}
        SELECT work.id
        FROM work_items AS work
        JOIN exact_bindings AS exact ON exact.work_item_id = work.id
        WHERE work.managed_worker_thread_id IS NOT NULL
          AND (
              work.managed_worker_thread_id
                  <> exact.managed_worker_thread_id
              OR work.managed_worker_thread_generation
                  <> exact.managed_worker_thread_generation
          )
        LIMIT 1
        """
    ).fetchone()
    if conflict is not None:
        raise RuntimeError("work item managed Worker thread binding conflicts with its Attempt")

    if not backfill_missing:
        return

    connection.execute(
        f"""
        WITH {candidates}
        UPDATE work_items
        SET managed_worker_thread_id = (
                SELECT exact.managed_worker_thread_id
                FROM exact_bindings AS exact
                WHERE exact.work_item_id = work_items.id
            ),
            managed_worker_thread_generation = (
                SELECT exact.managed_worker_thread_generation
                FROM exact_bindings AS exact
                WHERE exact.work_item_id = work_items.id
            )
        WHERE managed_worker_thread_id IS NULL
          AND managed_worker_thread_generation IS NULL
          AND EXISTS (
              SELECT 1 FROM exact_bindings AS exact
              WHERE exact.work_item_id = work_items.id
          )
        """
    )


def _install_work_thread_binding_triggers(connection: sqlite3.Connection) -> None:
    """Keep logical Work authority exact without consulting runtime health."""

    for trigger in _WORK_THREAD_BINDING_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")

    connection.execute(
        """
        CREATE TRIGGER work_items_managed_thread_pair_insert
        BEFORE INSERT ON work_items
        FOR EACH ROW
        WHEN (NEW.managed_worker_thread_id IS NULL)
                  <> (NEW.managed_worker_thread_generation IS NULL)
          OR NEW.managed_worker_thread_generation < 1
        BEGIN
            SELECT RAISE(ABORT, 'invalid managed Worker thread binding pair');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_managed_thread_pair_update
        BEFORE UPDATE OF managed_worker_thread_id,
                         managed_worker_thread_generation ON work_items
        FOR EACH ROW
        WHEN (NEW.managed_worker_thread_id IS NULL)
                  <> (NEW.managed_worker_thread_generation IS NULL)
          OR NEW.managed_worker_thread_generation < 1
        BEGIN
            SELECT RAISE(ABORT, 'invalid managed Worker thread binding pair');
        END
        """
    )

    exact_lane = """
        EXISTS (
            SELECT 1
            FROM managed_worker_threads AS thread
            JOIN managed_worker_specs AS spec
              ON spec.id = thread.managed_spec_id
            JOIN cao_session_attachments AS source_attachment
              ON source_attachment.id = spec.attachment_id
            JOIN cao_session_attachments AS supervisor_attachment
              ON supervisor_attachment.id = NEW.supervisor_attachment_id
             AND supervisor_attachment.principal_id = source_attachment.principal_id
             AND supervisor_attachment.project_scope_digest = source_attachment.project_scope_digest
            JOIN managed_worker_thread_epochs AS epoch
              ON epoch.thread_id = thread.id
             AND epoch.generation = thread.generation
             AND epoch.runtime_session_id = spec.runtime_session_id
             AND epoch.enrollment_id = spec.enrollment_id
             AND epoch.retired_at IS NULL
            WHERE thread.id = NEW.managed_worker_thread_id
              AND thread.generation = NEW.managed_worker_thread_generation
              AND thread.state = 'active'
              AND spec.state = 'enabled'
              AND spec.principal_id = NEW.assigned_worker_id
        )
    """
    for suffix, event in (
        ("insert", "INSERT"),
        (
            "update",
            "UPDATE OF assigned_worker_id, supervisor_attachment_id, "
            "managed_worker_thread_id, managed_worker_thread_generation",
        ),
    ):
        connection.execute(
            f"""
            CREATE TRIGGER work_items_managed_thread_exact_{suffix}
            BEFORE {event} ON work_items
            FOR EACH ROW
            WHEN (
                NEW.managed_worker_thread_id IS NOT NULL
                OR EXISTS (
                    SELECT 1 FROM managed_worker_specs
                    WHERE principal_id = NEW.assigned_worker_id
                )
            )
            AND NOT ({exact_lane})
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'managed Work requires an exact active Worker thread generation'
                );
            END
            """
        )

    connection.execute(
        """
        CREATE TRIGGER work_items_managed_thread_rebind_update
        BEFORE UPDATE OF assigned_worker_id, managed_worker_thread_id,
                         managed_worker_thread_generation ON work_items
        FOR EACH ROW
        WHEN (
            NEW.assigned_worker_id = OLD.assigned_worker_id
            AND (
                NEW.managed_worker_thread_id IS NOT OLD.managed_worker_thread_id
                OR NEW.managed_worker_thread_generation
                    IS NOT OLD.managed_worker_thread_generation
            )
        ) OR (
            OLD.managed_worker_thread_id IS NOT NULL
            AND NEW.managed_worker_thread_id IS OLD.managed_worker_thread_id
            AND NEW.managed_worker_thread_generation
                IS NOT OLD.managed_worker_thread_generation
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'managed Work cannot change its logical thread generation'
            );
        END
        """
    )

    connection.execute(
        """
        CREATE TRIGGER attempts_managed_thread_exact_insert
        BEFORE INSERT ON attempts
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1
            FROM work_items AS work
            WHERE work.id = NEW.work_item_id
              AND (
                  NEW.worker_id IS NOT work.assigned_worker_id
                  OR
                  (
                      work.managed_worker_thread_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM managed_worker_thread_epochs AS epoch
                          JOIN managed_worker_threads AS thread
                            ON thread.id = epoch.thread_id
                          JOIN managed_worker_specs AS spec
                            ON spec.id = thread.managed_spec_id
                          JOIN cao_session_attachments AS source_attachment
                            ON source_attachment.id = spec.attachment_id
                          JOIN cao_session_attachments AS supervisor_attachment
                            ON supervisor_attachment.id = work.supervisor_attachment_id
                           AND supervisor_attachment.principal_id =
                               source_attachment.principal_id
                           AND supervisor_attachment.project_scope_digest =
                               source_attachment.project_scope_digest
                          WHERE epoch.runtime_session_id = NEW.runtime_session_id
                            AND epoch.thread_id = work.managed_worker_thread_id
                            AND epoch.generation =
                                work.managed_worker_thread_generation
                            AND epoch.retired_at IS NULL
                            AND thread.state = 'active'
                            AND thread.generation =
                                work.managed_worker_thread_generation
                            AND spec.state = 'enabled'
                            AND spec.runtime_session_id = epoch.runtime_session_id
                            AND spec.enrollment_id = epoch.enrollment_id
                            AND spec.principal_id = work.assigned_worker_id
                            AND spec.principal_id = NEW.worker_id
                      )
                  ) OR (
                      work.managed_worker_thread_id IS NULL
                      AND (
                          EXISTS (
                              SELECT 1 FROM managed_worker_specs AS spec
                              WHERE spec.principal_id = work.assigned_worker_id
                          )
                          OR EXISTS (
                              SELECT 1 FROM managed_worker_thread_epochs AS epoch
                              WHERE epoch.runtime_session_id = NEW.runtime_session_id
                          )
                      )
                  )
              )
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'Attempt worker or runtime does not match the Work binding'
            );
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER attempts_managed_thread_identity_immutable
        BEFORE UPDATE OF work_item_id, worker_id, runtime_session_id ON attempts
        FOR EACH ROW
        WHEN NEW.work_item_id IS NOT OLD.work_item_id
          OR NEW.worker_id IS NOT OLD.worker_id
          OR NEW.runtime_session_id IS NOT OLD.runtime_session_id
        BEGIN
            SELECT RAISE(ABORT, 'Attempt Work and runtime binding is immutable');
        END
        """
    )

    unresolved_work = """
        EXISTS (
            SELECT 1
            FROM work_items AS work
            JOIN managed_worker_specs AS spec
              ON spec.principal_id = work.assigned_worker_id
             AND spec.id = OLD.managed_spec_id
            WHERE work.state NOT IN ('completed', 'canceled', 'failed')
              AND (
                  work.managed_worker_thread_id = OLD.id
                  OR (
                      work.managed_worker_thread_id IS NULL
                      AND work.managed_worker_thread_generation IS NULL
                  )
              )
        )
    """
    connection.execute(
        f"""
        CREATE TRIGGER managed_worker_threads_nonterminal_work_update
        BEFORE UPDATE OF state, generation ON managed_worker_threads
        FOR EACH ROW
        WHEN (
            NEW.state IS NOT OLD.state
            OR NEW.generation IS NOT OLD.generation
        ) AND {unresolved_work}
        BEGIN
            SELECT RAISE(
                ABORT,
                'managed Worker lifecycle has nonterminal Work'
            );
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER managed_worker_threads_nonterminal_work_delete
        BEFORE DELETE ON managed_worker_threads
        FOR EACH ROW
        WHEN {unresolved_work}
        BEGIN
            SELECT RAISE(
                ABORT,
                'managed Worker lifecycle has nonterminal Work'
            );
        END
        """
    )


def _ensure_work_thread_binding_schema(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Install schema36 atomically, rejecting a preexisting half migration."""

    columns = {
        str(column["name"]) for column in connection.execute("PRAGMA table_info(work_items)")
    }
    present = columns & _WORK_THREAD_BINDING_COLUMNS
    if present and present != _WORK_THREAD_BINDING_COLUMNS:
        raise RuntimeError("work item managed Worker thread binding schema is partial")
    pair_was_added = not present
    if not present:
        connection.execute("ALTER TABLE work_items ADD COLUMN managed_worker_thread_id TEXT")
        connection.execute(
            "ALTER TABLE work_items ADD COLUMN managed_worker_thread_generation "
            "INTEGER CHECK(managed_worker_thread_generation >= 1)"
        )

    _validate_and_backfill_work_thread_bindings(
        connection,
        # The unreleased pre-pair schema36 build needs the same one-time
        # migration as v35. Once both columns exist, a schema36 NULL pair is a
        # durable unresolved-authority decision and later Attempts cannot make
        # it acquire a lifecycle generation.
        backfill_missing=pair_was_added or existing_schema_version < 36,
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS work_items_managed_thread_generation_state_idx
        ON work_items(
            managed_worker_thread_id,
            managed_worker_thread_generation,
            state,
            updated_at DESC
        )
        WHERE managed_worker_thread_id IS NOT NULL
          AND managed_worker_thread_generation IS NOT NULL
        """
    )
    _install_work_thread_binding_triggers(connection)


_DELIVERY_ATTACHMENT_COLUMN = "recipient_attachment_id"
_DELIVERY_ATTACHMENT_TRIGGERS = (
    "message_deliveries_attachment_exact_insert",
    "message_deliveries_identity_immutable_update",
    "message_deliveries_attachment_immutable_update",
)


def _install_delivery_attachment_triggers(connection: sqlite3.Connection) -> None:
    for trigger in _DELIVERY_ATTACHMENT_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
    connection.execute(
        """
        CREATE TRIGGER message_deliveries_attachment_exact_insert
        BEFORE INSERT ON message_deliveries
        FOR EACH ROW
        WHEN (
            (
                NEW.recipient_attachment_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM cao_session_attachments AS attachment
                    JOIN principals AS recipient ON recipient.id = NEW.recipient_id
                    WHERE attachment.id = NEW.recipient_attachment_id
                      AND attachment.principal_id = NEW.recipient_id
                      AND recipient.role = 'cao'
                )
            )
            OR EXISTS (
                SELECT 1
                FROM messages AS message
                JOIN work_items AS work ON work.id = message.work_item_id
                JOIN principals AS recipient ON recipient.id = NEW.recipient_id
                WHERE message.id = NEW.message_id
                  AND recipient.role = 'cao'
                  AND NEW.recipient_attachment_id IS NOT work.supervisor_attachment_id
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'message delivery attachment binding is invalid');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER message_deliveries_identity_immutable_update
        BEFORE UPDATE OF message_id, recipient_id
        ON message_deliveries
        FOR EACH ROW
        WHEN NEW.message_id IS NOT OLD.message_id
          OR NEW.recipient_id IS NOT OLD.recipient_id
        BEGIN
            SELECT RAISE(ABORT, 'message delivery identity is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER message_deliveries_attachment_immutable_update
        BEFORE UPDATE OF recipient_attachment_id ON message_deliveries
        FOR EACH ROW
        WHEN NEW.recipient_attachment_id IS NOT OLD.recipient_attachment_id
        BEGIN
            SELECT RAISE(ABORT, 'message delivery attachment binding is immutable');
        END
        """
    )


def _ensure_delivery_attachment_schema(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Install the durable CAO attachment delivery lane exactly once.

    A CAO runtime is a replaceable wake route.  The Work's immutable
    supervisor attachment is the authority and inbox-ordering lane.  Legacy
    rows are backfilled only during the v36->v37 cutover (or when recovering
    an unreleased schema37 build that did not yet contain the column); a NULL
    binding that survives that cutover remains explicitly unresolved.
    """

    columns = {
        str(column["name"])
        for column in connection.execute("PRAGMA table_info(message_deliveries)")
    }
    column_was_added = _DELIVERY_ATTACHMENT_COLUMN not in columns
    if column_was_added:
        connection.execute(
            "ALTER TABLE message_deliveries ADD COLUMN recipient_attachment_id TEXT "
            "REFERENCES cao_session_attachments(id) ON DELETE RESTRICT"
        )

    conflict = connection.execute(
        """
        SELECT delivery.message_id, delivery.recipient_id
        FROM message_deliveries AS delivery
        WHERE delivery.recipient_attachment_id IS NOT NULL
          AND (
            NOT EXISTS (
                SELECT 1
                FROM cao_session_attachments AS attachment
                JOIN principals AS recipient ON recipient.id = delivery.recipient_id
                WHERE attachment.id = delivery.recipient_attachment_id
                  AND attachment.principal_id = delivery.recipient_id
                  AND recipient.role = 'cao'
            )
            OR EXISTS (
                SELECT 1
                FROM messages AS message
                JOIN work_items AS work ON work.id = message.work_item_id
                JOIN principals AS recipient ON recipient.id = delivery.recipient_id
                WHERE message.id = delivery.message_id
                  AND recipient.role = 'cao'
                  AND delivery.recipient_attachment_id
                      IS NOT work.supervisor_attachment_id
            )
          )
        LIMIT 1
        """
    ).fetchone()
    if conflict is not None:
        raise RuntimeError("database contains a conflicting CAO delivery attachment binding")

    if column_was_added or existing_schema_version < 37:
        connection.execute(
            """
            UPDATE message_deliveries AS delivery
            SET recipient_attachment_id = (
                SELECT work.supervisor_attachment_id
                FROM messages AS message
                JOIN work_items AS work ON work.id = message.work_item_id
                JOIN principals AS recipient ON recipient.id = delivery.recipient_id
                JOIN cao_session_attachments AS attachment
                  ON attachment.id = work.supervisor_attachment_id
                 AND attachment.principal_id = delivery.recipient_id
                WHERE message.id = delivery.message_id
                  AND recipient.role = 'cao'
            )
            WHERE delivery.recipient_attachment_id IS NULL
              AND EXISTS (
                SELECT 1
                FROM messages AS message
                JOIN work_items AS work ON work.id = message.work_item_id
                JOIN principals AS recipient ON recipient.id = delivery.recipient_id
                JOIN cao_session_attachments AS attachment
                  ON attachment.id = work.supervisor_attachment_id
                 AND attachment.principal_id = delivery.recipient_id
                WHERE message.id = delivery.message_id
                  AND recipient.role = 'cao'
              )
            """
        )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS message_deliveries_attachment_state_idx
        ON message_deliveries(recipient_attachment_id, state, updated_at)
        WHERE recipient_attachment_id IS NOT NULL
        """
    )
    _install_delivery_attachment_triggers(connection)


_USER_NEEDED_BOUNDARY_COLUMN = "user_needed_boundary_id"
_BOUNDARY_CONTINUATION_COLUMNS = {
    "boundary_id",
    "work_item_id",
    "source_attempt_id",
    "source_generation",
    "successor_attempt_id",
    "successor_generation",
    "message_id",
    "decided_by",
    "created_at",
}
_BOUNDARY_CONTINUATION_TRIGGERS = (
    "work_items_user_needed_contract_insert",
    "work_items_user_needed_contract_update",
    "work_items_user_needed_boundary_exact_update",
    "work_items_user_needed_boundary_legacy_claim_update",
    "work_items_user_needed_boundary_replace_update",
    "work_items_user_needed_boundary_clear_update",
    "boundary_continuations_exact_insert",
    "boundary_continuations_immutable_update",
    "boundary_continuations_immutable_delete",
    "boundary_continuation_messages_immutable_update",
    "boundary_continuation_deliveries_exact_insert",
    "boundary_continuation_deliveries_identity_update",
    "boundary_continuation_deliveries_immutable_delete",
)


def _user_needed_boundary_candidates_sql() -> str:
    """Return exact current WAIT_USER candidates without JSON or event inference."""

    return """
        latest_attempt_numbers AS (
            SELECT work_item_id, MAX(attempt_number) AS attempt_number
            FROM attempts
            GROUP BY work_item_id
        ),
        latest_attempts AS (
            SELECT attempt.*
            FROM attempts AS attempt
            JOIN latest_attempt_numbers AS latest
              ON latest.work_item_id = attempt.work_item_id
             AND latest.attempt_number = attempt.attempt_number
        ),
        candidate_rows AS (
            SELECT work.id AS work_item_id,
                   boundary.id AS boundary_id
            FROM work_items AS work
            JOIN latest_attempts AS attempt
              ON attempt.work_item_id = work.id
             AND attempt.worker_id = work.assigned_worker_id
             AND attempt.state = 'input_required'
            JOIN boundaries AS boundary
              ON boundary.work_item_id = work.id
             AND boundary.attempt_id = attempt.id
             AND boundary.generation = work.generation
             AND boundary.goal_version = attempt.goal_version
             AND boundary.goal_packet_digest = attempt.goal_packet_digest
             AND boundary.task_packet_digest = attempt.task_packet_digest
             AND boundary.source_principal_id = attempt.worker_id
            JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
             AND disposition.kind = 'wait_user'
             AND disposition.generation = work.generation
             AND disposition.decided_by = work.supervisor_id
            JOIN reasoner_turns AS turn
              ON turn.id = disposition.reasoner_turn_id
             AND turn.boundary_id = boundary.id
             AND turn.work_item_id = work.id
             AND turn.supervisor_id = disposition.decided_by
             AND turn.generation = work.generation
             AND turn.goal_version = boundary.goal_version
             AND turn.goal_packet_digest = boundary.goal_packet_digest
             AND turn.task_packet_digest = boundary.task_packet_digest
            LEFT JOIN boundary_supersessions AS supersession
              ON supersession.boundary_id = boundary.id
            LEFT JOIN boundary_continuations AS continuation
              ON continuation.boundary_id = boundary.id
            WHERE work.state = 'user_needed'
              AND work.attention_owner = 'user'
              AND boundary.goal_version = work.goal_version
              AND supersession.boundary_id IS NULL
              AND continuation.boundary_id IS NULL
        ),
        exact_candidates AS (
            SELECT work_item_id, MIN(boundary_id) AS boundary_id
            FROM candidate_rows
            GROUP BY work_item_id
            HAVING COUNT(*) = 1
        )
    """


def _validate_and_backfill_user_needed_boundaries(
    connection: sqlite3.Connection,
    *,
    backfill_missing: bool,
) -> None:
    """Validate typed WAIT_USER authority and optionally backfill exact v35 state."""

    invalid_pointer = connection.execute(
        """
        SELECT work.id
        FROM work_items AS work
        LEFT JOIN boundaries AS boundary
          ON boundary.id = work.user_needed_boundary_id
        LEFT JOIN attempts AS attempt
          ON attempt.id = boundary.attempt_id
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN reasoner_turns AS turn
          ON turn.id = disposition.reasoner_turn_id
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        LEFT JOIN boundary_continuations AS continuation
          ON continuation.boundary_id = boundary.id
        WHERE work.user_needed_boundary_id IS NOT NULL
          AND (
              work.state <> 'user_needed'
              OR work.attention_owner <> 'user'
              OR boundary.id IS NULL
              OR boundary.work_item_id <> work.id
              OR boundary.generation <> work.generation
              OR boundary.goal_version <> work.goal_version
              OR attempt.id IS NULL
              OR attempt.work_item_id <> work.id
              OR attempt.worker_id <> work.assigned_worker_id
              OR attempt.state <> 'input_required'
              OR attempt.goal_version <> boundary.goal_version
              OR attempt.goal_packet_digest <> boundary.goal_packet_digest
              OR attempt.task_packet_digest <> boundary.task_packet_digest
              OR boundary.source_principal_id <> attempt.worker_id
              OR disposition.boundary_id IS NULL
              OR disposition.kind <> 'wait_user'
              OR disposition.generation <> work.generation
              OR disposition.decided_by <> work.supervisor_id
              OR turn.id IS NULL
              OR turn.boundary_id <> boundary.id
              OR turn.work_item_id <> work.id
              OR turn.supervisor_id <> disposition.decided_by
              OR turn.generation <> work.generation
              OR turn.goal_version <> boundary.goal_version
              OR turn.goal_packet_digest <> boundary.goal_packet_digest
              OR turn.task_packet_digest <> boundary.task_packet_digest
              OR supersession.boundary_id IS NOT NULL
              OR continuation.boundary_id IS NOT NULL
              OR EXISTS (
                  SELECT 1 FROM attempts AS later
                  WHERE later.work_item_id = work.id
                    AND later.attempt_number > attempt.attempt_number
              )
          )
        LIMIT 1
        """
    ).fetchone()
    if invalid_pointer is not None:
        raise RuntimeError("Work has an invalid current WAIT_USER boundary binding")

    invalid_continuation = connection.execute(
        """
        SELECT continuation.boundary_id
        FROM boundary_continuations AS continuation
        LEFT JOIN work_items AS work
          ON work.id = continuation.work_item_id
        LEFT JOIN boundaries AS boundary
          ON boundary.id = continuation.boundary_id
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN reasoner_turns AS turn
          ON turn.id = disposition.reasoner_turn_id
        LEFT JOIN attempts AS source_attempt
          ON source_attempt.id = continuation.source_attempt_id
        LEFT JOIN attempts AS successor_attempt
          ON successor_attempt.id = continuation.successor_attempt_id
        LEFT JOIN messages AS message
          ON message.id = continuation.message_id
        LEFT JOIN message_deliveries AS delivery
          ON delivery.message_id = message.id
         AND delivery.recipient_id = successor_attempt.worker_id
        LEFT JOIN principals AS decider
          ON decider.id = continuation.decided_by
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        WHERE work.id IS NULL
           OR boundary.id IS NULL
           OR boundary.work_item_id <> continuation.work_item_id
           OR work.supervisor_id <> continuation.decided_by
           OR boundary.attempt_id <> continuation.source_attempt_id
           OR boundary.generation <> continuation.source_generation
           OR disposition.boundary_id IS NULL
           OR disposition.kind <> 'wait_user'
           OR disposition.generation <> continuation.source_generation
           OR disposition.decided_by <> continuation.decided_by
           OR turn.id IS NULL
           OR turn.boundary_id <> boundary.id
           OR turn.work_item_id <> continuation.work_item_id
           OR turn.supervisor_id <> continuation.decided_by
           OR turn.generation <> continuation.source_generation
           OR turn.goal_version <> boundary.goal_version
           OR turn.goal_packet_digest <> boundary.goal_packet_digest
           OR turn.task_packet_digest <> boundary.task_packet_digest
           OR source_attempt.id IS NULL
           OR source_attempt.work_item_id <> continuation.work_item_id
           OR source_attempt.id <> boundary.attempt_id
           OR source_attempt.worker_id <> boundary.source_principal_id
           OR source_attempt.goal_version <> boundary.goal_version
           OR source_attempt.goal_packet_digest <> boundary.goal_packet_digest
           OR source_attempt.task_packet_digest <> boundary.task_packet_digest
           OR successor_attempt.id IS NULL
           OR successor_attempt.work_item_id <> continuation.work_item_id
           OR decider.id IS NULL
           OR decider.role <> 'cao'
           OR supersession.boundary_id IS NOT NULL
           OR NOT (
                (
                    continuation.successor_attempt_id = continuation.source_attempt_id
                    AND continuation.successor_generation
                        = continuation.source_generation
                ) OR (
                    continuation.successor_attempt_id
                        <> continuation.source_attempt_id
                    AND successor_attempt.attempt_number
                        = source_attempt.attempt_number + 1
                    AND continuation.successor_generation
                        = continuation.source_generation + 1
                )
           )
           OR message.id IS NULL
           OR message.work_item_id <> continuation.work_item_id
           OR message.attempt_id <> continuation.successor_attempt_id
           OR message.sender_id <> continuation.decided_by
           OR message.goal_version <> successor_attempt.goal_version
           OR message.goal_packet_digest <> successor_attempt.goal_packet_digest
           OR message.task_packet_digest <> successor_attempt.task_packet_digest
           OR delivery.message_id IS NULL
           OR EXISTS (
                SELECT 1 FROM message_deliveries AS other_delivery
                WHERE other_delivery.message_id = message.id
                  AND other_delivery.recipient_id <> successor_attempt.worker_id
           )
           OR (
                continuation.successor_attempt_id = continuation.source_attempt_id
                AND message.kind <> 'instruction'
           )
           OR (
                continuation.successor_attempt_id <> continuation.source_attempt_id
                AND message.kind <> 'assignment'
           )
        LIMIT 1
        """
    ).fetchone()
    if invalid_continuation is not None:
        raise RuntimeError("boundary continuation has an invalid exact binding")

    if not backfill_missing:
        return

    candidates = _user_needed_boundary_candidates_sql()
    connection.execute(
        f"""
        WITH {candidates}
        UPDATE work_items
        SET user_needed_boundary_id = (
            SELECT exact.boundary_id
            FROM exact_candidates AS exact
            WHERE exact.work_item_id = work_items.id
        )
        WHERE user_needed_boundary_id IS NULL
          AND EXISTS (
              SELECT 1 FROM exact_candidates AS exact
              WHERE exact.work_item_id = work_items.id
          )
        """
    )


def _install_boundary_continuation_triggers(connection: sqlite3.Connection) -> None:
    """Enforce current WAIT_USER ownership and append-only exact consumption."""

    for trigger in _BOUNDARY_CONTINUATION_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")

    invariant = """
        (NEW.user_needed_boundary_id IS NOT NULL AND (
            NEW.state <> 'user_needed' OR NEW.attention_owner <> 'user'
        )) OR (
            NEW.state = 'user_needed' AND (
                NEW.attention_owner <> 'user'
                OR NEW.user_needed_boundary_id IS NULL
            )
        )
    """
    connection.execute(
        f"""
        CREATE TRIGGER work_items_user_needed_contract_insert
        BEFORE INSERT ON work_items
        FOR EACH ROW WHEN {invariant}
        BEGIN
            SELECT RAISE(ABORT, 'invalid Work WAIT_USER current binding');
        END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER work_items_user_needed_contract_update
        BEFORE UPDATE OF state, attention_owner, user_needed_boundary_id ON work_items
        FOR EACH ROW WHEN {invariant}
        BEGIN
            SELECT RAISE(ABORT, 'invalid Work WAIT_USER current binding');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_user_needed_boundary_exact_update
        BEFORE UPDATE OF user_needed_boundary_id ON work_items
        FOR EACH ROW
        WHEN NEW.user_needed_boundary_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM boundaries AS boundary
              JOIN attempts AS attempt
                ON attempt.id = boundary.attempt_id
               AND attempt.work_item_id = NEW.id
              JOIN boundary_dispositions AS disposition
                ON disposition.boundary_id = boundary.id
               AND disposition.kind = 'wait_user'
              JOIN reasoner_turns AS turn
                ON turn.id = disposition.reasoner_turn_id
               AND turn.boundary_id = boundary.id
               AND turn.work_item_id = NEW.id
              LEFT JOIN boundary_supersessions AS supersession
                ON supersession.boundary_id = boundary.id
              LEFT JOIN boundary_continuations AS continuation
                ON continuation.boundary_id = boundary.id
              WHERE boundary.id = NEW.user_needed_boundary_id
                AND boundary.work_item_id = NEW.id
                AND boundary.generation = NEW.generation
                AND boundary.goal_version = NEW.goal_version
                AND boundary.source_principal_id = attempt.worker_id
                AND attempt.worker_id = NEW.assigned_worker_id
                AND attempt.state = 'input_required'
                AND attempt.goal_version = boundary.goal_version
                AND attempt.goal_packet_digest = boundary.goal_packet_digest
                AND attempt.task_packet_digest = boundary.task_packet_digest
                AND disposition.generation = NEW.generation
                AND disposition.decided_by = NEW.supervisor_id
                AND turn.supervisor_id = disposition.decided_by
                AND turn.generation = NEW.generation
                AND turn.goal_version = boundary.goal_version
                AND turn.goal_packet_digest = boundary.goal_packet_digest
                AND turn.task_packet_digest = boundary.task_packet_digest
                AND supersession.boundary_id IS NULL
                AND continuation.boundary_id IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM attempts AS later
                    WHERE later.work_item_id = NEW.id
                      AND later.attempt_number > attempt.attempt_number
                )
          )
        BEGIN
            SELECT RAISE(ABORT, 'Work WAIT_USER boundary exact binding violation');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_user_needed_boundary_legacy_claim_update
        BEFORE UPDATE OF user_needed_boundary_id ON work_items
        FOR EACH ROW
        WHEN OLD.user_needed_boundary_id IS NULL
          AND OLD.state = 'user_needed'
          AND OLD.attention_owner = 'user'
          AND NEW.user_needed_boundary_id IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'ambiguous legacy WAIT_USER authority cannot be claimed');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_user_needed_boundary_replace_update
        BEFORE UPDATE OF user_needed_boundary_id ON work_items
        FOR EACH ROW
        WHEN OLD.user_needed_boundary_id IS NOT NULL
          AND NEW.user_needed_boundary_id IS NOT NULL
          AND NEW.user_needed_boundary_id IS NOT OLD.user_needed_boundary_id
        BEGIN
            SELECT RAISE(ABORT, 'current WAIT_USER boundary cannot be replaced');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_user_needed_boundary_clear_update
        BEFORE UPDATE OF user_needed_boundary_id ON work_items
        FOR EACH ROW
        WHEN OLD.user_needed_boundary_id IS NOT NULL
          AND NEW.user_needed_boundary_id IS NULL
          AND NOT (
              (
                  NEW.state IN ('completed', 'canceled', 'failed')
                  AND NEW.attention_owner = 'none'
                  AND NEW.generation > OLD.generation
              ) OR (
                  NEW.state = 'active'
                  AND NEW.attention_owner = 'worker'
                  AND EXISTS (
                      SELECT 1
                      FROM boundary_continuations AS continuation
                      JOIN attempts AS successor
                        ON successor.id = continuation.successor_attempt_id
                       AND successor.work_item_id = NEW.id
                      WHERE continuation.boundary_id
                            = OLD.user_needed_boundary_id
                        AND continuation.work_item_id = NEW.id
                        AND continuation.successor_generation = NEW.generation
                        AND successor.worker_id = NEW.assigned_worker_id
                        AND successor.goal_version = NEW.goal_version
                        AND NOT EXISTS (
                            SELECT 1 FROM attempts AS later
                            WHERE later.work_item_id = NEW.id
                              AND later.attempt_number > successor.attempt_number
                        )
                  )
              )
          )
        BEGIN
            SELECT RAISE(ABORT, 'WAIT_USER boundary requires exact consumption or terminal exit');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuations_exact_insert
        BEFORE INSERT ON boundary_continuations
        FOR EACH ROW
        WHEN NOT EXISTS (
            SELECT 1
            FROM work_items AS work
            JOIN boundaries AS boundary
              ON boundary.id = NEW.boundary_id
             AND boundary.work_item_id = work.id
            JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
             AND disposition.kind = 'wait_user'
            JOIN reasoner_turns AS turn
              ON turn.id = disposition.reasoner_turn_id
             AND turn.boundary_id = boundary.id
             AND turn.work_item_id = work.id
            JOIN attempts AS source_attempt
              ON source_attempt.id = NEW.source_attempt_id
             AND source_attempt.id = boundary.attempt_id
             AND source_attempt.work_item_id = work.id
            JOIN attempts AS successor_attempt
              ON successor_attempt.id = NEW.successor_attempt_id
             AND successor_attempt.work_item_id = work.id
            JOIN principals AS decider
              ON decider.id = NEW.decided_by
             AND decider.role = 'cao'
            LEFT JOIN messages AS message
              ON message.id = NEW.message_id
            LEFT JOIN message_deliveries AS delivery
              ON delivery.message_id = message.id
             AND delivery.recipient_id = successor_attempt.worker_id
            LEFT JOIN boundary_supersessions AS supersession
              ON supersession.boundary_id = boundary.id
            WHERE work.id = NEW.work_item_id
              AND work.user_needed_boundary_id = NEW.boundary_id
              AND work.state = 'user_needed'
              AND work.attention_owner = 'user'
              AND work.supervisor_id = NEW.decided_by
              AND work.generation = NEW.source_generation
              AND work.assigned_worker_id = successor_attempt.worker_id
              AND work.goal_version = successor_attempt.goal_version
              AND boundary.generation = NEW.source_generation
              AND boundary.goal_version = source_attempt.goal_version
              AND boundary.goal_packet_digest = source_attempt.goal_packet_digest
              AND boundary.task_packet_digest = source_attempt.task_packet_digest
              AND boundary.source_principal_id = source_attempt.worker_id
              AND disposition.generation = NEW.source_generation
              AND disposition.decided_by = NEW.decided_by
              AND turn.supervisor_id = NEW.decided_by
              AND turn.generation = NEW.source_generation
              AND turn.goal_version = boundary.goal_version
              AND turn.goal_packet_digest = boundary.goal_packet_digest
              AND turn.task_packet_digest = boundary.task_packet_digest
              AND supersession.boundary_id IS NULL
              AND (
                  (
                      NEW.successor_attempt_id = NEW.source_attempt_id
                      AND NEW.successor_generation = NEW.source_generation
                      AND source_attempt.state = 'input_required'
                      AND message.kind = 'instruction'
                  ) OR (
                      NEW.successor_attempt_id <> NEW.source_attempt_id
                      AND successor_attempt.attempt_number
                          = source_attempt.attempt_number + 1
                      AND NEW.successor_generation = NEW.source_generation + 1
                      AND source_attempt.state = 'canceled'
                      AND message.kind = 'assignment'
                  )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM attempts AS later
                  WHERE later.work_item_id = work.id
                    AND later.attempt_number > successor_attempt.attempt_number
              )
              AND message.work_item_id = work.id
              AND message.attempt_id = successor_attempt.id
              AND message.sender_id = NEW.decided_by
              AND message.goal_version = successor_attempt.goal_version
              AND message.goal_packet_digest = successor_attempt.goal_packet_digest
              AND message.task_packet_digest = successor_attempt.task_packet_digest
              AND delivery.message_id = message.id
              AND NOT EXISTS (
                  SELECT 1 FROM message_deliveries AS other_delivery
                  WHERE other_delivery.message_id = message.id
                    AND other_delivery.recipient_id <> successor_attempt.worker_id
              )
        )
        BEGIN
            SELECT RAISE(ABORT, 'boundary continuation exact binding violation');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuations_immutable_update
        BEFORE UPDATE ON boundary_continuations
        FOR EACH ROW BEGIN
            SELECT RAISE(ABORT, 'boundary continuation is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuations_immutable_delete
        BEFORE DELETE ON boundary_continuations
        FOR EACH ROW BEGIN
            SELECT RAISE(ABORT, 'boundary continuation is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuation_messages_immutable_update
        BEFORE UPDATE ON messages
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM boundary_continuations
            WHERE message_id = OLD.id
        )
        BEGIN
            SELECT RAISE(ABORT, 'boundary continuation message is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuation_deliveries_exact_insert
        BEFORE INSERT ON message_deliveries
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM boundary_continuations
            WHERE message_id = NEW.message_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'boundary continuation delivery set is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuation_deliveries_identity_update
        BEFORE UPDATE OF message_id, recipient_id ON message_deliveries
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM boundary_continuations
            WHERE message_id IN (OLD.message_id, NEW.message_id)
        )
        BEGIN
            SELECT RAISE(ABORT, 'boundary continuation delivery identity is immutable');
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER boundary_continuation_deliveries_immutable_delete
        BEFORE DELETE ON message_deliveries
        FOR EACH ROW
        WHEN EXISTS (
            SELECT 1 FROM boundary_continuations
            WHERE message_id = OLD.message_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'boundary continuation delivery is immutable');
        END
        """
    )


def _ensure_boundary_continuation_schema(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Install schema36 WAIT_USER authority without guessing consumed history."""

    work_columns = {
        str(column["name"]): column
        for column in connection.execute("PRAGMA table_info(work_items)")
    }
    pointer = work_columns.get(_USER_NEEDED_BOUNDARY_COLUMN)
    if pointer is None:
        connection.execute(
            "ALTER TABLE work_items ADD COLUMN user_needed_boundary_id TEXT "
            "REFERENCES boundaries(id) ON DELETE RESTRICT"
        )
    elif str(pointer["type"]).upper() != "TEXT" or int(pointer["notnull"]) != 0:
        raise RuntimeError("Work WAIT_USER boundary schema is incompatible")

    columns = {
        str(column["name"]): column
        for column in connection.execute("PRAGMA table_info(boundary_continuations)")
    }
    if set(columns) != _BOUNDARY_CONTINUATION_COLUMNS:
        raise RuntimeError("boundary continuation schema is partial or incompatible")
    expected_required = _BOUNDARY_CONTINUATION_COLUMNS
    if any(
        str(columns[name]["type"]).upper()
        != ("INTEGER" if name in {"source_generation", "successor_generation"} else "TEXT")
        or (
            name in expected_required
            and int(columns[name]["notnull"]) == 0
            and name != "boundary_id"
        )
        for name in _BOUNDARY_CONTINUATION_COLUMNS
    ):
        raise RuntimeError("boundary continuation column contract is incompatible")
    if int(columns["boundary_id"]["pk"]) != 1:
        raise RuntimeError("boundary continuation boundary key is not unique")
    table_row = connection.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'table' AND name = 'boundary_continuations'
        """
    ).fetchone()
    normalized_table_sql = "".join(str(table_row["sql"] if table_row else "").lower().split())
    if (
        "check(source_generation>=1)" not in normalized_table_sql
        or "check(successor_generation>=1)" not in normalized_table_sql
    ):
        raise RuntimeError("boundary continuation generation checks are incompatible")

    expected_foreign_keys = {
        ("boundary_id", "boundaries", "id", "RESTRICT"),
        ("work_item_id", "work_items", "id", "RESTRICT"),
        ("source_attempt_id", "attempts", "id", "RESTRICT"),
        ("successor_attempt_id", "attempts", "id", "RESTRICT"),
        ("message_id", "messages", "id", "RESTRICT"),
        ("decided_by", "principals", "id", "RESTRICT"),
    }
    actual_foreign_keys = {
        (
            str(row["from"]),
            str(row["table"]),
            str(row["to"]),
            str(row["on_delete"]).upper(),
        )
        for row in connection.execute("PRAGMA foreign_key_list(boundary_continuations)")
    }
    if actual_foreign_keys != expected_foreign_keys:
        raise RuntimeError("boundary continuation foreign-key contract is incompatible")
    pointer_foreign_keys = {
        (
            str(row["from"]),
            str(row["table"]),
            str(row["to"]),
            str(row["on_delete"]).upper(),
        )
        for row in connection.execute("PRAGMA foreign_key_list(work_items)")
    }
    if (
        _USER_NEEDED_BOUNDARY_COLUMN,
        "boundaries",
        "id",
        "RESTRICT",
    ) not in pointer_foreign_keys:
        raise RuntimeError("Work WAIT_USER boundary foreign key is incompatible")

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS boundary_continuations_work_generation_idx
        ON boundary_continuations(
            work_item_id, successor_generation, created_at DESC
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS boundary_continuations_source_attempt_idx "
        "ON boundary_continuations(source_attempt_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS boundary_continuations_successor_attempt_idx "
        "ON boundary_continuations(successor_attempt_id)"
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS boundary_continuations_message_unique "
        "ON boundary_continuations(message_id)"
    )
    expected_indexes = {
        "boundary_continuations_work_generation_idx": (
            ("work_item_id", "successor_generation", "created_at"),
            False,
            False,
        ),
        "boundary_continuations_source_attempt_idx": (("source_attempt_id",), False, False),
        "boundary_continuations_successor_attempt_idx": (
            ("successor_attempt_id",),
            False,
            False,
        ),
        "boundary_continuations_message_unique": (("message_id",), True, False),
    }
    index_rows = {
        str(row["name"]): row
        for row in connection.execute("PRAGMA index_list(boundary_continuations)")
    }
    for name, (expected_columns, unique, partial) in expected_indexes.items():
        row = index_rows.get(name)
        actual_columns = tuple(
            str(item["name"]) for item in connection.execute(f'PRAGMA index_info("{name}")')
        )
        if (
            row is None
            or actual_columns != expected_columns
            or bool(row["unique"]) is not unique
            or bool(row["partial"]) is not partial
        ):
            raise RuntimeError("boundary continuation index contract is incompatible")

    _validate_and_backfill_user_needed_boundaries(
        connection,
        # The unreleased pre-pointer schema36 build needs the same one-time
        # migration as v35.  Once the column exists, a schema36 NULL pointer
        # is a durable ambiguity decision and must never acquire authority on
        # a later reopen.
        backfill_missing=pointer is None or existing_schema_version < 36,
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS work_items_user_needed_boundary_idx
        ON work_items(user_needed_boundary_id)
        WHERE user_needed_boundary_id IS NOT NULL
        """
    )
    pointer_index = next(
        (
            row
            for row in connection.execute("PRAGMA index_list(work_items)")
            if str(row["name"]) == "work_items_user_needed_boundary_idx"
        ),
        None,
    )
    pointer_index_columns = tuple(
        str(row["name"])
        for row in connection.execute('PRAGMA index_info("work_items_user_needed_boundary_idx")')
    )
    if (
        pointer_index is None
        or pointer_index_columns != ("user_needed_boundary_id",)
        or bool(pointer_index["unique"])
        or not bool(pointer_index["partial"])
    ):
        raise RuntimeError("Work WAIT_USER boundary index contract is incompatible")
    _install_boundary_continuation_triggers(connection)


def work_pause_record_binding_sql(alias: str = "pause") -> str:
    """Validate immutable pause provenance; aliases are internal SQL identifiers."""

    return f"""
        EXISTS (
            SELECT 1 FROM boundaries AS boundary
            JOIN work_items AS work ON work.id = boundary.work_item_id
            JOIN attempts AS attempt
              ON attempt.id = boundary.attempt_id AND attempt.work_item_id = work.id
            JOIN boundary_dispositions AS disposition ON disposition.boundary_id = boundary.id
            JOIN reasoner_turns AS turn
              ON turn.id = disposition.reasoner_turn_id
             AND turn.boundary_id = boundary.id AND turn.work_item_id = work.id
            JOIN principals AS decider ON decider.id = {alias}.paused_by AND decider.role = 'cao'
            WHERE boundary.id = {alias}.boundary_id
              AND work.id = {alias}.work_item_id AND attempt.id = {alias}.attempt_id
              AND boundary.source_principal_id IN (attempt.worker_id, {alias}.paused_by)
              AND boundary.generation = {alias}.source_generation
              AND {alias}.source_generation >= 1
              AND {alias}.pause_generation = {alias}.source_generation + 1
              AND boundary.goal_version = attempt.goal_version
              AND boundary.goal_packet_digest = attempt.goal_packet_digest
              AND boundary.task_packet_digest = attempt.task_packet_digest
              AND disposition.kind = 'pause'
              AND disposition.decided_by = {alias}.paused_by
              AND disposition.generation = {alias}.source_generation
              AND length(trim(disposition.reason)) > 0
              AND length(trim(disposition.resume_condition)) > 0
              AND turn.supervisor_id = {alias}.paused_by
              AND turn.generation = {alias}.source_generation
              AND turn.goal_version = boundary.goal_version
              AND turn.goal_packet_digest = boundary.goal_packet_digest
              AND turn.task_packet_digest = boundary.task_packet_digest
              AND NOT EXISTS (
                  SELECT 1 FROM boundary_supersessions AS supersession
                  WHERE supersession.boundary_id = boundary.id
              )
        )
    """


def work_pause_resumption_binding_sql(alias: str = "resumption") -> str:
    """Validate one append-only, exact successor without consulting provider text."""

    return f"""
        EXISTS (
            SELECT 1 FROM work_pauses AS pause
            JOIN attempts AS previous ON previous.id = pause.attempt_id
            JOIN attempts AS successor ON successor.id = {alias}.successor_attempt_id
            JOIN principals AS decider ON decider.id = {alias}.resumed_by AND decider.role = 'cao'
            JOIN messages AS message ON message.id = {alias}.message_id
            JOIN message_deliveries AS delivery ON delivery.message_id = message.id
            WHERE pause.boundary_id = {alias}.boundary_id
              AND pause.work_item_id = {alias}.work_item_id
              AND previous.id = {alias}.previous_attempt_id
              AND previous.work_item_id = pause.work_item_id
              AND successor.work_item_id = pause.work_item_id
              AND successor.id <> previous.id
              AND successor.attempt_number = previous.attempt_number + 1
              AND successor.worker_id = previous.worker_id
              AND successor.goal_version = previous.goal_version
              AND successor.goal_packet_digest = previous.goal_packet_digest
              AND {alias}.expected_generation = pause.pause_generation
              AND {alias}.successor_generation = {alias}.expected_generation + 1
              AND message.kind = 'assignment'
              AND message.work_item_id = pause.work_item_id
              AND message.attempt_id = successor.id
              AND message.sender_id = {alias}.resumed_by
              AND message.goal_version = successor.goal_version
              AND message.goal_packet_digest = successor.goal_packet_digest
              AND message.task_packet_digest = successor.task_packet_digest
              AND delivery.recipient_id = successor.worker_id
              AND delivery.runtime_session_id IS successor.runtime_session_id
              AND NOT EXISTS (
                  SELECT 1 FROM message_deliveries AS other
                  WHERE other.message_id = message.id AND other.recipient_id <> successor.worker_id
              )
        )
    """


_SUPERVISION_PAUSE_TRIGGERS = (
    "work_pauses_exact_insert",
    "work_pauses_immutable_update",
    "work_pauses_immutable_delete",
    "work_pause_resumptions_exact_insert",
    "work_pause_resumptions_immutable_update",
    "work_pause_resumptions_immutable_delete",
    "work_items_pause_insert",
    "work_items_pause_exact_update",
    "work_items_pause_replace_update",
    "work_items_pause_clear_update",
    "work_pause_dispositions_immutable_update",
    "work_pause_dispositions_immutable_delete",
    "work_pause_boundaries_identity_update",
    "work_pause_attempts_identity_update",
    "work_pause_reasoner_turns_identity_update",
    "work_pause_messages_immutable_update",
    "work_pause_deliveries_exact_insert",
    "work_pause_deliveries_identity_update",
    "work_pause_deliveries_immutable_delete",
)


def _ensure_supervision_pause_schema(connection: sqlite3.Connection) -> None:
    """Add typed pause authority without guessing from legacy suspended state."""

    work_columns = {
        str(row["name"]): row for row in connection.execute("PRAGMA table_info(work_items)")
    }
    pointer = work_columns.get("paused_boundary_id")
    if pointer is None:
        connection.execute(
            "ALTER TABLE work_items ADD COLUMN paused_boundary_id TEXT "
            "REFERENCES boundaries(id) ON DELETE RESTRICT"
        )
    elif str(pointer["type"]).upper() != "TEXT" or int(pointer["notnull"]) != 0:
        raise RuntimeError("Work pause pointer schema is incompatible")
    if not any(
        str(row["from"]) == "paused_boundary_id"
        and str(row["table"]) == "boundaries"
        and str(row["to"]) == "id"
        and str(row["on_delete"]).upper() == "RESTRICT"
        for row in connection.execute("PRAGMA foreign_key_list(work_items)")
    ):
        raise RuntimeError("Work pause pointer foreign key is incompatible")
    table_columns = {
        "work_pauses": {
            "boundary_id",
            "work_item_id",
            "attempt_id",
            "source_generation",
            "pause_generation",
            "paused_by",
            "created_at",
        },
        "work_pause_resumptions": {
            "boundary_id",
            "work_item_id",
            "previous_attempt_id",
            "successor_attempt_id",
            "expected_generation",
            "successor_generation",
            "message_id",
            "resumed_by",
            "reason",
            "instruction",
            "resume_evidence",
            "created_at",
        },
    }
    expected_foreign_keys = {
        "work_pauses": {
            ("boundary_id", "boundaries", "id", "RESTRICT"),
            ("work_item_id", "work_items", "id", "RESTRICT"),
            ("attempt_id", "attempts", "id", "RESTRICT"),
            ("paused_by", "principals", "id", "RESTRICT"),
        },
        "work_pause_resumptions": {
            ("boundary_id", "work_pauses", "boundary_id", "RESTRICT"),
            ("work_item_id", "work_items", "id", "RESTRICT"),
            ("previous_attempt_id", "attempts", "id", "RESTRICT"),
            ("successor_attempt_id", "attempts", "id", "RESTRICT"),
            ("message_id", "messages", "id", "RESTRICT"),
            ("resumed_by", "principals", "id", "RESTRICT"),
        },
    }
    required_checks: dict[str, tuple[str, ...]] = {
        "work_pauses": (
            "check(typeof(source_generation)='integer'andsource_generation>=1)",
            "check(typeof(pause_generation)='integer'andpause_generation=source_generation+1)",
        ),
        "work_pause_resumptions": (
            "check(typeof(expected_generation)='integer'andexpected_generation>=1)",
            "check(typeof(successor_generation)='integer'andsuccessor_generation=expected_generation+1)",
            "check(length(trim(reason))>0)",
            "check(length(trim(instruction))>0)",
            "check(length(trim(resume_evidence))>0)",
        ),
    }
    required_unique_keys: dict[str, set[tuple[str, ...]]] = {
        "work_pauses": {("boundary_id",), ("work_item_id", "pause_generation")},
        "work_pause_resumptions": {("boundary_id",), ("successor_attempt_id",), ("message_id",)},
    }
    for table, expected in table_columns.items():
        columns = {
            str(row["name"]): row for row in connection.execute(f"PRAGMA table_info({table})")
        }
        if (
            set(columns) != expected
            or any(
                str(row["type"]).upper() != ("INTEGER" if name.endswith("_generation") else "TEXT")
                or not int(row["notnull"])
                for name, row in columns.items()
            )
            or int(columns["boundary_id"]["pk"]) != 1
        ):
            raise RuntimeError("supervision pause ledger schema is incompatible")
        foreign_keys = {
            (str(row["from"]), str(row["table"]), str(row["to"]), str(row["on_delete"]).upper())
            for row in connection.execute(f"PRAGMA foreign_key_list({table})")
        }
        if foreign_keys != expected_foreign_keys[table]:
            raise RuntimeError("supervision pause ledger foreign keys are incompatible")
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        normalized_sql = "".join(str(table_sql["sql"] if table_sql else "").lower().split())
        if any(check not in normalized_sql for check in required_checks[table]):
            raise RuntimeError("supervision pause ledger checks are incompatible")
        unique_keys = {
            tuple(
                str(column["name"])
                for column in connection.execute(
                    "SELECT name FROM pragma_index_info(?) ORDER BY seqno", (str(index["name"]),)
                )
            )
            for index in connection.execute(f"PRAGMA index_list({table})")
            if bool(index["unique"]) and not bool(index["partial"])
        }
        if not required_unique_keys[table] <= unique_keys:
            raise RuntimeError("supervision pause ledger unique keys are incompatible")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS work_items_paused_boundary_idx "
        "ON work_items(paused_boundary_id) WHERE paused_boundary_id IS NOT NULL"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS work_pause_resumptions_work_idx "
        "ON work_pause_resumptions(work_item_id, successor_generation)"
    )
    for trigger in _SUPERVISION_PAUSE_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
    connection.execute(
        f"""
        CREATE TRIGGER work_pauses_exact_insert BEFORE INSERT ON work_pauses
        FOR EACH ROW WHEN NOT {work_pause_record_binding_sql("NEW")}
          OR NOT EXISTS (
              SELECT 1 FROM work_items AS work JOIN attempts AS attempt ON attempt.id = NEW.attempt_id
              WHERE work.id = NEW.work_item_id AND work.supervisor_id = NEW.paused_by
                AND work.generation = NEW.source_generation
                AND work.assigned_worker_id = attempt.worker_id
                AND work.goal_version = attempt.goal_version
                AND work.paused_boundary_id IS NULL
                AND NOT EXISTS (SELECT 1 FROM attempts AS later WHERE later.work_item_id = work.id
                                AND later.attempt_number > attempt.attempt_number)
          )
        BEGIN SELECT RAISE(ABORT, 'supervision pause exact binding violation'); END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER work_pause_resumptions_exact_insert BEFORE INSERT ON work_pause_resumptions
        FOR EACH ROW WHEN NOT {work_pause_resumption_binding_sql("NEW")}
          OR NOT EXISTS (
              SELECT 1 FROM work_items AS work JOIN attempts AS successor ON successor.id = NEW.successor_attempt_id
              WHERE work.id = NEW.work_item_id AND work.supervisor_id = NEW.resumed_by
                AND work.paused_boundary_id IS NULL AND work.state = 'active'
                AND work.attention_owner = 'worker' AND work.generation = NEW.successor_generation
                AND work.assigned_worker_id = successor.worker_id
                AND work.goal_version = successor.goal_version
                AND NOT EXISTS (SELECT 1 FROM attempts AS later WHERE later.work_item_id = work.id
                                AND later.attempt_number > successor.attempt_number)
          )
        BEGIN SELECT RAISE(ABORT, 'supervision pause resumption exact binding violation'); END
        """
    )
    for table in ("work_pauses", "work_pause_resumptions"):
        for action in ("update", "delete"):
            connection.execute(
                f"CREATE TRIGGER {table}_immutable_{action} BEFORE {action.upper()} ON {table} "
                "FOR EACH ROW BEGIN SELECT RAISE(ABORT, 'supervision pause history is immutable'); END"
            )
    connection.execute(
        """
        CREATE TRIGGER work_items_pause_insert BEFORE INSERT ON work_items
        FOR EACH ROW WHEN NEW.paused_boundary_id IS NOT NULL
        BEGIN SELECT RAISE(ABORT, 'new Work cannot claim pause history'); END
        """
    )
    connection.execute(
        f"""
        CREATE TRIGGER work_items_pause_exact_update
        BEFORE UPDATE OF paused_boundary_id, state, attention_owner, generation, goal_version,
                         assigned_worker_id, supervisor_id, suspended_by_work_item_id, user_needed_boundary_id ON work_items
        FOR EACH ROW WHEN NEW.paused_boundary_id IS NOT NULL AND (
            NEW.state <> 'suspended' OR NEW.attention_owner <> 'none'
            OR NEW.suspended_by_work_item_id IS NOT NULL OR NEW.user_needed_boundary_id IS NOT NULL
            OR (OLD.paused_boundary_id IS NULL AND NEW.generation <> OLD.generation + 1)
            OR NOT EXISTS (
                SELECT 1 FROM work_pauses AS pause JOIN attempts AS attempt ON attempt.id = pause.attempt_id
                WHERE pause.boundary_id = NEW.paused_boundary_id AND pause.work_item_id = NEW.id
                  AND pause.pause_generation = NEW.generation AND pause.paused_by = NEW.supervisor_id
                  AND attempt.state = 'suspended' AND attempt.worker_id = NEW.assigned_worker_id
                  AND attempt.goal_version = NEW.goal_version
                  AND {work_pause_record_binding_sql()}
                  AND NOT EXISTS (SELECT 1 FROM work_pause_resumptions AS resumption WHERE resumption.boundary_id = pause.boundary_id)
                  AND NOT EXISTS (SELECT 1 FROM attempts AS later WHERE later.work_item_id = NEW.id
                                  AND later.attempt_number > attempt.attempt_number)
            )
        )
        BEGIN SELECT RAISE(ABORT, 'Work supervision pause exact binding violation'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_pause_replace_update BEFORE UPDATE OF paused_boundary_id ON work_items
        FOR EACH ROW WHEN OLD.paused_boundary_id IS NOT NULL AND NEW.paused_boundary_id IS NOT NULL
          AND NEW.paused_boundary_id IS NOT OLD.paused_boundary_id
        BEGIN SELECT RAISE(ABORT, 'current supervision pause cannot be replaced'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_items_pause_clear_update BEFORE UPDATE OF paused_boundary_id ON work_items
        FOR EACH ROW WHEN OLD.paused_boundary_id IS NOT NULL AND NEW.paused_boundary_id IS NULL
          AND NOT (
              (NEW.state = 'active' AND NEW.attention_owner = 'worker' AND NEW.generation = OLD.generation + 1)
              OR (NEW.state IN ('completed', 'canceled', 'failed') AND NEW.attention_owner = 'none'
                  AND NEW.generation > OLD.generation)
          )
        BEGIN SELECT RAISE(ABORT, 'supervision pause requires explicit resumption or terminal exit'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_pause_dispositions_immutable_update BEFORE UPDATE ON boundary_dispositions
        FOR EACH ROW WHEN EXISTS (SELECT 1 FROM work_pauses WHERE boundary_id = OLD.boundary_id)
        BEGIN SELECT RAISE(ABORT, 'supervision pause disposition is immutable'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_pause_messages_immutable_update BEFORE UPDATE ON messages
        FOR EACH ROW WHEN EXISTS (SELECT 1 FROM work_pause_resumptions WHERE message_id = OLD.id)
        BEGIN SELECT RAISE(ABORT, 'supervision pause resumption message is immutable'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_pause_dispositions_immutable_delete BEFORE DELETE ON boundary_dispositions
        FOR EACH ROW WHEN EXISTS (SELECT 1 FROM work_pauses WHERE boundary_id = OLD.boundary_id)
        BEGIN SELECT RAISE(ABORT, 'supervision pause disposition is immutable'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_pause_boundaries_identity_update
        BEFORE UPDATE OF id, source_principal_id, work_item_id, attempt_id, goal_version,
                         generation, goal_packet_digest, task_packet_digest ON boundaries
        FOR EACH ROW WHEN EXISTS (SELECT 1 FROM work_pauses WHERE boundary_id = OLD.id)
        BEGIN SELECT RAISE(ABORT, 'supervision pause Boundary identity is immutable'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_pause_attempts_identity_update
        BEFORE UPDATE OF id, work_item_id, worker_id, attempt_number, goal_version,
                         goal_packet_digest, task_packet_digest ON attempts
        FOR EACH ROW WHEN EXISTS (SELECT 1 FROM work_pauses WHERE attempt_id = OLD.id)
          OR EXISTS (SELECT 1 FROM work_pause_resumptions WHERE successor_attempt_id = OLD.id)
        BEGIN SELECT RAISE(ABORT, 'supervision pause Attempt identity is immutable'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER work_pause_reasoner_turns_identity_update
        BEFORE UPDATE OF id, supervisor_id, work_item_id, boundary_id, generation,
                         goal_version, goal_packet_digest, task_packet_digest ON reasoner_turns
        FOR EACH ROW WHEN EXISTS (
            SELECT 1 FROM work_pauses pause JOIN boundary_dispositions disposition
              ON disposition.boundary_id = pause.boundary_id WHERE disposition.reasoner_turn_id = OLD.id
        )
        BEGIN SELECT RAISE(ABORT, 'supervision pause decision identity is immutable'); END
        """
    )
    for suffix, operation, identity in (
        ("exact_insert", "INSERT", "NEW"),
        ("identity_update", "UPDATE OF message_id, recipient_id, runtime_session_id", "OLD"),
        ("immutable_delete", "DELETE", "OLD"),
    ):
        connection.execute(
            f"CREATE TRIGGER work_pause_deliveries_{suffix} BEFORE {operation} ON message_deliveries "
            f"FOR EACH ROW WHEN EXISTS (SELECT 1 FROM work_pause_resumptions WHERE message_id = {identity}.message_id) "
            "BEGIN SELECT RAISE(ABORT, 'supervision pause resumption delivery identity is immutable'); END"
        )


def _managed_worker_pre_mcp_lifecycle_is_proven(
    connection: sqlite3.Connection,
    *,
    worker_id: str,
    runtime_id: str,
    reason: str,
) -> bool:
    """Prove that one terminal managed epoch never acquired Worker authority."""

    lifecycle = connection.execute(
        """
        SELECT runtime.state AS runtime_state, runtime.native_session_id,
               enrollment.state AS enrollment_state, enrollment.discovered_at,
               enrollment.heartbeat_sequence,
               (SELECT COUNT(*) FROM runtime_credentials AS credential
                 WHERE credential.enrollment_id = enrollment.id) AS credentials,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                 WHERE ticket.enrollment_id = enrollment.id) AS tickets,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                 WHERE ticket.enrollment_id = enrollment.id
                   AND ticket.consumed_at IS NOT NULL) AS consumed_tickets,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                 WHERE ticket.enrollment_id = enrollment.id
                   AND ticket.state = 'pending') AS pending_tickets
        FROM runtime_sessions AS runtime
        JOIN worker_enrollments AS enrollment
          ON enrollment.runtime_session_id = runtime.id
         AND enrollment.principal_id = runtime.principal_id
        WHERE runtime.id = ? AND runtime.principal_id = ?
          AND enrollment.managed = 1
        """,
        (runtime_id, worker_id),
    ).fetchone()
    return bool(
        lifecycle is not None
        and str(lifecycle["runtime_state"]) in {"failed", "missing", "stopped"}
        and str(lifecycle["enrollment_state"]) in {"failed", "revoked", "stale"}
        and not str(lifecycle["native_session_id"] or "")
        and lifecycle["discovered_at"] is None
        and not int(lifecycle["heartbeat_sequence"])
        and not int(lifecycle["credentials"])
        and not int(lifecycle["consumed_tickets"])
        and not int(lifecycle["pending_tickets"])
        and (reason == "runtime_unavailable" or int(lifecycle["tickets"]) >= 1)
    )


def _managed_worker_pre_submit_lifecycle_is_proven(
    connection: sqlite3.Connection,
    *,
    worker_id: str,
    runtime_id: str,
    attempt_id: str,
) -> bool:
    """Prove a terminal managed epoch never submitted its Assignment to Codex.

    A fresh native Worker consumes its launch ticket before MCP bootstrap.  A
    resumed native thread can fail earlier, while App Server is binding the
    saved thread, so its ticket is issued but remains unconsumed.  Keep both
    paths fail-closed by binding the only ticket to this exact Attempt and, for
    the unconsumed path, requiring that no Worker enrollment authority ever
    existed.
    """

    lifecycle = connection.execute(
        """
        SELECT runtime.state AS runtime_state, runtime.native_session_id,
               enrollment.state AS enrollment_state,
               enrollment.discovered_at, enrollment.heartbeat_sequence,
               enrollment.protocol_version, enrollment.discovered_tools_digest,
               (SELECT COUNT(*) FROM runtime_credentials AS credential
                WHERE credential.enrollment_id = enrollment.id) AS credentials,
               (SELECT COUNT(*) FROM runtime_credentials AS credential
                WHERE credential.enrollment_id = enrollment.id
                  AND credential.state = 'active') AS active_credentials,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                WHERE ticket.enrollment_id = enrollment.id) AS tickets,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                WHERE ticket.enrollment_id = enrollment.id
                  AND ticket.state = 'pending') AS pending_tickets,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                WHERE ticket.enrollment_id = enrollment.id
                  AND ticket.consumed_at IS NOT NULL) AS consumed_tickets,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                WHERE ticket.enrollment_id = enrollment.id
                  AND ticket.attempt_id = ?) AS attempt_tickets,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                WHERE ticket.enrollment_id = enrollment.id
                  AND ticket.attempt_id = ?
                  AND ticket.consumed_at IS NULL
                  AND ticket.state IN ('revoked', 'expired')) AS terminal_unconsumed_tickets
        FROM runtime_sessions AS runtime
        JOIN worker_enrollments AS enrollment
          ON enrollment.runtime_session_id = runtime.id
         AND enrollment.principal_id = runtime.principal_id
        WHERE runtime.id = ? AND runtime.principal_id = ?
          AND enrollment.managed = 1
        """,
        (attempt_id, attempt_id, runtime_id, worker_id),
    ).fetchone()
    if (
        lifecycle is None
        or str(lifecycle["runtime_state"]) not in {"failed", "missing", "stopped"}
        or str(lifecycle["enrollment_state"]) not in {"failed", "revoked", "stale"}
        or not str(lifecycle["native_session_id"] or "")
        or int(lifecycle["active_credentials"])
        or int(lifecycle["pending_tickets"])
        or int(lifecycle["tickets"]) != 1
        or int(lifecycle["attempt_tickets"]) != 1
    ):
        return False
    if int(lifecycle["consumed_tickets"]) == 1:
        return True
    return bool(
        not int(lifecycle["consumed_tickets"])
        and int(lifecycle["terminal_unconsumed_tickets"]) == 1
        and not int(lifecycle["credentials"])
        and lifecycle["discovered_at"] is None
        and not int(lifecycle["heartbeat_sequence"])
        and not str(lifecycle["protocol_version"] or "")
        and not str(lifecycle["discovered_tools_digest"] or "")
    )


def _exact_pre_mcp_unknown_event_is_proven(
    connection: sqlite3.Connection,
    *,
    boundary_id: str,
    message_id: str,
    recipient_id: str,
    runtime_id: str,
    delivery_generation: int,
    failure_code: str,
) -> bool:
    """Accept one v2 binding, or one genuinely pre-v31 legacy binding."""

    event_data_json = _safe_json_document_sql("events.data_json")
    opening_data_json = _safe_json_document_sql("opening.data_json")

    event = connection.execute(
        f"""
        SELECT COUNT(*) AS count,
               COALESCE(SUM(CASE
                 WHEN json_extract({event_data_json}, '$.failure_code') = ?
                  AND json_extract({event_data_json}, '$.recipient_id') = ?
                  AND (
                    (
                      json_extract({event_data_json}, '$.binding_version') = 2
                      AND json_extract({event_data_json}, '$.runtime_session_id') = ?
                      AND json_extract({event_data_json}, '$.delivery_generation') = ?
                    ) OR (
                      json_type({event_data_json}, '$.binding_version') IS NULL
                      AND json_type({event_data_json}, '$.runtime_session_id') IS NULL
                      AND json_type({event_data_json}, '$.delivery_generation') IS NULL
                      AND (
                        SELECT COUNT(*) FROM events AS opening
                        WHERE opening.event_type = 'boundary.recorded'
                          AND opening.aggregate_type = 'work_item'
                          AND json_extract(
                                {opening_data_json}, '$.boundary_id'
                              ) = ?
                      ) = 1
                      AND events.sequence < (
                        SELECT opening.sequence FROM events AS opening
                        WHERE opening.event_type = 'boundary.recorded'
                          AND opening.aggregate_type = 'work_item'
                          AND json_extract(
                                {opening_data_json}, '$.boundary_id'
                              ) = ?
                      )
                    )
                  ) THEN 1 ELSE 0 END), 0) AS matching_count
        FROM events
        WHERE event_type = 'runtime.message_delivery_unknown'
          AND aggregate_type = 'message' AND aggregate_id = ?
        """,
        (
            failure_code,
            recipient_id,
            runtime_id,
            delivery_generation,
            boundary_id,
            boundary_id,
            message_id,
        ),
    ).fetchone()
    return bool(
        event is not None and int(event["count"]) == 1 and int(event["matching_count"]) == 1
    )


def _exact_assignment_not_submitted_is_proven(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
) -> bool:
    """Prove App Server submission never occurred for one exact Assignment."""

    deliveries = connection.execute(
        """
        SELECT delivery.message_id, delivery.recipient_id, delivery.state,
               delivery.generation, delivery.runtime_session_id,
               delivery.last_error
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
          AND delivery.recipient_id = ?
        """,
        (row["attempt_id"], row["worker_id"]),
    ).fetchall()
    if len(deliveries) != 1:
        return False
    delivery = deliveries[0]
    runtime_id = str(row["runtime_session_id"] or "")
    if (
        str(delivery["recipient_id"]) != str(row["worker_id"])
        or str(delivery["runtime_session_id"] or "") != runtime_id
        or str(delivery["state"]) != "dead"
        or str(delivery["last_error"] or "") != "runtime_dispatch_pre_submit_failed"
    ):
        return False
    runtime = connection.execute(
        "SELECT metadata_json FROM runtime_sessions WHERE id = ?",
        (runtime_id,),
    ).fetchone()
    try:
        metadata = json.loads(str(runtime["metadata_json"] or "{}")) if runtime else {}
    except (TypeError, ValueError):
        return False
    last_dispatch = metadata.get("last_dispatch") if isinstance(metadata, Mapping) else None
    diagnostics = last_dispatch.get("diagnostics") if isinstance(last_dispatch, Mapping) else None
    phase = diagnostics.get("dispatch_phase") if isinstance(diagnostics, Mapping) else None
    failure_code = diagnostics.get("failure_code") if isinstance(diagnostics, Mapping) else None
    if (
        str(metadata.get("last_dispatch_message_id") or "") != str(delivery["message_id"])
        or not isinstance(last_dispatch, Mapping)
        or last_dispatch.get("success") is not False
        or not isinstance(diagnostics, Mapping)
        or diagnostics.get("delivery_acceptance") != "not_submitted"
        or phase not in {"initialize", "thread_binding", "mcp_startup"}
        or failure_code
        not in {
            "mcp_startup_timeout",
            "mcp_startup_failed",
            "runtime_timeout",
            "runtime_protocol_invalid_json",
            "runtime_process_exited",
            "runtime_dispatch_failed",
        }
    ):
        return False
    events = connection.execute(
        """
        SELECT event_type, data_json FROM events
        WHERE aggregate_type = 'message' AND aggregate_id = ?
          AND event_type IN (
              'runtime.message_not_submitted',
              'runtime.message_delivery_unknown'
          )
        """,
        (delivery["message_id"],),
    ).fetchall()
    if len(events) != 1 or str(events[0]["event_type"]) != "runtime.message_not_submitted":
        return False
    try:
        evidence = json.loads(str(events[0]["data_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    return bool(
        isinstance(evidence, Mapping)
        and evidence.get("binding_version") == 2
        and str(evidence.get("recipient_id") or "") == str(row["worker_id"])
        and str(evidence.get("runtime_session_id") or "") == runtime_id
        and evidence.get("delivery_generation") == int(delivery["generation"])
        and evidence.get("dispatch_phase") == phase
        and evidence.get("failure_code") == failure_code
    )


def _prior_attempt_ticket_precedes_assignment(
    connection: sqlite3.Connection,
    *,
    ticket_id: str,
    enrollment_id: str,
    ticket_attempt_id: str,
    ticket_generation: int,
    current_attempt_id: str,
    assignment_message_id: str,
) -> bool:
    event_data_json = _safe_json_document_sql("events.data_json")
    prior = connection.execute(
        """
        SELECT 1 FROM attempts AS attempt
        JOIN work_items AS work ON work.id = attempt.work_item_id
        WHERE attempt.id = ? AND work.state IN ('completed', 'canceled', 'failed')
        """,
        (ticket_attempt_id,),
    ).fetchone()
    if prior is None or ticket_attempt_id == current_attempt_id:
        return False
    sequences = connection.execute(
        f"""
        SELECT
          (SELECT COUNT(*) FROM events
           WHERE event_type = 'runtime.launch_ticket_issued'
             AND aggregate_type = 'worker_enrollment' AND aggregate_id = ?
             AND json_extract({event_data_json}, '$.ticket_id') = ?
             AND json_extract({event_data_json}, '$.generation') = ?) AS ticket_count,
          (SELECT sequence FROM events
           WHERE event_type = 'runtime.launch_ticket_issued'
             AND aggregate_type = 'worker_enrollment' AND aggregate_id = ?
             AND json_extract({event_data_json}, '$.ticket_id') = ?
             AND json_extract({event_data_json}, '$.generation') = ?
           ORDER BY sequence LIMIT 1) AS ticket_sequence,
          (SELECT COUNT(*) FROM events
           WHERE event_type = 'message.created'
             AND aggregate_type = 'message' AND aggregate_id = ?
             AND json_extract({event_data_json}, '$.attempt_id') = ?) AS assignment_count,
          (SELECT sequence FROM events
           WHERE event_type = 'message.created'
             AND aggregate_type = 'message' AND aggregate_id = ?
             AND json_extract({event_data_json}, '$.attempt_id') = ?
           ORDER BY sequence LIMIT 1) AS assignment_sequence
        """,
        (
            enrollment_id,
            ticket_id,
            ticket_generation,
            enrollment_id,
            ticket_id,
            ticket_generation,
            assignment_message_id,
            current_attempt_id,
            assignment_message_id,
            current_attempt_id,
        ),
    ).fetchone()
    return bool(
        sequences is not None
        and int(sequences["ticket_count"]) == 1
        and int(sequences["assignment_count"]) == 1
        and int(sequences["ticket_sequence"]) < int(sequences["assignment_sequence"])
    )


def _pre_dispatch_timeout_recovery_is_proven(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
) -> bool:
    """Prove one watchdog-owned, current-Attempt unclaimed Assignment.

    Historical MCP authority on an earlier Attempt is irrelevant here.  The
    proof is instead bound to the exact queued deliveries and launch-ticket
    generation of the current Attempt, plus the watchdog event that observed
    the safe runtime before terminalizing it.
    """

    event_data_json = _safe_json_document_sql("events.data_json")

    boundary = connection.execute(
        """
        SELECT boundary.source_event_id, boundary.source_principal_id,
               boundary.goal_version,
               boundary.goal_packet_digest, boundary.task_packet_digest,
               boundary.generation, boundary.metadata_json, boundary.created_at,
               boundary.kind, boundary.summary,
               boundary.runtime_state AS boundary_runtime_state,
               boundary.input_digest,
               attempt.created_at AS attempt_created_at,
               runtime.state AS runtime_state,
               enrollment.id AS enrollment_id,
               enrollment.state AS enrollment_state,
               enrollment.generation AS enrollment_generation,
               (SELECT COUNT(*) FROM runtime_credentials AS credential
                 WHERE credential.enrollment_id = enrollment.id
                   AND credential.state = 'active') AS active_credentials,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                 WHERE ticket.enrollment_id = enrollment.id
                   AND ticket.state = 'pending') AS pending_tickets
        FROM boundaries AS boundary
        JOIN attempts AS attempt ON attempt.id = boundary.attempt_id
        JOIN runtime_sessions AS runtime ON runtime.id = ?
        JOIN worker_enrollments AS enrollment
          ON enrollment.runtime_session_id = runtime.id
         AND enrollment.principal_id = ?
         AND enrollment.managed = 1
        WHERE boundary.id = ? AND boundary.attempt_id = ?
          AND boundary.work_item_id = ?
        """,
        (
            row["runtime_session_id"],
            row["worker_id"],
            row["id"],
            row["attempt_id"],
            row["work_item_id"],
        ),
    ).fetchone()
    if boundary is None:
        return False
    try:
        metadata = json.loads(str(boundary["metadata_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    if (
        not isinstance(metadata, Mapping)
        or metadata.get("runtime_recovery") is not True
        or metadata.get("pre_dispatch_timeout") is not True
        or metadata.get("reason") != "runtime_dispatch_failed"
        or str(boundary["source_principal_id"]) != str(row["worker_id"])
        or str(boundary["runtime_state"]) not in {"failed", "missing", "stopped"}
        or str(boundary["enrollment_state"]) not in {"failed", "revoked", "stale"}
        or int(boundary["active_credentials"])
        or int(boundary["pending_tickets"])
        or int(
            connection.execute(
                f"""
                SELECT COUNT(*) FROM events
                WHERE event_type = 'boundary.recorded'
                  AND aggregate_type = 'work_item' AND aggregate_id = ?
                  AND json_extract({event_data_json}, '$.boundary_id') = ?
                """,
                (row["work_item_id"], row["id"]),
            ).fetchone()[0]
        )
        != 1
    ):
        return False
    boundary_input = {
        "source_event_id": str(boundary["source_event_id"]),
        "work_item_id": str(row["work_item_id"]),
        "attempt_id": str(row["attempt_id"]),
        "expected_goal_version": int(boundary["goal_version"]),
        "expected_goal_packet_digest": str(boundary["goal_packet_digest"]),
        "expected_task_packet_digest": str(boundary["task_packet_digest"]),
        "expected_generation": int(boundary["generation"]),
        "kind": str(boundary["kind"]),
        "summary": str(boundary["summary"]),
        "runtime_state": str(boundary["boundary_runtime_state"]),
        "metadata": dict(metadata),
    }
    if canonical_digest(boundary_input) != str(boundary["input_digest"]):
        return False

    assignment_rows = connection.execute(
        """
        SELECT message.id, message.payload_json, message.payload_digest,
               delivery.generation,
               delivery.state, delivery.last_error
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
          AND delivery.recipient_id = ? AND delivery.runtime_session_id = ?
          AND delivery.state IN ('queued', 'dead')
        """,
        (row["attempt_id"], row["worker_id"], row["runtime_session_id"]),
    ).fetchall()
    if len(assignment_rows) != 1:
        return False
    assignment = assignment_rows[0]
    delivery_state = str(assignment["state"])
    if delivery_state == "dead" and str(assignment["last_error"] or "") != (
        "runtime_dispatch_failed"
    ):
        return False
    try:
        assignment_payload = json.loads(str(assignment["payload_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    if (
        not isinstance(assignment_payload, Mapping)
        or assignment_payload.get("goal_version") != int(boundary["goal_version"])
        or assignment_payload.get("generation") != int(boundary["generation"]) - 1
        or assignment_payload.get("goal_packet_digest") != str(boundary["goal_packet_digest"])
        or assignment_payload.get("task_packet_digest") != str(boundary["task_packet_digest"])
        or assignment_payload.get("work_item_id") != str(row["work_item_id"])
        or assignment_payload.get("attempt_id") != str(row["attempt_id"])
        or canonical_digest(assignment_payload) != str(assignment["payload_digest"])
    ):
        return False

    source_prefix = "unstarted-status:"
    source_event_id = str(boundary["source_event_id"] or "")
    source_suffix = f":{row['attempt_id']}"
    if not source_event_id.startswith(source_prefix) or not source_event_id.endswith(source_suffix):
        return False
    status_id = source_event_id[len(source_prefix) : -len(source_suffix)]
    if not status_id:
        return False
    status = connection.execute(
        """
        SELECT message.id, message.sequence, message.payload_json,
               message.payload_digest
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.id = ? AND message.attempt_id = ?
          AND message.kind = 'status_request'
          AND message.sequence = (
            SELECT MAX(latest.sequence) FROM messages AS latest
            WHERE latest.attempt_id = ? AND latest.kind = 'status_request'
          )
          AND delivery.recipient_id = ? AND delivery.runtime_session_id = ?
          AND delivery.state = ?
        """,
        (
            status_id,
            row["attempt_id"],
            row["attempt_id"],
            row["worker_id"],
            row["runtime_session_id"],
            delivery_state,
        ),
    ).fetchone()
    if status is None:
        return False
    try:
        status_payload = json.loads(str(status["payload_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    if (
        not isinstance(status_payload, Mapping)
        or status_payload.get("generation") != int(boundary["generation"]) - 1
        or not isinstance(status_payload.get("response_due_at"), str)
        or str(status_payload["response_due_at"]) > str(boundary["created_at"])
        or canonical_digest(status_payload) != str(status["payload_digest"])
    ):
        return False
    worker_deliveries = connection.execute(
        """
        SELECT delivery.state, delivery.runtime_session_id, delivery.last_error
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND delivery.recipient_id = ?
        """,
        (row["attempt_id"], row["worker_id"]),
    ).fetchall()
    if not worker_deliveries or any(
        str(delivery["state"]) != delivery_state
        or str(delivery["runtime_session_id"] or "") != str(row["runtime_session_id"])
        or (
            delivery_state == "dead"
            and str(delivery["last_error"] or "") != "runtime_dispatch_failed"
        )
        for delivery in worker_deliveries
    ):
        return False

    tickets = connection.execute(
        """
        SELECT id, enrollment_id, attempt_id, generation, state, consumed_at,
               created_at
        FROM runtime_enrollment_tickets WHERE enrollment_id = ?
        """,
        (boundary["enrollment_id"],),
    ).fetchall()
    for ticket in tickets:
        ticket_attempt_id = str(ticket["attempt_id"] or "")
        if ticket_attempt_id == str(row["attempt_id"]):
            if (
                ticket["consumed_at"] is not None
                or str(ticket["state"]) not in {"revoked", "expired"}
                or int(ticket["generation"]) != int(boundary["enrollment_generation"])
            ):
                return False
        else:
            if not ticket_attempt_id or int(ticket["generation"]) >= int(
                boundary["enrollment_generation"]
            ):
                return False
            if not _prior_attempt_ticket_precedes_assignment(
                connection,
                ticket_id=str(ticket["id"]),
                enrollment_id=str(ticket["enrollment_id"]),
                ticket_attempt_id=ticket_attempt_id,
                ticket_generation=int(ticket["generation"]),
                current_attempt_id=str(row["attempt_id"]),
                assignment_message_id=str(assignment["id"]),
            ):
                return False

    proof = connection.execute(
        f"""
        SELECT COUNT(*) AS count,
               COALESCE(SUM(CASE
                 WHEN json_extract({event_data_json}, '$.attempt_id') = ?
                  AND json_extract({event_data_json}, '$.work_item_id') = ?
                  AND json_extract({event_data_json}, '$.worker_id') = ?
                  AND json_extract({event_data_json}, '$.runtime_session_id') = ?
                  AND json_extract({event_data_json}, '$.assignment_message_id') = ?
                  AND json_extract(
                        {event_data_json}, '$.assignment_delivery_generation'
                      ) = ?
                  AND json_extract({event_data_json}, '$.status_message_id') = ?
                  AND json_extract({event_data_json}, '$.status_sequence') = ?
                  AND json_extract({event_data_json}, '$.recovery_generation') = ?
                  AND json_extract({event_data_json}, '$.delivery_state') = ?
                  AND (
                    (
                      json_extract(
                        {event_data_json}, '$.prior_enrollment_generation'
                      ) + 1 = ?
                      AND json_extract({event_data_json}, '$.prior_runtime_state')
                          IN ('starting', 'ready', 'waiting', 'busy')
                      AND json_extract({event_data_json}, '$.prior_enrollment_state')
                          IN ('awaiting_handshake', 'ready', 'stale')
                    ) OR (
                      json_extract({event_data_json}, '$.legacy_terminal_residual') = 1
                      AND json_extract({event_data_json}, '$.observed_runtime_state') = ?
                      AND json_extract({event_data_json}, '$.observed_enrollment_state') = ?
                      AND json_extract(
                            {event_data_json}, '$.observed_enrollment_generation'
                          ) = ?
                    )
                  )
                 THEN 1 ELSE 0 END), 0) AS matching_count
        FROM events
        WHERE event_type = 'runtime.pre_dispatch_timeout_proven'
          AND aggregate_type = 'boundary' AND aggregate_id = ?
        """,
        (
            row["attempt_id"],
            row["work_item_id"],
            row["worker_id"],
            row["runtime_session_id"],
            assignment["id"],
            assignment["generation"],
            status["id"],
            status["sequence"],
            boundary["generation"],
            delivery_state,
            boundary["enrollment_generation"],
            boundary["runtime_state"],
            boundary["enrollment_state"],
            boundary["enrollment_generation"],
            row["id"],
        ),
    ).fetchone()
    return bool(
        proof is not None and int(proof["count"]) == 1 and int(proof["matching_count"]) == 1
    )


def _same_thread_recovery_is_executable(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    reason: str,
) -> bool:
    """Apply every no-MCP/no-effect fence before advertising disposition."""

    event_data_json = _safe_json_document_sql("events.data_json")

    if reason not in {
        "runtime_unavailable",
        "runtime_dispatch_failed",
        "runtime_provider_rate_limited",
    }:
        return False
    runtime_id = str(row["runtime_session_id"] or "")
    worker_id = str(row["worker_id"] or "")
    worker_thread_id = str(row["managed_worker_thread_id"] or "")
    raw_thread_generation = row["managed_worker_thread_generation"]
    worker_thread_generation = (
        int(raw_thread_generation) if raw_thread_generation is not None else None
    )
    if (
        not runtime_id
        or not worker_id
        or not worker_thread_id
        or worker_thread_generation is None
        or worker_thread_generation < 1
        or str(row["work_state"]) != "waiting_supervisor"
        or str(row["attention_owner"]) != "cao"
        or int(row["work_generation"]) != int(row["boundary_generation"])
        or str(row["attempt_state"]) != "waiting_supervisor"
    ):
        return False
    specs = connection.execute(
        """
        SELECT spec.id, spec.enrollment_id, thread.id AS thread_id,
               thread.generation AS thread_generation,
               epoch.runtime_session_id AS epoch_runtime_session_id,
               epoch.enrollment_id AS epoch_enrollment_id
        FROM managed_worker_specs AS spec
        JOIN managed_worker_threads AS thread
          ON thread.managed_spec_id = spec.id AND thread.state = 'active'
        JOIN managed_worker_thread_epochs AS epoch
          ON epoch.thread_id = thread.id
         AND epoch.generation = thread.generation
         AND epoch.retired_at IS NULL
        WHERE spec.principal_id = ? AND spec.runtime_session_id = ?
          AND thread.id = ? AND thread.generation = ?
          AND spec.state = 'enabled'
          AND epoch.runtime_session_id = spec.runtime_session_id
          AND epoch.enrollment_id = spec.enrollment_id
        """,
        (worker_id, runtime_id, worker_thread_id, worker_thread_generation),
    ).fetchall()
    if len(specs) != 1:
        return False
    try:
        metadata = json.loads(str(row["metadata_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    pre_dispatch_timeout = bool(
        isinstance(metadata, Mapping) and metadata.get("pre_dispatch_timeout") is True
    )
    assignment_not_submitted = bool(
        reason == "runtime_dispatch_failed"
        and _exact_assignment_not_submitted_is_proven(connection, row)
    )
    if pre_dispatch_timeout:
        if not _pre_dispatch_timeout_recovery_is_proven(connection, row):
            return False
    elif assignment_not_submitted:
        if not _managed_worker_pre_submit_lifecycle_is_proven(
            connection,
            worker_id=worker_id,
            runtime_id=runtime_id,
            attempt_id=str(row["attempt_id"]),
        ):
            return False
    elif not _managed_worker_pre_mcp_lifecycle_is_proven(
        connection,
        worker_id=worker_id,
        runtime_id=runtime_id,
        reason=reason,
    ):
        return False
    unsafe = connection.execute(
        f"""
        SELECT 1
        WHERE EXISTS (
            SELECT 1 FROM messages
            WHERE attempt_id = ? AND sender_id = ?
              AND kind IN (
                'question', 'blocker', 'progress', 'artifact',
                'completion_claim'
              )
        ) OR EXISTS (
            SELECT 1 FROM artifacts WHERE attempt_id = ?
        ) OR EXISTS (
            SELECT 1 FROM effect_operations
            WHERE principal_id = ? AND status IN ('started', 'unknown')
        ) OR EXISTS (
            SELECT 1 FROM work_items
            WHERE assigned_worker_id = ? AND id <> ?
              AND state NOT IN ('completed', 'canceled', 'failed')
              AND NOT (
                state = 'waiting_user'
                AND EXISTS (
                    SELECT 1 FROM attempts AS latest
                    WHERE latest.work_item_id = work_items.id
                      AND latest.state = 'completed'
                      AND latest.attempt_number = (
                          SELECT MAX(candidate.attempt_number)
                          FROM attempts AS candidate
                          WHERE candidate.work_item_id = work_items.id
                      )
                )
                AND NOT EXISTS (
                    SELECT 1 FROM boundaries AS boundary
                    LEFT JOIN boundary_dispositions AS disposition
                      ON disposition.boundary_id = boundary.id
                    LEFT JOIN boundary_supersessions AS supersession
                      ON supersession.boundary_id = boundary.id
                    WHERE boundary.work_item_id = work_items.id
                      AND disposition.id IS NULL
                      AND supersession.boundary_id IS NULL
                )
              )
        ) OR EXISTS (
            SELECT 1 FROM worker_enrollments
            WHERE principal_id = ? AND id <> ?
              AND (
                state IN ('awaiting_handshake', 'ready', 'stale')
                OR EXISTS (
                    SELECT 1 FROM runtime_credentials AS credential
                    WHERE credential.enrollment_id = worker_enrollments.id
                      AND credential.state = 'active'
                )
                OR EXISTS (
                    SELECT 1 FROM runtime_enrollment_tickets AS ticket
                    WHERE ticket.enrollment_id = worker_enrollments.id
                      AND ticket.state = 'pending'
                )
              )
        ) OR (
          NOT ? AND EXISTS (
            SELECT 1 FROM events
            WHERE event_type = 'managed_worker_thread.recovery_epoch_advanced'
              AND aggregate_type = 'managed_worker_thread'
              AND aggregate_id = ?
              AND json_extract({event_data_json}, '$.work_item_id') = ?
          )
        )
        LIMIT 1
        """,
        (
            row["attempt_id"],
            worker_id,
            row["attempt_id"],
            worker_id,
            worker_id,
            row["work_item_id"],
            worker_id,
            specs[0]["enrollment_id"],
            int(assignment_not_submitted),
            specs[0]["thread_id"],
            row["work_item_id"],
        ),
    ).fetchone()
    if unsafe is not None:
        return False

    deliveries = connection.execute(
        """
        SELECT delivery.message_id, delivery.recipient_id, delivery.state,
               delivery.generation, delivery.runtime_session_id,
               delivery.last_error, message.kind AS message_kind
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND delivery.recipient_id = ?
        ORDER BY message.sequence, delivery.recipient_id
        """,
        (row["attempt_id"], worker_id),
    ).fetchall()
    assignments = [
        delivery for delivery in deliveries if str(delivery["message_kind"]) == "assignment"
    ]
    if len(assignments) != 1:
        return False
    for delivery in deliveries:
        if str(delivery["runtime_session_id"] or "") != runtime_id:
            return False
        state = str(delivery["state"])
        if state in {"queued", "leased"}:
            continue
        last_error = str(delivery["last_error"] or "")
        if pre_dispatch_timeout and state == "dead" and last_error == "runtime_dispatch_failed":
            continue
        if (
            assignment_not_submitted
            and state == "dead"
            and last_error == "runtime_dispatch_pre_submit_failed"
        ):
            continue
        exact_failure = (
            reason == "runtime_provider_rate_limited"
            and last_error == "runtime_provider_rate_limited"
        ) or (
            reason == "runtime_dispatch_failed"
            and last_error
            in {
                "mcp_startup_timeout",
                "mcp_startup_failed",
                "runtime_timeout",
                "runtime_protocol_invalid_json",
                "runtime_process_exited",
                "runtime_dispatch_failed",
                "runtime_turn_failed",
            }
        )
        if (
            state != "dispatched"
            or not exact_failure
            or not _exact_pre_mcp_unknown_event_is_proven(
                connection,
                boundary_id=str(row["id"]),
                message_id=str(delivery["message_id"]),
                recipient_id=str(delivery["recipient_id"]),
                runtime_id=runtime_id,
                delivery_generation=int(delivery["generation"]),
                failure_code=last_error,
            )
        ):
            return False
    return True


def _completed_worker_turn_reconciliation_is_proven(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    delivery: Mapping[str, Any],
    *,
    reason: str,
) -> bool:
    """Prove one authoritative provider turn ended without a terminal report."""

    runtime_id = str(row["runtime_session_id"] or "")
    if str(delivery["runtime_session_id"] or "") != runtime_id or str(delivery["state"]) not in {
        "delivered",
        "acknowledged",
        "handled",
    }:
        return False
    completed_events = connection.execute(
        """
        SELECT event_type, aggregate_type, aggregate_id, causation_id, data_json
        FROM events
        WHERE (
            event_type = 'runtime.worker_turn_reconciliation_created'
            AND aggregate_type = 'work_item' AND aggregate_id = ?
        ) OR (
            event_type = 'runtime.message_delivered'
            AND aggregate_type = 'runtime' AND aggregate_id = ?
            AND causation_id = ?
        )
        ORDER BY sequence
        """,
        (row["work_item_id"], runtime_id, delivery["message_id"]),
    ).fetchall()
    reconciliation_events = [
        event
        for event in completed_events
        if str(event["event_type"]) == "runtime.worker_turn_reconciliation_created"
    ]
    delivery_events = [
        event
        for event in completed_events
        if str(event["event_type"]) == "runtime.message_delivered"
    ]
    if len(reconciliation_events) != 1 or len(delivery_events) != 1:
        return False
    try:
        reconciliation_data = json.loads(str(reconciliation_events[0]["data_json"]))
        delivery_data = json.loads(str(delivery_events[0]["data_json"]))
    except json.JSONDecodeError:
        return False
    result = delivery_data.get("result") if isinstance(delivery_data, dict) else None
    return bool(
        isinstance(reconciliation_data, dict)
        and reconciliation_data.get("boundary_id") == row["id"]
        and reconciliation_data.get("generation") == int(row["boundary_generation"])
        and reconciliation_data.get("reason") == reason
        and reconciliation_events[0]["causation_id"] == delivery["message_id"]
        and isinstance(delivery_data, dict)
        and delivery_data.get("message_id") == delivery["message_id"]
        and delivery_data.get("adapter") == "codex-app-server"
        and isinstance(result, dict)
        and result.get("success") is True
        and result.get("state") == "ready"
    )


def _same_thread_reconciliation_is_executable(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    reason: str,
) -> bool:
    """Prove an unknown post-MCP Codex Assignment can be reconciled in place.

    This is deliberately not a redelivery proof.  The old Assignment remains
    an unknown outcome.  The executable operation is a new, generation-fenced
    continuation on the exact provider-native thread, whose instruction first
    reconciles the task and workspace state before doing any remaining work.
    """

    completed_worker_turn = reason == "worker_turn_completed_without_terminal_report"
    if reason not in {
        "runtime_dispatch_failed",
        "worker_turn_completed_without_terminal_report",
    }:
        return False
    runtime_id = str(row["runtime_session_id"] or "")
    worker_id = str(row["worker_id"] or "")
    worker_thread_id = str(row["managed_worker_thread_id"] or "")
    raw_thread_generation = row["managed_worker_thread_generation"]
    worker_thread_generation = (
        int(raw_thread_generation) if raw_thread_generation is not None else None
    )
    if (
        not runtime_id
        or not worker_id
        or not worker_thread_id
        or worker_thread_generation is None
        or worker_thread_generation < 1
        or str(row["work_state"]) != "waiting_supervisor"
        or str(row["attention_owner"]) != "cao"
        or int(row["work_generation"]) != int(row["boundary_generation"])
        or str(row["attempt_state"]) != "waiting_supervisor"
    ):
        return False

    lanes = connection.execute(
        """
        SELECT spec.id, spec.enrollment_id, spec.adapter,
               thread.id AS thread_id, thread.generation AS thread_generation,
               epoch.runtime_session_id AS epoch_runtime_session_id,
               epoch.enrollment_id AS epoch_enrollment_id,
               runtime.native_session_id, runtime.state AS runtime_state,
               enrollment.state AS enrollment_state,
               (SELECT COUNT(*) FROM runtime_credentials AS credential
                WHERE credential.enrollment_id = enrollment.id
                  AND credential.state = 'active') AS active_credentials,
               (SELECT COUNT(*) FROM runtime_enrollment_tickets AS ticket
                WHERE ticket.enrollment_id = enrollment.id
                  AND ticket.state = 'pending') AS pending_tickets
        FROM managed_worker_specs AS spec
        JOIN managed_worker_threads AS thread
          ON thread.managed_spec_id = spec.id AND thread.state = 'active'
        JOIN managed_worker_thread_epochs AS epoch
          ON epoch.thread_id = thread.id
         AND epoch.generation = thread.generation
         AND epoch.retired_at IS NULL
        JOIN runtime_sessions AS runtime
          ON runtime.id = epoch.runtime_session_id
         AND runtime.principal_id = spec.principal_id
        JOIN worker_enrollments AS enrollment
          ON enrollment.id = epoch.enrollment_id
         AND enrollment.runtime_session_id = runtime.id
         AND enrollment.principal_id = spec.principal_id
        WHERE spec.principal_id = ? AND spec.runtime_session_id = ?
          AND thread.id = ? AND thread.generation = ?
          AND spec.state = 'enabled'
          AND epoch.runtime_session_id = spec.runtime_session_id
          AND epoch.enrollment_id = spec.enrollment_id
        """,
        (worker_id, runtime_id, worker_thread_id, worker_thread_generation),
    ).fetchall()
    if len(lanes) != 1:
        return False
    lane = lanes[0]
    runtime_states = (
        {"waiting", "failed", "missing", "stopped"}
        if completed_worker_turn
        else {"failed", "missing", "stopped"}
    )
    enrollment_states = (
        {"ready", "failed", "revoked", "stale"}
        if completed_worker_turn
        else {"failed", "revoked", "stale"}
    )
    if (
        str(lane["adapter"]) != "codex-app-server"
        or not str(lane["native_session_id"] or "")
        or str(lane["runtime_state"]) not in runtime_states
        or str(lane["enrollment_state"]) not in enrollment_states
        or int(lane["active_credentials"])
        or int(lane["pending_tickets"])
    ):
        return False

    unsafe = connection.execute(
        """
        SELECT 1
        WHERE (
            ? = 0 AND (
                EXISTS (
                    SELECT 1 FROM messages
                    WHERE attempt_id = ? AND sender_id = ?
                      AND kind IN (
                        'question', 'blocker', 'progress', 'artifact',
                        'completion_claim'
                      )
                ) OR EXISTS (
                    SELECT 1 FROM artifacts WHERE attempt_id = ?
                )
            )
        ) OR EXISTS (
            SELECT 1 FROM effect_operations
            WHERE status IN ('started', 'unknown')
              AND (principal_id = ? OR cleanup_work_item_id = ?)
        ) OR EXISTS (
            SELECT 1 FROM work_items
            WHERE assigned_worker_id = ? AND id <> ?
              AND state NOT IN ('completed', 'canceled', 'failed')
              AND NOT (
                state = 'waiting_user'
                AND EXISTS (
                    SELECT 1 FROM attempts AS latest
                    WHERE latest.work_item_id = work_items.id
                      AND latest.state = 'completed'
                      AND latest.attempt_number = (
                          SELECT MAX(candidate.attempt_number)
                          FROM attempts AS candidate
                          WHERE candidate.work_item_id = work_items.id
                      )
                )
                AND NOT EXISTS (
                    SELECT 1 FROM boundaries AS boundary
                    LEFT JOIN boundary_dispositions AS disposition
                      ON disposition.boundary_id = boundary.id
                    LEFT JOIN boundary_supersessions AS supersession
                      ON supersession.boundary_id = boundary.id
                    WHERE boundary.work_item_id = work_items.id
                      AND disposition.id IS NULL
                      AND supersession.boundary_id IS NULL
                )
              )
        ) OR EXISTS (
            SELECT 1 FROM worker_enrollments
            WHERE principal_id = ? AND id <> ?
              AND (
                state IN ('awaiting_handshake', 'ready', 'stale')
                OR EXISTS (
                    SELECT 1 FROM runtime_credentials AS credential
                    WHERE credential.enrollment_id = worker_enrollments.id
                      AND credential.state = 'active'
                )
                OR EXISTS (
                    SELECT 1 FROM runtime_enrollment_tickets AS ticket
                    WHERE ticket.enrollment_id = worker_enrollments.id
                      AND ticket.state = 'pending'
                )
              )
        ) OR EXISTS (
            SELECT 1 FROM events
            WHERE event_type = 'managed_worker_thread.reconciliation_epoch_advanced'
              AND aggregate_type = 'managed_worker_thread'
              AND aggregate_id = ?
              AND json_extract(data_json, '$.source_attempt_id') = ?
        )
        LIMIT 1
        """,
        (
            int(completed_worker_turn),
            row["attempt_id"],
            worker_id,
            row["attempt_id"],
            worker_id,
            row["work_item_id"],
            worker_id,
            row["work_item_id"],
            worker_id,
            lane["enrollment_id"],
            worker_thread_id,
            row["attempt_id"],
        ),
    ).fetchone()
    if unsafe is not None:
        return False

    deliveries = connection.execute(
        """
        SELECT delivery.message_id, delivery.recipient_id, delivery.state,
               delivery.generation, delivery.runtime_session_id,
               delivery.last_error, message.kind AS message_kind
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND delivery.recipient_id = ?
        ORDER BY message.sequence, delivery.recipient_id
        """,
        (row["attempt_id"], worker_id),
    ).fetchall()
    assignments = [
        delivery for delivery in deliveries if str(delivery["message_kind"]) == "assignment"
    ]
    if len(deliveries) != 1 or len(assignments) != 1:
        return False
    delivery = assignments[0]
    if completed_worker_turn:
        return _completed_worker_turn_reconciliation_is_proven(
            connection,
            row,
            delivery,
            reason=reason,
        )

    failure_code = str(delivery["last_error"] or "")
    if (
        str(delivery["runtime_session_id"] or "") != runtime_id
        or str(delivery["state"]) != "dispatched"
        or failure_code
        not in {
            "mcp_startup_timeout",
            "mcp_startup_failed",
            "runtime_timeout",
            "runtime_protocol_invalid_json",
            "runtime_process_exited",
            "runtime_dispatch_failed",
            "runtime_turn_failed",
            "worker_inactive_timeout",
        }
    ):
        return False
    return _exact_pre_mcp_unknown_event_is_proven(
        connection,
        boundary_id=str(row["id"]),
        message_id=str(delivery["message_id"]),
        recipient_id=worker_id,
        runtime_id=runtime_id,
        delivery_generation=int(delivery["generation"]),
        failure_code=failure_code,
    )


def _repair_legacy_pre_dispatch_timeout_recoveries(
    connection: sqlite3.Connection,
) -> None:
    """Fence exact v30 watchdog rows before deriving an executable action."""

    boundary_metadata_json = _safe_json_document_sql("boundary.metadata_json")
    event_data_json = _safe_json_document_sql("events.data_json")

    candidates = connection.execute(
        f"""
        SELECT boundary.*, attempt.worker_id, attempt.runtime_session_id,
               attempt.created_at AS attempt_created_at,
               attempt.state AS attempt_state,
               work.state AS work_state, work.attention_owner,
               work.generation AS work_generation,
               runtime.state AS runtime_state,
               boundary.runtime_state AS boundary_runtime_state,
               enrollment.id AS enrollment_id,
               enrollment.state AS enrollment_state,
               enrollment.generation AS enrollment_generation
        FROM boundaries AS boundary
        JOIN attempts AS attempt ON attempt.id = boundary.attempt_id
        JOIN work_items AS work ON work.id = boundary.work_item_id
        JOIN runtime_sessions AS runtime ON runtime.id = attempt.runtime_session_id
        JOIN worker_enrollments AS enrollment
          ON enrollment.runtime_session_id = runtime.id
         AND enrollment.principal_id = attempt.worker_id
         AND enrollment.managed = 1
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        WHERE boundary.kind = 'failure'
          AND json_extract({boundary_metadata_json}, '$.runtime_recovery') = 1
          AND json_extract({boundary_metadata_json}, '$.pre_dispatch_timeout') = 1
          AND json_extract({boundary_metadata_json}, '$.reason')
              = 'runtime_dispatch_failed'
          AND disposition.id IS NULL AND supersession.boundary_id IS NULL
          AND (
            (
              runtime.state IN ('starting', 'ready', 'waiting', 'busy')
              AND enrollment.state IN ('awaiting_handshake', 'ready', 'stale')
            ) OR (
              runtime.state IN ('stopped', 'failed', 'missing')
              AND enrollment.state IN ('revoked', 'failed', 'stale')
              AND NOT EXISTS (
                SELECT 1 FROM runtime_credentials AS active
                WHERE active.enrollment_id = enrollment.id
                  AND active.state = 'active'
              )
              AND NOT EXISTS (
                SELECT 1 FROM runtime_enrollment_tickets AS pending
                WHERE pending.enrollment_id = enrollment.id
                  AND pending.state = 'pending'
              )
            )
          )
          AND NOT EXISTS (
            SELECT 1 FROM events AS proof
            WHERE proof.event_type = 'runtime.pre_dispatch_timeout_proven'
              AND proof.aggregate_type = 'boundary'
              AND proof.aggregate_id = boundary.id
          )
        ORDER BY boundary.created_at, boundary.id
        """
    ).fetchall()
    for row in candidates:
        runtime_already_terminal = str(row["runtime_state"]) in {
            "stopped",
            "failed",
            "missing",
        }
        enrollment_already_terminal = str(row["enrollment_state"]) in {
            "revoked",
            "failed",
        }
        terminal_generation = int(row["enrollment_generation"]) + (
            0 if enrollment_already_terminal else 1
        )
        if (
            str(row["work_state"]) != "waiting_supervisor"
            or str(row["attention_owner"]) != "cao"
            or int(row["work_generation"]) != int(row["generation"])
            or str(row["attempt_state"]) != "waiting_supervisor"
            or str(row["source_principal_id"]) != str(row["worker_id"])
        ):
            continue
        latest_attempt = connection.execute(
            "SELECT id FROM attempts WHERE work_item_id = ? ORDER BY attempt_number DESC LIMIT 1",
            (row["work_item_id"],),
        ).fetchone()
        if latest_attempt is None or str(latest_attempt["id"]) != str(row["attempt_id"]):
            continue
        boundary_events = connection.execute(
            f"""
                SELECT sequence FROM events
                WHERE event_type = 'boundary.recorded'
                  AND aggregate_type = 'work_item' AND aggregate_id = ?
                  AND json_extract({event_data_json}, '$.boundary_id') = ?
                """,
            (row["work_item_id"], row["id"]),
        ).fetchall()
        if len(boundary_events) != 1:
            continue
        boundary_event_sequence = int(boundary_events[0]["sequence"])

        source_prefix = "unstarted-status:"
        source_suffix = f":{row['attempt_id']}"
        source_event_id = str(row["source_event_id"] or "")
        if not source_event_id.startswith(source_prefix) or not source_event_id.endswith(
            source_suffix
        ):
            continue
        status_id = source_event_id[len(source_prefix) : -len(source_suffix)]
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except (TypeError, ValueError):
            continue
        boundary_input = {
            "source_event_id": source_event_id,
            "work_item_id": str(row["work_item_id"]),
            "attempt_id": str(row["attempt_id"]),
            "expected_goal_version": int(row["goal_version"]),
            "expected_goal_packet_digest": str(row["goal_packet_digest"]),
            "expected_task_packet_digest": str(row["task_packet_digest"]),
            "expected_generation": int(row["generation"]),
            "kind": str(row["kind"]),
            "summary": str(row["summary"]),
            "runtime_state": str(row["boundary_runtime_state"]),
            "metadata": dict(metadata),
        }
        if canonical_digest(boundary_input) != str(row["input_digest"]):
            continue
        deliveries = connection.execute(
            """
            SELECT message.id, message.kind, message.sequence, message.payload_json,
                   message.payload_digest,
                   delivery.generation, delivery.state, delivery.last_error,
                   delivery.runtime_session_id
            FROM messages AS message
            JOIN message_deliveries AS delivery ON delivery.message_id = message.id
            WHERE message.attempt_id = ? AND delivery.recipient_id = ?
            ORDER BY message.sequence
            """,
            (row["attempt_id"], row["worker_id"]),
        ).fetchall()
        assignments = [item for item in deliveries if str(item["kind"]) == "assignment"]
        statuses = [item for item in deliveries if str(item["kind"]) == "status_request"]
        if (
            len(assignments) != 1
            or not statuses
            or str(statuses[-1]["id"]) != status_id
            or any(
                str(item["state"]) != "dead"
                or str(item["last_error"] or "") != "runtime_dispatch_failed"
                or str(item["runtime_session_id"] or "") != str(row["runtime_session_id"])
                for item in deliveries
            )
        ):
            continue
        assignment, status = assignments[0], statuses[-1]
        try:
            assignment_payload = json.loads(str(assignment["payload_json"] or "{}"))
            status_payload = json.loads(str(status["payload_json"] or "{}"))
        except (TypeError, ValueError):
            continue
        if (
            not isinstance(assignment_payload, Mapping)
            or not isinstance(status_payload, Mapping)
            or assignment_payload.get("goal_version") != int(row["goal_version"])
            or assignment_payload.get("generation") != int(row["generation"]) - 1
            or assignment_payload.get("goal_packet_digest") != str(row["goal_packet_digest"])
            or assignment_payload.get("task_packet_digest") != str(row["task_packet_digest"])
            or assignment_payload.get("work_item_id") != str(row["work_item_id"])
            or assignment_payload.get("attempt_id") != str(row["attempt_id"])
            or canonical_digest(assignment_payload) != str(assignment["payload_digest"])
            or status_payload.get("generation") != int(row["generation"]) - 1
            or not isinstance(status_payload.get("response_due_at"), str)
            or str(status_payload["response_due_at"]) > str(row["created_at"])
            or canonical_digest(status_payload) != str(status["payload_digest"])
        ):
            continue
        if any(
            int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM events
                    WHERE event_type = 'message.delivery_superseded'
                      AND aggregate_type = 'message' AND aggregate_id = ?
                      AND causation_id = ?
                      AND json_extract({event_data_json}, '$.reason_code')
                          = 'runtime_dispatch_failed'
                      AND sequence < ?
                    """,
                    (item["id"], item["id"], boundary_event_sequence),
                ).fetchone()[0]
            )
            != 1
            for item in deliveries
        ):
            continue
        unsafe = connection.execute(
            """
            SELECT 1
            WHERE EXISTS (
                SELECT 1 FROM messages
                WHERE attempt_id = ? AND sender_id = ?
                  AND kind IN (
                    'question', 'blocker', 'progress', 'artifact',
                    'completion_claim'
                  )
            ) OR EXISTS (
                SELECT 1 FROM artifacts WHERE attempt_id = ?
            ) OR EXISTS (
                SELECT 1 FROM effect_operations
                WHERE principal_id = ? AND status IN ('started', 'unknown')
            ) OR EXISTS (
                SELECT 1 FROM work_items
                WHERE assigned_worker_id = ? AND id <> ?
                  AND state NOT IN ('completed', 'canceled', 'failed')
            )
            LIMIT 1
            """,
            (
                row["attempt_id"],
                row["worker_id"],
                row["attempt_id"],
                row["worker_id"],
                row["worker_id"],
                row["work_item_id"],
            ),
        ).fetchone()
        if unsafe is not None:
            continue

        tickets = connection.execute(
            """
            SELECT id, enrollment_id, attempt_id, generation, state,
                   consumed_at, created_at
            FROM runtime_enrollment_tickets WHERE enrollment_id = ?
            """,
            (row["enrollment_id"],),
        ).fetchall()
        tickets_safe = True
        for ticket in tickets:
            ticket_attempt = str(ticket["attempt_id"] or "")
            if ticket_attempt == str(row["attempt_id"]):
                tickets_safe = bool(
                    ticket["consumed_at"] is None
                    and str(ticket["state"])
                    in (
                        {"revoked", "expired"}
                        if runtime_already_terminal
                        else {"pending", "revoked", "expired"}
                    )
                    and int(ticket["generation"]) == terminal_generation
                )
            else:
                tickets_safe = bool(
                    ticket_attempt
                    and int(ticket["generation"]) < terminal_generation
                    and _prior_attempt_ticket_precedes_assignment(
                        connection,
                        ticket_id=str(ticket["id"]),
                        enrollment_id=str(ticket["enrollment_id"]),
                        ticket_attempt_id=ticket_attempt,
                        ticket_generation=int(ticket["generation"]),
                        current_attempt_id=str(row["attempt_id"]),
                        assignment_message_id=str(assignment["id"]),
                    )
                )
            if not tickets_safe:
                break
        if not tickets_safe:
            continue
        active_credentials = connection.execute(
            """
            SELECT generation, created_at FROM runtime_credentials
            WHERE enrollment_id = ? AND state = 'active'
            """,
            (row["enrollment_id"],),
        ).fetchall()
        if enrollment_already_terminal and active_credentials:
            continue
        if len(active_credentials) > 1 or any(
            int(credential["generation"]) != int(row["enrollment_generation"])
            for credential in active_credentials
        ):
            continue
        if active_credentials:
            prior_credential_tickets = connection.execute(
                """
                SELECT ticket.id, ticket.enrollment_id, ticket.attempt_id,
                       ticket.generation
                FROM runtime_enrollment_tickets AS ticket
                JOIN attempts AS attempt ON attempt.id = ticket.attempt_id
                JOIN work_items AS work ON work.id = attempt.work_item_id
                WHERE ticket.enrollment_id = ? AND ticket.generation = ?
                  AND ticket.state = 'consumed' AND ticket.consumed_at IS NOT NULL
                  AND ticket.attempt_id <> ?
                  AND work.state IN ('completed', 'canceled', 'failed')
                """,
                (
                    row["enrollment_id"],
                    row["enrollment_generation"],
                    row["attempt_id"],
                ),
            ).fetchall()
            if len(prior_credential_tickets) != 1:
                continue
            prior_credential_ticket = prior_credential_tickets[0]
            if not _prior_attempt_ticket_precedes_assignment(
                connection,
                ticket_id=str(prior_credential_ticket["id"]),
                enrollment_id=str(prior_credential_ticket["enrollment_id"]),
                ticket_attempt_id=str(prior_credential_ticket["attempt_id"]),
                ticket_generation=int(prior_credential_ticket["generation"]),
                current_attempt_id=str(row["attempt_id"]),
                assignment_message_id=str(assignment["id"]),
            ):
                continue

        now = utc_now()
        prior_enrollment_generation = int(row["enrollment_generation"])
        savepoint = f"legacy_watchdog_{uuid.uuid4().hex}"
        connection.execute(f"SAVEPOINT {savepoint}")
        if not enrollment_already_terminal:
            terminalized = connection.execute(
                """
            UPDATE worker_enrollments
            SET state = 'failed', generation = generation + 1,
                revoked_at = ?, updated_at = ?
            WHERE id = ? AND generation = ?
              AND state IN ('awaiting_handshake', 'ready', 'stale')
            """,
                (
                    now,
                    now,
                    row["enrollment_id"],
                    prior_enrollment_generation,
                ),
            ).rowcount
            if terminalized != 1:
                connection.execute(f"ROLLBACK TO {savepoint}")
                connection.execute(f"RELEASE {savepoint}")
                continue
            credentials_revoked = connection.execute(
                """
            UPDATE runtime_credentials
            SET state = 'revoked', revoked_at = ?, updated_at = ?
            WHERE enrollment_id = ? AND state = 'active'
            """,
                (now, now, row["enrollment_id"]),
            ).rowcount
            if credentials_revoked != len(active_credentials):
                connection.execute(f"ROLLBACK TO {savepoint}")
                connection.execute(f"RELEASE {savepoint}")
                continue
            pending_ticket_count = sum(1 for ticket in tickets if str(ticket["state"]) == "pending")
            tickets_revoked = connection.execute(
                """
            UPDATE runtime_enrollment_tickets
            SET state = 'revoked', updated_at = ?
            WHERE enrollment_id = ? AND state = 'pending'
            """,
                (now, row["enrollment_id"]),
            ).rowcount
            if tickets_revoked != pending_ticket_count:
                connection.execute(f"ROLLBACK TO {savepoint}")
                connection.execute(f"RELEASE {savepoint}")
                continue
        if not runtime_already_terminal:
            runtime_terminalized = connection.execute(
                """
            UPDATE runtime_sessions SET state = 'failed', updated_at = ?
            WHERE id = ? AND state IN ('starting', 'ready', 'waiting', 'busy')
            """,
                (now, row["runtime_session_id"]),
            ).rowcount
            if runtime_terminalized != 1:
                connection.execute(f"ROLLBACK TO {savepoint}")
                connection.execute(f"RELEASE {savepoint}")
                continue
        connection.execute(
            """
            INSERT INTO events(
                id, event_type, aggregate_type, aggregate_id,
                actor_id, data_json, causation_id, created_at
            ) VALUES(?, 'runtime.pre_dispatch_timeout_proven', 'boundary',
                     ?, '', ?, ?, ?)
            """,
            (
                f"evt_{uuid.uuid4().hex}",
                row["id"],
                canonical_json(
                    {
                        "attempt_id": str(row["attempt_id"]),
                        "work_item_id": str(row["work_item_id"]),
                        "worker_id": str(row["worker_id"]),
                        "runtime_session_id": str(row["runtime_session_id"]),
                        "assignment_message_id": str(assignment["id"]),
                        "assignment_delivery_generation": int(assignment["generation"]),
                        "status_message_id": str(status["id"]),
                        "status_sequence": int(status["sequence"]),
                        "recovery_generation": int(row["generation"]),
                        "delivery_state": "dead",
                        "legacy_schema_version": 30,
                        **(
                            {
                                "legacy_terminal_residual": True,
                                "observed_runtime_state": (
                                    str(row["runtime_state"])
                                    if runtime_already_terminal
                                    else "failed"
                                ),
                                "observed_enrollment_state": (
                                    str(row["enrollment_state"])
                                    if enrollment_already_terminal
                                    else "failed"
                                ),
                                "observed_enrollment_generation": terminal_generation,
                            }
                            if runtime_already_terminal
                            else {
                                "prior_runtime_state": str(row["runtime_state"]),
                                "prior_enrollment_state": str(row["enrollment_state"]),
                                "prior_enrollment_generation": (prior_enrollment_generation),
                            }
                        ),
                    }
                ),
                row["source_event_id"],
                now,
            ),
        )
        connection.execute(f"RELEASE {savepoint}")


def _ensure_event_operator_scope_schema(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Make Dashboard event visibility immutable and wake legacy surfaces once."""

    columns = {str(column["name"]) for column in connection.execute("PRAGMA table_info(events)")}
    column_was_added = "operator_scope" not in columns
    if column_was_added:
        connection.execute(
            "ALTER TABLE events ADD COLUMN operator_scope TEXT NOT NULL "
            "DEFAULT 'unclassified' CHECK(operator_scope IN "
            "('production', 'acceptance-test', 'system', 'unclassified'))"
        )
    if not column_was_added and existing_schema_version >= 39:
        return

    backfill_event_operator_scopes_tx(connection)
    if existing_schema_version > 0:
        now = utc_now()
        connection.execute(
            """
            INSERT INTO events(
                id, event_type, aggregate_type, aggregate_id, operator_scope,
                actor_id, data_json, correlation_id, causation_id, created_at
            ) VALUES(?, 'dashboard.resync_requested', 'control_plane', 'dashboard',
                     'production', '', ?, '', '', ?)
            """,
            (
                f"evt_{uuid.uuid4().hex}",
                json.dumps(
                    {"schema_version": SCHEMA_VERSION},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                now,
            ),
        )


def _supersede_boundary_deliveries(
    connection: sqlite3.Connection,
    *,
    boundary_id: str,
    reason: str,
) -> None:
    """Make one durably superseded Boundary wake non-deliverable.

    This is deliberately idempotent so startup also repairs databases written
    by an earlier build which persisted the Boundary supersession but did not
    finish retiring its queued CAO delivery.
    """

    message_payload_json = _safe_json_document_sql("message.payload_json")

    connection.execute(
        "UPDATE reasoner_turns SET state = 'abandoned', updated_at = ? "
        "WHERE boundary_id = ? AND state = 'leased'",
        (utc_now(), boundary_id),
    )

    deliveries = connection.execute(
        f"""
        SELECT delivery.message_id, delivery.recipient_id
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE json_extract({message_payload_json}, '$.boundary_id') = ?
          AND delivery.state IN (
            'queued', 'leased', 'delivered', 'acknowledged'
          )
        """,
        (boundary_id,),
    ).fetchall()
    reason_digest = canonical_digest(reason)
    for delivery in deliveries:
        now = utc_now()
        cursor = connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'dead', lease_until = NULL, owner_token = '',
                last_error = ?, updated_at = ?
            WHERE message_id = ? AND recipient_id = ?
              AND state IN (
                'queued', 'leased', 'delivered', 'acknowledged'
              )
            """,
            (
                f"superseded;evidence_digest={reason_digest}",
                now,
                delivery["message_id"],
                delivery["recipient_id"],
            ),
        )
        if cursor.rowcount != 1:
            continue
        connection.execute(
            """
            INSERT INTO events(
                id, event_type, aggregate_type, aggregate_id,
                actor_id, data_json, causation_id, created_at
            ) VALUES(?, 'message.delivery_superseded', 'message',
                     ?, ?, ?, ?, ?)
            """,
            (
                f"evt_{uuid.uuid4().hex}",
                delivery["message_id"],
                delivery["recipient_id"],
                canonical_json(
                    {
                        "boundary_id": boundary_id,
                        "reason_digest": reason_digest,
                    }
                ),
                boundary_id,
                now,
            ),
        )


def _backfill_goal_replacement_boundary_supersessions(
    connection: sqlite3.Connection,
) -> None:
    """Close exact old-generation Boundaries after an authoritative Goal replacement.

    Goal revisions and their successor Attempts are immutable authority.  A
    schema-40 writer could commit that complete replacement lineage while
    leaving the prior Boundary open.  Repair only a unique, fully joined
    Boundary-event/revision/Attempt/replacement-event chain; ambiguity remains
    visible instead of being guessed away.
    """

    for supersession in connection.execute(
        "SELECT boundary_id FROM boundary_supersessions WHERE reason = 'goal_replaced'"
    ):
        _supersede_boundary_deliveries(
            connection,
            boundary_id=str(supersession["boundary_id"]),
            reason="goal_replaced",
        )

    boundary_event_json = _safe_json_document_sql("boundary_event.data_json")
    replacement_event_json = _safe_json_document_sql("replacement_event.data_json")
    rows = connection.execute(
        f"""
        SELECT boundary.id AS boundary_id,
               boundary_event.sequence AS boundary_event_sequence,
               replacement_event.sequence AS replacement_event_sequence
        FROM boundaries AS boundary
        JOIN work_items AS work ON work.id = boundary.work_item_id
        JOIN events AS boundary_event
          ON boundary_event.event_type = 'boundary.recorded'
         AND boundary_event.aggregate_type = 'work_item'
         AND boundary_event.aggregate_id = boundary.work_item_id
         AND json_extract(
               {boundary_event_json}, '$.boundary_id'
             ) = boundary.id
        JOIN events AS replacement_event
          ON replacement_event.event_type = 'work.goal_replaced'
         AND replacement_event.aggregate_type = 'work_item'
         AND replacement_event.aggregate_id = boundary.work_item_id
         AND replacement_event.sequence > boundary_event.sequence
         AND CAST(json_extract(
               {replacement_event_json}, '$.version'
             ) AS INTEGER) = boundary.goal_version + 1
         AND CAST(json_extract(
               {replacement_event_json}, '$.generation'
             ) AS INTEGER) = boundary.generation + 1
        JOIN goal_revisions AS replacement_goal
          ON replacement_goal.work_item_id = boundary.work_item_id
         AND replacement_goal.version = CAST(json_extract(
               {replacement_event_json}, '$.version'
             ) AS INTEGER)
         AND replacement_goal.source_directive_id = json_extract(
               {replacement_event_json}, '$.directive_id'
             )
        JOIN attempts AS successor_attempt
          ON successor_attempt.id = json_extract(
               {replacement_event_json}, '$.attempt_id'
             )
         AND successor_attempt.work_item_id = boundary.work_item_id
         AND successor_attempt.goal_version = replacement_goal.version
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        WHERE disposition.id IS NULL
          AND supersession.boundary_id IS NULL
          AND work.goal_version >= replacement_goal.version
          AND work.generation >= boundary.generation + 1
        ORDER BY boundary.id, replacement_event.sequence
        """
    ).fetchall()
    candidates: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        candidates.setdefault(str(row["boundary_id"]), []).append(row)
    for boundary_id, matches in candidates.items():
        if len(matches) != 1:
            continue
        match = matches[0]
        connection.execute(
            """
            INSERT INTO boundary_supersessions(
                boundary_id, boundary_event_sequence,
                superseding_event_sequence, reason, created_at
            ) VALUES(?, ?, ?, 'goal_replaced', ?)
            """,
            (
                boundary_id,
                match["boundary_event_sequence"],
                match["replacement_event_sequence"],
                utc_now(),
            ),
        )
        _supersede_boundary_deliveries(
            connection,
            boundary_id=boundary_id,
            reason="goal_replaced",
        )


def _backfill_managed_worker_recovery_actions(
    connection: sqlite3.Connection,
    settings: Settings,
) -> None:
    """Reconcile open recovery routes from current durable proof and Settings.

    Recovery columns and Attempt routing fields are derived state.  Sealed
    Boundary metadata/input digests and Message payload/digests are never
    rewritten.  Running this on every startup makes configuration drift fail
    closed instead of preserving an action that is no longer executable.
    """

    opening_data_json = _safe_json_document_sql("opening.data_json")
    boundary_metadata_json = _safe_json_document_sql("boundary.metadata_json")
    message_payload_json = _safe_json_document_sql("message.payload_json")

    _repair_legacy_pre_dispatch_timeout_recoveries(connection)

    already_superseded = connection.execute(
        """
        SELECT boundary_id FROM boundary_supersessions
        WHERE reason = 'recovery_boundary_replaced'
        """
    ).fetchall()
    for supersession in already_superseded:
        _supersede_boundary_deliveries(
            connection,
            boundary_id=str(supersession["boundary_id"]),
            reason="recovery_boundary_replaced",
        )

    rows = connection.execute(
        f"""
        SELECT boundary.id, boundary.metadata_json, boundary.work_item_id,
               boundary.attempt_id, boundary.generation AS boundary_generation,
               attempt.worker_id, attempt.runtime_session_id,
               attempt.state AS attempt_state, work.goal_version,
               work.state AS work_state, work.attention_owner,
               work.generation AS work_generation,
               work.managed_worker_thread_id,
               work.managed_worker_thread_generation,
               (
                 SELECT COUNT(*) FROM events AS opening
                 WHERE opening.event_type = 'boundary.recorded'
                   AND opening.aggregate_type = 'work_item'
                   AND opening.aggregate_id = boundary.work_item_id
                   AND json_extract({opening_data_json}, '$.boundary_id') = boundary.id
               ) AS boundary_event_count,
               (
                 SELECT opening.sequence FROM events AS opening
                 WHERE opening.event_type = 'boundary.recorded'
                   AND opening.aggregate_type = 'work_item'
                   AND opening.aggregate_id = boundary.work_item_id
                   AND json_extract({opening_data_json}, '$.boundary_id') = boundary.id
                 ORDER BY opening.sequence DESC LIMIT 1
               ) AS boundary_event_sequence,
               spec.catalog_target_id, spec.workspace_ref,
               spec.provider_scope_digest, spec.adapter,
               spec.effective_model, spec.effective_reasoning_effort
        FROM boundaries AS boundary
        JOIN attempts AS attempt ON attempt.id = boundary.attempt_id
        JOIN work_items AS work ON work.id = boundary.work_item_id
        LEFT JOIN managed_worker_specs AS spec
          ON spec.principal_id = attempt.worker_id
         AND spec.runtime_session_id = attempt.runtime_session_id
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        WHERE boundary.kind = 'failure'
          AND (
            json_extract({boundary_metadata_json}, '$.runtime_recovery') = 1
            OR json_extract({boundary_metadata_json}, '$.system_recovery') = 1
          )
          AND disposition.id IS NULL
          AND supersession.boundary_id IS NULL
        ORDER BY boundary.created_at, boundary.id
        """
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["attempt_id"]), []).append(row)
    canonical_ids: set[str] = set()
    for attempt_rows in grouped.values():
        if len(attempt_rows) == 1:
            canonical_ids.add(str(attempt_rows[0]["id"]))
            continue
        if (
            any(int(item["boundary_event_count"]) != 1 for item in attempt_rows)
            or len({str(item["work_item_id"]) for item in attempt_rows}) != 1
            or len({int(item["boundary_generation"]) for item in attempt_rows}) != 1
            or any(
                connection.execute(
                    f"""
                    SELECT 1 FROM message_deliveries AS delivery
                    JOIN messages AS message ON message.id = delivery.message_id
                    WHERE json_extract({message_payload_json}, '$.boundary_id') = ?
                      AND delivery.state = 'dispatched'
                    LIMIT 1
                    """,
                    (item["id"],),
                ).fetchone()
                is not None
                for item in attempt_rows
            )
        ):
            continue
        canonical = max(attempt_rows, key=lambda item: int(item["boundary_event_sequence"]))
        canonical_ids.add(str(canonical["id"]))
        for stale in attempt_rows:
            if str(stale["id"]) == str(canonical["id"]):
                continue
            connection.execute(
                """
                INSERT INTO boundary_supersessions(
                    boundary_id, boundary_event_sequence,
                    superseding_event_sequence, reason, created_at
                ) VALUES(?, ?, ?, 'recovery_boundary_replaced', ?)
                ON CONFLICT(boundary_id) DO NOTHING
                """,
                (
                    stale["id"],
                    stale["boundary_event_sequence"],
                    canonical["boundary_event_sequence"],
                    utc_now(),
                ),
            )
            _supersede_boundary_deliveries(
                connection,
                boundary_id=str(stale["id"]),
                reason="recovery_boundary_replaced",
            )
    attempt_counts = {
        attempt_id: sum(1 for item in attempt_rows if str(item["id"]) in canonical_ids)
        for attempt_id, attempt_rows in grouped.items()
    }

    attempt_routes: dict[str, tuple[str, str]] = {}
    for row in rows:
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except json.JSONDecodeError:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        reason = str(metadata.get("reason") or "")
        runtime_recovery = metadata.get("runtime_recovery") is True
        system_recovery = metadata.get("system_recovery") is True
        action = "system_reconciliation"
        stage = "system_reconciliation"
        next_boundary = "system_reconciliation"
        recovery_target = ""
        recovery_model = ""
        recovery_effort = ""
        if str(row["id"]) in canonical_ids and attempt_counts[str(row["attempt_id"])] == 1:
            ambiguous_runtime_ownership = metadata.get("ambiguous_runtime_ownership") is True
            if (
                not ambiguous_runtime_ownership
                and runtime_recovery
                and reason
                in {
                    "runtime_unavailable",
                    "runtime_dispatch_failed",
                    "runtime_provider_rate_limited",
                }
                and _same_thread_recovery_is_executable(connection, row, reason=reason)
            ):
                action = "dispose_continue_or_correct"
                stage = "runtime_recovery"
                next_boundary = "cao_disposition"
            elif (
                (runtime_recovery and reason == "runtime_dispatch_failed")
                or (system_recovery and reason == "worker_turn_completed_without_terminal_report")
            ) and _same_thread_reconciliation_is_executable(
                connection,
                row,
                reason=reason,
            ):
                action = "reconcile_continue_same_thread"
                stage = "runtime_reconciliation"
                next_boundary = "cao_disposition"
        connection.execute(
            "UPDATE boundaries SET recovery_action = ?, recovery_target = ?, "
            "recovery_model = ?, recovery_reasoning_effort = ? WHERE id = ?",
            (
                action,
                recovery_target,
                recovery_model,
                recovery_effort,
                row["id"],
            ),
        )
        if str(row["id"]) in canonical_ids:
            attempt_routes[str(row["attempt_id"])] = (stage, next_boundary)
    for attempt_id in grouped:
        stage, next_boundary = attempt_routes.get(
            attempt_id,
            ("system_reconciliation", "system_reconciliation"),
        )
        connection.execute(
            "UPDATE attempts SET stage = ?, next_boundary = ? WHERE id = ?",
            (stage, next_boundary, attempt_id),
        )


def _retire_legacy_cutover_schema_v40(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Remove the retired pre-Control-Plane import/cutover surface.

    The old tables were never part of steady-state operation.  Refuse the
    upgrade when they still contain evidence so upgrading cannot silently
    discard owner data; an operator must first preserve it with an older
    release.  Empty legacy state is removed and authority becomes structurally
    canonical-only.
    """

    if existing_schema_version == 0 or existing_schema_version >= 40:
        return

    authority = connection.execute(
        "SELECT mode FROM control_authority WHERE singleton = 1"
    ).fetchone()
    if authority is None and existing_schema_version < 5:
        now = utc_now()
        connection.execute(
            "INSERT INTO control_authority("
            "singleton, mode, generation, activated_at, updated_at"
            ") VALUES(1, 'canonical', 1, ?, ?)",
            (now, now),
        )
        authority = connection.execute(
            "SELECT mode FROM control_authority WHERE singleton = 1"
        ).fetchone()
    mode = str(authority["mode"]) if authority is not None else "missing"
    if mode != "canonical":
        raise RuntimeError(
            "schema 40 requires canonical authority; preserve or complete the "
            f"retired cutover state with an older release first (mode={mode})"
        )

    retired_tables = (
        "user_acceptances",
        "cutover_verifications",
        "cutover_canonical_manifest",
        "legacy_entity_mappings",
        "legacy_import_snapshots",
    )
    occupied: list[str] = []
    for table in retired_tables:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        if exists is None:
            continue
        count = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if count:
            occupied.append(f"{table}={count}")
    for table in ("principals", "work_items", "events"):
        count = int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE operator_scope = 'migration'"
            ).fetchone()[0]
        )
        if count:
            occupied.append(f"{table}.migration_scope={count}")
    if occupied:
        raise RuntimeError(
            "schema 40 will not discard retired compatibility evidence; preserve it with "
            "an older release first (" + ", ".join(occupied) + ")"
        )

    for table in retired_tables:
        connection.execute(f"DROP TABLE IF EXISTS {table}")

    authority_columns = {
        str(column["name"]) for column in connection.execute("PRAGMA table_info(control_authority)")
    }
    if "legacy_snapshot_digest" not in authority_columns:
        return

    connection.executescript(
        """
        ALTER TABLE control_authority RENAME TO control_authority_v39;
        CREATE TABLE control_authority (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            mode TEXT NOT NULL CHECK(mode = 'canonical'),
            generation INTEGER NOT NULL,
            activated_at TEXT,
            updated_at TEXT NOT NULL
        );
        INSERT INTO control_authority(
            singleton, mode, generation, activated_at, updated_at
        )
        SELECT singleton, mode, generation, activated_at, updated_at
        FROM control_authority_v39;
        DROP TABLE control_authority_v39;
        """
    )


def _ensure_delivery_reactivation_policy_schema(
    connection: sqlite3.Connection,
    *,
    existing_schema_version: int,
) -> None:
    """Separate retry exhaustion from irreversible semantic lane closure.

    Schema 41 and earlier represented both outcomes as ``state='dead'`` and
    inferred reactivation from Message/Attempt state.  That let a heartbeat
    resurrect a semantically closed Cancel after its Work had already ended.
    The migration is fail-closed and marks only an exact historical bounded
    transport-exhaustion event as retryable.
    """

    columns = {
        str(column["name"])
        for column in connection.execute("PRAGMA table_info(message_deliveries)")
    }
    if "reactivation_policy" not in columns:
        connection.execute(
            "ALTER TABLE message_deliveries ADD COLUMN reactivation_policy "
            "TEXT NOT NULL DEFAULT 'terminal' "
            "CHECK(reactivation_policy IN ('terminal', 'retryable'))"
        )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS message_deliveries_reactivation_idx "
        "ON message_deliveries(recipient_id, state, reactivation_policy, updated_at)"
    )
    if existing_schema_version == 0 or existing_schema_version >= 42:
        return

    connection.execute("UPDATE message_deliveries SET reactivation_policy = 'terminal'")
    dead_event_data_json = _safe_json_document_sql("dead_event.data_json")
    connection.execute(
        f"""
        UPDATE message_deliveries
        SET reactivation_policy = 'retryable'
        WHERE state = 'dead'
          AND EXISTS (
              SELECT 1
              FROM events AS dead_event
              WHERE dead_event.event_type = 'runtime.message_dead'
                AND dead_event.aggregate_type = 'message'
                AND dead_event.aggregate_id = message_deliveries.message_id
                AND COALESCE(
                      json_extract({dead_event_data_json}, '$.failure_code'), ''
                    ) NOT IN ('message_missing', 'assignment_dependency_unavailable')
          )
          AND NOT EXISTS (
              SELECT 1
              FROM events AS terminal_event
              WHERE terminal_event.aggregate_type = 'message'
                AND terminal_event.aggregate_id = message_deliveries.message_id
                AND terminal_event.event_type IN (
                    'message.delivery_superseded',
                    'runtime.message_not_submitted',
                    'runtime.owner_private_placement_blocked'
                )
          )
          AND EXISTS (
              SELECT 1
              FROM messages AS message
              LEFT JOIN attempts AS attempt ON attempt.id = message.attempt_id
              WHERE message.id = message_deliveries.message_id
                AND (
                    message.attempt_id IS NULL
                    OR message.kind = 'cancel'
                    OR attempt.state NOT IN ('completed', 'failed', 'canceled')
                )
          )
        """
    )


class Database:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.path = settings.database_path
        self._schema_lock = threading.Lock()
        # This is deliberately process-local. SQLite has no portable
        # cross-process commit notification primitive, so dashboard streams
        # use it only to avoid polling while this Control Plane process makes
        # a successful commit. Their bounded timeout remains the recovery
        # scan for another process modifying the same database.
        self._commit_condition = threading.Condition()
        self._commit_generation = 0
        self.settings.ensure_directories()
        self.initialize()

    def commit_generation(self) -> int:
        """Return the in-process generation of successful database commits."""

        with self._commit_condition:
            return self._commit_generation

    def wait_for_commit(
        self,
        after: int,
        timeout: float,
        *,
        cancelled: threading.Event | None = None,
    ) -> int:
        """Wait once for a later in-process successful commit.

        Callers must perform a durable recovery scan after a timeout: this
        condition cannot observe commits made by another process.  ``cancelled``
        lets an async caller release its worker thread promptly on shutdown.
        """

        with self._commit_condition:
            remaining = max(timeout, 0.0)
            deadline = monotonic() + remaining
            while (
                self._commit_generation <= after
                and remaining > 0
                and (cancelled is None or not cancelled.is_set())
            ):
                self._commit_condition.wait(remaining)
                remaining = deadline - monotonic()
            return self._commit_generation

    def wake_commit_waiters(self) -> None:
        """Wake local commit waiters without publishing a commit generation."""

        with self._commit_condition:
            self._commit_condition.notify_all()

    def _notify_commit(self) -> None:
        with self._commit_condition:
            self._commit_generation += 1
            self._commit_condition.notify_all()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            factory=_ClosingConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.create_function(
            "cao_service_json_digest", 1, _service_json_digest_sql, deterministic=True
        )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute("PRAGMA trusted_schema = OFF")
        connection.execute("PRAGMA secure_delete = FAST")
        self._secure_files()
        return connection

    @contextmanager
    def connection_scope(self) -> Iterator[sqlite3.Connection]:
        """Yield one SQLite connection and close it eagerly on scope exit.

        CAO connections close from ``__exit__`` as well as this wrapper's
        ``finally`` block. Long-running supervisors use this named scope so
        ownership remains explicit even if the connection factory changes.
        """

        connection = self.connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _secure_files(self) -> None:
        for path in (
            self.path,
            self.path.with_name(f"{self.path.name}-wal"),
            self.path.with_name(f"{self.path.name}-shm"),
        ):
            with suppress(OSError):
                os.chmod(path, 0o600)

    def initialize(self) -> None:
        """Create or migrate the database under an exclusive SQLite lock.

        Migrations are additive and idempotent.  The application ID and
        ``user_version`` provide a cheap guard against accidentally opening an
        unrelated SQLite database at the configured path.
        """
        with self._schema_lock:
            with self.connection_scope() as connection:
                try:
                    changes_before = connection.total_changes
                    boundary_supersession_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(boundary_supersessions)"
                        )
                    }
                    boundary_supersession_column_added = (
                        bool(boundary_supersession_columns)
                        and "boundary_event_sequence" not in boundary_supersession_columns
                    )
                    # Older-table rebuilds may have caused SQLite to retarget
                    # these trigger bodies to the temporary table name before
                    # that table was dropped. Remove them before *any* schema
                    # ALTER, then reinstall their canonical definitions after
                    # every additive migration has completed.
                    schema36_trigger_prelude = "".join(
                        f"DROP TRIGGER IF EXISTS {trigger};\n"
                        for trigger in (
                            *_WORK_THREAD_BINDING_TRIGGERS,
                            *_BOUNDARY_CONTINUATION_TRIGGERS,
                            *_DELIVERY_ATTACHMENT_TRIGGERS,
                            *_SUPERVISION_PAUSE_TRIGGERS,
                        )
                    )
                    boundary_prelude = (
                        "ALTER TABLE boundary_supersessions ADD COLUMN "
                        "boundary_event_sequence INTEGER REFERENCES events(sequence);\n"
                        if boundary_supersession_column_added
                        else ""
                    )
                    schema_prelude = schema36_trigger_prelude + boundary_prelude
                    connection.executescript("BEGIN EXCLUSIVE;\n" + schema_prelude + SCHEMA)
                    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
                    if application_id not in {0, APPLICATION_ID}:
                        raise RuntimeError(
                            f"database application_id {application_id} does not belong to CAO"
                        )
                    row = connection.execute(
                        "SELECT value FROM metadata WHERE key = 'schema_version'"
                    ).fetchone()
                    existing = int(row["value"]) if row else 0
                    if existing > SCHEMA_VERSION:
                        raise RuntimeError(
                            f"database schema {existing} is newer than supported {SCHEMA_VERSION}"
                        )
                    _ensure_delivery_reactivation_policy_schema(
                        connection,
                        existing_schema_version=existing,
                    )
                    principal_metadata_json = _safe_json_document_sql("principals.metadata_json")
                    event_data_json = _safe_json_document_sql("event.data_json")
                    events_data_json = _safe_json_document_sql("events.data_json")
                    opening_data_json = _safe_json_document_sql("opening.data_json")
                    exact_opening_data_json = _safe_json_document_sql("exact_opening.data_json")
                    boundary_event_data_json = _safe_json_document_sql("boundary_event.data_json")
                    exact_boundary_event_data_json = _safe_json_document_sql(
                        "exact_boundary_event.data_json"
                    )
                    scrub_marker = connection.execute(
                        "SELECT value FROM metadata WHERE key = 'runtime_diagnostic_scrub_physical_complete'"
                    ).fetchone()
                    physical_runtime_scrub_needed = existing > 0 and (
                        existing < 18
                        or scrub_marker is None
                        or str(scrub_marker["value"]) != "complete"
                    )

                    bootstrap_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(cao_attachment_bootstrap_credentials)"
                        )
                    }
                    if "one_time" not in bootstrap_columns:
                        connection.execute(
                            "ALTER TABLE cao_attachment_bootstrap_credentials "
                            "ADD COLUMN one_time INTEGER NOT NULL DEFAULT 0"
                        )
                    for column_name, definition in (
                        ("attachment_generation", "INTEGER CHECK(attachment_generation >= 0)"),
                        ("peer_pid", "INTEGER NOT NULL DEFAULT 0"),
                        ("peer_start_signature", "TEXT NOT NULL DEFAULT ''"),
                        ("native_thread_id", "TEXT NOT NULL DEFAULT ''"),
                        ("project_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("proxy_catalog_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("proxy_abi_version", "INTEGER NOT NULL DEFAULT 0"),
                    ):
                        if column_name not in bootstrap_columns:
                            connection.execute(
                                "ALTER TABLE cao_attachment_bootstrap_credentials "
                                f"ADD COLUMN {column_name} {definition}"
                            )
                    if "host_attestation_receipt_id" in bootstrap_columns:
                        connection.execute(
                            "ALTER TABLE cao_attachment_bootstrap_credentials "
                            "DROP COLUMN host_attestation_receipt_id"
                        )
                    connection.execute("DROP TABLE IF EXISTS cao_host_attestation_receipts")

                    runtime_ticket_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(runtime_enrollment_tickets)"
                        )
                    }
                    if "attempt_id" not in runtime_ticket_columns:
                        connection.execute(
                            "ALTER TABLE runtime_enrollment_tickets ADD COLUMN "
                            "attempt_id TEXT REFERENCES attempts(id)"
                        )
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS "
                        "runtime_enrollment_tickets_attempt_generation_idx "
                        "ON runtime_enrollment_tickets(attempt_id, generation)"
                    )

                    attachment_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(cao_session_attachments)"
                        )
                    }
                    for column_name, definition in (
                        ("project_scope_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("project_identity_version", "INTEGER NOT NULL DEFAULT 1 CHECK(project_identity_version IN (1, 2))"),
                    ):
                        if column_name not in attachment_columns:
                            connection.execute(
                                "ALTER TABLE cao_session_attachments "
                                f"ADD COLUMN {column_name} {definition}"
                            )
                    connection.execute(
                        "UPDATE cao_session_attachments SET project_scope_digest = project_digest "
                        "WHERE project_scope_digest = ''"
                    )
                    connection.execute(
                        "CREATE TRIGGER IF NOT EXISTS cao_attachment_project_scope_default "
                        "AFTER INSERT ON cao_session_attachments WHEN NEW.project_scope_digest = '' "
                        "BEGIN UPDATE cao_session_attachments SET project_scope_digest = NEW.project_digest "
                        "WHERE id = NEW.id; END"
                    )
                    legacy_attachment_process_columns = {
                        "host_root_pid",
                        "host_root_start_signature",
                        "bridge_pid",
                        "bridge_start_signature",
                    } <= attachment_columns
                    conversation_credential_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(cao_conversation_credentials)"
                        )
                    }
                    if "connection_id" not in conversation_credential_columns:
                        connection.execute(
                            "ALTER TABLE cao_conversation_credentials "
                            "ADD COLUMN connection_id TEXT "
                            "REFERENCES cao_attachment_connections(id) ON DELETE CASCADE"
                        )
                    attachment_connection_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(cao_attachment_connections)"
                        )
                    }
                    if "connection_generation" not in attachment_connection_columns:
                        connection.execute(
                            "ALTER TABLE cao_attachment_connections "
                            "ADD COLUMN connection_generation INTEGER NOT NULL DEFAULT 1"
                        )
                    for column_name, definition in (
                        ("peer_pid", "INTEGER NOT NULL DEFAULT 0"),
                        ("peer_start_signature", "TEXT NOT NULL DEFAULT ''"),
                    ):
                        if column_name not in attachment_connection_columns:
                            connection.execute(
                                "ALTER TABLE cao_attachment_connections "
                                f"ADD COLUMN {column_name} {definition}"
                            )
                    if existing < 20:
                        # Pre-binding CABs and their descendants carry no exact
                        # peer-process proof. They must not survive the
                        # migration as a bypass around the new issuer fence.
                        now = utc_now()
                        connection.execute(
                            "UPDATE cao_attachment_bootstrap_credentials "
                            "SET state = 'revoked', revoked_at = ?, updated_at = ? "
                            "WHERE state = 'active' AND (peer_pid <= 0 "
                            "OR peer_start_signature = '' OR native_thread_id = '' "
                            "OR project_digest = '')",
                            (now, now),
                        )

                        if legacy_attachment_process_columns:
                            unbound_attachment_ids = (
                                "SELECT id FROM cao_session_attachments "
                                "WHERE host_root_pid <= 0 "
                                "OR host_root_start_signature = '' OR bridge_pid <= 0 "
                                "OR bridge_start_signature = ''"
                            )
                            connection.execute(
                                "UPDATE cao_conversation_credentials "
                                "SET state = 'revoked', revoked_at = ?, updated_at = ? "
                                f"WHERE state = 'active' AND attachment_id IN ({unbound_attachment_ids})",
                                (now, now),
                            )
                            connection.execute(
                                "UPDATE cao_runtime_credentials "
                                "SET state = 'revoked', revoked_at = ?, updated_at = ? "
                                f"WHERE state = 'active' AND attachment_id IN ({unbound_attachment_ids})",
                                (now, now),
                            )
                            connection.execute(
                                "UPDATE cao_runtime_tickets "
                                "SET state = 'revoked', updated_at = ? "
                                f"WHERE state = 'pending' AND attachment_id IN ({unbound_attachment_ids})",
                                (now,),
                            )
                            connection.execute(
                                "UPDATE cao_session_attachments SET state = 'stale', "
                                "generation = generation + 1, revoked_at = ?, updated_at = ? "
                                "WHERE state = 'active' AND (host_root_pid <= 0 "
                                "OR host_root_start_signature = '' OR bridge_pid <= 0 "
                                "OR bridge_start_signature = '')",
                                (now, now),
                            )

                    if existing < 21:
                        _install_requester_decision_binding_triggers(connection)
                    if existing < 28:
                        _install_work_close_receipt_binding_triggers(connection)

                    principal_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(principals)")
                    }
                    if "operator_scope" not in principal_columns:
                        connection.execute(
                            "ALTER TABLE principals ADD COLUMN operator_scope "
                            "TEXT NOT NULL DEFAULT 'unclassified'"
                        )
                    if "operator_label" not in principal_columns:
                        connection.execute(
                            "ALTER TABLE principals ADD COLUMN operator_label "
                            "TEXT NOT NULL DEFAULT ''"
                        )
                    work_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(work_items)")
                    }
                    if "operator_scope" not in work_columns:
                        connection.execute(
                            "ALTER TABLE work_items ADD COLUMN operator_scope "
                            "TEXT NOT NULL DEFAULT 'unclassified'"
                        )
                    if existing < 23:
                        # A partially applied development build may already
                        # have installed the v23 triggers without advancing the
                        # ledger.  Remove them before the one-time backfill and
                        # reinstall them below in the same transaction.
                        _drop_operator_scope_triggers(connection)
                        connection.execute(
                            f"""
                            UPDATE principals
                            SET operator_scope = 'system', operator_label = ''
                            WHERE role = 'worker'
                              AND json_extract(
                                    {principal_metadata_json}, '$.migration_state'
                                  )
                                  = 'cao-owned-holding-queue'
                            """
                        )
                        # Before schema v23, disposable acceptance Workers
                        # used the reserved ``disposable-*`` workspace-ref
                        # convention.  Migrate that retired convention once;
                        # all new callers must provide an explicit scope.
                        managed_workers = connection.execute(
                            """
                            SELECT principal.id, spec.workspace_ref
                            FROM principals AS principal
                            JOIN managed_worker_specs AS spec
                              ON spec.principal_id = principal.id
                            WHERE principal.role = 'worker'
                            ORDER BY principal.id
                            """
                        ).fetchall()
                        managed_scope_indexes = {"production": 0, "acceptance-test": 0}
                        for managed_worker in managed_workers:
                            scope = (
                                "acceptance-test"
                                if str(managed_worker["workspace_ref"]).startswith("disposable-")
                                else "production"
                            )
                            managed_scope_indexes[scope] += 1
                            label_prefix = (
                                "Disposable acceptance Worker"
                                if scope == "acceptance-test"
                                else "Managed production Worker"
                            )
                            safe_workspace_label = _safe_historical_operator_label(
                                managed_worker["workspace_ref"]
                            )
                            operator_label = (
                                safe_workspace_label
                                if scope == "production" and safe_workspace_label is not None
                                else f"{label_prefix} {managed_scope_indexes[scope]}"
                            )
                            connection.execute(
                                "UPDATE principals SET operator_scope = ?, "
                                "operator_label = ? WHERE id = ?",
                                (
                                    scope,
                                    operator_label,
                                    managed_worker["id"],
                                ),
                            )
                        # Work provenance is a creation-time snapshot.  The
                        # principal classification is migrated first, then all
                        # retained Work inherits its assigned Worker's scope.
                        connection.execute(
                            """
                            UPDATE work_items
                            SET operator_scope = COALESCE((
                                SELECT principal.operator_scope
                                FROM principals AS principal
                                WHERE principal.id = work_items.assigned_worker_id
                            ), 'unclassified')
                            """
                        )

                    work_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(work_items)")
                    }
                    if "requester_id" not in work_columns:
                        connection.execute(
                            "ALTER TABLE work_items ADD COLUMN requester_id TEXT "
                            "REFERENCES principals(id)"
                        )
                    if "supervisor_id" not in work_columns:
                        connection.execute(
                            "ALTER TABLE work_items ADD COLUMN supervisor_id TEXT "
                            "REFERENCES principals(id)"
                        )
                    if "supervisor_attachment_id" not in work_columns:
                        connection.execute(
                            "ALTER TABLE work_items ADD COLUMN supervisor_attachment_id TEXT "
                            "REFERENCES cao_session_attachments(id)"
                        )
                    if "generation" not in work_columns:
                        connection.execute(
                            "ALTER TABLE work_items ADD COLUMN generation INTEGER "
                            "NOT NULL DEFAULT 1"
                        )
                    if "suspended_by_work_item_id" not in work_columns:
                        connection.execute(
                            "ALTER TABLE work_items ADD COLUMN suspended_by_work_item_id TEXT "
                            "REFERENCES work_items(id)"
                        )
                    _install_operator_scope_triggers(connection)
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS work_items_operator_scope_state_idx "
                        "ON work_items(operator_scope, state, updated_at DESC)"
                    )
                    goal_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(goal_revisions)")
                    }
                    goal_semantic_columns = {
                        "title",
                        "priority",
                        "requester_id",
                        "supervisor_id",
                        "metadata_json",
                    }
                    missing_goal_semantic_columns = goal_semantic_columns - goal_columns
                    for column_name, definition in (
                        ("title", "TEXT NOT NULL DEFAULT ''"),
                        ("priority", "INTEGER NOT NULL DEFAULT 0"),
                        ("requester_id", "TEXT REFERENCES principals(id)"),
                        ("supervisor_id", "TEXT REFERENCES principals(id)"),
                        ("metadata_json", "TEXT NOT NULL DEFAULT '{}'"),
                        ("packet_json", "TEXT NOT NULL DEFAULT '{}'"),
                        ("packet_digest", "TEXT NOT NULL DEFAULT ''"),
                        (
                            "source_intent_id",
                            "TEXT REFERENCES submitted_intents(id)",
                        ),
                        ("source_directive_id", "TEXT"),
                        ("correlation_id", "TEXT NOT NULL DEFAULT ''"),
                        ("prior_version", "INTEGER"),
                        (
                            "supervisor_attachment_generation",
                            "INTEGER CHECK(supervisor_attachment_generation >= 0)",
                        ),
                        ("supervisor_runtime_session_id", "TEXT"),
                    ):
                        if column_name not in goal_columns:
                            connection.execute(
                                f"ALTER TABLE goal_revisions ADD COLUMN {column_name} {definition}"
                            )
                    if missing_goal_semantic_columns:
                        ambiguous_goal = connection.execute(
                            """
                            SELECT work_item_id FROM goal_revisions
                            GROUP BY work_item_id HAVING COUNT(*) > 1 LIMIT 1
                            """
                        ).fetchone()
                        if ambiguous_goal is not None:
                            raise RuntimeError(
                                "historical goal revision semantics are ambiguous; "
                                "operator repair is required"
                            )
                    attempt_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(attempts)")
                    }
                    for column_name, definition in (
                        ("goal_version", "INTEGER NOT NULL DEFAULT 1"),
                        ("goal_packet_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("task_packet_digest", "TEXT NOT NULL DEFAULT ''"),
                    ):
                        if column_name not in attempt_columns:
                            connection.execute(
                                f"ALTER TABLE attempts ADD COLUMN {column_name} {definition}"
                            )
                    if {
                        "objective",
                        "maturity",
                        "acceptance_json",
                        "non_goals_json",
                    } <= work_columns:
                        connection.execute(
                            """
                            INSERT INTO goal_revisions(
                                work_item_id, version, title, objective, maturity,
                                acceptance_json, non_goals_json, priority,
                                requester_id, supervisor_id, metadata_json,
                                packet_json, packet_digest, reason,
                                created_by, created_at
                            )
                            SELECT
                                w.id, w.goal_version, w.title, w.objective, w.maturity,
                                w.acceptance_json, w.non_goals_json, w.priority,
                                w.requester_id, w.supervisor_id, w.metadata_json,
                                '{}', '',
                                'legacy current-goal import', w.created_by, w.created_at
                            FROM work_items AS w
                            WHERE NOT EXISTS (
                                SELECT 1 FROM goal_revisions AS g
                                WHERE g.work_item_id = w.id
                                  AND g.version = w.goal_version
                            )
                            """
                        )
                    connection.execute(
                        """
                        UPDATE work_items
                        SET supervisor_id = created_by
                        WHERE supervisor_id IS NULL
                          AND created_by IN (SELECT id FROM principals WHERE role = 'cao')
                        """
                    )
                    connection.execute(
                        """
                        UPDATE work_items
                        SET supervisor_id = (
                            SELECT id FROM principals
                            WHERE role = 'cao' AND enabled = 1
                            ORDER BY created_at, id LIMIT 1
                        )
                        WHERE supervisor_id IS NULL
                        """
                    )
                    connection.execute(
                        """
                        UPDATE work_items
                        SET requester_id = created_by
                        WHERE requester_id IS NULL
                          AND created_by IN (SELECT id FROM principals WHERE role = 'user')
                        """
                    )
                    connection.execute(
                        """
                        UPDATE work_items
                        SET requester_id = (
                            SELECT id FROM principals
                            WHERE role = 'user' AND enabled = 1
                            ORDER BY created_at, id LIMIT 1
                        )
                        WHERE requester_id IS NULL
                          AND (
                            SELECT COUNT(*) FROM principals
                            WHERE role = 'user' AND enabled = 1
                          ) = 1
                        """
                    )
                    connection.execute(
                        """
                        UPDATE goal_revisions
                        SET title = (SELECT title FROM work_items WHERE id = work_item_id),
                            priority = (SELECT priority FROM work_items WHERE id = work_item_id),
                            requester_id = (SELECT requester_id FROM work_items WHERE id = work_item_id),
                            supervisor_id = (SELECT supervisor_id FROM work_items WHERE id = work_item_id),
                            metadata_json = (SELECT metadata_json FROM work_items WHERE id = work_item_id)
                        WHERE packet_digest = ''
                        """
                    )
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS work_items_requester_idx "
                        "ON work_items(requester_id, state, updated_at DESC)"
                    )
                    connection.execute(
                        "CREATE INDEX IF NOT EXISTS work_items_supervisor_idx "
                        "ON work_items(supervisor_id, attention_owner, updated_at DESC)"
                    )
                    boundary_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(boundaries)")
                    }
                    if "input_digest" not in boundary_columns:
                        connection.execute(
                            "ALTER TABLE boundaries ADD COLUMN input_digest TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                    if "recovery_action" not in boundary_columns:
                        connection.execute(
                            "ALTER TABLE boundaries ADD COLUMN recovery_action TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                    if "recovery_target" not in boundary_columns:
                        connection.execute(
                            "ALTER TABLE boundaries ADD COLUMN recovery_target TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                    if "recovery_model" not in boundary_columns:
                        connection.execute(
                            "ALTER TABLE boundaries ADD COLUMN recovery_model TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                    if "recovery_reasoning_effort" not in boundary_columns:
                        connection.execute(
                            "ALTER TABLE boundaries ADD COLUMN "
                            "recovery_reasoning_effort TEXT NOT NULL DEFAULT ''"
                        )
                    disposition_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(boundary_dispositions)")
                    }
                    if "request_digest" not in disposition_columns:
                        connection.execute(
                            "ALTER TABLE boundary_dispositions "
                            "ADD COLUMN request_digest TEXT NOT NULL DEFAULT ''"
                        )
                    if "resume_condition" not in disposition_columns:
                        connection.execute(
                            "ALTER TABLE boundary_dispositions "
                            "ADD COLUMN resume_condition TEXT NOT NULL DEFAULT ''"
                        )
                    delivery_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(message_deliveries)")
                    }
                    if "generation" not in delivery_columns:
                        connection.execute(
                            "ALTER TABLE message_deliveries ADD COLUMN generation INTEGER "
                            "NOT NULL DEFAULT 1"
                        )
                    if "next_attempt_at" not in delivery_columns:
                        connection.execute(
                            "ALTER TABLE message_deliveries ADD COLUMN next_attempt_at TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                        connection.execute(
                            "UPDATE message_deliveries SET next_attempt_at = created_at "
                            "WHERE next_attempt_at = ''"
                        )
                    if "runtime_session_id" not in delivery_columns:
                        connection.execute(
                            "ALTER TABLE message_deliveries ADD COLUMN runtime_session_id TEXT "
                            "REFERENCES runtime_sessions(id) ON DELETE SET NULL"
                        )
                    connection.execute(
                        "DROP INDEX IF EXISTS reasoner_turns_supervisor_leased_unique"
                    )
                    reasoner_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(reasoner_turns)")
                    }
                    if "boundary_id" not in reasoner_columns:
                        connection.execute(
                            "ALTER TABLE reasoner_turns ADD COLUMN boundary_id TEXT "
                            "REFERENCES boundaries(id) ON DELETE CASCADE"
                        )
                    connection.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS
                            reasoner_turns_subject_leased_unique
                        ON reasoner_turns(supervisor_id, work_item_id)
                        WHERE state = 'leased'
                        """
                    )
                    connection.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS
                            reasoner_turns_boundary_leased_unique
                        ON reasoner_turns(boundary_id)
                        WHERE state = 'leased' AND boundary_id IS NOT NULL
                        """
                    )
                    message_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(messages)")
                    }
                    if "payload_digest" not in message_columns:
                        connection.execute(
                            "ALTER TABLE messages ADD COLUMN payload_digest TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                    if "message_digest" not in message_columns:
                        connection.execute(
                            "ALTER TABLE messages ADD COLUMN message_digest TEXT "
                            "NOT NULL DEFAULT ''"
                        )
                    for column_name in ("goal_packet_digest", "task_packet_digest"):
                        if column_name not in message_columns:
                            connection.execute(
                                f"ALTER TABLE messages ADD COLUMN {column_name} "
                                "TEXT NOT NULL DEFAULT ''"
                            )
                    directive_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(directives)")
                    }
                    if "expected_goal_packet_digest" not in directive_columns:
                        connection.execute(
                            "ALTER TABLE directives ADD COLUMN "
                            "expected_goal_packet_digest TEXT NOT NULL DEFAULT ''"
                        )
                    for table in ("boundaries", "reviews"):
                        columns = {
                            str(column["name"])
                            for column in connection.execute(f"PRAGMA table_info({table})")
                        }
                        if table == "reviews" and "goal_version" not in columns:
                            connection.execute(
                                f"ALTER TABLE {table} ADD COLUMN goal_version "
                                "INTEGER NOT NULL DEFAULT 1"
                            )
                        for column_name in ("goal_packet_digest", "task_packet_digest"):
                            if column_name not in columns:
                                connection.execute(
                                    f"ALTER TABLE {table} ADD COLUMN {column_name} "
                                    "TEXT NOT NULL DEFAULT ''"
                                )
                    review_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(reviews)")
                    }
                    if "boundary_id" not in review_columns:
                        connection.execute(
                            "ALTER TABLE reviews ADD COLUMN boundary_id "
                            "TEXT REFERENCES boundaries(id) ON DELETE SET NULL"
                        )
                    for column_name, definition in (
                        (
                            "supervisor_attachment_id",
                            "TEXT REFERENCES cao_session_attachments(id)",
                        ),
                        ("supervisor_attachment_generation", "INTEGER"),
                        ("work_generation", "INTEGER NOT NULL DEFAULT 1"),
                    ):
                        if column_name not in review_columns:
                            connection.execute(
                                f"ALTER TABLE reviews ADD COLUMN {column_name} {definition}"
                            )
                    if existing < 26:
                        connection.execute(
                            f"""
                            UPDATE reviews
                            SET boundary_id = (
                                SELECT json_extract(
                                    {event_data_json}, '$.boundary_id'
                                )
                                FROM events AS event
                                WHERE event.event_type = 'work.reviewed'
                                  AND event.aggregate_type = 'work_item'
                                  AND event.aggregate_id = reviews.work_item_id
                                  AND json_extract(
                                        {event_data_json}, '$.review_id'
                                      ) = reviews.id
                                  AND json_extract(
                                        {event_data_json}, '$.boundary_id'
                                      ) IS NOT NULL
                                ORDER BY event.sequence DESC LIMIT 1
                            )
                            WHERE boundary_id IS NULL
                              AND (
                                  SELECT COUNT(*) FROM events AS event
                                  WHERE event.event_type = 'work.reviewed'
                                    AND event.aggregate_type = 'work_item'
                                    AND event.aggregate_id = reviews.work_item_id
                                    AND json_extract(
                                          {event_data_json}, '$.review_id'
                                        ) = reviews.id
                                    AND json_extract(
                                          {event_data_json}, '$.boundary_id'
                                        ) IS NOT NULL
                              ) = 1
                            """
                        )
                        # Older retention could remove the audit event that
                        # named a Review's completion Boundary.  Reconstruct
                        # the canonical FK only when the immutable packet and
                        # generation tuple identifies exactly one Boundary.
                        connection.execute(
                            """
                            UPDATE reviews
                            SET boundary_id = (
                                SELECT boundary.id
                                FROM boundaries AS boundary
                                WHERE boundary.kind = 'completion'
                                  AND boundary.work_item_id = reviews.work_item_id
                                  AND boundary.attempt_id = reviews.attempt_id
                                  AND boundary.generation = reviews.work_generation
                                  AND boundary.goal_version = reviews.goal_version
                                  AND boundary.goal_packet_digest =
                                      reviews.goal_packet_digest
                                  AND boundary.task_packet_digest =
                                      reviews.task_packet_digest
                            )
                            WHERE boundary_id IS NULL
                              AND (
                                  SELECT COUNT(*)
                                  FROM boundaries AS boundary
                                  WHERE boundary.kind = 'completion'
                                    AND boundary.work_item_id = reviews.work_item_id
                                    AND boundary.attempt_id = reviews.attempt_id
                                    AND boundary.generation = reviews.work_generation
                                    AND boundary.goal_version = reviews.goal_version
                                    AND boundary.goal_packet_digest =
                                        reviews.goal_packet_digest
                                    AND boundary.task_packet_digest =
                                        reviews.task_packet_digest
                              ) = 1
                            """
                        )
                        unresolved_review = connection.execute(
                            """
                            SELECT review.id
                            FROM reviews AS review
                            JOIN boundaries AS boundary
                              ON boundary.kind = 'completion'
                             AND boundary.work_item_id = review.work_item_id
                             AND boundary.attempt_id = review.attempt_id
                             AND boundary.generation = review.work_generation
                             AND boundary.goal_version = review.goal_version
                             AND boundary.goal_packet_digest =
                                 review.goal_packet_digest
                             AND boundary.task_packet_digest =
                                 review.task_packet_digest
                            LEFT JOIN boundary_dispositions AS disposition
                              ON disposition.boundary_id = boundary.id
                            LEFT JOIN boundary_supersessions AS supersession
                              ON supersession.boundary_id = boundary.id
                            WHERE review.boundary_id IS NULL
                              AND disposition.id IS NULL
                              AND supersession.boundary_id IS NULL
                            LIMIT 1
                            """
                        ).fetchone()
                        if unresolved_review is not None:
                            raise RuntimeError(
                                "legacy completion review cannot be bound to one canonical Boundary"
                            )
                    connection.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS reviews_boundary_unique "
                        "ON reviews(boundary_id) WHERE boundary_id IS NOT NULL"
                    )
                    reasoner_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(reasoner_turns)")
                    }
                    for column_name, definition in (
                        ("goal_packet_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("task_packet_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("input_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("result_digest", "TEXT NOT NULL DEFAULT ''"),
                    ):
                        if column_name not in reasoner_columns:
                            connection.execute(
                                f"ALTER TABLE reasoner_turns ADD COLUMN {column_name} {definition}"
                            )
                    idempotency_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(idempotency_results)")
                    }
                    if "request_digest" not in idempotency_columns:
                        connection.execute(
                            "ALTER TABLE idempotency_results ADD COLUMN "
                            "request_digest TEXT NOT NULL DEFAULT ''"
                        )
                    close_receipt_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(work_close_receipts)")
                    }
                    if "cleanup_inventory_evidence_id" not in close_receipt_columns:
                        connection.execute(
                            "ALTER TABLE work_close_receipts ADD COLUMN "
                            "cleanup_inventory_evidence_id TEXT NOT NULL DEFAULT ''"
                        )
                    if "close_preparation_id" not in close_receipt_columns:
                        connection.execute(
                            "ALTER TABLE work_close_receipts ADD COLUMN "
                            "close_preparation_id TEXT REFERENCES work_close_preparations(id)"
                        )
                    preparation_columns = {
                        str(row["name"])
                        for row in connection.execute(
                            "PRAGMA table_info(work_close_preparations)"
                        ).fetchall()
                    }
                    for column_name, definition in (
                        ("supervisor_attachment_id", "TEXT NOT NULL DEFAULT ''"),
                        ("supervisor_attachment_generation", "INTEGER NOT NULL DEFAULT 0"),
                        ("artifact_preservations_json", "TEXT NOT NULL DEFAULT '[]'"),
                        ("artifact_preservations_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("artifact_manifest_scope", "TEXT NOT NULL DEFAULT 'work_history_v1'"),
                        ("cleanup_execution_request_digest", "TEXT NOT NULL DEFAULT ''"),
                        ("cleanup_execution_result_json", "TEXT NOT NULL DEFAULT '{}'"),
                        ("cleanup_executed_at", "TEXT"),
                    ):
                        if column_name not in preparation_columns:
                            connection.execute(
                                f"ALTER TABLE work_close_preparations ADD COLUMN {column_name} {definition}"
                            )
                    effect_columns = {
                        str(row["name"])
                        for row in connection.execute(
                            "PRAGMA table_info(effect_operations)"
                        ).fetchall()
                    }
                    if "cleanup_preparation_id" not in effect_columns:
                        connection.execute(
                            "ALTER TABLE effect_operations ADD COLUMN cleanup_preparation_id TEXT NOT NULL DEFAULT ''"
                        )
                    if "cleanup_execution_digest" not in effect_columns:
                        connection.execute(
                            "ALTER TABLE effect_operations ADD COLUMN cleanup_execution_digest TEXT NOT NULL DEFAULT ''"
                        )
                    effect_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(effect_operations)")
                    }
                    for column_name, definition in (
                        (
                            "cleanup_work_item_id",
                            "TEXT REFERENCES work_items(id) ON DELETE CASCADE",
                        ),
                        ("cleanup_generation", "INTEGER"),
                        ("cleanup_target_kind", "TEXT NOT NULL DEFAULT ''"),
                        ("cleanup_target_fingerprint", "TEXT NOT NULL DEFAULT ''"),
                    ):
                        if column_name not in effect_columns:
                            connection.execute(
                                f"ALTER TABLE effect_operations ADD COLUMN {column_name} {definition}"
                            )
                    managed_spec_columns = {
                        str(column["name"])
                        for column in connection.execute("PRAGMA table_info(managed_worker_specs)")
                    }
                    if "provider_scope_digest" not in managed_spec_columns:
                        connection.execute(
                            "ALTER TABLE managed_worker_specs ADD COLUMN "
                            "provider_scope_digest TEXT NOT NULL DEFAULT ''"
                        )
                    if "catalog_target_id" not in managed_spec_columns:
                        connection.execute(
                            "ALTER TABLE managed_worker_specs ADD COLUMN "
                            "catalog_target_id TEXT NOT NULL DEFAULT ''"
                        )
                    provider_circuit_columns = {
                        str(column["name"])
                        for column in connection.execute(
                            "PRAGMA table_info(provider_runtime_circuits)"
                        )
                    }
                    if "probe_outcome_state" not in provider_circuit_columns:
                        connection.execute(
                            "ALTER TABLE provider_runtime_circuits ADD COLUMN "
                            "probe_outcome_state TEXT NOT NULL DEFAULT 'none' "
                            "CHECK(probe_outcome_state IN ('none', 'active', 'unknown'))"
                        )
                        # A pre-marker HALF_OPEN row may already have crossed
                        # the provider handoff.  Liveness cannot prove that it
                        # did not, so upgrade it to the conservative outcome.
                        connection.execute(
                            "UPDATE provider_runtime_circuits "
                            "SET probe_outcome_state = CASE "
                            "WHEN state = 'half_open' THEN 'unknown' ELSE 'none' END"
                        )
                    for spec in connection.execute(
                        "SELECT id, adapter, effective_model, provider_scope_digest "
                        "FROM managed_worker_specs"
                    ).fetchall():
                        if str(spec["provider_scope_digest"]):
                            continue
                        scope_digest = hashlib.sha256(
                            json.dumps(
                                {
                                    "adapter": str(spec["adapter"]),
                                    "auth_scope": "owner-local",
                                    "model": str(spec["effective_model"]),
                                },
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        ).hexdigest()
                        connection.execute(
                            "UPDATE managed_worker_specs SET provider_scope_digest = ? "
                            "WHERE id = ?",
                            (scope_digest, spec["id"]),
                        )
                    for spec in connection.execute(
                        "SELECT id, catalog_target_id FROM managed_worker_specs"
                    ).fetchall():
                        if str(spec["catalog_target_id"]):
                            continue
                        target_events = connection.execute(
                            f"""
                            SELECT json_extract(
                                {events_data_json}, '$.catalog_target_id'
                            ) AS target_id
                            FROM events
                            WHERE event_type = 'managed_worker.provisioned'
                              AND aggregate_type = 'managed_worker_spec'
                              AND aggregate_id = ?
                              AND json_type(
                                    {events_data_json}, '$.catalog_target_id'
                                  ) = 'text'
                              AND json_extract(
                                    {events_data_json}, '$.catalog_target_id'
                                  ) <> ''
                            ORDER BY sequence
                            """,
                            (spec["id"],),
                        ).fetchall()
                        if len(target_events) == 1:
                            connection.execute(
                                "UPDATE managed_worker_specs SET catalog_target_id = ? "
                                "WHERE id = ?",
                                (target_events[0]["target_id"], spec["id"]),
                            )
                    if existing < 18:
                        _scrub_pre_v18_runtime_diagnostics(connection)
                    _backfill_goal_attachment_epochs(connection)
                    _backfill_goal_and_task_packets(connection)
                    _validate_close_lifecycle_bindings(connection)
                    if {"recipient_id", "delivery_state", "updated_at"} <= message_columns:
                        connection.execute(
                            """
                        INSERT INTO message_deliveries(
                            message_id, recipient_id, state, generation, attempts,
                            next_attempt_at, runtime_session_id,
                            lease_until, owner_token, delivered_at,
                            acknowledged_at, handled_at, last_error,
                            created_at, updated_at
                        )
                        SELECT
                            m.id,
                            m.recipient_id,
                            CASE WHEN r.acknowledged_at IS NOT NULL
                                 THEN 'acknowledged' ELSE m.delivery_state END,
                            1,
                            0,
                            m.created_at,
                            w.runtime_session_id,
                            NULL,
                            '',
                            CASE WHEN m.delivery_state IN ('delivered', 'acknowledged')
                                 THEN m.updated_at ELSE NULL END,
                            r.acknowledged_at,
                            NULL,
                            '',
                            m.created_at,
                            m.updated_at
                        FROM messages AS m
                        LEFT JOIN message_receipts AS r
                          ON r.message_id = m.id
                         AND r.principal_id = m.recipient_id
                        LEFT JOIN wakeups AS w
                          ON w.message_id = m.id
                         AND w.principal_id = m.recipient_id
                        ON CONFLICT(message_id, recipient_id) DO NOTHING
                            """
                        )
                    duplicate = connection.execute(
                        """
                        SELECT principal_id, kind, target, action, content_digest,
                               argv_digest, workdir_digest, cleanup_work_item_id,
                               cleanup_generation, cleanup_target_kind,
                               cleanup_target_fingerprint, COUNT(*) AS count
                        FROM effect_operations
                        WHERE status IN ('started', 'unknown') AND kind <> 'local'
                        GROUP BY principal_id, kind, target, action, content_digest,
                                 argv_digest, workdir_digest, cleanup_work_item_id,
                                 cleanup_generation, cleanup_target_kind,
                                 cleanup_target_fingerprint
                        HAVING COUNT(*) > 1
                        LIMIT 1
                        """
                    ).fetchone()
                    if duplicate is not None:
                        raise RuntimeError(
                            "database contains duplicate unresolved effect operations; "
                            "resolve them before upgrading"
                        )
                    connection.execute("DROP INDEX IF EXISTS effect_operations_unresolved_unique")
                    connection.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS
                            effect_operations_unresolved_unique
                        ON effect_operations(
                            principal_id, kind, target, action, content_digest,
                            argv_digest, workdir_digest, cleanup_work_item_id,
                            cleanup_generation, cleanup_target_kind,
                            cleanup_target_fingerprint
                        )
                        WHERE status IN ('started', 'unknown') AND kind <> 'local'
                        """
                    )

                    migrations = {
                        1: "initial durable control-plane schema",
                        2: "schema ledger and dispatcher/query indexes",
                        3: "work ownership and atomic unresolved-effect reservations",
                        4: "sole-authority supervisor ingress, turns, boundaries, and deliveries",
                        5: "exact reasoner recovery, sole-authority mode, and cutover evidence",
                        6: "immutable goal/task packet binding and parameter-fenced idempotency",
                        7: "authenticated monotonic legacy fence evidence",
                        8: "managed Worker MCP enrollment tickets and runtime credentials",
                        9: "managed CAO session attachments and runtime credentials",
                        10: "immutable WorkItem-to-CAO-thread attachment binding",
                        11: "owner-private managed Worker placement decisions",
                        12: "exact CAO conversation client credentials",
                        13: "attachment-bound requester decisions and explicit close receipts",
                        14: "attachment-bootstrap capabilities and one-active conversation credentials",
                        15: "server-created close cleanup inventories and work-bound effect receipts",
                        16: "attachment-scoped immutable managed Worker provisioning specs",
                        17: "artifact-before-cleanup evidence and exactly-once close cleanup execution",
                        18: "scrub legacy runtime adapter transcripts from durable diagnostics",
                        19: "peer-bound CAO attachment bootstrap bindings",
                        20: "exact MCP bridge process bindings",
                        21: "same-conversation requester decisions across CAO runtime epochs",
                        22: "immutable Goal attachment generation snapshots",
                        23: "operator-scoped Worker inventory and acceptance-test quarantine",
                        24: "durable provider rate-limit circuit and single half-open Worker probe",
                        25: "structured requester decisions and safe user-needed recovery",
                        26: "cancellation-bound boundary supersession and historical readiness recovery",
                        27: "exact opening-event binding for retained Boundary supersessions",
                        28: "monotonic attachment binding for close receipts",
                        29: "attempt-bound managed runtime launch generations",
                        30: "verified artifact manifests and explicit Worker instruction evidence",
                        31: "stable managed Worker threads and resumable runtime epochs",
                        32: "durable CAO attachments with replaceable concurrent MCP connections",
                        33: "independent managed Worker lifecycle and connection generations",
                        34: "owner-local attachment admission and legacy receipt removal",
                        35: (
                            "single-peer CAO connections, generation-bound attachment reopen, "
                            "and immutable Goal supervisor runtimes"
                        ),
                        36: (
                            "exact Work-to-managed-Worker lifecycle generation bindings "
                            "and typed WAIT_USER continuations"
                        ),
                        37: (
                            "attachment-scoped CAO inbox authority independent of "
                            "replaceable wake runtimes"
                        ),
                        38: (
                            "event-time Dashboard visibility and one-time legacy "
                            "surface resynchronization"
                        ),
                        39: ("deleted Worker event attribution from retained exact Work bindings"),
                        40: "canonical-only authority and retired cutover schema removal",
                        41: ("exact Goal-replacement Boundary supersession and historical repair"),
                        42: (
                            "typed Delivery reactivation policy and continuous runtime command lanes"
                        ),
                        43: "provider-owned Worker output receipts and attachment notification lanes",
                        44: "typed supervision pauses, explicit resumption, and scoped durable memory",
                        45: "persistent directory identity and sealed project scope migration",
                    }
                    if existing < 31:
                        _backfill_managed_worker_threads(connection)
                    if existing < 27 or boundary_supersession_column_added:
                        # An unreleased v26 build stored only the cancellation
                        # event. Additive ALTER keeps that database readable
                        # long enough to recover the exact opening event, but
                        # only one canonical observation is acceptable.
                        connection.execute(
                            "DROP TRIGGER IF EXISTS boundary_supersessions_exact_binding_insert"
                        )
                        connection.execute(
                            "DROP TRIGGER IF EXISTS boundary_supersessions_immutable_update"
                        )
                        connection.execute(
                            f"""
                            UPDATE boundary_supersessions
                            SET boundary_event_sequence = (
                                SELECT opening.sequence
                                FROM events AS opening
                                JOIN boundaries AS boundary
                                  ON boundary.id =
                                     boundary_supersessions.boundary_id
                                WHERE opening.event_type = 'boundary.recorded'
                                  AND opening.aggregate_type = 'work_item'
                                  AND opening.aggregate_id = boundary.work_item_id
                                  AND json_extract(
                                        {opening_data_json}, '$.boundary_id'
                                      ) = boundary.id
                                  AND opening.sequence <
                                      boundary_supersessions.superseding_event_sequence
                            )
                            WHERE boundary_event_sequence IS NULL
                              AND (
                                  SELECT COUNT(*)
                                  FROM events AS exact_opening
                                  JOIN boundaries AS boundary
                                    ON boundary.id =
                                       boundary_supersessions.boundary_id
                                  WHERE exact_opening.event_type = 'boundary.recorded'
                                    AND exact_opening.aggregate_type = 'work_item'
                                    AND exact_opening.aggregate_id =
                                        boundary.work_item_id
                                    AND json_extract(
                                          {exact_opening_data_json}, '$.boundary_id'
                                        ) = boundary.id
                              ) = 1
                            """
                        )
                        invalid_supersession = connection.execute(
                            f"""
                            SELECT supersession.boundary_id
                            FROM boundary_supersessions AS supersession
                            LEFT JOIN boundaries AS boundary
                              ON boundary.id = supersession.boundary_id
                            LEFT JOIN work_items AS work
                              ON work.id = boundary.work_item_id
                            LEFT JOIN events AS opening
                              ON opening.sequence =
                                 supersession.boundary_event_sequence
                            LEFT JOIN events AS cancellation
                              ON cancellation.sequence =
                                 supersession.superseding_event_sequence
                            LEFT JOIN boundary_dispositions AS disposition
                              ON disposition.boundary_id = boundary.id
                            WHERE supersession.boundary_event_sequence IS NULL
                               OR boundary.id IS NULL
                               OR work.state <> 'canceled'
                               OR boundary.generation >= work.generation
                               OR disposition.id IS NOT NULL
                               OR supersession.reason <> 'work_canceled'
                               OR opening.sequence IS NULL
                               OR opening.event_type <> 'boundary.recorded'
                               OR opening.aggregate_type <> 'work_item'
                               OR opening.aggregate_id <> boundary.work_item_id
                               OR json_extract(
                                      {opening_data_json}, '$.boundary_id'
                                  ) <> boundary.id
                               OR cancellation.sequence IS NULL
                               OR cancellation.event_type <> 'work.canceled'
                               OR cancellation.aggregate_type <> 'work_item'
                               OR cancellation.aggregate_id <> boundary.work_item_id
                               OR cancellation.sequence <= opening.sequence
                               OR (
                                   SELECT COUNT(*)
                                   FROM events AS exact_opening
                                   WHERE exact_opening.event_type =
                                         'boundary.recorded'
                                     AND exact_opening.aggregate_type = 'work_item'
                                     AND exact_opening.aggregate_id =
                                         boundary.work_item_id
                                     AND json_extract(
                                           {exact_opening_data_json}, '$.boundary_id'
                                         ) = boundary.id
                               ) <> 1
                            LIMIT 1
                            """
                        ).fetchone()
                        if invalid_supersession is not None:
                            raise RuntimeError(
                                "legacy Boundary supersession lacks canonical event proof"
                            )
                        _install_boundary_supersession_binding_triggers(connection)
                    if existing < 25:
                        incomplete_user_needed = connection.execute(
                            "SELECT id FROM work_items WHERE state = 'user_needed'"
                        ).fetchall()
                        for user_needed_work in incomplete_user_needed:
                            disposition = connection.execute(
                                """
                                SELECT d.instruction, d.resume_condition
                                FROM boundary_dispositions AS d
                                JOIN boundaries AS b ON b.id = d.boundary_id
                                WHERE b.work_item_id = ? AND d.kind = 'wait_user'
                                ORDER BY d.created_at DESC, d.id DESC LIMIT 1
                                """,
                                (user_needed_work["id"],),
                            ).fetchone()
                            if (
                                disposition is not None
                                and str(disposition["instruction"]).strip()
                                and str(disposition["resume_condition"]).strip()
                            ):
                                continue
                            connection.execute(
                                "UPDATE work_items SET state = 'waiting_supervisor', "
                                "attention_owner = 'cao', updated_at = ? WHERE id = ?",
                                (utc_now(), user_needed_work["id"]),
                            )
                            connection.execute(
                                """
                                UPDATE attempts
                                SET state = CASE
                                      WHEN state = 'input_required' THEN 'waiting_supervisor'
                                      ELSE state
                                    END,
                                    stage = CASE
                                      WHEN state = 'input_required' THEN 'system_recovery'
                                      ELSE stage
                                    END,
                                    next_boundary = CASE
                                      WHEN state = 'input_required' THEN 'cao_continue_prior'
                                      ELSE next_boundary
                                    END,
                                    updated_at = ?
                                WHERE id = (
                                    SELECT id FROM attempts
                                    WHERE work_item_id = ?
                                    ORDER BY attempt_number DESC LIMIT 1
                                )
                                """,
                                (utc_now(), user_needed_work["id"]),
                            )
                            recovery = connection.execute(
                                """
                                SELECT work.id AS work_item_id,
                                       work.generation, work.goal_version,
                                       attempt.id AS attempt_id,
                                       attempt.worker_id,
                                       attempt.goal_packet_digest,
                                       attempt.task_packet_digest,
                                       COALESCE(runtime.state, 'missing') AS runtime_state
                                FROM work_items AS work
                                JOIN attempts AS attempt ON attempt.id = (
                                    SELECT latest.id FROM attempts AS latest
                                    WHERE latest.work_item_id = work.id
                                    ORDER BY latest.attempt_number DESC LIMIT 1
                                )
                                LEFT JOIN runtime_sessions AS runtime
                                  ON runtime.id = attempt.runtime_session_id
                                WHERE work.id = ?
                                  AND NOT EXISTS (
                                      SELECT 1 FROM boundaries AS boundary
                                      LEFT JOIN boundary_dispositions AS disposition
                                        ON disposition.boundary_id = boundary.id
                                      LEFT JOIN boundary_supersessions AS supersession
                                        ON supersession.boundary_id = boundary.id
                                      WHERE boundary.work_item_id = work.id
                                        AND disposition.id IS NULL
                                        AND supersession.boundary_id IS NULL
                                  )
                                """,
                                (user_needed_work["id"],),
                            ).fetchone()
                            if recovery is not None:
                                boundary_id = f"bnd_{uuid.uuid4().hex}"
                                source_event_id = (
                                    "schema-v25-system-recovery:"
                                    f"{recovery['work_item_id']}:{recovery['attempt_id']}"
                                )
                                summary = (
                                    "CAO continuation is required because the migrated "
                                    "Work has no requester decision contract."
                                )
                                metadata = {
                                    "system_recovery": True,
                                    "reason": "incomplete_user_needed_contract",
                                    "next_action": "cao_continue_prior",
                                }
                                boundary_input = {
                                    "source_event_id": source_event_id,
                                    "work_item_id": str(recovery["work_item_id"]),
                                    "attempt_id": str(recovery["attempt_id"]),
                                    "expected_goal_version": int(recovery["goal_version"]),
                                    "expected_goal_packet_digest": str(
                                        recovery["goal_packet_digest"]
                                    ),
                                    "expected_task_packet_digest": str(
                                        recovery["task_packet_digest"]
                                    ),
                                    "expected_generation": int(recovery["generation"]),
                                    "kind": "failure",
                                    "summary": summary,
                                    "runtime_state": str(recovery["runtime_state"]),
                                    "metadata": metadata,
                                }
                                recorded_at = utc_now()
                                connection.execute(
                                    """
                                    INSERT INTO boundaries(
                                        id, source_principal_id, source_event_id,
                                        work_item_id, attempt_id, goal_version,
                                        generation, goal_packet_digest,
                                        task_packet_digest, kind, summary,
                                        runtime_state, metadata_json, input_digest,
                                        created_at
                                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'failure',
                                             ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        boundary_id,
                                        recovery["worker_id"],
                                        source_event_id,
                                        recovery["work_item_id"],
                                        recovery["attempt_id"],
                                        recovery["goal_version"],
                                        recovery["generation"],
                                        recovery["goal_packet_digest"],
                                        recovery["task_packet_digest"],
                                        summary,
                                        recovery["runtime_state"],
                                        canonical_json(metadata),
                                        canonical_digest(boundary_input),
                                        recorded_at,
                                    ),
                                )
                                connection.execute(
                                    """
                                    INSERT INTO events(
                                        id, event_type, aggregate_type, aggregate_id,
                                        actor_id, data_json, created_at
                                    ) VALUES(?, 'boundary.recorded', 'work_item',
                                             ?, ?, ?, ?)
                                    """,
                                    (
                                        f"evt_{uuid.uuid4().hex}",
                                        recovery["work_item_id"],
                                        recovery["worker_id"],
                                        canonical_json(
                                            {
                                                "boundary_id": boundary_id,
                                                "kind": "failure",
                                                "runtime_state": str(recovery["runtime_state"]),
                                                "generation": int(recovery["generation"]),
                                            }
                                        ),
                                        recorded_at,
                                    ),
                                )
                    if existing < 26:
                        # Pre-v26 retention treated the opening event as
                        # disposable even while its canonical Boundary stayed
                        # actionable. Re-seal that observation only for a
                        # nonterminal Work; this creates a new audit sequence,
                        # not a fabricated historical ordering claim.
                        missing_opening_events = connection.execute(
                            f"""
                            SELECT boundary.*, work.state AS work_state
                            FROM boundaries AS boundary
                            JOIN work_items AS work
                              ON work.id = boundary.work_item_id
                            LEFT JOIN boundary_dispositions AS disposition
                              ON disposition.boundary_id = boundary.id
                            LEFT JOIN boundary_supersessions AS supersession
                              ON supersession.boundary_id = boundary.id
                            WHERE disposition.id IS NULL
                              AND supersession.boundary_id IS NULL
                              AND work.state NOT IN ('completed', 'canceled', 'failed')
                              AND NOT EXISTS (
                                  SELECT 1 FROM events AS event
                                  WHERE event.event_type = 'boundary.recorded'
                                    AND event.aggregate_type = 'work_item'
                                    AND event.aggregate_id = boundary.work_item_id
                                    AND json_extract(
                                          {event_data_json}, '$.boundary_id'
                                        ) = boundary.id
                              )
                            """
                        ).fetchall()
                        for boundary in missing_opening_events:
                            connection.execute(
                                """
                                INSERT INTO events(
                                    id, event_type, aggregate_type, aggregate_id,
                                    actor_id, data_json, created_at
                                ) VALUES(?, 'boundary.recorded', 'work_item', ?, ?, ?, ?)
                                """,
                                (
                                    f"evt_{uuid.uuid4().hex}",
                                    boundary["work_item_id"],
                                    boundary["source_principal_id"],
                                    canonical_json(
                                        {
                                            "boundary_id": str(boundary["id"]),
                                            "kind": str(boundary["kind"]),
                                            "runtime_state": str(boundary["runtime_state"]),
                                            "generation": int(boundary["generation"]),
                                            "migration_reconstructed": True,
                                        }
                                    ),
                                    utc_now(),
                                ),
                            )
                        invalid_opening_event = connection.execute(
                            f"""
                            SELECT boundary.id
                            FROM boundaries AS boundary
                            JOIN work_items AS work
                              ON work.id = boundary.work_item_id
                            LEFT JOIN boundary_dispositions AS disposition
                              ON disposition.boundary_id = boundary.id
                            LEFT JOIN boundary_supersessions AS supersession
                              ON supersession.boundary_id = boundary.id
                            WHERE disposition.id IS NULL
                              AND supersession.boundary_id IS NULL
                              AND work.state NOT IN ('completed', 'canceled', 'failed')
                              AND (
                                  SELECT COUNT(*) FROM events AS event
                                  WHERE event.event_type = 'boundary.recorded'
                                    AND event.aggregate_type = 'work_item'
                                    AND event.aggregate_id = boundary.work_item_id
                                    AND json_extract(
                                          {event_data_json}, '$.boundary_id'
                                        ) = boundary.id
                              ) <> 1
                            LIMIT 1
                            """
                        ).fetchone()
                        if invalid_opening_event is not None:
                            raise RuntimeError("open Boundary lacks one canonical opening event")
                        # Older writers correctly made Work cancellation
                        # terminal but left any pre-existing Boundary without
                        # a disposition.  Reconstruct no decision: bind the
                        # now-inactive Boundary to the exact later cancellation
                        # event that already carries the terminal authority.
                        connection.execute(
                            f"""
                            INSERT INTO boundary_supersessions(
                                boundary_id, boundary_event_sequence,
                                superseding_event_sequence, reason, created_at
                            )
                            SELECT boundary.id, boundary_event.sequence,
                                   MIN(canceled_event.sequence), 'work_canceled',
                                   MIN(canceled_event.created_at)
                            FROM boundaries AS boundary
                            JOIN work_items AS work
                              ON work.id = boundary.work_item_id
                             AND work.state = 'canceled'
                            JOIN events AS boundary_event
                              ON boundary_event.event_type = 'boundary.recorded'
                             AND boundary_event.aggregate_type = 'work_item'
                             AND boundary_event.aggregate_id = boundary.work_item_id
                             AND json_extract(
                                   {boundary_event_data_json}, '$.boundary_id'
                                 ) = boundary.id
                            JOIN events AS canceled_event
                              ON canceled_event.event_type = 'work.canceled'
                             AND canceled_event.aggregate_type = 'work_item'
                             AND canceled_event.aggregate_id = boundary.work_item_id
                             AND canceled_event.sequence > boundary_event.sequence
                            LEFT JOIN boundary_dispositions AS disposition
                              ON disposition.boundary_id = boundary.id
                            LEFT JOIN boundary_supersessions AS supersession
                              ON supersession.boundary_id = boundary.id
                            WHERE disposition.id IS NULL
                              AND supersession.boundary_id IS NULL
                              AND boundary.generation < work.generation
                              AND (
                                  SELECT COUNT(*)
                                  FROM events AS exact_boundary_event
                                  WHERE exact_boundary_event.event_type = 'boundary.recorded'
                                    AND exact_boundary_event.aggregate_type = 'work_item'
                                    AND exact_boundary_event.aggregate_id = boundary.work_item_id
                                    AND json_extract(
                                          {exact_boundary_event_data_json},
                                          '$.boundary_id'
                                        ) = boundary.id
                              ) = 1
                            GROUP BY boundary.id, boundary_event.sequence
                            """
                        )
                        unresolved_terminal_boundary = connection.execute(
                            """
                            SELECT boundary.id
                            FROM boundaries AS boundary
                            JOIN work_items AS work
                              ON work.id = boundary.work_item_id
                            LEFT JOIN boundary_dispositions AS disposition
                              ON disposition.boundary_id = boundary.id
                            LEFT JOIN boundary_supersessions AS supersession
                              ON supersession.boundary_id = boundary.id
                            WHERE disposition.id IS NULL
                              AND supersession.boundary_id IS NULL
                              AND work.state IN ('completed', 'canceled', 'failed')
                            LIMIT 1
                            """
                        ).fetchone()
                        if unresolved_terminal_boundary is not None:
                            raise RuntimeError("terminal Work has an unresolved legacy Boundary")
                    # Earlier versions could issue several healthy CSCs for a
                    # single attachment during lease renewal.  Retain only the
                    # latest row before installing the uniqueness fence; raw
                    # credentials are never present in this migration.
                    if existing < 14:
                        connection.execute(
                            """
                            UPDATE cao_conversation_credentials
                            SET state = 'revoked', revoked_at = ?, updated_at = ?
                            WHERE state = 'active'
                              AND id NOT IN (
                                  SELECT newest.id
                                  FROM cao_conversation_credentials AS newest
                                  WHERE newest.state = 'active'
                                    AND newest.id = (
                                        SELECT candidate.id
                                        FROM cao_conversation_credentials AS candidate
                                        WHERE candidate.attachment_id = newest.attachment_id
                                          AND candidate.state = 'active'
                                        ORDER BY candidate.created_at DESC, candidate.id DESC
                                        LIMIT 1
                                    )
                              )
                            """,
                            (utc_now(), utc_now()),
                        )
                        connection.execute(
                            """
                            CREATE UNIQUE INDEX IF NOT EXISTS
                                cao_conversation_credentials_one_active_attachment
                            ON cao_conversation_credentials(attachment_id)
                            WHERE state = 'active'
                            """
                        )
                    if existing < 32:
                        # v14 serialized every stdio bridge through one active
                        # attachment credential but recorded no process-loaded
                        # catalog/ABI identity.  Preserve its exact historic
                        # binding as a stale connection and revoke the bearer;
                        # attributing current daemon metadata to an old bridge
                        # would recreate the catalog-staleness incident.
                        connection.execute(
                            "DROP INDEX IF EXISTS "
                            "cao_conversation_credentials_one_active_attachment"
                        )
                        migration_now = utc_now()
                        legacy_peer_projection = (
                            "attachment.bridge_pid AS peer_pid, "
                            "attachment.bridge_start_signature AS peer_start_signature"
                            if legacy_attachment_process_columns
                            else "0 AS peer_pid, '' AS peer_start_signature"
                        )
                        active_legacy_credentials = connection.execute(
                            f"""
                            SELECT credential.id AS credential_id,
                                   credential.attachment_id,
                                   credential.principal_id,
                                   credential.generation,
                                   credential.expires_at,
                                   attachment.state AS attachment_state,
                                   attachment.generation AS attachment_generation,
                                   attachment.lease_expires_at,
                                   {legacy_peer_projection}
                            FROM cao_conversation_credentials AS credential
                            JOIN cao_session_attachments AS attachment
                              ON attachment.id = credential.attachment_id
                            WHERE credential.state = 'active'
                              AND credential.connection_id IS NULL
                            ORDER BY credential.attachment_id,
                                     credential.created_at, credential.id
                            """
                        ).fetchall()
                        next_generation_by_attachment: dict[str, int] = {}
                        for credential in active_legacy_credentials:
                            attachment_id = str(credential["attachment_id"])
                            safe = (
                                str(credential["attachment_state"]) == "active"
                                and int(credential["generation"])
                                == int(credential["attachment_generation"])
                                and str(credential["expires_at"]) > migration_now
                                and str(credential["lease_expires_at"]) > migration_now
                            )
                            if not safe:
                                connection.execute(
                                    "UPDATE cao_conversation_credentials "
                                    "SET state = 'revoked', revoked_at = ?, "
                                    "updated_at = ? WHERE id = ?",
                                    (
                                        migration_now,
                                        migration_now,
                                        credential["credential_id"],
                                    ),
                                )
                                continue
                            connection_generation = next_generation_by_attachment.get(attachment_id)
                            if connection_generation is None:
                                newest = connection.execute(
                                    "SELECT MAX(connection_generation) AS generation "
                                    "FROM cao_attachment_connections "
                                    "WHERE attachment_id = ?",
                                    (attachment_id,),
                                ).fetchone()
                                connection_generation = (
                                    int(newest["generation"]) + 1
                                    if newest is not None and newest["generation"] is not None
                                    else 1
                                )
                            next_generation_by_attachment[attachment_id] = connection_generation + 1
                            connection_id = f"cac_{uuid.uuid4().hex}"
                            connection.execute(
                                """
                                INSERT INTO cao_attachment_connections(
                                    id, attachment_id, principal_id, generation,
                                    connection_generation, peer_pid,
                                    peer_start_signature, proxy_catalog_digest,
                                    proxy_abi_version, state, lease_expires_at,
                                    revoked_at, created_at, updated_at
                                ) VALUES(?, ?, ?, ?, ?, ?, ?, '', 0,
                                         'stale', ?, ?, ?, ?)
                                """,
                                (
                                    connection_id,
                                    attachment_id,
                                    credential["principal_id"],
                                    credential["generation"],
                                    connection_generation,
                                    credential["peer_pid"],
                                    credential["peer_start_signature"],
                                    min(
                                        str(credential["expires_at"]),
                                        str(credential["lease_expires_at"]),
                                    ),
                                    migration_now,
                                    migration_now,
                                    migration_now,
                                ),
                            )
                            connection.execute(
                                "UPDATE cao_conversation_credentials "
                                "SET connection_id = ?, state = 'revoked', "
                                "revoked_at = ?, updated_at = ? WHERE id = ?",
                                (
                                    connection_id,
                                    migration_now,
                                    migration_now,
                                    credential["credential_id"],
                                ),
                            )
                        connection.execute(
                            """
                            UPDATE cao_conversation_credentials
                            SET state = 'revoked', revoked_at = ?, updated_at = ?
                            WHERE state = 'active' AND connection_id IS NULL
                            """,
                            (migration_now, migration_now),
                        )
                        connection.execute(
                            """
                            CREATE UNIQUE INDEX IF NOT EXISTS
                                cao_conversation_credentials_one_active_connection
                            ON cao_conversation_credentials(connection_id)
                            WHERE state = 'active' AND connection_id IS NOT NULL
                            """
                        )
                    if existing < 33:
                        epoch_columns = {
                            str(column["name"])
                            for column in connection.execute(
                                "PRAGMA table_info(managed_worker_thread_epochs)"
                            )
                        }
                        if "connection_generation" not in epoch_columns:
                            connection.execute(
                                "DROP INDEX IF EXISTS managed_worker_thread_epochs_one_current"
                            )
                            connection.execute(
                                "ALTER TABLE managed_worker_thread_epochs "
                                "RENAME TO managed_worker_thread_epochs_v32"
                            )
                            connection.execute(
                                """
                                CREATE TABLE managed_worker_thread_epochs (
                                    id TEXT PRIMARY KEY,
                                    thread_id TEXT NOT NULL
                                        REFERENCES managed_worker_threads(id)
                                        ON DELETE CASCADE,
                                    generation INTEGER NOT NULL
                                        CHECK(generation >= 1),
                                    connection_generation INTEGER NOT NULL
                                        DEFAULT 1 CHECK(connection_generation >= 1),
                                    runtime_session_id TEXT NOT NULL UNIQUE
                                        REFERENCES runtime_sessions(id)
                                        ON DELETE RESTRICT,
                                    enrollment_id TEXT NOT NULL UNIQUE
                                        REFERENCES worker_enrollments(id)
                                        ON DELETE RESTRICT,
                                    created_at TEXT NOT NULL,
                                    retired_at TEXT,
                                    UNIQUE(
                                        thread_id, generation,
                                        connection_generation
                                    )
                                )
                                """
                            )
                            connection.execute(
                                """
                                INSERT INTO managed_worker_thread_epochs(
                                    id, thread_id, generation,
                                    connection_generation, runtime_session_id,
                                    enrollment_id, created_at, retired_at
                                )
                                SELECT id, thread_id, generation, 1,
                                       runtime_session_id, enrollment_id,
                                       created_at, retired_at
                                FROM managed_worker_thread_epochs_v32
                                """
                            )
                            connection.execute("DROP TABLE managed_worker_thread_epochs_v32")
                        connection.execute(
                            """
                            CREATE UNIQUE INDEX IF NOT EXISTS
                                managed_worker_thread_epochs_one_current
                            ON managed_worker_thread_epochs(thread_id)
                            WHERE retired_at IS NULL
                            """
                        )
                    # Rebuild the supersession table before reinstalling the
                    # schema-36+ triggers which reference it. SQLite otherwise
                    # refuses the table swap through those live dependencies.
                    _ensure_recovery_boundary_supersession_schema(connection)
                    _ensure_work_thread_binding_schema(
                        connection,
                        existing_schema_version=existing,
                    )
                    _ensure_boundary_continuation_schema(
                        connection,
                        existing_schema_version=existing,
                    )
                    _ensure_supervision_pause_schema(connection)
                    from .supervision_memory import ensure_supervision_memory_schema

                    ensure_supervision_memory_schema(connection)
                    _ensure_delivery_attachment_schema(
                        connection,
                        existing_schema_version=existing,
                    )
                    _ensure_event_operator_scope_schema(
                        connection,
                        existing_schema_version=existing,
                    )
                    _normalize_cao_attachment_peer_schema_v35(
                        connection,
                        existing_schema_version=existing,
                    )
                    connection.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS
                            cao_conversation_credentials_one_active_connection
                        ON cao_conversation_credentials(connection_id)
                        WHERE state = 'active' AND connection_id IS NOT NULL
                        """
                    )
                    connection.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS
                            cao_attachment_connections_generation_unique
                        ON cao_attachment_connections(
                            attachment_id, connection_generation
                        )
                        """
                    )
                    _install_cao_connection_credential_triggers(connection)
                    _retire_legacy_cutover_schema_v40(
                        connection,
                        existing_schema_version=existing,
                    )
                    now = utc_now()
                    connection.execute(
                        """
                        INSERT INTO control_authority(
                            singleton, mode, generation, activated_at, updated_at
                        ) VALUES(1, 'canonical', 1, ?, ?)
                        ON CONFLICT(singleton) DO NOTHING
                        """,
                        (now, now),
                    )
                    for version in range(max(existing + 1, 1), SCHEMA_VERSION + 1):
                        connection.execute(
                            """
                            INSERT INTO schema_migrations(version, description, applied_at)
                            VALUES (?, ?, ?)
                            ON CONFLICT(version) DO NOTHING
                            """,
                            (version, migrations[version], now),
                        )
                    _backfill_goal_replacement_boundary_supersessions(connection)
                    _backfill_managed_worker_recovery_actions(connection, self.settings)
                    connection.execute(
                        "INSERT INTO metadata(key, value) VALUES('schema_version', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(SCHEMA_VERSION),),
                    )
                    connection.execute(
                        "INSERT INTO metadata(key, value) VALUES(?, ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (
                            "runtime_diagnostic_scrub_physical_complete",
                            "pending" if physical_runtime_scrub_needed else "complete",
                        ),
                    )
                    connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
                    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                    connection.commit()
                    if physical_runtime_scrub_needed:
                        _checkpoint_and_compact_runtime_diagnostic_storage(connection)
                        connection.execute("BEGIN EXCLUSIVE")
                        connection.execute(
                            "UPDATE metadata SET value = 'complete' "
                            "WHERE key = 'runtime_diagnostic_scrub_physical_complete'"
                        )
                        connection.commit()
                    if connection.total_changes > changes_before:
                        self._notify_commit()
                except BaseException:
                    connection.rollback()
                    raise
            self._secure_files()

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            changes_before = connection.total_changes
            row = connection.execute(
                "SELECT mode FROM control_authority WHERE singleton = 1"
            ).fetchone()
            mode = str(row["mode"]) if row is not None else "missing"
            if mode != "canonical":
                raise AuthorityModeError(mode)
            yield connection
            connection.commit()
            if connection.total_changes > changes_before:
                self._notify_commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        with self.connection_scope() as connection:
            row: sqlite3.Row | None = connection.execute(sql, params).fetchone()
            return row

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        with self.connection_scope() as connection:
            return list(connection.execute(sql, params).fetchall())

    def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(sql, params)
            return cursor.rowcount

    def authority_state(self) -> dict[str, Any]:
        row = self.fetchone("SELECT * FROM control_authority WHERE singleton = 1")
        if row is None:
            raise AuthorityModeError("missing")
        return dict(row)

    def is_canonical_authority(self) -> bool:
        return str(self.authority_state()["mode"]) == "canonical"

    def backup(self, destination: Path) -> Path:
        """Create an atomic, private and durable SQLite backup."""

        return backup_sqlite_database(self.path, destination).path

    def integrity_check(self) -> dict[str, Any]:
        with self.connection_scope() as connection:
            rows = [row[0] for row in connection.execute("PRAGMA integrity_check")]
            foreign_keys = [dict(row) for row in connection.execute("PRAGMA foreign_key_check")]
        return {
            "ok": rows == ["ok"] and not foreign_keys,
            "integrity": rows,
            "foreign_key_errors": foreign_keys,
        }

    def prune(self, *, event_days: int, message_days: int) -> dict[str, int]:
        event_cutoff = (
            (datetime.now(UTC) - timedelta(days=event_days))
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        message_cutoff = (
            (datetime.now(UTC) - timedelta(days=message_days))
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        with self.transaction() as connection:
            event_count = connection.execute(
                """
                DELETE FROM events
                WHERE created_at < ?
                  AND sequence NOT IN (
                      SELECT event_sequence FROM a2a_push_deliveries
                      WHERE state != 'delivered'
                  )
                  AND sequence NOT IN (
                      SELECT source_boundary_sequence
                      FROM provider_runtime_circuits
                      WHERE source_boundary_sequence > 0
                  )
                  AND sequence NOT IN (
                      SELECT boundary_event_sequence FROM boundary_supersessions
                      UNION
                      SELECT superseding_event_sequence FROM boundary_supersessions
                  )
                  AND NOT COALESCE((
                      events.event_type = 'worker_output.read'
                      AND events.aggregate_type = 'attempt'
                      AND json_valid(events.data_json)
                      AND json_type(events.data_json) = 'object'
                      AND (
                          SELECT COUNT(*) FROM json_each(events.data_json)
                      ) = 11
                      AND json_type(
                          events.data_json,
                          '$.supervisor_attachment_generation'
                      ) = 'integer'
                      AND json_extract(
                          events.data_json,
                          '$.supervisor_attachment_generation'
                      ) >= 0
                      AND json_type(events.data_json, '$.byte_offset') = 'integer'
                      AND json_type(events.data_json, '$.byte_count') = 'integer'
                      AND json_type(events.data_json, '$.total_bytes') = 'integer'
                      AND json_extract(events.data_json, '$.byte_offset') >= 0
                      AND json_extract(events.data_json, '$.byte_count') >= 0
                      AND json_extract(events.data_json, '$.byte_offset') +
                          json_extract(events.data_json, '$.byte_count') <=
                          json_extract(events.data_json, '$.total_bytes')
                      AND json_type(events.data_json, '$.complete') IN (
                          'true', 'false'
                      )
                      AND length(json_extract(
                          events.data_json, '$.chunk_digest'
                      )) = 64
                      AND length(json_extract(
                          events.data_json, '$.idempotency_key_digest'
                      )) = 64
                      AND length(json_extract(
                          events.data_json, '$.request_digest'
                      )) = 64
                      AND EXISTS (
                          SELECT 1
                          FROM worker_output_receipts AS output
                          JOIN attempts AS attempt
                            ON attempt.id = output.attempt_id
                           AND attempt.work_item_id = output.work_item_id
                          JOIN work_items AS work
                            ON work.id = output.work_item_id
                          JOIN cao_session_attachments AS attachment
                            ON attachment.id = work.supervisor_attachment_id
                           AND attachment.principal_id = events.actor_id
                          WHERE output.id = json_extract(
                              events.data_json, '$.output_id'
                          )
                            AND output.attempt_id = events.aggregate_id
                            AND events.actor_id = work.supervisor_id
                            AND output.capture_state IN ('available', 'partial')
                            AND length(output.content_digest) = 64
                            AND json_extract(
                                events.data_json, '$.digest'
                            ) = output.content_digest
                            AND json_extract(
                                events.data_json, '$.total_bytes'
                            ) = output.byte_count
                            AND json_extract(
                                events.data_json, '$.supervisor_attachment_id'
                            ) = attachment.id
                      )
                  ), 0)
                  AND NOT COALESCE((
                      events.event_type = 'artifact.content_read'
                      AND events.aggregate_type = 'artifact'
                      AND json_valid(events.data_json)
                      AND (
                          SELECT COUNT(*)
                          FROM json_each(events.data_json)
                      ) = 15
                      AND json_extract(events.data_json, '$.format') =
                          'cao-verified-artifact-text-read/v1'
                      AND json_extract(events.data_json, '$.artifact_id') =
                          events.aggregate_id
                      AND json_type(events.data_json, '$.media_type') = 'text'
                      AND json_type(
                          events.data_json,
                          '$.supervisor_attachment_generation'
                      ) = 'integer'
                      AND json_type(events.data_json, '$.byte_offset') = 'integer'
                      AND json_type(events.data_json, '$.byte_count') = 'integer'
                      AND json_type(events.data_json, '$.total_bytes') = 'integer'
                      AND json_type(events.data_json, '$.complete') IN (
                          'true', 'false'
                      )
                      AND length(json_extract(
                          events.data_json, '$.chunk_digest'
                      )) = 64
                      AND length(json_extract(
                          events.data_json, '$.idempotency_key_digest'
                      )) = 64
                      AND length(json_extract(
                          events.data_json, '$.request_digest'
                      )) = 64
                      AND EXISTS (
                          SELECT 1
                          FROM artifacts AS artifact
                          JOIN attempts AS attempt
                            ON attempt.id = artifact.attempt_id
                           AND attempt.work_item_id = artifact.work_item_id
                          JOIN work_items AS work
                            ON work.id = artifact.work_item_id
                          WHERE artifact.id = events.aggregate_id
                            AND events.actor_id = work.supervisor_id
                            AND json_extract(
                                events.data_json, '$.work_item_id'
                            ) = artifact.work_item_id
                            AND json_extract(
                                events.data_json, '$.attempt_id'
                            ) = artifact.attempt_id
                            AND json_extract(
                                events.data_json, '$.digest'
                            ) = artifact.digest
                            AND json_extract(
                                events.data_json, '$.supervisor_attachment_id'
                            ) = work.supervisor_attachment_id
                      )
                  ), 0)
                  AND NOT COALESCE((
                      events.event_type = 'cao.conversation_closed'
                      AND events.aggregate_type = 'cao_session_attachment'
                      AND json_type(
                          events.data_json, '$.closed_generation'
                      ) = 'integer'
                      AND (
                          SELECT COUNT(*) FROM json_each(events.data_json)
                      ) IN (4, 9)
                      AND json_type(
                          events.data_json, '$.work_items_canceled'
                      ) = 'integer'
                      AND json_type(
                          events.data_json, '$.managed_workers_stopped'
                      ) = 'integer'
                      AND json_type(
                          events.data_json, '$.idempotency_key_digest'
                      ) = 'text'
                      AND EXISTS (
                          SELECT 1
                          FROM cao_session_attachments AS attachment
                          JOIN idempotency_results AS close_result
                            ON close_result.actor_id = events.actor_id
                           AND close_result.operation =
                               'close_cao_conversation:' || attachment.id || ':' ||
                               CAST(json_extract(
                                   events.data_json, '$.closed_generation'
                               ) AS TEXT)
                          WHERE attachment.id = events.aggregate_id
                            AND attachment.principal_id = events.actor_id
                            AND json_valid(close_result.result_json)
                            AND json_extract(
                                events.data_json, '$.idempotency_key_digest'
                            ) = cao_service_json_digest(
                                json_quote(close_result.idempotency_key)
                            )
                            AND json_extract(
                                close_result.result_json, '$.status'
                            ) = 'closed'
                            AND json_extract(
                                close_result.result_json, '$.scope'
                            ) = 'conversation'
                            AND json_extract(
                                events.data_json, '$.work_items_canceled'
                            ) = json_extract(
                                close_result.result_json,
                                '$.work_items_canceled'
                            )
                            AND json_extract(
                                events.data_json, '$.managed_workers_stopped'
                            ) = json_extract(
                                close_result.result_json,
                                '$.managed_workers_stopped'
                            )
                            AND (
                                (
                                    (
                                        SELECT COUNT(*)
                                        FROM json_each(events.data_json)
                                    ) = 4
                                    AND (
                                        SELECT COUNT(*)
                                        FROM json_each(close_result.result_json)
                                    ) = 4
                                )
                                OR (
                                    (
                                        SELECT COUNT(*)
                                        FROM json_each(events.data_json)
                                    ) = 9
                                    AND (
                                        SELECT COUNT(*)
                                        FROM json_each(close_result.result_json)
                                    ) = 9
                                    AND json_type(
                                        events.data_json,
                                        '$.non_resumable_worker_threads'
                                    ) = 'integer'
                                    AND json_type(
                                        events.data_json,
                                        '$.resume_loss_acknowledged'
                                    ) IN ('true', 'false')
                                    AND json_type(
                                        events.data_json,
                                        '$.conversation_evidence_digest'
                                    ) = 'text'
                                    AND json_type(
                                        events.data_json,
                                        '$.retired_spec_generation_count'
                                    ) = 'integer'
                                    AND json_type(
                                        events.data_json,
                                        '$.retired_spec_generations_digest'
                                    ) = 'text'
                                    AND json_extract(
                                        events.data_json,
                                        '$.non_resumable_worker_threads'
                                    ) = json_extract(
                                        close_result.result_json,
                                        '$.non_resumable_worker_threads'
                                    )
                                    AND json_extract(
                                        events.data_json,
                                        '$.resume_loss_acknowledged'
                                    ) = json_extract(
                                        close_result.result_json,
                                        '$.resume_loss_acknowledged'
                                    )
                                    AND json_extract(
                                        events.data_json,
                                        '$.conversation_evidence_digest'
                                    ) = json_extract(
                                        close_result.result_json,
                                        '$.conversation_evidence_digest'
                                    )
                                    AND json_extract(
                                        events.data_json,
                                        '$.retired_spec_generation_count'
                                    ) = json_extract(
                                        close_result.result_json,
                                        '$.retired_spec_generation_count'
                                    )
                                    AND json_extract(
                                        events.data_json,
                                        '$.retired_spec_generations_digest'
                                    ) = json_extract(
                                        close_result.result_json,
                                        '$.retired_spec_generations_digest'
                                    )
                                )
                            )
                      )
                  ), 0)
                  AND NOT COALESCE((
                      events.event_type = 'managed_worker.stopped'
                      AND events.aggregate_type = 'managed_worker_spec'
                      AND json_extract(
                          events.data_json, '$.reason_code'
                      ) = 'cao_conversation_closed'
                      AND json_type(
                          events.data_json, '$.closed_generation'
                      ) = 'integer'
                      AND (
                          SELECT COUNT(*) FROM json_each(events.data_json)
                      ) = 3
                      AND EXISTS (
                          SELECT 1
                          FROM managed_worker_specs AS spec
                          JOIN cao_session_attachments AS attachment
                            ON attachment.id = spec.attachment_id
                           AND attachment.principal_id = events.actor_id
                          JOIN idempotency_results AS close_result
                            ON close_result.actor_id = events.actor_id
                           AND close_result.operation =
                               'close_cao_conversation:' || attachment.id || ':' ||
                               CAST(json_extract(
                                   events.data_json, '$.closed_generation'
                               ) AS TEXT)
                           AND close_result.request_digest = json_extract(
                               events.data_json, '$.close_request_digest'
                           )
                          WHERE spec.id = events.aggregate_id
                            AND spec.attachment_generation <= json_extract(
                                events.data_json, '$.closed_generation'
                            )
                      )
                  ), 0)
                  AND NOT COALESCE((
                      events.event_type = 'work.canceled'
                      AND events.aggregate_type = 'work_item'
                      AND json_extract(events.data_json, '$.reason') =
                          'cao_conversation_closed'
                      AND json_type(
                          events.data_json, '$.closed_generation'
                      ) = 'integer'
                      AND (
                          SELECT COUNT(*) FROM json_each(events.data_json)
                      ) = 4
                      AND json_type(
                          events.data_json, '$.directive_id'
                      ) = 'null'
                      AND EXISTS (
                          SELECT 1
                          FROM work_items AS work
                          JOIN cao_session_attachments AS attachment
                            ON attachment.id = work.supervisor_attachment_id
                           AND attachment.principal_id = events.actor_id
                          JOIN idempotency_results AS close_result
                            ON close_result.actor_id = events.actor_id
                           AND close_result.operation =
                               'close_cao_conversation:' || attachment.id || ':' ||
                               CAST(json_extract(
                                   events.data_json, '$.closed_generation'
                               ) AS TEXT)
                           AND close_result.request_digest = json_extract(
                               events.data_json, '$.close_request_digest'
                           )
                          WHERE work.id = events.aggregate_id
                      )
                  ), 0)
                  AND NOT COALESCE((
                      events.event_type = 'boundary.disposed'
                      AND events.aggregate_type = 'work_item'
                      AND json_extract(events.data_json, '$.kind') = 'cancel'
                      AND json_extract(
                          events.data_json, '$.reason_code'
                      ) = 'cao_conversation_closed'
                      AND json_type(
                          events.data_json, '$.attachment_generation'
                      ) = 'integer'
                      AND (
                          SELECT COUNT(*) FROM json_each(events.data_json)
                      ) = 7
                      AND EXISTS (
                          SELECT 1
                          FROM boundaries AS boundary
                          JOIN boundary_dispositions AS disposition
                            ON disposition.boundary_id = boundary.id
                           AND disposition.id = json_extract(
                               events.data_json, '$.disposition_id'
                           )
                           AND disposition.decided_by = events.actor_id
                           AND disposition.kind = 'cancel'
                           AND disposition.generation = json_extract(
                               events.data_json, '$.generation'
                           )
                          JOIN work_items AS work
                            ON work.id = boundary.work_item_id
                           AND work.id = events.aggregate_id
                          JOIN cao_session_attachments AS attachment
                            ON attachment.id = work.supervisor_attachment_id
                           AND attachment.id = json_extract(
                               events.data_json, '$.attachment_id'
                           )
                           AND attachment.principal_id = events.actor_id
                          JOIN idempotency_results AS close_result
                            ON close_result.actor_id = events.actor_id
                           AND close_result.operation =
                               'close_cao_conversation:' || attachment.id || ':' ||
                               CAST(json_extract(
                                   events.data_json,
                                   '$.attachment_generation'
                               ) AS TEXT)
                          WHERE boundary.id = json_extract(
                              events.data_json, '$.boundary_id'
                          )
                      )
                  ), 0)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM boundaries AS boundary
                      LEFT JOIN boundary_dispositions AS disposition
                        ON disposition.boundary_id = boundary.id
                      LEFT JOIN boundary_supersessions AS supersession
                        ON supersession.boundary_id = boundary.id
                      WHERE events.event_type = 'boundary.recorded'
                        AND events.aggregate_type = 'work_item'
                        AND events.aggregate_id = boundary.work_item_id
                        AND json_extract(events.data_json, '$.boundary_id') = boundary.id
                        AND disposition.id IS NULL
                        AND supersession.boundary_id IS NULL
                  )
                """,
                (event_cutoff,),
            ).rowcount
            message_count = connection.execute(
                """
                DELETE FROM messages
                WHERE created_at < ?
                  -- Output receipts remain immutable evidence after the Work
                  -- settles. Retain only their exact source/outbox references;
                  -- unrelated handled messages keep their ordinary retention.
                  AND id NOT IN (
                    SELECT source_message_id FROM worker_output_streams
                    UNION
                    SELECT source_message_id FROM worker_output_receipts
                    UNION
                    SELECT notification_message_id FROM worker_output_receipts
                    WHERE notification_message_id IS NOT NULL
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM boundary_continuations AS continuation
                    WHERE continuation.message_id = messages.id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM work_pause_resumptions AS resumption
                    WHERE resumption.message_id = messages.id
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM message_deliveries AS d
                    WHERE d.message_id = messages.id AND d.state <> 'handled'
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM boundaries AS boundary
                    JOIN work_items AS work
                      ON work.id = boundary.work_item_id
                    JOIN message_deliveries AS delivery
                      ON delivery.message_id = messages.id
                     AND delivery.recipient_id = work.supervisor_id
                    LEFT JOIN boundary_dispositions AS disposition
                      ON disposition.boundary_id = boundary.id
                    LEFT JOIN boundary_supersessions AS supersession
                      ON supersession.boundary_id = boundary.id
                    WHERE boundary.recovery_action = 'system_reconciliation'
                      AND disposition.id IS NULL
                      AND supersession.boundary_id IS NULL
                      AND messages.kind = 'system'
                      AND messages.sender_id = boundary.source_principal_id
                      AND messages.work_item_id = boundary.work_item_id
                      AND messages.attempt_id = boundary.attempt_id
                      AND messages.goal_version = boundary.goal_version
                      AND messages.goal_packet_digest =
                          boundary.goal_packet_digest
                      AND messages.task_packet_digest =
                          boundary.task_packet_digest
                      AND json_valid(messages.payload_json)
                      AND messages.payload_digest =
                          cao_service_json_digest(messages.payload_json)
                      AND json_type(
                          messages.payload_json, '$.action'
                      ) = 'text'
                      AND json_extract(
                          messages.payload_json, '$.action'
                      ) IN (
                          'recover_terminal_worker_attempt',
                          'recover_expired_reasoner_turn',
                          'recover_incomplete_reasoner_turn',
                          'review_runtime_boundary'
                      )
                      AND json_extract(
                          messages.payload_json, '$.boundary_id'
                      ) = boundary.id
                      AND json_extract(
                          messages.payload_json, '$.boundary_kind'
                      ) = boundary.kind
                      AND json_type(
                          messages.payload_json, '$.generation'
                      ) = 'integer'
                      AND json_extract(
                          messages.payload_json, '$.generation'
                      ) = boundary.generation
                      AND (
                          (
                              json_extract(
                                  messages.payload_json, '$.action'
                              ) = 'recover_terminal_worker_attempt'
                              AND (
                                  SELECT COUNT(*) FROM json_each(
                                      messages.payload_json
                                  )
                              ) = 7
                              AND json_type(
                                  messages.payload_json, '$.reason'
                              ) = 'text'
                              AND json_extract(
                                  messages.payload_json, '$.reason'
                              ) = json_extract(
                                  boundary.metadata_json, '$.reason'
                              )
                              AND json_extract(
                                  messages.payload_json,
                                  '$.goal_packet_digest'
                              ) = boundary.goal_packet_digest
                              AND json_extract(
                                  messages.payload_json,
                                  '$.task_packet_digest'
                              ) = boundary.task_packet_digest
                          )
                          OR (
                              json_extract(
                                  messages.payload_json, '$.action'
                              ) IN (
                                  'recover_expired_reasoner_turn',
                                  'recover_incomplete_reasoner_turn'
                              )
                              AND json_type(
                                  messages.payload_json, '$.prior_turn_id'
                              ) = 'text'
                              AND (
                                  (
                                      (
                                          SELECT COUNT(*) FROM json_each(
                                              messages.payload_json
                                          )
                                      ) = 5
                                      AND json_type(
                                          messages.payload_json,
                                          '$.goal_packet_digest'
                                      ) IS NULL
                                      AND json_type(
                                          messages.payload_json,
                                          '$.task_packet_digest'
                                      ) IS NULL
                                  )
                                  OR (
                                      (
                                          SELECT COUNT(*) FROM json_each(
                                              messages.payload_json
                                          )
                                      ) = 7
                                      AND json_extract(
                                          messages.payload_json,
                                          '$.goal_packet_digest'
                                      ) = boundary.goal_packet_digest
                                      AND json_extract(
                                          messages.payload_json,
                                          '$.task_packet_digest'
                                      ) = boundary.task_packet_digest
                                  )
                              )
                          )
                          OR (
                              json_extract(
                                  messages.payload_json, '$.action'
                              ) = 'review_runtime_boundary'
                              AND (
                                  (
                                      (
                                          SELECT COUNT(*) FROM json_each(
                                              messages.payload_json
                                          )
                                      ) = 4
                                      AND json_type(
                                          messages.payload_json,
                                          '$.goal_packet_digest'
                                      ) IS NULL
                                      AND json_type(
                                          messages.payload_json,
                                          '$.task_packet_digest'
                                      ) IS NULL
                                  )
                                  OR (
                                      (
                                          SELECT COUNT(*) FROM json_each(
                                              messages.payload_json
                                          )
                                      ) = 6
                                      AND json_extract(
                                          messages.payload_json,
                                          '$.goal_packet_digest'
                                      ) = boundary.goal_packet_digest
                                      AND json_extract(
                                          messages.payload_json,
                                          '$.task_packet_digest'
                                      ) = boundary.task_packet_digest
                                  )
                              )
                          )
                      )
                  )
                """,
                (message_cutoff,),
            ).rowcount
        return {"events": event_count, "messages": message_count}
