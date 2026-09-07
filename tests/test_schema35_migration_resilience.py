from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer

import cao_control_plane.database as database_module
from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.database import SCHEMA_VERSION, utc_after, utc_now
from cao_control_plane.errors import AuthenticationError
from cao_control_plane.goal_packets import canonical_json, goal_packet_digest
from cao_control_plane.models import (
    BoundaryInput,
    CAOSessionAttachment,
    CloseCAOConversationInput,
    EffectCheckInput,
    EffectGrantInput,
    InstructWorkerThreadInput,
    NewWorkerThreadInput,
    WorkAssignment,
)
from cao_control_plane.projection import verify_projection
from cao_control_plane.release_identity import current_release_identity

_PROJECT_DIGEST = "9" * 64
_MALFORMED_BOUNDARY = '{"broken-boundary":'
_MALFORMED_EVENT = '{"broken-event":'
_MALFORMED_MESSAGE = '{"broken-message":'
_DOMAIN_TABLES = (
    "goal_revisions",
    "work_items",
    "attempts",
    "messages",
    "message_deliveries",
    "boundaries",
    "effect_operations",
    "events",
)


def _attach_current(service: Any, thread_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    release = current_release_identity()
    service.reconcile_conversation_tool_catalog(
        release.mcp_catalog_digest,
        release.release_id,
    )
    attachment = attach_cao_session_with_peer(
        service,
        CAOSessionAttachment(
            native_thread_id=thread_id,
            project_digest=_PROJECT_DIGEST,
            proxy_catalog_digest=release.mcp_catalog_digest,
            proxy_abi_version=CAO_CONVERSATION_PROXY_ABI_VERSION,
        ),
    )
    return attachment, service.authenticate(str(attachment["context_token"]))


def _new_worker(
    service: Any,
    actor: dict[str, Any],
    tmp_path: Path,
    suffix: str,
) -> dict[str, Any]:
    directory = tmp_path / suffix
    directory.mkdir()
    return service.new_worker_thread(
        actor,
        NewWorkerThreadInput(
            working_directory=str(directory),
            runner="codex",
            model="gpt-5.6-terra",
            reasoning_effort="medium",
            name=f"Schema migration Worker {suffix}",
            idempotency_key=f"new-worker-{suffix}",
        ),
    )


def _domain_snapshot(service: Any) -> dict[str, list[tuple[Any, ...]]]:
    with service.db.connect() as connection:
        return {
            table: [
                tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
            ]
            for table in _DOMAIN_TABLES
        }


def _domain_snapshot_without_dashboard_resync(
    service: Any,
) -> dict[str, list[tuple[Any, ...]]]:
    snapshot = _domain_snapshot(service)
    snapshot["events"] = [
        row for row in snapshot["events"] if row[2] != "dashboard.resync_requested"
    ]
    return snapshot


def _database_snapshot(service: Any) -> dict[str, list[tuple[Any, ...]]]:
    """Capture every durable row so a rejected delegation proves zero mutation."""

    with service.db.connect() as connection:
        tables = [
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        return {
            table: [
                tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
            ]
            for table in tables
        }


def _malformed_hex(service: Any, ids: dict[str, str]) -> dict[str, str]:
    with service.db.connect() as connection:
        boundary = connection.execute(
            "SELECT hex(CAST(metadata_json AS BLOB)) AS value FROM boundaries WHERE id = ?",
            (ids["boundary_id"],),
        ).fetchone()
        event = connection.execute(
            "SELECT hex(CAST(data_json AS BLOB)) AS value FROM events WHERE sequence = ?",
            (ids["event_sequence"],),
        ).fetchone()
        message = connection.execute(
            "SELECT hex(CAST(payload_json AS BLOB)) AS value FROM messages WHERE id = ?",
            (ids["message_id"],),
        ).fetchone()
    assert boundary is not None and event is not None and message is not None
    return {
        "boundary": str(boundary["value"]),
        "event": str(event["value"]),
        "message": str(message["value"]),
    }


def _seed_malformed_domain(system: dict[str, Any], actor: dict[str, Any]) -> dict[str, str]:
    service = system["service"]
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Preserve malformed optional evidence",
            objective="Keep the durable domain ledger byte-identical across migration.",
            acceptance=["Migration preserves every canonical domain row."],
            runtime_session_id=system["runtime"]["id"],
            idempotency_key="schema35-malformed-domain",
        ),
    )
    current = service.get_work(str(work["id"]))
    attempt = current["current_attempt"]
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="schema35-malformed-boundary-source",
            work_item_id=str(work["id"]),
            attempt_id=str(attempt["id"]),
            expected_goal_version=int(attempt["goal_version"]),
            expected_goal_packet_digest=str(attempt["goal_packet_digest"]),
            expected_task_packet_digest=str(attempt["task_packet_digest"]),
            expected_generation=int(current["generation"]),
            kind="failure",
            summary="Legacy optional recovery metadata is unavailable.",
            runtime_state="failed",
        ),
    )
    effect_request = EffectCheckInput(
        principal_id=system["worker"]["id"],
        kind="external",
        target="schema35:test-target",
        action="preserve",
        content_digest="8" * 64,
    )
    service.grant_effect(
        actor,
        EffectGrantInput(
            principal_id=system["worker"]["id"],
            kind="external",
            target_pattern="schema35:test-target",
            action_pattern="preserve",
            content_digest="8" * 64,
        ),
    )
    service.start_effect(system["worker"], effect_request)
    with service.db.transaction() as connection:
        event = connection.execute(
            "SELECT sequence FROM events WHERE event_type = 'boundary.recorded' "
            "AND aggregate_type = 'work_item' AND aggregate_id = ? "
            "AND json_extract(data_json, '$.boundary_id') = ?",
            (work["id"], boundary["id"]),
        ).fetchone()
        message = connection.execute(
            "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
            (attempt["id"],),
        ).fetchone()
        assert event is not None and message is not None
        connection.execute(
            "UPDATE boundaries SET metadata_json = ? WHERE id = ?",
            (_MALFORMED_BOUNDARY, boundary["id"]),
        )
        connection.execute(
            "UPDATE events SET data_json = ? WHERE sequence = ?",
            (_MALFORMED_EVENT, event["sequence"]),
        )
        connection.execute(
            "UPDATE messages SET payload_json = ? WHERE id = ?",
            (_MALFORMED_MESSAGE, message["id"]),
        )
    return {
        "boundary_id": str(boundary["id"]),
        "event_sequence": str(event["sequence"]),
        "message_id": str(message["id"]),
    }


def _rebuild_v32_worker_epochs(connection: sqlite3.Connection) -> None:
    connection.execute("DROP INDEX IF EXISTS managed_worker_thread_epochs_one_current")
    connection.execute(
        "ALTER TABLE managed_worker_thread_epochs RENAME TO managed_worker_thread_epochs_v35"
    )
    connection.execute(
        """
        CREATE TABLE managed_worker_thread_epochs (
            id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL
                REFERENCES managed_worker_threads(id) ON DELETE CASCADE,
            generation INTEGER NOT NULL CHECK(generation >= 1),
            runtime_session_id TEXT NOT NULL UNIQUE
                REFERENCES runtime_sessions(id) ON DELETE RESTRICT,
            enrollment_id TEXT NOT NULL UNIQUE
                REFERENCES worker_enrollments(id) ON DELETE RESTRICT,
            created_at TEXT NOT NULL,
            retired_at TEXT,
            UNIQUE(thread_id, generation)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO managed_worker_thread_epochs(
            id, thread_id, generation, runtime_session_id,
            enrollment_id, created_at, retired_at
        )
        SELECT id, thread_id, generation, runtime_session_id,
               enrollment_id, created_at, retired_at
        FROM managed_worker_thread_epochs_v35
        """
    )
    connection.execute("DROP TABLE managed_worker_thread_epochs_v35")
    connection.execute(
        "CREATE UNIQUE INDEX managed_worker_thread_epochs_one_current "
        "ON managed_worker_thread_epochs(thread_id) WHERE retired_at IS NULL"
    )


def _downgrade_attachment_schema(
    system: dict[str, Any],
    attachment: dict[str, Any],
    *,
    version: int,
) -> dict[str, tuple[str, ...]]:
    service = system["service"]
    actor_id = str(system["cao"]["id"])
    now = utc_now()
    expires_at = utc_after(3600)
    with service.db.transaction() as connection:
        goal_columns = {
            str(row["name"]) for row in connection.execute("PRAGMA table_info(goal_revisions)")
        }
        if version < 35 and "supervisor_runtime_session_id" in goal_columns:
            connection.execute(
                "ALTER TABLE goal_revisions DROP COLUMN supervisor_runtime_session_id"
            )
        for table in (
            "cao_session_attachments",
            "cao_attachment_bootstrap_credentials",
            "cao_attachment_connections",
        ):
            columns = {
                str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")
            }
            for column, definition in (
                ("host_root_pid", "INTEGER NOT NULL DEFAULT 0"),
                ("host_root_start_signature", "TEXT NOT NULL DEFAULT ''"),
                ("bridge_pid", "INTEGER NOT NULL DEFAULT 0"),
                ("bridge_start_signature", "TEXT NOT NULL DEFAULT ''"),
            ):
                if column not in columns:
                    connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        bootstrap_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(cao_attachment_bootstrap_credentials)")
        }
        if "host_attestation_receipt_id" not in bootstrap_columns:
            connection.execute(
                "ALTER TABLE cao_attachment_bootstrap_credentials "
                "ADD COLUMN host_attestation_receipt_id TEXT"
            )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS cao_host_attestation_receipts("
            "id TEXT PRIMARY KEY, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT OR REPLACE INTO cao_host_attestation_receipts VALUES('receipt_legacy', ?)",
            (now,),
        )

        connection.execute("DROP INDEX IF EXISTS cao_attachment_connections_one_active_peer")
        connection.execute("DROP TRIGGER IF EXISTS cao_attachment_connections_active_peer_insert")
        connection.execute("DROP TRIGGER IF EXISTS cao_attachment_connections_active_peer_update")
        connection.execute("DROP TRIGGER IF EXISTS cao_conversation_credentials_connection_insert")
        connection.execute("DROP TRIGGER IF EXISTS cao_conversation_credentials_connection_update")
        connection.execute(
            "UPDATE cao_attachment_connections SET "
            "host_root_pid = peer_pid + 1000, "
            "host_root_start_signature = 'legacy-root-' || peer_start_signature, "
            "bridge_pid = peer_pid, bridge_start_signature = peer_start_signature"
        )
        connection.execute(
            "UPDATE cao_attachment_bootstrap_credentials SET "
            "host_root_pid = peer_pid + 1000, "
            "host_root_start_signature = 'legacy-root-' || peer_start_signature, "
            "bridge_pid = peer_pid, bridge_start_signature = peer_start_signature"
        )

        current = connection.execute(
            "SELECT * FROM cao_attachment_connections WHERE id = ?",
            (attachment["connection_id"],),
        ).fetchone()
        assert current is not None
        next_generation = (
            int(
                connection.execute(
                    "SELECT MAX(connection_generation) AS value "
                    "FROM cao_attachment_connections WHERE attachment_id = ?",
                    (attachment["id"],),
                ).fetchone()["value"]
            )
            + 1
        )
        connection_ids = (
            "cac_legacy_duplicate",
            "cac_legacy_zero",
        )
        connection.execute(
            """
            INSERT INTO cao_attachment_connections(
                id, attachment_id, principal_id, generation,
                connection_generation, peer_pid, peer_start_signature,
                proxy_catalog_digest, proxy_abi_version, state,
                lease_expires_at, revoked_at, created_at, updated_at,
                host_root_pid, host_root_start_signature,
                bridge_pid, bridge_start_signature
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, NULL, ?, ?,
                     ?, ?, ?, ?)
            """,
            (
                connection_ids[0],
                attachment["id"],
                actor_id,
                attachment["generation"],
                next_generation,
                current["peer_pid"],
                current["peer_start_signature"],
                current["proxy_catalog_digest"],
                current["proxy_abi_version"],
                expires_at,
                now,
                now,
                int(current["peer_pid"]) + 2000,
                "another-legacy-root",
                current["peer_pid"],
                current["peer_start_signature"],
            ),
        )
        connection.execute(
            """
            INSERT INTO cao_attachment_connections(
                id, attachment_id, principal_id, generation,
                connection_generation, peer_pid, peer_start_signature,
                proxy_catalog_digest, proxy_abi_version, state,
                lease_expires_at, revoked_at, created_at, updated_at,
                host_root_pid, host_root_start_signature,
                bridge_pid, bridge_start_signature
            ) VALUES(?, ?, ?, ?, ?, 0, '', ?, ?, 'active', ?, NULL, ?, ?,
                     0, '', 0, '')
            """,
            (
                connection_ids[1],
                attachment["id"],
                actor_id,
                attachment["generation"],
                next_generation + 1,
                current["proxy_catalog_digest"],
                current["proxy_abi_version"],
                expires_at,
                now,
                now,
            ),
        )
        credential_ids = ("csc_legacy_duplicate", "csc_legacy_zero")
        for ordinal, (credential_id, connection_id) in enumerate(
            zip(credential_ids, connection_ids, strict=True), start=1
        ):
            connection.execute(
                """
                INSERT INTO cao_conversation_credentials(
                    id, attachment_id, connection_id, principal_id, generation,
                    token_hash, state, expires_at, revoked_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'active', ?, NULL, ?, ?)
                """,
                (
                    credential_id,
                    attachment["id"],
                    connection_id,
                    actor_id,
                    attachment["generation"],
                    f"{ordinal:064x}",
                    expires_at,
                    now,
                    now,
                ),
            )
        connection.execute(
            """
            INSERT INTO cao_attachment_bootstrap_credentials(
                id, principal_id, attachment_id, attachment_generation,
                token_hash, one_time, peer_pid, peer_start_signature,
                native_thread_id, project_digest, proxy_catalog_digest,
                proxy_abi_version, state, expires_at, revoked_at,
                created_at, updated_at, host_root_pid,
                host_root_start_signature, bridge_pid,
                bridge_start_signature, host_attestation_receipt_id
            ) VALUES(
                'cab_legacy_active', ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?,
                'active', ?, NULL, ?, ?, ?, 'legacy-root', ?, ?, 'receipt_legacy'
            )
            """,
            (
                actor_id,
                attachment["id"],
                attachment["generation"],
                "7" * 64,
                current["peer_pid"],
                current["peer_start_signature"],
                attachment["native_thread_id"],
                attachment["project_digest"],
                current["proxy_catalog_digest"],
                current["proxy_abi_version"],
                expires_at,
                now,
                now,
                int(current["peer_pid"]) + 1000,
                current["peer_pid"],
                current["peer_start_signature"],
            ),
        )

        if version == 31:
            connection.execute(
                "UPDATE cao_conversation_credentials SET connection_id = NULL WHERE id = ?",
                (attachment["context_credential_id"],),
            )
        if version < 33:
            _rebuild_v32_worker_epochs(connection)
        connection.execute("DELETE FROM schema_migrations WHERE version > ?", (version,))
        connection.execute(
            "UPDATE metadata SET value = ? WHERE key = 'schema_version'",
            (str(version),),
        )
        connection.execute(f"PRAGMA user_version = {version}")
    return {
        "connections": connection_ids,
        "credentials": credential_ids,
        "bootstraps": ("cab_legacy_active",),
    }


@pytest.mark.parametrize("legacy_version", (31, 32, 33, 34))
def test_schema35_upgrade_is_idempotent_and_preserves_domain_bytes(
    system: dict[str, Any], legacy_version: int, tmp_path: Path
) -> None:
    service = system["service"]
    attachment, actor = _attach_current(service, f"schema35-v{legacy_version}")
    managed = _new_worker(service, actor, tmp_path, f"schema35-v{legacy_version}")
    assert managed["worker_thread_id"]
    malformed_ids = _seed_malformed_domain(system, actor)
    expected_domain = _domain_snapshot(service)
    expected_malformed = _malformed_hex(service, malformed_ids)
    legacy = _downgrade_attachment_schema(
        system,
        attachment,
        version=legacy_version,
    )

    service.db.initialize()
    service.db.initialize()

    assert service.db.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    assert service.db.fetchone("SELECT value FROM metadata WHERE key = 'schema_version'")[
        "value"
    ] == str(SCHEMA_VERSION)
    assert _domain_snapshot_without_dashboard_resync(service) == expected_domain
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events WHERE event_type = 'dashboard.resync_requested'"
        )["count"]
        == 1
    )
    assert _malformed_hex(service, malformed_ids) == expected_malformed
    with pytest.raises(AuthenticationError):
        service.authenticate(str(attachment["context_token"]))
    with service.db.connect() as connection:
        assert {
            str(row["state"])
            for row in connection.execute(
                "SELECT state FROM cao_attachment_connections WHERE id IN (?, ?)",
                legacy["connections"],
            )
        } == {"stale"}
        assert {
            str(row["state"])
            for row in connection.execute(
                "SELECT state FROM cao_conversation_credentials WHERE id IN (?, ?)",
                legacy["credentials"],
            )
        } == {"revoked"}
        assert (
            connection.execute(
                "SELECT state FROM cao_attachment_bootstrap_credentials "
                "WHERE id = 'cab_legacy_active'"
            ).fetchone()["state"]
            == "revoked"
        )
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'cao_host_attestation_receipts'"
            ).fetchone()
            is None
        )
        forbidden = {
            "host_root_pid",
            "host_root_start_signature",
            "bridge_pid",
            "bridge_start_signature",
            "host_attestation_receipt_id",
        }
        for table in (
            "cao_session_attachments",
            "cao_attachment_bootstrap_credentials",
            "cao_attachment_connections",
        ):
            columns = {
                str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")
            }
            assert not columns & forbidden
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def _downgrade_goal_runtime_epoch_column(service: Any) -> None:
    with service.db.transaction() as connection:
        columns = {
            str(row["name"]) for row in connection.execute("PRAGMA table_info(goal_revisions)")
        }
        assert "supervisor_runtime_session_id" in columns
        connection.execute("ALTER TABLE goal_revisions DROP COLUMN supervisor_runtime_session_id")
        connection.execute("DELETE FROM schema_migrations WHERE version > 34")
        connection.execute("UPDATE metadata SET value = '34' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 34")


def test_schema35_backfills_historical_goal_runtime_from_sealed_pre_close_epoch(
    system: dict[str, Any], tmp_path: Path
) -> None:
    service = system["service"]
    attachment, actor = _attach_current(service, "schema35-goal-runtime-reopen")
    managed = _new_worker(service, actor, tmp_path, "schema35-goal-runtime-reopen")
    work = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=managed["worker_thread_id"],
            expected_generation=managed["thread_generation"],
            objective="Keep historical Goal and Task packets valid after attachment reopen.",
            acceptance=["The pre-close supervisor runtime remains immutable."],
            idempotency_key="schema35-goal-runtime-reopen-work",
        ),
    )
    work_id = str(work["task"]["work_item_id"])
    before = service.db.fetchone(
        "SELECT packet_json, packet_digest, supervisor_attachment_generation, "
        "supervisor_runtime_session_id FROM goal_revisions "
        "WHERE work_item_id = ? AND version = 1",
        (work_id,),
    )
    assert before is not None
    old_runtime_id = str(before["supervisor_runtime_session_id"])
    assert old_runtime_id == attachment["runtime_session_id"]

    closed = service.close_cao_conversation(
        actor,
        CloseCAOConversationInput(
            idempotency_key="schema35-goal-runtime-reopen-close",
        ),
    )
    assert closed["status"] == "closed"
    reopened, _reopened_actor = _attach_current(
        service,
        "schema35-goal-runtime-reopen",
    )
    assert reopened["id"] == attachment["id"]
    assert reopened["generation"] == attachment["generation"] + 1
    assert reopened["runtime_session_id"] != old_runtime_id

    _downgrade_goal_runtime_epoch_column(service)
    service.db.initialize()
    service.db.initialize()

    migrated = service.db.fetchone(
        "SELECT packet_json, packet_digest, supervisor_attachment_generation, "
        "supervisor_runtime_session_id FROM goal_revisions "
        "WHERE work_item_id = ? AND version = 1",
        (work_id,),
    )
    assert migrated is not None
    assert str(migrated["packet_json"]) == str(before["packet_json"])
    assert str(migrated["packet_digest"]) == str(before["packet_digest"])
    assert int(migrated["supervisor_attachment_generation"]) == int(
        before["supervisor_attachment_generation"]
    )
    assert str(migrated["supervisor_runtime_session_id"]) == old_runtime_id
    assert verify_projection(service.db).healthy is True


def test_schema35_rejects_foreign_runtime_in_sealed_goal_without_mutation(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    _attachment, actor = _attach_current(service, "schema35-goal-runtime-tamper")
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Reject a foreign supervisor runtime",
            objective="Never infer a Goal authority epoch from another principal.",
            acceptance=["Migration rolls back without rewriting sealed evidence."],
            idempotency_key="schema35-goal-runtime-tamper-work",
        ),
    )
    goal = service.db.fetchone(
        "SELECT packet_json FROM goal_revisions WHERE work_item_id = ? AND version = 1",
        (work["id"],),
    )
    assert goal is not None
    packet = json.loads(str(goal["packet_json"]))
    packet["supervisor_attachment"]["runtime_session_id"] = system["runtime"]["id"]
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE goal_revisions SET packet_json = ?, packet_digest = ? "
            "WHERE work_item_id = ? AND version = 1",
            (canonical_json(packet), goal_packet_digest(packet), work["id"]),
        )
    _downgrade_goal_runtime_epoch_column(service)
    before = _database_snapshot(service)

    with pytest.raises(RuntimeError, match="goal attachment runtime is invalid"):
        service.db.initialize()

    assert _database_snapshot(service) == before
    assert service.db.fetchone("PRAGMA user_version")[0] == 34
    assert (
        service.db.fetchone(
            "SELECT 1 FROM pragma_table_info('goal_revisions') "
            "WHERE name = 'supervisor_runtime_session_id'"
        )
        is None
    )


def test_schema35_does_not_guess_runtime_for_unsealed_prior_generation(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment, actor = _attach_current(service, "schema35-goal-runtime-unsealed")
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Do not guess an unsealed runtime epoch",
            objective="Reject an ambiguous legacy Goal after attachment generation advances.",
            acceptance=["Migration preserves the ambiguous legacy bytes and rolls back."],
            idempotency_key="schema35-goal-runtime-unsealed-work",
        ),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE goal_revisions SET packet_json = '{}', packet_digest = '' "
            "WHERE work_item_id = ? AND version = 1",
            (work["id"],),
        )
        connection.execute(
            "UPDATE cao_session_attachments SET generation = generation + 1 WHERE id = ?",
            (attachment["id"],),
        )
    _downgrade_goal_runtime_epoch_column(service)
    before = _database_snapshot(service)

    with pytest.raises(
        RuntimeError,
        match="legacy unsealed goal attachment epoch is ambiguous",
    ):
        service.db.initialize()

    assert _database_snapshot(service) == before
    assert service.db.fetchone("PRAGMA user_version")[0] == 34


def test_fresh_goal_runtime_epoch_is_tamper_evident(system: dict[str, Any]) -> None:
    service = system["service"]
    attachment, actor = _attach_current(service, "schema35-goal-runtime-fresh")
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Seal the fresh supervisor runtime",
            objective="Bind the Goal packet to its exact CAO wake epoch.",
            acceptance=["A column-only epoch rewrite is detected."],
            idempotency_key="schema35-goal-runtime-fresh-work",
        ),
    )
    goal = service.db.fetchone(
        "SELECT packet_json, packet_digest, supervisor_runtime_session_id "
        "FROM goal_revisions WHERE work_item_id = ? AND version = 1",
        (work["id"],),
    )
    assert goal is not None
    assert str(goal["supervisor_runtime_session_id"]) == attachment["runtime_session_id"]
    assert (
        json.loads(str(goal["packet_json"]))["supervisor_attachment"]["runtime_session_id"]
        == attachment["runtime_session_id"]
    )
    assert verify_projection(service.db).healthy is True

    service.db.execute(
        "UPDATE goal_revisions SET supervisor_runtime_session_id = ? "
        "WHERE work_item_id = ? AND version = 1",
        (system["runtime"]["id"], work["id"]),
    )
    projection = verify_projection(service.db)
    assert projection.healthy is False
    assert "goal.packet_invalid" in {violation.code for violation in projection.violations}
    unchanged = service.db.fetchone(
        "SELECT packet_json, packet_digest FROM goal_revisions "
        "WHERE work_item_id = ? AND version = 1",
        (work["id"],),
    )
    assert unchanged is not None
    assert str(unchanged["packet_json"]) == str(goal["packet_json"])
    assert str(unchanged["packet_digest"]) == str(goal["packet_digest"])


def test_fresh_attachment_contract_columns_have_no_permissive_defaults(
    system: dict[str, Any],
) -> None:
    with system["service"].db.connect() as connection:
        for table in (
            "cao_attachment_bootstrap_credentials",
            "cao_attachment_connections",
        ):
            columns = {
                str(row["name"]): row for row in connection.execute(f"PRAGMA table_info({table})")
            }
            for column in (
                "peer_pid",
                "peer_start_signature",
                "proxy_catalog_digest",
                "proxy_abi_version",
            ):
                assert columns[column]["dflt_value"] is None


def test_active_attachment_contract_rejects_raw_invalid_rows_but_preserves_history(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment, actor = _attach_current(service, "schema35-active-contract")
    active_connection = service.db.fetchone(
        "SELECT proxy_catalog_digest, proxy_abi_version "
        "FROM cao_attachment_connections WHERE id = ?",
        (attachment["connection_id"],),
    )
    assert active_connection is not None
    valid_digest = str(active_connection["proxy_catalog_digest"])
    valid_abi = int(active_connection["proxy_abi_version"])
    invalid_contracts = (
        (0, "peer", valid_digest, valid_abi),
        (91_000, "", valid_digest, valid_abi),
        (91_001, "peer-digest-empty", "", valid_abi),
        (91_002, "peer-digest-uppercase", "A" * 64, valid_abi),
        (91_003, "peer-abi-stale", valid_digest, valid_abi + 1),
    )
    next_connection_generation = (
        int(
            service.db.fetchone(
                "SELECT MAX(connection_generation) AS value "
                "FROM cao_attachment_connections WHERE attachment_id = ?",
                (attachment["id"],),
            )["value"]
        )
        + 1
    )
    for index, (peer_pid, peer_signature, digest, abi) in enumerate(invalid_contracts):
        with pytest.raises(sqlite3.IntegrityError, match="active CAO connection contract"):
            service.db.execute(
                """
                INSERT INTO cao_attachment_connections(
                    id, attachment_id, principal_id, generation,
                    connection_generation, peer_pid, peer_start_signature,
                    proxy_catalog_digest, proxy_abi_version, state,
                    lease_expires_at, revoked_at, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, NULL, ?, ?)
                """,
                (
                    f"cac_invalid_contract_{index}",
                    attachment["id"],
                    actor["id"],
                    attachment["generation"],
                    next_connection_generation + index,
                    peer_pid,
                    peer_signature,
                    digest,
                    abi,
                    utc_after(300),
                    utc_now(),
                    utc_now(),
                ),
            )
        with pytest.raises(sqlite3.IntegrityError, match="active CAO bootstrap contract"):
            service.db.execute(
                """
                INSERT INTO cao_attachment_bootstrap_credentials(
                    id, principal_id, attachment_id, attachment_generation,
                    token_hash, one_time, peer_pid, peer_start_signature,
                    native_thread_id, project_digest, proxy_catalog_digest,
                    proxy_abi_version, state, expires_at, revoked_at,
                    created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?,
                         'active', ?, NULL, ?, ?)
                """,
                (
                    f"cab_invalid_contract_{index}",
                    actor["id"],
                    attachment["id"],
                    attachment["generation"],
                    f"invalid-contract-hash-{index}",
                    peer_pid,
                    peer_signature,
                    "schema35-active-contract",
                    _PROJECT_DIGEST,
                    digest,
                    abi,
                    utc_after(300),
                    utc_now(),
                    utc_now(),
                ),
            )

    history_created_at = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO cao_attachment_connections(
                id, attachment_id, principal_id, generation,
                connection_generation, peer_pid, peer_start_signature,
                proxy_catalog_digest, proxy_abi_version, state,
                lease_expires_at, revoked_at, created_at, updated_at
            ) VALUES('cac_legacy_blank_history', ?, ?, ?, ?, 0, '', '', 0,
                     'stale', ?, ?, ?, ?)
            """,
            (
                attachment["id"],
                actor["id"],
                attachment["generation"],
                next_connection_generation + len(invalid_contracts),
                utc_after(300),
                history_created_at,
                history_created_at,
                history_created_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO cao_attachment_bootstrap_credentials(
                id, principal_id, attachment_id, attachment_generation,
                token_hash, one_time, peer_pid, peer_start_signature,
                native_thread_id, project_digest, proxy_catalog_digest,
                proxy_abi_version, state, expires_at, revoked_at,
                created_at, updated_at
            ) VALUES('cab_legacy_blank_history', ?, ?, ?,
                     'legacy-blank-history-hash', 1, 0, '', '', '', '', 0,
                     'revoked', ?, ?, ?, ?)
            """,
            (
                actor["id"],
                attachment["id"],
                attachment["generation"],
                utc_after(300),
                history_created_at,
                history_created_at,
                history_created_at,
            ),
        )
    service.db.initialize()
    assert dict(
        service.db.fetchone(
            "SELECT state, peer_pid, peer_start_signature, proxy_catalog_digest, "
            "proxy_abi_version FROM cao_attachment_connections "
            "WHERE id = 'cac_legacy_blank_history'"
        )
    ) == {
        "state": "stale",
        "peer_pid": 0,
        "peer_start_signature": "",
        "proxy_catalog_digest": "",
        "proxy_abi_version": 0,
    }
    assert dict(
        service.db.fetchone(
            "SELECT state, peer_pid, peer_start_signature, proxy_catalog_digest, "
            "proxy_abi_version FROM cao_attachment_bootstrap_credentials "
            "WHERE id = 'cab_legacy_blank_history'"
        )
    ) == {
        "state": "revoked",
        "peer_pid": 0,
        "peer_start_signature": "",
        "proxy_catalog_digest": "",
        "proxy_abi_version": 0,
    }
    with pytest.raises(sqlite3.IntegrityError, match="active CAO connection contract"):
        service.db.execute(
            "UPDATE cao_attachment_connections SET state = 'active' "
            "WHERE id = 'cac_legacy_blank_history'"
        )
    with pytest.raises(sqlite3.IntegrityError, match="active CAO bootstrap contract"):
        service.db.execute(
            "UPDATE cao_attachment_bootstrap_credentials SET state = 'active' "
            "WHERE id = 'cab_legacy_blank_history'"
        )


def _authority_snapshot(service: Any) -> dict[str, Any]:
    with service.db.connect() as connection:
        return {
            "metadata": tuple(
                connection.execute(
                    "SELECT value FROM metadata WHERE key = 'schema_version'"
                ).fetchone()
            ),
            "user_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
            "schema": [
                tuple(row)
                for row in connection.execute(
                    "SELECT type, name, tbl_name, sql FROM sqlite_master "
                    "WHERE name LIKE 'cao_%' ORDER BY type, name"
                )
            ],
            "connections": [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM cao_attachment_connections ORDER BY id"
                )
            ],
            "credentials": [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM cao_conversation_credentials ORDER BY id"
                )
            ],
            "bootstraps": [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM cao_attachment_bootstrap_credentials ORDER BY id"
                )
            ],
        }


def test_schema35_upgrade_rolls_back_every_authority_change_on_failure(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    service = system["service"]
    attachment, actor = _attach_current(service, "schema35-rollback")
    _new_worker(service, actor, tmp_path, "schema35-rollback")
    malformed_ids = _seed_malformed_domain(system, actor)
    expected_domain = _domain_snapshot(service)
    expected_malformed = _malformed_hex(service, malformed_ids)
    _downgrade_attachment_schema(system, attachment, version=34)
    expected_legacy_domain = _domain_snapshot(service)
    expected_authority = _authority_snapshot(service)
    real_normalize = database_module._normalize_cao_attachment_peer_schema_v35

    def fail_after_normalization(
        connection: sqlite3.Connection, *, existing_schema_version: int
    ) -> None:
        real_normalize(
            connection,
            existing_schema_version=existing_schema_version,
        )
        raise RuntimeError("injected schema35 failure")

    monkeypatch.setattr(
        database_module,
        "_normalize_cao_attachment_peer_schema_v35",
        fail_after_normalization,
    )
    with pytest.raises(RuntimeError, match="injected schema35 failure"):
        service.db.initialize()
    assert _authority_snapshot(service) == expected_authority
    assert _domain_snapshot(service) == expected_legacy_domain
    assert _malformed_hex(service, malformed_ids) == expected_malformed

    monkeypatch.setattr(
        database_module,
        "_normalize_cao_attachment_peer_schema_v35",
        real_normalize,
    )
    service.db.initialize()
    service.db.initialize()
    assert _domain_snapshot_without_dashboard_resync(service) == expected_domain
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events WHERE event_type = 'dashboard.resync_requested'"
        )["count"]
        == 1
    )
    assert _malformed_hex(service, malformed_ids) == expected_malformed


def _insert_public_malformed_boundary(service: Any, work_item_id: str) -> dict[str, str]:
    with service.db.transaction() as connection:
        attempt = connection.execute(
            "SELECT * FROM attempts WHERE work_item_id = ? ORDER BY attempt_number DESC LIMIT 1",
            (work_item_id,),
        ).fetchone()
        work = connection.execute(
            "SELECT * FROM work_items WHERE id = ?",
            (work_item_id,),
        ).fetchone()
        message = connection.execute(
            "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
            (attempt["id"],),
        ).fetchone()
        assert attempt is not None and work is not None and message is not None
        boundary_id = "bnd_public_malformed_optional"
        event_id = "evt_public_malformed_optional"
        connection.execute(
            """
            INSERT INTO boundaries(
                id, source_principal_id, source_event_id, work_item_id,
                attempt_id, goal_version, generation, goal_packet_digest,
                task_packet_digest, kind, summary, runtime_state,
                metadata_json, recovery_action, recovery_target,
                recovery_model, recovery_reasoning_effort,
                input_digest, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'failure', ?, 'failed', ?,
                     '', '', '', '', ?, ?)
            """,
            (
                boundary_id,
                attempt["worker_id"],
                "public-malformed-source",
                work_item_id,
                attempt["id"],
                attempt["goal_version"],
                work["generation"],
                attempt["goal_packet_digest"],
                attempt["task_packet_digest"],
                "Optional recovery metadata is malformed.",
                _MALFORMED_BOUNDARY,
                "6" * 64,
                utc_now(),
            ),
        )
        cursor = connection.execute(
            """
            INSERT INTO events(
                id, event_type, aggregate_type, aggregate_id,
                actor_id, data_json, correlation_id, causation_id, created_at
            ) VALUES(?, 'boundary.recorded', 'work_item', ?, ?, ?, '', ?, ?)
            """,
            (
                event_id,
                work_item_id,
                attempt["worker_id"],
                _MALFORMED_EVENT,
                boundary_id,
                utc_now(),
            ),
        )
        connection.execute(
            "UPDATE messages SET payload_json = ? WHERE id = ?",
            (_MALFORMED_MESSAGE, message["id"]),
        )
    return {
        "boundary_id": boundary_id,
        "event_sequence": str(cursor.lastrowid),
        "message_id": str(message["id"]),
    }
