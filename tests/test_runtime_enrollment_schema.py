from __future__ import annotations

import json
import sqlite3
from dataclasses import replace

import pytest
from pydantic import ValidationError

from cao_control_plane.config import Settings
from cao_control_plane.database import SCHEMA_VERSION, Database, utc_now
from cao_control_plane.models import (
    EnrollmentState,
    PrincipalCreate,
    RuntimeHeartbeat,
    RuntimeRegistration,
)


def test_schema_v18_fresh_database_records_the_runtime_diagnostic_scrub_migration(
    settings: Settings,
) -> None:
    database = Database(settings)

    version = database.fetchone("SELECT value FROM metadata WHERE key = 'schema_version'")
    migration = database.fetchone(
        "SELECT description FROM schema_migrations WHERE version = 18"
    )

    assert version is not None and version["value"] == str(SCHEMA_VERSION)
    assert migration is not None
    assert migration["description"] == (
        "scrub legacy runtime adapter transcripts from durable diagnostics"
    )
    ticket_columns = {
        row["name"]
        for row in database.fetchall("PRAGMA table_info(runtime_enrollment_tickets)")
    }
    assert "attempt_id" in ticket_columns


def test_schema_v29_adds_attempt_binding_to_existing_runtime_tickets(
    settings: Settings,
) -> None:
    database = Database(settings)
    with database.transaction() as connection:
        connection.execute(
            "DROP INDEX IF EXISTS runtime_enrollment_tickets_attempt_generation_idx"
        )
        connection.execute(
            "ALTER TABLE runtime_enrollment_tickets DROP COLUMN attempt_id"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 29")
        connection.execute(
            "UPDATE metadata SET value = '28' WHERE key = 'schema_version'"
        )
        connection.execute("PRAGMA user_version = 28")

    database.initialize()

    columns = {
        row["name"]
        for row in database.fetchall("PRAGMA table_info(runtime_enrollment_tickets)")
    }
    assert "attempt_id" in columns
    assert database.fetchone(
        "SELECT description FROM schema_migrations WHERE version = 29"
    ) is not None


def test_schema_v17_upgrade_removes_legacy_runtime_transcripts_value_blindly(
    settings: Settings,
) -> None:
    database = Database(settings)
    now = utc_now()
    sentinel = "legacy-runtime-conversation-bearing-sentinel"
    with database.transaction() as connection:
        connection.execute(
            """
            INSERT INTO principals(id, name, role, token_hash, enabled, metadata_json, created_at, updated_at)
            VALUES ('runtime-diagnostic-principal', 'runtime-diagnostic-principal', 'worker', 'hash', 1, '{}', ?, ?)
            """,
            (now, now),
        )
        connection.execute(
            """
            INSERT INTO runtime_sessions(
                id, principal_id, adapter, endpoint, native_session_id, state,
                lease_expires_at, heartbeat_at, metadata_json, created_at, updated_at
            ) VALUES (?, 'runtime-diagnostic-principal', 'codex-app-server', '', '', 'ready', ?, ?, ?, ?, ?)
            """,
            (
                "runtime-diagnostic-runtime",
                now,
                now,
                json.dumps(
                    {
                        "command": ["codex", "app-server"],
                        "safe_launch_option": "retain",
                        "last_output": sentinel,
                        "last_dispatch": {
                            "output": sentinel,
                            "error": sentinel,
                            "metadata": {"notification": sentinel},
                        },
                    }
                ),
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO runtime_sessions(
                id, principal_id, adapter, endpoint, native_session_id, state,
                lease_expires_at, heartbeat_at, metadata_json, created_at, updated_at
            ) VALUES ('runtime-diagnostic-malformed', 'runtime-diagnostic-principal',
                      'codex-app-server', '', '', 'ready', ?, ?, ?, ?, ?)
            """,
            (now, now, sentinel, now, now),
        )
        connection.execute(
            """
            INSERT INTO messages(
                id, work_item_id, attempt_id, sender_id, kind, payload_json,
                payload_digest, message_digest, correlation_id, causation_id,
                idempotency_key, goal_version, goal_packet_digest, task_packet_digest, created_at
            ) VALUES ('runtime-diagnostic-message', NULL, NULL, 'runtime-diagnostic-principal', 'instruction',
                      '{}', '', '', '', '', '', NULL, '', '', ?)
            """,
            (now,),
        )
        connection.execute(
            """
            INSERT INTO message_deliveries(
                message_id, recipient_id, state, generation, attempts, next_attempt_at,
                runtime_session_id, lease_until, owner_token, delivered_at, acknowledged_at,
                handled_at, last_error, created_at, updated_at
            ) VALUES ('runtime-diagnostic-message', 'runtime-diagnostic-principal', 'dispatched', 1, 1, ?,
                      'runtime-diagnostic-runtime', NULL, '', NULL, NULL, NULL, ?, ?, ?)
            """,
            (now, sentinel, now, now),
        )
        for index, event_type in enumerate(
            (
                "runtime.message_delivered",
                "runtime.message_retry_scheduled",
                "runtime.message_dead",
                "runtime.message_delivery_unknown",
            ),
            1,
        ):
            connection.execute(
                """
                INSERT INTO events(
                    id, event_type, aggregate_type, aggregate_id, actor_id,
                    data_json, correlation_id, causation_id, created_at
                ) VALUES (?, ?, 'runtime', 'runtime-diagnostic-runtime', '', ?, '', '', ?)
                """,
                (
                    f"runtime-diagnostic-event-{index}",
                    event_type,
                    json.dumps(
                        {
                            "result": {"output": sentinel, "error": sentinel},
                            "error": sentinel,
                            "metadata": {"raw_notification": sentinel},
                        }
                    ),
                    now,
                ),
            )

    database.execute("UPDATE metadata SET value = '17' WHERE key = 'schema_version'")
    database.execute("PRAGMA user_version = 17")
    database.initialize()

    runtime = database.fetchone(
        "SELECT metadata_json FROM runtime_sessions WHERE id = 'runtime-diagnostic-runtime'"
    )
    malformed_runtime = database.fetchone(
        "SELECT metadata_json FROM runtime_sessions WHERE id = 'runtime-diagnostic-malformed'"
    )
    delivery = database.fetchone(
        "SELECT state, last_error FROM message_deliveries WHERE message_id = 'runtime-diagnostic-message'"
    )
    event_rows = database.fetchall(
        "SELECT event_type, data_json FROM events WHERE id LIKE 'runtime-diagnostic-event-%' ORDER BY id"
    )
    migration = database.fetchone(
        "SELECT description FROM schema_migrations WHERE version = 18"
    )

    assert runtime is not None
    assert json.loads(str(runtime["metadata_json"])) == {
        "command": ["codex", "app-server"],
        "safe_launch_option": "retain",
    }
    assert malformed_runtime is not None and malformed_runtime["metadata_json"] == "{}"
    assert delivery is not None
    assert delivery["state"] == "dispatched"
    assert delivery["last_error"] == "legacy_runtime_diagnostic_redacted"
    assert [(row["event_type"], json.loads(str(row["data_json"]))) for row in event_rows] == [
        ("runtime.message_delivered", {"outcome": "delivered"}),
        ("runtime.message_retry_scheduled", {"outcome": "retry_scheduled"}),
        ("runtime.message_dead", {"outcome": "dead"}),
        ("runtime.message_delivery_unknown", {"outcome": "unknown"}),
    ]
    assert migration is not None

    # A subsequent startup must be a no-op for scrubbed state; v18 does not
    # reinterpret current allowlisted runtime summaries or touch new events.
    before = (
        str(runtime["metadata_json"]),
        str(delivery["last_error"]),
        tuple((str(row["event_type"]), str(row["data_json"])) for row in event_rows),
    )
    database.initialize()
    after_runtime = database.fetchone(
        "SELECT metadata_json FROM runtime_sessions WHERE id = 'runtime-diagnostic-runtime'"
    )
    after_delivery = database.fetchone(
        "SELECT last_error FROM message_deliveries WHERE message_id = 'runtime-diagnostic-message'"
    )
    after_events = database.fetchall(
        "SELECT event_type, data_json FROM events WHERE id LIKE 'runtime-diagnostic-event-%' ORDER BY id"
    )
    assert after_runtime is not None and after_delivery is not None
    assert before == (
        str(after_runtime["metadata_json"]),
        str(after_delivery["last_error"]),
        tuple((str(row["event_type"]), str(row["data_json"])) for row in after_events),
    )
    for table, column in (
        ("runtime_sessions", "metadata_json"),
        ("events", "data_json"),
        ("message_deliveries", "last_error"),
    ):
        row = database.fetchone(
            f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
            (f"%{sentinel}%",),
        )
        assert row is not None and row["count"] == 0, (table, column)
    for path in (
        database.path,
        database.path.with_name(f"{database.path.name}-wal"),
        database.path.with_name(f"{database.path.name}-shm"),
    ):
        if path.exists():
            assert sentinel.encode("utf-8") not in path.read_bytes(), path


def _runtime(connection: sqlite3.Connection, principal_id: str, runtime_id: str) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO runtime_sessions(
            id, principal_id, adapter, state, lease_expires_at, heartbeat_at,
            created_at, updated_at
        ) VALUES (?, ?, 'codex-app-server', 'ready', ?, ?, ?, ?)
        """,
        (runtime_id, principal_id, now, now, now, now),
    )


def _enrollment(
    connection: sqlite3.Connection,
    *,
    enrollment_id: str,
    principal_id: str,
    runtime_id: str,
    state: str = "awaiting_handshake",
    generation: int = 0,
) -> None:
    now = utc_now()
    connection.execute(
        """
        INSERT INTO worker_enrollments(
            id, principal_id, runtime_session_id, state, generation, managed,
            required_tools_digest, discovered_tools_digest, protocol_version,
            heartbeat_sequence, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 1, '', '', '', 0, ?, ?)
        """,
        (enrollment_id, principal_id, runtime_id, state, generation, now, now),
    )


def test_schema_v8_enforces_managed_worker_enrollment_invariants(system):
    database = system["service"].db
    principal_id = system["service"].create_principal(
        system["cao"],
        PrincipalCreate(name="schema-enrollment-worker", role="worker"),
    )["principal"]["id"]
    now = utc_now()
    with database.transaction() as connection:
        _runtime(connection, principal_id, "runtime-enrollment-1")
        _runtime(connection, principal_id, "runtime-enrollment-2")
        _runtime(connection, principal_id, "runtime-enrollment-3")
        _enrollment(
            connection,
            enrollment_id="enrollment-1",
            principal_id=principal_id,
            runtime_id="runtime-enrollment-1",
        )

        with pytest.raises(sqlite3.IntegrityError):
            _enrollment(
                connection,
                enrollment_id="enrollment-duplicate-principal",
                principal_id=principal_id,
                runtime_id="runtime-enrollment-2",
                state="ready",
            )

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE worker_enrollments SET generation = -1 WHERE id = 'enrollment-1'"
            )

        connection.execute(
            "UPDATE worker_enrollments SET state = 'revoked', revoked_at = ? "
            "WHERE id = 'enrollment-1'",
            (now,),
        )
        _enrollment(
            connection,
            enrollment_id="enrollment-2",
            principal_id=principal_id,
            runtime_id="runtime-enrollment-2",
            state="ready",
            generation=1,
        )
        connection.execute(
            """
            INSERT INTO runtime_credentials(
                id, enrollment_id, principal_id, generation, token_hash, state,
                expires_at, created_at, updated_at
            ) VALUES ('credential-1', 'enrollment-2', ?, 1, 'token-hash-1', 'active', ?, ?, ?)
            """,
            (principal_id, now, now, now),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO runtime_credentials(
                    id, enrollment_id, principal_id, generation, token_hash, state,
                    expires_at, created_at, updated_at
                ) VALUES ('credential-2', 'enrollment-2', ?, 1, 'token-hash-2', 'active', ?, ?, ?)
                """,
                (principal_id, now, now, now),
            )
        connection.execute(
            """
            INSERT INTO runtime_enrollment_tickets(
                id, enrollment_id, generation, ticket_hash, state, expires_at, created_at, updated_at
            ) VALUES ('ticket-1', 'enrollment-2', 1, 'ticket-hash-1', 'pending', ?, ?, ?)
            """,
            (now, now, now),
        )
        connection.execute("DELETE FROM worker_enrollments WHERE id = 'enrollment-2'")

    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert connection.execute(
            "SELECT COUNT(*) FROM runtime_credentials WHERE enrollment_id = 'enrollment-2'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM runtime_enrollment_tickets "
            "WHERE enrollment_id = 'enrollment-2'"
        ).fetchone()[0] == 0


def test_schema_v8_adds_enrollment_tables_to_an_existing_v7_database(tmp_path):
    settings = replace(Settings(), state_dir=tmp_path / "state")
    database = Database(settings)
    with database.connect() as connection:
        connection.execute("DROP TABLE runtime_credentials")
        connection.execute("DROP TABLE runtime_enrollment_tickets")
        connection.execute("DROP TABLE worker_enrollments")
        connection.execute("UPDATE metadata SET value = '7' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 7")

    restarted = Database(settings)
    with restarted.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        } >= {
            "worker_enrollments",
            "runtime_enrollment_tickets",
            "runtime_credentials",
        }
        assert connection.execute(
            "SELECT description FROM schema_migrations WHERE version = 8"
        ).fetchone()["description"] == "managed Worker MCP enrollment tickets and runtime credentials"


def test_runtime_enrollment_api_models_hide_tickets_and_validate_generations():
    assert EnrollmentState.READY.value == "ready"
    assert RuntimeRegistration(adapter="codex-app-server").managed_mcp is None
    assert RuntimeRegistration(adapter="codex-app-server", managed_mcp=True).managed_mcp is True
    with pytest.raises(ValidationError):
        RuntimeRegistration(adapter="tmux")
    heartbeat = RuntimeHeartbeat(expected_enrollment_generation=0, sequence=0)
    assert heartbeat.expected_enrollment_generation == heartbeat.sequence == 0
    with pytest.raises(ValidationError):
        RuntimeHeartbeat(expected_enrollment_generation=-1)
    with pytest.raises(ValidationError):
        RuntimeHeartbeat(sequence=-1)

def test_runtime_enrollment_settings_validate_and_load(monkeypatch, tmp_path):
    launch_dir = tmp_path / "launches"
    monkeypatch.setenv("CAO_A2A_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("CAO_A2A_RUNTIME_LAUNCH_DIR", str(launch_dir))
    monkeypatch.setenv("CAO_A2A_RUNTIME_ENROLLMENT_TICKET_TTL_SECONDS", "120")
    monkeypatch.setenv("CAO_A2A_RUNTIME_CREDENTIAL_TTL_SECONDS", "600")
    settings = Settings.load(tmp_path / "missing.toml")
    assert settings.runtime_launch_dir == launch_dir
    assert settings.runtime_enrollment_ticket_ttl_seconds == 120
    assert settings.runtime_credential_ttl_seconds == 600
    assert launch_dir.is_dir()

    with pytest.raises(ValueError, match="runtime_enrollment_ticket_ttl_seconds"):
        replace(settings, runtime_enrollment_ticket_ttl_seconds=0)
