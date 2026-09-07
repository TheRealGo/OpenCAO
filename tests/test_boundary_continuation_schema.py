from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from cao_control_plane.database import _SUPERVISION_PAUSE_TRIGGERS, SCHEMA_VERSION, utc_now
from cao_control_plane.goal_packets import canonical_digest
from cao_control_plane.models import WorkAssignment

_CONTINUATION_TRIGGERS = (
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


def _create_work(system: dict[str, Any], suffix: str) -> dict[str, Any]:
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=str(system["worker"]["id"]),
            runtime_session_id=str(system["runtime"]["id"]),
            title=f"WAIT_USER schema {suffix}",
            objective=f"Prove typed requester continuation {suffix}",
            acceptance=["The exact WAIT_USER boundary is consumed once."],
            non_goals=["Do not derive authority from JSON or events."],
        ),
    )


def _seed_wait_user(
    system: dict[str, Any],
    work: dict[str, Any],
    suffix: str,
    *,
    set_pointer: bool = True,
) -> dict[str, Any]:
    service = system["service"]
    current = service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))
    attempt = service.db.fetchone(
        "SELECT * FROM attempts WHERE work_item_id = ? ORDER BY attempt_number DESC LIMIT 1",
        (work["id"],),
    )
    assert current is not None and attempt is not None
    boundary_id = f"bnd_wait_{suffix}"
    turn_id = f"turn_wait_{suffix}"
    disposition_id = f"bdisp_wait_{suffix}"
    now = utc_now()
    boundary_input_digest = f"input-digest-{suffix}"
    request_digest = f"request-digest-{suffix}"
    turn_input_digest = canonical_digest(
        {
            "boundary_id": boundary_id,
            "boundary_input_digest": boundary_input_digest,
            "goal_packet_digest": attempt["goal_packet_digest"],
            "task_packet_digest": attempt["task_packet_digest"],
            "generation": current["generation"],
        }
    )
    turn_result_digest = canonical_digest(
        {
            "boundary_id": boundary_id,
            "disposition_id": disposition_id,
            "request_digest": request_digest,
            "goal_packet_digest": attempt["goal_packet_digest"],
            "task_packet_digest": attempt["task_packet_digest"],
        }
    )
    with service.db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO boundaries(
                id, source_principal_id, source_event_id, work_item_id,
                attempt_id, goal_version, generation, goal_packet_digest,
                task_packet_digest, kind, summary, runtime_state,
                metadata_json, input_digest, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'question', ?, 'waiting',
                     '{}', ?, ?)
            """,
            (
                boundary_id,
                attempt["worker_id"],
                f"event-wait-{suffix}",
                work["id"],
                attempt["id"],
                attempt["goal_version"],
                current["generation"],
                attempt["goal_packet_digest"],
                attempt["task_packet_digest"],
                f"Question {suffix}",
                boundary_input_digest,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO reasoner_turns(
                id, supervisor_id, work_item_id, boundary_id, generation,
                goal_version, goal_packet_digest, task_packet_digest,
                input_digest, result_digest, state, lease_token_digest,
                lease_expires_at, idempotency_key, completed_at,
                created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?)
            """,
            (
                turn_id,
                current["supervisor_id"],
                work["id"],
                boundary_id,
                current["generation"],
                attempt["goal_version"],
                attempt["goal_packet_digest"],
                attempt["task_packet_digest"],
                turn_input_digest,
                turn_result_digest,
                f"lease-{suffix}",
                now,
                f"turn-key-{suffix}",
                now,
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO boundary_dispositions(
                id, boundary_id, reasoner_turn_id, decided_by, generation,
                kind, reason, instruction, resume_condition,
                request_digest, created_at
            ) VALUES(?, ?, ?, ?, ?, 'wait_user', ?, ?, ?, ?, ?)
            """,
            (
                disposition_id,
                boundary_id,
                turn_id,
                current["supervisor_id"],
                current["generation"],
                f"Requester decision {suffix}",
                f"Choose exact option {suffix}",
                f"Requester supplies option {suffix}",
                request_digest,
                now,
            ),
        )
        connection.execute(
            "UPDATE attempts SET state = 'input_required', updated_at = ? WHERE id = ?",
            (now, attempt["id"]),
        )
        if set_pointer:
            connection.execute(
                """
                UPDATE work_items
                SET state = 'user_needed', attention_owner = 'user',
                    user_needed_boundary_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (boundary_id, now, work["id"]),
            )
        else:
            connection.execute(
                """
                UPDATE work_items
                SET state = 'user_needed', attention_owner = 'user', updated_at = ?
                WHERE id = ?
                """,
                (now, work["id"]),
            )
    return {
        "boundary_id": boundary_id,
        "turn_id": turn_id,
        "disposition_id": disposition_id,
        "attempt_id": str(attempt["id"]),
        "generation": int(current["generation"]),
    }


def _insert_continuation(
    system: dict[str, Any],
    wait: dict[str, Any],
    *,
    successor_attempt_id: str | None = None,
    successor_generation: int | None = None,
    message_id: str,
) -> None:
    system["service"].db.execute(
        """
        INSERT INTO boundary_continuations(
            boundary_id, work_item_id, source_attempt_id, source_generation,
            successor_attempt_id, successor_generation, message_id,
            decided_by, created_at
        )
        SELECT ?, boundary.work_item_id, boundary.attempt_id, boundary.generation,
               ?, ?, ?, disposition.decided_by, ?
        FROM boundaries AS boundary
        JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        WHERE boundary.id = ?
        """,
        (
            wait["boundary_id"],
            successor_attempt_id or wait["attempt_id"],
            successor_generation or wait["generation"],
            message_id,
            utc_now(),
            wait["boundary_id"],
        ),
    )


def _insert_message_tx(
    connection: sqlite3.Connection,
    system: dict[str, Any],
    *,
    attempt_id: str,
    suffix: str,
    kind: str,
    recipient_id: str | None = None,
    created_at: str | None = None,
) -> str:
    attempt = connection.execute(
        """
        SELECT attempt.*, work.supervisor_id
        FROM attempts AS attempt
        JOIN work_items AS work ON work.id = attempt.work_item_id
        WHERE attempt.id = ?
        """,
        (attempt_id,),
    ).fetchone()
    assert attempt is not None
    message_id = f"msg_wait_{suffix}"
    now = created_at or utc_now()
    connection.execute(
        """
        INSERT INTO messages(
            id, work_item_id, attempt_id, sender_id, kind,
            payload_json, payload_digest, message_digest,
            goal_version, goal_packet_digest, task_packet_digest, created_at
        ) VALUES(?, ?, ?, ?, ?, '{}', ?, ?, ?, ?, ?, ?)
        """,
        (
            message_id,
            attempt["work_item_id"],
            attempt_id,
            attempt["supervisor_id"],
            kind,
            f"payload-{suffix}",
            f"message-{suffix}",
            attempt["goal_version"],
            attempt["goal_packet_digest"],
            attempt["task_packet_digest"],
            now,
        ),
    )
    connection.execute(
        """
        INSERT INTO message_deliveries(
            message_id, recipient_id, state, next_attempt_at, created_at, updated_at
        ) VALUES(?, ?, 'queued', ?, ?, ?)
        """,
        (message_id, recipient_id or attempt["worker_id"], now, now, now),
    )
    return message_id


def _consume_same_attempt(
    system: dict[str, Any],
    work_id: str,
    wait: dict[str, Any],
    *,
    message_created_at: str | None = None,
) -> None:
    service = system["service"]
    with service.db.transaction() as connection:
        message_id = _insert_message_tx(
            connection,
            system,
            attempt_id=wait["attempt_id"],
            suffix=f"consume_{wait['boundary_id']}",
            kind="instruction",
            created_at=message_created_at,
        )
        connection.execute(
            """
            INSERT INTO boundary_continuations(
                boundary_id, work_item_id, source_attempt_id, source_generation,
                successor_attempt_id, successor_generation, message_id,
                decided_by, created_at
            )
            SELECT boundary.id, boundary.work_item_id, boundary.attempt_id,
                   boundary.generation, boundary.attempt_id, boundary.generation,
                   ?, disposition.decided_by, ?
            FROM boundaries AS boundary
            JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
            WHERE boundary.id = ?
            """,
            (message_id, utc_now(), wait["boundary_id"]),
        )
        connection.execute(
            "UPDATE attempts SET state = 'working', updated_at = ? WHERE id = ?",
            (utc_now(), wait["attempt_id"]),
        )
        connection.execute(
            """
            UPDATE work_items
            SET state = 'active', attention_owner = 'worker',
                user_needed_boundary_id = NULL, updated_at = ?
            WHERE id = ?
            """,
            (utc_now(), work_id),
        )


def _drop_continuation_guards(connection: sqlite3.Connection) -> None:
    for trigger in _CONTINUATION_TRIGGERS:
        connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")


def _set_schema_version_35(connection: sqlite3.Connection) -> None:
    connection.execute("UPDATE metadata SET value = '35' WHERE key = 'schema_version'")
    connection.execute("DELETE FROM schema_migrations WHERE version = 36")
    connection.execute("PRAGMA user_version = 35")


def _remove_wait_user_schema(
    system: dict[str, Any],
    *,
    schema_version: int,
) -> None:
    with system["service"].db.transaction() as connection:
        _drop_continuation_guards(connection)
        # A pre-pointer fixture cannot retain later pause guards that reference
        # that pointer. initialize() must install them again after migration.
        for name in _SUPERVISION_PAUSE_TRIGGERS:
            connection.execute(f"DROP TRIGGER IF EXISTS {name}")
        connection.execute("DROP TABLE boundary_continuations")
        connection.execute("DROP INDEX IF EXISTS work_items_user_needed_boundary_idx")
        connection.execute("ALTER TABLE work_items DROP COLUMN user_needed_boundary_id")
        if schema_version == 35:
            _set_schema_version_35(connection)


def _payload_snapshot(connection: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    rows: list[tuple[Any, ...]] = []
    for table, columns in (
        ("work_items", ("metadata_json",)),
        ("goal_revisions", ("packet_json", "packet_digest", "metadata_json")),
        (
            "attempts",
            ("goal_packet_digest", "task_packet_digest", "completion_claim_json"),
        ),
        ("boundaries", ("summary", "metadata_json", "input_digest")),
        (
            "boundary_dispositions",
            ("reason", "instruction", "resume_condition", "request_digest"),
        ),
    ):
        expressions = ", ".join(f"hex(CAST({column} AS BLOB))" for column in columns)
        rows.extend(
            tuple(row)
            for row in connection.execute(
                f"SELECT '{table}', rowid, {expressions} FROM {table} ORDER BY rowid"
            )
        )
    return tuple(rows)


def test_fresh_schema_enforces_exact_append_only_repeated_wait_user(
    system: dict[str, Any],
) -> None:
    work = _create_work(system, "fresh")
    first = _seed_wait_user(system, work, "fresh_one")

    with system["service"].db.connection_scope() as connection:
        message_column = next(
            row
            for row in connection.execute("PRAGMA table_info(boundary_continuations)")
            if row["name"] == "message_id"
        )
        assert message_column["notnull"] == 1
        trigger_names = {
            str(row["name"])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger' AND name LIKE '%user_needed%'
                   OR type = 'trigger' AND name LIKE 'boundary_continuation_%'
                """
            )
        }
        assert set(_CONTINUATION_TRIGGERS) <= trigger_names

    with system["service"].db.transaction() as connection:
        wrong_message_id = _insert_message_tx(
            connection,
            system,
            attempt_id=first["attempt_id"],
            suffix="fresh_wrong_generation",
            kind="instruction",
        )

    with pytest.raises(sqlite3.IntegrityError, match="exact binding violation"):
        _insert_continuation(
            system,
            first,
            successor_generation=first["generation"] + 1,
            message_id=wrong_message_id,
        )

    with system["service"].db.transaction() as connection:
        wrong_kind_message_id = _insert_message_tx(
            connection,
            system,
            attempt_id=first["attempt_id"],
            suffix="fresh_wrong_kind",
            kind="assignment",
        )
    with pytest.raises(sqlite3.IntegrityError, match="exact binding violation"):
        _insert_continuation(system, first, message_id=wrong_kind_message_id)

    with system["service"].db.transaction() as connection:
        wrong_recipient_message_id = _insert_message_tx(
            connection,
            system,
            attempt_id=first["attempt_id"],
            suffix="fresh_wrong_recipient",
            kind="instruction",
            recipient_id=str(system["user"]["id"]),
        )
    with pytest.raises(sqlite3.IntegrityError, match="exact binding violation"):
        _insert_continuation(system, first, message_id=wrong_recipient_message_id)

    _consume_same_attempt(system, str(work["id"]), first)
    continuation = system["service"].db.fetchone(
        "SELECT message_id FROM boundary_continuations WHERE boundary_id = ?",
        (first["boundary_id"],),
    )
    assert continuation is not None
    continuation_message_id = str(continuation["message_id"])
    # Dispatcher-owned delivery state stays mutable; the evidence identity and
    # its exact one-recipient set do not.
    system["service"].db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?",
        (continuation_message_id,),
    )
    with pytest.raises(sqlite3.IntegrityError, match="message is immutable"):
        system["service"].db.execute(
            "UPDATE messages SET created_at = created_at WHERE id = ?",
            (continuation_message_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="delivery set is immutable"):
        system["service"].db.execute(
            """
            INSERT INTO message_deliveries(
                message_id, recipient_id, state, next_attempt_at,
                created_at, updated_at
            ) VALUES(?, ?, 'queued', ?, ?, ?)
            """,
            (
                continuation_message_id,
                system["user"]["id"],
                utc_now(),
                utc_now(),
                utc_now(),
            ),
        )
    with pytest.raises(sqlite3.IntegrityError, match="delivery identity is immutable"):
        system["service"].db.execute(
            "UPDATE message_deliveries SET recipient_id = ? WHERE message_id = ?",
            (system["user"]["id"], continuation_message_id),
        )
    with pytest.raises(sqlite3.IntegrityError, match="delivery is immutable"):
        system["service"].db.execute(
            "DELETE FROM message_deliveries WHERE message_id = ?",
            (continuation_message_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        system["service"].db.execute(
            "UPDATE boundary_continuations SET created_at = ? WHERE boundary_id = ?",
            (utc_now(), first["boundary_id"]),
        )
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        system["service"].db.execute(
            "DELETE FROM boundary_continuations WHERE boundary_id = ?",
            (first["boundary_id"],),
        )

    refreshed = system["service"].get_work(str(work["id"]))
    second = _seed_wait_user(system, refreshed, "fresh_two")
    assert second["attempt_id"] == first["attempt_id"]
    assert second["generation"] == first["generation"]
    _consume_same_attempt(system, str(work["id"]), second)

    rows = system["service"].db.fetchall(
        """
        SELECT boundary_id, source_attempt_id, source_generation,
               successor_attempt_id, successor_generation
        FROM boundary_continuations WHERE work_item_id = ? ORDER BY created_at, boundary_id
        """,
        (work["id"],),
    )
    assert {str(row["boundary_id"]) for row in rows} == {
        first["boundary_id"],
        second["boundary_id"],
    }
    assert all(row["source_attempt_id"] == row["successor_attempt_id"] for row in rows)
    assert all(row["source_generation"] == row["successor_generation"] for row in rows)
    current = system["service"].db.fetchone(
        "SELECT state, attention_owner, user_needed_boundary_id FROM work_items WHERE id = ?",
        (work["id"],),
    )
    assert current is not None
    assert tuple(current) == ("active", "worker", None)


def test_successor_generation_and_required_assignment_message_are_exact(
    system: dict[str, Any],
) -> None:
    work = _create_work(system, "successor")
    wait = _seed_wait_user(system, work, "successor")
    service = system["service"]
    source = service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (wait["attempt_id"],))
    assert source is not None
    successor_id = "att_wait_successor_2"
    message_id = "msg_wait_successor_2"
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'canceled', updated_at = ? WHERE id = ?",
            (now, source["id"]),
        )
        connection.execute(
            """
            INSERT INTO attempts(
                id, work_item_id, attempt_number, worker_id,
                runtime_session_id, goal_version, goal_packet_digest,
                task_packet_digest, state, trajectory, evidence_confidence,
                completion_claim_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'assigned', 'untracked', 'unknown',
                     '{}', ?, ?)
            """,
            (
                successor_id,
                source["work_item_id"],
                int(source["attempt_number"]) + 1,
                source["worker_id"],
                source["runtime_session_id"],
                source["goal_version"],
                source["goal_packet_digest"],
                f"successor-{source['task_packet_digest']}",
                now,
                now,
            ),
        )
        successor = connection.execute(
            "SELECT * FROM attempts WHERE id = ?", (successor_id,)
        ).fetchone()
        assert successor is not None
        connection.execute(
            """
            INSERT INTO messages(
                id, work_item_id, attempt_id, sender_id, kind,
                payload_json, payload_digest, message_digest,
                goal_version, goal_packet_digest, task_packet_digest, created_at
            ) VALUES(?, ?, ?, ?, 'assignment', '{}', ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                work["id"],
                successor_id,
                system["cao"]["id"],
                "payload-successor",
                "message-successor",
                successor["goal_version"],
                successor["goal_packet_digest"],
                successor["task_packet_digest"],
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO message_deliveries(
                message_id, recipient_id, state, next_attempt_at,
                created_at, updated_at
            ) VALUES(?, ?, 'queued', ?, ?, ?)
            """,
            (message_id, successor["worker_id"], now, now, now),
        )
        connection.execute(
            """
            INSERT INTO boundary_continuations(
                boundary_id, work_item_id, source_attempt_id, source_generation,
                successor_attempt_id, successor_generation, message_id,
                decided_by, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wait["boundary_id"],
                work["id"],
                source["id"],
                wait["generation"],
                successor_id,
                wait["generation"] + 1,
                message_id,
                system["cao"]["id"],
                now,
            ),
        )
        connection.execute(
            """
            UPDATE work_items
            SET state = 'active', attention_owner = 'worker',
                generation = generation + 1, user_needed_boundary_id = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (now, work["id"]),
        )

    row = service.db.fetchone(
        "SELECT * FROM boundary_continuations WHERE boundary_id = ?",
        (wait["boundary_id"],),
    )
    assert row is not None
    assert row["successor_attempt_id"] == successor_id
    assert row["successor_generation"] == wait["generation"] + 1
    assert row["message_id"] == message_id


def test_nonterminal_clear_requires_consumption_but_terminal_exit_does_not(
    system: dict[str, Any],
) -> None:
    work = _create_work(system, "terminal_exit")
    wait = _seed_wait_user(system, work, "terminal_exit")

    with pytest.raises(sqlite3.IntegrityError, match="exact consumption or terminal exit"):
        system["service"].db.execute(
            """
            UPDATE work_items
            SET state = 'active', attention_owner = 'worker',
                user_needed_boundary_id = NULL
            WHERE id = ?
            """,
            (work["id"],),
        )

    system["service"].db.execute(
        """
        UPDATE work_items
        SET state = 'canceled', attention_owner = 'none',
            generation = generation + 1, user_needed_boundary_id = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (utc_now(), work["id"]),
    )
    current = system["service"].db.fetchone(
        "SELECT state, generation, user_needed_boundary_id FROM work_items WHERE id = ?",
        (work["id"],),
    )
    assert current is not None
    assert tuple(current) == ("canceled", wait["generation"] + 1, None)
    assert (
        system["service"].db.fetchone(
            "SELECT 1 FROM boundary_continuations WHERE boundary_id = ?",
            (wait["boundary_id"],),
        )
        is None
    )


def test_retention_keeps_continuation_proof_and_prunes_unrelated_message(
    system: dict[str, Any],
) -> None:
    work = _create_work(system, "retention")
    wait = _seed_wait_user(system, work, "retention")
    old = "2000-01-01T00:00:00Z"
    _consume_same_attempt(
        system,
        str(work["id"]),
        wait,
        message_created_at=old,
    )
    continuation = system["service"].db.fetchone(
        "SELECT message_id FROM boundary_continuations WHERE boundary_id = ?",
        (wait["boundary_id"],),
    )
    assert continuation is not None
    retained_message_id = str(continuation["message_id"])
    with system["service"].db.transaction() as connection:
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'handled', handled_at = ?, updated_at = ?
            WHERE message_id = ?
            """,
            (old, old, retained_message_id),
        )
        unrelated_message_id = _insert_message_tx(
            connection,
            system,
            attempt_id=wait["attempt_id"],
            suffix="retention_unrelated",
            kind="instruction",
            created_at=old,
        )
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'handled', handled_at = ?, updated_at = ?
            WHERE message_id = ?
            """,
            (old, old, unrelated_message_id),
        )

    pruned = system["service"].db.prune(event_days=36_500, message_days=1)

    assert pruned["messages"] == 1
    assert (
        system["service"].db.fetchone(
            "SELECT 1 FROM messages WHERE id = ?", (unrelated_message_id,)
        )
        is None
    )
    assert (
        system["service"].db.fetchone("SELECT 1 FROM messages WHERE id = ?", (retained_message_id,))
        is not None
    )
    assert (
        system["service"].db.fetchone(
            "SELECT 1 FROM message_deliveries WHERE message_id = ?",
            (retained_message_id,),
        )
        is not None
    )
    assert (
        system["service"].db.fetchone(
            "SELECT 1 FROM boundary_continuations WHERE boundary_id = ?",
            (wait["boundary_id"],),
        )
        is not None
    )


def test_v35_migration_backfills_only_one_exact_current_pointer_and_preserves_bytes(
    system: dict[str, Any],
) -> None:
    exact_work = _create_work(system, "migration_exact")
    ambiguous_work = _create_work(system, "migration_ambiguous")
    historical_work = _create_work(system, "migration_historical")
    _remove_wait_user_schema(system, schema_version=35)

    exact = _seed_wait_user(system, exact_work, "migration_exact", set_pointer=False)
    _seed_wait_user(system, ambiguous_work, "migration_ambiguous_one", set_pointer=False)
    _seed_wait_user(system, ambiguous_work, "migration_ambiguous_two", set_pointer=False)
    historical = _seed_wait_user(system, historical_work, "migration_historical", set_pointer=False)
    with system["service"].db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'working' WHERE id = ?",
            (historical["attempt_id"],),
        )
        connection.execute(
            "UPDATE work_items SET state = 'active', attention_owner = 'worker' WHERE id = ?",
            (historical_work["id"],),
        )
        before = _payload_snapshot(connection)

    system["service"].db.initialize()

    with system["service"].db.connection_scope() as connection:
        pointers = {
            str(row["id"]): row["user_needed_boundary_id"]
            for row in connection.execute(
                """
                SELECT id, user_needed_boundary_id FROM work_items
                WHERE id IN (?, ?, ?)
                """,
                (exact_work["id"], ambiguous_work["id"], historical_work["id"]),
            )
        }
        assert pointers[str(exact_work["id"])] == exact["boundary_id"]
        assert pointers[str(ambiguous_work["id"])] is None
        assert pointers[str(historical_work["id"])] is None
        assert connection.execute("SELECT COUNT(*) FROM boundary_continuations").fetchone()[0] == 0
        assert _payload_snapshot(connection) == before
        assert (
            int(
                connection.execute(
                    "SELECT value FROM metadata WHERE key = 'schema_version'"
                ).fetchone()[0]
            )
            == SCHEMA_VERSION
        )


def test_pre_pointer_schema36_backfills_once_and_leaves_ambiguity_null(
    system: dict[str, Any],
) -> None:
    exact_work = _create_work(system, "old36_exact")
    ambiguous_work = _create_work(system, "old36_ambiguous")
    _remove_wait_user_schema(system, schema_version=36)

    exact = _seed_wait_user(system, exact_work, "old36_exact", set_pointer=False)
    _seed_wait_user(system, ambiguous_work, "old36_ambiguous_one", set_pointer=False)
    _seed_wait_user(system, ambiguous_work, "old36_ambiguous_two", set_pointer=False)

    system["service"].db.initialize()
    with system["service"].db.connection_scope() as connection:
        pointers = {
            str(row["id"]): row["user_needed_boundary_id"]
            for row in connection.execute(
                """
                SELECT id, user_needed_boundary_id FROM work_items
                WHERE id IN (?, ?)
                """,
                (exact_work["id"], ambiguous_work["id"]),
            )
        }
        assert pointers[str(exact_work["id"])] == exact["boundary_id"]
        assert pointers[str(ambiguous_work["id"])] is None
        assert connection.execute("SELECT COUNT(*) FROM boundary_continuations").fetchone()[0] == 0

    # Once the column exists at schema36, NULL is a durable fail-closed
    # authority decision and a later exact-looking reopen must not claim it.
    with system["service"].db.transaction() as connection:
        connection.execute(
            "UPDATE boundary_dispositions SET kind = 'correct' WHERE boundary_id = ?",
            ("bnd_wait_old36_ambiguous_two",),
        )
    with pytest.raises(sqlite3.IntegrityError, match="ambiguous legacy WAIT_USER"):
        system["service"].db.execute(
            """
            UPDATE work_items SET user_needed_boundary_id = ? WHERE id = ?
            """,
            ("bnd_wait_old36_ambiguous_one", ambiguous_work["id"]),
        )
    system["service"].db.initialize()
    row = system["service"].db.fetchone(
        "SELECT user_needed_boundary_id FROM work_items WHERE id = ?",
        (ambiguous_work["id"],),
    )
    assert row is not None
    assert row["user_needed_boundary_id"] is None


def test_schema36_reopen_never_claims_a_null_current_pointer(
    system: dict[str, Any],
) -> None:
    work = _create_work(system, "reopen_null")
    with system["service"].db.transaction() as connection:
        _drop_continuation_guards(connection)
    wait = _seed_wait_user(system, work, "reopen_null", set_pointer=False)

    system["service"].db.initialize()
    row = system["service"].db.fetchone(
        "SELECT user_needed_boundary_id FROM work_items WHERE id = ?", (work["id"],)
    )
    assert row is not None
    assert row["user_needed_boundary_id"] is None
    assert (
        system["service"].db.fetchone(
            "SELECT 1 FROM boundary_continuations WHERE boundary_id = ?",
            (wait["boundary_id"],),
        )
        is None
    )


def test_partial_schema36_relation_fails_atomically(system: dict[str, Any]) -> None:
    with system["service"].db.transaction() as connection:
        _drop_continuation_guards(connection)
        connection.execute("DROP TABLE boundary_continuations")
        connection.execute("CREATE TABLE boundary_continuations(boundary_id TEXT PRIMARY KEY)")

    with pytest.raises(RuntimeError, match="partial or incompatible"):
        system["service"].db.initialize()

    with system["service"].db.connection_scope() as connection:
        columns = tuple(
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(boundary_continuations)")
        )
        assert columns == ("boundary_id",)
        assert (
            int(
                connection.execute(
                    "SELECT value FROM metadata WHERE key = 'schema_version'"
                ).fetchone()[0]
            )
            == SCHEMA_VERSION
        )


def test_conflicting_pointer_index_reopen_rolls_back(system: dict[str, Any]) -> None:
    with system["service"].db.transaction() as connection:
        connection.execute("DROP INDEX work_items_user_needed_boundary_idx")
        connection.execute("CREATE INDEX work_items_user_needed_boundary_idx ON work_items(id)")

    with pytest.raises(RuntimeError, match="index contract is incompatible"):
        system["service"].db.initialize()

    with system["service"].db.connection_scope() as connection:
        columns = tuple(
            str(row["name"])
            for row in connection.execute(
                'PRAGMA index_info("work_items_user_needed_boundary_idx")'
            )
        )
        assert columns == ("id",)


def test_conflicting_prepopulated_continuation_reopen_rolls_back(
    system: dict[str, Any],
) -> None:
    work = _create_work(system, "reopen_conflict")
    wait = _seed_wait_user(system, work, "reopen_conflict")
    with system["service"].db.transaction() as connection:
        message_id = _insert_message_tx(
            connection,
            system,
            attempt_id=wait["attempt_id"],
            suffix="reopen_conflict",
            kind="instruction",
        )
        _drop_continuation_guards(connection)
        connection.execute(
            """
            INSERT INTO boundary_continuations(
                boundary_id, work_item_id, source_attempt_id, source_generation,
                successor_attempt_id, successor_generation, message_id,
                decided_by, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wait["boundary_id"],
                work["id"],
                wait["attempt_id"],
                wait["generation"],
                wait["attempt_id"],
                wait["generation"] + 1,
                message_id,
                system["cao"]["id"],
                utc_now(),
            ),
        )

    with pytest.raises(
        RuntimeError,
        match=r"invalid (current WAIT_USER boundary|exact binding)",
    ):
        system["service"].db.initialize()

    row = system["service"].db.fetchone(
        "SELECT successor_generation FROM boundary_continuations WHERE boundary_id = ?",
        (wait["boundary_id"],),
    )
    assert row is not None
    assert row["successor_generation"] == wait["generation"] + 1
