from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

import cao_control_plane.database as database_module
from cao_control_plane.dashboard import build_operator_view
from cao_control_plane.database import _SUPERVISION_PAUSE_TRIGGERS, Database, utc_now
from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    MessageKind,
    ReportInput,
    WorkAssignment,
    WorkResumeInput,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime import Dispatcher


def _assign(system: dict[str, Any], suffix: str) -> dict[str, Any]:
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title=f"Pause storage {suffix}",
            objective="Preserve the exact explicit supervision decision.",
            acceptance=["Only an explicit resumption can consume the pause."],
            idempotency_key=f"pause-storage:{suffix}",
        ),
    )


@pytest.fixture
def paused(system: dict[str, Any]) -> dict[str, Any]:
    service = system["service"]
    work = _assign(system, "source")
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            summary="The supervisor must choose the next execution condition.",
            expected_goal_version=work["goal_version"],
            expected_generation=work["generation"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            idempotency_key="pause-storage:question",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="pause-storage:decision",
    )
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "INSERT INTO boundary_dispositions "
            "(id,boundary_id,reasoner_turn_id,decided_by,generation,kind,reason,resume_condition,request_digest,created_at) "
            "VALUES(?,?,?,?,?,'pause',?,?,?,?)",
            (
                f"disp_{uuid4().hex}",
                boundary["id"],
                turn["id"],
                system["cao"]["id"],
                work["generation"],
                "Execution is explicitly paused.",
                "A verified changed condition is available.",
                "a" * 64,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO work_pauses VALUES(?,?,?,?,?,?,?)",
            (
                boundary["id"],
                work["id"],
                attempt["id"],
                work["generation"],
                work["generation"] + 1,
                system["cao"]["id"],
                now,
            ),
        )
        connection.execute("UPDATE attempts SET state='suspended' WHERE id=?", (attempt["id"],))
        connection.execute(
            "UPDATE work_items SET state='suspended',attention_owner='none',"
            "generation=generation+1,paused_boundary_id=? WHERE id=?",
            (boundary["id"], work["id"]),
        )
        connection.execute(
            "UPDATE reasoner_turns SET state='completed',completed_at=? WHERE id=?",
            (now, turn["id"]),
        )
    return {
        "work_id": work["id"],
        "attempt_id": attempt["id"],
        "boundary_id": boundary["id"],
        "turn_id": turn["id"],
        "source_generation": work["generation"],
    }


def _resume(system: dict[str, Any], paused: dict[str, Any]) -> dict[str, Any]:
    return system["service"].resume_work(
        system["cao"],
        paused["work_id"],
        WorkResumeInput(
            expected_generation=paused["source_generation"] + 1,
            pause_boundary_id=paused["boundary_id"],
            reason="The next condition was verified.",
            instruction="Apply the newly verified execution condition.",
            resume_evidence="The independent prerequisite check succeeded.",
            idempotency_key="pause-storage:resume",
        ),
    )


def test_pause_schema_is_typed_without_automatic_decision_fields(system) -> None:
    database = system["service"].db
    assert database.fetchone("PRAGMA user_version")[0] == 45
    columns = {row["name"] for row in database.fetchall("PRAGMA table_info(boundary_dispositions)")}
    assert not columns.intersection(
        {"requested_kind", "continuation_fingerprint", "unchanged_count"}
    )
    pause_columns = {row["name"] for row in database.fetchall("PRAGMA table_info(work_pauses)")}
    assert pause_columns == {
        "boundary_id",
        "work_item_id",
        "attempt_id",
        "source_generation",
        "pause_generation",
        "paused_by",
        "created_at",
    }
    assert database.fetchone("PRAGMA foreign_key_check") is None


def test_pause_generation_is_distinct_from_its_source_boundary(system, paused) -> None:
    database = system["service"].db
    pause = database.fetchone(
        "SELECT * FROM work_pauses WHERE boundary_id=?", (paused["boundary_id"],)
    )
    boundary = database.fetchone(
        "SELECT generation FROM boundaries WHERE id=?", (paused["boundary_id"],)
    )
    work = database.fetchone("SELECT generation FROM work_items WHERE id=?", (paused["work_id"],))
    assert pause["source_generation"] == boundary["generation"]
    assert pause["pause_generation"] == pause["source_generation"] + 1 == work["generation"]


@pytest.mark.parametrize(
    "change",
    [
        "paused_boundary_id=NULL",
        "state='active',attention_owner='worker'",
        "attention_owner='user'",
        "generation=generation+1",
        "suspended_by_work_item_id=id",
        "user_needed_boundary_id=paused_boundary_id",
        "goal_version=goal_version+1",
    ],
)
def test_current_pause_pointer_rejects_incoherent_work_updates(system, paused, change) -> None:
    database = system["service"].db
    before = database.commit_generation()
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(f"UPDATE work_items SET {change} WHERE id=?", (paused["work_id"],))
    assert database.commit_generation() == before
    assert (
        database.fetchone(
            "SELECT paused_boundary_id FROM work_items WHERE id=?", (paused["work_id"],)
        )[0]
        == paused["boundary_id"]
    )


@pytest.mark.parametrize(
    "table,key,field",
    [
        ("work_pauses", "boundary_id", "pause_generation"),
        ("boundary_dispositions", "boundary_id", "reason"),
        ("boundaries", "id", "generation"),
        ("attempts", "id", "task_packet_digest"),
        ("reasoner_turns", "id", "generation"),
    ],
)
def test_pause_history_and_provenance_are_immutable(system, paused, table, key, field) -> None:
    database = system["service"].db
    identity = (
        paused["attempt_id"]
        if table == "attempts"
        else paused["turn_id"]
        if table == "reasoner_turns"
        else paused["boundary_id"]
    )
    before = database.commit_generation()
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(f"UPDATE {table} SET {field}={field} WHERE {key}=?", (identity,))
    assert database.commit_generation() == before


@pytest.mark.parametrize("table", ["work_pauses", "boundary_dispositions"])
def test_pause_history_cannot_be_deleted(system, paused, table) -> None:
    database = system["service"].db
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(f"DELETE FROM {table} WHERE boundary_id=?", (paused["boundary_id"],))


def test_terminal_exit_clears_only_current_pointer_and_preserves_history(system, paused) -> None:
    database = system["service"].db
    with database.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state='canceled' WHERE id=?", (paused["attempt_id"],)
        )
        connection.execute(
            "UPDATE work_items SET state='canceled',attention_owner='none',generation=generation+1,"
            "paused_boundary_id=NULL WHERE id=?",
            (paused["work_id"],),
        )
    assert database.fetchone("SELECT COUNT(*) FROM work_pauses")[0] == 1
    assert database.fetchone("SELECT COUNT(*) FROM work_pause_resumptions")[0] == 0
    projection = build_projection(database)
    row = next(row for row in projection.snapshot["work_items"] if row["id"] == paused["work_id"])
    assert row["supervision_pause"] is None
    assert not [violation for violation in projection.violations if "pause" in violation.code]


def test_partial_pointer_clear_is_visible_as_unconsumed_authority(system, paused) -> None:
    database = system["service"].db
    # The service performs this intermediate write only within its atomic
    # successor transaction; a malformed partial commit cannot appear healthy.
    database.execute(
        "UPDATE work_items SET state='active',attention_owner='worker',generation=generation+1,"
        "paused_boundary_id=NULL WHERE id=?",
        (paused["work_id"],),
    )
    codes = {item.code for item in build_projection(database).violations}
    assert "work_pause.unconsumed_not_current" in codes


def test_wrong_pause_record_owner_is_reported_without_copying_record_text(system, paused) -> None:
    database = system["service"].db
    other = _assign(system, "unrelated")
    database.execute("DROP TRIGGER work_pauses_immutable_update")
    database.execute(
        "UPDATE work_pauses SET work_item_id=? WHERE boundary_id=?",
        (other["id"], paused["boundary_id"]),
    )
    projection = build_projection(database)
    codes = {item.code for item in projection.violations}
    assert {"work_pause.binding_invalid", "work.supervision_pause_binding_invalid"} <= codes
    row = next(row for row in projection.snapshot["work_items"] if row["id"] == paused["work_id"])
    assert row["supervision_pause"] is None


def test_pause_projection_is_deterministic_and_dashboard_is_locator_free(system, paused) -> None:
    database = system["service"].db
    before = database.commit_generation()
    comparison_time = utc_now()
    first = build_projection(database, as_of=comparison_time)
    second = build_projection(database, as_of=comparison_time)
    row = next(row for row in first.snapshot["work_items"] if row["id"] == paused["work_id"])
    assert set(row["supervision_pause"]) == {
        "boundary_id",
        "source_generation",
        "pause_generation",
        "reason",
        "resume_condition",
        "paused_at",
    }
    assert (
        row["supervision_pause"]
        == system["service"].get_work(paused["work_id"])["supervision_pause"]
    )
    assert first.canonical_digest == second.canonical_digest
    assert not [item for item in first.violations if "pause" in item.code]
    malicious = {
        **row,
        "operator_scope": "production",
        "supervision_pause": {
            **row["supervision_pause"],
            "reason": "Pause until /private/untrusted is checked.",
            "resume_condition": "Verify https://private.example.test/condition first.",
            "resume_evidence": "PROVIDER_TEXT_MUST_NOT_APPEAR",
            "instruction": "UNTRUSTED_INSTRUCTION",
        },
    }
    operator = build_operator_view(
        {
            "work_items": [malicious],
            "operator_workers": [
                {
                    "principal_id": row["assigned_worker_id"],
                    "operator_scope": "production",
                    "operator_label": "Pause storage worker",
                    "principal_enabled": True,
                    "worker_state": "enabled",
                    "runner_availability": "available",
                    "runner_connection_state": "connected-idle",
                }
            ],
        }
    )
    view = operator["work_items"][0]["supervision_pause"]
    assert set(view) == {
        "source_generation",
        "pause_generation",
        "reason",
        "resume_condition",
        "paused_at",
    }
    rendered = json.dumps(operator)
    for private in (
        paused["boundary_id"],
        "/private/untrusted",
        "private.example.test",
        "PROVIDER_TEXT_MUST_NOT_APPEAR",
        "UNTRUSTED_INSTRUCTION",
    ):
        assert private not in rendered
    assert database.commit_generation() == before


def test_resumption_history_is_unique_immutable_and_bound_to_new_assignment(system, paused) -> None:
    resumed = _resume(system, paused)
    database = system["service"].db
    record = dict(
        database.fetchone(
            "SELECT * FROM work_pause_resumptions WHERE boundary_id=?", (paused["boundary_id"],)
        )
    )
    assert record["previous_attempt_id"] == paused["attempt_id"]
    assert record["successor_attempt_id"] == resumed["current_attempt"]["id"]
    assert record["successor_attempt_id"] != record["previous_attempt_id"]
    assert record["expected_generation"] == paused["source_generation"] + 1
    assert record["successor_generation"] == record["expected_generation"] + 1
    assert resumed["supervision_pause"] is None
    assert (
        database.fetchone("SELECT kind FROM messages WHERE id=?", (record["message_id"],))[0]
        == "assignment"
    )
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(
            "UPDATE work_pause_resumptions SET reason=reason WHERE boundary_id=?",
            (paused["boundary_id"],),
        )
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(
            "DELETE FROM work_pause_resumptions WHERE boundary_id=?", (paused["boundary_id"],)
        )
    with pytest.raises(sqlite3.IntegrityError):
        database.execute(
            "UPDATE messages SET kind='instruction' WHERE id=?", (record["message_id"],)
        )
    assert not [item for item in build_projection(database).violations if "pause" in item.code]


def test_resumption_reference_survives_normal_message_pruning(system, paused, monkeypatch) -> None:
    resumed = _resume(system, paused)
    service = system["service"]
    record = service.db.fetchone(
        "SELECT message_id FROM work_pause_resumptions WHERE boundary_id=?",
        (paused["boundary_id"],),
    )
    with service.db.transaction() as connection:
        unrelated = service._message(
            connection,
            sender_id=system["cao"]["id"],
            recipient_id=system["worker"]["id"],
            kind=MessageKind.SYSTEM,
            work_item_id=paused["work_id"],
            attempt_id=resumed["current_attempt"]["id"],
            goal_version=resumed["goal_version"],
            payload={"summary": "An unrelated retained-history candidate."},
            idempotency_key="pause-storage:unrelated-history",
        )
        connection.execute(
            "UPDATE message_deliveries SET state='handled',handled_at=? WHERE message_id IN (?,?)",
            (utc_now(), record["message_id"], unrelated["id"]),
        )

    class FutureClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2100, 1, 1, tzinfo=UTC)

    monkeypatch.setattr(database_module, "datetime", FutureClock)
    result = service.db.prune(event_days=1, message_days=1)
    assert result["messages"] >= 1
    assert service.db.fetchone("SELECT id FROM messages WHERE id=?", (unrelated["id"],)) is None
    assert (
        service.db.fetchone("SELECT id FROM messages WHERE id=?", (record["message_id"],))
        is not None
    )
    assert service.db.fetchone("SELECT COUNT(*) FROM work_pause_resumptions")[0] == 1
    assert service.db.fetchone("PRAGMA foreign_key_check") is None


def test_wrong_resumption_record_owner_is_visible_as_fixed_violation(system, paused) -> None:
    _resume(system, paused)
    database = system["service"].db
    other = _assign(system, "resumption-unrelated")
    database.execute("DROP TRIGGER work_pause_resumptions_immutable_update")
    database.execute(
        "UPDATE work_pause_resumptions SET work_item_id=? WHERE boundary_id=?",
        (other["id"], paused["boundary_id"]),
    )
    codes = {item.code for item in build_projection(database).violations}
    assert "work_pause_resumption.binding_invalid" in codes


def test_reopen_preserves_explicit_pause_without_inventing_resumption(system, paused) -> None:
    database = system["service"].db
    before = dict(database.fetchone("SELECT * FROM work_items WHERE id=?", (paused["work_id"],)))
    reopened = Database(system["settings"])
    reopened.initialize()
    assert (
        dict(reopened.fetchone("SELECT * FROM work_items WHERE id=?", (paused["work_id"],)))
        == before
    )
    assert reopened.fetchone("SELECT COUNT(*) FROM work_pauses")[0] == 1
    assert reopened.fetchone("SELECT COUNT(*) FROM work_pause_resumptions")[0] == 0


def test_schema43_migration_keeps_interruption_distinct_and_reopens_idempotently(system) -> None:
    database = system["service"].db
    first = _assign(system, "interrupted")
    second = _assign(system, "interrupting")
    with database.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state='suspended' WHERE id=?", (first["current_attempt"]["id"],)
        )
        connection.execute(
            "UPDATE work_items SET state='suspended',attention_owner='none',suspended_by_work_item_id=? WHERE id=?",
            (second["id"], first["id"]),
        )
        for trigger in _SUPERVISION_PAUSE_TRIGGERS:
            connection.execute(f"DROP TRIGGER {trigger}")
        connection.execute("DROP TABLE work_pause_resumptions")
        connection.execute("DROP TABLE work_pauses")
        connection.execute("DROP INDEX work_items_paused_boundary_idx")
        connection.execute("ALTER TABLE work_items DROP COLUMN paused_boundary_id")
        connection.execute("UPDATE metadata SET value='43' WHERE key='schema_version'")
        connection.execute("DELETE FROM schema_migrations WHERE version=44")
        connection.execute("PRAGMA user_version=43")
    reopened = Database(system["settings"])
    reopened.initialize()
    once = dict(reopened.fetchone("SELECT * FROM work_items WHERE id=?", (first["id"],)))
    reopened.initialize()
    twice = dict(reopened.fetchone("SELECT * FROM work_items WHERE id=?", (first["id"],)))
    assert once == twice
    assert once["state"] == "suspended" and once["attention_owner"] == "none"
    assert once["suspended_by_work_item_id"] == second["id"]
    assert once["paused_boundary_id"] is None
    assert reopened.fetchone("SELECT COUNT(*) FROM work_pauses")[0] == 0
    assert reopened.fetchone("SELECT COUNT(*) FROM work_pause_resumptions")[0] == 0
    assert reopened.fetchone("SELECT COUNT(*) FROM schema_migrations WHERE version=44")[0] == 1
    assert reopened.fetchone("PRAGMA foreign_key_check") is None


def test_migration_rejects_partial_pause_ledger_without_guessing_authority(system) -> None:
    database = system["service"].db
    with database.transaction() as connection:
        for trigger in _SUPERVISION_PAUSE_TRIGGERS:
            connection.execute(f"DROP TRIGGER {trigger}")
        connection.execute("DROP TABLE work_pause_resumptions")
        connection.execute("DROP TABLE work_pauses")
        connection.execute("CREATE TABLE work_pauses(boundary_id TEXT PRIMARY KEY)")
        connection.execute("UPDATE metadata SET value='43' WHERE key='schema_version'")
        connection.execute("PRAGMA user_version=43")
    with pytest.raises(RuntimeError, match="pause ledger schema is incompatible"):
        Database(system["settings"]).initialize()
    assert database.fetchone("SELECT value FROM metadata WHERE key='schema_version'")[0] == "43"
    assert {row["name"] for row in database.fetchall("PRAGMA table_info(work_pauses)")} == {
        "boundary_id"
    }


@pytest.mark.parametrize("delivery_state", ["delivered", "acknowledged"])
def test_pause_waits_for_worker_handling_without_stranding_the_shared_lane(
    system, delivery_state
) -> None:
    service = system["service"]
    work = _assign(system, "delivery-processing")
    attempt = work["current_attempt"]
    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id=? AND kind='assignment'", (attempt["id"],)
    )
    service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    service.mark_message_handled(
        system["worker"], assignment["id"], evidence="The initial assignment was incorporated."
    )
    # Reports settle their Assignment automatically, but do not fabricate
    # incorporation of a distinct instruction omitted from the report.
    with service.db.transaction() as connection:
        instruction = service._message(
            connection,
            sender_id=system["cao"]["id"],
            recipient_id=system["worker"]["id"],
            kind=MessageKind.INSTRUCTION,
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            goal_version=work["goal_version"],
            payload={
                "action": "correct",
                "instruction": "Check the newly admitted prerequisite.",
                "generation": work["generation"],
            },
            idempotency_key="pause-storage:unincorporated-instruction",
        )
    service.db.execute(
        "UPDATE message_deliveries SET state='delivered' WHERE message_id=?",
        (instruction["id"],),
    )
    if delivery_state == "acknowledged":
        service.acknowledge(system["worker"], AckInput(message_ids=[instruction["id"]]))
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            summary="The supervisor must choose the next execution condition.",
            expected_goal_version=work["goal_version"],
            expected_generation=work["generation"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            idempotency_key="pause-storage:delivery-question",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="pause-storage:delivery-decision",
    )
    request = BoundaryDispositionInput(
        turn_id=turn["id"],
        lease_token=turn["lease_token"],
        expected_generation=work["generation"],
        kind="pause",
        reason="Execution is explicitly paused.",
        resume_condition="A verified changed condition is available.",
    )
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id=?", (instruction["id"],)
        )[0]
        == delivery_state
    )
    before = service.db.commit_generation()
    with pytest.raises(ConflictError, match="settled execution boundary"):
        service.dispose_boundary(system["cao"], boundary["id"], request)
    assert service.db.commit_generation() == before
    assert service.db.fetchone("SELECT COUNT(*) FROM work_pauses")[0] == 0
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id=?", (instruction["id"],)
        )[0]
        == delivery_state
    )

    service.acknowledge(system["worker"], AckInput(message_ids=[instruction["id"]]))
    service.mark_message_handled(
        system["worker"], instruction["id"], evidence="The exact instruction was incorporated."
    )
    service.dispose_boundary(system["cao"], boundary["id"], request)
    independent = _assign(system, "delivery-independent")
    claimed = Dispatcher(service, system["settings"])._claim_delivery()
    assert claimed is not None
    assert (
        service.db.fetchone(
            "SELECT work_item_id FROM messages WHERE id=?", (claimed["message_id"],)
        )[0]
        == independent["id"]
    )
