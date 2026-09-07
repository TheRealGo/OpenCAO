from __future__ import annotations

from dataclasses import replace

import pytest

from cao_control_plane.database import SCHEMA_VERSION, Database
from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    BoundaryInput,
    GoalRevision,
    IntentDisposition,
    MessageKind,
    SubmittedIntent,
    WorkAssignment,
)
from cao_control_plane.projection import verify_projection
from cao_control_plane.service import ControlPlane


def _assignment(system, title: str) -> WorkAssignment:
    return WorkAssignment(
        worker_id=system["worker"]["id"],
        title=title,
        objective=f"Complete {title}",
        acceptance=[f"{title} is verified"],
        non_goals=["Do not publish externally"],
    )


def _submit(system, source_id: str, text: str) -> dict:
    return system["service"]._receive_canonical_intent_for_compat(
        system["cao"],
        SubmittedIntent(
            source_id=source_id,
            payload={"type": "canonical_cao_command", "title": text},
        ),
    )


def _create_task(
    system,
    source_id: str,
    title: str,
    *,
    relation: str = "independent",
    target_work_item_id: str | None = None,
) -> dict:
    receipt = _submit(system, source_id, title)
    return system["service"]._classify_canonical_intent_for_compat(
        system["cao"],
        receipt["intent"]["id"],
        IntentDisposition(
            kind="task",
            relation=relation,
            target_work_item_id=target_work_item_id,
            assignment=_assignment(system, title),
            reason="Classified from a submitted request",
        ),
    )["work"]


def test_submitted_source_identity_and_disposition_are_exactly_once(system):
    service = system["service"]
    first = _submit(system, "client:turn-1", "Build the report")
    replay = _submit(system, "client:turn-1", "Build the report")
    assert replay == first
    assert first["supervision_message_id"]
    assert first["supervisor_delivery_count"] == 1
    queued = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (first["supervision_message_id"], system["cao"]["id"]),
    )
    assert queued is not None
    assert queued["state"] == "queued"

    disposition = IntentDisposition(
        kind="task",
        relation="independent",
        assignment=_assignment(system, "Report"),
        reason="A new objective",
    )
    first_result = service._classify_canonical_intent_for_compat(
        system["cao"], first["intent"]["id"], disposition
    )
    replay_result = service._classify_canonical_intent_for_compat(
        system["cao"], first["intent"]["id"], disposition
    )
    assert replay_result == first_result
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM source_receipts")["count"] == 1
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM submitted_intents")["count"] == 1
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM intent_dispositions")["count"] == 1
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM work_items")["count"] == 1
    handled = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (first["supervision_message_id"], system["cao"]["id"]),
    )
    assert handled is not None
    assert handled["state"] == "handled"

    with pytest.raises(ConflictError):
        _submit(system, "client:turn-1", "A different payload")


def test_legacy_assignment_facade_cannot_bypass_durable_ingress(system):
    assignment = _assignment(system, "Facade task").model_copy(
        update={"idempotency_key": "facade-task-1"}
    )

    first = system["service"].assign_work(system["cao"], assignment)
    replay = system["service"].assign_work(system["cao"], assignment)

    assert replay == first
    receipt = system["service"].db.fetchone(
        "SELECT * FROM source_receipts WHERE source_id = ?",
        ("assign_work:facade-task-1",),
    )
    assert receipt is not None
    intent = system["service"].db.fetchone(
        "SELECT * FROM submitted_intents WHERE source_receipt_id = ?",
        (receipt["id"],),
    )
    assert intent is not None
    directive = system["service"].db.fetchone(
        "SELECT * FROM directives WHERE submitted_intent_id = ?",
        (intent["id"],),
    )
    disposition = system["service"].db.fetchone(
        "SELECT * FROM intent_dispositions WHERE submitted_intent_id = ?",
        (intent["id"],),
    )
    assert directive is not None
    assert directive["relation"] == "independent"
    assert directive["created_work_item_id"] == first["id"]
    assert disposition is not None
    assert disposition["result_work_item_id"] == first["id"]
    assert system["service"].db.fetchone(
        "SELECT COUNT(*) AS count FROM work_items"
    )["count"] == 1


def test_augment_creates_a_directive_without_rewriting_the_goal(system):
    service = system["service"]
    work = _create_task(system, "client:base", "Base task")
    receipt = _submit(system, "client:augment", "Also include edge cases")
    result = service._classify_canonical_intent_for_compat(
        system["cao"],
        receipt["intent"]["id"],
        IntentDisposition(
            kind="directive",
            relation="augment",
            target_work_item_id=work["id"],
            directive="Include edge-case evidence",
            reason="Adds a constraint without replacing the objective",
        ),
    )

    current = service.get_work(work["id"])
    assert result["directive"]["target_work_item_id"] == work["id"]
    assert result["directive"]["state"] == "pending"
    assert result["boundary"]["kind"] == "directive"
    assert current["goal_version"] == 1
    assert len(current["goal_revisions"]) == 1

    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        expected_generation=work["generation"],
        idempotency_key="augment-turn",
    )
    disposed = service.dispose_boundary(
        system["cao"],
        result["boundary"]["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=work["generation"],
            kind="continue",
            reason="The added requirement is in scope",
        ),
    )
    assert disposed["kind"] == "continue"
    handled = service.db.fetchone(
        "SELECT * FROM directives WHERE id = ?", (result["directive"]["id"],)
    )
    assert handled["state"] == "handled"
    assignment = service.get_inbox(system["worker"])["items"]
    assert [message["kind"] for message in assignment] == ["assignment"]
    service.acknowledge(
        system["worker"], AckInput(message_ids=[assignment[0]["id"]])
    )
    service.mark_message_handled(
        system["worker"],
        assignment[0]["id"],
        evidence="base assignment incorporated before its augment",
    )
    messages = service.get_inbox(system["worker"], include_acknowledged=True)["items"]
    assert any(
        message["payload"].get("directive_id") == result["directive"]["id"]
        and message["payload"].get("instruction") == "Include edge-case evidence"
        for message in messages
    )


def test_independent_interrupt_and_replace_have_distinct_effects(system):
    service = system["service"]
    original = _create_task(system, "client:original", "Original", relation="independent")
    independent = _create_task(system, "client:parallel", "Parallel", relation="independent")
    assert service.get_work(original["id"])["state"] == "active"
    assert service.get_work(independent["id"])["state"] == "active"

    interrupt = _create_task(
        system,
        "client:interrupt",
        "Urgent",
        relation="interrupt",
        target_work_item_id=original["id"],
    )
    suspended = service.get_work(original["id"])
    assert suspended["state"] == "suspended"
    assert suspended["suspended_by_work_item_id"] == interrupt["id"]

    receipt = _submit(system, "client:replace", "Replacement")
    replacement = service._classify_canonical_intent_for_compat(
        system["cao"],
        receipt["intent"]["id"],
        IntentDisposition(
            kind="directive",
            relation="replace",
            target_work_item_id=independent["id"],
            assignment=_assignment(system, "Replacement"),
            directive="Replace the current objective with the successor goal",
            reason="The submitted request explicitly supersedes the goal",
        ),
    )
    replaced = service.get_work(independent["id"])
    assert replacement["work"]["id"] == independent["id"]
    assert replaced["state"] == "active"
    assert replaced["goal_version"] == 2
    assert len(replaced["goal_revisions"]) == 2


def test_nested_interrupts_resume_lifo_after_database_reopen(system):
    first = _create_task(system, "client:first", "First")
    second = _create_task(
        system,
        "client:second",
        "Second",
        relation="interrupt",
        target_work_item_id=first["id"],
    )
    third = _create_task(
        system,
        "client:third",
        "Third",
        relation="interrupt",
        target_work_item_id=second["id"],
    )

    reopened_settings = replace(system["settings"])
    reopened = ControlPlane(Database(reopened_settings), reopened_settings)
    reopened.cancel_work(system["cao"], third["id"], "Interrupt completed")
    assert reopened.get_work(second["id"])["state"] == "active"
    assert reopened.get_work(first["id"])["state"] == "suspended"

    reopened.cancel_work(system["cao"], second["id"], "Interrupt completed")
    assert reopened.get_work(first["id"])["state"] == "active"


def test_goal_revision_fences_a_stale_reasoner_turn(system):
    service = system["service"]
    work = _create_task(system, "client:goal", "Goal task")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:event-1",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=1,
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="The runtime is ready for the next instruction",
            runtime_state="ready",
        ),
    )
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="goal-turn",
    )
    revised = service.revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=1,
            objective="Complete the revised task",
            maturity="defined",
            acceptance=["The revised task is verified"],
            reason="A submitted requirement changed the goal",
        ),
    )
    assert revised["generation"] > work["generation"]
    supersession = service.db.fetchone(
        "SELECT * FROM boundary_supersessions WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert supersession is not None
    assert supersession["reason"] == "goal_replaced"
    replacement_event = service.db.fetchone(
        "SELECT event_type, aggregate_id FROM events WHERE sequence = ?",
        (supersession["superseding_event_sequence"],),
    )
    assert replacement_event is not None
    assert replacement_event["event_type"] == "work.goal_replaced"
    assert replacement_event["aggregate_id"] == work["id"]
    assert verify_projection(service.db).healthy is True

    with pytest.raises(ConflictError) as stale_disposition:
        service.dispose_boundary(
            system["cao"],
            boundary["id"],
            BoundaryDispositionInput(
                turn_id=turn["id"],
                lease_token=turn["lease_token"],
                expected_generation=work["generation"],
                kind="continue",
                reason="Continue stale work",
            ),
        )
    assert stale_disposition.value.details == {"reason": "goal_replaced"}
    successor_boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:event-2",
            work_item_id=work["id"],
            attempt_id=revised["current_attempt"]["id"],
            expected_goal_version=revised["goal_version"],
            expected_goal_packet_digest=revised["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=revised["current_attempt"]["task_packet_digest"],
            expected_generation=revised["generation"],
            kind="idle",
            summary="The revised runtime is ready",
            runtime_state="ready",
        ),
    )
    successor_turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=successor_boundary["id"],
        expected_generation=revised["generation"],
        idempotency_key="goal-turn-successor",
    )
    assert successor_turn["generation"] == revised["generation"]


def test_schema41_upgrade_repairs_a_schema40_goal_replacement_boundary(system):
    service = system["service"]
    work = _create_task(system, "client:goal-v40", "Schema 40 goal task")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:event-v40",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="The schema-40 runtime is ready for the next instruction",
            runtime_state="ready",
        ),
    )
    service.revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=work["goal_version"],
            objective="Complete the schema-41 revised task",
            maturity="defined",
            acceptance=["The schema-41 migration is verified"],
            reason="Reproduce the schema-40 partial replacement history",
        ),
    )
    original = service.db.fetchone(
        "SELECT superseding_event_sequence FROM boundary_supersessions "
        "WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert original is not None

    with service.db.transaction() as connection:
        dependent_trigger_sql = [
            str(row["sql"])
            for row in connection.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'trigger' AND tbl_name <> 'boundary_supersessions' "
                "AND sql LIKE '%boundary_supersessions%' AND sql IS NOT NULL"
            )
        ]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' "
            "AND sql LIKE '%boundary_supersessions%'"
        ).fetchall():
            connection.execute(f'DROP TRIGGER "{row["name"]}"')
        connection.execute(
            "DELETE FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        connection.execute("DROP INDEX boundary_supersessions_event_idx")
        connection.execute("DROP INDEX boundary_supersessions_boundary_event_idx")
        connection.execute(
            """
            CREATE TABLE boundary_supersessions_v40 (
                boundary_id TEXT PRIMARY KEY
                    REFERENCES boundaries(id) ON DELETE CASCADE,
                boundary_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                superseding_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
                reason TEXT NOT NULL CHECK(
                    reason IN ('work_canceled', 'recovery_boundary_replaced')
                ),
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO boundary_supersessions_v40 "
            "SELECT * FROM boundary_supersessions"
        )
        connection.execute("DROP TABLE boundary_supersessions")
        connection.execute(
            "ALTER TABLE boundary_supersessions_v40 RENAME TO boundary_supersessions"
        )
        connection.execute(
            "CREATE INDEX boundary_supersessions_event_idx "
            "ON boundary_supersessions(superseding_event_sequence)"
        )
        connection.execute(
            "CREATE INDEX boundary_supersessions_boundary_event_idx "
            "ON boundary_supersessions(boundary_event_sequence)"
        )
        for trigger_sql in dependent_trigger_sql:
            connection.execute(trigger_sql)
        connection.execute("DELETE FROM schema_migrations WHERE version > 40")
        connection.execute(
            "UPDATE metadata SET value = '40' WHERE key = 'schema_version'"
        )
        connection.execute("PRAGMA user_version = 40")

    service.db.initialize()

    repaired = service.db.fetchone(
        "SELECT reason, superseding_event_sequence FROM boundary_supersessions "
        "WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert service.db.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    assert repaired is not None
    assert repaired["reason"] == "goal_replaced"
    assert repaired["superseding_event_sequence"] == original["superseding_event_sequence"]
    assert service.get_work(work["id"])["open_boundaries"] == []
    assert verify_projection(service.db).healthy is True


def test_reasoner_turn_is_an_exclusive_generation_bound_lease(system):
    service = system["service"]
    work = _create_task(system, "client:lease", "Lease task")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:lease-event",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="A single decision is required",
            runtime_state="ready",
        ),
    )
    first = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="same-turn",
    )
    replay = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="same-turn",
    )
    assert replay["id"] == first["id"]

    with pytest.raises(ConflictError):
        service.acquire_reasoner_turn(
            system["cao"],
            work["id"],
            boundary_id=boundary["id"],
            expected_generation=work["generation"],
            idempotency_key="another-turn",
        )


def test_expired_reasoner_turn_rearms_exact_boundary_once(system):
    service = system["service"]
    work = _create_task(system, "client:reasoner-recovery", "Recovery task")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:reasoner-recovery",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="A decision lease will be interrupted",
            runtime_state="ready",
        ),
    )
    original_message = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? ORDER BY sequence DESC LIMIT 1",
        (work["id"],),
    )
    assert original_message is not None
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="expiring-reasoner-turn",
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", turn["id"]),
        )

    assert service.recover_expired_reasoner_turns() == 1
    assert service.recover_expired_reasoner_turns() == 0
    abandoned = service.db.fetchone(
        "SELECT * FROM reasoner_turns WHERE id = ?", (turn["id"],)
    )
    assert abandoned["state"] == "abandoned"
    stale_delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (original_message["id"], system["cao"]["id"]),
    )
    assert stale_delivery["state"] == "dead"
    recovery_messages = service.db.fetchall(
        "SELECT * FROM messages WHERE idempotency_key = ?",
        (f"reasoner-recovery:{turn['id']}",),
    )
    assert len(recovery_messages) == 1

    successor = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="recovered-reasoner-turn",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=successor["id"],
            lease_token=successor["lease_token"],
            expected_generation=work["generation"],
            kind="continue",
            reason="Recovery evidence allows the work to continue",
        ),
    )
    recovery_delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (recovery_messages[0]["id"], system["cao"]["id"]),
    )
    assert recovery_delivery["state"] == "queued"
    assert recovery_delivery["acknowledged_at"] is None
    assert recovery_delivery["handled_at"] is None
    service.acknowledge(system["cao"], AckInput(message_ids=[recovery_messages[0]["id"]]))
    assert (
        service.mark_message_handled(
            system["cao"],
            recovery_messages[0]["id"],
            evidence="The resolved recovery notification was explicitly received",
        )["state"]
        == "handled"
    )


def test_each_expired_reasoner_turn_schedules_one_successor_wake(system):
    service = system["service"]
    work = _create_task(system, "client:reasoner-recovery-exhausted", "Recovery task")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:reasoner-recovery-exhausted",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="Repeated decision expiry must not create a wake loop",
            runtime_state="ready",
        ),
    )
    first_turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="first-expiring-reasoner-turn",
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", first_turn["id"]),
        )

    assert service.recover_expired_reasoner_turns() == 1
    recovery_message = service.db.fetchone(
        "SELECT id FROM messages WHERE idempotency_key = ?",
        (f"reasoner-recovery:{first_turn['id']}",),
    )
    assert recovery_message is not None
    second_turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key=str(recovery_message["id"]),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", second_turn["id"]),
        )

    assert service.recover_expired_reasoner_turns() == 1
    assert service.recover_expired_reasoner_turns() == 0
    recovery_messages = service.db.fetchall(
        """
        SELECT m.id, json_extract(m.payload_json, '$.action') AS action, d.state
        FROM messages AS m
        JOIN message_deliveries AS d ON d.message_id = m.id
        WHERE m.work_item_id = ?
          AND json_extract(m.payload_json, '$.action') IN (
              'recover_expired_reasoner_turn',
              'recover_incomplete_reasoner_turn'
          )
        ORDER BY m.sequence
        """,
        (work["id"],),
    )
    assert [(row["action"], row["state"]) for row in recovery_messages] == [
        ("recover_expired_reasoner_turn", "dead"),
        ("recover_expired_reasoner_turn", "queued"),
    ]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'reasoner.turn_recovery_exhausted'"
        )["count"]
        == 0
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_route_drifted_expired_reasoner_notification_requires_terminal_disposition(system):
    service = system["service"]
    work = _create_task(
        system,
        "client:system-reconciliation-reasoner-recovery",
        "System reconciliation recovery",
    )
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:system-reconciliation-reasoner-recovery",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"][
                "goal_packet_digest"
            ],
            expected_task_packet_digest=work["current_attempt"][
                "task_packet_digest"
            ],
            expected_generation=work["generation"],
            kind="failure",
            summary="A decision lease will be interrupted before route drift",
            runtime_state="failed",
        ),
    )
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="expiring-system-reconciliation-reasoner-turn",
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE reasoner_turns SET lease_expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00Z", turn["id"]),
        )

    assert service.recover_expired_reasoner_turns() == 1
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE boundaries SET recovery_action = 'system_reconciliation' "
            "WHERE id = ?",
            (boundary["id"],),
        )
    recovery_message = service.db.fetchone(
        "SELECT id FROM messages WHERE idempotency_key = ?",
        (f"reasoner-recovery:{turn['id']}",),
    )
    assert recovery_message is not None

    service.acknowledge(
        system["cao"], AckInput(message_ids=[str(recovery_message["id"])])
    )
    with pytest.raises(ConflictError, match="must be disposed"):
        service.mark_message_handled(
            system["cao"],
            str(recovery_message["id"]),
            evidence="The route drift and bounded state were observed.",
        )
    successor = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="route-drift-terminal-reasoner-turn",
    )
    disposition = service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=successor["id"],
            lease_token=successor["lease_token"],
            expected_generation=work["generation"],
            kind=BoundaryDispositionKind.FAIL,
            reason="The recovery route is no longer executable",
        ),
    )

    assert disposition["kind"] == "fail"
    handled = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (recovery_message["id"], system["cao"]["id"]),
    )
    assert handled is not None and handled["state"] == "handled"
    assert service.db.fetchone(
        "SELECT kind FROM boundary_dispositions WHERE boundary_id = ?",
        (boundary["id"],),
    )["kind"] == "fail"
    assert service.db.fetchone(
        "SELECT 1 FROM boundary_supersessions WHERE boundary_id = ?",
        (boundary["id"],),
    ) is None
    assert service.get_work(work["id"])["state"] == "failed"


def test_boundary_has_exactly_one_durable_disposition(system):
    service = system["service"]
    work = _create_task(system, "client:boundary", "Boundary task")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="runtime:boundary",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=1,
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="Ready",
            runtime_state="ready",
        ),
    )
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        expected_generation=work["generation"],
        idempotency_key="boundary-turn",
    )
    request = BoundaryDispositionInput(
        turn_id=turn["id"],
        lease_token=turn["lease_token"],
        expected_generation=work["generation"],
        kind="continue",
        reason="The next step is locally decidable",
    )
    first = service.dispose_boundary(system["cao"], boundary["id"], request)
    assert first["kind"] == "continue"
    assert service.dispose_boundary(system["cao"], boundary["id"], request) == first

    with pytest.raises(ConflictError):
        service.dispose_boundary(
            system["cao"],
            boundary["id"],
            request.model_copy(
                update={
                    "kind": BoundaryDispositionKind.WAIT_USER,
                    "reason": "Duplicate",
                }
            ),
        )


def test_goal_revision_is_the_only_authoritative_goal_payload(system):
    service = system["service"]
    work = _create_task(system, "client:canonical-goal", "Canonical goal")
    columns = {
        row["name"] for row in service.db.fetchall("PRAGMA table_info(work_items)")
    }
    assert {"objective", "maturity", "acceptance_json", "non_goals_json"}.isdisjoint(columns)
    assert work["objective"] == work["current_goal_revision"]["objective"]
    assert work["acceptance"] == work["current_goal_revision"]["acceptance"]


def test_retry_is_a_new_attempt_not_an_interrupt_relation(system):
    service = system["service"]
    work = _create_task(system, "client:retry", "Retry task")
    retried = service.create_attempt(
        system["cao"],
        work["id"],
        reason="Retry cleanly",
        idempotency_key="retry-cleanly",
    )
    replay = service.create_attempt(
        system["cao"],
        work["id"],
        reason="Retry cleanly",
        idempotency_key="retry-cleanly",
    )
    assert retried["id"] == work["id"]
    assert retried["state"] == "active"
    assert retried["suspended_by_work_item_id"] is None
    assert retried["generation"] == work["generation"] + 1
    assert [attempt["state"] for attempt in retried["attempts"]] == [
        "canceled",
        "assigned",
    ]
    assert len(replay["attempts"]) == 2
    retry_boundaries = [
        boundary for boundary in retried["boundaries"] if boundary["kind"] == "retry_request"
    ]
    assert len(retry_boundaries) == 1
    assert retry_boundaries[0]["disposition"]["kind"] == "retry"
    assert not any(
        directive["relation"] == "interrupt" for directive in retried["directives"]
    )


def test_message_is_immutable_and_delivery_is_per_recipient(system):
    service = system["service"]
    message = service.send_message(
        system["cao"],
        [system["worker"]["id"], system["user"]["id"]],
        kind=MessageKind.SYSTEM,
        payload={"action": "inspect"},
        idempotency_key="multicast-1",
    )
    message_columns = {
        row["name"] for row in service.db.fetchall("PRAGMA table_info(messages)")
    }
    assert {"recipient_id", "delivery_state", "updated_at"}.isdisjoint(message_columns)
    assert len(message["deliveries"]) == 2

    service.acknowledge(system["worker"], AckInput(message_ids=[message["id"]]))
    worker_delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (message["id"], system["worker"]["id"]),
    )
    user_delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (message["id"], system["user"]["id"]),
    )
    assert worker_delivery["state"] == "acknowledged"
    assert user_delivery["state"] == "queued"
    assert service.db.fetchone("SELECT COUNT(*) AS count FROM work_items")["count"] == 0

    handled = service.mark_message_handled(
        system["worker"], message["id"], evidence="The instruction was incorporated"
    )
    replay = service.mark_message_handled(
        system["worker"], message["id"], evidence="The instruction was incorporated"
    )
    assert handled["state"] == "handled"
    assert replay["handled_at"] == handled["handled_at"]

    with pytest.raises(ConflictError):
        service.send_message(
            system["cao"],
            [system["worker"]["id"], system["user"]["id"]],
            kind=MessageKind.SYSTEM,
            payload={"action": "different"},
            idempotency_key="multicast-1",
        )
