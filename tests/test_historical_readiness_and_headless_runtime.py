from __future__ import annotations

import asyncio
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from enrollment_helpers import EnrollmentHandshakeAdapter, EnrollmentHandshakeRegistry

from cao_control_plane.database import Database
from cao_control_plane.errors import AuthenticationError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    CompletionContract,
    ReportInput,
    RequesterDecisionInput,
    ReviewInput,
    ReviewVerdict,
    RuntimeRegistration,
    WorkAssignment,
)
from cao_control_plane.projection import build_projection, verify_projection
from cao_control_plane.runtime import Dispatcher


def _canceled_work_with_boundary(
    system: dict[str, Any], *, key: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title=f"Historical cancellation {key}",
            objective="Keep the old boundary auditable without keeping it actionable.",
            acceptance=["Only a later exact cancellation may supersede the boundary."],
            idempotency_key=f"assign:historical-cancellation:{key}",
        ),
    )
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="A supervisor decision is required before work can continue.",
            idempotency_key=f"report:historical-cancellation:{key}",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    canceled = service.cancel_work(
        system["cao"],
        work["id"],
        "The requester withdrew this historical task.",
        idempotency_key=f"cancel:historical-cancellation:{key}",
    )
    assert canceled["state"] == "canceled"
    return canceled, boundary


def _rewind_schema_ledger_to_v24(database: Database) -> None:
    with database.transaction() as connection:
        connection.execute("DELETE FROM schema_migrations WHERE version > 24")
        connection.execute("UPDATE metadata SET value = '24' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 24")


def test_v26_migration_supersedes_only_a_cancellation_bound_historical_boundary(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    canceled, boundary = _canceled_work_with_boundary(system, key="proven")

    with service.db.transaction() as connection:
        connection.execute(
            "DELETE FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
    _rewind_schema_ledger_to_v24(service.db)

    service.db.initialize()

    supersession = service.db.fetchone(
        "SELECT reason, superseding_event_sequence FROM boundary_supersessions "
        "WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert supersession is not None
    assert supersession["reason"] == "work_canceled"
    event = service.db.fetchone(
        "SELECT event_type, aggregate_type FROM events WHERE sequence = ?",
        (supersession["superseding_event_sequence"],),
    )
    assert event is not None
    assert (event["event_type"], event["aggregate_type"]) == (
        "work.canceled",
        "work_item",
    )
    current = service.get_work(canceled["id"])
    assert current["open_boundaries"] == []
    assert current["boundaries"][0]["supersession"]["reason"] == "work_canceled"
    assert verify_projection(service.db).healthy is True


def test_v27_repairs_interim_supersession_shape_with_exact_event_proof(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    _, boundary = _canceled_work_with_boundary(system, key="interim-v26-shape")
    supersession = service.db.fetchone(
        "SELECT boundary_event_sequence FROM boundary_supersessions WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert supersession is not None
    expected_opening_sequence = supersession["boundary_event_sequence"]

    with service.db.transaction() as connection:
        connection.execute("DROP TRIGGER IF EXISTS boundary_supersessions_exact_binding_insert")
        connection.execute("DROP TRIGGER IF EXISTS boundary_supersessions_immutable_update")
        connection.execute("DROP INDEX IF EXISTS boundary_supersessions_boundary_event_idx")
        connection.execute("ALTER TABLE boundary_supersessions DROP COLUMN boundary_event_sequence")
        connection.execute("DELETE FROM schema_migrations WHERE version > 26")
        connection.execute("UPDATE metadata SET value = '26' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 26")

    service.db.initialize()

    columns = {
        row["name"] for row in service.db.fetchall("PRAGMA table_info(boundary_supersessions)")
    }
    repaired = service.db.fetchone(
        "SELECT boundary_event_sequence FROM boundary_supersessions WHERE boundary_id = ?",
        (boundary["id"],),
    )
    assert "boundary_event_sequence" in columns
    assert repaired is not None
    assert repaired["boundary_event_sequence"] == expected_opening_sequence
    assert verify_projection(service.db).healthy is True


@pytest.mark.parametrize(
    "invalid_history",
    ["current_generation", "missing_cancellation_event"],
)
def test_v26_migration_keeps_unproven_boundary_history_fail_closed(
    system: dict[str, Any], invalid_history: str
) -> None:
    service = system["service"]
    canceled, boundary = _canceled_work_with_boundary(system, key=invalid_history)
    with service.db.transaction() as connection:
        connection.execute(
            "DELETE FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        if invalid_history == "current_generation":
            connection.execute(
                "UPDATE boundaries SET generation = ? WHERE id = ?",
                (canceled["generation"], boundary["id"]),
            )
        else:
            connection.execute(
                "DELETE FROM events WHERE event_type = 'work.canceled' "
                "AND aggregate_type = 'work_item' AND aggregate_id = ?",
                (canceled["id"],),
            )
    _rewind_schema_ledger_to_v24(service.db)

    with pytest.raises(RuntimeError, match="terminal Work has an unresolved legacy Boundary"):
        service.db.initialize()

    assert (
        service.db.fetchone(
            "SELECT boundary_id FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )


def test_new_attachment_connection_preserves_historical_requester_acceptance_projection(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment_request = current_cao_session_attachment(
        native_thread_id="historical-readiness-conversation",
        project_digest="a" * 64,
    )
    attachment = attach_cao_session_with_peer(service, attachment_request)
    conversation = service.authenticate(str(attachment["context_token"]))
    work = service.assign_work(
        conversation,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            title="Historical requester acceptance",
            objective="Keep accepted completion valid across transport connections.",
            acceptance=["A later connection does not rewrite lifecycle history."],
            completion_contract=CompletionContract.NO_ARTIFACT_EXPECTED,
            idempotency_key="assign:historical-requester-acceptance",
        ),
    )
    attempt = work["current_attempt"]
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The exact no-artifact result is complete.",
            idempotency_key="report:historical-requester-acceptance",
        ),
    )
    boundary = reported["open_boundaries"][0]
    reviewed = service.review(
        conversation,
        ReviewInput(
            attempt_id=attempt["id"],
            verdict=ReviewVerdict.OK,
            summary="Independent review verified the claimed result.",
            idempotency_key="review:historical-requester-acceptance",
        ),
    )
    turn = service.acquire_reasoner_turn(
        conversation,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key="turn:historical-requester-acceptance",
    )
    service.dispose_boundary(
        conversation,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind=BoundaryDispositionKind.ACCEPT,
            reason="The durable OK review matches this completion boundary.",
        ),
    )
    completed = service.record_requester_decision(
        conversation,
        RequesterDecisionInput(
            review_id=reviewed["reviews"][-1]["id"],
            verdict="accepted",
            summary="The requester accepted the reviewed result.",
            evidence=[],
            conversation_evidence_id="historical-requester-acceptance",
            idempotency_key="decision:historical-requester-acceptance",
        ),
    )
    decision = completed["requester_decisions"][-1]
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?",
        (attachment["runtime_session_id"],),
    )
    wake_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?", (attachment["runtime_session_id"],)
        )
    )
    work_before = dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
    worker_before = dict(
        service.db.fetchone("SELECT * FROM principals WHERE id = ?", (work["assigned_worker_id"],))
    )
    connection_before = dict(
        service.db.fetchone(
            "SELECT * FROM cao_attachment_connections WHERE id = ?",
            (attachment["connection_id"],),
        )
    )
    credential_before = dict(
        service.db.fetchone(
            "SELECT * FROM cao_conversation_credentials WHERE id = ?",
            (attachment["context_credential_id"],),
        )
    )

    connected = attach_cao_session_with_peer(service, attachment_request)

    assert connected["id"] == attachment["id"]
    assert connected["generation"] == decision["supervisor_attachment_generation"]
    assert connected["runtime_session_id"] != attachment["runtime_session_id"]
    assert connected["runtime"]["state"] == "ready"
    assert connected["runtime"]["native_session_id"] == ("historical-readiness-conversation")
    assert connected["connection_generation"] == attachment["connection_generation"] + 1
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?", (attachment["runtime_session_id"],)
            )
        )
        == wake_before
    )
    assert dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))) == (
        work_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM principals WHERE id = ?", (work["assigned_worker_id"],)
            )
        )
        == worker_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_attachment_connections WHERE id = ?",
                (attachment["connection_id"],),
            )
        )
        == connection_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM cao_conversation_credentials WHERE id = ?",
                (attachment["context_credential_id"],),
            )
        )
        == credential_before
    )
    projection = verify_projection(service.db)
    assert projection.healthy is True
    assert {
        "requester_decision.binding_invalid",
        "work.completed_without_requester_acceptance",
    }.isdisjoint(violation.code for violation in projection.violations)


def test_completion_correction_relaunches_waiting_logical_runtime_with_fresh_epoch(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    service.stop_runtime(system["cao"], system["runtime"]["id"])
    runtime = service.register_runtime(
        system["cao"],
        system["worker"]["id"],
        RuntimeRegistration(adapter="claude", metadata={"command": ["/bin/cat"]}),
    )
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=runtime["id"],
            title="Headless correction relaunch",
            objective="Continue through a fresh credential epoch after clean exit.",
            acceptance=["CAO correction does not manage the OS process lifecycle."],
            completion_contract=CompletionContract.NO_ARTIFACT_EXPECTED,
            idempotency_key="assign:headless-correction-relaunch",
        ),
    )
    attempt = work["current_attempt"]
    adapter: EnrollmentHandshakeAdapter

    def consume_delivery_and_report_once(_runtime: dict[str, Any], message: dict[str, Any]) -> None:
        actor = adapter.actors[-1]
        message_id = str(message["id"])
        service.acknowledge(actor, AckInput(message_ids=[message_id]))
        service.mark_message_handled(
            actor, message_id, evidence="The exact launch delivery was consumed."
        )
        if message["kind"] != "assignment":
            return
        service.report(
            actor,
            attempt["id"],
            ReportInput(
                kind="completion_claim",
                expected_goal_version=work["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=work["generation"],
                summary="The first headless launch produced a reviewable result.",
                idempotency_key="report:headless-correction-relaunch",
            ),
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=consume_delivery_and_report_once)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    first_epoch = adapter.actors[-1]["_enrollment_generation"]
    assert service.get_runtime(runtime["id"])["state"] == "waiting"
    with pytest.raises(AuthenticationError):
        service.authenticate(adapter.credentials[-1])

    reported = service.get_work(work["id"])
    boundary = reported["open_boundaries"][0]
    service.review(
        system["cao"],
        ReviewInput(
            attempt_id=attempt["id"],
            verdict=ReviewVerdict.NEEDS_WORK,
            summary="The result needs one bounded correction.",
            idempotency_key="review:headless-correction-relaunch",
        ),
    )
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key="turn:headless-correction-relaunch",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind=BoundaryDispositionKind.CORRECT,
            reason="Apply the independent needs-work review.",
            instruction="Correct the reviewed issue and report the result.",
        ),
    )

    assert asyncio.run(dispatcher.run_once()) == 1

    assert len(adapter.actors) == 2
    assert adapter.actors[-1]["_enrollment_generation"] == first_epoch + 1
    assert adapter.credentials[0] != adapter.credentials[1]
    credential_generations = [
        int(
            service.db.fetchone(
                "SELECT generation FROM runtime_credentials WHERE id = ?",
                (actor["_runtime_credential_id"],),
            )["generation"]
        )
        for actor in adapter.actors
    ]
    assert credential_generations == [first_epoch, first_epoch + 1]
    assert {delivery["runtime_id"] for delivery in adapter.deliveries} == {runtime["id"]}
    current = service.get_work(work["id"])
    assert current["current_attempt"]["id"] == attempt["id"]
    assert current["state"] == "waiting_supervisor"
    assert current["current_attempt"]["state"] == "waiting_supervisor"
    assert current["current_attempt"]["stage"] == "system_reconciliation"
    assert any(
        boundary["metadata"].get("reason") == "worker_turn_completed_without_terminal_report"
        for boundary in current["open_boundaries"]
    )
    assert service.get_runtime(runtime["id"])["state"] == "waiting"
    instruction_delivery = service.db.fetchone(
        "SELECT delivery.state, delivery.runtime_session_id "
        "FROM messages AS message JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'instruction'",
        (attempt["id"],),
    )
    assert instruction_delivery is not None
    assert instruction_delivery["state"] == "handled"
    assert instruction_delivery["runtime_session_id"] == runtime["id"]


def test_clean_headless_exit_retires_terminal_lane_before_fresh_work_epoch(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    service.stop_runtime(system["cao"], system["runtime"]["id"])
    runtime = service.register_runtime(
        system["cao"],
        system["worker"]["id"],
        RuntimeRegistration(adapter="claude", metadata={"command": ["/bin/cat"]}),
    )
    prior = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=runtime["id"],
            title="Terminal predecessor",
            objective="End this Work during its first headless epoch.",
            acceptance=["Its closed delivery lane cannot block later Work."],
            idempotency_key="assign:terminal-headless-predecessor",
        ),
    )
    current = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=runtime["id"],
            title="Fresh headless successor",
            objective="Launch in a new credential generation after prior exit.",
            acceptance=["The second Assignment reaches a fresh headless process."],
            idempotency_key="assign:fresh-headless-successor",
        ),
    )
    prior_attempt_id = prior["current_attempt"]["id"]
    current_attempt_id = current["current_attempt"]["id"]
    assert (
        service.get_work(current["id"])["current_attempt"]["activity"]["runtime_heartbeat_at"] == ""
    )
    adapter: EnrollmentHandshakeAdapter

    def finish_prior_then_consume_current(
        _runtime: dict[str, Any], message: dict[str, Any]
    ) -> None:
        actor = adapter.actors[-1]
        message_id = str(message["id"])
        service.acknowledge(actor, AckInput(message_ids=[message_id]))
        if message["attempt_id"] == prior_attempt_id:
            # Leave the Assignment acknowledged and its cancel queued. The
            # exiting Worker credential cannot later mark either handled.
            service.cancel_work(
                system["cao"],
                prior["id"],
                "The first Work is complete as terminal history.",
                idempotency_key="cancel:terminal-headless-predecessor",
            )
            return
        assert message["attempt_id"] == current_attempt_id
        service.mark_message_handled(
            actor,
            message_id,
            evidence="The fresh runtime consumed only its exact Assignment.",
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=finish_prior_then_consume_current)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    first_generation = int(adapter.actors[0]["_enrollment_generation"])
    assert service.get_runtime(runtime["id"])["state"] == "waiting"
    prior_deliveries = service.db.fetchall(
        """
        SELECT message.kind, delivery.state
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND delivery.recipient_id = ?
        ORDER BY message.sequence
        """,
        (prior_attempt_id, system["worker"]["id"]),
    )
    assert [(row["kind"], row["state"]) for row in prior_deliveries] == [
        ("assignment", "handled"),
        ("cancel", "dead"),
    ]

    assert asyncio.run(dispatcher.run_once()) == 1

    assert len(adapter.actors) == 2
    assert int(adapter.actors[1]["_enrollment_generation"]) == first_generation + 1
    assert [delivery["runtime_id"] for delivery in adapter.deliveries] == [
        runtime["id"],
        runtime["id"],
    ]
    current_assignment = service.db.fetchone(
        """
        SELECT delivery.state FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (current_attempt_id,),
    )
    assert current_assignment is not None
    assert current_assignment["state"] == "handled"
    current_activity = service.get_work(current["id"])["current_attempt"]["activity"]
    assert current_activity["runtime_heartbeat_at"]
    dashboard_item = next(
        item
        for item in build_projection(service.db).snapshot["work_items"]
        if item["id"] == current["id"]
    )
    assert (
        dashboard_item["operator_content"]["runtime_heartbeat_at"]
        == current_activity["runtime_heartbeat_at"]
    )
    event_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type IN ('message.handled', 'message.delivery_superseded') "
        "AND aggregate_id IN (SELECT id FROM messages WHERE attempt_id = ?)",
        (prior_attempt_id,),
    )
    assert event_count is not None
    service.reconcile_terminal_headless_delivery_lanes()
    replay_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type IN ('message.handled', 'message.delivery_superseded') "
        "AND aggregate_id IN (SELECT id FROM messages WHERE attempt_id = ?)",
        (prior_attempt_id,),
    )
    assert replay_count is not None
    assert replay_count["count"] == event_count["count"]


@pytest.mark.parametrize("nonexecuting_state", ["stopped", "failed", "missing"])
def test_terminal_lane_retires_for_every_nonexecuting_runtime_state(
    system: dict[str, Any],
    nonexecuting_state: str,
) -> None:
    """Connection loss cannot preserve terminal history as a live FIFO head."""

    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=runtime["id"],
            title="Terminal lane on a non-executing runtime",
            objective="Remove terminal history from the live command lane.",
            acceptance=["Every non-executing runtime state converges identically."],
            idempotency_key=f"assign:terminal-nonexecuting:{nonexecuting_state}",
        ),
    )
    service.cancel_work(
        system["cao"],
        work["id"],
        "End the acceptance fixture before its runtime disappears.",
        idempotency_key=f"cancel:terminal-nonexecuting:{nonexecuting_state}",
    )
    service.stop_runtime(system["cao"], runtime["id"])
    service.db.execute(
        "UPDATE runtime_sessions SET state = ? WHERE id = ?",
        (nonexecuting_state, runtime["id"]),
    )

    assert service.reconcile_terminal_headless_delivery_lanes() == 2
    rows = service.db.fetchall(
        """
        SELECT message.kind, delivery.state, delivery.reactivation_policy
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND delivery.recipient_id = ?
        ORDER BY message.sequence
        """,
        (work["current_attempt"]["id"], system["worker"]["id"]),
    )
    assert [(row["kind"], row["state"], row["reactivation_policy"]) for row in rows] == [
        ("assignment", "dead", "terminal"),
        ("cancel", "dead", "terminal"),
    ]
    assert service.reconcile_terminal_headless_delivery_lanes() == 0
