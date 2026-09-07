from __future__ import annotations

import pytest

from cao_control_plane.errors import (
    AuthorizationError,
    StaleGenerationError,
    StaleGoalError,
)
from cao_control_plane.models import (
    GoalRevision,
    ReportInput,
    WorkAssignment,
)


def assign(system):
    service = system["service"]
    return service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Implement feature",
            objective="Implement a deterministic feature",
            acceptance=["Tests pass", "Evidence is attached"],
            non_goals=["Do not deploy"],
            idempotency_key="assign-1",
        ),
    )


def test_assignment_is_idempotent(system):
    first = assign(system)
    second = assign(system)
    assert first["id"] == second["id"]
    count = system["service"].db.fetchone("SELECT COUNT(*) AS count FROM work_items")
    assert count["count"] == 1


def test_stale_goal_report_is_rejected(system):
    service = system["service"]
    work = assign(system)
    revised = service.revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=1,
            objective="Implement revised feature",
            maturity="defined",
            acceptance=["Revised tests pass"],
            reason="Requirements changed",
        ),
    )
    assert revised["goal_version"] == 2
    with pytest.raises(StaleGoalError):
        service.report(
            system["worker"],
            work["current_attempt"]["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=1,
                expected_generation=work["generation"],
                expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
                expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
                summary="Old work",
            ),
        )


def test_attempt_history_is_preserved(system):
    service = system["service"]
    work = assign(system)
    retried = service.create_attempt(system["cao"], work["id"], reason="clean retry")
    assert len(retried["attempts"]) == 2
    assert retried["attempts"][0]["state"] == "canceled"
    assert retried["attempts"][1]["attempt_number"] == 2


def test_worker_cannot_read_other_worker_attempt(system):
    service = system["service"]
    work = assign(system)
    other_created = service.create_principal(
        system["cao"],
        __import__("cao_control_plane.models", fromlist=["PrincipalCreate"]).PrincipalCreate(
            name="worker-2", role="worker"
        ),
    )
    other = service.authenticate(other_created["token"])
    with pytest.raises(AuthorizationError):
        service.get_worker_context(other, work["current_attempt"]["id"])


def test_terminal_work_cannot_be_reported(system):
    service = system["service"]
    work = assign(system)
    service.cancel_work(system["cao"], work["id"], "stop")
    with pytest.raises(StaleGenerationError):
        service.report(
            system["worker"],
            work["current_attempt"]["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=1,
                expected_generation=work["generation"],
                expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
                expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
                summary="late",
            ),
        )


def test_cancel_terminalizes_work_when_worker_runtime_is_already_stopped(system):
    service = system["service"]
    work = assign(system)
    service.stop_runtime(system["cao"], system["runtime"]["id"])

    canceled = service.cancel_work(
        system["cao"], work["id"], "stop abandoned work", "cancel-stopped-runtime"
    )

    assert canceled["state"] == "canceled"
    assert canceled["current_attempt"]["state"] == "canceled"
    delivery = service.db.fetchone(
        """
        SELECT d.state, d.last_error
        FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.attempt_id = ?
        ORDER BY m.created_at DESC LIMIT 1
        """,
        (work["current_attempt"]["id"],),
    )
    assert delivery is not None
    assert delivery["state"] == "dead"
    assert delivery["last_error"] == "work_canceled"
    skipped = service.db.fetchone(
        "SELECT data_json FROM events WHERE event_type = ? AND aggregate_id = ?",
        ("work.cancel_notification_skipped", work["id"]),
    )
    assert skipped is not None


def test_user_cannot_bypass_cao_supervision_or_cross_request_scope(system):
    service = system["service"]
    work = assign(system)
    other_created = service.create_principal(
        system["cao"],
        __import__("cao_control_plane.models", fromlist=["PrincipalCreate"]).PrincipalCreate(
            name="other-user", role="user"
        ),
    )
    other = service.authenticate(other_created["token"])

    assert service.get_work(work["id"], system["user"])["requester_id"] == system["user"]["id"]
    assert service.query_work(
        __import__("cao_control_plane.models", fromlist=["QueryInput"]).QueryInput(),
        other,
    )["items"] == []

    with pytest.raises(AuthorizationError):
        service.get_work(work["id"], other)
    with pytest.raises(AuthorizationError):
        service.cancel_work(other, work["id"], "not mine")
    with pytest.raises(AuthorizationError):
        service.assign_work(
            system["user"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title="Bypass",
                objective="Bypass CAO",
                acceptance=["Should not happen"],
            ),
        )
    with pytest.raises(AuthorizationError):
        service.reply(system["user"], work["id"], "Direct Worker instruction")
