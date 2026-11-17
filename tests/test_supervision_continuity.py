from __future__ import annotations

import pytest
from test_managed_worker_command_admission import (
    _attached,
    _enroll_current_runtime,
    _instruct,
    _new_thread,
    _question,
)
from test_worker_output_delivery import _capture_fixture, _event, _terminal

from cao_control_plane.errors import AuthorizationError, ConflictError
from cao_control_plane.models import (
    BoundaryDispositionInput,
    CloseCAOConversationInput,
    InstructWorkerThreadInput,
    ReportInput,
    ResumeWorkerThreadInput,
    StatusRequestInput,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import verify_projection


def _blocked_command(system, tmp_path):
    service = system["service"]
    actor = _attached(service, suffix="supervision-denial")
    thread = _new_thread(service, actor, tmp_path, suffix="supervision-denial")
    worker, _ = _enroll_current_runtime(service, thread)
    result = _instruct(service, actor, thread, suffix="supervision-denial")
    work = _question(service, worker, result["task"]["work_item_id"], suffix="supervision-denial")
    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND kind = 'assignment'", (work["id"],),
    )
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched', handled_at = NULL WHERE message_id = ?",
        (assignment["id"],),
    )
    boundary = work["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        actor, work["id"], boundary_id=boundary["id"],
        expected_generation=work["generation"], idempotency_key="denied-correction",
    )
    request = BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
        expected_purpose_version=1, kind="correct", reason="Continue toward the objective.",
        instruction="Produce the remaining evidence.",
    )
    return service, actor, work, boundary, request, assignment["id"]


def test_nonretryable_denial_retains_cao_recovery_without_replaying_unknown_operation(system, tmp_path):
    service, actor, work, boundary, request, assignment = _blocked_command(system, tmp_path)
    before = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment,)))
    disposition = service.dispose_boundary(actor, boundary["id"], request)
    assert disposition["kind"] == "wait_system"
    assert service.dispose_boundary(actor, boundary["id"], request) == disposition
    after = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment,)))
    assert after == before
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["attention_owner"] == "cao"
    assert current["requester_escalation"] is None
    assert current["system_recovery"]["notification_state"] == "queued"
    assert current["system_recovery"]["requester_action_required"] is False
    assert current["current_attempt"]["completion_claim"] == {}
    assert current["generation"] == work["generation"]
    with pytest.raises(ConflictError) as caught:
        service.close_cao_conversation(actor, CloseCAOConversationInput(idempotency_key="close-live-unknown"))
    assert caught.value.details["blockers"]["unknown_delivery"] == 1
    assert dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment,))) == before
    assert service.recover_expired_reasoner_turns() == 0
    assert service.reconcile_cao_supervision_obligations() == 0
    assert verify_projection(service.db).healthy


def test_invalid_lease_cannot_turn_a_denial_into_escalation_authority(system, tmp_path):
    service, actor, work, boundary, request, _ = _blocked_command(system, tmp_path)
    request = request.model_copy(update={"lease_token": "wrong-token"})
    with pytest.raises(AuthorizationError):
        service.dispose_boundary(actor, boundary["id"], request)
    assert service.get_work(work["id"])["requester_escalation"] is None


@pytest.mark.parametrize("historical_state", ["canceled", "waiting_supervisor"])
@pytest.mark.parametrize("operation", ["correct", "close"])
def test_retired_other_work_unknown_delivery_cannot_veto_current_work(system, tmp_path, historical_state, operation):
    service = system["service"]
    actor = _attached(service, suffix="historical-evidence")
    thread = _new_thread(service, actor, tmp_path, suffix="historical-evidence")
    worker, runtime_id = _enroll_current_runtime(service, thread)
    old = _instruct(service, actor, thread, suffix="historical-evidence")
    if historical_state == "waiting_supervisor":
        _question(service, worker, old["task"]["work_item_id"], suffix="historical-held")
    assignment = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND kind = 'assignment'",
        (old["task"]["work_item_id"],),
    )
    service.db.execute("UPDATE message_deliveries SET state = 'dispatched' WHERE message_id = ?", (assignment["id"],))
    if historical_state == "waiting_supervisor":
        service.fail_runtime_enrollment(runtime_id, reason="runtime_fixture_failed")
    else:
        service.finish_worker_thread(actor, WorkerThreadLifecycleInput(
            worker_thread_id=thread, idempotency_key="finish-historical",
        ))
        service.resume_worker_thread(actor, ResumeWorkerThreadInput(
            worker_thread_id=thread, idempotency_key="resume-historical",
        ))
    instructed = service.instruct_worker_thread(actor, InstructWorkerThreadInput(
        worker_thread_id=thread, objective="Produce independent new evidence.",
        idempotency_key="new-independent-work", expected_purpose_version=1,
    ))
    worker, _ = _enroll_current_runtime(service, thread,
        attempt_id=service.get_work(instructed["task"]["work_item_id"])["current_attempt"]["id"])
    work = _question(service, worker, instructed["task"]["work_item_id"], suffix="new-evidence")
    unknown = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment["id"],)))
    old_work = dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (old["task"]["work_item_id"],)))
    old_attempt = dict(service.db.fetchone("SELECT * FROM attempts WHERE work_item_id = ?", (old_work["id"],)))
    assert old_attempt["state"] == historical_state
    if operation == "close":
        lifecycle = dict(service.db.fetchone("SELECT * FROM managed_worker_threads WHERE id = ?", (thread,)))
        service.close_cao_conversation(actor, CloseCAOConversationInput(idempotency_key="close-retired-history"))
        assert service.get_work(work["id"])["state"] == "canceled"
        assert dict(service.db.fetchone("SELECT * FROM managed_worker_threads WHERE id = ?", (thread,))) == lifecycle
        assert dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment["id"],))) == unknown
        return
    boundary = work["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(actor, work["id"], boundary_id=boundary["id"],
        expected_generation=work["generation"], idempotency_key="correct-independent-work")
    result = service.dispose_boundary(actor, boundary["id"], BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
        expected_purpose_version=1, kind="correct", reason="Resolve the new question.",
        instruction="Use the verified independent input.",
    ))
    assert result["kind"] == "correct"
    assert service.get_work(work["id"])["attention_owner"] == "worker"
    assert dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment["id"],))) == unknown
    assert dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (old_work["id"],))) == old_work
    assert dict(service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (old_attempt["id"],))) == old_attempt
    assert unknown["state"] == "dispatched"


@pytest.mark.parametrize("decision_kind", ["question", "blocker", "completion_claim"])
def test_progress_burst_is_recorded_without_wake_and_decision_still_wakes(system, decision_kind):
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    attempt = work["current_attempt"]
    fields = dict(expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"], expected_generation=work["generation"])
    for index in range(12):
        service.report(system["worker"], attempt["id"], ReportInput(
            **fields, kind="progress", summary="Routine execution remains in progress.",
            idempotency_key=f"routine-progress-{index}",
        ))
        service.observe_worker_output(**binding, event=_event(item_id=f"comment-{index}", phase="commentary"))
    assert service.get_inbox(actor)["items"] == []
    assert len(service.get_work(work["id"])["worker_outputs"]) == 12
    service.report(system["worker"], attempt["id"], ReportInput(
        **fields, kind=decision_kind, summary="A supervision decision is now required.",
        idempotency_key="decision-report",
    ))
    assert [item["kind"] for item in service.get_inbox(actor)["items"]] == [decision_kind]
    _terminal(service, binding)
    assert len(service.get_work(work["id"])["open_boundaries"]) == 1


def test_denied_continuation_and_consultation_notification_commit_together(system, tmp_path, monkeypatch):
    service, actor, work, boundary, request, _ = _blocked_command(system, tmp_path)
    original = service._message

    def fail_consultation(connection, **kwargs):
        if kwargs.get("payload", {}).get("action") == "requester_escalation":
            raise OSError("consultation unavailable")
        return original(connection, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(service, "_message", fail_consultation)
        with pytest.raises(OSError, match="consultation unavailable"):
            service.dispose_boundary(actor, boundary["id"], request)
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["requester_escalation"] is None
    assert current["open_boundaries"][0]["id"] == boundary["id"]
    assert service.dispose_boundary(actor, boundary["id"], request)["kind"] == "wait_system"


def test_system_recovery_rearms_same_work_after_verified_recovery(system, tmp_path):
    service, actor, work, boundary, request, assignment = _blocked_command(system, tmp_path)
    assert service.dispose_boundary(actor, boundary["id"], request)["kind"] == "wait_system"
    # Test-only independent consumption evidence, not an automatic production repair.
    service.db.execute("UPDATE message_deliveries SET state = 'handled' WHERE message_id = ?", (assignment,))
    assert service.reconcile_cao_supervision_obligations() == 1
    ready = service.get_work(work["id"])
    assert ready["system_recovery"] is None
    assert ready["generation"] == work["generation"] + 1
    assert service.reconcile_cao_supervision_obligations() == 0
    successor_boundary = ready["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(actor, work["id"],
        boundary_id=successor_boundary["id"], expected_generation=ready["generation"],
        idempotency_key="automatic-recovery-decision")
    decision = service.dispose_boundary(actor, successor_boundary["id"], BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=ready["generation"],
        expected_purpose_version=1, kind="correct", reason="The exact blocker cleared.",
        instruction="Complete only verified unmet conditions.",
    ))
    assert decision["kind"] == "correct", decision
    resumed = service.get_work(work["id"])
    assert resumed["state"] == "active"
    assert resumed["id"] == work["id"]
    assert resumed["objective"] == work["objective"]
    assert resumed["current_attempt"]["id"] == work["current_attempt"]["id"]
    assert resumed["requester_escalation"] is None
    assert verify_projection(service.db).healthy


def test_explicit_status_request_gets_one_reply_without_enabling_routine_wakes(system):
    service = system["service"]
    actor, work, _binding = _capture_fixture(system)
    service.request_status(actor, work["id"], StatusRequestInput(
        expected_generation=work["generation"], idempotency_key="explicit-status-ask",
    ))
    attempt = work["current_attempt"]
    fields = dict(expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"], expected_generation=work["generation"])
    for index in range(2):
        service.report(system["worker"], attempt["id"], ReportInput(
            **fields, kind="progress", summary="Requested current execution status.",
            idempotency_key=f"status-reply-{index}",
        ))
    inbox = service.get_inbox(actor)["items"]
    assert len(inbox) == 1 and inbox[0]["kind"] == "progress"
    assert service.get_work(work["id"])["attention_owner"] == "worker"
    assert verify_projection(service.db).healthy


def test_execution_start_is_durable_without_waking_cao_and_keeps_worker_attention(system):
    service = system["service"]
    actor, work, _binding = _capture_fixture(system)
    attempt = work["current_attempt"]
    for index in range(2):
        service.report(system["worker"], attempt["id"], ReportInput(
            kind="progress", progress_event="execution_started",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The first assigned operation produced a recorded result.",
            evidence=[{"action": "The fixture result was saved and read back."}],
            idempotency_key=f"execution-start-{index}",
        ))
    inbox = service.get_inbox(actor)["items"]
    assert inbox == []
    current = service.get_work(work["id"])
    assert current["attention_owner"] == "worker"
    assert current["open_boundaries"] == []
    assert current["current_attempt"]["activity"]["execution_start"]["message_id"].startswith("msg_")
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM events WHERE event_type='attempt.execution_started'")["n"] == 1
    service.report(system["worker"], attempt["id"], ReportInput(
        kind="completion_claim", expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"], summary="The bounded fixture is complete.",
        idempotency_key="completion-after-start",
    ))
    assert service.reconcile_cao_progress_wake_deliveries() == 0
    assert [item["kind"] for item in service.get_inbox(actor)["items"]] == ["completion_claim"]


def test_execution_start_cannot_be_an_ack_without_evidence():
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="concrete evidence"):
        ReportInput(kind="progress", progress_event="execution_started", summary="Acknowledged.",
            expected_goal_version=1, expected_goal_packet_digest="a" * 64,
            expected_task_packet_digest="b" * 64, expected_generation=1)
