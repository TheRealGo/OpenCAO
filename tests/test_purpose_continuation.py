from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_managed_worker_command_admission import _attached, _enroll_current_runtime, _new_thread

from cao_control_plane import purpose_continuation
from cao_control_plane.database import Database
from cao_control_plane.errors import (
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    ValidationError,
)
from cao_control_plane.mcp import MCPServer, conversation_server_tools
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    CloseCAOConversationInput,
    InstructWorkerThreadInput,
    PurposeNextActionInput,
    ReportInput,
    ResumeWorkerThreadInput,
    ReviewInput,
    SetWorkerPurposeInput,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import verify_projection
from cao_control_plane.runtime import render_message
from cao_control_plane.runtime_enrollment import ProcessIdentity
from cao_control_plane.service import ControlPlane


def _legacy_checkpoint_accept(service, actor, boundary_id, disposition):
    def legacy(connection, _actor, work, *, review_id, next_action):
        assert next_action is None
        purpose_continuation.require_tx(service, connection, work, review_id=review_id)
    with patch.object(service, "_accept_checkpoint_next_tx", legacy):
        return service.dispose_boundary(actor, boundary_id, disposition)


def _checkpoint(
    system, tmp_path, *, suffix="purpose-next", accept=True, finite=False, worker_out=None
):
    service = system["service"]
    actor = _attached(service, suffix=suffix)
    thread = _new_thread(service, actor, tmp_path, suffix=suffix)
    version = 1
    if finite:
        from conftest import business_purpose_contract

        contract = business_purpose_contract()
        contract["completion"] = (
            "This finite responsibility ends when its one fixed verification outcome is independently established."
        )
        service.set_worker_purpose(
            actor,
            SetWorkerPurposeInput(
                worker_thread_id=thread,
                purpose="Complete one finite verification responsibility.",
                contract=contract,
                expected_purpose_version=1,
                reason="The requester explicitly assigned a finite verification responsibility.",
                idempotency_key=f"finite:{suffix}",
            ),
        )
        version = 2
    worker, _ = _enroll_current_runtime(service, thread)
    if worker_out is not None:
        worker_out.append(worker)
    assigned = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=thread,
            expected_purpose_version=version,
            instruction="Produce one bounded verified checkpoint toward the enduring purpose.",
            completion_contract="no_artifact_expected",
            idempotency_key=f"initial:{suffix}",
        ),
    )
    work = service.get_work(assigned["task"]["work_item_id"])
    attempt = work["current_attempt"]
    reported = service.report(
        worker,
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            summary="The bounded checkpoint is verified; the purpose continues.",
            expected_goal_version=work["goal_version"],
            expected_generation=work["generation"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            evidence=[{"result": "verified checkpoint"}],
            idempotency_key=f"completion:{suffix}",
        ),
    )
    boundary = reported["open_boundaries"][0]
    service.review(
        actor,
        ReviewInput(
            attempt_id=attempt["id"],
            expected_purpose_version=version,
            verdict="ok",
            summary="The exact checkpoint is satisfied; broader purpose remains ongoing.",
            criterion_assessments=[
                {
                    "criterion_id": "task_instruction",
                    "verdict": "met",
                    "evidence": "The bounded declared result was independently verified.",
                }
            ],
            idempotency_key=f"review:{suffix}",
        ),
    )
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key=f"turn:{suffix}",
    )
    disposition = BoundaryDispositionInput(
        turn_id=turn["id"],
        lease_token=turn["lease_token"],
        expected_generation=work["generation"],
        expected_purpose_version=version,
        kind="accept",
        reason="Accept this satisfied checkpoint.",
    )
    if accept:
        _legacy_checkpoint_accept(service, actor, boundary["id"], disposition)
    return service, actor, thread, work, boundary, disposition


def _pending(service, work):
    values = service.get_work(work["id"])["purpose_continuations"]
    assert values
    return values[0]


def _wake(service, continuation):
    row = service.db.fetchone(
        "SELECT * FROM messages WHERE kind='system' AND "
        "json_extract(payload_json, '$.purpose_continuation_id')=? ORDER BY sequence DESC LIMIT 1",
        (continuation["id"],),
    )
    assert row is not None
    return service._message_view(row)


def _next_request(thread, continuation, *, key="next"):
    return InstructWorkerThreadInput(
        worker_thread_id=thread,
        expected_purpose_version=1,
        purpose_continuation_id=continuation["id"],
        expected_continuation_generation=continuation["generation"],
        instruction="Execute the next distinct bounded outcome using the verified checkpoint.",
        completion_contract="no_artifact_expected",
        idempotency_key=key,
    )


def test_accepted_checkpoint_keeps_a_separate_durable_next_decision(system, tmp_path):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_user" and current["attention_owner"] == "user"
    assert current["open_boundaries"] == []
    continuation = _pending(service, work)
    assert continuation["state"] == "pending"
    wake = _wake(service, continuation)
    assert wake["work_item_id"] == work["id"]
    assert "次の目的判断待ち" in render_message(wake)
    [exact] = service.get_inbox(actor, message_id=wake["id"])["items"]
    assert "next purpose decision" in exact["processing_instructions"]
    delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id=?", (wake["id"],)
    )
    assert delivery["recipient_attachment_id"] == actor["_cao_attachment_id"]
    service.acknowledge(actor, AckInput(message_ids=[wake["id"]]))
    with pytest.raises(ConflictError, match="next purpose action"):
        service.mark_message_handled(actor, wake["id"], evidence="A reminder was read.")
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    assert restarted.get_work(work["id"])["purpose_continuations"] == [continuation]
    assert restarted.reconcile_cao_supervision_obligations() == 0
    assert verify_projection(service.db).healthy


def test_next_assignment_and_continuation_resolve_atomically_and_replay_once(system, tmp_path):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    request = _next_request(thread, continuation)
    result = service.instruct_worker_thread(actor, request)
    assert service.instruct_worker_thread(actor, request) == result
    recorded = _pending(service, work)
    assert recorded["state"] == "assigned"
    assert recorded["next_work_item_id"] == result["task"]["work_item_id"]
    assert result["task"]["work_item_id"] != work["id"]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
        )[0]
        == 2
    )
    assert service.reconcile_cao_supervision_obligations() == 0


def test_failed_continuation_commit_rolls_back_the_new_work(system, tmp_path, monkeypatch):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)

    def fail(*args, **kwargs):
        raise OSError("decision persistence unavailable")

    monkeypatch.setattr(purpose_continuation, "resolve_tx", fail)
    with pytest.raises(OSError, match="persistence unavailable"):
        service.instruct_worker_thread(actor, _next_request(thread, continuation))
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
        )[0]
        == 1
    )
    assert _pending(service, work)["state"] == "pending"


def test_acceptance_and_next_obligation_commit_together(system, tmp_path, monkeypatch):
    service, actor, _, work, boundary, disposition = _checkpoint(system, tmp_path, accept=False)
    original = service._message

    def fail_wake(connection, **kwargs):
        if kwargs.get("payload", {}).get("action") == "purpose_continuation":
            raise OSError("wake persistence unavailable")
        return original(connection, **kwargs)

    monkeypatch.setattr(service, "_message", fail_wake)
    with pytest.raises(OSError, match="wake persistence unavailable"):
        _legacy_checkpoint_accept(service, actor, boundary["id"], disposition)
    assert service.get_work(work["id"])["state"] == "waiting_supervisor"
    assert service.get_work(work["id"])["purpose_continuations"] == []
    assert (
        service.db.fetchone(
            "SELECT id FROM boundary_dispositions WHERE boundary_id=?", (boundary["id"],)
        )
        is None
    )


def test_achievement_requires_all_fixed_purpose_evidence_and_does_not_finish_worker(
    system, tmp_path
):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path, finite=True)
    continuation = _pending(service, work)
    request = dict(
        continuation_id=continuation["id"],
        expected_generation=1,
        expected_purpose_version=2,
        kind="achieved",
        reason="The entrusted outcome is verified.",
        evidence="All fixed conditions have direct verification.",
        idempotency_key="achieved",
    )
    with pytest.raises(ValidationError, match="every fixed purpose criterion"):
        service.record_purpose_next_action(actor, PurposeNextActionInput(**request))
    purpose = service.get_work(work["id"])["worker_purpose"]
    request["criterion_assessments"] = [
        {"criterion_id": item["id"], "evidence": "Direct verified result for this condition."}
        for item in purpose["contract"]["criteria"]
    ]
    result = service.record_purpose_next_action(actor, PurposeNextActionInput(**request))
    assert result["state"] == "achieved"
    assert service.record_purpose_next_action(actor, PurposeNextActionInput(**request)) == result
    assert (
        service.db.fetchone("SELECT state FROM managed_worker_threads WHERE id=?", (thread,))[0]
        == "active"
    )
    assert service.reconcile_cao_supervision_obligations() == 0


def test_observation_wait_rearms_once_when_due_without_requester_approval(
    system, tmp_path, monkeypatch
):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    request = PurposeNextActionInput(
        continuation_id=continuation["id"],
        expected_generation=1,
        expected_purpose_version=1,
        kind="wait_until",
        reason="The measurement interval must elapse.",
        evidence="The initial observation is preserved.",
        resume_at="2100-01-01T00:00:00Z",
        resume_condition="Compare the next observation with the preserved baseline.",
        idempotency_key="wait",
    )
    assert service.record_purpose_next_action(actor, request)["state"] == "waiting"
    assert service.reconcile_cao_supervision_obligations() == 0
    monkeypatch.setattr(purpose_continuation, "utc_now", lambda: "2101-01-01T00:00:00Z")
    assert service.reconcile_cao_supervision_obligations() == 1
    assert service.reconcile_cao_supervision_obligations() == 0
    assert _pending(service, work)["state"] == "pending"
    assert _pending(service, work)["generation"] == 3


def test_requester_wait_requires_concrete_input_and_another_attachment_cannot_decide(
    system, tmp_path
):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    request = PurposeNextActionInput(
        continuation_id=continuation["id"],
        expected_generation=1,
        expected_purpose_version=1,
        kind="wait_user",
        reason="An additional resource is outside the current budget.",
        evidence="The existing authorized budget is exhausted.",
        idempotency_key="input",
    )
    with pytest.raises(ValidationError, match="exact missing requester input"):
        service.record_purpose_next_action(actor, request)
    other = _attached(service, suffix="foreign-next")
    with pytest.raises(AuthorizationError):
        service.record_purpose_next_action(other, request)
    ready = request.model_copy(
        update={
            "requester_input_kind": "budget",
            "resume_condition": "Provide the specific additional budget.",
        }
    )
    assert service.record_purpose_next_action(actor, ready)["state"] == "user_needed"
    assert service.reconcile_cao_supervision_obligations() == 0


def test_worker_finish_and_resume_do_not_complete_or_consume_the_purpose(system, tmp_path):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    wake = _wake(service, _pending(service, work))
    service.finish_worker_thread(
        actor, WorkerThreadLifecycleInput(worker_thread_id=thread, idempotency_key="finish")
    )
    assert _pending(service, work)["state"] == "pending"
    assert (
        service.db.fetchone("SELECT state FROM managed_worker_threads WHERE id=?", (thread,))[0]
        == "archived"
    )
    service.resume_worker_thread(
        actor, ResumeWorkerThreadInput(worker_thread_id=thread, idempotency_key="resume")
    )
    assert _pending(service, work)["state"] == "pending"
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id=?", (wake["id"],)
        )[0]
        == "queued"
    )
    assert service.reconcile_cao_supervision_obligations() == 0


def test_missing_wake_is_replaced_but_unknown_transport_is_not_replayed(system, tmp_path):
    service, _, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    wake = _wake(service, continuation)
    service.db.execute("DELETE FROM message_deliveries WHERE message_id=?", (wake["id"],))
    assert service.reconcile_cao_supervision_obligations() == 1
    successor = _wake(service, continuation)
    assert successor["id"] != wake["id"]
    service.db.execute(
        "UPDATE message_deliveries SET state='dispatched' WHERE message_id=?", (successor["id"],)
    )
    assert service.reconcile_cao_supervision_obligations() == 0
    assert _wake(service, continuation)["id"] == successor["id"]


def test_proven_incomplete_native_turn_rearms_the_same_obligation_once(system, tmp_path):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    wake = _wake(service, continuation)
    service.db.execute(
        "UPDATE message_deliveries SET state='delivered' WHERE message_id=?", (wake["id"],)
    )
    from cao_control_plane.delivery_lane import codex_delivery_client_id

    proof = dict(
        message_id=wake["id"],
        attachment_id=actor["_cao_attachment_id"],
        delivery_generation=1,
        native_thread_id=service.get_cao_attachment(actor["_cao_attachment_id"])[
            "native_thread_id"
        ],
        client_user_message_id=codex_delivery_client_id(wake["id"]),
        native_turn_id="exact-next-turn",
        terminal_status="completed",
        started_at=int(datetime.now(UTC).timestamp()) - 5,
        completed_at=int(datetime.now(UTC).timestamp()) - 1,
    )
    assert service.reconcile_incomplete_cao_provider_turn(**proof) == "scheduled"
    assert service.reconcile_incomplete_cao_provider_turn(**proof) == "not_required"
    assert (
        json.loads(
            service.db.fetchone(
                "SELECT payload_json FROM messages WHERE id=?",
                (_wake(service, continuation)["id"],),
            )[0]
        )["purpose_continuation_id"]
        == continuation["id"]
    )


def test_conversation_catalog_and_work_projection_expose_continuation_without_private_authority(
    system, tmp_path
):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    assert "cao_record_purpose_next_action" in {
        tool["name"] for tool in conversation_server_tools()
    }
    projected = MCPServer(service).call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})
    continuation = projected["purpose_continuations"][0]
    assert continuation["state"] == "pending"
    assert not {"attachment_id", "supervisor_id", "operator_scope"} & continuation.keys()


def test_purpose_revision_fences_old_continuation_without_creating_work(system, tmp_path):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    purpose = service.get_work(work["id"])["worker_purpose"]
    service.set_worker_purpose(
        actor,
        SetWorkerPurposeInput(
            worker_thread_id=thread,
            purpose="Preserve the revised entrusted service outcome.",
            contract=purpose["contract"],
            expected_purpose_version=1,
            reason="The requester explicitly revised the business direction.",
            idempotency_key="revision",
        ),
    )
    continuations = service.get_work(work["id"])["purpose_continuations"]
    assert (
        next(item for item in continuations if item["id"] == continuation["id"])["state"]
        == "superseded"
    )
    assert _pending(service, work)["state"] == "pending"
    assert _pending(service, work)["purpose_version"] == 2
    with pytest.raises(ConflictError):
        service.instruct_worker_thread(actor, _next_request(thread, continuation))
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
        )[0]
        == 1
    )
    assert service.reconcile_cao_supervision_obligations() == 0
    revised = _pending(service, work)
    next_work = service.instruct_worker_thread(
        actor,
        _next_request(thread, revised).model_copy(update={"expected_purpose_version": 2}),
    )
    assert _pending(service, work)["state"] == "assigned"
    assert _pending(service, work)["next_work_item_id"] == next_work["task"]["work_item_id"]


def test_a_previously_committed_successor_is_linked_without_another_assignment(system, tmp_path):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    request = _next_request(thread, continuation).model_copy(
        update={
            "purpose_continuation_id": None,
            "expected_continuation_generation": None,
        }
    )
    successor = service.instruct_worker_thread(actor, request)
    assert _pending(service, work)["state"] == "pending"
    result = service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=continuation["id"],
            expected_generation=1,
            expected_purpose_version=1,
            kind="continue",
            next_work_item_id=successor["task"]["work_item_id"],
            reason="The distinct next Work already committed before interruption.",
            evidence="The exact current Work and sealed purpose were reread.",
            idempotency_key="recover-link",
        ),
    )
    assert result["state"] == "assigned"
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
        )[0]
        == 2
    )


def test_a_truly_missing_legacy_checkpoint_is_recovered_once(system, tmp_path):
    with patch.object(purpose_continuation, "require_tx", return_value=None):
        service, _, _, work, _, _ = _checkpoint(system, tmp_path)
    assert service.get_work(work["id"])["purpose_continuations"] == []
    assert service.reconcile_cao_supervision_obligations() == 1
    restored = service.get_work(work["id"])["purpose_continuations"]
    assert len(restored) == 1 and restored[0]["state"] == "pending"
    assert service.reconcile_cao_supervision_obligations() == 0
    assert service.get_work(work["id"])["purpose_continuations"] == restored


def _same_project_conversation(service, actor):
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="retained-checkpoint-other-conversation",
            project_digest=actor["_cao_project_digest"],
        ),
        peer=ProcessIdentity(
            pid=1_710_200_001, parent_pid=1, start_signature="retained-checkpoint-other-peer"
        ),
    )
    return service.authenticate(str(attachment["context_token"]))


@pytest.mark.parametrize("active_work", [False, True])
def test_another_conversation_revision_does_not_recover_an_old_purpose_checkpoint(
    system, tmp_path, active_work
):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    other = _same_project_conversation(service, actor)
    purpose = service.get_work(work["id"])["worker_purpose"]
    service.set_worker_purpose(
        other,
        SetWorkerPurposeInput(
            worker_thread_id=thread,
            purpose="Deliver the newly entrusted service outcome.",
            contract=purpose["contract"],
            expected_purpose_version=1,
            reason="The requester assigned a new outcome in this conversation.",
            idempotency_key="other-revision",
        ),
    )
    if active_work:
        service.instruct_worker_thread(
            other,
            InstructWorkerThreadInput(
                worker_thread_id=thread,
                expected_purpose_version=2,
                instruction="Implement the newly entrusted bounded outcome.",
                completion_contract="no_artifact_expected",
                idempotency_key="other-current-work",
            ),
        )
    before = service.get_work(work["id"])["purpose_continuations"]
    assert before and all(value["state"] == "superseded" for value in before)
    assert service.reconcile_cao_supervision_obligations() == 0
    assert service.get_work(work["id"])["purpose_continuations"] == before
    assert service.reconcile_cao_supervision_obligations() == 0


def test_other_conversation_current_purpose_checkpoint_still_recovers(system, tmp_path):
    workers = []
    service, actor, thread, _, _, _ = _checkpoint(system, tmp_path, worker_out=workers)
    other = _same_project_conversation(service, actor)
    assigned = service.instruct_worker_thread(
        other,
        InstructWorkerThreadInput(
            worker_thread_id=thread,
            expected_purpose_version=1,
            instruction="Produce a distinct checkpoint within the existing purpose.",
            completion_contract="no_artifact_expected",
            idempotency_key="other-checkpoint",
        ),
    )
    with patch.object(purpose_continuation, "require_tx", return_value=None):
        work = _accept_successor(service, other, workers[0], assigned, purpose_version=1)
    assert service.get_work(work["id"])["purpose_continuations"] == []
    assert service.reconcile_cao_supervision_obligations() == 1
    [restored] = service.get_work(work["id"])["purpose_continuations"]
    assert restored["state"] == "pending" and restored["purpose_version"] == 1
    assert service.reconcile_cao_supervision_obligations() == 0


def test_cross_worker_successor_does_not_recreate_an_assigned_checkpoint(system, tmp_path):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    target = _new_thread(service, actor, tmp_path, suffix="active-handoff-next")
    successor = service.instruct_worker_thread(
        actor,
        _next_request(target, continuation).model_copy(
            update={"purpose_continuation_id": None, "expected_continuation_generation": None}
        ),
    )
    recorded = service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=continuation["id"],
            expected_generation=1,
            expected_purpose_version=1,
            kind="continue",
            next_work_item_id=successor["task"]["work_item_id"],
            reason="The next outcome is assigned to another Worker with the same purpose.",
            evidence="The sealed purposes and fixed contracts match.",
            idempotency_key="active-handoff-link",
        ),
    )
    messages = service.db.fetchone("SELECT COUNT(*) FROM messages")[0]
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    assert restarted.reconcile_cao_supervision_obligations() == 0
    assert restarted.reconcile_cao_supervision_obligations() == 0
    assert restarted.get_work(work["id"])["purpose_continuations"] == [recorded]
    assert service.db.fetchone("SELECT COUNT(*) FROM messages")[0] == messages
    assert service.db.fetchone("SELECT COUNT(*) FROM work_items")[0] == 2
    assert verify_projection(service.db).healthy


@pytest.mark.parametrize("terminal_operation", ["finish_worker_thread", "delete_worker_thread"])
def test_exact_purpose_handoff_can_link_another_worker_without_forcing_reuse(
    system, tmp_path, terminal_operation
):
    service, actor, source, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    getattr(service, terminal_operation)(
        actor, WorkerThreadLifecycleInput(worker_thread_id=source, idempotency_key="finish-source")
    )
    assert _pending(service, work)["state"] == "pending"
    target = _new_thread(service, actor, tmp_path, suffix="handoff-next")
    successor = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=target,
            expected_purpose_version=1,
            instruction="Continue the preserved service purpose with the next bounded outcome.",
            idempotency_key="handoff-next-work",
        ),
    )
    result = service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=continuation["id"],
            expected_generation=1,
            expected_purpose_version=1,
            kind="continue",
            next_work_item_id=successor["task"]["work_item_id"],
            reason="This exact purpose and authority were explicitly preserved on the successor.",
            evidence="Both sealed purposes and complete fixed contracts match.",
            idempotency_key="handoff-link",
        ),
    )
    assert result["state"] == "assigned"


def test_live_wake_on_one_purpose_does_not_starve_another_missing_wake(system, tmp_path):
    service, _, _, first, _, _ = _checkpoint(system, tmp_path, suffix="first-next")
    _, _, _, second, _, _ = _checkpoint(system, tmp_path, suffix="second-next")
    first_wake = _wake(service, _pending(service, first))
    second_wake = _wake(service, _pending(service, second))
    service.db.execute("DELETE FROM message_deliveries WHERE message_id=?", (second_wake["id"],))
    assert service.reconcile_cao_supervision_obligations(limit=1) == 1
    assert _wake(service, _pending(service, first))["id"] == first_wake["id"]
    assert _wake(service, _pending(service, second))["id"] != second_wake["id"]


def test_reconnect_reactivates_only_typed_retryable_purpose_wakes(system, tmp_path):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    wake = _wake(service, continuation)
    runtime_id = service.get_cao_attachment(actor["_cao_attachment_id"])["runtime_session_id"]
    service.db.execute(
        "UPDATE message_deliveries SET state='dead', reactivation_policy='terminal' WHERE message_id=?",
        (wake["id"],),
    )
    with service.db.transaction() as connection:
        assert (
            purpose_continuation.reactivate_tx(
                service,
                connection,
                attachment_id=actor["_cao_attachment_id"],
                runtime_id=runtime_id,
            )
            == 0
        )
    service.db.execute(
        "UPDATE message_deliveries SET reactivation_policy='retryable' WHERE message_id=?",
        (wake["id"],),
    )
    with service.db.transaction() as connection:
        assert (
            purpose_continuation.reactivate_tx(
                service,
                connection,
                attachment_id=actor["_cao_attachment_id"],
                runtime_id=runtime_id,
            )
            == 1
        )
        assert (
            purpose_continuation.reactivate_tx(
                service,
                connection,
                attachment_id=actor["_cao_attachment_id"],
                runtime_id=runtime_id,
            )
            == 0
        )


def test_dashboard_keeps_next_decision_visible_after_the_work_boundary_is_resolved(
    system, tmp_path
):
    from cao_control_plane.dashboard import DashboardReadModel

    service, _, _, _, _, _ = _checkpoint(system, tmp_path)
    snapshot = DashboardReadModel(service).snapshot()
    workers = snapshot["operator"]["cao_processing"]
    assert any(worker["purpose_supervision_state"] == "pending" for worker in workers)


def test_conversation_close_fences_pending_continuation_without_finishing_worker(system, tmp_path):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    result = service.close_cao_conversation(
        actor, CloseCAOConversationInput(idempotency_key="close-next")
    )
    assert result["status"] == "closed"
    assert _pending(service, work)["state"] == "stopped"
    assert (
        service.db.fetchone("SELECT state FROM managed_worker_threads WHERE id=?", (thread,))[0]
        == "active"
    )
    assert service.reconcile_cao_supervision_obligations() == 0


def test_next_action_commit_can_be_confirmed_by_repeated_reads(system, tmp_path):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    server = MCPServer(service)
    current = server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})
    continuation = current["purpose_continuations"][0]
    request = dict(
        continuation_id=continuation["id"],
        expected_generation=1,
        expected_purpose_version=1,
        kind="wait_until",
        reason="Wait for the next observation.",
        evidence="The baseline is preserved.",
        resume_at="2100-01-01T00:00:00Z",
        resume_condition="Read the scheduled observation.",
        idempotency_key="read-after-action",
    )
    server.call_tool(actor, "cao_record_purpose_next_action", request)
    assert (
        server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})[
            "purpose_continuations"
        ][0]["state"]
        == "waiting"
    )
    server.call_tool(actor, "cao_record_purpose_next_action", request)
    assert server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})[
        "purpose_continuations"
    ][0]["state"] == "waiting"


def test_fractional_observation_time_does_not_resume_before_its_deadline(
    system, tmp_path, monkeypatch
):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=continuation["id"],
            expected_generation=1,
            expected_purpose_version=1,
            kind="wait_until",
            reason="Wait for the full measurement interval.",
            evidence="The initial observation is preserved.",
            resume_at="2100-01-01T00:00:00.500000Z",
            resume_condition="Collect the next measurement.",
            idempotency_key="fractional-wait",
        ),
    )
    monkeypatch.setattr(purpose_continuation, "utc_now", lambda: "2100-01-01T00:00:00Z")
    assert service.reconcile_cao_supervision_obligations() == 0
    monkeypatch.setattr(purpose_continuation, "utc_now", lambda: "2100-01-01T00:00:01Z")
    assert service.reconcile_cao_supervision_obligations() == 1


def test_requester_pause_fences_assignments_until_reviewed_resumption(system, tmp_path):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    direction = ["Pause this entrusted purpose until I explicitly resume it."]
    service.instruction_review_source = lambda _: [{"role": "requester", "text": direction[0]}]
    paused = service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=continuation["id"],
            expected_generation=1,
            expected_purpose_version=1,
            kind="pause",
            reason="The requester explicitly paused the purpose.",
            evidence=direction[0],
            idempotency_key="purpose-pause",
        ),
    )
    assert paused["state"] == "paused"
    assert service.list_managed_workers(actor)[0]["purpose_supervision"]["state"] == "paused"
    plain = _next_request(thread, continuation).model_copy(
        update={
            "purpose_continuation_id": None,
            "expected_continuation_generation": None,
        }
    )
    with pytest.raises(ConflictError, match="explicitly paused"):
        service.instruct_worker_thread(actor, plain)
    with pytest.raises(ControlPlaneError, match="requester-directed resumption") as refused:
        MCPServer(service).call_tool(
            actor, "cao_instruct_worker_thread", plain.model_dump(mode="json", exclude_none=True)
        )
    assert refused.value.details == {
        "reason_code": "worker_purpose_paused", "retryable": False
    }
    current = service.get_work(work["id"])
    with pytest.raises(ConflictError, match="explicitly paused"):
        service.assign_work(
            actor,
            WorkAssignment(
                expected_purpose_version=1,
                worker_id=current["assigned_worker_id"],
                runtime_session_id=current["current_attempt"]["runtime_session_id"],
                managed_worker_thread_id=thread,
                managed_worker_thread_generation=1,
                title="A blocked assignment",
                objective="Do not escape the purpose pause.",
                acceptance=["Return only the declared bounded outcome."],
                completion_contract="no_artifact_expected",
            ),
        )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
        )[0]
        == 1
    )
    assert service.reconcile_cao_supervision_obligations() == 0
    direction[0] = "Resume this same purpose within its existing authority and budget."
    resumed = service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=continuation["id"],
            expected_generation=paused["generation"],
            expected_purpose_version=1,
            kind="resume",
            reason="The requester explicitly resumed the purpose.",
            evidence=direction[0],
            idempotency_key="purpose-resume",
        ),
    )
    assert resumed["state"] == "pending" and resumed["generation"] == 3
    next_work = service.instruct_worker_thread(actor, _next_request(thread, resumed))
    assert _pending(service, work)["next_work_item_id"] == next_work["task"]["work_item_id"]


@pytest.mark.parametrize("kind", ["pause", "achieved", "wait_until", "wait_user"])
def test_cao_only_purpose_decisions_do_not_invoke_the_instruction_checker(system, tmp_path, kind):
    from types import SimpleNamespace

    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    purpose = service.get_work(work["id"])["worker_purpose"]
    def unexpected(_):
        raise AssertionError("CAO-only decisions are not Worker instructions")
    service.instruction_reviewer = SimpleNamespace(review=unexpected)
    request = PurposeNextActionInput(
        continuation_id=continuation["id"],
        expected_generation=1,
        expected_purpose_version=1,
        kind=kind,
        reason="CAO determined the next action against the registered purpose.",
        evidence="The requester direction, fixed criteria and current blocker were reviewed by CAO.",
        resume_at="2099-01-01T00:00:00Z" if kind == "wait_until" else None,
        resume_condition="The declared required observation or missing business direction is available."
        if kind in {"wait_until", "wait_user"}
        else "",
        requester_input_kind="business_direction" if kind == "wait_user" else None,
        criterion_assessments=(
            [
                {"criterion_id": criterion["id"], "evidence": "CAO verified this fixed purpose condition."}
                for criterion in purpose["contract"]["criteria"]
            ]
            if kind == "achieved"
            else []
        ),
        idempotency_key=f"unjustified-{kind}",
    )
    result = service.record_purpose_next_action(actor, request)
    assert result["state"] != "pending"
    assert _pending(service, work)["generation"] == 2


def test_purpose_notification_remains_readable_after_reconciliation(
    system, tmp_path
):
    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    server = MCPServer(service)
    current = server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})
    wake = _wake(service, current["purpose_continuations"][0])
    service.db.execute("DELETE FROM message_deliveries WHERE message_id=?", (wake["id"],))
    assert service.reconcile_cao_supervision_obligations() == 1
    assert server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})["id"] == work["id"]
    received = _wake(service, current["purpose_continuations"][0])
    assert received["id"]
    assert server.call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})["id"] == work["id"]


def _accept_successor(service, actor, worker, assigned, *, purpose_version):
    work = service.get_work(assigned["task"]["work_item_id"])
    attempt = work["current_attempt"]
    reported = service.report(
        worker,
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            summary="The second distinct bounded result is verified.",
            expected_goal_version=work["goal_version"],
            expected_generation=work["generation"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            evidence=[{"result": "second verified checkpoint"}],
            idempotency_key="second-report",
        ),
    )
    boundary = reported["open_boundaries"][0]
    service.review(
        actor,
        ReviewInput(
            attempt_id=attempt["id"],
            expected_purpose_version=purpose_version,
            verdict="ok",
            summary="The second frozen checkpoint is independently verified.",
            criterion_assessments=[
                {
                    "criterion_id": "task_instruction",
                    "verdict": "met",
                    "evidence": "The declared second outcome was independently checked.",
                }
            ],
            idempotency_key="second-review",
        ),
    )
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="second-turn",
    )
    _legacy_checkpoint_accept(
        service, actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=work["generation"],
            expected_purpose_version=purpose_version,
            kind="accept",
            reason="Accept the independently verified second checkpoint.",
        ),
    )
    return work


@pytest.mark.parametrize("operation", ["finish_worker_thread", "delete_worker_thread"])
def test_ended_unaccepted_successor_returns_the_next_decision_without_replaying_it(
    system, tmp_path, operation
):
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path)
    source = _pending(service, work)
    successor = service.instruct_worker_thread(actor, _next_request(thread, source))
    next_id = successor["task"]["work_item_id"]
    delivery = service.db.fetchone(
        "SELECT delivery.* FROM message_deliveries AS delivery JOIN messages AS message "
        "ON message.id=delivery.message_id WHERE message.work_item_id=? AND message.kind='assignment'",
        (next_id,),
    )
    assert delivery is not None
    service.db.execute(
        "UPDATE message_deliveries SET state='dispatched' WHERE message_id=?",
        (delivery["message_id"],),
    )
    unknown = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id=?", (delivery["message_id"],)
        )
    )
    getattr(service, operation)(
        actor,
        WorkerThreadLifecycleInput(worker_thread_id=thread, idempotency_key="end-next-worker"),
    )
    assert service.reconcile_cao_supervision_obligations() == 1
    current = _pending(service, work)
    assert current["id"] == source["id"] and current["generation"] == 3
    assert current["state"] == "pending" and current["prior_next_work_item_id"] == next_id
    assert service.get_work(next_id)["state"] == "canceled"
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id=?", (delivery["message_id"],)
            )
        )
        == unknown
    )
    assert service.reconcile_cao_supervision_obligations() == 0
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
        )[0]
        == 2
    )


def test_accepted_successor_owns_continuity_when_the_worker_is_later_finished(system, tmp_path):
    actors = []
    service, actor, thread, work, _, _ = _checkpoint(system, tmp_path, worker_out=actors)
    source = _pending(service, work)
    assigned = service.instruct_worker_thread(actor, _next_request(thread, source))
    next_work = _accept_successor(service, actor, actors[0], assigned, purpose_version=1)
    service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(worker_thread_id=thread, idempotency_key="finish-accepted-next"),
    )
    assert service.reconcile_cao_supervision_obligations() == 0
    assert _pending(service, work)["state"] == "assigned"
    assert _pending(service, next_work)["state"] == "pending"


def test_verified_purpose_achievement_resolves_other_pending_checkpoints(system, tmp_path):
    actors = []
    service, actor, thread, work, _, _ = _checkpoint(
        system, tmp_path, finite=True, worker_out=actors
    )
    first = _pending(service, work)
    first_wake = _wake(service, first)
    service.acknowledge(actor, AckInput(message_ids=[first_wake["id"]]))
    assigned = service.instruct_worker_thread(
        actor,
        _next_request(thread, first).model_copy(
            update={
                "expected_purpose_version": 2,
                "purpose_continuation_id": None,
                "expected_continuation_generation": None,
            }
        ),
    )
    next_work = _accept_successor(service, actor, actors[0], assigned, purpose_version=2)
    final = _pending(service, next_work)
    purpose = service.get_work(work["id"])["worker_purpose"]
    result = service.record_purpose_next_action(
        actor,
        PurposeNextActionInput(
            continuation_id=final["id"],
            expected_generation=1,
            expected_purpose_version=2,
            kind="achieved",
            reason="Both supporting checkpoints verify the fixed finite outcome.",
            evidence="The independently verified frozen results satisfy the finite boundary.",
            criterion_assessments=[
                {
                    "criterion_id": item["id"],
                    "evidence": "Both independently accepted supporting results.",
                }
                for item in purpose["contract"]["criteria"]
            ],
            idempotency_key="finite-global-achievement",
        ),
    )
    assert result["state"] == "achieved"
    assert _pending(service, work)["state"] == "achieved"
    assert _pending(service, work)["purpose_achievement_id"] == final["id"]
    service.mark_message_handled(
        actor,
        first_wake["id"],
        evidence="The same fixed purpose was independently verified as achieved.",
        expected_purpose_version=2,
    )
    assert service.list_managed_workers(actor)[0]["purpose_supervision"]["state"] == "achieved"
    with pytest.raises(ConflictError) as caught:
        service.instruct_worker_thread(
            actor,
            _next_request(thread, first, key="after-achievement").model_copy(
                update={
                    "expected_purpose_version": 2,
                    "purpose_continuation_id": None,
                    "expected_continuation_generation": None,
                }
            ),
        )
    assert caught.value.details["reason_code"] == "worker_purpose_achieved"
    with pytest.raises(ControlPlaneError, match="requester-directed new purpose") as refused:
        MCPServer(service).call_tool(
            actor,
            "cao_instruct_worker_thread",
            {
                "worker_thread_id": thread,
                "expected_purpose_version": 2,
                "instruction": "Record a distinct bounded outcome.",
                "idempotency_key": "public-after-achievement",
            },
        )
    assert refused.value.details == {
        "reason_code": "worker_purpose_achieved", "retryable": False
    }
    assert service.db.fetchone(
        "SELECT COUNT(*) FROM work_items WHERE managed_worker_thread_id=?", (thread,)
    )[0] == 2
    assert service.reconcile_cao_supervision_obligations() == 0


@pytest.mark.parametrize("changed", ["requester_input", "system_repair"])
def test_unchanged_next_failure_stays_durable_without_wakes_and_changed_input_resumes(system, tmp_path, changed):
    from cao_control_plane.delivery_lane import codex_delivery_client_id
    from cao_control_plane.instruction_review import requester_condition_digest

    service, actor, _, work, _, _ = _checkpoint(system, tmp_path)
    continuation = _pending(service, work)
    # Native observation is independent of instruction alignment. Seed the
    # already observed input, rather than relying on a former AI audit to read it.
    initial_source = service.instruction_review_source("owned-fixture")
    with service.db.transaction() as connection:
        service._event(connection, "cao.requester_source_observed", "cao_attachment",
            actor["_cao_attachment_id"], "", {"source_condition_digest": requester_condition_digest(initial_source),
                "source_status": "available"})


    def finish_notice():
        wake = _wake(service, continuation)
        service.db.execute("UPDATE message_deliveries SET state='delivered' WHERE message_id=?", (wake['id'],))
        proof = dict(message_id=wake['id'], attachment_id=actor['_cao_attachment_id'], delivery_generation=1,
                     native_thread_id=service.get_cao_attachment(actor['_cao_attachment_id'])['native_thread_id'],
                     client_user_message_id=codex_delivery_client_id(wake['id']), native_turn_id='turn-' + wake['id'],
                     terminal_status='completed', started_at=int(datetime.now(UTC).timestamp())-5,
                     completed_at=int(datetime.now(UTC).timestamp())-1)
        return service.reconcile_incomplete_cao_provider_turn(**proof)

    assert finish_notice() == 'scheduled'
    assert finish_notice() == 'not_required'
    count = service.db.fetchone('SELECT COUNT(*) AS n FROM messages')['n']
    candidate = purpose_continuation.pending_recovery_probes(service)[0]
    source = service.instruction_review_source(candidate['native_thread_id'])
    for _ in range(30):
        assert service.reconcile_cao_supervision_obligations() == 0
        assert purpose_continuation.record_recovery_probe(service, candidate,
            source_digest=requester_condition_digest(source), source_status='available') == 0
    assert service.db.fetchone('SELECT COUNT(*) AS n FROM messages')['n'] == count
    pending = _pending(service, work)
    assert pending['state'] == 'pending'
    assert pending['technical_recovery']['status'] == 'waiting_dependency_change'
    resumed_source = source
    if changed == 'system_repair':
        service._purpose_recovery_release_id = 'corrected-executable-release'
    else:
        resumed_source = [{'role': 'requester', 'text': 'A concrete new input.'}]
    assert purpose_continuation.record_recovery_probe(service, candidate,
        source_digest=requester_condition_digest(resumed_source),
        source_status='available') == 1
    assert service.db.fetchone('SELECT COUNT(*) AS n FROM messages')['n'] == count + 1
    assert purpose_continuation.record_recovery_probe(service, candidate,
        source_digest=requester_condition_digest(resumed_source),
        source_status='available') == 0
    assert _pending(service, work)['state'] == 'pending'
