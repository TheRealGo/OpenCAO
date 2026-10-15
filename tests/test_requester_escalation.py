from __future__ import annotations

import pytest
from test_worker_thread_lifecycle_core import _attached, _prepare_runtime_recovery

from cao_control_plane.dashboard import build_operator_view
from cao_control_plane.database import Database
from cao_control_plane.delivery_lane import codex_delivery_client_id
from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import AckInput, BoundaryDispositionInput
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime import render_message
from cao_control_plane.service import ControlPlane


@pytest.fixture
def escalation(system):
    service = system["service"]
    actor = _attached(system, suffix="escalation-notice")
    _, work, boundary, _ = _prepare_runtime_recovery(
        system, actor, ordinal=9729, reason="worker_inactive_timeout", acquire_turn=False,
    )
    turn = service.acquire_reasoner_turn(
        actor, work["id"], boundary_id=boundary["id"],
        expected_generation=work["generation"], idempotency_key="notice-decision",
    )
    request = BoundaryDispositionInput(
        expected_purpose_version=1, turn_id=turn["id"], lease_token=turn["lease_token"],
        expected_generation=work["generation"], kind="wait_user",
        reason="The supervisor cannot determine a safe solution.",
        instruction="Advise how to resolve the remaining uncertainty.",
        resume_condition="Guidance is incorporated and safe execution is verified.",
    )
    return service, actor, work, boundary, request


def _dispose(escalation):
    service, actor, work, boundary, request = escalation
    service.dispose_boundary(actor, boundary["id"], request)
    message_id = service.get_work(work["id"])["requester_escalation"]["message_id"]
    return next(item for item in service.get_inbox(actor)["items"] if item["id"] == message_id)


def test_notice_creation_and_escalation_are_atomic(escalation, monkeypatch):
    service, actor, work, boundary, request = escalation
    original = service._message

    def reject_notice(connection, **kwargs):
        if kwargs.get("payload", {}).get("action") == "requester_escalation":
            raise RuntimeError("notification persistence unavailable")
        return original(connection, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(service, "_message", reject_notice)
        with pytest.raises(RuntimeError, match="notification persistence"):
            service.dispose_boundary(actor, boundary["id"], request)
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["requester_escalation"] is None
    assert current["open_boundaries"][0]["id"] == boundary["id"]
    assert _dispose(escalation)["delivery_state"] == "queued"


def test_inbox_and_wake_require_visible_current_requester_communication(escalation):
    service, actor, _work, _boundary, request = escalation
    message = _dispose(escalation)
    public = MCPServer(service).call_tool(actor, "cao_get_inbox", {})
    notice = next(item for item in public["items"] if item["id"] == message["id"])
    assert notice["payload"]["action"] == "requester_escalation"
    assert notice["payload"]["decision"] == request.instruction
    assert notice["payload"]["resume_condition"] == request.resume_condition
    rendered = render_message(message)
    assert "tell the requester the unmet objective" in rendered
    assert "unknown solution, or system failure" in rendered
    assert "newer requester guidance already resolved" in rendered
    assert "Handling the notification never resolves" in rendered
    assert "unknown-effect fence" in rendered
    view = build_operator_view(build_projection(service.db).snapshot)
    consultation = next(
        item["requester_escalation"] for item in view["work_items"]
        if item.get("requester_escalation")
    )
    assert consultation["decision"] == request.instruction
    assert consultation["reason"] == request.reason
    assert consultation["notification_state"] == "queued"
    assert "boundary_id" not in consultation and "message_id" not in consultation


@pytest.mark.parametrize("terminal_status", ["completed", "failed", "interrupted"])
def test_consumed_but_unreported_notice_rearms_only_communication(escalation, terminal_status):
    service, actor, work, _boundary, _request = escalation
    message = _dispose(escalation)
    attachment = service.db.fetchone(
        "SELECT * FROM cao_session_attachments WHERE id = ?", (actor["_cao_attachment_id"],),
    )
    service.db.execute(
        "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?", (message["id"],),
    )
    arguments = dict(
        message_id=message["id"], attachment_id=attachment["id"], delivery_generation=1,
        native_thread_id=attachment["native_thread_id"],
        client_user_message_id=codex_delivery_client_id(message["id"]),
        native_turn_id="exact-notice-turn", terminal_status=terminal_status,
        started_at=100, completed_at=101,
    )
    reconcile = service.reconcile_incomplete_cao_provider_turn
    assert reconcile(**(arguments | {"native_thread_id": "another-thread"})) == "not_required"
    assert reconcile(**arguments) == "observed"
    assert reconcile(**arguments) == "scheduled"
    assert reconcile(**arguments) == "not_required"
    current = service.get_work(work["id"])
    assert current["state"] == "user_needed"
    assert current["current_attempt"]["id"] == work["current_attempt"]["id"]
    assert current["generation"] == work["generation"]
    assert current["requester_escalation"]["notification_state"] == "queued"
    successor = current["requester_escalation"]["message_id"]
    assert successor != message["id"]
    service.acknowledge(actor, AckInput(message_ids=[successor]))
    service.mark_message_handled(
        actor, successor, evidence="Requester was told the blocker and advice needed.",
        expected_purpose_version=1,
    )
    assert reconcile(**(arguments | {"message_id": successor,
        "client_user_message_id": codex_delivery_client_id(successor)})) == "not_required"
    assert service.get_work(work["id"])["state"] == "user_needed"


def test_failed_notice_transport_reactivates_on_exact_owner_reconnection(escalation):
    service, actor, work, _boundary, _request = escalation
    message = _dispose(escalation)
    attachment = service.db.fetchone(
        "SELECT * FROM cao_session_attachments WHERE id = ?", (actor["_cao_attachment_id"],),
    )
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dead', reactivation_policy = 'retryable' "
        "WHERE message_id = ?", (message["id"],),
    )
    with service.db.transaction() as connection:
        args = dict(attachment_id=attachment["id"], runtime_id=attachment["runtime_session_id"],
                    reason="authenticated-reconnection")
        assert service._reactivate_cao_supervision_deliveries_tx(connection, **args) == 1
        assert service._reactivate_cao_supervision_deliveries_tx(connection, **args) == 0
    current = service.get_work(work["id"])
    assert current["requester_escalation"]["notification_state"] == "queued"
    assert current["state"] == "user_needed"


@pytest.mark.parametrize("changed_proof", [False, True])
def test_proven_unstarted_failure_can_continue_after_requester_guidance(system, changed_proof):
    service = system["service"]
    actor = _attached(system, suffix="continued-escalation")
    _, work, boundary, _ = _prepare_runtime_recovery(
        system, actor, ordinal=9730, reason="runtime_dispatch_failed", acquire_turn=False,
    )
    turn = service.acquire_reasoner_turn(
        actor, work["id"], boundary_id=boundary["id"],
        expected_generation=work["generation"], idempotency_key="safe-notice-decision",
    )
    service.dispose_boundary(actor, boundary["id"], BoundaryDispositionInput(
        expected_purpose_version=1, turn_id=turn["id"], lease_token=turn["lease_token"],
        expected_generation=work["generation"], kind="wait_user",
        reason="The supervisor needs advice about the unavailable runtime.",
        instruction="Advise how the runtime can be restored.",
        resume_condition="The unstarted runtime is available for safe continuation.",
    ))
    restarted = ControlPlane(Database(system["settings"]), system["settings"])
    if changed_proof:
        restarted.db.execute(
            "UPDATE runtime_sessions SET native_session_id = 'unexplained-native-thread' WHERE id = ?",
            (work["current_attempt"]["runtime_session_id"],),
        )
        with pytest.raises(ConflictError):
            restarted.reply(
                actor, work["id"], "Continue after guidance.", expected_purpose_version=1,
                idempotency_key="stale-proof-answer",
            )
        assert restarted.get_work(work["id"])["state"] == "user_needed"
        return
    resumed = restarted.reply(
        actor, work["id"], "The prerequisite is restored; continue the original task.",
        expected_purpose_version=1, idempotency_key="safe-notice-answer",
    )
    assert resumed["state"] == "active"
    assert resumed["requester_escalation"] is None
    assert resumed["objective"] == work["objective"]


def test_failed_communication_recovery_remains_visible_without_another_model_loop(escalation):
    service, actor, work, _boundary, _request = escalation
    message = _dispose(escalation)
    attachment = service.db.fetchone(
        "SELECT * FROM cao_session_attachments WHERE id = ?", (actor["_cao_attachment_id"],),
    )
    for index in range(2):
        service.db.execute(
            "UPDATE message_deliveries SET state = 'delivered' WHERE message_id = ?", (message["id"],),
        )
        arguments = dict(
            message_id=message["id"], attachment_id=attachment["id"], delivery_generation=1,
            native_thread_id=attachment["native_thread_id"],
            client_user_message_id=codex_delivery_client_id(message["id"]),
            native_turn_id=f"failed-communication-{index}", terminal_status="completed",
            started_at=100 + index, completed_at=101 + index,
        )
        assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "observed"
        outcome = service.reconcile_incomplete_cao_provider_turn(**arguments)
        assert outcome == ("scheduled" if index == 0 else "not_required")
        current = service.get_work(work["id"])
        if index == 0:
            message = next(m for m in service.get_inbox(actor)["items"]
                if m["id"] == current["requester_escalation"]["message_id"])
    assert current["state"] == "user_needed"
    assert current["requester_escalation"]["notification_state"] == "dead"
    assert service.db.fetchone(
        "SELECT COUNT(*) AS n FROM messages WHERE work_item_id = ? "
        "AND json_extract(payload_json, '$.action') = 'requester_escalation'", (work["id"],),
    )["n"] == 2
    assert service.reconcile_cao_supervision_obligations() == 0
    assert service.recover_expired_reasoner_turns() == 0
    assert service.reconcile_incomplete_cao_provider_turn(**arguments) == "not_required"
