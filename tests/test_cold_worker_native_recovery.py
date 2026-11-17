from __future__ import annotations

import asyncio
import json

import pytest
from test_managed_worker_command_admission import _attached, _new_thread, _thread_route
from test_worker_dispatch_recovery import _capture

from cao_control_plane.models import (
    BoundaryDispositionInput,
    InstructWorkerThreadInput,
    RuntimeDispatchResult,
    WorkAssignment,
)
from cao_control_plane.runtime import Dispatcher
from cao_control_plane.worker_output import WorkerOutputEvent


@pytest.mark.parametrize("submitted", [False, True])
def test_startup_binding_requires_submission_and_preserves_unknown_context(system, submitted):
    service, ids, _work, values, _ticket = _capture(system)
    service.db.execute("UPDATE runtime_sessions SET native_session_id='' WHERE id=?", (ids["runtime_id"],))
    output = service.observe_worker_output(**values, event=WorkerOutputEvent(
        native_thread_id="private-unpersisted-native", turn_id="", item_id="", kind="turn_end",
        status="failed", complete=False, bind_native_thread=submitted))
    assert service.get_runtime(ids["runtime_id"])["native_session_id"] == ("private-unpersisted-native" if submitted else "")
    assert output["kind"] == "turn_end" and output["capture_state"] == "empty"
    receipt = service.db.fetchone("SELECT source_thread_digest FROM worker_output_receipts WHERE id=?", (output["id"],))
    assert receipt["source_thread_digest"]  # Retain the observation as evidence, not a resumable handle.


def _failed_cold_thread(system, tmp_path):
    service = system["service"]
    actor = _attached(service, suffix="cold-recovery")
    worker = _new_thread(service, actor, tmp_path, suffix="cold-recovery")
    route = _thread_route(service, worker)
    instructed = service.instruct_worker_thread(actor, InstructWorkerThreadInput(
        worker_thread_id=worker, expected_purpose_version=1,
        instruction="Complete the finite local verification checkpoint.", idempotency_key="cold-first-input"))
    work = service.get_work(instructed["task"]["work_item_id"])
    # A retained pre-upgrade startup marker supplied this handle. The provider
    # later rejects it authoritatively before accepting any model input.
    service.db.execute("UPDATE runtime_sessions SET native_session_id=? WHERE id=?",
        ("00000000-0000-0000-0000-000000000001", route["runtime_session_id"]))

    class FailedAdapter:
        async def dispatch(self, _runtime, _message):
            return RuntimeDispatchResult(success=False, state="failed", error="runtime_thread_not_found", metadata={
                "failure_code": "runtime_thread_not_found", "dispatch_phase": "thread_binding",
                "delivery_acceptance": "not_submitted",
            })

    class Registry:
        def get(self, _name):
            return FailedAdapter()

    assert asyncio.run(Dispatcher(service, system["settings"], registry=Registry()).run_once()) == 1
    return service, actor, worker, route, work


@pytest.mark.parametrize("condition", [
    "cold", "provider_start", "unknown_delivery", "worker_report", "missing_origin",
    "effect", "started_output", "pending_ticket", "missing_receipt", "unrelated_unknown",
])
def test_missing_handle_replacement_requires_positive_never_submitted_history(system, tmp_path, condition):
    from cao_control_plane.database import _cold_native_handle_replacement_is_proven

    service, _actor, _worker, route, work = _failed_cold_thread(system, tmp_path)
    if condition == "provider_start":
        with service.db.transaction() as connection:
            service._event(connection, "attempt.provider_turn_started", "attempt", work["current_attempt"]["id"], "", {})
    elif condition == "unknown_delivery":
        service.db.execute("UPDATE message_deliveries SET state='dispatched' WHERE recipient_id=?", (route["principal_id"],))
    elif condition == "worker_report":
        service.db.execute("UPDATE messages SET sender_id=?,kind='progress' WHERE kind='system' AND attempt_id=?",
            (route["principal_id"], work["current_attempt"]["id"]))
    elif condition == "missing_origin":
        service.db.execute("DELETE FROM events WHERE event_type='managed_worker.created' AND aggregate_id=("
            "SELECT managed_spec_id FROM managed_worker_threads WHERE id=?)", (_worker,))
    elif condition == "effect":
        service.db.execute("INSERT INTO effect_operations(id,principal_id,kind,target,action,status,created_at,updated_at) "
            "VALUES('eff_cold_fixture',?,'external','opaque-target','opaque-action','unknown','2026-01-01','2026-01-01')",
            (route["principal_id"],))
    elif condition == "started_output":
        service.db.execute("UPDATE worker_output_streams SET source_turn_digest=? WHERE attempt_id=?",
            ("e" * 64, work["current_attempt"]["id"]))
    elif condition == "pending_ticket":
        service.db.execute("UPDATE runtime_enrollment_tickets SET state='pending' WHERE attempt_id=?", (work["current_attempt"]["id"],))
    elif condition == "missing_receipt":
        service.db.execute("DELETE FROM events WHERE event_type='runtime.message_not_submitted' AND aggregate_id IN "
            "(SELECT id FROM messages WHERE attempt_id=?)", (work["current_attempt"]["id"],))
    elif condition == "unrelated_unknown":
        other = service.assign_work(system["cao"], WorkAssignment(worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"], title="Independent fixture", objective="Preserve an unrelated unknown input.",
            acceptance=["The unknown input is retained without replay."]))
        service.db.execute("UPDATE message_deliveries SET state='dispatched' WHERE message_id IN "
            "(SELECT id FROM messages WHERE attempt_id=?)", (other["current_attempt"]["id"],))
    with service.db.connection_scope() as connection:
        assert _cold_native_handle_replacement_is_proven(connection,
            worker_id=route["principal_id"], runtime_id=route["runtime_session_id"]) is (condition in {"cold", "unrelated_unknown"})


def test_distinct_work_replaces_only_a_proven_unused_handle_and_preserves_old_evidence(system, tmp_path):
    service, actor, worker, route, work = _failed_cold_thread(system, tmp_path)
    old_runtime = service.get_runtime(route["runtime_session_id"])
    service.cancel_work(actor, work["id"], "The test conversation closed this exact checkpoint.", "cold-cancel")
    prior_delivery = dict(service.db.fetchone("SELECT d.* FROM message_deliveries d JOIN messages m ON m.id=d.message_id "
        "WHERE m.attempt_id=? AND m.kind='assignment'", (work["current_attempt"]["id"],)))
    successor = service.instruct_worker_thread(actor, InstructWorkerThreadInput(
        worker_thread_id=worker, expected_purpose_version=1,
        instruction="Complete the distinct next finite checkpoint within the same purpose.", idempotency_key="cold-next-input"))
    next_route = _thread_route(service, worker)
    assert next_route["runtime_session_id"] != route["runtime_session_id"]
    assert service.get_runtime(next_route["runtime_session_id"])["native_session_id"] == ""
    assert service.get_runtime(route["runtime_session_id"])["native_session_id"] == old_runtime["native_session_id"]
    assert successor["worker_thread_id"] == worker
    current = service.get_work(successor["task"]["work_item_id"])
    assert current["worker_purpose"]["version"] == 1
    assert dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=? AND recipient_id=?",
        (prior_delivery["message_id"], prior_delivery["recipient_id"]))) == prior_delivery
    event = service.db.fetchone("SELECT data_json FROM events WHERE event_type='managed_worker_thread.connection_advanced' "
        "AND aggregate_id=? ORDER BY sequence DESC LIMIT 1", (worker,))
    assert json.loads(event["data_json"])["unstarted_native_handle_replaced"] is True
    assert old_runtime["native_session_id"] not in event["data_json"]


def test_exact_cold_work_can_continue_on_a_fresh_handle_without_replaying_the_failed_assignment(system, tmp_path):
    service, actor, worker, route, work = _failed_cold_thread(system, tmp_path)
    current = service.get_work(work["id"])
    boundary = current["open_boundaries"][0]
    assert boundary["recovery_action"] == "dispose_continue_or_correct"
    turn = service.acquire_reasoner_turn(actor, work["id"], boundary_id=boundary["id"],
        expected_generation=current["generation"], idempotency_key="cold-decision")
    service.dispose_boundary(actor, boundary["id"], BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=current["generation"],
        expected_purpose_version=1, kind="continue", reason="No provider input was submitted on this unused handle.",
        instruction="Inspect the sealed checkpoint and complete only its unmet conditions."))
    next_route = _thread_route(service, worker)
    assert next_route["runtime_session_id"] != route["runtime_session_id"]
    assert service.get_runtime(next_route["runtime_session_id"])["native_session_id"] == ""
    assert service.get_work(work["id"])["current_attempt"]["attempt_number"] == 2
