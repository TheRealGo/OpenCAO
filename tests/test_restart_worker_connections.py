from __future__ import annotations

import json

from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_managed_worker_command_admission import (
    _attached,
    _enroll_current_runtime,
    _instruct,
    _new_thread,
    _thread_route,
)

from cao_control_plane.models import CloseCAOConversationInput
from cao_control_plane.runtime import Dispatcher


def test_controlled_stop_retires_busy_connection_without_replaying_closed_work(system, tmp_path):
    service = system["service"]
    actor = _attached(service, suffix="restart")
    thread = _new_thread(service, actor, tmp_path, suffix="restart")
    assigned = _instruct(service, actor, thread, suffix="restart-old")
    old_work = assigned["task"]["work_item_id"]
    old_attempt = service.get_work(old_work)["current_attempt"]["id"]
    _, runtime = _enroll_current_runtime(service, thread, attempt_id=old_attempt)
    old_delivery = service.db.fetchone(
        "SELECT d.* FROM message_deliveries d JOIN messages m ON m.id=d.message_id "
        "WHERE m.attempt_id=? AND m.kind='assignment'", (old_attempt,),
    )
    # Model a submitted input when its owning conversation closes. Its effect
    # remains unknown even after the controlled restart fences transport.
    service.db.execute(
        "UPDATE message_deliveries SET state='acknowledged',owner_token='',"
        "acknowledged_at='2026-01-01T00:00:00Z' "
        "WHERE message_id=?", (old_delivery["message_id"],),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state='busy',lease_expires_at='2999-01-01T00:00:00Z' WHERE id=?",
        (runtime,),
    )
    service.close_cao_conversation(actor, CloseCAOConversationInput(idempotency_key="close-before-restart"))
    source = service.db.fetchone("SELECT project_digest FROM cao_session_attachments WHERE id=?",
                                (actor["_cao_attachment_id"],))

    def successor(name):
        attachment = attach_cao_session_with_peer(service, current_cao_session_attachment(
            native_thread_id=name, project_digest=source["project_digest"],
        ))
        return service.authenticate(attachment["context_token"])

    next_actor = successor("restart-before-fence")
    pending = _instruct(service, next_actor, thread, suffix="restart-pending")
    dispatcher = Dispatcher(service, system["settings"])
    assert dispatcher._claim_delivery() is None  # The pre-fix, future busy lease.
    service.close_cao_conversation(next_actor, CloseCAOConversationInput(idempotency_key="close-pending"))
    old_snapshot = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?",
                                          (old_delivery["message_id"],)))
    assert service._retire_worker_connections_after_shared_stop(restart_plan_digest="a" * 64) >= 1
    assert service._retire_worker_connections_after_shared_stop(restart_plan_digest="a" * 64) == 0
    assert dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?",
                                   (old_delivery["message_id"],))) == old_snapshot
    assert service.get_work(old_work)["state"] == "canceled"
    assert service.get_work(pending["task"]["work_item_id"])["state"] == "canceled"
    assert service.db.fetchone("SELECT state FROM runtime_sessions WHERE id=?", (runtime,))["state"] == "failed"
    assert not service.db.fetchone(
        "SELECT id FROM runtime_credentials WHERE enrollment_id=? AND state='active'",
        (_thread_route(service, thread)["enrollment_id"],),
    )
    final_actor = successor("restart-after-fence")
    final = _instruct(service, final_actor, thread, suffix="restart-after")
    route = _thread_route(service, thread)
    assert route["runtime_session_id"] != runtime
    assert route["thread_generation"] == 1
    assert service.get_work(final["task"]["work_item_id"])["worker_purpose"]["version"] == 1
    delivery = dispatcher._claim_delivery()
    assert delivery is not None and delivery["runtime_session_id"] == route["runtime_session_id"]
    event = service.db.fetchone("SELECT data_json FROM events WHERE event_type=? AND aggregate_id=?",
                               ("runtime.connection_retired_for_restart", runtime))
    assert json.loads(event["data_json"]) == {"restart_plan_digest": "a" * 64}


def test_restart_retirement_leaves_unmanaged_connections_unchanged(system):
    service = system["service"]
    runtime = system["runtime"]["id"]
    service.db.execute("UPDATE worker_enrollments SET managed=0 WHERE runtime_session_id=?", (runtime,))
    before = dict(service.db.fetchone("SELECT * FROM runtime_sessions WHERE id=?", (runtime,)))
    service._retire_worker_connections_after_shared_stop(restart_plan_digest="b" * 64)
    assert dict(service.db.fetchone("SELECT * FROM runtime_sessions WHERE id=?", (runtime,))) == before


def test_restart_preserves_submitted_unknown_delivery_and_opens_existing_recovery(system, tmp_path):
    service = system["service"]
    actor = _attached(service, suffix="unknown-restart")
    thread = _new_thread(service, actor, tmp_path, suffix="unknown-restart")
    assigned = _instruct(service, actor, thread, suffix="unknown-restart")
    work_id = assigned["task"]["work_item_id"]
    attempt = service.get_work(work_id)["current_attempt"]["id"]
    _, runtime = _enroll_current_runtime(service, thread, attempt_id=attempt)
    service.db.execute(
        "UPDATE message_deliveries SET state='dispatched',owner_token='prior-coordinator' "
        "WHERE message_id IN (SELECT id FROM messages WHERE attempt_id=? AND kind='assignment')",
        (attempt,),
    )
    service.db.execute("UPDATE runtime_sessions SET state='busy' WHERE id=?", (runtime,))
    before = dict(service.db.fetchone(
        "SELECT d.* FROM message_deliveries d JOIN messages m ON m.id=d.message_id "
        "WHERE m.attempt_id=? AND m.kind='assignment'", (attempt,),
    ))
    service._retire_worker_connections_after_shared_stop(restart_plan_digest="c" * 64)
    assert dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?",
                                   (before["message_id"],))) == before
    work = service.get_work(work_id)
    assert work["open_boundaries"]
    assert work["state"] not in {"completed", "canceled"}
    assert service.db.fetchone("SELECT state FROM managed_worker_threads WHERE id=?", (thread,))["state"] == "active"
