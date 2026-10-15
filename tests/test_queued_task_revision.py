from __future__ import annotations

import pytest
from test_managed_worker_command_admission import (
    _attached,
    _enroll_current_runtime,
    _instruct,
    _new_thread,
    _thread_route,
)

from cao_control_plane.errors import ConflictError
from cao_control_plane.models import GoalRevision


def _busy_with_queued_task(system, tmp_path):
    service = system["service"]
    actor = _attached(service, suffix="queued-revision")
    thread = _new_thread(service, actor, tmp_path, suffix="queued-revision")
    _enroll_current_runtime(service, thread)
    first = _instruct(service, actor, thread, suffix="first")
    first_id = first["task"]["work_item_id"]
    second = _instruct(service, actor, thread, suffix="second")
    second_id = second["task"]["work_item_id"]
    route = _thread_route(service, thread)
    service.db.execute("UPDATE runtime_sessions SET state='busy' WHERE id=?", (route["runtime_session_id"],))
    service.db.execute("UPDATE attempts SET state='working' WHERE work_item_id=?", (first_id,))
    service.db.execute("UPDATE message_deliveries SET state='dispatched' WHERE message_id IN (SELECT id FROM messages WHERE work_item_id=?)", (first_id,))
    return service, actor, thread, first_id, second_id, route


def _revision():
    return GoalRevision(expected_version=1, expected_purpose_version=1,
        instruction="Produce the bounded result under the fixed criteria.", maturity="defined",
        acceptance=["Return the recorded result and its source."],
        reason="The requester clarified the unstarted checkpoint.", idempotency_key="queued-revision")


def test_unclaimed_queued_task_can_change_without_replaying_or_rotating_busy_work(system, tmp_path):
    service, actor, thread, first_id, second_id, route = _busy_with_queued_task(system, tmp_path)
    first = service.get_work(first_id, actor)
    before_delivery = dict(service.db.fetchone("SELECT d.* FROM message_deliveries d JOIN messages m ON m.id=d.message_id WHERE m.work_item_id=?", (first_id,)))
    second = service.get_work(second_id, actor)
    revised = service.revise_goal(actor, second_id, _revision())
    assert revised["goal_version"] == 2
    assert revised["current_attempt"]["runtime_session_id"] == route["runtime_session_id"]
    assert _thread_route(service, thread) == route
    assert service.get_work(first_id, actor)["current_attempt"] == first["current_attempt"]
    assert dict(service.db.fetchone("SELECT d.* FROM message_deliveries d JOIN messages m ON m.id=d.message_id WHERE m.work_item_id=?", (first_id,))) == before_delivery
    old = service.db.fetchone("SELECT d.state FROM message_deliveries d JOIN messages m ON m.id=d.message_id WHERE m.attempt_id=?", (second["current_attempt"]["id"],))
    assert old["state"] == "dead"
    assert revised["current_attempt"]["assignment_delivery"]["state"] == "queued"


@pytest.mark.parametrize("state,owner", [("leased", "claimed"), ("dispatched", ""), ("queued", "claimed")])
def test_claimed_or_uncertain_task_cannot_use_queue_only_revision(system, tmp_path, state, owner):
    service, actor, thread, _first_id, second_id, route = _busy_with_queued_task(system, tmp_path)
    service.db.execute("UPDATE message_deliveries SET state=?, owner_token=? WHERE message_id IN (SELECT id FROM messages WHERE work_item_id=?)", (state, owner, second_id))
    with pytest.raises(ConflictError) as raised:
        service.revise_goal(actor, second_id, _revision())
    assert raised.value.details["reason_code"] == "worker_command_outcome_unsettled"
    assert service.get_work(second_id, actor)["goal_version"] == 1
    assert _thread_route(service, thread) == route


def test_previously_claimed_task_cannot_be_treated_as_never_claimed_after_requeue(system, tmp_path):
    service, actor, thread, _first_id, second_id, route = _busy_with_queued_task(system, tmp_path)
    service.db.execute("UPDATE message_deliveries SET attempts=1 WHERE message_id IN (SELECT id FROM messages WHERE work_item_id=?)", (second_id,))
    with pytest.raises(ConflictError) as raised:
        service.revise_goal(actor, second_id, _revision())
    assert raised.value.details["reason_code"] == "worker_command_outcome_unsettled"
    assert service.get_work(second_id, actor)["goal_version"] == 1
    assert _thread_route(service, thread) == route


def test_next_instruction_preserves_first_assignment_route_before_mcp_enrollment(system, tmp_path):
    service = system["service"]
    actor = _attached(service, suffix="bootstrap-queue")
    thread = _new_thread(service, actor, tmp_path, suffix="bootstrap-queue")
    first = _instruct(service, actor, thread, suffix="bootstrap-first")
    first_work = service.get_work(first["task"]["work_item_id"], actor)
    route = _thread_route(service, thread)
    service.db.execute("UPDATE runtime_sessions SET state='starting' WHERE id=?", (route["runtime_session_id"],))
    service.db.execute("UPDATE message_deliveries SET state='dispatched', attempts=1 WHERE message_id IN (SELECT id FROM messages WHERE work_item_id=?)", (first_work["id"],))
    second = _instruct(service, actor, thread, suffix="bootstrap-second")
    second_work = service.get_work(second["task"]["work_item_id"], actor)
    assert _thread_route(service, thread) == route
    assert first_work["current_attempt"]["runtime_session_id"] == route["runtime_session_id"]
    assert second_work["current_attempt"]["runtime_session_id"] == route["runtime_session_id"]
    assert service.get_work(first_work["id"], actor)["current_attempt"]["id"] == first_work["current_attempt"]["id"]
    assert second_work["current_attempt"]["assignment_delivery"]["state"] == "queued"


@pytest.mark.parametrize("scoped_launch", [True, False])
def test_worker_context_stays_on_executing_assignment_when_later_work_is_queued(system, tmp_path, scoped_launch):
    service = system["service"]
    actor = _attached(service, suffix="context-queue")
    thread = _new_thread(service, actor, tmp_path, suffix="context-queue")
    if not scoped_launch:
        worker, _runtime_id = _enroll_current_runtime(service, thread)
    first = _instruct(service, actor, thread, suffix="context-first")
    first_work = service.get_work(first["task"]["work_item_id"], actor)
    if scoped_launch:
        worker, _runtime_id = _enroll_current_runtime(service, thread, attempt_id=first_work["current_attempt"]["id"])
    second = _instruct(service, actor, thread, suffix="context-second")
    assert second["task"]["work_item_id"] != first_work["id"]
    context = service.get_worker_context(worker)
    assert context["work"]["id"] == first_work["id"]
    assert context["attempt"]["id"] == first_work["current_attempt"]["id"]
