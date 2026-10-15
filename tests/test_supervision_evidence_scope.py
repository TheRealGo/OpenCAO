from __future__ import annotations

import json

import pytest
from test_supervision_memory import _actor, _work, _write
from test_verified_artifact_content_read import _attached_cao, _read_arguments, _reported_artifact
from test_worker_output_delivery import (
    _begin_successor_capture,
    _capture_fixture,
    _dispose,
    _event,
    _read_request,
    _review,
    _terminal,
)

from cao_control_plane.mcp import (
    MCPServer,
    _conversation_message_projection,
    _conversation_work_projection,
)
from cao_control_plane.models import MemorySearchInput, ReportKind, WorkHistoryReadInput
from cao_control_plane.runtime import render_message
from cao_control_plane.worker_purpose import PURPOSE_GUIDANCE


@pytest.mark.parametrize("kind", [ReportKind.ARTIFACT, ReportKind.PROGRESS, ReportKind.BLOCKER])
def test_intermediate_verified_content_is_readable_without_acceptance(system, kind):
    attachment, actor = _attached_cao(system, thread_id="intermediate-review", project_marker="a")
    work, artifact = _reported_artifact(
        system, attachment, content=b'{"measured":false}', media_type="application/json",
        key="intermediate", report_kind=kind,
    )
    service = system["service"]
    before = service.get_work(work["id"])
    result = MCPServer(service).call_tool(actor, "cao_read_artifact", _read_arguments(work, artifact))
    assert result["content"] == '{"measured":false}' and result["complete"] is True
    after = service.get_work(work["id"])
    assert after == before
    assert not after["current_attempt"]["completion_claim"]
    assert after["delivery_state"] == "pending"
    assert not after["reviews"]


def test_default_recall_uses_exact_work_but_explicit_search_can_cross_work(system):
    service = system["service"]
    actor = _actor(system, "targeted-recall")
    first = _work(system, actor, "first")
    second = _work(system, actor, "second")
    memories = []
    for work in (first, second):
        case = {**system, "actor": actor, "work": work}
        memories.append(service.remember_memory(actor, _write(
            case, primary_abstraction="Preserve scoped operational experience " + work["title"],
            idempotency_key="memory:" + work["id"],
        )))
    with service.db.connection_scope() as connection:
        row = connection.execute("SELECT * FROM work_items WHERE id=?", (second["id"],)).fetchone()
        recall = service._supervision_memory_context_tx(connection, actor, row)["recall"]
    assert [item["memory_id"] for item in recall["memories"]] == [memories[1]["memory_id"]]
    explicit = service.search_memories(actor, MemorySearchInput(query="scoped operational experience"))
    assert {item["memory_id"] for item in explicit["memories"]} == {m["memory_id"] for m in memories}
    read = MCPServer(service).call_tool(actor, "cao_read_memory", {
        "memory_id": memories[0]["memory_id"], "expected_revision": 1,
    })
    assert read["source_work_item_id"] == first["id"]
    assert read["kind"] == "curated" and read["untrusted"] is True
    assert "current purpose, roles and permissions prevail" in read["guidance"]


def test_output_notification_targets_exact_final_and_current_projection_retains_history(system):
    service = system["service"]
    actor, work, binding = _capture_fixture(system)
    first = service.observe_worker_output(**binding, event=_event(text="First incomplete submission."))
    first_end = _terminal(service, binding)
    first_boundary = service.get_work(work["id"])["open_boundaries"][0]["id"]
    notice = service.db.fetchone("SELECT * FROM messages WHERE id=?", (first_end["notification_message_id"],))
    payload = json.loads(notice["payload_json"])
    assert payload["output_refs"] == [{
        "output_id": first["id"], "digest": first["digest"], "capture_state": "available", "complete": True,
    }]
    public_notice = _conversation_message_projection({**dict(notice), "payload": payload})
    assert public_notice["payload"]["output_refs"] == payload["output_refs"]
    assert public_notice["payload"]["output_id"] == first_end["id"]
    assert first_end["digest"] == ""
    wake = render_message({**dict(notice), "payload": payload})
    assert first["id"] in wake and first["digest"] in wake
    assert "never submit an empty" in wake
    service.read_worker_output(actor, _read_request(work, first))
    _review(service, actor, work, verdict="needs_work")
    _dispose(service, actor, work, kind="correct")
    successor = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id=? AND kind='instruction' ORDER BY sequence DESC LIMIT 1",
        (binding["attempt_id"],),
    )
    assert successor is not None
    second_binding = _begin_successor_capture(service, binding, successor["id"])
    second = service.observe_worker_output(**second_binding, event=_event(
        turn_id="second-provider-turn", item_id="second-final", text="Second measured submission."
    ))
    _terminal(service, second_binding, turn_id="second-provider-turn")
    current = service.get_work(work["id"])
    projected = _conversation_work_projection(current)
    assert second["id"] in {o["id"] for o in projected["worker_outputs"]}
    assert first["id"] not in {o["id"] for o in projected["worker_outputs"]}
    history = service.read_work_history(actor, WorkHistoryReadInput(work_item_id=work["id"]))
    old = next(c for c in history["cycles"] if c["boundary_id"] == first_boundary)
    assert first["id"] in {o["output_id"] for o in old["output_refs"]}
    assert service.read_worker_output(actor, _read_request(work, first))["content"] == "First incomplete submission."


def test_assignment_and_supervision_share_worker_owned_execution_boundary():
    assignment = render_message({"kind": "assignment", "payload": {
        "worker_purpose": {"text": "Achieve a measured outcome.", "version": 1},
        "objective": "Produce and verify the requested outcome.",
    }})
    assert "You own method selection" in assignment
    assert "A plan, ACK or explanation alone is not completion" in assignment
    assert "report proposals to CAO" not in assignment
    assert "without prescribing the implementation or doing the Worker's work" in PURPOSE_GUIDANCE
    assert "Accept a satisfied task without moving its acceptance conditions" in PURPOSE_GUIDANCE


@pytest.mark.parametrize("kind", ["instruction", "user_input"])
def test_continuation_does_not_end_with_plan_or_add_method_approval(kind):
    rendered = render_message({"kind": kind, "work_item_id": "wrk_fixture", "payload": {
        "instruction": "Resolve the missing outcome within existing authority.",
    }})
    assert "choose and execute ordinary methods yourself" in rendered
    assert "continue to the requested outcome and evidence" in rendered
    assert "Respect explicit pauses, cancellation and effect/authority limits" in rendered


def test_output_notice_rejects_malformed_read_references():
    projected = _conversation_message_projection({"payload": {
        "action": "worker_output", "output_id": {"native": "private"},
        "output_refs": [
            {"output_id": "wout_example", "digest": "owner-token", "capture_state": "available", "complete": True},
            {"output_id": "wout_example", "digest": "a" * 64, "capture_state": ["available"], "complete": True},
        ],
    }})
    assert projected["payload"] == {"action": "worker_output", "output_refs": []}
