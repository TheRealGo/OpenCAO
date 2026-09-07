"""Executable public contract for explicit close through one CAO conversation.

This is intentionally a boundary specification, not an SQLite test. It keeps
the requester outside the MCP graph and requires the Control Plane to use the
attachment-bound CAO credential as the sole authority for recording a decision
and closing the exact accepted WorkItem generation.
"""

from __future__ import annotations

from typing import Any

from cao_control_plane.api import create_app
from cao_control_plane.dashboard import build_operator_view
from cao_control_plane.mcp import MCPServer

_DIGEST_A = "a" * 64


def _bound_cao_actor(*, attachment: str, thread: str) -> dict[str, Any]:
    """Representative actor after authenticating a ``csc_...`` credential."""

    return {
        "id": "prn-cao",
        "role": "cao",
        "_cao_conversation_credential_id": "csc-credential",
        "_cao_attachment_id": attachment,
        "_cao_attachment_generation": 7,
        "_runtime_session_id": "rt-cao",
        "_native_thread_id": thread,
        "_cao_project_digest": _DIGEST_A,
    }


def test_close_mutations_exist_only_for_an_attachment_bound_cao_conversation(system) -> None:
    """Neither User/Worker nor an unbound CAO bearer gets a requester UI API."""

    server = MCPServer(system["service"])
    attached = {
        tool["name"]
        for tool in server.tools_for(_bound_cao_actor(attachment="att-a", thread="thread-a"))
    }
    unbound_cao = {tool["name"] for tool in server.tools_for(system["cao"])}
    requester = {tool["name"] for tool in server.tools_for(system["user"])}
    worker = {tool["name"] for tool in server.tools_for(system["worker"])}

    assert {
        "cao_record_requester_decision",
        "cao_close_conversation",
        "cao_finish_worker_thread",
    } <= attached
    low_level_close_tools = {
        "cao_stop_work_runtime",
        "cao_close_work",
        "cao_prepare_work_close",
        "cao_execute_prepared_cleanup",
    }
    assert low_level_close_tools.isdisjoint(attached)
    assert "cao_record_requester_decision" not in unbound_cao
    assert "cao_close_conversation" not in unbound_cao
    assert low_level_close_tools.isdisjoint(unbound_cao)
    assert "cao_record_requester_decision" not in requester | worker
    assert "cao_close_conversation" not in requester | worker
    assert low_level_close_tools.isdisjoint(requester | worker)
    assert "cao_user_acceptance" not in attached | requester | worker
    app = create_app(system["settings"])
    paths = {route.path for route in app.routes}
    assert "/api/v1/user-acceptances" not in paths


def test_close_tool_inputs_are_exact_and_cannot_name_a_different_conversation(system) -> None:
    """Conversation ownership is authenticated context, never command input."""

    tools = {
        tool["name"]: tool
        for tool in MCPServer(system["service"]).tools_for(
            _bound_cao_actor(attachment="att-a", thread="thread-a")
        )
    }
    decision_schema = tools["cao_record_requester_decision"]["inputSchema"]
    conversation_close_schema = tools["cao_close_conversation"]["inputSchema"]

    assert set(decision_schema["required"]) >= {
        "review_id",
        "verdict",
        "summary",
        "conversation_evidence_id",
        "idempotency_key",
    }
    supplied = set(decision_schema["properties"])
    assert "recorded_requester_id" not in supplied
    assert conversation_close_schema["type"] == "object"
    assert conversation_close_schema["required"] == ["idempotency_key"]
    assert conversation_close_schema["additionalProperties"] is False
    assert set(conversation_close_schema["properties"]) == {"idempotency_key"}
    supplied |= set(conversation_close_schema["properties"])
    assert not supplied & {
        "supervisor_attachment_id",
        "supervisor_project_digest",
        "native_thread_id",
        "runtime_session_id",
        "recorded_by",
        "credential",
        "workspace_path",
        "cleanup_path",
        "command",
    }


def test_dashboard_distinguishes_completed_from_explicitly_closed_and_keeps_closed_visible() -> (
    None
):
    """A terminal WorkItem state alone must never make a dashboard row closed."""

    base = {
        "assigned_worker_id": "wrk-1",
        "state": "completed",
        "current_attempt_state": "completed",
        "current_attempt_trajectory": "complete",
        "attention_owner": "none",
        "open_boundary_count": 0,
    }
    awaiting = build_operator_view({"work_items": [base]})["work_items"]
    closed = build_operator_view({"work_items": [{**base, "closure_state": "closed"}]})[
        "work_items"
    ]

    assert awaiting[0]["closure_state"] == "awaiting-explicit-close"
    assert len(closed) == 1
    assert closed[0]["closure_state"] == "closed"


def test_cross_conversation_scope_is_part_of_the_public_service_contract() -> None:
    """Service use cases must accept an authenticated bound actor, not a thread field.

    The integration implementation must prove that actor A is denied for every
    WorkItem, decision, close receipt, inbox entry and delivery belonging to
    attachment B. A `close_contract.py`-only check cannot provide that proof.
    """

    import cao_control_plane.models as models
    from cao_control_plane.service import ControlPlane

    assert hasattr(models, "RequesterDecisionInput"), (
        "add RequesterDecisionInput and persist its attachment/packet bindings"
    )
    assert hasattr(models, "WorkCloseInput"), (
        "add WorkCloseInput for the exact reviewed/accepted cleanup plan"
    )
    assert hasattr(ControlPlane, "record_requester_decision"), (
        "add a transactionally attachment-scoped requester-decision use case"
    )
    assert hasattr(ControlPlane, "close_work"), (
        "add a transactionally attachment-scoped explicit-close use case"
    )
    assert not hasattr(ControlPlane, "user_acceptance"), (
        "remove the legacy User-role acceptance mutation from the MCP-only path"
    )
