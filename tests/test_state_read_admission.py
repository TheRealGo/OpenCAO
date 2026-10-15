from __future__ import annotations

import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.database import Database
from cao_control_plane.errors import AuthorizationError
from cao_control_plane.mcp import MCPServer, _canonical_http_request
from cao_control_plane.models import (
    BoundaryInput,
    ReportInput,
    ReportKind,
    RuntimeHeartbeat,
    WorkAssignment,
)
from cao_control_plane.runtime import RuntimeAdapterError, read_cao_state_read_input
from cao_control_plane.service import ControlPlane
from cao_control_plane.state_read_admission import READINESS_PROOF_META_KEY, readiness_proof


def test_sync_native_observer_preserves_an_idle_host_event_loop(system, monkeypatch):
    previous_policy = asyncio.get_event_loop_policy()
    policy = asyncio.DefaultEventLoopPolicy()
    host_loop = policy.new_event_loop()

    async def observe(settings, native_thread_id):
        return "b" * 64

    monkeypatch.setattr("cao_control_plane.runtime.read_cao_state_read_input", observe)
    try:
        asyncio.set_event_loop_policy(policy)
        policy.set_event_loop(host_loop)
        assert MCPServer(system["service"])._read_state_input("native-conversation") == "b" * 64
        assert policy.get_event_loop() is host_loop
        assert not host_loop.is_closed()
    finally:
        host_loop.close()
        asyncio.set_event_loop_policy(previous_policy)


@pytest.mark.asyncio
async def test_sync_native_observer_preserves_a_running_host_event_loop(system, monkeypatch):
    host_loop = asyncio.get_running_loop()

    async def observe(settings, native_thread_id):
        assert asyncio.get_running_loop() is not host_loop
        return "c" * 64

    monkeypatch.setattr("cao_control_plane.runtime.read_cao_state_read_input", observe)
    assert MCPServer(system["service"])._read_state_input("native-conversation") == "c" * 64
    assert asyncio.get_running_loop() is host_loop
    assert not host_loop.is_closed()


@pytest.fixture
def conversation(system: dict[str, Any]) -> dict[str, Any]:
    service = system["service"]
    attachment = attach_cao_session_with_peer(service, current_cao_session_attachment(
        native_thread_id="state-admission-conversation", project_digest="a" * 64,
    ))
    actor = service.authenticate(attachment["context_token"])
    frontier = ["b" * 64]
    server = MCPServer(service, state_read_input_reader=lambda _: frontier[0])
    work = service.assign_work(actor, WorkAssignment(
        worker_id=system["worker"]["id"], runtime_session_id=system["runtime"]["id"],
        title="Bounded checkpoint", objective="Produce one inspectable checkpoint.",
        acceptance=["The checkpoint is inspectable"], idempotency_key="state-admission-work",
    ))
    return {**system, "actor": actor, "attachment": attachment, "frontier": frontier,
            "server": server, "work": work}


def read_work(c: dict[str, Any], server: MCPServer | None = None) -> Any:
    return (server or c["server"]).call_tool(c["actor"], "cao_get_work", {"work_item_id": c["work"]["id"]})


def assert_polling_denied(action: Any) -> None:
    with pytest.raises(AuthorizationError) as caught:
        action()
    assert caught.value.details == {
        "reason_code": "cao_state_polling_prohibited", "retryable": False,
        "next_action": "end_turn_await_notification",
    }


def test_repeat_is_rejected_before_the_state_is_read(conversation: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    read_work(conversation)
    def unexpected_read(*args: Any) -> None:
        raise AssertionError("a rejected poll must not retrieve Work state")
    monkeypatch.setattr(conversation["service"], "get_work", unexpected_read)
    assert_polling_denied(lambda: read_work(conversation))


def test_new_input_reopens_once_and_old_input_cannot_be_replayed(conversation: dict[str, Any]) -> None:
    read_work(conversation)
    assert_polling_denied(lambda: read_work(conversation))
    conversation["frontier"][0] = "c" * 64
    read_work(conversation)
    assert_polling_denied(lambda: read_work(conversation))
    conversation["frontier"][0] = "b" * 64
    assert_polling_denied(lambda: read_work(conversation))


def test_background_heartbeat_and_state_change_do_not_reopen_a_read(conversation: dict[str, Any]) -> None:
    read_work(conversation)
    conversation["service"].heartbeat_runtime(conversation["worker"], conversation["runtime"]["id"], RuntimeHeartbeat(
        expected_enrollment_generation=conversation["worker"]["_enrollment_generation"], sequence=2,
    ))
    assert_polling_denied(lambda: read_work(conversation))


def test_actual_command_reopens_but_idempotent_command_replay_does_not(conversation: dict[str, Any]) -> None:
    read_work(conversation)
    args = {"work_item_id": conversation["work"]["id"], "reason": "Checkpoint withdrawn", "idempotency_key": "withdraw-once"}
    conversation["server"].call_tool(conversation["actor"], "cao_cancel", args)
    assert read_work(conversation)["state"] == "canceled"
    conversation["server"].call_tool(conversation["actor"], "cao_cancel", args)
    assert_polling_denied(lambda: read_work(conversation))


def test_new_notification_reopens_but_consumption_does_not_poll(conversation: dict[str, Any]) -> None:
    read_work(conversation)
    work = conversation["work"]
    attempt = work["current_attempt"]
    conversation["service"].report(conversation["worker"], attempt["id"], ReportInput(
        kind=ReportKind.COMPLETION_CLAIM, summary="Checkpoint prepared for review",
        expected_generation=work["generation"], expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"], expected_task_packet_digest=attempt["task_packet_digest"],
        idempotency_key="checkpoint-notification",
    ))
    read_work(conversation)
    assert_polling_denied(lambda: read_work(conversation))


def test_bridge_replacement_and_database_restart_do_not_reset_admission(conversation: dict[str, Any]) -> None:
    read_work(conversation)
    service = conversation["service"]
    attached = attach_cao_session_with_peer(service, current_cao_session_attachment(
        native_thread_id="state-admission-conversation", project_digest="a" * 64,
    ))
    conversation["actor"] = service.authenticate(attached["context_token"])
    restarted = ControlPlane(Database(conversation["settings"]), conversation["settings"])
    server = MCPServer(restarted, state_read_input_reader=lambda _: "b" * 64)
    assert_polling_denied(lambda: read_work(conversation, server))


def test_concurrent_reads_have_one_admission(conversation: dict[str, Any]) -> None:
    def once(_: int) -> str:
        try:
            read_work(conversation)
            return "admitted"
        except AuthorizationError:
            return "denied"
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(once, range(2))) == ["admitted", "denied"]


def test_query_allows_only_issued_forward_pages(conversation: dict[str, Any]) -> None:
    service = conversation["service"]
    for n in range(2):
        service.assign_work(conversation["actor"], WorkAssignment(
            worker_id=conversation["worker"]["id"], runtime_session_id=conversation["runtime"]["id"],
            title="Another checkpoint", objective="Produce another checkpoint", acceptance=["Inspect output"],
            idempotency_key=f"page-checkpoint-{n}",
        ))
    server, actor = conversation["server"], conversation["actor"]
    first = server.call_tool(actor, "cao_query", {"limit": 1})
    assert first["next_cursor"]
    assert_polling_denied(lambda: server.call_tool(actor, "cao_query", {"limit": 2, "attention_owner": "worker"}))
    assert_polling_denied(lambda: server.call_tool(actor, "cao_query", {"cursor": "fabricated-cursor"}))
    second = server.call_tool(actor, "cao_query", {"limit": 1, "cursor": first["next_cursor"]})
    assert second["items"][0]["id"] != first["items"][0]["id"]
    assert_polling_denied(lambda: server.call_tool(actor, "cao_query", {"limit": 1, "cursor": first["next_cursor"]}))
    final = server.call_tool(actor, "cao_query", {"limit": 1, "cursor": second["next_cursor"]})
    assert final["next_cursor"] is None
    assert_polling_denied(lambda: server.call_tool(actor, "cao_query", {}))


def test_inbox_filter_changes_do_not_reopen_a_read(conversation: dict[str, Any]) -> None:
    server, actor = conversation["server"], conversation["actor"]
    server.call_tool(actor, "cao_get_inbox", {})
    assert_polling_denied(lambda: server.call_tool(actor, "cao_get_inbox", {"include_acknowledged": True, "limit": 1}))
    assert_polling_denied(lambda: server.call_tool(actor, "cao_get_inbox", {"after": 123}))


def test_history_head_cannot_be_used_to_poll_but_older_pages_remain_readable(conversation: dict[str, Any]) -> None:
    server, actor = conversation["server"], conversation["actor"]
    work = conversation["work"]
    attempt = work["current_attempt"]
    for n in range(2):
        conversation["service"].record_boundary(conversation["worker"], BoundaryInput(
            source_event_id=f"history-runtime-fact-{n}", work_item_id=work["id"], attempt_id=attempt["id"],
            kind="blocker", summary=f"Runtime fact {n}", runtime_state="waiting",
            expected_generation=work["generation"], expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"], expected_task_packet_digest=attempt["task_packet_digest"],
        ))
    args = {"work_item_id": conversation["work"]["id"], "limit": 1}
    first = server.call_tool(actor, "cao_read_work_history", args)
    assert first["next_before_sequence"]
    assert_polling_denied(lambda: server.call_tool(actor, "cao_read_work_history", {**args, "limit": 2}))
    assert_polling_denied(lambda: server.call_tool(actor, "cao_read_work_history", {**args, "before_sequence": 999999}))
    server.call_tool(actor, "cao_read_work_history", {**args, "before_sequence": first["next_before_sequence"]})
    assert_polling_denied(lambda: server.call_tool(actor, "cao_read_work_history", {**args, "before_sequence": first["next_before_sequence"]}))


def test_readiness_requires_proxy_authentication_and_does_not_reset_model_reads(conversation: dict[str, Any]) -> None:
    server, actor = conversation["server"], conversation["actor"]
    server.call_tool(actor, "cao_list_managed_workers", {})
    request_id = "private-readiness-probe"
    key = hashlib.sha256(conversation["attachment"]["context_token"].encode()).hexdigest()
    params = {"name": "cao_list_managed_workers", "arguments": {},
              "_meta": {READINESS_PROOF_META_KEY: readiness_proof(key, request_id)}}
    verified = server.handle_modern(actor, _canonical_http_request({"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": params}))
    assert verified is not None
    assert verified["result"]["isError"] is False
    forged = server.handle_modern(actor, _canonical_http_request({"jsonrpc": "2.0", "id": "another-request", "method": "tools/call", "params": params}))
    assert forged is not None
    assert forged["result"]["isError"] is True
    assert_polling_denied(lambda: server.call_tool(actor, "cao_list_managed_workers", {}))


def test_failed_read_preserves_admission(conversation: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    service = conversation["service"]
    original = service.get_work
    def failing(*args: Any) -> None:
        raise ValueError("temporary read failure")
    monkeypatch.setattr(service, "get_work", failing)
    with pytest.raises(ValueError):
        read_work(conversation)
    monkeypatch.setattr(service, "get_work", original)
    read_work(conversation)
    assert_polling_denied(lambda: read_work(conversation))


def test_unavailable_input_observation_cannot_refresh_an_existing_read(conversation: dict[str, Any]) -> None:
    server = MCPServer(conversation["service"], state_read_input_reader=lambda _: None)
    read_work(conversation, server)
    assert_polling_denied(lambda: read_work(conversation, server))


@pytest.mark.asyncio
@pytest.mark.parametrize("input_kind", ["user", "coordination", "own_tool_output", "none"])
async def test_native_observer_uses_input_identity_not_agent_output(
    settings: Any, monkeypatch: pytest.MonkeyPatch, input_kind: str,
) -> None:
    requests: list[str] = []
    class RPC:
        async def request(self, method: str, params: Any, **kwargs: Any) -> Any:
            requests.append(method)
            if method == "initialize":
                return 1, {}
            assert params["threadId"] == "native-conversation"
            assert params["limit"] == 1 and params["sortDirection"] == "desc"
            items = [{"type": "agentMessage", "id": "changing-agent-output"}]
            # The real persisted summary view omits native inputs once an
            # assistant message exists. An input must come from the full view.
            if params.get("itemsView") != "full":
                return 2, {"data": [{"id": "turn-with-agent-output", "items": items}]}
            if input_kind == "user":
                items.insert(0, {"type": "userMessage", "id": "stable-input", "content": "private input is not retained"})
            elif input_kind in {"coordination", "own_tool_output"}:
                items.insert(0, {"type": "functionCallOutput", "id": "stable-input",
                                 "namespace": "codex_app" if input_kind == "coordination" else "functions",
                                 "name": "send_message_to_thread" if input_kind == "coordination" else "exec",
                                 "output": "untrusted output never supplies effect authority"})
            return 2, {"data": [{"id": "turn-with-agent-output", "items": items}]}
        async def send(self, value: Any) -> None:
            assert value["method"] == "initialized"
        async def close(self) -> None:
            requests.append("close")
    async def connect(*args: Any, **kwargs: Any) -> RPC:
        return RPC()
    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)
    observed = await read_cao_state_read_input(settings, "native-conversation")
    assert observed == (hashlib.sha256(b"stable-input").hexdigest() if input_kind in {"user", "coordination"} else None)
    assert requests == ["initialize", "thread/turns/list", "close"]


@pytest.mark.asyncio
async def test_native_observer_failure_does_not_create_input_authority(settings: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    async def unavailable(*args: Any, **kwargs: Any) -> None:
        raise RuntimeAdapterError("unavailable")
    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", unavailable)
    assert await read_cao_state_read_input(settings, "native-conversation") is None
