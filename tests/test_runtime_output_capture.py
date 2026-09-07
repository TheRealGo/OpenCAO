from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from enrollment_helpers import EnrollmentHandshakeAdapter, EnrollmentHandshakeRegistry

from cao_control_plane.config import Settings
from cao_control_plane.models import RuntimeDispatchResult, WorkAssignment, WorkerOutputReadInput
from cao_control_plane.runtime import (
    ClaudeAdapter,
    CodexAppServerAdapter,
    Dispatcher,
    RuntimeAdapterError,
    _ClaudeOutputParser,
    _WorkerOutputSink,
    render_message,
)
from cao_control_plane.worker_output import WorkerOutputEvent


def _assistant(
    text, *, item_id="assistant-item", user_id=None, session_id="native-session", **extra
):
    value = {
        "type": "assistant",
        "uuid": item_id,
        "session_id": session_id,
        "parent_tool_use_id": None,
        "message": {"id": "shared-provider-message", "content": [{"type": "text", "text": text}]},
        **extra,
    }
    if user_id is not None:
        value["user_message_uuid"] = user_id
    return value


def _result(*, user_id="input-id", is_error=False):
    return {
        "type": "result",
        "subtype": "success",
        "uuid": "result-item",
        "session_id": "native-session",
        "user_message_uuid": user_id,
        "is_error": is_error,
        "result": "Result available for review.",
    }


def test_claude_captures_only_exact_main_session_text_and_deduplicates_envelopes():
    events = []
    parser = _ClaudeOutputParser(
        _WorkerOutputSink(
            events.append, 4096, native_thread_id="native-session", turn_id="input-id"
        ),
        "input-id",
    )
    parser.observe(_assistant("unbound output"))
    parser.observe(_assistant("wrong input", user_id="other-input"))
    parser.observe(_assistant("wrong session", user_id="input-id", session_id="other-session"))
    parser.observe(_assistant("subagent output", user_id="input-id", parent_tool_use_id="tool-id"))
    parser.observe(
        _assistant("synthetic output", user_id="input-id", origin={"kind": "task-notification"})
    )
    assert events == []

    value = _assistant("visible response", user_id="input-id")
    value["message"]["content"].extend(
        [
            {"type": "thinking", "thinking": "private-reasoning"},
            {"type": "tool_use", "input": "private-tool-input"},
        ]
    )
    parser.observe(value)
    parser.observe(value)
    # Native API message IDs may be shared; envelope UUIDs remain distinct.
    parser.observe(_assistant("another response", item_id="another-envelope"))
    parser.observe(_result())
    parser.observe(_assistant("late output", item_id="late-envelope", user_id="input-id"))
    parser.observe(_result())

    assert [(event.kind, event.item_id) for event in events] == [
        ("message", "assistant-item"),
        ("message", "another-envelope"),
        ("turn_end", "result-item"),
    ]
    assert events[0].text == "visible response"
    assert all(
        event.native_thread_id == "native-session" and event.turn_id == "input-id"
        for event in events
    )
    assert events[-1].status == "completed"
    assert "private-" not in str(events)


@pytest.mark.parametrize("is_error", [True, False])
def test_claude_terminal_result_is_observed_even_without_assistant_text(is_error):
    events = []
    parser = _ClaudeOutputParser(
        _WorkerOutputSink(events.append, 4096, turn_id="input-id"), "input-id"
    )
    parser.observe(_result(is_error=is_error))
    assert len(events) == 1
    assert events[0].kind == "turn_end"
    assert events[0].status == ("failed" if is_error else "completed")
    assert events[0].text == ("" if is_error else "Result available for review.")


def test_claude_truncated_and_aborted_messages_remain_explicitly_incomplete():
    events = []
    parser = _ClaudeOutputParser(
        _WorkerOutputSink(events.append, 1024, turn_id="input-id"), "input-id"
    )
    parser.observe(_assistant("界" * 2048, user_id="input-id", aborted=True))
    parser.observe(_result())
    assert len(events[0].text.encode()) <= 1024
    assert "[output truncated by CAO control plane]" in events[0].text
    assert events[0].complete is False
    assert events[-1].complete is False


def test_claude_rejects_conflicting_duplicate_identity():
    events = []
    parser = _ClaudeOutputParser(
        _WorkerOutputSink(events.append, 4096, turn_id="input-id"), "input-id"
    )
    parser.observe(_assistant("first", user_id="input-id"))
    with pytest.raises(RuntimeAdapterError, match="identity conflict"):
        parser.observe(_assistant("changed", user_id="input-id"))
    parser.sink.finish("failed", complete=False)
    assert [event.text for event in events] == ["first", ""]
    assert events[-1].complete is False


_CLAUDE_PROGRAM = r"""
import json
import sys
import time

request = json.loads(sys.stdin.readline())
assert '--input-format' in sys.argv and '--replay-user-messages' in sys.argv
assert request['type'] == 'user' and request['message']['role'] == 'user'
frame = {
    'type': 'assistant', 'uuid': 'assistant-item', 'session_id': 'native-session',
    'parent_tool_use_id': None, 'user_message_uuid': request['uuid'],
    'message': {'content': [{'type': 'text', 'text': 'Automatic delivery before process exit.'}]},
}
print(json.dumps(frame), flush=True)
time.sleep(0.15)
if 'malformed' in request['message']['content']:
    print('{broken-json', flush=True)
    time.sleep(10)
elif 'partial' in request['message']['content']:
    sys.stdout.write('{"type":')
    sys.stdout.flush()
elif 'missing-result' not in request['message']['content']:
    print(json.dumps({
        'type': 'result', 'subtype': 'success', 'uuid': 'result-item',
        'session_id': 'native-session', 'user_message_uuid': request['uuid'],
        'is_error': 'provider-error' in request['message']['content'], 'result': 'Finished provider turn.',
    }), flush=True)
"""


def test_claude_stream_delivers_before_exit_without_reporting_tool():
    async def scenario():
        events = []
        observed = asyncio.Event()

        def capture(event):
            events.append(event)
            if event.kind == "message":
                observed.set()

        task = asyncio.create_task(
            ClaudeAdapter(Settings(runtime_timeout_seconds=2)).dispatch(
                {
                    "metadata": {"command": [sys.executable, "-u", "-c", _CLAUDE_PROGRAM]},
                    "_worker_output_observer": capture,
                },
                {"id": "delivery-one", "kind": "instruction", "payload": {"message": "answer"}},
            )
        )
        await asyncio.wait_for(observed.wait(), timeout=1)
        assert not task.done()
        result = await task
        assert result.success is True
        assert result.output == ""
        assert [event.kind for event in events] == ["message", "turn_end"]
        assert events[-1].status == "completed"

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["malformed", "partial", "missing-result", "provider-error"])
def test_claude_parser_or_provider_failure_always_delivers_terminal_observation(mode):
    events = []
    invocation = ClaudeAdapter(Settings(runtime_timeout_seconds=2)).dispatch(
        {
            "metadata": {"command": [sys.executable, "-u", "-c", _CLAUDE_PROGRAM]},
            "_worker_output_observer": events.append,
        },
        {"id": "delivery-failure", "kind": "instruction", "payload": {"message": mode}},
    )
    if mode in {"malformed", "partial"}:
        with pytest.raises(RuntimeAdapterError):
            asyncio.run(invocation)
    else:
        assert asyncio.run(invocation).success is False
    assert len([event for event in events if event.kind == "turn_end"]) == 1
    assert events[-1].status == "failed"
    assert events[-1].complete is (mode == "provider-error")
    assert "broken-json" not in str(events)


_CODEX_PROGRAM = r"""
import json
import sys

def emit(method, params):
    print(json.dumps({'method': method, 'params': params}), flush=True)

for raw in sys.stdin:
    request = json.loads(raw)
    if 'id' not in request:
        continue
    method = request['method']
    if method == 'thread/start':
        result = {'thread': {'id': 'native-thread'}}
    elif method == 'turn/start':
        result = {'turn': {'id': 'native-turn'}}
    elif method == 'thread/queue/add':
        user = {'type': 'userMessage', 'id': 'input-item', 'clientId': request['params']['clientUserMessageId']}
        emit('item/started', {'threadId': 'other-thread', 'turnId': 'wrong-turn', 'item': user})
        emit('item/started', {'threadId': 'native-thread', 'turnId': 'native-turn', 'item': user})
        result = {'queuedSubmission': {'id': 'queue-item', 'clientUserMessageId': user['clientId']}}
    else:
        result = {}
    print(json.dumps({'id': request['id'], 'result': result}), flush=True)
    if method == 'thread/start':
        config = request['params'].get('config', {})
        for name in config:
            if name.startswith('mcp_servers.'):
                emit('mcpServer/startupStatus/updated', {'threadId': 'native-thread', 'name': name.split('.', 1)[1], 'status': 'ready'})
    if method not in ('turn/start', 'thread/queue/add'):
        continue
    text = request['params']['input'][0]['text']
    emit('item/completed', {'threadId': 'other-thread', 'turnId': 'native-turn', 'item': {'type': 'agentMessage', 'id': 'wrong-thread', 'text': 'wrong-thread-output'}})
    emit('item/completed', {'threadId': 'native-thread', 'turnId': 'other-turn', 'item': {'type': 'agentMessage', 'id': 'wrong-turn', 'text': 'wrong-turn-output'}})
    emit('turn/completed', {'threadId': 'other-thread', 'turn': {'id': 'native-turn', 'status': 'failed'}})
    emit('item/completed', {'threadId': 'native-thread', 'turnId': 'native-turn', 'item': {'type': 'reasoning', 'id': 'thought', 'text': 'private-reasoning'}})
    if 'empty' not in text:
        emit('item/agentMessage/delta', {'threadId': 'native-thread', 'turnId': 'native-turn', 'itemId': 'answer', 'delta': 'unfinished-delta'})
    if 'empty' not in text and 'partial-only' not in text:
        item = {'threadId': 'native-thread', 'turnId': 'native-turn', 'item': {'type': 'agentMessage', 'id': 'answer', 'text': 'Complete answer.', 'phase': 'final_answer'}}
        emit('item/completed', item)
        emit('item/completed', item)
    if 'malformed' in text:
        print('{broken-json', flush=True)
    else:
        emit('turn/completed', {'threadId': 'native-thread', 'turn': {'id': 'native-turn', 'status': 'completed'}})
"""


@pytest.mark.parametrize("mode", ["answer", "empty", "malformed", "partial-only"])
def test_codex_authoritative_items_and_terminal_outcome_bind_exact_thread_and_turn(mode):
    events = []
    invocation = CodexAppServerAdapter(Settings(runtime_timeout_seconds=2)).dispatch(
        {
            "metadata": {"command": [sys.executable, "-u", "-c", _CODEX_PROGRAM]},
            "_worker_output_observer": events.append,
        },
        {"id": "delivery-one", "kind": "instruction", "payload": {"message": mode}},
    )
    if mode == "malformed":
        with pytest.raises(RuntimeAdapterError):
            asyncio.run(invocation)
    else:
        result = asyncio.run(invocation)
        assert result.success is True
        assert result.output == ""
    assert all(
        event.native_thread_id == "native-thread" and event.turn_id == "native-turn"
        for event in events
    )
    assert len([event for event in events if event.kind == "message"]) == (
        0 if mode in {"empty", "partial-only"} else 1
    )
    assert len([event for event in events if event.kind == "turn_end"]) == 1
    assert events[-1].status == ("failed" if mode == "malformed" else "completed")
    assert events[-1].complete is (mode not in {"malformed", "partial-only"})
    assert "private-reasoning" not in str(events)
    assert "wrong-" not in str(events)
    assert "unfinished-delta" not in str(events)


def test_codex_managed_queue_binds_the_client_message_before_queue_ack(monkeypatch):
    events = []
    monkeypatch.setattr(
        "cao_control_plane.runtime._enrollment_mcp_config",
        lambda runtime: {"mcpServers": {"cao_control_plane": {"command": "unused", "args": []}}},
    )
    result = asyncio.run(
        CodexAppServerAdapter(Settings(runtime_timeout_seconds=2)).dispatch(
            {
                "id": "runtime-capture",
                "metadata": {"command": [sys.executable, "-u", "-c", _CODEX_PROGRAM]},
                "_worker_output_observer": events.append,
            },
            {"id": "delivery-one", "kind": "instruction", "payload": {"message": "answer"}},
        )
    )
    assert result.success is True
    assert result.metadata["delivery_method"] == "thread_queue"
    assert [event.kind for event in events] == ["message", "turn_end"]
    assert all(
        event.native_thread_id == "native-thread" and event.turn_id == "native-turn"
        for event in events
    )


def test_codex_startup_failure_emits_one_unbound_incomplete_terminal_observation():
    events = []
    program = r"""
import json
import sys
for raw in sys.stdin:
    request = json.loads(raw)
    if 'id' in request:
        if request['method'] == 'initialize':
            print(json.dumps({'id': request['id'], 'result': {}}), flush=True)
        else:
            print(json.dumps({'id': request['id'], 'error': {'code': -1, 'message': 'private-provider-error'}}), flush=True)
"""
    with pytest.raises(RuntimeAdapterError):
        asyncio.run(
            CodexAppServerAdapter(Settings(runtime_timeout_seconds=2)).dispatch(
                {
                    "metadata": {"command": [sys.executable, "-u", "-c", program]},
                    "_worker_output_observer": events.append,
                },
                {"id": "delivery-one", "kind": "instruction", "payload": {"message": "answer"}},
            )
        )
    assert len(events) == 1
    assert events[0].kind == "turn_end"
    assert events[0].native_thread_id == events[0].turn_id == ""
    assert events[0].status == "failed"
    assert events[0].complete is False
    assert "private-provider-error" not in str(events)


def test_dispatcher_injects_the_exact_immutable_output_delivery_binding(system, monkeypatch):
    service = system["service"]
    observed = []
    monkeypatch.setattr(
        service, "observe_worker_output", lambda **binding: observed.append(binding)
    )
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Automatic output",
            objective="Deliver a normal assistant answer.",
            acceptance=["A typed observation reaches the attached supervisor."],
        ),
    )

    class ObservingAdapter(EnrollmentHandshakeAdapter):
        async def dispatch(self, runtime, message):
            result = await super().dispatch(runtime, message)
            runtime["_worker_output_observer"](
                WorkerOutputEvent(
                    native_thread_id="native-thread",
                    turn_id="native-turn",
                    item_id="answer",
                    kind="message",
                    text="Result for review.",
                )
            )
            return result

    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(ObservingAdapter(service))
    )
    asyncio.run(dispatcher.run_once())
    assert len(observed) == 1
    binding = observed[0]
    assert binding["runtime_id"] == system["runtime"]["id"]
    assert binding["attempt_id"] == work["current_attempt"]["id"]
    assert binding["owner_token"] == dispatcher.owner_token
    delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id = ?", (binding["delivery_message_id"],)
    )
    assert delivery is not None
    assert binding["delivery_generation"] == delivery["generation"]
    enrollment = service.db.fetchone(
        "SELECT generation FROM worker_enrollments WHERE runtime_session_id = ?",
        (binding["runtime_id"],),
    )
    assert enrollment is not None
    assert binding["enrollment_generation"] == enrollment["generation"]


def test_assignment_protocol_makes_normal_answer_delivery_independent_of_report_calls():
    rendered = render_message(
        {
            "kind": "assignment",
            "payload": {
                "objective": "Reply with evidence.",
                "acceptance": ["Evidence can be reviewed."],
            },
        }
    )
    assert "delivery does not depend on calling cao_report" in rendered
    assert "always submit one terminal cao_report" not in rendered


def test_dispatcher_persists_provider_output_without_any_worker_mcp_calls(system, monkeypatch):
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="automatic-output-supervisor", project_digest="a" * 64
        ),
    )
    actor = service.authenticate(str(attachment["context_token"]))
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Automatic answer without MCP",
            objective="Return a bounded answer for independent review.",
            acceptance=["Provider output is delivered even without a Worker MCP call."],
            completion_contract="no_artifact_expected",
        ),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET metadata_json = ? WHERE id = ?",
        (
            json.dumps({"command": [sys.executable, "-u", "-c", _CLAUDE_PROGRAM]}),
            system["runtime"]["id"],
        ),
    )
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(ClaudeAdapter(system["settings"])),
    )
    original_observe = service.observe_worker_output
    checked_busy_source = []

    def observe(**binding):
        output = original_observe(**binding)
        if binding["event"].kind == "turn_end":
            # A prior availability wake may already exist. Settling its
            # transport must not admit the terminal wake while the source is
            # still executing the adapter return path.
            service.db.execute(
                "UPDATE message_deliveries SET state = 'handled' WHERE message_id IN "
                "(SELECT notification_message_id FROM worker_output_receipts "
                "WHERE stream_id = (SELECT stream_id FROM worker_output_receipts WHERE id = ?) "
                "AND event_kind = 'message')",
                (output["id"],),
            )
            assert service.get_runtime(system["runtime"]["id"])["state"] == "busy"
            pending = service.get_work(work["id"], actor=actor)
            assert pending["state"] == "active"
            assert pending["open_boundaries"] == []
            assert dispatcher._claim_delivery() is None
            checked_busy_source.append(True)
        return output

    monkeypatch.setattr(service, "observe_worker_output", observe)
    asyncio.run(dispatcher.run_once())
    # Failure recovery settles first. The maintenance path then attaches
    # already captured output to that canonical recovery boundary.
    service.reconcile_abandoned_worker_output_captures()

    current = service.get_work(work["id"], actor=actor)
    outputs = current["worker_outputs"]
    assert [output["kind"] for output in outputs] == ["message", "turn_end"]
    assert outputs[-1]["turn_status"] == "completed"
    assert checked_busy_source == [True]
    assert current["state"] == "waiting_supervisor"
    assert current["open_boundaries"][0]["kind"] != "worker_output"
    assert current["open_boundaries"][0]["metadata"]["runtime_recovery"] is True
    assert current["open_boundaries"][0]["recovery_action"] in {
        "system_reconciliation",
        "reconcile_continue_same_thread",
    }
    assert outputs[-1]["boundary_id"] == current["open_boundaries"][0]["id"]
    assert current["current_attempt"]["completion_claim"] == {}
    assert service.get_runtime(system["runtime"]["id"])["state"] == "failed"
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM messages WHERE attempt_id = ? "
            "AND kind IN ('progress', 'completion_claim', 'question', 'blocker')",
            (work["current_attempt"]["id"],),
        )["count"]
        == 0
    )
    read = service.read_worker_output(
        actor,
        WorkerOutputReadInput(
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            output_id=outputs[0]["id"],
            expected_digest=outputs[0]["digest"],
            idempotency_key="read-automatic-provider-answer",
        ),
    )
    assert read["content"] == "Automatic delivery before process exit."
    claimed = dispatcher._claim_delivery()
    assert claimed is not None
    terminal = service.db.fetchone(
        "SELECT notification_message_id FROM worker_output_receipts WHERE id = ?",
        (outputs[-1]["id"],),
    )
    assert claimed["message_id"] == terminal["notification_message_id"]


@pytest.mark.parametrize("status", ["completed", "failed", "interrupted"])
def test_dispatcher_finalizes_output_only_after_authoritative_runtime_settlement(system, status):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Capture then settle provider output",
            objective="Preserve automatic output without preempting canonical runtime recovery.",
            acceptance=["Only an authoritative successful dispatch creates an output boundary."],
            completion_contract="no_artifact_expected",
        ),
    )

    class ObservingAdapter(EnrollmentHandshakeAdapter):
        async def dispatch(self, runtime, message):
            result = await super().dispatch(runtime, message)
            for event in (
                WorkerOutputEvent(
                    native_thread_id="native-thread",
                    turn_id="native-turn",
                    item_id="answer",
                    kind="message",
                    text="Result evidence before provider settlement.",
                ),
                WorkerOutputEvent(
                    native_thread_id="native-thread",
                    turn_id="native-turn",
                    item_id="terminal",
                    kind="turn_end",
                    status=status,
                ),
            ):
                runtime["_worker_output_observer"](event)
            # The capture callback cannot take attention from failure
            # recovery or present a continuation capability before return.
            captured = service.get_work(work["id"])
            assert captured["state"] == "active"
            assert captured["open_boundaries"] == []
            assert service.get_runtime(runtime["id"])["state"] == "busy"
            return result.model_copy(
                update={
                    "success": status == "completed",
                    "error": "" if status == "completed" else "runtime_turn_failed",
                    "native_session_id": "native-thread",
                    "metadata": {"turn_status": status},
                }
            )

    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(ObservingAdapter(service)),
    )
    asyncio.run(dispatcher.run_once())
    service.reconcile_abandoned_worker_output_captures()
    current = service.get_work(work["id"])
    assert [output["kind"] for output in current["worker_outputs"]] == ["message", "turn_end"]
    assert current["worker_outputs"][-1]["turn_status"] == status
    assert current["current_attempt"]["completion_claim"] == {}
    boundary = current["open_boundaries"][0]
    assert current["worker_outputs"][-1]["boundary_id"] == boundary["id"]
    if status == "completed":
        assert boundary["kind"] == "worker_output"
        assert not boundary["metadata"].get("runtime_recovery")
        assert service.get_runtime(system["runtime"]["id"])["state"] == "waiting"
    else:
        assert boundary["kind"] != "worker_output"
        assert boundary["metadata"]["runtime_recovery"] is True
        assert boundary["recovery_action"] in {
            "system_reconciliation",
            "reconcile_continue_same_thread",
        }
        assert service.get_runtime(system["runtime"]["id"])["state"] == "failed"


@pytest.mark.parametrize("entrypoint", ["run_once", "background"])
def test_dispatcher_maintenance_reconciles_abandoned_output_capture(
    system, monkeypatch, entrypoint
):
    calls = []
    monkeypatch.setattr(
        system["service"],
        "reconcile_abandoned_worker_output_captures",
        lambda: calls.append("reconciled") or 1,
    )
    dispatcher = Dispatcher(system["service"], system["settings"])

    async def run():
        if entrypoint == "run_once":
            assert await dispatcher.run_once() == 0
        else:
            assert dispatcher._start_background_jobs() == 0

    asyncio.run(run())
    assert calls == ["reconciled"]


@pytest.mark.parametrize("failure_phase", ["preparation", "not_submitted", "unknown", "inactive"])
def test_runtime_failure_and_recovery_boundary_roll_back_together(
    system, monkeypatch, failure_phase
):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Atomic failure settlement",
            objective="Preserve one failure and recovery transaction.",
            acceptance=[
                "A failed recovery commit cannot leave a terminal runtime without its boundary."
            ],
            completion_contract="no_artifact_expected",
        ),
    )

    class FailedAdapter(EnrollmentHandshakeAdapter):
        async def dispatch(self, runtime, message):
            await super().dispatch(runtime, message)
            return RuntimeDispatchResult(
                success=False,
                state="failed",
                error="worker_inactive_timeout"
                if failure_phase == "inactive"
                else "runtime_turn_failed",
                metadata=(
                    {"delivery_acceptance": "not_submitted", "dispatch_phase": "mcp_startup"}
                    if failure_phase == "not_submitted"
                    else {}
                ),
            )

    class Registry(EnrollmentHandshakeRegistry):
        def prepare_launch(self, _name, runtime):
            if failure_phase == "preparation":
                raise RuntimeAdapterError("runtime_unavailable")
            return runtime

    snapshots = []
    original_fail = service.fail_runtime_enrollment

    def fail(runtime_id, *, reason, _connection=None):
        assert _connection is not None and _connection.in_transaction
        before = dict(
            _connection.execute(
                "SELECT state, generation FROM worker_enrollments WHERE runtime_session_id = ?",
                (runtime_id,),
            ).fetchone()
        )
        snapshots.append((before, reason, _connection))
        return original_fail(runtime_id, reason=reason, _connection=_connection)

    def interrupted_recovery(runtime_id, *, reason, _connection=None):
        assert _connection is snapshots[-1][2]
        assert (
            _connection.execute(
                "SELECT state FROM runtime_sessions WHERE id = ?", (runtime_id,)
            ).fetchone()["state"]
            == "failed"
        )
        assert reason == (
            "runtime_unavailable"
            if failure_phase == "preparation"
            else "worker_inactive_timeout"
            if failure_phase == "inactive"
            else "runtime_dispatch_failed"
        )
        raise RuntimeError("injected recovery transaction interruption")

    monkeypatch.setattr(service, "fail_runtime_enrollment", fail)
    monkeypatch.setattr(service, "recover_terminal_worker_attempt", interrupted_recovery)
    dispatcher = Dispatcher(
        service,
        replace(system["settings"], max_dispatch_attempts=1),
        registry=Registry(FailedAdapter(service)),
    )
    with pytest.raises(RuntimeError, match="injected recovery transaction interruption"):
        asyncio.run(dispatcher.run_once())
    assert len(snapshots) == 1
    enrollment = service.db.fetchone(
        "SELECT state, generation FROM worker_enrollments WHERE runtime_session_id = ?",
        (system["runtime"]["id"],),
    )
    assert dict(enrollment) == snapshots[0][0]
    assert service.get_runtime(system["runtime"]["id"])["state"] != "failed"
    assert service.get_work(work["id"])["open_boundaries"] == []
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events WHERE event_type = 'runtime.enrollment_failed'"
        )["count"]
        == 0
    )
    source = service.db.fetchone(
        "SELECT d.state FROM message_deliveries d JOIN messages m ON m.id = d.message_id "
        "WHERE m.attempt_id = ? AND m.kind = 'assignment'",
        (work["current_attempt"]["id"],),
    )
    assert source["state"] == (
        "dead" if failure_phase in {"preparation", "not_submitted"} else "dispatched"
    )
