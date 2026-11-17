from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace

import pytest
from test_runtime_enrollment import _runtime, _Stdin
from test_worker_thread_lifecycle_core import (
    _attached,
    _seed_managed_thread,
    _seeded_managed_work_assignment,
)

from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import _conversation_work_projection
from cao_control_plane.models import ReportInput, RuntimeHeartbeat
from cao_control_plane.runtime import (
    CodexAppServerAdapter,
    CodexRpcError,
    Dispatcher,
    RuntimeDispatchPhaseError,
    _managed_codex_mcp_server_name,
    _ManagedWorkerActivityMonitor,
    render_message_details,
)
from cao_control_plane.runtime_recovery import (
    RuntimeRecoveryObservation,
    pending_runtime_start_diagnostics,
    record_runtime_start_diagnosis,
    recover_unobserved_worker_starts,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane
from cao_control_plane.worker_dispatch import WorkerDispatchObservation, observe_dispatch


def _capture(system):
    service = system["service"]
    actor = _attached(system)
    ids = _seed_managed_thread(system, actor, ordinal=806, native_session_id="private-source-thread")
    work = service.assign_work(actor, _seeded_managed_work_assignment(ids,
        title="Finite continuation", objective="Complete the preserved outcome.", acceptance=["Exact scope is retained."]))
    dispatcher = Dispatcher(service, system["settings"])
    delivery = dispatcher._claim_delivery()
    assert delivery["recipient_id"] == ids["principal_id"]
    service.db.execute("UPDATE message_deliveries SET state='dispatched' WHERE message_id=?", (delivery["message_id"],))
    ticket = service.issue_runtime_launch_ticket(ids["runtime_id"], attempt_id=work["current_attempt"]["id"])
    service.db.execute("UPDATE runtime_sessions SET state='busy' WHERE id=?", (ids["runtime_id"],))
    generation = service.db.fetchone("SELECT generation FROM runtime_enrollment_tickets WHERE id=?", (ticket["ticket_id"],))["generation"]
    values = dict(runtime_id=ids["runtime_id"], attempt_id=work["current_attempt"]["id"],
        delivery_message_id=delivery["message_id"], delivery_generation=delivery["generation"],
        enrollment_generation=generation, owner_token=dispatcher.owner_token)
    service.begin_worker_output_capture(**values)
    return service, ids, work, values, ticket


@pytest.mark.parametrize("fence", ["none", "effect", "owner", "accepted_input", "work_generation"])
def test_native_rebinding_keeps_logical_authority_and_rejects_unsettled_outcomes(system, fence):
    service, ids, work, values, _ = _capture(system)
    if fence == "effect":
        service.db.execute("INSERT INTO effect_operations(id,principal_id,kind,target,action,status,created_at,updated_at) "
            "VALUES('eff_binding',?,'external','opaque-target','opaque-action','unknown','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            (ids["principal_id"],))
    elif fence == "owner":
        values["owner_token"] = "other-owner"
    elif fence == "accepted_input":
        observe_dispatch(service, **values, event=WorkerDispatchObservation("queue_accepted", "private-source-thread"))
    elif fence == "work_generation":
        service.db.execute("UPDATE work_items SET generation=generation+1 WHERE id=?", (work["id"],))
    before = {table: [tuple(row) for row in service.db.fetchall(f"SELECT * FROM {table}")] for table in
              ("managed_worker_threads", "managed_worker_specs", "goal_revisions", "effect_operations", "message_deliveries")}
    event = WorkerDispatchObservation("thread_rebound", "private-new-thread", "private-source-thread")
    if fence == "none":
        observe_dispatch(service, **values, event=event)
        assert service.get_runtime(ids["runtime_id"])["native_session_id"] == "private-new-thread"
        assert service.get_work(work["id"])["managed_worker_thread_id"] == ids["thread_id"]
        audit = service.db.fetchone("SELECT data_json FROM events WHERE event_type='worker.runtime_thread_rebound'")
        assert "private-source-thread" not in audit["data_json"] and "private-new-thread" not in audit["data_json"]
    else:
        with pytest.raises(ConflictError):
            observe_dispatch(service, **values, event=event)
        assert service.get_runtime(ids["runtime_id"])["native_session_id"] == "private-source-thread"
    assert before == {table: [tuple(row) for row in service.db.fetchall(f"SELECT * FROM {table}")] for table in before}


def test_restart_surfaces_unobserved_dispatch_once_without_replaying_or_revoking(system):
    service, ids, work, values, _ = _capture(system)
    service.db.execute("UPDATE message_deliveries SET updated_at='2000-01-01T00:00:00Z', "
        "lease_until='2999-01-01T00:00:00Z' WHERE message_id=?", (values["delivery_message_id"],))
    assert recover_unobserved_worker_starts(service, dispatch_owner="another-live-dispatcher") == 0
    service.db.execute("UPDATE message_deliveries SET lease_until='2000-01-01T00:00:00Z' WHERE message_id=?", (values["delivery_message_id"],))
    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_unavailable")
    before_runtime = service.get_runtime(ids["runtime_id"])
    assert recover_unobserved_worker_starts(service, dispatch_owner=values["owner_token"]) == 0
    assert recover_unobserved_worker_starts(service, dispatch_owner="new-dispatcher") == 1
    assert recover_unobserved_worker_starts(service, dispatch_owner="new-dispatcher") == 0
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor" and current["attention_owner"] == "cao"
    assert current["open_boundaries"][0]["recovery_action"] == "system_reconciliation"
    assert current["requester_escalation"] is None
    delivery = service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?", (values["delivery_message_id"],))
    assert delivery["state"] == "dispatched" and delivery["last_error"] == "runtime_assignment_start_timeout"
    assert service.get_runtime(ids["runtime_id"]) == before_runtime
    assert service.db.fetchone("SELECT COUNT(*) n FROM attempts WHERE work_item_id=?", (work["id"],))["n"] == 1


def test_live_bridge_without_start_gets_one_notice_and_late_report_remains_valid(system):
    service, ids, work, values, ticket = _capture(system)
    exchange = service.exchange_runtime_launch_ticket(ticket["ticket"])
    actor = service.authenticate(exchange["token"])
    service.record_mcp_tool_discovery(actor, protocol_version="2025-06-18", tool_names=WORKER_MCP_REQUIRED_TOOLS)
    service.heartbeat_runtime(actor, ids["runtime_id"], RuntimeHeartbeat(
        expected_enrollment_generation=actor["_enrollment_generation"], sequence=1))
    service.db.execute("UPDATE message_deliveries SET updated_at='2000-01-01T00:00:00Z', "
        "lease_until='2000-01-01T00:00:00Z' WHERE message_id=?", (values["delivery_message_id"],))
    before = service.get_runtime(ids["runtime_id"])
    assert recover_unobserved_worker_starts(service, dispatch_owner="new-dispatcher") == 1
    assert recover_unobserved_worker_starts(service, dispatch_owner="new-dispatcher") == 0
    current = service.get_work(work["id"])
    assert current["generation"] == work["generation"] and current["state"] == "active"
    assert current["open_boundaries"] == []
    assert current["current_attempt"]["state"] == "assigned"
    assert current["current_attempt"]["activity"]["provider_start_watch"]["source"] == "system_watch"
    notice = service.db.fetchone("SELECT * FROM messages WHERE work_item_id=? "
        "AND json_extract(payload_json,'$.action')='worker_start_unconfirmed'", (work["id"],))
    assert notice is not None
    selected = Dispatcher(service, system["settings"])._claim_delivery()
    assert selected is not None and selected["message_id"] == notice["id"]
    assert selected["recipient_id"] == work["supervisor_id"]
    rendered = render_message_details({**dict(notice), "payload": json.loads(notice["payload_json"])})
    assert "CAO owns diagnosis" in rendered and "no confirmed start" in rendered
    assert service.get_runtime(ids["runtime_id"]) == before
    attempt = work["current_attempt"]
    service.report(actor, attempt["id"], ReportInput(
        expected_goal_version=work["goal_version"], expected_generation=work["generation"],
        expected_goal_packet_digest=attempt["goal_packet_digest"], expected_task_packet_digest=attempt["task_packet_digest"],
        kind="progress", summary="The scoped local inspection has started.",
        evidence=[{"kind": "local_inspection", "result": "The assigned local source was inspected."}],
        progress_event="execution_started",
    ))
    assert service.get_work(work["id"])["current_attempt"]["activity"]["execution_start"] is not None
    assert recover_unobserved_worker_starts(service, dispatch_owner="new-dispatcher") == 0


def test_turn_submission_cannot_wait_forever_on_healthy_mcp(settings, tmp_path, monkeypatch):
    monkeypatch.setattr("cao_control_plane.runtime.shutil.which", lambda _name: None)
    monkeypatch.setattr("cao_control_plane.runtime._BUNDLED_CODEX_EXECUTABLES", ())
    runtime = _runtime(tmp_path / "fixture.sock")
    runtime["metadata"]["command"] = ["fixture-codex", "app-server", "--stdio"]
    message = {"id": "msg-start-deadline", "kind": "instruction", "payload": {"message": "go"}}
    server_name = _managed_codex_mcp_server_name(runtime)
    writes = []

    class HealthyMonitor(_ManagedWorkerActivityMonitor):
        def __init__(self):
            pass

        async def wait_for_failure(self):
            await asyncio.Event().wait()
            return "worker_inactive_timeout"

    class Process:
        def __init__(self):
            self.pid = 1
            self.returncode = 0
            self.stdin = _Stdin()
            writes.append(self.stdin.writes)
            self.stdout = asyncio.StreamReader()
            self.stderr = None
            for value in (
                {"id": 1, "result": {}}, {"id": 2, "result": {"thread": {"id": "native-deadline"}}},
                {"method": "mcpServer/startupStatus/updated", "params": {
                    "threadId": "native-deadline", "name": server_name, "status": "ready"}},
            ):
                self.stdout.feed_data((json.dumps(value) + "\n").encode())

    async def spawn(*_args, **_kwargs):
        assert _args[:3] == ("fixture-codex", "app-server", "--stdio")
        return Process()

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    runtime["_managed_worker_activity_monitor"] = HealthyMonitor()
    with pytest.raises(RuntimeDispatchPhaseError) as failure:
        asyncio.run(CodexAppServerAdapter(replace(settings, runtime_mcp_startup_timeout_seconds=.05)).dispatch(runtime, message))
    assert failure.value.metadata["failure_code"] == "runtime_assignment_start_timeout"
    assert failure.value.metadata["delivery_acceptance"] == "submitted"
    assert failure.value.metadata["delivery_method"] == "direct_turn"
    assert sum(json.loads(value).get("method") == "turn/start" for value in writes[0]) == 1
    assert not any(json.loads(value).get("method") == "thread/queue/add" for value in writes[0])


def test_provider_rpc_errors_keep_only_fixed_codes():
    code = CodexRpcError({"code": -32600, "message": "thread 00000000-0000-0000-0000-000000000001 already has an active writer"})
    assert str(code) == "runtime_thread_writer_conflict"
    private = CodexRpcError({"code": -32600, "message": "private-provider-payload /private/secret"})
    assert str(private) == "runtime_rpc_rejected"
    assert "private-provider" not in repr(private)


@pytest.mark.parametrize(("payload", "expected"), [
    ({"code": -32603, "message": "failed to read thread: invalid thread-store request: no rollout found for thread id 00000000-0000-0000-0000-000000000001"}, "runtime_thread_not_found"),
    ({"code": -32600, "message": "session 00000000-0000-0000-0000-000000000001 is archived. Run `codex unarchive 00000000-0000-0000-0000-000000000001` to unarchive it first."}, "runtime_thread_archived"),
    ({"code": -32603, "message": "failed to read thread: private provider error"}, "runtime_rpc_rejected"),
    ({"code": -32600, "message": "session 00000000-0000-0000-0000-000000000001 is archived. Run `codex unarchive 00000000-0000-0000-0000-000000000002` to unarchive it first."}, "runtime_rpc_rejected"),
])
def test_historical_thread_rpc_errors_have_exact_bounded_classification(payload, expected):
    error = CodexRpcError(payload)
    assert error.failure_code == expected
    assert str(error) == expected


def test_handled_start_notice_keeps_exact_quiet_diagnostic_watch(system):
    service, ids, work, values, _ = _capture(system)
    service.db.execute("UPDATE message_deliveries SET updated_at='2000-01-01T00:00:00Z', "
        "lease_until=NULL WHERE message_id=?", (values["delivery_message_id"],))
    assert recover_unobserved_worker_starts(service, dispatch_owner="replacement-dispatcher") == 1
    service.db.execute("UPDATE message_deliveries SET state='handled' WHERE message_id IN "
        "(SELECT id FROM messages WHERE kind='system')")
    [candidate] = pending_runtime_start_diagnostics(service)
    tables = ("runtime_sessions", "attempts", "work_items", "worker_enrollments", "effect_operations")
    before = {table: [tuple(row) for row in service.db.fetchall(f"SELECT * FROM {table}")] for table in tables}
    observation = RuntimeRecoveryObservation("blocked", "runtime_assignment_pending", "c" * 64, "pending", True, True)
    assert record_runtime_start_diagnosis(service, candidate, observation)
    current = service.get_work(work["id"])
    diagnosis = _conversation_work_projection(current)["current_attempt"]["activity"]["provider_start_diagnosis"]
    assert diagnosis["input_state"] == "pending" and diagnosis["other_pending_input"] is True
    assert diagnosis["history_complete"] is True and diagnosis["watch_active"] is True
    assert "private-source-thread" not in json.dumps(diagnosis)
    seq = service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"]
    reloaded = ControlPlane(service.db, system["settings"])
    [candidate] = pending_runtime_start_diagnostics(reloaded)
    assert not record_runtime_start_diagnosis(reloaded, candidate, observation)
    assert service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"] == seq
    changed = RuntimeRecoveryObservation("ready", "runtime_assignment_terminal", "d" * 64, "terminal", False, True)
    assert record_runtime_start_diagnosis(reloaded, candidate, changed)
    assert not record_runtime_start_diagnosis(reloaded, candidate, changed)
    assert record_runtime_start_diagnosis(reloaded, candidate, observation)
    assert service.db.fetchone("SELECT COUNT(*) n FROM messages WHERE "
        "json_extract(payload_json,'$.action')='worker_start_diagnosed'")["n"] == 3
    assert before == {table: [tuple(row) for row in service.db.fetchall(f"SELECT * FROM {table}")] for table in tables}
    notice = service.db.fetchone("SELECT * FROM messages WHERE json_extract(payload_json,'$.action')='worker_start_diagnosed'")
    rendered = render_message_details({**dict(notice), "payload": json.loads(notice["payload_json"])})
    assert "provider_start_diagnosis" in rendered and "after this notice is handled" in rendered
    assert "Choose and invoke the permitted next recovery operation" in rendered
    assert "not a blanket veto" in rendered and "do not request the same authority again" in rendered
    assert service.db.fetchone("SELECT state FROM message_deliveries WHERE message_id=?", (values["delivery_message_id"],))["state"] == "dispatched"
    service.db.execute("UPDATE runtime_sessions SET native_session_id='a-new-private-binding' WHERE id=?", (ids["runtime_id"],))
    assert not record_runtime_start_diagnosis(service, candidate, observation)


@pytest.mark.parametrize("fence", ["work_generation", "finished", "worker_report"])
def test_start_diagnosis_racing_authoritative_change_is_inert(system, fence):
    service, _ids, work, values, _ = _capture(system)
    service.db.execute("UPDATE message_deliveries SET updated_at='2000-01-01T00:00:00Z', lease_until=NULL WHERE message_id=?",
        (values["delivery_message_id"],))
    recover_unobserved_worker_starts(service, dispatch_owner="replacement-dispatcher")
    [candidate] = pending_runtime_start_diagnostics(service)
    if fence == "work_generation":
        service.db.execute("UPDATE work_items SET generation=generation+1 WHERE id=?", (work["id"],))
    elif fence == "finished":
        service.db.execute("UPDATE work_items SET state='canceled' WHERE id=?", (work["id"],))
    else:
        service.db.execute("UPDATE attempts SET state='working' WHERE id=?", (values["attempt_id"],))
    seq = service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"]
    assert not record_runtime_start_diagnosis(service, candidate,
        RuntimeRecoveryObservation("blocked", "runtime_assignment_pending", "c" * 64, "pending", True, True))
    assert service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"] == seq


@pytest.mark.parametrize("mode,expected", [
    ("pending", "pending"), ("running", "running"), ("terminal", "terminal"),
    ("missing_chronology", "ambiguous"), ("wrong_role", "unmatched"),
    ("duplicate", "ambiguous"), ("pending_and_turn", "ambiguous"), ("partial", "unmatched"),
    ("beyond_five", "terminal"), ("late_duplicate", "ambiguous"),
])
def test_read_only_input_diagnosis_uses_exact_structured_identity_and_chronology(mode, expected):
    methods = []
    turn = {"id": "native-turn", "status": "completed", "startedAt": 1, "completedAt": 2,
        "items": [{"type": "userMessage", "clientId": "exact-client"}]}
    queued = [{"clientUserMessageId": "exact-client"}] if mode in {"pending", "pending_and_turn"} else []
    if mode == "running":
        turn.update(status="inProgress", completedAt=None)
    elif mode == "missing_chronology":
        turn["completedAt"] = None
    elif mode == "wrong_role":
        turn["items"][0]["type"] = "agentMessage"

    class Rpc:
        async def request(self, method, params, **kwargs):
            methods.append(method)
            assert method == "thread/turns/list"
            turns = [] if mode in {"pending", "partial"} or len(methods) == 1 else [turn]
            if mode == "beyond_five":
                turns = [turn] if len(methods) == 6 else []
            if mode == "late_duplicate" and len(methods) in {3, 4, 5}:
                turns = []
            if mode == "late_duplicate" and len(methods) == 6:
                turns = [dict(turn, id="late-other-turn")]
            if mode == "duplicate" and len(methods) == 2:
                turns.append(dict(turn, id="other-turn"))
            return 1, {"data": turns, "nextCursor": str(len(methods)) if (mode in {"partial", "beyond_five", "late_duplicate"} and len(methods) < 6) or len(methods) == 1 else None}

    state, complete, _ = asyncio.run(CodexAppServerAdapter._worker_input_diagnosis(
        Rpc(), "private-native", "exact-client", queued, lambda: 1.0))
    assert state == expected
    assert complete is True
    assert len(methods) == (6 if mode in {"partial", "beyond_five", "late_duplicate"} else 2)


def test_provider_stderr_cannot_block_binding_or_turn(settings):
    program = """
import json,sys
sys.stderr.write('discarded-provider-body' * 100000)
sys.stderr.flush()
for raw in sys.stdin:
    request=json.loads(raw)
    if 'id' not in request: continue
    method=request['method']
    result={'thread':{'id':'fixture-native'}} if method=='thread/start' else {'turn':{'id':'fixture-turn'}} if method=='turn/start' else {}
    print(json.dumps({'id':request['id'],'result':result}),flush=True)
    if method=='turn/start':
        print(json.dumps({'method':'turn/completed','params':{'turn':{'id':'fixture-turn','status':'completed'}}}),flush=True)
"""
    result = asyncio.run(CodexAppServerAdapter(replace(settings, runtime_mcp_startup_timeout_seconds=2)).dispatch(
        {"metadata": {"command": [sys.executable, "-c", program]}},
        {"id": "stderr-fixture", "kind": "instruction", "payload": {"message": "Finite local protocol test."}},
    ))
    assert result.success
    assert "discarded-provider-body" not in str(result)


@pytest.mark.parametrize("status,completed", [("inProgress", None), ("interrupted", None), ("completed", None)])
def test_writer_repair_never_forks_an_unsettled_source(settings, status, completed):
    methods = []

    class Rpc:
        async def request(self, method, params, **kwargs):
            methods.append(method)
            if method == "thread/queue/list":
                return 1, {"data": [], "nextCursor": None}
            return 2, {"data": [{"id": "source-turn", "status": status, "startedAt": 1, "completedAt": completed}]}

    with pytest.raises(Exception, match="runtime_thread_source_unsettled"):
        asyncio.run(CodexAppServerAdapter._fork_idle_worker_thread(
            Rpc(), native_thread_id="source-native", config={}, remaining=lambda: 1.0, observer=lambda _: None,
        ))
    assert "thread/fork" not in methods


@pytest.mark.parametrize("source,expected", [
    ("absent", "not_submitted"), ("pending", "ambiguous"), ("terminal", "ambiguous"),
])
def test_proven_non_submission_is_not_an_unmatched_input_or_permission_to_ignore_conflicts(source, expected):
    class Rpc:
        async def request(self, method, params, **kwargs):
            assert method == "thread/turns/list"
            turn = {"id": "exact-turn", "status": "completed", "startedAt": 1, "completedAt": 2,
                    "items": [{"type": "userMessage", "clientId": "exact-client"}]}
            return 1, {"data": [turn] if source == "terminal" else [], "nextCursor": None}

    queued = [{"clientUserMessageId": "exact-client"}] if source == "pending" else []
    state, complete, _ = asyncio.run(CodexAppServerAdapter._worker_input_diagnosis(
        Rpc(), "private-native", "exact-client", queued, lambda: 1.0,
        not_submitted_proven=True,
    ))
    assert state == expected and complete
