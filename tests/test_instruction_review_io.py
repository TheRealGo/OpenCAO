from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

import pytest

from cao_control_plane.config import Settings
from cao_control_plane.instruction_review import CodexInstructionReviewer, ReviewUnavailable
from cao_control_plane.runtime import read_cao_requester_context


@pytest.mark.asyncio
async def test_desktop_oversized_response_is_typed_for_complete_item_paging():
    import struct

    from cao_control_plane.runtime import NativeMessageTooLarge, _JsonRpcDesktopSocket
    reader = asyncio.StreamReader()
    reader.feed_data(b'\x81\x7f' + struct.pack('!Q', 2**32))
    rpc = _JsonRpcDesktopSocket(reader, None)
    with pytest.raises(NativeMessageTooLarge):
        await rpc.read_line(1)


def test_oversized_turn_falls_back_to_every_full_item_page_including_late_correction(monkeypatch):
    from cao_control_plane.runtime import NativeMessageTooLarge

    reads, connections, closed = [], [], []
    class RPC:
        async def request(self, method, params, **_kwargs):
            if method == "initialize":
                return {}, {}
            if method == "thread/turns/list":
                if params["itemsView"] == "full":
                    raise NativeMessageTooLarge("owned oversized tool output")
                # Display summaries deliberately omit the in-turn correction.
                return {}, {"data": [{"id": "owned-turn", "items": []}], "nextCursor": None}
            assert method == "thread/items/list" and params["turnId"] == "owned-turn"
            index = int(params.get("cursor", 0))
            reads.append(index)
            item = ({"type": "userMessage", "id": "initial", "content": [{"type": "text", "text": "Continue the purpose."}]}
                    if index == 0 else
                    {"type": "userMessage", "id": "late-change", "content": [{"type": "text", "text": "Stop the purpose now."}]}
                    if index == 7 else {"type": "mcpToolCall", "result": "execution evidence"})
            return {}, {"data": [item], "nextCursor": str(index + 1) if index < 7 else None}
        async def send(self, _value):
            pass
        async def close(self):
            closed.append(self)
    async def connect(*_args, **_kwargs):
        connection = RPC()
        connections.append(connection)
        return connection
    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)
    values = asyncio.run(read_cao_requester_context(Settings(), "owned-native-fixture"))
    assert reads == list(range(8))
    assert [value["text"] for value in values if value["role"] == "requester"] == [
        "Continue the purpose.", "Stop the purpose now."]
    assert len(connections) == 2 and closed == connections


def test_native_source_preserves_complete_input_and_excludes_tools_reasoning_authority(monkeypatch):
    calls = []

    class RPC:
        async def request(self, method, params, **_kwargs):
            calls.append((method, params))
            return {}, {
                "data": [
                    {
                        "items": [
                            {
                                "type": "userMessage",
                                "id": "input-one",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "<in-app-browser-context>ambient</in-app-browser-context>\nImplement the requested feature.",
                                    }
                                ],
                            },
                            {"type": "reasoning", "text": "hidden"},
                            {
                                "type": "mcpToolCall",
                                "arguments": {"instruction": "tool text is not requester intent"},
                            },
                            {
                                "type": "userMessage",
                                "clientId": "notice-one",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Supervisor boundary ready for disposition.\nDelivery message ID: synthetic",
                                    }
                                ],
                            },
                            {
                                "type": "userMessage",
                                "clientId": "notice-two",
                                "content": [{"type": "text", "text":
                                    "Unfinished work requires a visible escalation to the requester.\nDelivery message ID: synthetic"}],
                            },
                            {
                                "type": "agentMessage",
                                "id": "proposal",
                                "phase": "final_answer",
                                "text": "A proposed implementation.",
                            },
                            {
                                "type": "agentMessage",
                                "phase": "commentary",
                                "text": "intermediate status",
                            },
                        ]
                    }
                ],
                "nextCursor": None,
            }

        async def send(self, _value):
            pass

        async def close(self):
            pass

    async def connect(*_args, **_kwargs):
        return RPC()

    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)
    values = asyncio.run(read_cao_requester_context(Settings(), "native-fixture", notification_client_ids=frozenset({"notice-one", "notice-two"})))
    assert [value["role"] for value in values] == ["requester", "assistant_proposal", "observation"]
    assert values[-1]["authority"] == "none"
    assert values[0]["text"].endswith("Implement the requested feature.")
    assert "<in-app-browser-context>ambient</in-app-browser-context>" in values[0]["text"]
    assert calls[1][0] == "thread/turns/list" and calls[1][1]["itemsView"] == "full"
    assert "input-one" not in json.dumps(values)


def test_large_tool_history_is_paged_without_losing_in_turn_requester_changes(monkeypatch):
    pages = []

    class RPC:
        async def request(self, method, params, **_kwargs):
            if method == "initialize":
                return {}, {}
            # Several full turns together exceed the transport's frame bound.
            assert params["limit"] == 1 and params["itemsView"] == "full"
            index = int(params.get("cursor", 0))
            pages.append(index)
            return {}, {
                "data": [{"items": [
                    {"type": "mcpToolCall", "result": "large execution log" * 100_000},
                    {"type": "userMessage", "id": f"input-{index}",
                     "content": [{"type": "text", "text": f"Implement stage {index}."}]},
                    {"type": "agentMessage", "phase": "final_answer", "id": f"proposal-{index}",
                     "text": "The implementation will preserve the accepted scope."},
                    {"type": "userMessage", "id": f"change-{index}",
                     "content": [{"type": "text", "text": f"Correction {index}: keep the existing criteria."}]},
                    {"type": "hookPrompt", "content": [{"type": "text",
                     "text": '<hook_prompt hook_run_id="stop:fixture">Internal capture continuation.</hook_prompt>'}]},
                ]}],
                "nextCursor": str(index + 1) if index < 5 else None,
            }

        async def send(self, _value):
            pass

        async def close(self):
            pass

    async def connect(*_args, **_kwargs):
        return RPC()

    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)
    values = asyncio.run(read_cao_requester_context(Settings(), "native-fixture"))
    assert pages == list(range(6))
    assert [v["text"] for v in values if v["role"] == "requester"] == [
        text for index in reversed(range(6)) for text in (
            f"Implement stage {index}.", f"Correction {index}: keep the existing criteria."
        )
    ]
    assert "large execution log" not in json.dumps(values)
    assert "Internal capture" not in json.dumps(values)


def test_older_source_growth_reports_coverage_and_keeps_current_direction(monkeypatch):
    class RPC:
        async def request(self, method, params, **_kwargs):
            if method == "initialize":
                return {}, {}
            old = "cursor" in params
            return {}, {"data": [{"items": [
                {"type": "userMessage", "id": "old" if old else "current", "content": [
                    {"type": "text", "text": "Older context " * 20_000 if old else "Continue the approved implementation."}
                ]}
            ]}], "nextCursor": None if old else "older"}

        async def send(self, _value):
            pass

        async def close(self):
            pass

    async def connect(*_args, **_kwargs):
        return RPC()

    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)
    values = asyncio.run(read_cao_requester_context(Settings(), "native-fixture"))
    assert values[0]["text"] == ("Older context " * 20_000).rstrip()
    assert values[1]["text"] == "Continue the approved implementation."


@pytest.mark.parametrize("mode", ["valid", "malformed", "missing", "tools", "incomplete", "timeout", "start_error"])
def test_codex_runner_is_ephemeral_has_no_cao_environment_and_validates_final_output(monkeypatch, mode):
    import cao_control_plane.instruction_review as module
    captured = {}
    stopped = []
    monkeypatch.setattr(module, "_owner_review_effort", lambda: "max")

    class Writer:
        def write(self, prompt):
            captured["prompt"] = prompt
            assert b"INPUT:" in prompt
            target = Path(captured["argv"][captured["argv"].index("-o") + 1])
            if mode != "missing":
                target.write_text("invalid" if mode == "malformed" else json.dumps({"verdict": "pass", "summary": "The instruction matches the registered purpose."}))
        async def drain(self):
            pass
        def close(self):
            pass

    class Process:
        returncode = 0
        stdin = Writer()
        stderr = None
        def __init__(self):
            self.stdout = asyncio.StreamReader()
            events = [] if mode == "incomplete" else [{"type": "turn.completed", "usage": {"input_tokens": 12}}]
            if mode == "tools":
                events.insert(0, {"type": "item.started", "item": {"type": "mcp_tool_call", "server": "unowned", "tool": "write"}})
            for event in events:
                self.stdout.feed_data((json.dumps(event) + "\n").encode())
            if mode != "timeout":
                self.stdout.feed_eof()
            else:
                self.returncode = None
        async def wait(self):
            return self.returncode

    async def create(*argv, **kwargs):
        captured.update(argv=list(argv), **kwargs)
        if mode == "start_error":
            raise FileNotFoundError("private executable path")
        return Process()
    async def terminate(process):
        stopped.append(process)
        process.returncode = -15

    monkeypatch.setenv("CAO_PRIVATE_BEARER", "not-forwarded")
    monkeypatch.setenv("CODEX_THREAD_ID", "not-forwarded")
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr("cao_control_plane.runtime._terminate_runtime_process", terminate)
    monkeypatch.setattr("cao_control_plane.runtime._default_codex_app_server_command", lambda: ["codex"])
    settings = Settings(worker_inactivity_timeout_seconds=0.01)
    if mode == "valid":
        result = _ORIGINAL_REVIEW(CodexInstructionReviewer(settings), {
            "purpose": {"text": "Maintain the assigned outcome."}, "candidate": {},
            "requester_context": "ARCHIVE_SHOULD_NOT_BE_READ " * 100_000,
            "history": [{"summary": "ARCHIVE_SHOULD_NOT_BE_READ"}],
        })
        assert result.decision.verdict == "pass" and result.usage["input_tokens"] == 12
    else:
        with pytest.raises((ValueError, ReviewUnavailable, TimeoutError)) as raised:
            _ORIGINAL_REVIEW(CodexInstructionReviewer(settings), {})
        if mode in {"missing", "malformed"}:
            assert raised.value.code == ("review_output_missing" if mode == "missing" else "review_output_invalid")
            assert raised.value.model == settings.managed_worker_profile("codex").default_model
            assert raised.value.diagnostics == {"exit_code": 0}
    if mode == "timeout":
        assert len(stopped) == 1
    assert "CAO_PRIVATE_BEARER" not in captured["env"] and "CODEX_THREAD_ID" not in captured["env"]
    assert captured["start_new_session"] is True
    assert 'model_reasoning_effort="max"' in captured["argv"]
    for required in ("--ephemeral", "--ignore-user-config", "--ignore-rules", "--output-schema", "read-only"):
        assert required in captured["argv"]
    assert not any("mcp_servers" in arg for arg in captured["argv"])
    assert "code_mode_host" in captured["argv"] and "code_mode" in captured["argv"]
    if mode != "start_error":
        assert b"requester_context" not in captured["prompt"]
        assert b"ARCHIVE_SHOULD_NOT_BE_READ" not in captured["prompt"]
        assert len(captured["prompt"]) < 8000
    assert not Path(captured["cwd"]).exists()


_ORIGINAL_REVIEW = CodexInstructionReviewer.review


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("events", "exit_code", "stderr", "expected"),
    [
        ([{"type": "turn.failed", "error": {"code": "context_length_exceeded", "message": "private evidence"}}],
         1, "", "review_context_window_exceeded"),
        ([{"type": "turn.failed", "error": {"status": 429, "message": "private account"}}],
         1, "", "review_provider_rate_limited"),
        ([], 2, "unrecognized arguments: private path", "review_process_failed"),
        ([{"type": "turn.failed"}, {"type": "turn.completed"}], 0, "", "review_process_failed"),
        ([{"type": "error", "message": "temporary connection failure"}, {"type": "turn.completed"}], 0, "", None),
    ],
)
async def test_native_review_failures_have_bounded_diagnostics_and_never_accept_a_failed_turn(
    tmp_path, events, exit_code, stderr, expected,
):
    import os
    import sys

    from cao_control_plane.instruction_review import _run_review_process

    script = (
        "import json,sys; sys.stdin.read(); "
        f"sys.stderr.write({stderr!r}); "
        f"[print(json.dumps(event),flush=True) for event in {events!r}]; "
        f"sys.exit({exit_code})"
    )
    pending = _run_review_process([sys.executable, "-c", script], tmp_path, dict(os.environ), "input", 3)
    if expected is None:
        assert await pending == {}
        return
    with pytest.raises(ReviewUnavailable) as raised:
        await pending
    failure = raised.value
    assert failure.code == expected
    assert failure.diagnostics["exit_code"] == exit_code
    assert failure.diagnostics["event_count"] == len(events)
    assert "private" not in json.dumps(failure.diagnostics)


@pytest.mark.asyncio
async def test_review_stderr_is_drained_without_becoming_progress_or_persisted_text(tmp_path):
    import os
    import sys

    from cao_control_plane.instruction_review import _run_review_process

    script = (
        "import sys; sys.stdin.read(); sys.stderr.write('private evidence' * 100000); "
        "sys.stderr.flush(); print('{\"type\":\"turn.completed\"}', flush=True)"
    )
    assert await _run_review_process([sys.executable, "-c", script], tmp_path, dict(os.environ), "input", 3) == {}


@pytest.mark.asyncio
async def test_pending_instruction_review_keeps_health_route_responsive(settings, monkeypatch):
    import httpx

    from cao_control_plane.api import create_app

    app = create_app(settings)
    entered = threading.Event()
    release = threading.Event()

    def held_assignment(*_args, **_kwargs):
        entered.set()
        release.wait(timeout=5)
        return {"id": "work_fixture"}

    monkeypatch.setattr(app.state.service, "assign_work", held_assignment)
    token = app.state.bootstrap["tokens"]["cao"]["token"]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://localhost"
    ) as client:
        pending = asyncio.create_task(
            client.post(
                "/api/v1/work",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "worker_id": "worker_fixture",
                    "title": "Bounded task",
                    "objective": "Perform the requested outcome.",
                    "maturity": "exploring",
                },
            )
        )
        try:
            assert await asyncio.to_thread(entered.wait, 3)
            health = await asyncio.wait_for(client.get("/health"), timeout=2)
            assert health.status_code == 200 and not pending.done()
        finally:
            release.set()
        assert (await pending).status_code == 200


def test_notification_history_cannot_erase_standing_authority_and_checkpoint_reads_new_corrections(monkeypatch):
    calls = []
    correction = False

    class RPC:
        async def request(self, method, params, **_kwargs):
            if method == 'initialize':
                return {}, {}
            index = int(params.get('cursor', 0))
            calls.append(index)
            items = [{"type": "userMessage", "id": f"notice-{index}", "clientId": f"notice-{index}", "content": [{"type": "text", "text":
                "A system recovery Boundary requires CAO-owned reconciliation.\nDelivery message ID: fixture"}]},
                {"type": "agentMessage", "id": f"status-{index}", "phase": "final_answer", "text": "Unchanged blocker. " * 5000}]
            if index == 80:
                items = [{"type": "userMessage", "id": "original-grant", "content": [{"type": "text", "text":
                    "Continue training with the existing account and allocation; no additional spending."}]}]
            if correction and index == 0:
                items.append({"type": "userMessage", "id": "new-stop", "content": [{"type": "text", "text": "Stop training now."}]})
            return {}, {"data": [{"id": f"turn-{index}", "items": items}], "nextCursor": str(index + 1) if index < 80 else None}

        async def send(self, _value):
            pass

        async def close(self):
            pass

    async def connect(*_args, **_kwargs):
        return RPC()

    monkeypatch.setattr('cao_control_plane.runtime._JsonRpcDesktopSocket.connect', connect)
    notice_ids = frozenset(f'notice-{i}' for i in range(80))
    initial = asyncio.run(read_cao_requester_context(Settings(), 'native-fixture', notification_client_ids=notice_ids))
    assert calls == list(range(81))
    assert [v['text'] for v in initial if v['role'] == 'requester'] == [
        'Continue training with the existing account and allocation; no additional spending.'
    ]
    assert 'Unchanged blocker' not in json.dumps(initial)
    calls.clear()
    correction = True
    updated = asyncio.run(read_cao_requester_context(Settings(), 'native-fixture', checkpoint=initial, notification_client_ids=notice_ids))
    assert calls == [0]
    assert [v['text'] for v in updated if v['role'] == 'requester'][-1] == 'Stop training now.'
    assert any('existing account' in v.get('text', '') for v in updated)


def test_checkpoint_cannot_retain_retracted_requester_authority(monkeypatch):
    import hashlib

    class RPC:
        async def request(self, method, _params, **_kwargs):
            return {}, {"data": [{"id": "same-turn", "items": [{"type": "userMessage", "id": "replacement", "content": [
                {"type": "text", "text": "No training is authorized."}]}]}], "nextCursor": None}

        async def send(self, _value):
            pass

        async def close(self):
            pass

    async def connect(*_args, **_kwargs):
        return RPC()

    monkeypatch.setattr('cao_control_plane.runtime._JsonRpcDesktopSocket.connect', connect)
    previous = [{"role": "coverage", "native_head": {
        "turn_digest": hashlib.sha256(b'same-turn').hexdigest(), "requester_digests": ['old-source']}},
        {"role": "requester", "source_digest": "old-source", "text": "Training was authorized."}]
    result = asyncio.run(read_cao_requester_context(Settings(), 'native-fixture', checkpoint=previous))
    assert [v['text'] for v in result if v['role'] == 'requester'] == ['No training is authorized.']


@pytest.mark.parametrize("legacy_checkpoint", [False, True])
def test_checkpoint_keeps_late_proposal_from_notification_turn_before_requester_acceptance(monkeypatch, legacy_checkpoint):
    approved = False
    notice = {"type": "userMessage", "id": "notice", "clientId": "notice",
              "content": [{"type": "text", "text": "The draft is ready for review."}]}
    proposal = {"type": "agentMessage", "id": "displayed-draft", "phase": "final_answer",
                "text": "The exact draft to approve."}
    original = {"id": "request-turn", "items": [
        {"type": "userMessage", "id": "request", "content": [
            {"type": "text", "text": "Prepare a draft; send only after my approval."}]}]}

    class RPC:
        async def request(self, method, params, **_kwargs):
            if method == "initialize":
                return {}, {}
            assert method == "thread/turns/list"
            turns = [{"id": "notification-turn", "items": [notice, *([proposal] if approved else [])]}, original]
            if approved:
                turns.insert(0, {"id": "approval-turn", "items": [
                    {"type": "userMessage", "id": "approval", "content": [
                        {"type": "text", "text": "Send that exact draft."}]}]})
            index = int(params.get("cursor", 0))
            return {}, {"data": [turns[index]],
                        "nextCursor": str(index + 1) if index + 1 < len(turns) else None}

        async def send(self, _value):
            pass

        async def close(self):
            pass

    async def connect(*_args, **_kwargs):
        return RPC()

    monkeypatch.setattr("cao_control_plane.runtime._JsonRpcDesktopSocket.connect", connect)
    options = {"notification_client_ids": frozenset({"notice"})}
    retained = asyncio.run(read_cao_requester_context(Settings(), "native-fixture", **options))
    approved = True
    fresh = asyncio.run(read_cao_requester_context(Settings(), "native-fixture", **options))
    if legacy_checkpoint:
        # The old reader could persist a complete-looking newer checkpoint
        # after dropping the proposal. Do not inherit its missing source.
        retained = [dict(item) for item in fresh if item["role"] != "assistant_proposal"]
        retained[0]["origin_contract"] = "control-plane-native-input/v3"
    updated = asyncio.run(read_cao_requester_context(Settings(), "native-fixture", checkpoint=retained, **options))
    assert updated == fresh
    assert [item["text"] for item in updated if item["role"] == "assistant_proposal"] == [proposal["text"]]
    assert [item["text"] for item in updated if item["role"] == "requester"][-1] == "Send that exact draft."


def test_requester_source_restores_a_missing_desktop_connection_through_read_only_sdk(monkeypatch):
    from cao_control_plane.runtime import RuntimeAdapterError

    calls = []

    class Process:
        returncode = None

        def terminate(self):
            calls.append('terminate-owned-reader')
            self.returncode = 0

        async def wait(self):
            return self.returncode

    class RPC:
        def __init__(self, process):
            self.process = process

        async def request(self, method, params, **_kwargs):
            calls.append(method)
            if method == 'initialize':
                return {}, {}
            assert method == 'thread/turns/list'
            return {}, {'data': [{'items': [{'type': 'userMessage', 'id': 'standing-input', 'content': [
                {'type': 'text', 'text': 'Continue the authorized outcome.'}]}]}], 'nextCursor': None}

        async def send(self, _value):
            pass

    async def disconnected(*_args, **_kwargs):
        raise RuntimeAdapterError('connection unavailable')

    async def create(*_args, **_kwargs):
        return Process()

    monkeypatch.setattr('cao_control_plane.runtime._default_codex_app_server_command', lambda: ['codex', 'app-server'])
    monkeypatch.setattr('cao_control_plane.runtime._JsonRpcDesktopSocket.connect', disconnected)
    monkeypatch.setattr('cao_control_plane.runtime.asyncio.create_subprocess_exec', create)
    monkeypatch.setattr('cao_control_plane.runtime._JsonRpcProcess', RPC)
    values = asyncio.run(read_cao_requester_context(Settings(), 'native-fixture'))
    assert values[0]['text'] == 'Continue the authorized outcome.'
    assert calls == ['initialize', 'thread/turns/list', 'terminate-owned-reader']
