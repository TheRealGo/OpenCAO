from __future__ import annotations

import asyncio
import base64
import json
import os
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import pytest

from cao_control_plane.private_policy import OwnerPrivatePolicyEdge, PlacementBinding
from cao_control_plane.runtime import (
    _default_claude_command,
    _default_codex_app_server_command,
    _JsonRpcProcess,
)
from cao_control_plane.security import CONTROL_PLANE_SECRET_PATTERN

LIVE_RUN_ENV = "CAO_RUN_LIVE_VENDOR_E2E"


async def _create_persisted_native_cao_thread(*, cwd: Path) -> str:
    """Create the already-existing native conversation used by the release E2E.

    App-server does not create a durable rollout for ``thread/start`` alone.
    One deliberately tiny subscription-backed turn therefore establishes the
    existing conversation before a later host resume enables the MCP bridge.
    """

    env = _phase_one_environment(os.environ)
    process = await asyncio.create_subprocess_exec(
        *_default_codex_app_server_command(),
        cwd=str(cwd),
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    rpc = _JsonRpcProcess(process, trusted_mcp_servers=frozenset())
    try:
        _, _ = await rpc.request(
            "initialize",
            {
                "clientInfo": {"name": "cao-live-vendor-model-e2e", "version": "1"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await rpc.send({"method": "initialized", "params": {}})
        _, started = await rpc.request(
            "thread/start",
            {
                "model": "gpt-5.6-terra",
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "config": {"model_reasoning_effort": "high"},
            },
        )
        thread = started.get("thread", started)
        assert isinstance(thread, dict)
        thread_id = str(thread.get("id") or thread.get("threadId") or "")
        assert thread_id
        request_id = rpc.next_id
        rpc.next_id += 1
        await rpc.send(
            {
                "id": request_id,
                "method": "turn/start",
                "params": {
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": "Reply exactly READY."}],
                },
            }
        )
        deadline = asyncio.get_running_loop().time() + 60.0
        while True:
            value = await rpc.read_line(deadline - asyncio.get_running_loop().time())
            if await rpc._handle_server_request(value):
                continue
            rpc._observe_notification(value)
            if value.get("method") == "turn/completed":
                turn = value.get("params", {}).get("turn", {})
                assert turn.get("status") in {"completed", "succeeded", "success"}, turn
                break
        return thread_id
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()


def _phase_one_environment(environ: dict[str, str] | os._Environ[str]) -> dict[str, str]:
    value = dict(environ)
    value.pop("CODEX_THREAD_ID", None)
    value.pop("CAO_A2A_TOKEN", None)
    return value


def _phase_two_environment(
    environ: dict[str, str] | os._Environ[str], thread_id: str
) -> dict[str, str]:
    assert thread_id and "\x00" not in thread_id
    value = _phase_one_environment(environ)
    value["CODEX_THREAD_ID"] = thread_id
    return value


def _require_exact_resumed_thread(value: Any, expected_thread_id: str) -> None:
    assert isinstance(value, dict)
    actual = str(value.get("id") or value.get("threadId") or "")
    assert actual == expected_thread_id, "app-server resumed a different native thread"


def test_two_phase_host_binding_scrubs_outer_thread_and_rejects_replay() -> None:
    outer = "outer-thread"
    created = "created-thread"
    inherited = {"CODEX_THREAD_ID": outer, "CAO_A2A_TOKEN": "admin-token"}
    phase_one = _phase_one_environment(inherited)
    assert "CODEX_THREAD_ID" not in phase_one
    assert "CAO_A2A_TOKEN" not in phase_one
    resumed = _phase_two_environment(inherited, created)
    assert resumed["CODEX_THREAD_ID"] == created
    assert resumed["CODEX_THREAD_ID"] != outer
    with pytest.raises(AssertionError, match="different native thread"):
        _require_exact_resumed_thread({"id": outer}, created)


def _logical_control_plane_secrets(database_path: Path) -> list[str]:
    """Return complete bearer-shaped values visible through SQLite rows.

    Searching a short prefix in raw pages is probabilistic because encrypted
    hashes are arbitrary bytes.  Logical values plus the exact minted secrets
    give a deterministic persistence boundary without that false positive.
    """

    discovered: list[str] = []
    connection = sqlite3.connect(database_path)
    try:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        for (table_name,) in tables:
            quoted_table = str(table_name).replace('"', '""')
            rows = connection.execute(f'SELECT * FROM "{quoted_table}"').fetchall()
            for row in rows:
                for value in row:
                    if isinstance(value, bytes):
                        rendered = value.decode("utf-8", errors="ignore")
                    elif isinstance(value, str):
                        rendered = value
                    else:
                        continue
                    discovered.extend(CONTROL_PLANE_SECRET_PATTERN.findall(rendered))
    finally:
        connection.close()
    return discovered


def test_logical_secret_scan_uses_complete_values_not_raw_prefixes(tmp_path: Path) -> None:
    database_path = tmp_path / "secret-scan.sqlite3"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("CREATE TABLE sample(payload BLOB, note TEXT)")
        connection.execute(
            "INSERT INTO sample(payload, note) VALUES(?, ?)",
            (b"random-bytes-cao.rtc_-not-a-complete-token", "safe"),
        )
        connection.commit()
        assert _logical_control_plane_secrets(database_path) == []
        connection.execute(
            "UPDATE sample SET note = ?",
            ("embedded cao.rtc_example.complete-bearer value",),
        )
        connection.commit()
    finally:
        connection.close()

    assert _logical_control_plane_secrets(database_path) == ["cao.rtc_example.complete-bearer"]


def _write_live_owner_policy(tmp_path: Path, workspace: Path) -> Path:
    state = tmp_path / "owner-private"
    state.mkdir(mode=0o700)
    workspace.mkdir()
    # A managed workspace must have an explicit lifecycle identity.  This is
    # the persistent main worktree case, so close records all four cleanup
    # categories as provider-signed not-applicable instead of silently
    # treating an arbitrary directory as either disposable or persistent.
    subprocess.run(
        ["git", "init", "-q", "-b", "main"],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
    )
    denied_for_codex = tmp_path / "denied-for-codex"
    denied_for_codex.mkdir()
    policy = state / "placement.json"
    policy.write_text(
        json.dumps(
            {
                "policy_id": "live-e2e",
                "policy_version": "1",
                "evidence_key": base64.urlsafe_b64encode(b"e" * 32).decode().rstrip("="),
                "workspaces": {"live": str(workspace)},
                "runners": {
                    "codex": {
                        "allow_within": [str(workspace)],
                        "deny_within": [str(denied_for_codex)],
                    },
                    "claude": {
                        "allow_within": [str(workspace)],
                        "deny_within": [str(denied_for_codex)],
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    policy.chmod(0o600)
    return policy


def test_live_owner_policy_allows_only_the_bound_codex_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    policy = _write_live_owner_policy(tmp_path, workspace)
    edge = OwnerPrivatePolicyEdge(policy)
    decision = edge.evaluate(
        PlacementBinding("principal-1", "runtime-1", "assignment-1", "work-1", "codex", 1),
        workspace,
    )
    assert decision.decision == "allow"


def _require_subscription_auth(*, require_claude: bool = False) -> None:
    assert not os.environ.get("OPENAI_API_KEY"), "live test refuses OpenAI API-key billing"
    assert not os.environ.get("ANTHROPIC_API_KEY"), "live test refuses Anthropic API-key billing"

    if require_claude:
        claude = subprocess.run(
            [*_default_claude_command(), "auth", "status", "--json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert claude.returncode == 0, "Claude authentication status is unavailable"
        claude_status = json.loads(claude.stdout)
        assert claude_status.get("loggedIn") is True, "Claude must be logged in"
        assert claude_status.get("authMethod") == "claude.ai", (
            "Claude must use subscription authentication"
        )
        assert claude_status.get("subscriptionType"), "Claude subscription is unavailable"
        return

    codex = subprocess.run(
        [_default_codex_app_server_command()[0], "login", "status"],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    codex_status = f"{codex.stdout}\n{codex.stderr}"
    assert codex.returncode == 0 and "ChatGPT" in codex_status, (
        "Codex must use ChatGPT subscription authentication"
    )


def test_subscription_gate_refuses_api_key_billing_before_vendor_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The opt-in path must never silently fall back to API-key billing."""

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    calls = 0

    def unexpected_vendor_command(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        del args, kwargs
        calls += 1
        raise AssertionError("subscription validation must fail before a vendor command")

    monkeypatch.setattr(subprocess, "run", unexpected_vendor_command)
    with pytest.raises(AssertionError, match="API-key billing"):
        _require_subscription_auth()
    assert calls == 0


def test_live_vendor_e2e_is_explicitly_opt_in() -> None:
    """Keep the release check collectable while making paid turns opt-in only."""

    assert LIVE_RUN_ENV == "CAO_RUN_LIVE_VENDOR_E2E"


def test_live_vendor_objective_does_not_supply_the_reporting_protocol() -> None:
    """The shared Assignment renderer, not task wording, must drive MCP reporting."""

    objective = _completion_objective("LIVE_GENERIC_REPORT_OK")

    assert "cao_" not in objective
    assert "MCP" not in objective
    assert "attempt" not in objective.lower()
    assert "packet" not in objective.lower()


def _completion_objective(marker: str) -> str:
    return (
        f"Finish this no-effect check and report {marker!r} as the exact completion "
        "summary. Do not read or write files, run commands, use external network access, "
        "or create artifacts."
    )
