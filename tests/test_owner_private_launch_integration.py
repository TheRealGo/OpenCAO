from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from cao_control_plane.close_contract import CleanupTargetKind
from cao_control_plane.close_inventory_edge import CloseInventoryProviderError
from cao_control_plane.config import Settings
from cao_control_plane.models import WorkAssignment
from cao_control_plane.private_policy import (
    OwnerPrivatePolicyEdge,
    PlacementBinding,
    PrivatePolicyError,
    ensure_default_dynamic_workspace_policy,
)
from cao_control_plane.runtime import (
    ClaudeAdapter,
    CodexAppServerAdapter,
    Dispatcher,
    OwnerPrivateLaunchBlocked,
    _OwnerPrivateLaunchGate,
)

_SENTINEL = "owner-private-workspace-must-not-reach-durable-state"


def _write_policy(
    tmp_path: Path,
    *,
    claude_allow: Path,
    codex_deny: Path,
    workspaces: dict[str, Path],
    version: str = "1",
) -> Path:
    state = tmp_path / "owner-private-state"
    state.mkdir(exist_ok=True)
    state.chmod(0o700)
    policy = state / "placement.json"
    policy.write_text(
        json.dumps(
            {
                "policy_id": "placement-policy",
                "policy_version": version,
                "evidence_key": base64.urlsafe_b64encode(b"e" * 32)
                .decode("ascii")
                .rstrip("="),
                "workspaces": {name: str(path) for name, path in workspaces.items()},
                "runners": {
                    "claude": {"allow_within": [str(claude_allow)], "deny_within": []},
                    "codex": {"allow_within": [], "deny_within": [str(codex_deny)]},
                },
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    policy.chmod(0o600)
    return policy


def _gate(
    policy: Path,
    workspace: Path,
    *,
    runner: str,
    attempt: str = "attempt-1",
    generation: int = 1,
) -> _OwnerPrivateLaunchGate:
    edge = OwnerPrivatePolicyEdge(policy)
    binding = PlacementBinding(
        principal_id="principal-1",
        runtime_id="runtime-1",
        assignment_id=attempt,
        work_item_id="work-1",
        runner_adapter=runner,  # type: ignore[arg-type]
        launch_generation=generation,
    )
    return _OwnerPrivateLaunchGate(edge, binding, edge.evaluate(binding, workspace), workspace)


def _runner_program(adapter_name: str) -> str:
    if adapter_name == "claude":
        return (
            "import json,os,pathlib;"
            "pathlib.Path('.cao-test-cwd').write_text(os.getcwd());"
            'print(json.dumps({"session_id":"s","result":"ok"}))'
        )
    return """
import json
import os
import pathlib
import sys
pathlib.Path(".cao-test-cwd").write_text(os.getcwd())
for line in sys.stdin:
    request = json.loads(line)
    if "id" not in request:
        continue
    method = request.get("method")
    if method == "initialize":
        result = {}
    elif method == "thread/start":
        result = {"thread": {"id": "thread-1"}}
    elif method == "turn/start":
        print(json.dumps({"id": request["id"], "result": {"turn": {"id": "turn-1"}}}), flush=True)
        print(json.dumps({"method": "turn/completed", "params": {"turn": {"id": "turn-1", "status": "completed"}}}), flush=True)
        continue
    else:
        result = {}
    print(json.dumps({"id": request["id"], "result": result}), flush=True)
"""


@pytest.mark.parametrize("adapter_name", ["claude", "codex"])
def test_allowed_codex_and_claude_launch_only_with_private_resolved_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, adapter_name: str
) -> None:
    project_a = tmp_path / "project-a"
    project_b = tmp_path / "project-b"
    denied = tmp_path / "denied"
    project_a.mkdir()
    project_b.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=project_a,
        codex_deny=denied,
        workspaces={"project-a-ref": project_a, "project-b-ref": project_b},
    )
    edge = OwnerPrivatePolicyEdge(policy)
    assert edge.resolve_workspace("project-a-ref") == project_a.resolve()
    assert edge.resolve_workspace("project-b-ref") == project_b.resolve()
    gate = _gate(policy, project_a, runner=adapter_name)
    original_spawn = asyncio.create_subprocess_exec
    launch_cwds: list[str | None] = []
    launch_pass_fds: list[tuple[int, ...]] = []

    async def spawn(*argv: str, **kwargs: Any) -> asyncio.subprocess.Process:
        launch_cwds.append(kwargs.get("cwd"))
        launch_pass_fds.append(kwargs.get("pass_fds", ()))
        assert str(project_a) not in argv
        return await original_spawn(*argv, **kwargs)

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    adapter = ClaudeAdapter(Settings()) if adapter_name == "claude" else CodexAppServerAdapter(Settings())
    result = asyncio.run(
        adapter.dispatch(
            {
                "metadata": {"command": [sys.executable, "-c", _runner_program(adapter_name)]},
                "_owner_private_launch_gate": gate,
            },
            {"kind": "instruction", "payload": {"message": "go"}},
        )
    )
    assert result.success
    assert launch_cwds == [None]
    assert len(launch_pass_fds) == 1 and len(launch_pass_fds[0]) == 1
    assert (project_a / ".cao-test-cwd").read_text() == str(project_a.resolve())


@pytest.mark.parametrize("adapter_name", ["claude", "codex"])
def test_launch_cwd_cannot_be_redirected_after_final_policy_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, adapter_name: str
) -> None:
    approved = tmp_path / "approved"
    moved = tmp_path / "approved-object"
    denied = tmp_path / "denied"
    approved.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=approved,
        codex_deny=denied,
        workspaces={"approved-ref": approved},
    )
    gate = _gate(policy, approved, runner=adapter_name)
    original_spawn = asyncio.create_subprocess_exec
    inherited_fds: list[int] = []

    async def swap_then_spawn(
        *argv: str, **kwargs: Any
    ) -> asyncio.subprocess.Process:
        inherited_fds.extend(kwargs.get("pass_fds", ()))
        approved.rename(moved)
        approved.symlink_to(denied, target_is_directory=True)
        return await original_spawn(*argv, **kwargs)

    monkeypatch.setattr(
        "cao_control_plane.runtime.asyncio.create_subprocess_exec", swap_then_spawn
    )
    adapter = (
        ClaudeAdapter(Settings())
        if adapter_name == "claude"
        else CodexAppServerAdapter(Settings())
    )

    result = asyncio.run(
        adapter.dispatch(
            {
                "metadata": {
                    "command": [
                        sys.executable,
                        "-c",
                        _runner_program(adapter_name),
                    ]
                },
                "_owner_private_launch_gate": gate,
            },
            {"kind": "instruction", "payload": {"message": "go"}},
        )
    )

    assert result.success
    assert (moved / ".cao-test-cwd").read_text() == str(moved.resolve())
    assert not (denied / ".cao-test-cwd").exists()
    assert len(inherited_fds) == 1
    with pytest.raises(OSError):
        os.fstat(inherited_fds[0])


def test_cross_work_decision_replay_never_reaches_process_creation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    approved = tmp_path / "approved"
    denied = tmp_path / "denied"
    approved.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=approved,
        codex_deny=denied,
        workspaces={"project-ref": approved},
    )
    original = _gate(policy, approved, runner="claude", attempt="attempt-1")
    replay_binding = replace(
        original.binding,
        assignment_id="attempt-2",
        work_item_id="work-2",
    )
    replay = _OwnerPrivateLaunchGate(
        original.edge,
        replay_binding,
        original.decision,
        approved,
    )
    calls = 0

    async def spawn(*_argv: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("replayed decision must precede process creation")

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    with pytest.raises(OwnerPrivateLaunchBlocked):
        asyncio.run(
            ClaudeAdapter(Settings()).dispatch(
                {
                    "metadata": {"command": [sys.executable, "-c", "raise SystemExit(0)"]},
                    "_owner_private_launch_gate": replay,
                },
                {"kind": "instruction", "payload": {}},
            )
        )
    assert calls == 0


def test_dynamic_directory_symlink_swap_at_process_boundary_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "approved"
    denied = tmp_path / "denied"
    workspace.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=workspace,
        codex_deny=denied,
        workspaces={},
    )
    registry = policy.parent / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    workspace_ref = edge.register_workspace(workspace, runner="claude")
    binding = PlacementBinding(
        principal_id="principal-1",
        runtime_id="runtime-1",
        assignment_id="attempt-1",
        work_item_id="work-1",
        runner_adapter="claude",
        launch_generation=1,
    )
    gate = _OwnerPrivateLaunchGate(
        edge,
        binding,
        edge.evaluate(binding, workspace),
        workspace.resolve(),
        workspace_ref,
    )
    moved = tmp_path / "approved-original"
    workspace.rename(moved)
    workspace.symlink_to(moved, target_is_directory=True)
    calls = 0

    async def spawn(*_argv: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("registry recheck must precede process creation")

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    with pytest.raises(OwnerPrivateLaunchBlocked):
        asyncio.run(
            ClaudeAdapter(Settings()).dispatch(
                {
                    "metadata": {
                        "command": [sys.executable, "-c", "raise SystemExit(0)"]
                    },
                    "_owner_private_launch_gate": gate,
                },
                {"kind": "instruction", "payload": {}},
            )
        )
    assert calls == 0


@pytest.mark.parametrize("adapter_name", ["claude", "codex"])
def test_denied_private_gate_never_reaches_process_creation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, adapter_name: str
) -> None:
    allowed = tmp_path / "approved"
    denied = tmp_path / "denied"
    allowed.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=allowed,
        codex_deny=denied,
        workspaces={"approved": allowed, "denied": denied},
    )
    gate = _gate(policy, denied, runner=adapter_name)
    calls = 0

    async def spawn(*_argv: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("owner-private deny must precede process creation")

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    adapter = ClaudeAdapter(Settings()) if adapter_name == "claude" else CodexAppServerAdapter(Settings())
    runtime: dict[str, Any] = {
        "metadata": {"command": [sys.executable, "-c", "raise SystemExit(0)"]},
        "_owner_private_launch_gate": gate,
    }
    with pytest.raises(OwnerPrivateLaunchBlocked):
        asyncio.run(adapter.dispatch(runtime, {"kind": "instruction", "payload": {}}))
    assert calls == 0


@pytest.mark.parametrize("adapter_name", ["claude", "codex"])
def test_policy_change_between_evaluation_and_adapter_launch_stops_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, adapter_name: str
) -> None:
    approved = tmp_path / "approved"
    elsewhere = tmp_path / "elsewhere"
    approved.mkdir()
    elsewhere.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=approved,
        codex_deny=elsewhere,
        workspaces={"approved": approved},
    )
    workspace = approved
    gate = _gate(policy, workspace, runner=adapter_name)
    # The same provider revision now denies both adapter-specific placements.
    _write_policy(
        tmp_path,
        claude_allow=elsewhere,
        codex_deny=approved,
        workspaces={"approved": approved},
        version="2",
    ).chmod(0o600)
    calls = 0

    async def spawn(*_argv: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("changed policy must precede process creation")

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    adapter = ClaudeAdapter(Settings()) if adapter_name == "claude" else CodexAppServerAdapter(Settings())
    runtime: dict[str, Any] = {
        "metadata": {"command": [sys.executable, "-c", "raise SystemExit(0)"]},
        "_owner_private_launch_gate": gate,
    }
    with pytest.raises(OwnerPrivateLaunchBlocked):
        asyncio.run(adapter.dispatch(runtime, {"kind": "instruction", "payload": {}}))
    assert calls == 0


def test_dispatcher_denial_is_terminal_and_durable_state_contains_no_workspace(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    private_workspace = tmp_path / _SENTINEL / "denied"
    approved = tmp_path / "approved"
    private_workspace.mkdir(parents=True)
    approved.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=approved,
        codex_deny=private_workspace,
        workspaces={"project-ref": private_workspace},
    )
    service = system["service"]
    runtime = system["runtime"]
    service.db.execute(
        "UPDATE runtime_sessions SET metadata_json = ? WHERE id = ?",
        (json.dumps({"command": [sys.executable, "-c", "raise SystemExit(0)"], "workspace_ref": "project-ref"}), runtime["id"]),
    )
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Placement denial",
            objective="Must not launch.",
            acceptance=["No process starts."],
            runtime_session_id=runtime["id"],
        ),
    )
    credentials_before = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_credentials"
    )
    calls = 0

    async def spawn(*_argv: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("denied placement must not start a Worker")

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    settings = replace(
        system["settings"],
        owner_private_policy_file=policy,
        require_owner_private_policy=True,
    )
    assert asyncio.run(Dispatcher(service, settings).run_once()) == 1
    assert calls == 0
    credentials_after = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM runtime_credentials"
    )
    assert credentials_before is not None and credentials_after is not None
    assert credentials_after["count"] == credentials_before["count"]
    delivery = service.db.fetchone("SELECT state, last_error FROM message_deliveries")
    assert delivery is not None
    assert delivery["state"] == "dead"
    assert delivery["last_error"] == "owner_private_policy_launch_denied"
    decision = service.db.fetchone("SELECT decision FROM owner_private_placement_decisions")
    assert decision is not None and decision["decision"] == "deny"
    serialized = service.db.path.read_bytes().decode("latin1")
    assert _SENTINEL not in serialized
    assert str(private_workspace) not in serialized
    events = service.db.fetchall("SELECT data_json FROM events")
    assert all(_SENTINEL not in str(row["data_json"]) for row in events)


def test_allowed_launch_adopts_resolved_workspace_into_close_inventory(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workspace = tmp_path / "approved"
    denied = tmp_path / "denied"
    workspace.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=workspace,
        codex_deny=denied,
        workspaces={"workspace-ref": workspace},
    )
    dispatcher = Dispatcher(
        system["service"],
        replace(
            system["settings"],
            owner_private_policy_file=policy,
            require_owner_private_policy=True,
        ),
    )
    monkeypatch.setattr(
        dispatcher,
        "_record_owner_private_decision",
        lambda *_args, **_kwargs: "decision-1",
    )
    adopted: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "adopt_managed_work",
        lambda work_item_id, resolved: adopted.append((work_item_id, resolved)),
    )

    gate, decision_id = dispatcher._prepare_owner_private_launch_gate(
        {"recipient_id": "worker-1", "generation": 3},
        {
            "id": "runtime-1",
            "adapter": "claude",
            "metadata": {"workspace_ref": "workspace-ref"},
        },
        {"id": "message-1", "work_item_id": "work-1", "attempt_id": "attempt-1"},
        launch_generation=4,
    )

    assert gate is not None
    assert decision_id == "decision-1"
    assert adopted == [("work-1", workspace.resolve())]


def test_static_dynamic_shaped_ref_ignores_an_unrelated_corrupt_registry(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workspace = tmp_path / "approved-static"
    denied = tmp_path / "denied"
    workspace.mkdir()
    denied.mkdir()
    static_ref = "cao-dynamic-" + "a" * 64
    policy = _write_policy(
        tmp_path,
        claude_allow=workspace,
        codex_deny=denied,
        workspaces={static_ref: workspace},
    )
    settings = replace(
        system["settings"],
        state_dir=policy.parent,
        owner_private_policy_file=policy,
        require_owner_private_policy=True,
    )
    registry = settings.owner_private_workspace_registry_path
    registry.write_text("corrupt-unrelated-registry", encoding="utf-8")
    registry.chmod(0o600)
    dispatcher = Dispatcher(system["service"], settings)
    monkeypatch.setattr(
        dispatcher,
        "_record_owner_private_decision",
        lambda *_args, **_kwargs: "decision-1",
    )
    adopted: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "adopt_managed_work",
        lambda work_item_id, resolved: adopted.append((work_item_id, resolved)),
    )

    gate, decision_id = dispatcher._prepare_owner_private_launch_gate(
        {"recipient_id": "worker-1", "generation": 3},
        {
            "id": "runtime-1",
            "adapter": "claude",
            "metadata": {"workspace_ref": static_ref},
        },
        {"id": "message-1", "work_item_id": "work-1", "attempt_id": "attempt-1"},
        launch_generation=4,
    )

    assert gate is not None
    assert decision_id == "decision-1"
    assert adopted == [("work-1", workspace.resolve())]


@pytest.mark.parametrize("runner", ["claude", "codex"])
def test_dynamic_registered_directory_launches_at_exact_cwd_and_is_borrowed(
    system: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    runner: str,
) -> None:
    workspace = tmp_path / f"new-{runner}-project"
    denied = tmp_path / "denied"
    workspace.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=workspace,
        codex_deny=denied,
        workspaces={},
    )
    settings = replace(
        system["settings"],
        state_dir=policy.parent,
        owner_private_policy_file=policy,
        require_owner_private_policy=True,
    )
    edge = OwnerPrivatePolicyEdge(
        policy,
        workspace_registry_file=settings.owner_private_workspace_registry_path,
    )
    workspace_ref = edge.register_workspace(
        workspace,
        runner=runner,  # type: ignore[arg-type]
    )
    dispatcher = Dispatcher(system["service"], settings)
    monkeypatch.setattr(
        dispatcher,
        "_record_owner_private_decision",
        lambda *_args, **_kwargs: "decision-1",
    )
    declarations: list[tuple[str, CleanupTargetKind]] = []
    adopted: list[tuple[str, Path]] = []

    def declare_borrowed(
        *, work_item_id: str, target_kind: CleanupTargetKind
    ) -> None:
        declarations.append((work_item_id, target_kind))

    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "declare_not_applicable",
        declare_borrowed,
    )
    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "adopt_managed_work",
        lambda work_item_id, resolved: adopted.append((work_item_id, resolved)),
    )
    adapter_name = "codex-app-server" if runner == "codex" else "claude"
    gate, decision_id = dispatcher._prepare_owner_private_launch_gate(
        {"recipient_id": "worker-1", "generation": 3},
        {
            "id": "runtime-1",
            "adapter": adapter_name,
            "metadata": {},
            "managed_worker_spec": {"workspace_ref": workspace_ref},
        },
        {"id": "message-1", "work_item_id": "work-1", "attempt_id": "attempt-1"},
        launch_generation=4,
    )

    assert gate is not None
    assert decision_id == "decision-1"
    assert adopted == []
    assert declarations == [
        ("work-1", CleanupTargetKind.WORKSPACE),
        ("work-1", CleanupTargetKind.TEMPORARY),
        ("work-1", CleanupTargetKind.LOG),
        ("work-1", CleanupTargetKind.BRANCH),
    ]

    original_spawn = asyncio.create_subprocess_exec
    launch_cwds: list[str | None] = []
    launch_pass_fds: list[tuple[int, ...]] = []

    async def spawn(*argv: str, **kwargs: Any) -> asyncio.subprocess.Process:
        launch_cwds.append(kwargs.get("cwd"))
        launch_pass_fds.append(kwargs.get("pass_fds", ()))
        assert str(workspace) not in argv
        return await original_spawn(*argv, **kwargs)

    monkeypatch.setattr("cao_control_plane.runtime.asyncio.create_subprocess_exec", spawn)
    adapter = (
        ClaudeAdapter(settings)
        if runner == "claude"
        else CodexAppServerAdapter(settings)
    )
    result = asyncio.run(
        adapter.dispatch(
            {
                "metadata": {
                    "command": [sys.executable, "-c", _runner_program(runner)]
                },
                "_owner_private_launch_gate": gate,
            },
            {"kind": "instruction", "payload": {"message": "go"}},
        )
    )

    assert result.success
    assert launch_cwds == [None]
    assert len(launch_pass_fds) == 1 and len(launch_pass_fds[0]) == 1
    assert (workspace / ".cao-test-cwd").read_text() == str(workspace.resolve())


@pytest.mark.parametrize("registry_failure", ["missing", "corrupt"])
def test_default_dynamic_launch_fails_closed_when_registry_is_unavailable(
    system: dict[str, Any],
    tmp_path: Path,
    registry_failure: str,
) -> None:
    state = tmp_path / "owner-state"
    state.mkdir(mode=0o700)
    workspace = tmp_path / "borrowed-project"
    workspace.mkdir()
    settings = replace(
        system["settings"],
        state_dir=state,
        owner_private_policy_file=None,
        require_owner_private_policy=False,
    )
    policy = ensure_default_dynamic_workspace_policy(
        settings.owner_private_dynamic_policy_path,
        denied_root=state,
    )
    edge = OwnerPrivatePolicyEdge(
        policy,
        workspace_registry_file=settings.owner_private_workspace_registry_path,
    )
    workspace_ref = edge.register_workspace(workspace, runner="claude")
    registry = settings.owner_private_workspace_registry_path
    if registry_failure == "missing":
        registry.unlink()
    else:
        registry.write_text("not-json", encoding="utf-8")
        registry.chmod(0o600)
    dispatcher = Dispatcher(system["service"], settings)

    with pytest.raises(PrivatePolicyError) as caught:
        dispatcher._prepare_owner_private_launch_gate(
            {"recipient_id": "worker-1", "generation": 3},
            {
                "id": "runtime-1",
                "adapter": "claude",
                "metadata": {},
                "managed_worker_spec": {"workspace_ref": workspace_ref},
            },
            {
                "id": "message-1",
                "work_item_id": "work-1",
                "attempt_id": "attempt-1",
            },
            launch_generation=4,
        )

    assert caught.value.code == "owner_private_policy_workspace_unavailable"
    assert str(workspace) not in str(caught.value)


def test_close_inventory_adoption_failure_blocks_before_launch_gate_is_returned(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    workspace = tmp_path / "approved"
    denied = tmp_path / "denied"
    workspace.mkdir()
    denied.mkdir()
    policy = _write_policy(
        tmp_path,
        claude_allow=workspace,
        codex_deny=denied,
        workspaces={"workspace-ref": workspace},
    )
    dispatcher = Dispatcher(
        system["service"],
        replace(
            system["settings"],
            owner_private_policy_file=policy,
            require_owner_private_policy=True,
        ),
    )
    monkeypatch.setattr(
        dispatcher,
        "_record_owner_private_decision",
        lambda *_args, **_kwargs: "decision-1",
    )

    def reject_adoption(_work_item_id: str, _workspace: Path) -> None:
        raise CloseInventoryProviderError("cleanup_workspace_unavailable")

    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "adopt_managed_work",
        reject_adoption,
    )
    with pytest.raises(
        PrivatePolicyError, match="owner private policy workspace unavailable"
    ):
        dispatcher._prepare_owner_private_launch_gate(
            {"recipient_id": "worker-1", "generation": 3},
            {
                "id": "runtime-1",
                "adapter": "claude",
                "metadata": {"workspace_ref": "workspace-ref"},
            },
            {
                "id": "message-1",
                "work_item_id": "work-1",
                "attempt_id": "attempt-1",
            },
            launch_generation=4,
        )
