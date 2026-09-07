from __future__ import annotations

import base64
import json
import os
import stat
from dataclasses import replace
from pathlib import Path

import pytest

import cao_control_plane.private_policy as private_policy_module
from cao_control_plane.private_policy import (
    OwnerPrivatePolicyEdge,
    PlacementBinding,
    PrivatePolicyError,
)

_SENTINEL = "private-placement-sentinel-must-never-serialize"


def _binding(
    *,
    runner: str = "claude",
    principal: str = "principal-1",
    generation: int = 7,
) -> PlacementBinding:
    return PlacementBinding(
        principal_id=principal,
        runtime_id="runtime-1",
        assignment_id="assignment-1",
        work_item_id="work-1",
        runner_adapter=runner,  # type: ignore[arg-type]
        launch_generation=generation,
    )


def _write_policy(
    state: Path,
    *,
    allowed: Path,
    denied: Path,
    version: str = "1",
) -> Path:
    state.mkdir(exist_ok=True)
    state.chmod(0o700)
    policy = state / "policy.json"
    document = {
        "policy_id": "placement-policy",
        "policy_version": version,
        "evidence_key": base64.urlsafe_b64encode(b"e" * 32).decode("ascii").rstrip("="),
        "runners": {
            "claude": {"allow_within": [str(allowed)], "deny_within": []},
            "codex": {"allow_within": [], "deny_within": [str(denied)]},
        },
    }
    policy.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
    policy.chmod(0o600)
    return policy


@pytest.fixture
def policy_setup(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    private_root = tmp_path / _SENTINEL / "approved"
    private_root.mkdir(parents=True)
    public_root = tmp_path / "elsewhere"
    public_root.mkdir()
    state = tmp_path / "owner-state"
    policy = _write_policy(state, allowed=private_root, denied=private_root)
    return policy, private_root, public_root, state


def test_claude_allows_only_the_private_rule_root(policy_setup) -> None:
    policy, private_root, public_root, _ = policy_setup
    edge = OwnerPrivatePolicyEdge(policy, clock=lambda: 100)

    allowed = edge.evaluate(_binding(), private_root)
    denied = edge.evaluate(_binding(), public_root)

    assert allowed.decision == "allow"
    assert denied.decision == "deny"
    assert allowed.as_durable().keys() == {
        "policy_id",
        "policy_version",
        "policy_digest",
        "decision",
        "runner_adapter",
        "workspace_identity_digest",
        "evidence_id",
        "expires_at",
        "revoked_at",
    }


def test_wrong_runner_is_denied_by_its_own_private_rule(policy_setup) -> None:
    policy, private_root, _, _ = policy_setup
    decision = OwnerPrivatePolicyEdge(policy, clock=lambda: 100).evaluate(
        _binding(runner="codex"), private_root
    )

    assert decision.decision == "deny"


def test_symlink_escape_is_denied_after_canonicalization(policy_setup) -> None:
    policy, private_root, public_root, _ = policy_setup
    escaped = private_root / "escape"
    escaped.symlink_to(public_root, target_is_directory=True)

    decision = OwnerPrivatePolicyEdge(policy, clock=lambda: 100).evaluate(_binding(), escaped)

    assert decision.decision == "deny"


def test_prefix_sibling_is_not_contained(policy_setup) -> None:
    policy, private_root, _, _ = policy_setup
    sibling = private_root.parent / f"{private_root.name}-sibling"
    sibling.mkdir()

    decision = OwnerPrivatePolicyEdge(policy, clock=lambda: 100).evaluate(_binding(), sibling)

    assert decision.decision == "deny"


def test_policy_symlink_and_hardlink_fail_closed(policy_setup, tmp_path: Path) -> None:
    _, private_root, _, state = policy_setup
    source_state = tmp_path / "other-owner-state"
    source = _write_policy(source_state, allowed=private_root, denied=private_root)
    policy = state / "policy.json"
    policy.unlink()
    policy.symlink_to(source)

    with pytest.raises(PrivatePolicyError) as symlink_error:
        OwnerPrivatePolicyEdge(policy).evaluate(_binding(), private_root)
    assert symlink_error.value.code == "owner_private_policy_unavailable"

    policy.unlink()
    os.link(source, policy)
    with pytest.raises(PrivatePolicyError) as hardlink_error:
        OwnerPrivatePolicyEdge(policy).evaluate(_binding(), private_root)
    assert hardlink_error.value.code == "owner_private_policy_unavailable"


def test_policy_tamper_invalidates_a_previous_allow_at_launch_gate(policy_setup) -> None:
    policy, private_root, _, state = policy_setup
    edge = OwnerPrivatePolicyEdge(policy, clock=lambda: 100)
    binding = _binding()
    decision = edge.evaluate(binding, private_root)
    _write_policy(state, allowed=private_root, denied=private_root, version="2")
    (state / "policy.json").chmod(0o600)

    with pytest.raises(PrivatePolicyError) as caught:
        edge.require_fresh_launch_allow(
            decision, binding, private_root, launch_generation=binding.launch_generation
        )
    assert caught.value.code == "owner_private_policy_launch_denied"


def test_stale_generation_and_policy_revision_fail_launch_gate(policy_setup) -> None:
    policy, private_root, _, state = policy_setup
    edge = OwnerPrivatePolicyEdge(policy, clock=lambda: 100)
    binding = _binding()
    decision = edge.evaluate(binding, private_root)

    with pytest.raises(PrivatePolicyError) as generation_error:
        edge.require_fresh_launch_allow(decision, binding, private_root, launch_generation=8)
    assert generation_error.value.code == "owner_private_policy_stale_generation"

    _write_policy(state, allowed=private_root, denied=private_root, version="revision-2")
    (state / "policy.json").chmod(0o600)
    with pytest.raises(PrivatePolicyError) as revision_error:
        edge.require_fresh_launch_allow(
            decision, binding, private_root, launch_generation=binding.launch_generation
        )
    assert revision_error.value.code == "owner_private_policy_launch_denied"


def test_decision_replay_to_another_worker_fails(policy_setup) -> None:
    policy, private_root, _, _ = policy_setup
    edge = OwnerPrivatePolicyEdge(policy, clock=lambda: 100)
    decision = edge.evaluate(_binding(principal="principal-1"), private_root)
    different_worker = _binding(principal="principal-2")

    with pytest.raises(PrivatePolicyError) as caught:
        edge.require_fresh_launch_allow(
            decision,
            different_worker,
            private_root,
            launch_generation=different_worker.launch_generation,
        )
    assert caught.value.code == "owner_private_policy_launch_denied"


def test_revoked_or_expired_decision_fails_without_reaching_launch(policy_setup) -> None:
    policy, private_root, _, _ = policy_setup
    now = [100]
    edge = OwnerPrivatePolicyEdge(policy, decision_ttl_seconds=1, clock=lambda: now[0])
    binding = _binding()
    decision = edge.evaluate(binding, private_root)
    now[0] = 101

    with pytest.raises(PrivatePolicyError) as expired:
        edge.require_fresh_launch_allow(decision, binding, private_root, launch_generation=7)
    assert expired.value.code == "owner_private_policy_launch_denied"

    now[0] = 100
    current = OwnerPrivatePolicyEdge(policy, clock=lambda: now[0])
    allowed = current.evaluate(binding, private_root)
    with pytest.raises(PrivatePolicyError) as revoked:
        current.require_fresh_launch_allow(
            replace(allowed, revoked_at=100), binding, private_root, launch_generation=7
        )
    assert revoked.value.code == "owner_private_policy_launch_denied"


def test_fresh_launch_gate_accepts_a_current_decision_across_a_clock_tick(policy_setup) -> None:
    policy, private_root, _, _ = policy_setup
    now = [100]
    edge = OwnerPrivatePolicyEdge(policy, clock=lambda: now[0])
    binding = _binding()
    decision = edge.evaluate(binding, private_root)
    now[0] = 101

    edge.require_fresh_launch_allow(decision, binding, private_root, launch_generation=7)


def test_policy_file_and_parent_permissions_are_exact(policy_setup) -> None:
    policy, private_root, _, state = policy_setup
    assert stat.S_IMODE(state.stat().st_mode) == 0o700
    assert stat.S_IMODE(policy.stat().st_mode) == 0o600
    policy.chmod(0o640)

    with pytest.raises(PrivatePolicyError):
        OwnerPrivatePolicyEdge(policy).evaluate(_binding(), private_root)


def test_private_sentinel_is_absent_from_serialized_result_and_errors(policy_setup) -> None:
    policy, private_root, public_root, _ = policy_setup
    edge = OwnerPrivatePolicyEdge(policy, clock=lambda: 100)
    result = edge.evaluate(_binding(), public_root)
    serialized = json.dumps(result.as_durable(), sort_keys=True)

    assert _SENTINEL not in serialized
    with pytest.raises(PrivatePolicyError) as caught:
        edge.require_fresh_launch_allow(result, _binding(), private_root, launch_generation=7)
    assert _SENTINEL not in str(caught.value)
    assert _SENTINEL not in json.dumps(caught.value.as_dict())


def test_dynamic_workspace_registry_is_owner_private_opaque_and_idempotent(
    policy_setup,
) -> None:
    policy, private_root, _, state = policy_setup
    workspace = private_root / "new-project"
    workspace.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)

    workspace_ref = edge.register_workspace(workspace, runner="claude")
    retried_ref = OwnerPrivatePolicyEdge(
        policy, workspace_registry_file=registry
    ).register_workspace(workspace, runner="claude")

    assert workspace_ref == retried_ref
    assert edge.is_registered_workspace_ref(workspace_ref)
    assert str(workspace) not in workspace_ref
    assert _SENTINEL not in json.dumps({"workspace_ref": workspace_ref})
    assert stat.S_IMODE(registry.stat().st_mode) == 0o600
    document = json.loads(registry.read_text(encoding="utf-8"))
    assert document["revision"] == 1
    assert len(document["workspaces"]) == 1
    stored = document["workspaces"][workspace_ref]
    assert stored["path"] == str(workspace.resolve())
    assert (stored["device"], stored["inode"]) == (
        workspace.stat().st_dev,
        workspace.stat().st_ino,
    )
    assert stored["object_generation"].startswith(
        ("birthtime:", "stat-generation:", "stable-generation-unavailable")
    )
    assert edge.resolve_workspace(workspace_ref, runner="claude") == workspace.resolve()


def test_dynamic_workspace_registry_rejects_oversized_update_without_corruption(
    policy_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy, private_root, _, state = policy_setup
    first = private_root / "first-project"
    second = private_root / "second-project"
    first.mkdir()
    second.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    first_ref = edge.register_workspace(first, runner="claude")
    original = registry.read_bytes()
    monkeypatch.setattr(
        private_policy_module,
        "_WORKSPACE_REGISTRY_MAX_BYTES",
        len(original) + 1,
    )

    with pytest.raises(PrivatePolicyError) as caught:
        edge.register_workspace(second, runner="claude")

    assert caught.value.code == "owner_private_policy_workspace_unavailable"
    assert registry.read_bytes() == original
    assert edge.resolve_workspace(first_ref, runner="claude") == first.resolve()


def test_static_workspace_reference_keeps_precedence_over_dynamic_namespace(
    policy_setup,
) -> None:
    policy, private_root, _, state = policy_setup
    dynamic = private_root / "dynamic-project"
    static = private_root / "static-project"
    dynamic.mkdir()
    static.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    colliding_ref = edge.register_workspace(dynamic, runner="claude")
    document = json.loads(policy.read_text(encoding="utf-8"))
    document["workspaces"] = {colliding_ref: str(static.resolve())}
    policy.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
    policy.chmod(0o600)
    registry.write_text("corrupt-unrelated-registry", encoding="utf-8")
    registry.chmod(0o600)

    assert not edge.workspace_ref_is_dynamic(colliding_ref)
    assert edge.resolve_workspace(colliding_ref, runner="claude") == static.resolve()


def test_missing_static_workspace_does_not_block_dynamic_registration(
    policy_setup,
) -> None:
    policy, private_root, _, state = policy_setup
    existing_dynamic = private_root / "existing-dynamic"
    new_dynamic = private_root / "new-dynamic"
    stale_static = private_root / "removed-static"
    existing_dynamic.mkdir()
    new_dynamic.mkdir()
    stale_static.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    colliding_ref = edge.register_workspace(existing_dynamic, runner="claude")

    document = json.loads(policy.read_text(encoding="utf-8"))
    document["workspaces"] = {colliding_ref: str(stale_static)}
    policy.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
    policy.chmod(0o600)
    stale_static.rmdir()

    new_ref = edge.register_workspace(new_dynamic, runner="claude")

    assert edge.resolve_workspace(new_ref, runner="claude") == new_dynamic.resolve()
    assert edge.has_static_workspace_ref(colliding_ref)
    assert not edge.workspace_ref_is_dynamic(colliding_ref)
    with pytest.raises(PrivatePolicyError) as caught:
        edge.resolve_workspace(colliding_ref, runner="claude")
    assert caught.value.code == "owner_private_policy_workspace_unavailable"


@pytest.mark.parametrize("invalid_kind", ["relative", "missing", "file"])
def test_dynamic_workspace_registration_rejects_invalid_directories_path_free(
    policy_setup,
    invalid_kind: str,
) -> None:
    policy, private_root, _, state = policy_setup
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    if invalid_kind == "relative":
        candidate = Path("relative-project")
    elif invalid_kind == "missing":
        candidate = private_root / "missing-project"
    elif invalid_kind == "file":
        candidate = private_root / "project.txt"
        candidate.write_text("not a directory", encoding="utf-8")
    with pytest.raises(PrivatePolicyError) as caught:
        edge.register_workspace(candidate, runner="claude")

    assert caught.value.code == "owner_private_policy_workspace_unavailable"
    assert str(candidate) not in str(caught.value)


def test_dynamic_workspace_registration_canonicalizes_a_directory_symlink(
    policy_setup,
    tmp_path: Path,
) -> None:
    policy, private_root, _, state = policy_setup
    workspace = private_root / "real-project"
    workspace.mkdir()
    alias = tmp_path / "project-link"
    alias.symlink_to(workspace, target_is_directory=True)
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)

    workspace_ref = edge.register_workspace(alias, runner="claude")

    assert edge.resolve_workspace(workspace_ref, runner="claude") == workspace.resolve()
    stored = json.loads(registry.read_text(encoding="utf-8"))["workspaces"]
    assert stored[workspace_ref]["path"] == str(workspace.resolve())


def test_dynamic_workspace_rename_gets_a_new_ref_and_old_ref_fails_closed(
    policy_setup,
) -> None:
    policy, private_root, _, state = policy_setup
    original = private_root / "original-name"
    renamed = private_root / "renamed-project"
    original.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    original_ref = edge.register_workspace(original, runner="claude")
    original.rename(renamed)

    renamed_ref = edge.register_workspace(renamed, runner="claude")

    assert renamed_ref != original_ref
    document = json.loads(registry.read_text(encoding="utf-8"))
    assert document["revision"] == 2
    assert document["workspaces"][renamed_ref]["path"] == str(renamed.resolve())
    with pytest.raises(PrivatePolicyError):
        edge.resolve_workspace(original_ref, runner="claude")
    assert edge.resolve_workspace(renamed_ref, runner="claude") == renamed.resolve()


def test_dynamic_workspace_object_generation_change_never_reuses_old_ref(
    policy_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy, private_root, _, state = policy_setup
    workspace = private_root / "generation-project"
    workspace.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    original_ref = edge.register_workspace(workspace, runner="claude")

    # Simulate a filesystem reusing the same pathname and inode for a new
    # object. The non-reusable object-generation proof must still split the
    # authority and make the old Worker fail closed.
    replacement_generation = "birthtime:0x1.0000000000000p+0"
    monkeypatch.setattr(
        private_policy_module,
        "_stat_object_generation",
        lambda _info: replacement_generation,
    )
    replacement_ref = edge.register_workspace(workspace, runner="claude")

    assert replacement_ref != original_ref
    with pytest.raises(PrivatePolicyError):
        edge.resolve_workspace(original_ref, runner="claude")
    assert edge.resolve_workspace(replacement_ref, runner="claude") == workspace.resolve()


def test_portable_generation_fallback_survives_normal_directory_changes(
    policy_setup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy, private_root, _, state = policy_setup
    workspace = private_root / "portable-project"
    workspace.mkdir()
    registry = state / "managed-worker-workspaces-v1.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry)
    monkeypatch.setattr(
        private_policy_module,
        "_stat_object_generation",
        lambda _info: "stable-generation-unavailable",
    )
    workspace_ref = edge.register_workspace(workspace, runner="claude")

    (workspace / "normal-worker-output.txt").write_text("ok", encoding="utf-8")

    assert edge.resolve_workspace(workspace_ref, runner="claude") == workspace.resolve()


def test_dynamic_workspace_registration_applies_runner_policy_path_free(
    policy_setup,
) -> None:
    policy, private_root, public_root, state = policy_setup
    edge = OwnerPrivatePolicyEdge(
        policy,
        workspace_registry_file=state / "managed-worker-workspaces-v1.json",
    )

    with pytest.raises(PrivatePolicyError) as caught:
        edge.register_workspace(public_root, runner="claude")

    assert caught.value.code == "owner_private_policy_launch_denied"
    assert str(private_root) not in str(caught.value)
    assert str(public_root) not in str(caught.value)


def test_dynamic_workspace_resolution_detects_symlink_replacement_path_free(
    policy_setup,
) -> None:
    policy, private_root, public_root, state = policy_setup
    workspace = private_root / "replaceable-project"
    workspace.mkdir()
    edge = OwnerPrivatePolicyEdge(
        policy,
        workspace_registry_file=state / "managed-worker-workspaces-v1.json",
    )
    workspace_ref = edge.register_workspace(workspace, runner="claude")
    moved = private_root / "original-project"
    workspace.rename(moved)
    workspace.symlink_to(public_root, target_is_directory=True)

    with pytest.raises(PrivatePolicyError) as caught:
        edge.resolve_workspace(workspace_ref, runner="claude")

    assert caught.value.code == "owner_private_policy_workspace_unavailable"
    assert str(workspace) not in str(caught.value)
    assert str(public_root) not in str(caught.value)


def test_dynamic_workspace_resolution_detects_inode_replacement(policy_setup) -> None:
    policy, private_root, _, state = policy_setup
    workspace = private_root / "inode-project"
    workspace.mkdir()
    edge = OwnerPrivatePolicyEdge(
        policy,
        workspace_registry_file=state / "managed-worker-workspaces-v1.json",
    )
    workspace_ref = edge.register_workspace(workspace, runner="claude")
    workspace.rename(private_root / "original-inode-project")
    workspace.mkdir()

    with pytest.raises(PrivatePolicyError) as caught:
        edge.resolve_workspace(workspace_ref, runner="claude")

    assert caught.value.code == "owner_private_policy_workspace_unavailable"


def test_dynamic_workspace_resolution_rechecks_current_runner_policy(
    policy_setup,
) -> None:
    policy, private_root, public_root, state = policy_setup
    workspace = private_root / "policy-change-project"
    workspace.mkdir()
    edge = OwnerPrivatePolicyEdge(
        policy,
        workspace_registry_file=state / "managed-worker-workspaces-v1.json",
    )
    workspace_ref = edge.register_workspace(workspace, runner="claude")
    _write_policy(state, allowed=public_root, denied=private_root, version="2").chmod(0o600)

    with pytest.raises(PrivatePolicyError) as caught:
        edge.resolve_workspace(workspace_ref, runner="claude")

    assert caught.value.code == "owner_private_policy_launch_denied"
    assert str(workspace) not in str(caught.value)
