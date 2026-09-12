from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_private_policy import _binding, _write_policy
from test_supervision_memory import _actor, _remember, _search, _work
from test_worker_thread_lifecycle_core import _seed_managed_thread

import cao_control_plane.directory_identity as identity_module
from cao_control_plane.database import Database
from cao_control_plane.directory_identity import directory_identity
from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import (
    _attached_cao_conversation,
    _validate_attached_start_request,
    cao_conversation_context_for_thread,
)
from cao_control_plane.models import InstructWorkerThreadInput
from cao_control_plane.private_policy import OwnerPrivatePolicyEdge, PrivatePolicyError
from cao_control_plane.service import ControlPlane


class _RemountedStat:
    def __init__(self, original: os.stat_result) -> None:
        self.original = original
        self.st_dev = original.st_dev + 100 if stat.S_ISDIR(original.st_mode) else original.st_dev

    def __getattr__(self, name: str) -> Any:
        return getattr(self.original, name)

    def __getitem__(self, index: int) -> Any:
        return self.original[index]


def test_existing_conversation_identity_survives_device_renumbering(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    before = cao_conversation_context_for_thread("existing-conversation")
    old_stat = os.stat(".")
    legacy_before = hashlib.sha256(f"cao-project-inode-v1\0{old_stat.st_dev}\0{old_stat.st_ino}".encode()).hexdigest()
    original_stat, original_fstat, original_lstat = os.stat, os.fstat, os.lstat
    with monkeypatch.context() as reboot:
        reboot.setattr(os, "stat", lambda *a, **k: _RemountedStat(original_stat(*a, **k)))
        reboot.setattr(os, "fstat", lambda *a, **k: _RemountedStat(original_fstat(*a, **k)))
        reboot.setattr(os, "lstat", lambda *a, **k: _RemountedStat(original_lstat(*a, **k)))
        after = cao_conversation_context_for_thread("existing-conversation")
        new_stat = os.stat(".")
        legacy_after = hashlib.sha256(f"cao-project-inode-v1\0{new_stat.st_dev}\0{new_stat.st_ino}".encode()).hexdigest()
    assert legacy_before != legacy_after  # The previous implementation's exact failure.
    assert before == after


def test_persistent_identity_rejects_replacement_volume_and_directory(tmp_path, monkeypatch):
    root = tmp_path / "project"
    root.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    before = directory_identity(root)
    assert directory_identity(alias) == before
    with monkeypatch.context() as other_volume:
        other_volume.setattr(identity_module, "_volume_identity", lambda _fd: "different-volume")
        assert directory_identity(root) != before
    root.rename(tmp_path / "original")
    root.mkdir()
    assert directory_identity(root) != before


def _legacy_registry(tmp_path):
    workspace = tmp_path / "project"
    workspace.mkdir()
    state = tmp_path / "state"
    policy = _write_policy(state, allowed=workspace, denied=workspace)
    registry_path = state / "workspaces.json"
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=registry_path)
    current_ref = edge.register_workspace(workspace, runner="claude")
    registry = json.loads(registry_path.read_text())
    entry = registry["workspaces"].pop(current_ref)
    entry.pop("persistent_identity")
    entry.pop("seal")
    entry["device"] += 100  # A valid sealed record from the previous mount.
    key = edge._workspace_registry_key(registry)
    legacy_ref = edge._registered_workspace_ref(
        key, runner=entry["runner"], device=entry["device"], inode=entry["inode"],
        path=entry["path"], object_generation=entry["object_generation"],
    )
    entry["seal"] = edge._workspace_registry_seal(key, legacy_ref, entry)
    registry["workspaces"][legacy_ref] = entry
    edge._write_workspace_registry_locked(registry)
    return edge, workspace, registry_path, legacy_ref


def test_sealed_legacy_workspace_migrates_without_replacing_worker_ref(tmp_path):
    edge, workspace, registry_path, legacy_ref = _legacy_registry(tmp_path)
    if not hasattr(workspace.stat(), "st_birthtime") and not hasattr(workspace.stat(), "st_gen"):
        with pytest.raises(PrivatePolicyError):
            edge.resolve_workspace(legacy_ref, runner="claude")
        return
    assert edge.resolve_workspace(legacy_ref, runner="claude") == workspace
    assert edge.register_workspace(workspace, runner="claude") == legacy_ref
    migrated = json.loads(registry_path.read_text())
    assert migrated["workspaces"][legacy_ref]["persistent_identity"] == directory_identity(workspace)
    binding = _binding()
    decision = edge.evaluate(binding, workspace)
    resolved, fd = edge.acquire_launch_workspace(
        decision, binding, workspace, launch_generation=binding.launch_generation,
        workspace_ref=legacy_ref,
    )
    try:
        assert resolved == workspace and os.fstat(fd).st_ino == workspace.stat().st_ino
    finally:
        os.close(fd)
    workspace.rename(tmp_path / "retired")
    workspace.mkdir()
    with pytest.raises(PrivatePolicyError):
        edge.resolve_workspace(legacy_ref, runner="claude")


def test_project_migration_preserves_work_packets_and_shares_only_project_scope(system):
    service = system["service"]
    old = _actor(system, "before-reboot", project="a" * 64)
    current = _actor(system, "after-reboot", project="b" * 64)
    foreign = _actor(system, "foreign", project="c" * 64)
    work = _work(system, old, "sealed-before-reboot")
    case = {**system, "actor": old, "work": work}
    memory = _remember(case, scope="project")
    private = _remember(case, primary_abstraction="Private experience", idempotency_key="private")
    ids = _seed_managed_thread(system, old, ordinal=1900, runtime_state="missing")
    before = service.get_work(work["id"], old)
    with service.db.transaction() as connection:
        connection.execute("UPDATE cao_session_attachments SET project_identity_version=1 WHERE id=?", (old["_cao_attachment_id"],))
    assert service.list_managed_workers(current) == []
    observation = service.legacy_project_observation("supervision-memory-before-reboot")
    assert observation is not None
    service.bind_verified_project_identity(observation, "b" * 64)
    service.bind_verified_project_identity(observation, "b" * 64)  # concurrent replay
    with pytest.raises(ConflictError):
        service.bind_verified_project_identity(observation, "c" * 64)
    assert service.get_work(work["id"], old) == before
    assert [w["worker_thread_id"] for w in service.list_managed_workers(current)] == [ids["thread_id"]]
    assert service.list_managed_workers(foreign) == []
    found = _search({**case, "actor": current}, "progress recall")["memories"]
    assert [m["memory_id"] for m in found] == [memory["memory_id"]]
    assert private["memory_id"] not in {m["memory_id"] for m in found}
    instruction = service.instruct_worker_thread(current, InstructWorkerThreadInput(
        worker_thread_id=ids["thread_id"], objective="Continue on the exact retained Worker.",
        idempotency_key="after-reboot-instruction",
    ))
    assert instruction
    # A process restart preserves the new scope and every original sealed digest.
    reopened = ControlPlane(Database(system["settings"]), system["settings"])
    attached = attach_cao_session_with_peer(reopened, current_cao_session_attachment(
        native_thread_id="supervision-memory-before-reboot", project_digest="b" * 64,
    ))
    assert attached["id"] == old["_cao_attachment_id"]
    assert attached["generation"] == old["_cao_attachment_generation"]
    assert attached["project_digest"] == "a" * 64
    assert attached["project_identity_digest"] == "b" * 64
    assert [w["worker_thread_id"] for w in reopened.list_managed_workers(reopened.authenticate(attached["context_token"]))] == [ids["thread_id"]]


def test_migrated_bridge_validates_persistent_scope_not_sealed_legacy_digest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    context = cao_conversation_context_for_thread("existing-conversation")
    attached = _attached_cao_conversation({
        "id": "attachment", "native_thread_id": context.native_thread_id,
        "project_digest": "a" * 64, "project_identity_digest": context.project_digest,
        "context_token": "cao.csc_fixture",
    })
    request = {"id": 1, "method": "tools/call", "params": {
        "name": "cao_start", "arguments": {"native_thread_id": context.native_thread_id},
    }}
    assert _validate_attached_start_request(request, attached) is None


@pytest.mark.parametrize("proof", ["correct", "wrong-project", "unavailable"])
def test_restart_attachment_uses_native_workspace_proof(settings, tmp_path, monkeypatch, proof):
    async def scenario():
        import httpx

        import cao_control_plane.api as api_module
        from cao_control_plane.api import create_app
        from cao_control_plane.mcp import CAOStartStopped, _issue_verified_cao_attachment

        monkeypatch.chdir(tmp_path)
        first = create_app(settings)
        old = attach_cao_session_with_peer(first.state.service, current_cao_session_attachment(
            native_thread_id="native-legacy-conversation", project_digest="a" * 64,
        ))
        with first.state.database.transaction() as connection:
            connection.execute("UPDATE cao_session_attachments SET project_identity_version=1 WHERE id=?", (old["id"],))
            connection.execute("PRAGMA user_version=44")
            connection.execute("DELETE FROM schema_migrations WHERE version=45")
        context = cao_conversation_context_for_thread("native-legacy-conversation")
        observed = []

        async def native_identity(_settings, thread_id):
            observed.append(thread_id)
            if proof == "unavailable":
                raise OSError("private diagnostic must not escape")
            return context.project_digest if proof == "correct" else "c" * 64

        monkeypatch.setattr(api_module, "read_cao_project_identity", native_identity)
        restarted = create_app(settings)
        issuer = restarted.state.attachment_issuer
        await issuer.start()
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://127.0.0.1:8768") as client:
                request = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                    "name": "cao_start", "arguments": {"native_thread_id": context.native_thread_id},
                }}
                operation = _issue_verified_cao_attachment(
                    client, endpoint="http://127.0.0.1:8768/mcp", issuer_socket=issuer.path,
                    context=context, request=request, timeout_seconds=5,
                )
                if proof != "correct":
                    with pytest.raises(CAOStartStopped) as failure:
                        await operation
                    assert failure.value.reason_code == "attachment_context_invalid"
                    assert restarted.state.service.legacy_project_observation(context.native_thread_id) is not None
                else:
                    attached, verification = await operation
                    assert attached.attachment_id == old["id"]
                    assert attached.generation == old["generation"]
                    assert attached.project_digest == old["project_digest"]
                    assert attached.project_identity_digest == context.project_digest
                    assert verification.catalog_digest
                    # The following authentic connection needs no second migration lookup.
                    again, _ = await _issue_verified_cao_attachment(
                        client, endpoint="http://127.0.0.1:8768/mcp", issuer_socket=issuer.path,
                        context=context, request=request, timeout_seconds=5,
                    )
                    assert again.attachment_id == attached.attachment_id
        finally:
            await issuer.close()
        assert observed == [context.native_thread_id]

    asyncio.run(scenario())


def test_case_spelling_does_not_split_one_project_or_worker_directory(tmp_path):
    workspace = tmp_path / "MixedCaseProject"
    workspace.mkdir()
    alias = workspace.with_name("mixedcaseproject")
    if not alias.exists():
        pytest.skip("filesystem is case-sensitive")
    assert directory_identity(workspace) == directory_identity(alias)
    state = tmp_path / "state"
    policy = _write_policy(state, allowed=workspace, denied=workspace)
    edge = OwnerPrivatePolicyEdge(policy, workspace_registry_file=state / "workspaces.json")
    assert edge.register_workspace(workspace, runner="claude") == edge.register_workspace(alias, runner="claude")


def test_one_native_workspace_proof_never_migrates_another_conversation(system):
    service = system["service"]
    first = _actor(system, "first-location", project="a" * 64)
    second = _actor(system, "second-location", project="a" * 64)
    with service.db.transaction() as connection:
        connection.execute("UPDATE cao_session_attachments SET project_identity_version=1")
    first_observation = service.legacy_project_observation("supervision-memory-first-location")
    second_observation = service.legacy_project_observation("supervision-memory-second-location")
    service.bind_verified_project_identity(first_observation, "b" * 64)
    row = service.db.fetchone("SELECT project_scope_digest, project_identity_version FROM cao_session_attachments WHERE id=?", (second["_cao_attachment_id"],))
    assert row["project_scope_digest"] == "a" * 64 and row["project_identity_version"] == 1
    # Each trusted provider observation can independently establish location.
    service.bind_verified_project_identity(second_observation, "c" * 64)
    row = service.db.fetchone("SELECT project_scope_digest FROM cao_session_attachments WHERE id=?", (first["_cao_attachment_id"],))
    assert row["project_scope_digest"] == "b" * 64
