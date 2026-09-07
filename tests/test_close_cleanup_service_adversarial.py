"""Adversarial service regressions for owner-private explicit-close cleanup."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.close_contract import CleanupTargetKind
from cao_control_plane.close_inventory_edge import CloseInventoryProviderError
from cao_control_plane.errors import AuthorizationError, ConflictError
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    BoundaryDispositionInput,
    EffectCheckInput,
    EffectGrantInput,
    EffectKind,
    ExecutePreparedCleanupInput,
    ReportInput,
    ReportKind,
    RequesterDecisionInput,
    ReviewInput,
    WorkAssignment,
    WorkClosePreparationInput,
)
from cao_control_plane.projection import build_projection

_DIGEST_A = "a" * 64
_DIGEST_C = "c" * 64


def _completed_accepted_work(
    system: dict[str, Any], *, prefix: str, artifact_path: Path | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a completed generation owned by one attached CAO conversation."""

    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"{prefix}-thread",
            project_digest=_DIGEST_A,
            model="gpt-5.6-terra",
            sandbox="workspace-write",
        ),
    )
    ticket = service.issue_cao_runtime_launch_ticket(attachment["runtime_session_id"])
    runtime = service.authenticate(
        service.exchange_cao_runtime_launch_ticket(ticket["ticket"])["token"]
    )
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
            title="Adversarial close cleanup",
            objective="Exercise exact owner-private cleanup boundaries",
            acceptance=["Cleanup has immutable, effect-bound evidence"],
        ),
    )
    attempt = work["current_attempt"]
    artifact_digest = (
        hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if artifact_path is not None
        else _DIGEST_C
    )
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Adversarial cleanup fixture is ready for review",
            artifacts=[
                ArtifactInput(
                    name="result",
                    uri=(
                        str(artifact_path)
                        if artifact_path is not None
                        else "artifact:adversarial-result"
                    ),
                    media_type="text/plain",
                    digest=artifact_digest,
                )
            ],
            idempotency_key=f"{prefix}:completion",
        ),
    )
    boundary = reported["open_boundaries"][0]
    notification = next(
        item
        for item in service.get_inbox(runtime, attempt_id=attempt["id"])["items"]
        if item["kind"] == ReportKind.COMPLETION_CLAIM.value
        and item["payload"].get("boundary_id") == boundary["id"]
    )
    service.acknowledge(runtime, AckInput(message_ids=[notification["id"]]))
    turn = service.acquire_reasoner_turn(
        runtime,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key=f"{prefix}:turn",
    )
    service.dispose_boundary(
        runtime,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=work["generation"],
            kind="accept",
            reason="CAO reviewed the exact completion boundary",
        ),
    )
    reviewed = service.review(
        runtime,
        ReviewInput(
            attempt_id=attempt["id"],
            verdict="ok",
            summary="CAO review is OK",
            idempotency_key=f"{prefix}:review",
        ),
    )
    accepted = service.record_requester_decision(
        runtime,
        RequesterDecisionInput(
            review_id=reviewed["reviews"][-1]["id"],
            verdict="accepted",
            summary="Requester accepted this exact work",
            evidence=[],
            conversation_evidence_id=f"{prefix}:conversation",
            idempotency_key=f"{prefix}:decision",
        ),
    )
    return accepted, service.authenticate(attachment["context_token"])


def _declare_all_other_categories(
    system: dict[str, Any], work_id: str, *enumerated: CleanupTargetKind
) -> None:
    for kind in (
        CleanupTargetKind.WORKSPACE,
        CleanupTargetKind.TEMPORARY,
        CleanupTargetKind.LOG,
        CleanupTargetKind.BRANCH,
    ):
        if kind not in enumerated:
            system["service"].owner_private_close_inventory.declare_not_applicable(
                work_item_id=work_id, target_kind=kind
            )


def _prepare_temporary(
    system: dict[str, Any],
    tmp_path: Path,
    *,
    prefix: str,
    include_log: bool = False,
    stop_runtime: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[Path]]:
    cleanup_root = system["settings"].runtime_launch_dir / prefix
    cleanup_root.mkdir(parents=True)
    artifact_source = cleanup_root / "preserved-artifact.txt"
    artifact_source.write_text("preserve me before cleanup", encoding="utf-8")
    work, runtime = _completed_accepted_work(system, prefix=prefix, artifact_path=artifact_source)
    temporary = cleanup_root / "private-cleanup.tmp"
    temporary.write_text("remove me", encoding="utf-8")
    provider = system["service"].owner_private_close_inventory
    provider.register_resource(
        work_item_id=work["id"], target_kind=CleanupTargetKind.TEMPORARY, locator=temporary
    )
    targets = [temporary]
    kinds = [CleanupTargetKind.TEMPORARY]
    if include_log:
        log = cleanup_root / "private-cleanup.log"
        log.write_text("remove me too", encoding="utf-8")
        provider.register_resource(
            work_item_id=work["id"], target_kind=CleanupTargetKind.LOG, locator=log
        )
        targets.append(log)
        kinds.append(CleanupTargetKind.LOG)
    _declare_all_other_categories(system, work["id"], *kinds)
    preparation = system["service"].prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key=f"{prefix}:prepare",
        ),
    )
    if stop_runtime:
        system["service"].stop_work_runtime(runtime, work["id"])
    return work, runtime, preparation, targets


def _execute_input(
    _work: dict[str, Any],
    preparation: dict[str, Any],
    *,
    key: str,
) -> ExecutePreparedCleanupInput:
    return ExecutePreparedCleanupInput(
        close_preparation_id=preparation["id"],
        idempotency_key=key,
    )


def _private_records(preparation: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in preparation["inventory"] if item.get("coverage") == "enumerated"]


def _grant_records(
    system: dict[str, Any], records: list[dict[str, Any]], *, standing: bool
) -> list[dict[str, Any]]:
    return [
        system["service"].grant_effect(
            system["cao"],
            EffectGrantInput(
                principal_id=system["cao"]["id"],
                kind=EffectKind.DESTRUCTIVE,
                target_pattern=item["target_fingerprint"],
                action_pattern=item["action"],
                content_digest=item["execution_digest"],
                standing=standing,
            ),
        )
        for item in records
    ]


def _effect_rows(system: dict[str, Any], preparation_id: str) -> list[Any]:
    return system["service"].db.fetchall(
        "SELECT * FROM effect_operations WHERE cleanup_preparation_id = ? ORDER BY id",
        (preparation_id,),
    )


def _delete_requester_decision(system: dict[str, Any], work_id: str) -> None:
    system["service"].db.execute(
        "DELETE FROM requester_decisions WHERE work_item_id = ?",
        (work_id,),
    )


def test_stop_and_prepare_require_exact_accepted_requester_decision(
    system: dict[str, Any],
) -> None:
    work, actor = _completed_accepted_work(system, prefix="missing-decision-command")
    runtime_id = str(work["current_attempt"]["runtime_session_id"])
    before = system["service"].db.fetchone(
        "SELECT state FROM runtime_sessions WHERE id = ?", (runtime_id,)
    )
    assert before is not None
    _delete_requester_decision(system, work["id"])

    with pytest.raises(ConflictError) as stop_error:
        system["service"].stop_work_runtime(actor, work["id"])
    assert stop_error.value.details == {"reason_code": "work_close_requester_decision_required"}
    with pytest.raises(ConflictError) as prepare_error:
        system["service"].prepare_work_close(
            actor,
            WorkClosePreparationInput(
                work_item_id=work["id"],
                retention_policy_evidence_id="retention-v1",
                artifact_manifest_evidence_id="artifact-manifest-v1",
                idempotency_key="missing-decision-command:prepare",
            ),
        )
    assert prepare_error.value.details == {"reason_code": "work_close_requester_decision_required"}
    after = system["service"].db.fetchone(
        "SELECT state FROM runtime_sessions WHERE id = ?", (runtime_id,)
    )
    assert after is not None and after["state"] == before["state"]
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM work_close_preparations WHERE work_item_id = ?",
            (work["id"],),
        )["count"]
        == 0
    )


def test_cleanup_requires_exact_accepted_requester_decision_before_any_effect(
    system: dict[str, Any], tmp_path: Path
) -> None:
    work, actor, preparation, targets = _prepare_temporary(
        system,
        tmp_path,
        prefix="missing-decision-cleanup",
        stop_runtime=False,
    )
    grants = _grant_records(system, _private_records(preparation), standing=False)
    _delete_requester_decision(system, work["id"])

    with pytest.raises(ConflictError) as error:
        system["service"].execute_prepared_cleanup(
            actor,
            _execute_input(work, preparation, key="missing-decision-cleanup:execute"),
        )
    assert error.value.details == {"reason_code": "work_close_requester_decision_required"}
    assert targets[0].exists()
    assert not _effect_rows(system, preparation["id"])
    assert all(
        system["service"].get_effect_grant(grant["id"])["consumed_at"] is None for grant in grants
    )
    stored = system["service"].db.fetchone(
        "SELECT artifact_preservations_json, cleanup_execution_request_digest, "
        "cleanup_executed_at FROM work_close_preparations WHERE id = ?",
        (preparation["id"],),
    )
    assert stored is not None
    assert json.loads(str(stored["artifact_preservations_json"])) == []
    assert stored["cleanup_execution_request_digest"] == ""
    assert stored["cleanup_executed_at"] is None


def test_cleanup_requires_terminal_runtime_before_preservation_or_effect(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, actor, preparation, targets = _prepare_temporary(
        system,
        tmp_path,
        prefix="live-runtime-cleanup",
        stop_runtime=False,
    )
    grants = _grant_records(system, _private_records(preparation), standing=False)

    def unexpected_provider_call(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("cleanup touched an owner-private provider before runtime termination")

    monkeypatch.setattr(
        system["service"].owner_private_artifact_preservation,
        "register_file",
        unexpected_provider_call,
    )
    monkeypatch.setattr(
        system["service"].owner_private_artifact_preservation,
        "register_staged_file",
        unexpected_provider_call,
    )
    monkeypatch.setattr(
        system["service"].owner_private_artifact_preservation,
        "seal_receipt_set",
        unexpected_provider_call,
    )
    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "execute",
        unexpected_provider_call,
    )

    with pytest.raises(ConflictError) as error:
        system["service"].execute_prepared_cleanup(
            actor,
            _execute_input(work, preparation, key="live-runtime-cleanup:execute"),
        )
    assert error.value.details == {"reason_code": "work_close_runtime_not_terminal"}
    assert targets[0].exists()
    assert not _effect_rows(system, preparation["id"])
    assert all(
        system["service"].get_effect_grant(grant["id"])["consumed_at"] is None for grant in grants
    )
    stored = system["service"].db.fetchone(
        "SELECT artifact_preservations_json, cleanup_execution_request_digest, "
        "cleanup_executed_at FROM work_close_preparations WHERE id = ?",
        (preparation["id"],),
    )
    assert stored is not None
    assert json.loads(str(stored["artifact_preservations_json"])) == []
    assert stored["cleanup_execution_request_digest"] == ""
    assert stored["cleanup_executed_at"] is None


def test_cleanup_requires_worker_effect_outcomes_before_any_provider_mutation(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, actor, preparation, targets = _prepare_temporary(
        system,
        tmp_path,
        prefix="unknown-worker-effect",
    )
    unresolved = system["service"].start_effect(
        system["worker"],
        EffectCheckInput(
            principal_id=system["worker"]["id"],
            kind=EffectKind.LOCAL,
            target="worker-owned-effect",
            action="inspect",
        ),
    )

    def unexpected_provider_call(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("cleanup touched an owner-private provider with an unresolved effect")

    monkeypatch.setattr(
        system["service"].owner_private_artifact_preservation,
        "register_file",
        unexpected_provider_call,
    )
    monkeypatch.setattr(
        system["service"].owner_private_artifact_preservation,
        "register_staged_file",
        unexpected_provider_call,
    )
    monkeypatch.setattr(
        system["service"].owner_private_artifact_preservation,
        "seal_receipt_set",
        unexpected_provider_call,
    )
    monkeypatch.setattr(
        system["service"].owner_private_close_inventory,
        "execute",
        unexpected_provider_call,
    )

    with pytest.raises(ConflictError) as error:
        system["service"].execute_prepared_cleanup(
            actor,
            _execute_input(work, preparation, key="unknown-worker-effect:execute"),
        )
    assert error.value.details == {"reason_code": "work_close_effect_not_terminal"}
    assert targets[0].exists()
    assert system["service"].get_effect_operation(unresolved["id"])["status"] == "started"
    assert len(_effect_rows(system, preparation["id"])) == 0
    stored = system["service"].db.fetchone(
        "SELECT artifact_preservations_json, cleanup_execution_request_digest, "
        "cleanup_executed_at FROM work_close_preparations WHERE id = ?",
        (preparation["id"],),
    )
    assert stored is not None
    assert json.loads(str(stored["artifact_preservations_json"])) == []
    assert stored["cleanup_execution_request_digest"] == ""
    assert stored["cleanup_executed_at"] is None


def test_public_inventory_set_mismatch_is_rejected_before_any_effect(
    system: dict[str, Any], tmp_path: Path
) -> None:
    work, runtime, preparation, targets = _prepare_temporary(
        system, tmp_path, prefix="set-mismatch"
    )
    tampered = [dict(item) for item in preparation["inventory"]]
    next(item for item in tampered if item.get("coverage") == "enumerated")["action"] = "archive"
    encoded = json.dumps(tampered, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    system["service"].db.execute(
        "UPDATE work_close_preparations SET inventory_json = ?, inventory_digest = ? WHERE id = ?",
        (encoded, hashlib.sha256(encoded.encode()).hexdigest(), preparation["id"]),
    )

    with pytest.raises(ConflictError, match="owner-private cleanup inventory"):
        system["service"].execute_prepared_cleanup(
            runtime, _execute_input(work, preparation, key="set-mismatch:execute")
        )

    assert targets[0].exists()
    assert not _effect_rows(system, preparation["id"])


def test_exact_cleanup_replays_never_reserve_or_apply_a_second_destructive_effect(
    system: dict[str, Any], tmp_path: Path
) -> None:
    work, runtime, preparation, targets = _prepare_temporary(
        system, tmp_path, prefix="exact-replay"
    )
    _grant_records(system, _private_records(preparation), standing=True)
    request = _execute_input(work, preparation, key="exact-replay:one")

    first = system["service"].execute_prepared_cleanup(runtime, request)
    same_key = system["service"].execute_prepared_cleanup(runtime, request)
    different_key = system["service"].execute_prepared_cleanup(
        runtime, _execute_input(work, preparation, key="exact-replay:two")
    )

    assert first == same_key == different_key
    assert len(_effect_rows(system, preparation["id"])) == 1
    assert not targets[0].exists()


@pytest.mark.parametrize("source_change", ["missing", "wrong"], ids=["missing", "wrong"])
def test_missing_or_wrong_artifact_source_blocks_before_effect(
    system: dict[str, Any], tmp_path: Path, source_change: str
) -> None:
    work, runtime, preparation, targets = _prepare_temporary(
        system, tmp_path, prefix=f"artifact-{source_change}"
    )
    _grant_records(system, _private_records(preparation), standing=True)
    digest = str(work["artifacts"][0]["digest"])
    archive = system["settings"].state_dir / "artifact-archive-v1" / digest[:2] / digest
    if source_change == "missing":
        archive.unlink()
    else:
        archive.write_text("changed after canonical report", encoding="utf-8")

    with pytest.raises(ConflictError, match="could not be preserved"):
        system["service"].execute_prepared_cleanup(
            runtime,
            _execute_input(work, preparation, key=f"artifact-{source_change}:execute"),
        )

    assert targets[0].exists()
    assert not _effect_rows(system, preparation["id"])


def test_incomplete_multi_resource_authority_rolls_back_all_reservations_and_consumption(
    system: dict[str, Any], tmp_path: Path
) -> None:
    work, runtime, preparation, targets = _prepare_temporary(
        system, tmp_path, prefix="multi-grant", include_log=True
    )
    only_grant = _grant_records(system, _private_records(preparation)[:1], standing=False)[0]

    with pytest.raises(AuthorizationError):
        system["service"].execute_prepared_cleanup(
            runtime, _execute_input(work, preparation, key="multi-grant:execute")
        )

    assert system["service"].get_effect_grant(only_grant["id"])["consumed_at"] is None
    assert not _effect_rows(system, preparation["id"])
    assert all(target.exists() for target in targets)


def test_stale_attachment_generation_blocks_before_effect_reservation(
    system: dict[str, Any], tmp_path: Path
) -> None:
    work, runtime, preparation, targets = _prepare_temporary(
        system, tmp_path, prefix="stale-attachment"
    )
    _grant_records(system, _private_records(preparation), standing=True)
    system["service"].db.execute(
        "UPDATE work_close_preparations SET supervisor_attachment_generation = supervisor_attachment_generation + 1 WHERE id = ?",
        (preparation["id"],),
    )

    with pytest.raises(ConflictError, match="close preparation is stale"):
        system["service"].execute_prepared_cleanup(
            runtime, _execute_input(work, preparation, key="stale-attachment:execute")
        )

    assert targets[0].exists()
    assert not _effect_rows(system, preparation["id"])


def test_provider_exception_after_reservation_leaves_effect_started_and_replay_fenced(
    system: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work, runtime, preparation, targets = _prepare_temporary(
        system, tmp_path, prefix="provider-exception"
    )
    _grant_records(system, _private_records(preparation), standing=True)

    def _raise_after_reservation(**_kwargs: Any) -> list[dict[str, str]]:
        raise CloseInventoryProviderError("cleanup_provider_injected_failure")

    monkeypatch.setattr(
        system["service"].owner_private_close_inventory, "execute", _raise_after_reservation
    )
    request = _execute_input(work, preparation, key="provider-exception:execute")
    with pytest.raises(ConflictError, match="verify reserved effects"):
        system["service"].execute_prepared_cleanup(runtime, request)
    rows = _effect_rows(system, preparation["id"])
    assert [row["status"] for row in rows] == ["started"]

    with pytest.raises(ConflictError, match="unresolved outcome"):
        system["service"].execute_prepared_cleanup(runtime, request)
    assert targets[0].exists()


def test_raw_path_and_branch_sentinels_remain_absent_from_control_plane_surfaces(
    system: dict[str, Any], tmp_path: Path
) -> None:
    raw_path = system["settings"].runtime_launch_dir / "raw-path-sentinel"
    raw_path.mkdir(parents=True)
    artifact_source = raw_path / "artifact-source.txt"
    artifact_source.write_text("preserve without leaking locator", encoding="utf-8")
    work, runtime = _completed_accepted_work(
        system, prefix="surface-sentinels", artifact_path=artifact_source
    )
    temporary = raw_path / "raw-path-sentinel.tmp"
    temporary.write_text("remove me", encoding="utf-8")
    repository = tmp_path / "private-repository-sentinel"
    repository.mkdir()
    raw_branch = "private-branch-sentinel"
    subprocess.run(["git", "init", "-q", "-b", "main", str(repository)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "initial",
        ],
        check=True,
    )
    linked = tmp_path / "private-linked-worktree-sentinel"
    subprocess.run(
        ["git", "-C", str(repository), "worktree", "add", "-qb", raw_branch, str(linked)],
        check=True,
    )
    provider = system["service"].owner_private_close_inventory
    provider.register_resource(
        work_item_id=work["id"], target_kind=CleanupTargetKind.TEMPORARY, locator=temporary
    )
    provider.adopt_managed_worktree(work_item_id=work["id"], workspace=linked)
    preparation = system["service"].prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="surface-sentinels:prepare",
        ),
    )
    system["service"].stop_work_runtime(runtime, work["id"])
    _grant_records(system, _private_records(preparation), standing=True)
    result = system["service"].execute_prepared_cleanup(
        runtime, _execute_input(work, preparation, key="surface-sentinels:execute")
    )
    projection = build_projection(system["service"].db).as_dict()
    events = system["service"].list_events(actor=runtime)

    sentinels = (str(raw_path), raw_branch)
    control_plane_rows: list[dict[str, Any]] = []
    for table in system["service"].db.fetchall(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ):
        control_plane_rows.extend(
            dict(row) for row in system["service"].db.fetchall(f"SELECT * FROM {table['name']}")
        )
    control_plane_surface = json.dumps(
        {
            "preparation": preparation,
            "result": result,
            "projection": projection,
            "events": events,
            "sqlite": control_plane_rows,
        },
        sort_keys=True,
        default=str,
    )
    private_ledger = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            system["settings"].state_dir / "close-cleanup-owner-private-v2.json",
            system["settings"].state_dir / "close-cleanup-freeze-ledger-v2.json",
        )
    )
    for sentinel in sentinels:
        assert sentinel not in control_plane_surface
        assert sentinel in private_ledger
