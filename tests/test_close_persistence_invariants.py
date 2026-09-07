from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.close_contract import CleanupTargetKind
from cao_control_plane.config import Settings
from cao_control_plane.database import Database, backup_sqlite_database
from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    CloseArtifactInput,
    CompletionContract,
    EffectGrantInput,
    EffectKind,
    ExecutePreparedCleanupInput,
    ReportInput,
    ReportKind,
    RequesterDecisionInput,
    ReviewInput,
    ReviewVerdict,
    WorkAssignment,
    WorkCloseInput,
    WorkClosePreparationInput,
)
from cao_control_plane.projection import (
    build_projection,
    project_work_items_from_connection,
    verify_projection,
)

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_DIGEST_C = hashlib.sha256(b"close-persistence-result").hexdigest()


def _completed_accepted_work(
    system: dict[str, Any],
    *,
    declare_no_external_resources: bool = True,
    artifact_path: Path | None = None,
    completion_contract: CompletionContract = CompletionContract.LEGACY_UNCLASSIFIED,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create one exact attachment-bound accepted work generation."""

    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="close-persistence-thread",
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
            title="Close persistence invariant",
            objective="Persist one exact close lifecycle",
            acceptance=["Every durable close binding is exact"],
            completion_contract=completion_contract,
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
            summary="Exact persistence evidence is ready",
            artifacts=[
                ArtifactInput(
                    name="result",
                    uri=(
                        str(artifact_path)
                        if artifact_path is not None
                        else "data:text/plain,close-persistence-result"
                    ),
                    media_type="text/plain",
                    digest=artifact_digest,
                )
            ],
            idempotency_key="close-persistence:completion",
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
        expected_generation=reported["generation"],
        idempotency_key="close-persistence:review-turn",
    )
    service.dispose_boundary(
        runtime,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind=BoundaryDispositionKind.ACCEPT,
            reason="The exact completion boundary was reviewed",
        ),
    )
    reviewed = service.review(
        runtime,
        ReviewInput(
            attempt_id=attempt["id"],
            verdict=ReviewVerdict.OK,
            summary="CAO review is OK",
            idempotency_key="close-persistence:review",
        ),
    )
    decision_result = service.record_requester_decision(
        runtime,
        RequesterDecisionInput(
            review_id=reviewed["reviews"][-1]["id"],
            verdict="accepted",
            summary="Requester accepted in the attached conversation",
            evidence=[],
            conversation_evidence_id="close-persistence:conversation",
            idempotency_key="close-persistence:decision",
        ),
    )
    if declare_no_external_resources:
        for kind in (
            CleanupTargetKind.WORKSPACE,
            CleanupTargetKind.TEMPORARY,
            CleanupTargetKind.LOG,
            CleanupTargetKind.BRANCH,
        ):
            service.owner_private_close_inventory.declare_not_applicable(
                work_item_id=work["id"], target_kind=kind
            )
    return decision_result, service.authenticate(attachment["context_token"])


def _close_request(
    service: Any,
    actor: dict[str, Any],
    work: dict[str, Any],
    preparation: dict[str, Any] | None = None,
) -> WorkCloseInput:
    attempt = work["current_attempt"]
    decision = work["requester_decisions"][-1]
    if preparation is None:
        preparation = service.prepare_work_close(
            actor,
            WorkClosePreparationInput(
                work_item_id=work["id"],
                retention_policy_evidence_id="retention-v1",
                artifact_manifest_evidence_id="artifact-manifest-v1",
                idempotency_key=f"close-persistence:prepare:{work['id']}",
            ),
        )
    else:
        row = service.db.fetchone(
            "SELECT * FROM work_close_preparations WHERE id = ?",
            (preparation["id"],),
        )
        assert row is not None
        preparation = service._close_preparation_view(row)
    preserved = preparation.get("artifact_preservations", [])
    artifact_inputs = (
        [
            CloseArtifactInput(
                artifact_id=str(item["artifact_id"]),
                digest=str(item["digest"]),
                evidence_id=str(item["provider_evidence_id"]),
            )
            for item in preserved
        ]
        if preserved
        else [
            CloseArtifactInput(
                artifact_id=str(item["id"]),
                digest=str(item["digest"]),
                evidence_id="artifact-copy-verified",
            )
            for item in preparation["artifacts"]
        ]
    )
    return WorkCloseInput(
        work_item_id=work["id"],
        attempt_id=attempt["id"],
        review_id=work["reviews"][-1]["id"],
        requester_decision_id=decision["id"],
        expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"],
        retention_policy_evidence_id="retention-v1",
        artifact_manifest_evidence_id="artifact-manifest-v1",
        cleanup_inventory_evidence_id=preparation["id"],
        close_preparation_id=preparation["id"],
        artifacts=artifact_inputs,
        cleanup=[],
        idempotency_key="close-persistence:receipt",
    )


def test_completed_work_is_awaiting_explicit_close_until_a_valid_receipt(
    system: dict[str, Any],
) -> None:
    work, runtime = _completed_accepted_work(system)

    awaiting = build_projection(system["service"].db).snapshot["work_items"]
    awaiting_item = next(item for item in awaiting if item["id"] == work["id"])
    assert awaiting_item["requester_acceptance_source"] == "conversation"
    assert awaiting_item["closure_state"] == "awaiting-explicit-close"
    assert awaiting_item["close_receipt_id"] == ""
    assert awaiting_item["closure_summary"] == {
        "requester_decision": "accepted",
        "cao_review": "ok",
        "artifact_preservation": "pending",
        "cleanup": "pending",
        "unresolved_deliveries": 0,
        "unresolved_effects": None,
        "active_runtimes": 1,
    }
    assert "work.completed_without_requester_acceptance" not in {
        violation.code for violation in build_projection(system["service"].db).violations
    }

    system["service"].stop_work_runtime(runtime, work["id"])
    system["service"].close_work(runtime, _close_request(system["service"], runtime, work))

    closed = build_projection(system["service"].db).snapshot["work_items"]
    closed_item = next(item for item in closed if item["id"] == work["id"])
    assert closed_item["closure_state"] == "closed"
    assert closed_item["close_receipt_id"]
    assert closed_item["closure_summary"] == {
        "requester_decision": "accepted",
        "cao_review": "ok",
        "artifact_preservation": "preserved",
        "cleanup": "verified",
        "unresolved_deliveries": 0,
        "unresolved_effects": 0,
        "active_runtimes": 0,
    }


def test_close_preparation_rechecks_a_required_completion_manifest(
    system: dict[str, Any],
) -> None:
    work, runtime = _completed_accepted_work(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
    )
    attempt = work["current_attempt"]
    claim = dict(attempt["completion_claim"])
    claim["artifacts"] = []
    system["service"].db.execute(
        "UPDATE attempts SET completion_claim_json = ? WHERE id = ?",
        (json.dumps(claim), attempt["id"]),
    )
    system["service"].stop_work_runtime(runtime, work["id"])

    with pytest.raises(ConflictError, match="verified artifact delivery"):
        system["service"].prepare_work_close(
            runtime,
            WorkClosePreparationInput(
                work_item_id=work["id"],
                retention_policy_evidence_id="retention-v1",
                artifact_manifest_evidence_id="artifact-manifest-v1",
                idempotency_key="required-empty-manifest:prepare",
            ),
        )
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM work_close_preparations WHERE work_item_id = ?",
            (work["id"],),
        )["count"]
        == 0
    )


def test_cleanup_and_close_recheck_a_prepared_required_manifest(
    system: dict[str, Any],
) -> None:
    work, runtime = _completed_accepted_work(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
    )
    preparation = system["service"].prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="required-prepared-manifest:prepare",
        ),
    )
    attempt = work["current_attempt"]
    claim = dict(attempt["completion_claim"])
    claim["artifacts"] = []
    system["service"].db.execute(
        "UPDATE attempts SET completion_claim_json = ? WHERE id = ?",
        (json.dumps(claim), attempt["id"]),
    )
    system["service"].stop_work_runtime(runtime, work["id"])

    with pytest.raises(ConflictError, match="verified artifact delivery"):
        system["service"].execute_prepared_cleanup(
            runtime,
            ExecutePreparedCleanupInput(
                close_preparation_id=preparation["id"],
                idempotency_key="required-prepared-manifest:cleanup",
            ),
        )
    with pytest.raises(ConflictError, match="verified artifact delivery"):
        system["service"].close_work(
            runtime,
            _close_request(system["service"], runtime, work, preparation),
        )
    assert (
        system["service"].db.fetchone(
            "SELECT COUNT(*) AS count FROM work_close_receipts WHERE work_item_id = ?",
            (work["id"],),
        )["count"]
        == 0
    )


def test_projection_invalidates_a_closed_required_work_if_its_manifest_is_emptied(
    system: dict[str, Any],
) -> None:
    work, runtime = _completed_accepted_work(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
    )
    system["service"].stop_work_runtime(runtime, work["id"])
    system["service"].close_work(
        runtime,
        _close_request(system["service"], runtime, work),
    )
    attempt = work["current_attempt"]
    claim = dict(attempt["completion_claim"])
    claim["artifacts"] = []
    system["service"].db.execute(
        "UPDATE attempts SET completion_claim_json = ? WHERE id = ?",
        (json.dumps(claim), attempt["id"]),
    )

    projection = build_projection(system["service"].db)
    projected = next(item for item in projection.snapshot["work_items"] if item["id"] == work["id"])
    assert projected["closure_state"] == "awaiting-explicit-close"
    assert projected["close_receipt_id"] == ""
    assert "close_receipt.semantic_invalid" in {
        violation.code for violation in projection.violations
    }


def test_close_manifest_uses_final_claim_not_superseded_artifact_history(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    work, runtime = _completed_accepted_work(system)
    final_artifact_id = work["current_attempt"]["completion_claim"]["artifacts"][0]["id"]
    with service.db.transaction() as connection:
        connection.execute(
            "INSERT INTO artifacts(id, work_item_id, attempt_id, producer_id, name, "
            "uri, media_type, digest, metadata_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)",
            (
                "art_superseded_history",
                work["id"],
                work["current_attempt"]["id"],
                system["worker"]["id"],
                "superseded",
                "unverified-artifact:" + "f" * 64,
                "text/plain",
                "sha256:not-canonical",
                "2026-08-15T00:00:00Z",
            ),
        )

    service.stop_work_runtime(runtime, work["id"])
    preparation = service.prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="close-persistence:prepare:final-claim-only",
        ),
    )

    assert [item["id"] for item in preparation["artifacts"]] == [final_artifact_id]
    service.close_work(runtime, _close_request(service, runtime, work, preparation))
    assert verify_projection(service.db).healthy is True


@pytest.mark.parametrize("corruption", ["omit", "extra-field"])
def test_cleanup_rejects_a_tampered_artifact_manifest_before_any_effect(
    system: dict[str, Any],
    tmp_path: Path,
    corruption: str,
) -> None:
    artifact_source = tmp_path / "must-survive.txt"
    artifact_source.write_text("preserve before cleanup", encoding="utf-8")
    work, runtime = _completed_accepted_work(system, artifact_path=artifact_source)
    service = system["service"]
    preparation = service.prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key=f"tampered-manifest:{corruption}:prepare",
        ),
    )
    prepared_artifacts = [dict(item) for item in preparation["artifacts"]]
    if corruption == "omit":
        prepared_artifacts = []
    else:
        prepared_artifacts[0]["raw_locator"] = str(artifact_source)
    service.db.execute(
        "UPDATE work_close_preparations SET artifacts_json = ? WHERE id = ?",
        (json.dumps(prepared_artifacts), preparation["id"]),
    )
    service.stop_work_runtime(runtime, work["id"])

    with pytest.raises(ConflictError, match=r"malformed|artifact manifest"):
        service.execute_prepared_cleanup(
            runtime,
            ExecutePreparedCleanupInput(
                close_preparation_id=preparation["id"],
                idempotency_key=f"tampered-manifest:{corruption}:execute",
            ),
        )

    assert artifact_source.read_text(encoding="utf-8") == "preserve before cleanup"
    effects = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM effect_operations WHERE cleanup_work_item_id = ?",
        (work["id"],),
    )
    assert effects is not None and effects["count"] == 0
    stored = service.db.fetchone(
        "SELECT artifact_preservations_json, cleanup_executed_at "
        "FROM work_close_preparations WHERE id = ?",
        (preparation["id"],),
    )
    assert stored is not None
    assert json.loads(str(stored["artifact_preservations_json"])) == []
    assert stored["cleanup_executed_at"] is None


def test_v29_all_history_close_preparation_remains_closeable_after_migration(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    work, runtime = _completed_accepted_work(system)
    attempt = work["current_attempt"]
    legacy_artifact = {
        "id": "art_legacy_history_scope",
        "digest": "d" * 64,
    }
    with service.db.transaction() as connection:
        connection.execute(
            "INSERT INTO artifacts(id, work_item_id, attempt_id, producer_id, name, "
            "uri, media_type, digest, metadata_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)",
            (
                legacy_artifact["id"],
                work["id"],
                attempt["id"],
                system["worker"]["id"],
                "legacy-history",
                "workspace:legacy-history",
                "text/plain",
                legacy_artifact["digest"],
                "2026-08-15T00:00:00Z",
            ),
        )
    preparation = service.prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="legacy-history-scope:prepare",
        ),
    )
    all_history = sorted(
        [*preparation["artifacts"], legacy_artifact],
        key=lambda item: str(item["id"]),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE work_close_preparations SET artifacts_json = ? WHERE id = ?",
            (json.dumps(all_history), preparation["id"]),
        )
        connection.execute(
            "ALTER TABLE work_close_preparations DROP COLUMN artifact_manifest_scope"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version >= 30")
        connection.execute("UPDATE metadata SET value = '29' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 29")

    service.db.initialize()
    migrated = service.db.fetchone(
        "SELECT artifact_manifest_scope FROM work_close_preparations WHERE id = ?",
        (preparation["id"],),
    )
    assert migrated is not None
    assert migrated["artifact_manifest_scope"] == "work_history_v1"

    service.stop_work_runtime(runtime, work["id"])
    service.close_work(runtime, _close_request(service, runtime, work, preparation))
    assert verify_projection(service.db).healthy is True


def test_database_copy_verifies_close_receipts_against_bound_owner_private_evidence(
    system: dict[str, Any],
    tmp_path: Path,
) -> None:
    service = system["service"]
    work, runtime = _completed_accepted_work(system)
    service.stop_work_runtime(runtime, work["id"])
    service.close_work(runtime, _close_request(service, runtime, work))
    assert verify_projection(service.db).healthy is True

    copy_state = tmp_path / "copied-state"
    copy_state.mkdir(mode=0o700)
    copy_path = copy_state / "control-plane.sqlite3"
    backup_sqlite_database(service.db.path, copy_path, replace=False)
    copied = Database(
        Settings(
            state_dir=copy_state,
            runtime_launch_dir=copy_state / "runtime-launches",
        )
    )

    unbound = verify_projection(copied)
    assert {item.code for item in unbound.violations} == {"close_receipt.semantic_invalid"}
    bound = verify_projection(
        copied,
        owner_private_state_dir=service.db.path.resolve(strict=True).parent,
    )
    assert bound.healthy is True


def test_owner_private_executor_freezes_and_verifies_tmp_log_and_local_branch(
    system: dict[str, Any], tmp_path: Path
) -> None:
    """Raw locators stay owner-private while local effects get bound receipts."""

    artifact_source = tmp_path / "close-artifact-source.txt"
    artifact_source.write_text("retain this exact artifact", encoding="utf-8")
    work, runtime = _completed_accepted_work(
        system,
        declare_no_external_resources=False,
        artifact_path=artifact_source,
    )
    cleanup_root = system["settings"].runtime_launch_dir / "close-test"
    cleanup_root.mkdir(parents=True)
    temporary = cleanup_root / "disposable-tmp"
    log = cleanup_root / "disposable.log"
    temporary.mkdir()
    log.write_text("private log", encoding="utf-8")
    repository = tmp_path / "private-repository"
    repository.mkdir()
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
    linked = tmp_path / "private-linked-worktree"
    subprocess.run(
        [
            "git",
            "-C",
            str(repository),
            "worktree",
            "add",
            "-qb",
            "close-doomed",
            str(linked),
        ],
        check=True,
    )
    provider = system["service"].owner_private_close_inventory
    provider.register_resource(
        work_item_id=work["id"],
        target_kind=CleanupTargetKind.TEMPORARY,
        locator=temporary,
    )
    provider.register_resource(
        work_item_id=work["id"],
        target_kind=CleanupTargetKind.LOG,
        locator=log,
    )
    provider.adopt_managed_worktree(work_item_id=work["id"], workspace=linked)
    preparation = system["service"].prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="owner-private:prepare",
        ),
    )
    enumerated = [item for item in preparation["inventory"] if item.get("coverage") == "enumerated"]
    assert {item["target_kind"] for item in enumerated} >= {
        "workspace",
        "temporary",
        "log",
    }
    for item in enumerated:
        if item["target_kind"] not in {"workspace", "temporary", "log"}:
            continue
        system["service"].grant_effect(
            system["cao"],
            EffectGrantInput(
                principal_id=system["cao"]["id"],
                kind=EffectKind.DESTRUCTIVE,
                target_pattern=item["target_fingerprint"],
                action_pattern=item["action"],
                content_digest=item["execution_digest"],
                standing=True,
            ),
        )
    system["service"].stop_work_runtime(runtime, work["id"])
    result = system["service"].execute_prepared_cleanup(
        runtime,
        ExecutePreparedCleanupInput(
            close_preparation_id=preparation["id"],
            idempotency_key="owner-private:execute",
        ),
    )
    assert result["verified_cleanup_count"] == 3
    assert not temporary.exists() and not log.exists() and not linked.exists()
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "show-ref",
                "--verify",
                "--quiet",
                "refs/heads/close-doomed",
            ],
            check=False,
        ).returncode
        != 0
    )
    persisted = system["service"].db.fetchone(
        "SELECT inventory_json FROM work_close_preparations WHERE id = ?", (preparation["id"],)
    )
    assert persisted is not None
    assert str(temporary) not in str(persisted["inventory_json"])
    assert str(repository) not in str(persisted["inventory_json"])
    closed = system["service"].close_work(
        runtime, _close_request(system["service"], runtime, work, preparation)
    )
    assert closed["closure"]["state"] == "closed"


def test_tampered_provider_coverage_blocks_close_even_with_a_recomputed_digest(
    system: dict[str, Any],
) -> None:
    """An SQLite writer cannot replace signed N/A coverage with a fresh hash."""

    work, runtime = _completed_accepted_work(system)
    preparation = system["service"].prepare_work_close(
        runtime,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="coverage-tamper:prepare",
        ),
    )
    tampered = [dict(item) for item in preparation["inventory"]]
    coverage = next(item for item in tampered if item.get("coverage") == "not-applicable")
    coverage["provider_evidence_id"] = "hmac-sha256:" + "0" * 64
    encoded = json.dumps(tampered, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    with system["service"].db.connect() as connection:
        connection.execute(
            "UPDATE work_close_preparations SET inventory_json = ?, inventory_digest = ? WHERE id = ?",
            (encoded, hashlib.sha256(encoded.encode()).hexdigest(), preparation["id"]),
        )
    system["service"].stop_work_runtime(runtime, work["id"])
    with pytest.raises(ConflictError, match="cleanup inventory"):
        system["service"].close_work(
            runtime, _close_request(system["service"], runtime, work, preparation)
        )


def _closed_work(system: dict[str, Any]) -> dict[str, Any]:
    work, runtime = _completed_accepted_work(system)
    service = system["service"]
    service.stop_work_runtime(runtime, work["id"])
    service.close_work(runtime, _close_request(service, runtime, work))
    _assert_selected_projection_matches(service.db, work["id"])
    return work


def _assert_selected_projection_matches(database: Database, work_id: str) -> None:
    as_of = "2026-01-01T00:00:00Z"
    full = build_projection(database, as_of=as_of)
    expected = [item for item in full.snapshot["work_items"] if item["id"] == work_id]
    with database.connection_scope() as connection:
        connection.execute("BEGIN")
        assert project_work_items_from_connection(
            connection, work_ids=[work_id], as_of=as_of
        ) == expected


def _assert_invalid_receipt_is_not_projected_as_closed(
    system: dict[str, Any], work: dict[str, Any], forbidden: str
) -> None:
    _assert_selected_projection_matches(system["service"].db, work["id"])
    projection = build_projection(system["service"].db, as_of="2026-01-01T00:00:00Z")
    item = next(value for value in projection.snapshot["work_items"] if value["id"] == work["id"])
    assert item["closure_state"] == "awaiting-explicit-close"
    assert item["close_receipt_id"] == ""
    assert item["closure_summary"]["artifact_preservation"] == "pending"
    assert item["closure_summary"]["cleanup"] == "pending"
    assert "close_receipt.semantic_invalid" in {
        violation.code for violation in projection.violations
    }
    assert forbidden not in json.dumps(projection.as_dict(), sort_keys=True)


@pytest.mark.parametrize(
    ("corruption", "forbidden"),
    [
        ("preparation_inventory", "file:///private/preparation-inventory"),
        ("preparation_digest", "preparation-digest-corruption"),
        ("receipt_cleanup", "file:///private/receipt-cleanup"),
        ("receipt_artifacts", "artifact-uri-should-never-project"),
        ("receipt_plan_digest", "receipt-plan-digest-corruption"),
        ("older_attempt_runtime", "file:///private/older-attempt-runtime"),
        ("effect_binding", "file:///private/effect-target"),
    ],
)
def test_projection_fails_closed_for_post_insert_close_semantic_corruption(
    system: dict[str, Any], corruption: str, forbidden: str
) -> None:
    """A SQL-linked receipt cannot bypass projection's semantic close gate."""

    work = _closed_work(system)
    service = system["service"]
    with service.db.connect() as connection:
        receipt = connection.execute(
            "SELECT * FROM work_close_receipts WHERE work_item_id = ?", (work["id"],)
        ).fetchone()
        assert receipt is not None
        preparation = connection.execute(
            "SELECT * FROM work_close_preparations WHERE id = ?", (receipt["close_preparation_id"],)
        ).fetchone()
        assert preparation is not None
        if corruption == "preparation_inventory":
            connection.execute(
                "UPDATE work_close_preparations SET inventory_json = ? WHERE id = ?",
                (
                    json.dumps(
                        [
                            {
                                "target_kind": "runtime",
                                "target_fingerprint": "f" * 64,
                                "action": "stop",
                                "raw": forbidden,
                            }
                        ]
                    ),
                    preparation["id"],
                ),
            )
        elif corruption == "preparation_digest":
            connection.execute(
                "UPDATE work_close_preparations SET inventory_digest = ? WHERE id = ?",
                (_DIGEST_B, preparation["id"]),
            )
        elif corruption == "receipt_cleanup":
            connection.execute(
                "UPDATE work_close_receipts SET cleanup_json = ? WHERE id = ?",
                (json.dumps([{"evidence_id": forbidden}]), receipt["id"]),
            )
        elif corruption == "receipt_artifacts":
            connection.execute(
                "UPDATE work_close_receipts SET artifacts_json = ? WHERE id = ?",
                (
                    json.dumps(
                        [
                            {
                                "artifact_id": work["artifacts"][0]["id"],
                                "digest": _DIGEST_C,
                                "evidence_id": forbidden,
                            },
                            {
                                "artifact_id": "missing-artifact",
                                "digest": _DIGEST_A,
                                "evidence_id": "artifact-evidence",
                            },
                        ]
                    ),
                    receipt["id"],
                ),
            )
        elif corruption == "receipt_plan_digest":
            connection.execute(
                "UPDATE work_close_receipts SET plan_digest = ? WHERE id = ?",
                (_DIGEST_A, receipt["id"]),
            )
        elif corruption == "older_attempt_runtime":
            final_attempt = connection.execute(
                "SELECT * FROM attempts WHERE id = ?", (receipt["attempt_id"],)
            ).fetchone()
            assert final_attempt is not None
            connection.execute(
                "UPDATE attempts SET attempt_number = 2 WHERE id = ?", (final_attempt["id"],)
            )
            connection.execute(
                """
                INSERT INTO runtime_sessions(
                    id, principal_id, adapter, endpoint, native_session_id, state,
                    lease_expires_at, heartbeat_at, metadata_json, created_at, updated_at
                )
                SELECT 'runtime_older_live', principal_id, adapter, endpoint, ?, 'ready',
                       lease_expires_at, heartbeat_at, metadata_json, created_at, updated_at
                FROM runtime_sessions WHERE id = ?
                """,
                (forbidden, final_attempt["runtime_session_id"]),
            )
            connection.execute(
                """
                INSERT INTO attempts(
                    id, work_item_id, attempt_number, worker_id, runtime_session_id,
                    goal_version, goal_packet_digest, task_packet_digest, state,
                    trajectory, evidence_confidence, stage, next_boundary,
                    completion_claim_json, created_at, updated_at
                )
                SELECT 'attempt_older_live', work_item_id, 1, worker_id, ?,
                       goal_version, goal_packet_digest, task_packet_digest, state,
                       trajectory, evidence_confidence, stage, next_boundary,
                       completion_claim_json, created_at, updated_at
                FROM attempts WHERE id = ?
                """,
                ("runtime_older_live", final_attempt["id"]),
            )
        else:
            assert corruption == "effect_binding"
            connection.execute(
                """
                INSERT INTO effect_operations(
                    id, principal_id, kind, target, action, content_digest,
                    argv_digest, workdir_digest, status, evidence, grant_id,
                    cleanup_work_item_id, cleanup_generation, cleanup_target_kind,
                    cleanup_target_fingerprint, created_at, updated_at
                ) VALUES (?, ?, 'local', ?, 'delete', '', '', '', 'succeeded', '', NULL,
                          ?, ?, 'workspace', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (
                    "effect_corrupt",
                    receipt["closed_by"],
                    forbidden,
                    work["id"],
                    receipt["work_generation"],
                    "f" * 64,
                ),
            )

    _assert_invalid_receipt_is_not_projected_as_closed(system, work, forbidden)


def test_close_receipt_schema_rejects_orphan_duplicate_and_stale_bindings(
    system: dict[str, Any],
) -> None:
    work, runtime = _completed_accepted_work(system)
    system["service"].stop_work_runtime(runtime, work["id"])
    system["service"].close_work(runtime, _close_request(system["service"], runtime, work))

    with system["service"].db.connect() as connection:
        receipt = connection.execute(
            "SELECT * FROM work_close_receipts WHERE work_item_id = ?", (work["id"],)
        ).fetchone()
        assert receipt is not None
        decision = connection.execute(
            "SELECT * FROM requester_decisions WHERE id = ?", (receipt["requester_decision_id"],)
        ).fetchone()
        assert decision is not None
        for column, value in (
            (
                "supervisor_attachment_generation",
                int(decision["supervisor_attachment_generation"]) + 1,
            ),
            ("goal_packet_digest", _DIGEST_B),
            ("work_generation", int(decision["work_generation"]) + 1),
        ):
            with pytest.raises(sqlite3.IntegrityError, match="exact binding"):
                connection.execute(
                    f"UPDATE requester_decisions SET {column} = ? WHERE id = ?",
                    (value, decision["id"]),
                )
        values = dict(receipt)
        columns = tuple(values)
        placeholders = ", ".join("?" for _ in columns)

        duplicate = {**values, "id": "close-duplicate"}
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                f"INSERT INTO work_close_receipts({', '.join(columns)}) VALUES({placeholders})",
                tuple(duplicate[column] for column in columns),
            )
        orphan = {**values, "id": "close-orphan", "work_item_id": "missing-work"}
        with pytest.raises(sqlite3.IntegrityError, match="exact binding"):
            connection.execute(
                f"INSERT INTO work_close_receipts({', '.join(columns)}) VALUES({placeholders})",
                tuple(orphan[column] for column in columns),
            )
        for column, value in (
            (
                "supervisor_attachment_generation",
                int(values["supervisor_attachment_generation"]) + 1,
            ),
            ("goal_packet_digest", _DIGEST_B),
            ("work_generation", int(values["work_generation"]) + 1),
        ):
            with pytest.raises(sqlite3.IntegrityError, match="exact binding"):
                connection.execute(
                    f"UPDATE work_close_receipts SET {column} = ? WHERE id = ?",
                    (value, values["id"]),
                )


def test_reinitialization_accepts_a_historic_decision_after_new_attachment_connection(
    system: dict[str, Any],
) -> None:
    """A later transport connection must not invalidate requester acceptance."""

    work, _ = _completed_accepted_work(system)
    decision = work["requester_decisions"][-1]
    attachment = system["service"].db.fetchone(
        "SELECT * FROM cao_session_attachments WHERE id = ?",
        (decision["supervisor_attachment_id"],),
    )
    assert attachment is not None

    _actor, connected = _connect_close_attachment(system, work)
    assert connected["generation"] == int(decision["supervisor_attachment_generation"])

    # Database initialization validates all historic close evidence.  It must
    # accept the decision sealed under the prior epoch of this same attachment.
    system["service"].db.initialize()


def test_migration_rejects_a_nonempty_close_packet_binding_that_disagrees_with_source(
    system: dict[str, Any],
) -> None:
    work, runtime = _completed_accepted_work(system)
    system["service"].stop_work_runtime(runtime, work["id"])
    system["service"].close_work(runtime, _close_request(system["service"], runtime, work))

    with system["service"].db.connect() as connection:
        connection.execute("DROP TRIGGER work_close_receipts_exact_binding_update")
        connection.execute(
            "UPDATE work_close_receipts SET goal_packet_digest = ? WHERE work_item_id = ?",
            (_DIGEST_B, work["id"]),
        )
        connection.execute("UPDATE metadata SET value = '12' WHERE key = 'schema_version'")

    # A downgraded attachment-bound database may be rejected first by the
    # immutable Goal packet gate or by the close binding gate.  Either is the
    # required fail-closed outcome; migration must never repair the mismatch.
    with pytest.raises(RuntimeError, match=r"invalid|cannot be reconstructed"):
        system["service"].db.initialize()


def _connect_close_attachment(
    system: dict[str, Any], work: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = system["service"]
    decision = work["requester_decisions"][-1]
    attachment = service.db.fetchone(
        "SELECT * FROM cao_session_attachments WHERE id = ?",
        (decision["supervisor_attachment_id"],),
    )
    assert attachment is not None
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?",
        (attachment["runtime_session_id"],),
    )
    wake_before = dict(
        service.db.fetchone(
            "SELECT * FROM runtime_sessions WHERE id = ?",
            (attachment["runtime_session_id"],),
        )
    )
    work_before = dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
    worker_before = dict(
        service.db.fetchone(
            "SELECT * FROM principals WHERE id = ?", (work_before["assigned_worker_id"],)
        )
    )
    connections_before = tuple(
        tuple(row)
        for row in service.db.fetchall(
            "SELECT id, state, revoked_at, lease_expires_at "
            "FROM cao_attachment_connections WHERE attachment_id = ? ORDER BY id",
            (attachment["id"],),
        )
    )
    credentials_before = tuple(
        tuple(row)
        for row in service.db.fetchall(
            "SELECT id, connection_id, state, revoked_at, expires_at "
            "FROM cao_conversation_credentials WHERE attachment_id = ? ORDER BY id",
            (attachment["id"],),
        )
    )
    prior_connection_generation = int(
        service.db.fetchone(
            "SELECT MAX(connection_generation) AS value "
            "FROM cao_attachment_connections WHERE attachment_id = ?",
            (attachment["id"],),
        )["value"]
    )
    connected = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="close-persistence-thread",
            project_digest=_DIGEST_A,
            model="gpt-5.6-terra",
            sandbox="workspace-write",
        ),
    )
    actor = service.authenticate(connected["context_token"])
    assert connected["id"] == attachment["id"]
    assert connected["runtime_session_id"] != attachment["runtime_session_id"]
    assert connected["runtime"]["state"] == "ready"
    assert connected["runtime"]["native_session_id"] == "close-persistence-thread"
    assert int(connected["generation"]) == int(decision["supervisor_attachment_generation"])
    assert int(connected["connection_generation"]) == prior_connection_generation + 1
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM runtime_sessions WHERE id = ?",
                (attachment["runtime_session_id"],),
            )
        )
        == wake_before
    )
    assert dict(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))) == (
        work_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM principals WHERE id = ?", (work_before["assigned_worker_id"],)
            )
        )
        == worker_before
    )
    assert (
        tuple(
            tuple(row)
            for row in service.db.fetchall(
                "SELECT id, state, revoked_at, lease_expires_at "
                "FROM cao_attachment_connections WHERE attachment_id = ? AND id <> ? ORDER BY id",
                (attachment["id"], connected["connection_id"]),
            )
        )
        == connections_before
    )
    assert (
        tuple(
            tuple(row)
            for row in service.db.fetchall(
                "SELECT id, connection_id, state, revoked_at, expires_at "
                "FROM cao_conversation_credentials WHERE attachment_id = ? AND connection_id <> ? "
                "ORDER BY id",
                (attachment["id"], connected["connection_id"]),
            )
        )
        == credentials_before
    )
    return actor, connected


def test_v28_reinstalls_monotonic_close_receipt_binding(system: dict[str, Any]) -> None:
    service = system["service"]
    with service.db.transaction() as connection:
        for suffix, event in (("insert", "INSERT"), ("update", "UPDATE")):
            trigger = f"work_close_receipts_exact_binding_{suffix}"
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
            connection.execute(
                f"""
                CREATE TRIGGER {trigger}
                BEFORE {event} ON work_close_receipts
                FOR EACH ROW BEGIN
                    SELECT CASE WHEN NOT EXISTS (
                        SELECT 1 FROM cao_session_attachments AS attachment
                        JOIN requester_decisions AS decision
                          ON decision.id = NEW.requester_decision_id
                        WHERE attachment.id = NEW.supervisor_attachment_id
                          AND attachment.generation =
                              NEW.supervisor_attachment_generation
                          AND decision.supervisor_attachment_generation =
                              NEW.supervisor_attachment_generation
                    ) THEN RAISE(
                        ABORT, 'interim close receipt generation violation'
                    ) END;
                END
                """
            )
        connection.execute("DELETE FROM schema_migrations WHERE version > 27")
        connection.execute("UPDATE metadata SET value = '27' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 27")

    service.db.initialize()

    trigger = service.db.fetchone(
        "SELECT sql FROM sqlite_master "
        "WHERE type = 'trigger' "
        "AND name = 'work_close_receipts_exact_binding_insert'"
    )
    assert trigger is not None
    sql = " ".join(str(trigger["sql"]).split())
    assert "attachment.generation >= NEW.supervisor_attachment_generation" in sql
    assert "d.supervisor_attachment_generation <= NEW.supervisor_attachment_generation" in sql


def test_close_uses_historic_requester_decision_after_new_connection(
    system: dict[str, Any],
    tmp_path: Path,
) -> None:
    """Credential-epoch renewal cannot strand an accepted Work before prepare."""

    service = system["service"]
    artifact_path = tmp_path / "renew-before-prepare-result.txt"
    artifact_path.write_text("renew-before-prepare", encoding="utf-8")
    work, _ = _completed_accepted_work(system, artifact_path=artifact_path)
    connected_actor, _connected = _connect_close_attachment(system, work)
    service.stop_work_runtime(connected_actor, work["id"])
    preparation = service.prepare_work_close(
        connected_actor,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="renew-before-prepare:prepare",
        ),
    )
    execution = service.execute_prepared_cleanup(
        connected_actor,
        ExecutePreparedCleanupInput(
            close_preparation_id=preparation["id"],
            idempotency_key="renew-before-prepare:execute",
        ),
    )
    assert execution["close_preparation_id"] == preparation["id"]
    closed = service.close_work(
        connected_actor,
        _close_request(
            service,
            connected_actor,
            service.get_work(work["id"]),
            preparation,
        ),
    )
    assert closed["closure"]["state"] == "closed"


def test_prepared_close_survives_a_new_attachment_connection(
    system: dict[str, Any],
    tmp_path: Path,
) -> None:
    """A prepared close remains usable by a later epoch of the same attachment."""

    service = system["service"]
    artifact_path = tmp_path / "renew-after-prepare-result.txt"
    artifact_path.write_text("renew-after-prepare", encoding="utf-8")
    work, original_actor = _completed_accepted_work(system, artifact_path=artifact_path)
    preparation = service.prepare_work_close(
        original_actor,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key="renew-after-prepare:prepare",
        ),
    )
    connected_actor, _connected = _connect_close_attachment(system, work)
    service.stop_work_runtime(connected_actor, work["id"])
    execution = service.execute_prepared_cleanup(
        connected_actor,
        ExecutePreparedCleanupInput(
            close_preparation_id=preparation["id"],
            idempotency_key="renew-after-prepare:execute",
        ),
    )
    assert execution["close_preparation_id"] == preparation["id"]
    closed = service.close_work(
        connected_actor,
        _close_request(
            service,
            connected_actor,
            service.get_work(work["id"]),
            preparation,
        ),
    )
    assert closed["closure"]["state"] == "closed"
