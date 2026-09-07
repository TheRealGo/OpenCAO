from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment

from cao_control_plane.dashboard import DashboardReadModel
from cao_control_plane.database import SCHEMA_VERSION
from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    BoundaryDispositionInput,
    CompletionContract,
    ReportInput,
    RequesterDecisionInput,
    ReviewInput,
    RuntimeHeartbeat,
    WorkAssignment,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane


def _pre_completion_contract_digest(request: WorkAssignment) -> str:
    fields = request.model_dump(mode="json")
    fields.pop("completion_contract", None)
    encoded = json.dumps(fields, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _attached_cao(
    service: ControlPlane,
    cao: dict[str, Any],
    *,
    thread: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=thread,
            project_digest="a" * 64,
        ),
    )
    return attachment, service.authenticate(attachment["context_token"])


def _enroll_managed_worker(service: ControlPlane, work_id: str) -> dict[str, Any]:
    row = service.db.fetchone(
        """
        SELECT spec.runtime_session_id, attempt.id AS attempt_id
        FROM work_items AS work
        JOIN attempts AS attempt ON attempt.work_item_id = work.id
        JOIN managed_worker_specs AS spec
          ON spec.principal_id = attempt.worker_id
         AND spec.runtime_session_id = attempt.runtime_session_id
        WHERE work.id = ?
        """,
        (work_id,),
    )
    assert row is not None
    launch = service.issue_runtime_launch_ticket(str(row["runtime_session_id"]))
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2026-07-28",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        str(row["runtime_session_id"]),
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    return worker


def _report_completion(
    service: ControlPlane,
    worker: dict[str, Any],
    work: dict[str, Any],
    *,
    artifacts: list[ArtifactInput] | None = None,
    key: str,
) -> dict[str, Any]:
    attempt = work["current_attempt"]
    return service.report(
        worker,
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The completion claim is ready for independent review.",
            trajectory="complete",
            evidence=[{"check": "completion-contract", "result": "pass"}],
            artifacts=artifacts or [],
            idempotency_key=key,
        ),
    )


def _accept_completion_boundary(
    service: ControlPlane,
    cao: dict[str, Any],
    reported: dict[str, Any],
    *,
    key: str,
) -> dict[str, Any]:
    boundary = next(item for item in reported["open_boundaries"] if item["kind"] == "completion")
    turn = service.acquire_reasoner_turn(
        cao,
        reported["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key=f"{key}:turn",
    )
    service.dispose_boundary(
        cao,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind="accept",
            reason="The declared completion is ready for the review gate.",
        ),
    )
    return service.get_work(reported["id"])


def _assign_direct(
    system: dict[str, Any],
    *,
    completion_contract: CompletionContract,
    key: str,
    cao: dict[str, Any] | None = None,
    attachment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return system["service"].assign_work(
        cao or system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            supervisor_attachment_id=attachment["id"] if attachment else None,
            supervisor_project_digest=attachment["project_digest"] if attachment else None,
            title=f"Completion contract {key}",
            objective="Exercise the completion delivery contract.",
            acceptance=["The delivery state is derived from canonical artifacts."],
            completion_contract=completion_contract,
            idempotency_key=f"assign:{key}",
        ),
    )


def test_assign_work_replays_a_pre_completion_contract_digest(system) -> None:
    service = system["service"]
    request = WorkAssignment(
        worker_id=system["worker"]["id"],
        runtime_session_id=system["runtime"]["id"],
        title="Legacy assignment digest",
        objective="Replay one assignment created before completion contracts existed.",
        acceptance=["The retry returns the original durable Work."],
        idempotency_key="assign:legacy-completion-contract-digest",
    )
    created = service.assign_work(system["cao"], request)
    updated = service.db.execute(
        "UPDATE idempotency_results SET request_digest = ? "
        "WHERE actor_id = ? AND operation = 'assign_work' AND idempotency_key = ?",
        (
            _pre_completion_contract_digest(request),
            system["cao"]["id"],
            request.idempotency_key,
        ),
    )
    assert updated == 1
    before = service.db.fetchone("SELECT COUNT(*) AS count FROM work_items")

    replayed = service.assign_work(system["cao"], request)

    after = service.db.fetchone("SELECT COUNT(*) AS count FROM work_items")
    assert replayed == created
    assert before is not None and after is not None
    assert after["count"] == before["count"]


def test_delivery_readiness_distinguishes_digest_artifact_and_no_artifact_contract(
    system,
    tmp_path: Path,
) -> None:
    _attachment, cao = _attached_cao(
        system["service"],
        system["cao"],
        thread="completion-delivery-contract",
    )
    artifact_path = tmp_path / "result.txt"
    artifact_path.write_bytes(b"digest-bound-result")
    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    cases = (
        (
            CompletionContract.COMPLETION_REQUIRED,
            [
                ArtifactInput(
                    name="result.txt",
                    uri=str(artifact_path),
                    media_type="text/plain",
                    digest=digest,
                )
            ],
            "ready",
            "required",
        ),
        (
            CompletionContract.NO_ARTIFACT_EXPECTED,
            [],
            "not_required",
            "no-artifact",
        ),
    )

    for contract, artifacts, expected_state, key in cases:
        work = _assign_direct(
            system,
            completion_contract=contract,
            key=key,
            cao=cao,
        )
        assert work["delivery_state"] == "pending"
        reported = _report_completion(
            system["service"],
            system["worker"],
            work,
            artifacts=artifacts,
            key=f"report:{key}",
        )
        assert reported["delivery_state"] == expected_state
        submitted = _accept_completion_boundary(
            system["service"],
            cao,
            reported,
            key=f"boundary:{key}",
        )
        reviewed = system["service"].review(
            cao,
            ReviewInput(
                attempt_id=submitted["current_attempt"]["id"],
                verdict="ok",
                summary="The completion delivery contract was satisfied.",
                idempotency_key=f"review:{key}",
            ),
        )
        assert reviewed["state"] == "waiting_user"
        accepted = system["service"].record_requester_decision(
            cao,
            RequesterDecisionInput(
                review_id=reviewed["reviews"][-1]["id"],
                verdict="accepted",
                summary="The requester accepted the delivered result.",
                conversation_evidence_id=f"completion-contract:{key}",
                idempotency_key=f"accept:{key}",
            ),
        )
        assert accepted["state"] == "completed"
        assert accepted["delivery_state"] == expected_state

    invalid_digest_work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="invalid-digest",
    )
    invalid_digest_report = _report_completion(
        system["service"],
        system["worker"],
        invalid_digest_work,
        artifacts=[
            ArtifactInput(
                name="unverified-result.txt",
                uri="https://example.invalid/unverified-result.txt",
                media_type="text/plain",
                digest="not-a-sha256",
            )
        ],
        key="report:invalid-digest",
    )
    assert invalid_digest_report["delivery_state"] == "delivery_missing"


def test_standard_prefixed_sha256_artifact_is_canonical_and_delivery_ready(
    system,
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "prefixed-result.txt"
    artifact_path.write_bytes(b"prefixed-digest-result")
    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="prefixed-sha256",
    )

    reported = _report_completion(
        system["service"],
        system["worker"],
        work,
        artifacts=[
            ArtifactInput(
                name="prefixed-result.txt",
                uri=str(artifact_path),
                media_type="text/plain",
                digest=f"sha256:{digest}",
            )
        ],
        key="report:prefixed-sha256",
    )

    assert reported["delivery_state"] == "ready"
    assert reported["current_attempt"]["completion_claim"]["artifacts"][0]["digest"] == digest
    artifact = system["service"].db.fetchone(
        "SELECT digest, uri FROM artifacts WHERE attempt_id = ?",
        (reported["current_attempt"]["id"],),
    )
    assert artifact is not None
    assert artifact["digest"] == digest
    assert str(artifact["uri"]).startswith("owner-private-artifact:")


def test_artifact_report_registers_without_delivery_then_claim_freezes_it(
    system,
) -> None:
    service = system["service"]
    content = b"registered-before-completion"
    data_uri = "data:text/plain,registered-before-completion"
    digest = hashlib.sha256(content).hexdigest()
    _, cao = _attached_cao(
        service,
        system["cao"],
        thread="artifact-then-claim",
    )
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="artifact-then-claim",
        cao=cao,
    )
    attempt = work["current_attempt"]

    registered = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="artifact",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The result is durably registered; no approval is expected.",
            artifacts=[
                ArtifactInput(
                    name="registered.txt",
                    uri=data_uri,
                    media_type="text/plain",
                    digest=f"sha256:{digest}",
                )
            ],
            idempotency_key="artifact-then-claim:artifact",
        ),
    )

    artifact_message = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'artifact'",
        (attempt["id"],),
    )
    assert artifact_message is not None
    delivery_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM message_deliveries WHERE message_id = ?",
        (artifact_message["id"],),
    )
    assert delivery_count is not None and delivery_count["count"] == 0
    assert registered["delivery_state"] == "pending"

    completed = _report_completion(
        service,
        system["worker"],
        registered,
        artifacts=[],
        key="artifact-then-claim:completion",
    )
    claim = completed["current_attempt"]["completion_claim"]
    assert claim["artifact_manifest_scope"] == "verified_attempt_artifacts_v2"
    assert [item["id"] for item in claim["artifacts"]] == [registered["artifacts"][0]["id"]]
    assert completed["delivery_state"] == "ready"
    archive = system["settings"].state_dir / "artifact-archive-v1" / digest[:2] / digest
    assert archive.read_bytes() == content
    durable_text = "\n".join(
        str(row["value"])
        for row in service.db.fetchall(
            """
            SELECT uri AS value FROM artifacts WHERE attempt_id = ?
            UNION ALL
            SELECT payload_json AS value FROM messages WHERE attempt_id = ?
            UNION ALL
            SELECT data_json AS value FROM events WHERE aggregate_id = ?
            UNION ALL
            SELECT completion_claim_json AS value FROM attempts WHERE id = ?
            UNION ALL
            SELECT result_json AS value FROM idempotency_results
             WHERE actor_id = ? AND operation = 'report'
            """,
            (
                attempt["id"],
                attempt["id"],
                attempt["id"],
                attempt["id"],
                system["worker"]["id"],
            ),
        )
    )
    assert data_uri not in durable_text
    assert content.decode("utf-8") not in durable_text
    dashboard_text = json.dumps(DashboardReadModel(service).snapshot(), sort_keys=True)
    assert data_uri not in dashboard_text
    assert content.decode("utf-8") not in dashboard_text

    later_content = b"registered only after the completion manifest was frozen"
    later_digest = hashlib.sha256(later_content).hexdigest()
    later_uri = service.owner_private_artifact_preservation.stage_bytes(
        digest=later_digest,
        content=later_content,
    )
    created_at = service.db.fetchone(
        "SELECT created_at FROM artifacts WHERE id = ?",
        (registered["artifacts"][0]["id"],),
    )["created_at"]
    service.db.execute(
        """
        INSERT INTO artifacts(
            id, work_item_id, attempt_id, producer_id, name, uri,
            media_type, digest, metadata_json, created_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)
        """,
        (
            "art_later_verified",
            work["id"],
            attempt["id"],
            system["worker"]["id"],
            "later.txt",
            later_uri,
            "text/plain",
            later_digest,
            created_at,
        ),
    )
    after_later_artifact = service.get_work(work["id"])
    assert [
        item["id"]
        for item in after_later_artifact["current_attempt"]["completion_claim"]["artifacts"]
    ] == [registered["artifacts"][0]["id"]]
    assert after_later_artifact["delivery_state"] == "ready"


def test_unresolved_artifact_locator_is_opaque_and_never_enters_final_manifest(
    system,
) -> None:
    service = system["service"]
    raw_uri = "workspace:/Users/example/private/result.txt"
    raw_uri_digest = hashlib.sha256(raw_uri.encode("utf-8")).hexdigest()
    content_digest = hashlib.sha256(b"unverified-content").hexdigest()
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="opaque-unverified-artifact",
    )
    attempt = work["current_attempt"]

    registered = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="artifact",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Register one unresolved artifact reference.",
            artifacts=[
                ArtifactInput(
                    name="unresolved.txt",
                    uri=raw_uri,
                    media_type="text/plain",
                    digest=content_digest,
                )
            ],
            idempotency_key="opaque-unverified-artifact:artifact",
        ),
    )
    artifact = registered["artifacts"][0]
    assert artifact["uri"] == f"unverified-artifact:{artifact['id']}"

    durable_text = "\n".join(
        str(row["value"])
        for row in service.db.fetchall(
            """
            SELECT uri AS value FROM artifacts WHERE attempt_id = ?
            UNION ALL
            SELECT payload_json AS value FROM messages WHERE attempt_id = ?
            UNION ALL
            SELECT data_json AS value FROM events WHERE aggregate_id = ?
            UNION ALL
            SELECT result_json AS value FROM idempotency_results
             WHERE actor_id = ? AND operation = 'report'
            """,
            (attempt["id"], attempt["id"], attempt["id"], system["worker"]["id"]),
        )
    )
    assert raw_uri not in durable_text
    assert raw_uri_digest not in durable_text

    completed = _report_completion(
        service,
        system["worker"],
        registered,
        artifacts=[],
        key="opaque-unverified-artifact:completion",
    )
    claim = completed["current_attempt"]["completion_claim"]
    assert claim["artifact_manifest_scope"] == "verified_attempt_artifacts_v2"
    assert claim["artifacts"] == []
    assert completed["delivery_state"] == "delivery_missing"


@pytest.mark.parametrize(
    "invalid_digest",
    [
        "sha512:" + "a" * 64,
        "sha256:" + "a" * 63,
        "sha256:" + "A" * 64,
        " sha256:" + "a" * 64,
        "sha256:sha256:" + "a" * 64,
    ],
)
def test_digest_lookalikes_never_satisfy_delivery_gate(
    system,
    invalid_digest: str,
) -> None:
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="digest-lookalike-" + hashlib.sha256(invalid_digest.encode()).hexdigest()[:8],
    )
    reported = _report_completion(
        system["service"],
        system["worker"],
        work,
        artifacts=[
            ArtifactInput(
                name="lookalike.txt",
                uri="data:text/plain,lookalike",
                media_type="text/plain",
                digest=invalid_digest,
            )
        ],
        key="digest-lookalike-report-" + hashlib.sha256(invalid_digest.encode()).hexdigest()[:8],
    )
    assert reported["delivery_state"] == "delivery_missing"


def test_schema_migration_does_not_promote_historical_artifact_strings(
    system,
) -> None:
    service = system["service"]
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="prefixed-sha256-migration",
    )
    attempt_id = work["current_attempt"]["id"]
    valid_digest = "a" * 64
    attachment, attached_cao = _attached_cao(
        service,
        system["cao"],
        thread="prefixed-sha256-migration-sealed",
    )
    sealed_work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="prefixed-sha256-migration-sealed",
        cao=attached_cao,
    )
    sealed_attempt = sealed_work["current_attempt"]
    with service.db.transaction() as connection:
        connection.execute(
            "INSERT INTO artifacts(id, work_item_id, attempt_id, producer_id, name, "
            "uri, media_type, digest, metadata_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)",
            (
                "art_prefixed_migration",
                work["id"],
                attempt_id,
                system["worker"]["id"],
                "migration-result.txt",
                f"owner-private-artifact:{valid_digest}",
                "text/plain",
                f"sha256:{valid_digest}",
                "2026-08-15T00:00:00Z",
            ),
        )
        connection.execute(
            "INSERT INTO artifacts(id, work_item_id, attempt_id, producer_id, name, "
            "uri, media_type, digest, metadata_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)",
            (
                "art_invalid_migration",
                work["id"],
                attempt_id,
                system["worker"]["id"],
                "unverified-result.txt",
                "https://example.invalid/unverified-result.txt",
                "text/plain",
                "sha256:not-a-digest",
                "2026-08-15T00:00:01Z",
            ),
        )
        connection.execute(
            "UPDATE attempts SET completion_claim_json = ? WHERE id = ?",
            (
                json.dumps(
                    {
                        "summary": "Legacy prefixed claim",
                        "artifacts": [
                            {
                                "id": "art_prefixed_migration",
                                "uri": f"owner-private-artifact:{valid_digest}",
                                "digest": f"sha256:{valid_digest}",
                            }
                        ],
                    }
                ),
                attempt_id,
            ),
        )
        connection.execute(
            "INSERT INTO artifacts(id, work_item_id, attempt_id, producer_id, name, "
            "uri, media_type, digest, metadata_json, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)",
            (
                "art_sealed_prefixed_migration",
                sealed_work["id"],
                sealed_attempt["id"],
                system["worker"]["id"],
                "sealed-result.txt",
                f"owner-private-artifact:{valid_digest}",
                "text/plain",
                f"sha256:{valid_digest}",
                "2026-08-15T00:00:02Z",
            ),
        )
        connection.execute(
            "INSERT INTO work_close_preparations("
            "id, work_item_id, final_attempt_id, work_generation, goal_version, "
            "goal_packet_digest, task_packet_digest, supervisor_attachment_id, "
            "supervisor_attachment_generation, retention_policy_evidence_id, "
            "artifact_manifest_evidence_id, artifacts_json, inventory_json, "
            "inventory_digest, created_by, created_at"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?)",
            (
                "closeprep_prefixed_migration",
                sealed_work["id"],
                sealed_attempt["id"],
                sealed_work["generation"],
                sealed_work["goal_version"],
                sealed_attempt["goal_packet_digest"],
                sealed_attempt["task_packet_digest"],
                attachment["id"],
                attachment["generation"],
                "retention-evidence",
                "artifact-evidence",
                json.dumps(
                    [
                        {
                            "id": "art_sealed_prefixed_migration",
                            "digest": f"sha256:{valid_digest}",
                        }
                    ]
                ),
                hashlib.sha256(b"[]").hexdigest(),
                attached_cao["id"],
                "2026-08-15T00:00:03Z",
            ),
        )
        connection.execute("DELETE FROM schema_migrations WHERE version >= 30")
        connection.execute("UPDATE metadata SET value = '29' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 29")

    service.db.initialize()

    rows = service.db.fetchall(
        "SELECT id, digest FROM artifacts WHERE id IN (?, ?, ?) ORDER BY id",
        (
            "art_prefixed_migration",
            "art_invalid_migration",
            "art_sealed_prefixed_migration",
        ),
    )
    assert [(row["id"], row["digest"]) for row in rows] == [
        ("art_invalid_migration", "sha256:not-a-digest"),
        ("art_prefixed_migration", f"sha256:{valid_digest}"),
        ("art_sealed_prefixed_migration", f"sha256:{valid_digest}"),
    ]
    assert service.get_work(work["id"])["delivery_state"] == "delivery_missing"
    assert service.db.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION


@pytest.mark.parametrize("corruption", ["unknown-scope", "prefixed-v2-digest"])
def test_new_completion_manifest_is_strict_in_service_and_dashboard(
    system,
    corruption: str,
) -> None:
    service = system["service"]
    _, cao = _attached_cao(
        service,
        system["cao"],
        thread=f"strict-completion-manifest-{corruption}",
    )
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key=f"strict-manifest-{corruption}",
        cao=cao,
    )
    content = b"strict manifest result"
    digest = hashlib.sha256(content).hexdigest()
    reported = _report_completion(
        service,
        system["worker"],
        work,
        artifacts=[
            ArtifactInput(
                name="strict.txt",
                uri="data:text/plain,strict%20manifest%20result",
                media_type="text/plain",
                digest=digest,
            )
        ],
        key=f"strict-manifest-report-{corruption}",
    )
    assert reported["delivery_state"] == "ready"
    claim = dict(reported["current_attempt"]["completion_claim"])
    claim["artifacts"] = [dict(item) for item in claim["artifacts"]]
    if corruption == "unknown-scope":
        claim["artifact_manifest_scope"] = "future_manifest_v3"
    else:
        claim["artifacts"][0]["digest"] = f"sha256:{digest}"
    service.db.execute(
        "UPDATE attempts SET completion_claim_json = ? WHERE id = ?",
        (json.dumps(claim), reported["current_attempt"]["id"]),
    )

    assert service.get_work(work["id"])["delivery_state"] == "delivery_missing"
    projected_item = next(
        item
        for item in build_projection(service.db).snapshot["work_items"]
        if item["id"] == work["id"]
    )
    assert projected_item["operator_content"]["delivery_state"] == "delivery_missing"


def test_requester_acceptance_rechecks_required_delivery_after_review(
    system,
    tmp_path: Path,
) -> None:
    service = system["service"]
    _, cao = _attached_cao(
        service,
        system["cao"],
        thread="requester-delivery-recheck",
    )
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.COMPLETION_REQUIRED,
        key="requester-recheck",
        cao=cao,
    )
    artifact_path = tmp_path / "reviewed-result.txt"
    artifact_path.write_bytes(b"reviewed-result")
    reported = _report_completion(
        service,
        system["worker"],
        work,
        artifacts=[
            ArtifactInput(
                name="reviewed-result.txt",
                uri=str(artifact_path),
                media_type="text/plain",
                digest=hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
            )
        ],
        key="report:requester-recheck",
    )
    submitted = _accept_completion_boundary(
        service,
        cao,
        reported,
        key="boundary:requester-recheck",
    )
    reviewed = service.review(
        cao,
        ReviewInput(
            attempt_id=submitted["current_attempt"]["id"],
            verdict="ok",
            summary="The artifact was present during review.",
            idempotency_key="review:requester-recheck",
        ),
    )
    assert reviewed["delivery_state"] == "ready"

    attempt_id = reviewed["current_attempt"]["id"]
    attempt_row = service.db.fetchone(
        "SELECT completion_claim_json FROM attempts WHERE id = ?",
        (attempt_id,),
    )
    assert attempt_row is not None
    claim = json.loads(str(attempt_row["completion_claim_json"]))
    claim["artifacts"] = []
    service.db.execute("DELETE FROM artifacts WHERE attempt_id = ?", (attempt_id,))
    service.db.execute(
        "UPDATE attempts SET completion_claim_json = ? WHERE id = ?",
        (json.dumps(claim, sort_keys=True, separators=(",", ":")), attempt_id),
    )
    missing = service.get_work(work["id"])
    assert missing["delivery_state"] == "delivery_missing"

    with pytest.raises(ConflictError):
        service.record_requester_decision(
            cao,
            RequesterDecisionInput(
                review_id=reviewed["reviews"][-1]["id"],
                verdict="accepted",
                summary="The requester accepts only a presently deliverable result.",
                evidence=[],
                conversation_evidence_id="requester-delivery-recheck",
                idempotency_key="accept:requester-recheck",
            ),
        )
    still_waiting = service.get_work(work["id"])
    assert still_waiting["state"] == "waiting_user"
    assert still_waiting["requester_decisions"] == []


def test_terminal_dead_delivery_acknowledges_directly_to_audited_handled(system) -> None:
    service = system["service"]
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.LEGACY_UNCLASSIFIED,
        key="terminal-dead",
    )
    assignment = service.get_inbox(system["worker"])["items"][0]
    canceled = service.cancel_work(
        system["cao"],
        work["id"],
        "The obsolete Work is terminal.",
        idempotency_key="cancel:terminal-dead",
    )
    assert canceled["state"] == "canceled"
    # Reproduce the retained terminal notification from INC-042. Cancellation
    # itself need not synthesize a dead outcome for an otherwise queued packet.
    service.db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dead', last_error = 'terminal-history-fixture'
        WHERE message_id = ? AND recipient_id = ?
        """,
        (assignment["id"], system["worker"]["id"]),
    )
    dead = next(
        item
        for item in service.get_inbox(system["worker"])["items"]
        if item["id"] == assignment["id"]
    )
    assert dead["delivery_state"] == "dead"

    acknowledged = service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    assert acknowledged["acknowledged"] == [assignment["id"]]
    delivery = service.db.fetchone(
        """
        SELECT state, acknowledged_at, handled_at
        FROM message_deliveries
        WHERE message_id = ? AND recipient_id = ?
        """,
        (assignment["id"], system["worker"]["id"]),
    )
    assert delivery is not None
    assert delivery["state"] == "handled"
    assert delivery["acknowledged_at"]
    assert delivery["handled_at"]
    assert assignment["id"] not in {
        item["id"] for item in service.get_inbox(system["worker"])["items"]
    }

    audit_count = service.db.fetchone(
        """
        SELECT COUNT(*) AS count
        FROM events
        WHERE event_type = 'message.dead_acknowledged'
          AND aggregate_type = 'message' AND aggregate_id = ?
        """,
        (assignment["id"],),
    )
    assert audit_count is not None and int(audit_count["count"]) == 1
    replay = service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    assert replay["acknowledged"] == [assignment["id"]]
    replay_audit_count = service.db.fetchone(
        """
        SELECT COUNT(*) AS count
        FROM events
        WHERE event_type = 'message.dead_acknowledged'
          AND aggregate_type = 'message' AND aggregate_id = ?
        """,
        (assignment["id"],),
    )
    assert replay_audit_count is not None
    assert int(replay_audit_count["count"]) == 1


def test_nonterminal_dead_delivery_remains_visible_and_unacknowledgeable(system) -> None:
    service = system["service"]
    work = _assign_direct(
        system,
        completion_contract=CompletionContract.LEGACY_UNCLASSIFIED,
        key="active-dead",
    )
    assignment = service.get_inbox(system["worker"])["items"][0]
    service.db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dead', last_error = 'synthetic-active-dead'
        WHERE message_id = ? AND recipient_id = ?
        """,
        (assignment["id"], system["worker"]["id"]),
    )

    active = service.get_work(work["id"])
    assert active["state"] not in {"completed", "canceled", "failed"}
    visible = service.get_inbox(system["worker"])["items"]
    assert any(
        item["id"] == assignment["id"] and item["delivery_state"] == "dead" for item in visible
    )
    with pytest.raises(ConflictError):
        service.acknowledge(system["worker"], AckInput(message_ids=[assignment["id"]]))
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (assignment["id"], system["worker"]["id"]),
    )
    assert delivery is not None and delivery["state"] == "dead"
    assert (
        service.db.fetchone(
            """
        SELECT COUNT(*) AS count FROM events
        WHERE event_type = 'message.dead_acknowledged' AND aggregate_id = ?
        """,
            (assignment["id"],),
        )["count"]
        == 0
    )
