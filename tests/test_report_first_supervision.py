from __future__ import annotations

import base64
import hashlib
import json

import pytest
from pydantic import ValidationError as ModelValidationError
from test_verified_artifact_content_read import _attached_cao, _read_arguments, _reported_artifact
from test_worker_output_delivery import _capture_fixture, _event, _read_request, _terminal

from cao_control_plane.errors import ConflictError, ValidationError
from cao_control_plane.mcp import WORKER_TOOLS, MCPServer
from cao_control_plane.models import (
    ArtifactAuditScope,
    ArtifactContentReadInput,
    ReportInput,
    ReportKind,
    ReviewInput,
    WorkerDecisionReport,
)


def _report(**changes):
    value = {
        "outcome": "The bounded result is complete; the tested candidate was rejected.",
        "criterion_assessments": [{"criterion_id": "task_1", "verdict": "met",
                                   "evidence": "The requested comparison was completed."}],
        "verification": [{"check": "Compare the retained result with the fixed rule.",
                          "result": "passed", "evidence": "The comparison produced a rejection."}],
        "limitations": ["No independent audit has been performed."],
        "recommendation": "Retain the reference and move to the next authorized outcome.",
        "decision_needed": "Accept the completed comparison, without adopting the candidate.",
        "record_artifact_ids": [],
    }
    value.update(changes)
    return WorkerDecisionReport.model_validate(value)


def _structured_claim(system, actor, work, report):
    attempt = work["current_attempt"]
    return system["service"].report(system["worker"], attempt["id"], ReportInput(
        kind="completion_claim", expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"], summary="The finite comparison is complete.",
        decision_report=report, idempotency_key="structured-completion",
    ))


def _registered(system):
    attachment, actor = _attached_cao(system, thread_id="report-review", project_marker="b")
    work, artifact = _reported_artifact(
        system, attachment, content=b"First fact. Second fact. Detailed retained execution record.",
        media_type="text/plain", key="report-record", report_kind=ReportKind.ARTIFACT,
    )
    return actor, work, artifact


@pytest.mark.parametrize("legacy_cache", [False, True])
def test_worker_artifact_receipt_identifies_this_batch_before_completion(system, legacy_cache):
    _actor, work, prior = _registered(system)
    attempt = work["current_attempt"]
    content = b"A separately registered retained result."
    digest = hashlib.sha256(content).hexdigest()
    server = MCPServer(system["service"])
    arguments = {
        "attempt_id": attempt["id"], "kind": "artifact",
        "expected_goal_version": work["goal_version"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
        "expected_generation": work["generation"], "summary": "Retained result registered.",
        "artifacts": [{"name": "retained result", "uri": "data:text/plain;base64,"+base64.b64encode(content).decode(),
                       "media_type": "text/plain", "digest": digest}],
        "idempotency_key": "receipt-artifact",
    }
    result = server.call_tool(system["worker"], "cao_report", arguments)
    receipt = result["report_receipt"]
    [registered] = receipt["artifacts"]
    assert registered == {"id": registered["id"], "name": "retained result", "digest": digest, "verified": True}
    assert registered["id"] != prior["id"]
    assert receipt["artifact_ids"] == [registered["id"]]
    assert receipt["attempt_id"] == attempt["id"] and receipt["work_item_id"] == work["id"]
    assert receipt["completion_manifest_state"] == "not_finalized"
    assert not result["current_attempt"]["completion_claim"]
    assert {"goal_history", "attempts", "messages", "artifacts"}.isdisjoint(result)
    if legacy_cache:
        row = system["service"].db.fetchone("SELECT result_json FROM idempotency_results WHERE operation='report' AND idempotency_key='receipt-artifact'")
        old = json.loads(row["result_json"])
        old.pop("report_receipt")
        system["service"].db.execute("UPDATE idempotency_results SET result_json=? WHERE operation='report' AND idempotency_key='receipt-artifact'", (json.dumps(old),))
    before = system["service"].db.fetchone("SELECT COUNT(*) FROM events")[0]
    assert server.call_tool(system["worker"], "cao_report", arguments) == result
    assert system["service"].db.fetchone("SELECT COUNT(*) FROM events")[0] == before
    assert len(system["service"].get_work(work["id"])["artifacts"]) == 2

    completed = server.call_tool(system["worker"], "cao_report", {
        **{key: value for key, value in arguments.items() if key != "artifacts"},
        "kind": "completion_claim", "summary": "The bounded result is complete.",
        "idempotency_key": "receipt-completion",
    })
    final = completed["report_receipt"]
    assert final["completion_manifest_state"] == "finalized"
    assert set(final["completion_artifact_ids"]) == {prior["id"], registered["id"]}
    assert final["artifact_ids"] == []


def test_unverified_artifact_receipt_never_claims_content_registration(system):
    _actor, work, _prior = _registered(system)
    attempt = work["current_attempt"]
    result = MCPServer(system["service"]).call_tool(system["worker"], "cao_report", {
        "attempt_id": attempt["id"], "kind": "artifact",
        "expected_goal_version": work["goal_version"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
        "expected_generation": work["generation"], "summary": "Unverified reference retained.",
        "artifacts": [{"name": "remote reference", "uri": "https://example.test/unverified"}],
        "idempotency_key": "receipt-unverified",
    })
    [artifact] = result["report_receipt"]["artifacts"]
    assert artifact["verified"] is False and artifact["digest"] == ""
    assert result["report_receipt"]["completion_manifest_state"] == "not_finalized"


def test_structured_report_is_reviewable_without_raw_record_reads(system):
    actor, work, artifact = _registered(system)
    current = _structured_claim(system, actor, work, _report(record_artifact_ids=[artifact["id"]]))
    claim = current["current_attempt"]["completion_claim"]
    assert claim["decision_report"]["outcome"].startswith("The bounded result is complete")
    assert claim["report_checks"]["criteria_coverage"] == "matched"
    assert claim["report_checks"]["verification_provenance"] == "worker_reported"
    projected = MCPServer(system["service"]).call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})
    assert projected["current_attempt"]["completion_claim"]["decision_report"] == claim["decision_report"]
    review = system["service"].review(actor, ReviewInput(
        attempt_id=work["current_attempt"]["id"], verdict="ok",
        summary="The report supports acceptance of the finite comparison.",
    ))
    basis = next(item for item in review["reviews"][-1]["evidence"] if item.get("kind") == "review_basis")
    assert basis["basis"] == "report" and basis["audit_scopes"] == []
    assert basis["report_format"] == "structured"
    assert system["service"].db.fetchone(
        "SELECT COUNT(*) AS n FROM events WHERE event_type='artifact.content_read'"
    )["n"] == 0
    assert system["service"].get_work(work["id"])["state"] != "completed"


@pytest.mark.parametrize("assessments", [
    [{"criterion_id": "task_2", "verdict": "met", "evidence": "Wrong criterion."}],
    [{"criterion_id": "task_1", "verdict": "met", "evidence": "Duplicated."}] * 2,
    [{"criterion_id": "task_1", "verdict": "unverified", "evidence": "Not checked."}],
    [{"criterion_id": "task_1", "verdict": "unmet", "evidence": "Unfinished."}],
])
def test_incomplete_or_substituted_report_never_creates_a_completion_boundary(system, assessments):
    actor, work, _artifact = _registered(system)
    with pytest.raises(ConflictError):
        _structured_claim(system, actor, work, _report(criterion_assessments=assessments))
    current = system["service"].get_work(work["id"])
    assert current["state"] == "active"
    assert current["open_boundaries"] == []
    assert not current["current_attempt"]["completion_claim"]


def test_report_record_reference_must_bind_to_the_exact_attempt(system):
    actor, work, _artifact = _registered(system)
    with pytest.raises(ConflictError, match="exact Attempt"):
        _structured_claim(system, actor, work, _report(record_artifact_ids=["art_" + "f" * 32]))


def test_worker_receives_frozen_criterion_ids_and_honest_unfinished_progress_is_silent(system):
    actor, work, _artifact = _registered(system)
    context = system["service"].get_worker_context(system["worker"], work["current_attempt"]["id"])
    assert [item["id"] for item in context["work"]["review_criteria"]] == ["task_1"]
    attempt = work["current_attempt"]
    result = system["service"].report(system["worker"], attempt["id"], ReportInput(
        kind="progress", expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"], summary="Comparison remains unfinished.",
        decision_report=_report(criterion_assessments=[{
            "criterion_id": "task_1", "verdict": "unmet", "evidence": "Still checking the result."
        }]), idempotency_key="honest-progress",
    ))
    assert result["attention_owner"] == "worker" and result["open_boundaries"] == []
    assert system["service"].get_inbox(actor)["items"] == []


def test_old_attachment_read_cannot_authorize_a_new_generation_audit(system):
    actor, work, artifact = _registered(system)
    _structured_claim(system, actor, work, _report())
    system["service"].read_artifact_content(actor, ArtifactContentReadInput(
        **{**_read_arguments(work, artifact, idempotency_key="old-audit"), "max_bytes": 4}
    ))
    # A renewed connection at the same generation is deliberately valid. A new
    # attachment generation cannot borrow that old read receipt.
    changed = {**actor, "_cao_attachment_generation": actor["_cao_attachment_generation"] + 1}
    from cao_control_plane.decision_reports import review_basis_tx

    with system["service"].db.transaction() as connection:
        attempt = connection.execute("SELECT * FROM attempts WHERE id = ?", (work["current_attempt"]["id"],)).fetchone()
        with pytest.raises(ConflictError, match="exceeds"):
            review_basis_tx(system["service"], connection, actor=changed, work=work, attempt=attempt,
                            request=ReviewInput(attempt_id=attempt["id"], verdict="ok", summary="Old read.",
                                                review_basis="artifact_audit", audit_reason="Check one fact.",
                                                audit_scopes=[ArtifactAuditScope(artifact_id=artifact["id"], byte_count=4)]))


def test_audit_records_only_the_range_actually_read(system):
    actor, work, artifact = _registered(system)
    _structured_claim(system, actor, work, _report(record_artifact_ids=[artifact["id"]]))
    system["service"].read_artifact_content(actor, ArtifactContentReadInput(
        **{**_read_arguments(work, artifact, idempotency_key="targeted-audit"), "max_bytes": 11}
    ))
    review = system["service"].review(actor, ReviewInput(
        attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Resolved a specific question.",
        review_basis="artifact_audit", audit_reason="Resolve a disputed first fact.",
        audit_scopes=[ArtifactAuditScope(artifact_id=artifact["id"], byte_offset=0, byte_count=11)],
    ))
    basis = next(item for item in review["reviews"][-1]["evidence"] if item.get("kind") == "review_basis")
    assert basis["basis"] == "artifact_audit"
    assert basis["audit_scopes"][0]["byte_count"] == 11
    assert basis["audit_scopes"][0]["read_event_sequences"]


def test_audit_cannot_claim_unread_bytes(system):
    actor, work, artifact = _registered(system)
    _structured_claim(system, actor, work, _report())
    system["service"].read_artifact_content(actor, ArtifactContentReadInput(
        **{**_read_arguments(work, artifact, idempotency_key="partial-audit"), "max_bytes": 4}
    ))
    with pytest.raises(ConflictError, match="exceeds"):
        system["service"].review(actor, ReviewInput(
            attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Overstated audit.",
            review_basis="artifact_audit", audit_reason="Check a disputed fact.",
            audit_scopes=[ArtifactAuditScope(artifact_id=artifact["id"], byte_count=20)],
        ))
    assert system["service"].get_work(work["id"])["reviews"] == []


def test_caller_cannot_forge_server_review_basis(system):
    actor, work, _artifact = _registered(system)
    _structured_claim(system, actor, work, _report())
    with pytest.raises(ValidationError, match="derived"):
        system["service"].review(actor, ReviewInput(
            attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Forged audit.",
            evidence=[{"kind": "review_basis", "basis": "artifact_audit"}],
        ))


def test_free_text_final_report_keeps_exact_report_read_but_needs_no_raw_audit(system):
    actor, work, binding = _capture_fixture(system)
    output = system["service"].observe_worker_output(**binding, event=_event())
    _terminal(system["service"], binding)
    system["service"].read_worker_output(actor, _read_request(work, output))
    review = system["service"].review(actor, ReviewInput(
        attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Read the submitted report.",
    ))
    basis = next(item for item in review["reviews"][-1]["evidence"] if item.get("kind") == "review_basis")
    assert basis["basis"] == "report" and basis["report_format"] == "free_text"


def test_audit_requires_a_reason_and_scope_and_default_request_is_compatible():
    with pytest.raises(ModelValidationError):
        ReviewInput(attempt_id="att_test", verdict="ok", summary="Audit.", review_basis="artifact_audit")
    request = ReviewInput(attempt_id="att_test", verdict="ok", summary="Report.")
    assert not {"review_basis", "audit_reason", "audit_scopes"} & request.model_dump(mode="json").keys()
    assert "decision_report" in WORKER_TOOLS["cao_report"]["inputSchema"]["properties"]
    assert json.loads(json.dumps(_report().model_dump(mode="json")))["decision_needed"]
