from __future__ import annotations

import json

import pytest
from conftest import business_purpose_contract
from pydantic import ValidationError as PydanticValidationError
from test_protocol_conformance import _modern_request
from test_worker_business_purpose import _purpose, _reported, _worker

from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import ReviewInput, SetWorkerPurposeInput


@pytest.mark.parametrize("field", ["outcome", "scope", "criteria", "completion", "authority", "unmet_action"])
def test_missing_purpose_condition_is_rejected_before_mutation(system, field):
    service, cao, ids = _worker(system)
    contract = business_purpose_contract()
    contract.pop(field)
    response = MCPServer(service).handle_modern(cao, _modern_request("tools/call", {
        "name": "cao_set_worker_purpose", "arguments": {
            "worker_thread_id": ids["thread_id"], "purpose": "Improve service quality.",
            "reason": "The requester assigned this responsibility.", "contract": contract,
            "idempotency_key": "incomplete-purpose",
        },
    }))
    error = response["result"]["structuredContent"]["error"]
    assert error["code"] == "invalid_tool_arguments"
    assert any(field in item.get("missing_fields", []) for item in error["details"]["violations"])
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM worker_purpose_revisions")["n"] == 0


def test_purpose_cannot_reuse_criterion_id_or_omit_evidence():
    contract = business_purpose_contract()
    contract["criteria"] *= 2
    with pytest.raises(PydanticValidationError, match="unique"):
        SetWorkerPurposeInput(worker_thread_id="mwt_fixture", purpose="Reliable service.",
                              reason="Assigned responsibility.", contract=contract, idempotency_key="duplicate")
    contract = business_purpose_contract()
    contract["criteria"][0]["evidence"] = "   "
    with pytest.raises(PydanticValidationError):
        SetWorkerPurposeInput(worker_thread_id="mwt_fixture", purpose="Reliable service.",
                              reason="Assigned responsibility.", contract=contract, idempotency_key="no-evidence")


@pytest.mark.parametrize("assessments,verdict,reason", [
    ([], "ok", "review_criteria_mismatch"),
    ([{"criterion_id": "task_2", "verdict": "unmet", "evidence": "A new preference."}], "needs_work", "review_criteria_mismatch"),
    ([{"criterion_id": "task_1", "verdict": "unverified", "evidence": "Not read yet."}], "ok", "review_criteria_verdict_conflict"),
    ([{"criterion_id": "task_1", "verdict": "met", "evidence": "Verified."}], "needs_work", "review_criteria_verdict_conflict"),
])
def test_review_cannot_move_or_ignore_the_frozen_acceptance(system, assessments, verdict, reason):
    service, cao, _ids, _worker_actor, work = _reported(system)
    frozen = work["current_attempt"]["goal_packet_digest"]
    with pytest.raises(ConflictError) as raised:
        service.review(cao, ReviewInput(attempt_id=work["current_attempt"]["id"],
            verdict=verdict, summary="Assess the submitted checkpoint.", expected_purpose_version=1,
            criterion_assessments=assessments))
    assert raised.value.details["reason_code"] == reason
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM reviews")["n"] == 0
    assert service.get_work(work["id"], cao)["current_attempt"]["goal_packet_digest"] == frozen


def test_purpose_change_keeps_old_task_judgment_and_records_its_basis(system):
    service, cao, ids, _worker_actor, work = _reported(system)
    original = work["assignment_purpose"]
    _purpose(service, cao, ids, text="Maintain reliability in another region.", version=1)
    current = service.get_work(work["id"], cao)
    assert current["purpose_alignment"]["status"] == "changed"
    assert current["assignment_purpose"] == original
    values = dict(attempt_id=current["current_attempt"]["id"], verdict="ok",
        summary="The bounded measurement is verified; the continuing purpose remains.",
        expected_purpose_version=2,
        criterion_assessments=[{"criterion_id": "task_1", "verdict": "met", "evidence": "The recorded measurement was inspected."}])
    with pytest.raises(ConflictError) as raised:
        service.review(cao, ReviewInput(**values))
    assert raised.value.details["reason_code"] == "review_assignment_purpose_confirmation_required"
    reviewed = service.review(cao, ReviewInput(**values, expected_assignment_purpose_version=1))
    assert reviewed["reviews"][-1]["verdict"] == "ok"
    evidence = reviewed["reviews"][-1]["evidence"][-1]
    assert evidence["criterion_id"] == "task_1"
    assert evidence["assignment_purpose_version"] == 1
    assert evidence["goal_packet_digest"] == work["current_attempt"]["goal_packet_digest"]
    assert reviewed["worker_purpose"]["version"] == 2
    assert "contract" in json.loads(service.db.fetchone(
        "SELECT packet_json FROM worker_purpose_revisions WHERE worker_thread_id=? AND version=2",
        (ids["thread_id"],),
    )["packet_json"])
