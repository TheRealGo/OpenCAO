from __future__ import annotations

import json
import threading

import pytest
from test_worker_business_purpose import _instruction, _purpose, _reported, _turn, _worker
from test_worker_thread_lifecycle_core import _connect_seeded_managed_runtime

from cao_control_plane.errors import ConflictError
from cao_control_plane.instruction_review import CHECKS, ReviewDecision, ReviewOutcome
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    GoalRevision,
    ReportInput,
    ReviewInput,
    StatusRequestInput,
    WorkerThreadLifecycleInput,
)


def outcome(verdict="pass", *, reason="Source-grounded outcome."):
    values = [
        {
            "check": check,
            "verdict": "met",
            "reason": reason,
            "evidence_refs": ["requester_context", "purpose", "candidate", "history"],
        }
        for check in CHECKS
    ]
    if verdict != "pass":
        values[0]["verdict"] = "unmet" if verdict == "reject" else "unverified"
    return ReviewOutcome(
        ReviewDecision.model_validate(
            {"verdict": verdict, "summary": reason, "assessments": values}
        ),
        "independent-test-judge",
        17,
        {"input_tokens": 100},
    )


def counts(service):
    return {
        table: service.db.fetchone(f"SELECT COUNT(*) AS n FROM {table}")["n"]
        for table in (
            "work_items",
            "attempts",
            "messages",
            "message_deliveries",
            "worker_purpose_revisions",
        )
    }


def prepared(system):
    service, cao, ids = _worker(system)
    _purpose(service, cao, ids)
    return service, cao, ids


def test_rejection_cannot_create_work_or_delivery_and_returns_exact_findings(system, monkeypatch):
    service, cao, ids = prepared(system)
    before = counts(service)
    monkeypatch.setattr(
        service.instruction_reviewer,
        "review",
        lambda _: outcome("reject", reason="The requested report was already verified; reuse it."),
    )
    with pytest.raises(ConflictError) as raised:
        _instruction(
            service, cao, ids, "Repeat the already verified report without a changed input."
        )
    assert counts(service) == before
    assert (
        raised.value.details["assessment"]["summary"]
        == "The requested report was already verified; reuse it."
    )
    assert (
        service.db.fetchone("SELECT COUNT(*) AS n FROM instruction_reviews WHERE verdict='reject'")[
            "n"
        ]
        == 1
    )


def test_approval_is_private_exact_and_idempotent_replay_does_not_rejudge(system, monkeypatch):
    service, cao, ids = prepared(system)
    snapshots = []
    monkeypatch.setattr(
        service.instruction_reviewer, "review", lambda data: snapshots.append(data) or outcome()
    )
    first = _instruction(service, cao, ids, "Inspect the unmet prerequisite before implementation.")
    assert len(snapshots) == 1
    assert not {"original_request_context", "requester_context", "history"} & snapshots[0].keys()
    assert snapshots[0]["purpose"]
    receipt = service.db.fetchone(
        "SELECT * FROM instruction_reviews WHERE model='independent-test-judge'"
    )
    assert receipt["consumed"] == 1
    before = counts(service)
    again = _instruction(service, cao, ids, "Inspect the unmet prerequisite before implementation.")
    assert again["id"] == first["id"] and counts(service) == before and len(snapshots) == 1
    payload = service.db.fetchone(
        "SELECT payload_json FROM messages WHERE work_item_id=? AND kind='assignment'",
        (first["id"],),
    )["payload_json"]
    assert "requester_context" not in payload and "independent-test-judge" not in payload


def test_judge_runs_without_sqlite_writer_lock(system, monkeypatch):
    service, cao, ids = prepared(system)
    errors = []

    def judge(_):
        def concurrent_write():
            try:
                service.db.execute(
                    "INSERT INTO metadata(key,value) VALUES('review-concurrent-write','ok')"
                )
            except BaseException as error:
                errors.append(error)

        writer = threading.Thread(target=concurrent_write)
        writer.start()
        writer.join(timeout=3)
        assert not writer.is_alive(), "the model call held the domain writer lock"
        return outcome()

    monkeypatch.setattr(service.instruction_reviewer, "review", judge)
    _instruction(service, cao, ids, "Complete the next bounded outcome.")
    assert not errors


def test_failed_review_retains_safe_diagnostics_model_and_elapsed_without_delivery(system, monkeypatch):
    from cao_control_plane.instruction_review import ReviewUnavailable

    service, cao, ids = prepared(system)
    before = counts(service)
    private = "private source /home/owner/workspace provider credential"
    failure = ReviewUnavailable("review_process_failed", diagnostics={
        "exit_code": 1, "event_count": 3, "last_event": "turn.failed",
        "native_failure_code": private, "stderr": private, "event": {"message": private},
    })
    failure.model, failure.elapsed_ms = "configured-review-model", 123

    def unavailable(_snapshot):
        raise failure

    monkeypatch.setattr(service.instruction_reviewer, "review", unavailable)
    with pytest.raises(ConflictError) as raised:
        _instruction(service, cao, ids, "Complete the next bounded outcome.")
    assert raised.value.details["failure_code"] == "review_process_failed"
    assert counts(service) == before
    receipt = service.db.fetchone("SELECT * FROM instruction_reviews WHERE id=?", (raised.value.details["review_id"],))
    assert receipt["verdict"] == "unavailable" and not receipt["consumed"]
    assert receipt["model"] == "configured-review-model" and receipt["elapsed_ms"] == 123
    assert json.loads(receipt["result_json"]) == {
        "failure_code": "review_process_failed",
        "diagnostics": {"exit_code": 1, "event_count": 3, "last_event": "turn.failed"},
    }
    assert private not in receipt["result_json"]


@pytest.mark.parametrize("failure", ["timeout", "bad_output"])
def test_unavailable_or_changed_review_evidence_fails_before_delivery(system, monkeypatch, failure):
    service, cao, ids = prepared(system)
    before = counts(service)
    calls = 0

    def source(_):
        nonlocal calls
        calls += 1
        return (
            []
            if failure == "no_source"
            else [
                {
                    "role": "requester",
                    "text": "Current direction",
                    "revision": calls if failure == "source_change" else 1,
                }
            ]
        )

    def judge(_):
        if failure == "timeout":
            raise TimeoutError()
        if failure == "bad_output":
            raise ValueError("invalid judge output")
        return outcome()

    monkeypatch.setattr(service, "instruction_review_source", source)
    monkeypatch.setattr(service.instruction_reviewer, "review", judge)
    with pytest.raises(ConflictError) as raised:
        _instruction(service, cao, ids, "Proceed toward the current outcome.")
    assert raised.value.details["reason_code"] == "instruction_review_unavailable"
    assert counts(service) == before


def test_queued_revision_uses_same_gate(system, monkeypatch):
    service, cao, ids = prepared(system)
    work = _instruction(service, cao, ids, "Complete the required implementation.")
    monkeypatch.setattr(service.instruction_reviewer, "review", lambda _: outcome("reject"))
    before = counts(service)
    with pytest.raises(ConflictError) as raised:
        service.revise_goal(
            cao,
            work["id"],
            GoalRevision(
                expected_version=1,
                expected_purpose_version=1,
                maturity="exploring",
                instruction="Replace the request with advice.",
                reason="A proposed change.",
                idempotency_key="review-revision",
            ),
        )
    assert raised.value.details["reason_code"] == "instruction_review_reject"
    assert counts(service) == before


@pytest.mark.parametrize("route", ["reply", "boundary"])
def test_reply_and_boundary_correction_use_same_gate(system, monkeypatch, route):
    service, cao, _ids, _worker_actor, work = _reported(system, kind="question")
    turn = _turn(service, cao, work) if route == "boundary" else None
    monkeypatch.setattr(service.instruction_reviewer, "review", lambda _: outcome("reject"))
    before = counts(service)
    with pytest.raises(ConflictError) as raised:
        if route == "reply":
            service.reply(cao, work["id"], "Repeat completed work.", expected_purpose_version=1)
        else:
            service.dispose_boundary(
                cao,
                work["open_boundaries"][0]["id"],
                BoundaryDispositionInput(
                    turn_id=turn["id"],
                    lease_token=turn["lease_token"],
                    expected_generation=work["generation"],
                    expected_purpose_version=1,
                    kind="correct",
                    instruction="Repeat completed work.",
                    reason="A proposed correction.",
                ),
            )
    assert raised.value.details.get("reason_code") == "instruction_review_reject", (
        str(raised.value),
        raised.value.details,
    )
    assert counts(service) == before


def test_custom_status_text_cannot_carry_an_unreviewed_instruction(system, monkeypatch):
    service, cao, ids = prepared(system)
    _connect_seeded_managed_runtime(service, ids)
    work = _instruction(service, cao, ids, "Measure service quality.")
    monkeypatch.setattr(service.instruction_reviewer, "review", lambda _: outcome("reject"))
    before = counts(service)
    with pytest.raises(ConflictError) as raised:
        service.request_status(
            cao,
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="Discard the accepted work and redo it.",
                idempotency_key="status-review",
            ),
        )
    assert raised.value.details["reason_code"] == "instruction_review_reject"
    assert counts(service) == before


def test_review_failure_does_not_block_listing_finish_or_delete(system, monkeypatch):
    service, cao, ids = prepared(system)
    _instruction(service, cao, ids, "Perform a bounded checkpoint.")
    monkeypatch.setattr(
        service.instruction_reviewer, "review", lambda _: (_ for _ in ()).throw(TimeoutError())
    )
    assert service.list_managed_workers(cao)
    finished = service.finish_worker_thread(
        cao,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"], idempotency_key="finish-without-review"
        ),
    )
    assert finished["thread_state"] == "archived"
    deleted = service.delete_worker_thread(
        cao,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"], idempotency_key="delete-without-review"
        ),
    )
    assert deleted["thread_state"] == "deleted"


def test_requester_source_and_judge_diagnostics_never_persist_private_paths(system, monkeypatch):
    service, cao, ids = prepared(system)
    monkeypatch.setattr(
        service,
        "instruction_review_source",
        lambda _: [{"role": "requester", "text": "Use /home/owner/input.txt as source."}],
    )
    monkeypatch.setattr(
        service.instruction_reviewer,
        "review",
        lambda snapshot: outcome(
            "needs_evidence", reason="Read /home/owner/other.txt first."
        ),
    )
    with pytest.raises(ConflictError) as raised:
        _instruction(service, cao, ids, "Complete the requested local result.")
    row = service.db.fetchone(
        "SELECT snapshot_json, result_json FROM instruction_reviews WHERE model='independent-test-judge'"
    )
    assert "/home/owner" not in json.dumps(dict(row))
    assert "/home/owner" not in json.dumps(raised.value.details)


def test_judge_cannot_pass_with_missing_checks_or_no_requester_evidence():
    decision = outcome().decision
    decision.assessments[-1] = decision.assessments[0]
    with pytest.raises(ValueError):
        decision.validate_consistency()
    decision = outcome().decision
    decision.assessments[0].evidence_refs = ["purpose", "candidate"]
    with pytest.raises(ValueError):
        decision.validate_consistency()


def test_legacy_registered_purpose_is_retained_without_recreating_original_dialogue(system, monkeypatch):
    service, cao, ids = prepared(system)
    purpose = service.list_managed_workers(cao)[0]["worker_purpose"]
    # A pre-review purpose has its normal sealed contract but no review snapshot.
    service.db.execute("DELETE FROM instruction_reviews")
    snapshots = []
    monkeypatch.setattr(service, "instruction_review_source", lambda _: [
        {"role": "requester", "text": "Continue the unfinished work toward the existing purpose."}
    ])
    monkeypatch.setattr(service.instruction_reviewer, "review", lambda snapshot: snapshots.append(snapshot) or outcome())
    work = _instruction(service, cao, ids, "Measure the unmet reliability criterion.")
    assert work["worker_purpose"] == purpose
    assert snapshots[0]["purpose"] == purpose
    assert not {"original_request_context", "requester_context", "history"} & snapshots[0].keys()


def test_public_instruction_input_cannot_supply_its_own_review_verdict():
    from pydantic import ValidationError

    from cao_control_plane.models import InstructWorkerThreadInput

    with pytest.raises(ValidationError):
        InstructWorkerThreadInput(
            worker_thread_id="mwt_fixture",
            instruction="Do the requested work.",
            expected_purpose_version=1,
            idempotency_key="forged-verdict",
            review_verdict="pass",
        )


def test_common_message_fence_rejects_an_unreviewed_internal_send(system):
    from cao_control_plane.models import MessageKind

    service, cao, ids = prepared(system)
    work = _instruction(service, cao, ids, "Perform the accepted bounded checkpoint.")
    before = counts(service)
    with pytest.raises(ConflictError) as raised, service.db.transaction() as connection:
        service._message(
            connection,
            sender_id=cao["id"],
            recipient_id=ids["principal_id"],
            kind=MessageKind.INSTRUCTION,
            payload={"instruction": "An unreviewed addition."},
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            goal_version=1,
        )
    assert raised.value.details["reason_code"] == "instruction_review_required"
    assert counts(service) == before


@pytest.mark.parametrize("verdict", ["reject", "needs_evidence"])
def test_mcp_preserves_bounded_review_findings_for_the_next_cao_decision(
    system, monkeypatch, verdict
):
    from test_protocol_conformance import _modern_request

    from cao_control_plane.mcp import MCPServer

    service, cao, ids = prepared(system)
    monkeypatch.setattr(
        service.instruction_reviewer,
        "review",
        lambda _: outcome(
            verdict, reason="The implementation outcome has been replaced by advice."
        ),
    )
    before = counts(service)
    response = MCPServer(service).handle_modern(
        cao,
        _modern_request(
            "tools/call",
            {
                "name": "cao_instruct_worker_thread",
                "arguments": {
                    "worker_thread_id": ids["thread_id"],
                    "expected_purpose_version": 1,
                    "instruction": "Only offer advice and stop.",
                    "idempotency_key": "public-review-error",
                },
            },
        ),
    )
    assert response["result"]["isError"] is True
    details = response["result"]["structuredContent"]["error"]["details"]
    assert details["reason_code"] == "instruction_review_" + verdict
    assert (
        details["assessment"]["summary"]
        == "The implementation outcome has been replaced by advice."
    )
    assert details["retryable"] is False
    assert counts(service) == before


def test_registered_legacy_direction_does_not_require_a_new_human_message(system, monkeypatch):
    service, cao, ids = prepared(system)
    service.db.execute('DELETE FROM instruction_reviews')
    monkeypatch.setattr(service, 'instruction_review_source', lambda _: [
        {'role': 'coverage', 'native_head': {'turn_digest': 'current-native-head', 'requester_digests': []}}
    ])
    snapshots = []
    monkeypatch.setattr(service.instruction_reviewer, 'review', lambda s: snapshots.append(s) or outcome())
    assert _instruction(service, cao, ids, 'Advance the unmet registered criterion.')['id']
    assert snapshots[0]['purpose']['version'] == 1
    assert 'requester_context' not in snapshots[0]




def test_purpose_registration_and_revision_are_cao_decisions_without_ai_review(system, monkeypatch):
    service, cao, ids = _worker(system)
    def unexpected(_):
        raise AssertionError("Purpose definition must not invoke an instruction checker")
    monkeypatch.setattr(service.instruction_reviewer, "review", unexpected)
    _purpose(service, cao, ids)
    _purpose(service, cao, ids, text="Maintain reliability in the assigned region.", version=1)
    assert service.list_managed_workers(cao)[0]["worker_purpose"]["version"] == 2
    assert service.db.fetchone("SELECT count(*) FROM instruction_reviews")[0] == 0


def test_alignment_never_reads_native_conversation_or_previous_artifact_contents(system, monkeypatch):
    service, cao, ids = prepared(system)
    def unexpected(*_args, **_kwargs):
        raise AssertionError("Alignment must use the registered purpose and instruction only")
    monkeypatch.setattr(service, "instruction_review_source", unexpected)
    monkeypatch.setattr(service.owner_private_artifact_preservation, "read_verified_text_chunk", unexpected)
    snapshots = []
    monkeypatch.setattr(service.instruction_reviewer, "review", lambda s: snapshots.append(s) or outcome())
    assert _instruction(service, cao, ids, "Inspect the next unmet prerequisite.")["id"]
    assert snapshots[0]["purpose"]["version"] == 1
    assert not {"requester_context", "original_request_context", "history", "reviewed_source"} & snapshots[0].keys()


def test_prior_result_changes_do_not_rejudge_the_same_purpose_and_instruction(system, monkeypatch):
    service, cao, ids = prepared(system)
    old = _instruction(service, cao, ids, "Measure one prerequisite.")
    seen = []
    def judge(snapshot):
        seen.append(snapshot)
        service.db.execute("UPDATE work_items SET state='completed' WHERE id=?", (old["id"],))
        return outcome()
    monkeypatch.setattr(service.instruction_reviewer, "review", judge)
    assert _instruction(service, cao, ids, "Inspect another unmet prerequisite.")["id"]
    assert len(seen) == 1


def test_new_alignment_decision_claims_only_purpose_alignment():
    from cao_control_plane.instruction_review import AlignmentDecision
    decision = AlignmentDecision(verdict="pass", summary="The instruction stays within the purpose.").stored_decision()
    decision.validate_consistency()
    assert decision.scope == "purpose_alignment"
    assert len(decision.assessments) == 1
    assert decision.assessments[0].check == "purpose_alignment"
    assert decision.assessments[0].evidence_refs == ["purpose", "candidate"]


def test_evidence_correction_retains_task_for_review_and_reaches_a_new_report(system, monkeypatch):
    service, cao, _ids, worker, work = _reported(system)
    original_instruction = work["instruction"]
    purpose = work["worker_purpose"]
    service.review(cao, ReviewInput(
        attempt_id=work["current_attempt"]["id"], expected_purpose_version=1,
        verdict="needs_work", summary="The source of the measurement is missing.",
        criterion_assessments=[{"criterion_id": "task_1", "verdict": "unverified",
            "evidence": "The reported value lacks its execution source."}],
        idempotency_key="missing-execution-source",
    ))
    correction = (
        "Identify the existing measurement source and resubmit the evidence. "
        "Do not repeat the measurement merely to explain it."
    )
    snapshots = []

    def judge(snapshot):
        snapshots.append(snapshot)
        assert snapshot["purpose"] == purpose
        assert snapshot["candidate"]["task"]["instruction"] == original_instruction
        assert snapshot["candidate"]["message"]["instruction"] == correction
        return outcome()

    monkeypatch.setattr(service.instruction_reviewer, "review", judge)
    before = counts(service)
    turn = _turn(service, cao, work)
    service.dispose_boundary(cao, work["open_boundaries"][0]["id"], BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"],
        expected_generation=work["generation"], expected_purpose_version=1,
        kind="correct", reason="Resolve the one missing fixed criterion.", instruction=correction,
    ))
    current = service.get_work(work["id"], cao)
    assert len(snapshots) == 1
    assert counts(service)["work_items"] == before["work_items"]
    assert current["instruction"] == original_instruction
    assert current["worker_purpose"] == purpose
    assert current["state"] == "active"
    attempt = current["current_attempt"]
    message = service.db.fetchone(
        "SELECT id FROM messages WHERE attempt_id=? AND kind='instruction' ORDER BY sequence DESC LIMIT 1",
        (attempt["id"],),
    )
    assert message is not None
    service.acknowledge(worker, AckInput(message_ids=[message["id"]]))
    service.mark_message_handled(worker, message["id"], evidence="The exact correction was incorporated.")
    reported = service.report(worker, attempt["id"], ReportInput(
        kind="completion_claim", expected_goal_version=current["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=current["generation"],
        summary="Availability is 99.95 percent; the original measurement source is now identified.",
        idempotency_key="corrected-execution-source-report",
    ))
    service.review(cao, ReviewInput(
        attempt_id=reported["current_attempt"]["id"], expected_purpose_version=1,
        verdict="ok", summary="The fixed evidence gap is resolved.",
        criterion_assessments=[{"criterion_id": "task_1", "verdict": "met",
            "evidence": "The revised report identifies the original measurement source."}],
        idempotency_key="corrected-execution-source-review",
    ))
    final = service.get_work(work["id"], cao)
    assert [item["verdict"] for item in final["reviews"]] == ["needs_work", "ok"]
    assert final["worker_purpose"] == purpose
