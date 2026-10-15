from __future__ import annotations

import json

import pytest
from conftest import business_purpose_contract
from test_worker_thread_lifecycle_core import (
    _attached,
    _connect_seeded_managed_runtime,
    _seed_managed_thread,
)

from cao_control_plane.dashboard import DashboardReadModel
from cao_control_plane.database import Database
from cao_control_plane.errors import (
    AuthorizationError,
    ConflictError,
    NotFoundError,
    ValidationError,
)
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    CloseCAOConversationInput,
    DeleteWorkerThreadInput,
    InstructWorkerThreadInput,
    ReportInput,
    RequesterDecisionInput,
    ResumeWorkerThreadInput,
    ReviewInput,
    SetWorkerPurposeInput,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime import render_message
from cao_control_plane.service import ControlPlane

PURPOSE = "利用状況や要件の変化を通じて、サービスの信頼性を保つ。"


def _worker(system, ordinal=8101):
    cao = _attached(system)
    ids = _seed_managed_thread(system, cao, ordinal=ordinal, with_purpose=False)
    return system["service"], cao, ids


def _purpose(service, cao, ids, *, text=PURPOSE, version=0):
    return service.set_worker_purpose(
        cao,
        SetWorkerPurposeInput(
            contract=business_purpose_contract(),
            worker_thread_id=ids["thread_id"],
            purpose=text,
            expected_purpose_version=version,
            reason="依頼者がこの継続的な業務をWorkerに任せたため。",
            idempotency_key=f"purpose-{ids['thread_id']}-{version}",
        ),
    )["worker_purpose"]


def _instruction(service, cao, ids, text, *, version=1, **fields):
    result = service.instruct_worker_thread(
        cao,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            instruction=text,
            expected_purpose_version=version,
            idempotency_key=text,
            **fields,
        ),
    )
    return service.get_work(result["task"]["work_item_id"], cao)


def test_business_purpose_survives_distinct_instructions_and_restart(system):
    service, cao, ids = _worker(system)
    purpose = _purpose(service, cao, ids)
    first = _instruction(service, cao, ids, "Measure request failure rates.")
    second = _instruction(service, cao, ids, "Exclude temporary files from the report.")
    assert first["id"] != second["id"]
    for work in (first, second):
        assert work["worker_purpose"] == purpose
        assert work["assignment_purpose"] == purpose
        assert work["current_goal_revision"]["packet"]["worker_purpose"] == purpose
        assignment = service.db.fetchone(
            "SELECT * FROM messages WHERE work_item_id = ? AND kind = 'assignment'",
            (work["id"],),
        )
        payload = json.loads(assignment["payload_json"])
        assert payload["worker_purpose"] == purpose
        prompt = render_message({"kind": "assignment", "payload": payload})
        assert f"Business purpose: {PURPOSE}" in prompt
        assert f"Current instruction: {work['instruction']}" in prompt
    assert first["instruction"] != PURPOSE and second["instruction"] != PURPOSE
    before = first["current_goal_revision"]["packet_digest"]
    reopened = ControlPlane(Database(system["settings"]), system["settings"])
    retained = reopened.get_work(first["id"], cao)
    assert retained["worker_purpose"] == purpose
    assert retained["current_goal_revision"]["packet_digest"] == before
    assert not build_projection(reopened.db).violations


def test_unset_purpose_does_not_promote_the_immediate_instruction(system):
    service, cao, ids = _worker(system)
    before = service.db.fetchone("SELECT COUNT(*) AS n FROM work_items")["n"]
    with pytest.raises(ConflictError) as raised:
        _instruction(service, cao, ids, "Do not include an attachment.", version=0)
    assert raised.value.details["reason_code"] == "worker_purpose_unset"
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM work_items")["n"] == before
    assert service.list_managed_workers(cao)[0]["worker_purpose"] is None


def test_each_instruction_confirms_current_purpose_without_rewriting_prior_work(system):
    service, cao, ids = _worker(system)
    original = _purpose(service, cao, ids)
    work = _instruction(service, cao, ids, "Measure current availability.")
    frozen = work["current_goal_revision"]["packet_digest"]
    revised = _purpose(
        service, cao, ids, text="Maintain continuity during a service migration.", version=1
    )
    with pytest.raises(ConflictError) as raised:
        _instruction(service, cao, ids, "Choose the next measurement.", version=1)
    assert raised.value.details["reason_code"] == "worker_purpose_version_conflict"
    with pytest.raises(ConflictError) as raised:
        service.instruct_worker_thread(
            cao,
            InstructWorkerThreadInput(
                worker_thread_id=ids["thread_id"],
                instruction="Consider a mitigation.",
                idempotency_key="missing-purpose-confirmation",
            ),
        )
    assert raised.value.details["reason_code"] == "worker_purpose_confirmation_required"
    current = service.get_work(work["id"], cao)
    assert current["worker_purpose"] == revised
    assert current["assignment_purpose"] == original
    assert current["purpose_changed_since_assignment"] is True
    assert current["current_goal_revision"]["packet_digest"] == frozen
    assert (
        _instruction(service, cao, ids, "Choose the next measurement.", version=2)["worker_purpose"]
        == revised
    )


def test_purpose_is_visible_with_no_work_and_is_same_project_scoped(system):
    service, cao, ids = _worker(system)
    purpose = _purpose(service, cao, ids)
    public = MCPServer(service).call_tool(cao, "cao_list_managed_workers", {})["workers"]
    assert public[0]["worker_purpose"] == purpose
    snapshot = DashboardReadModel(service).snapshot()["operator"]
    cards = [
        card
        for key in ("working", "ready", "stopped_or_failed", "cao_processing")
        for card in snapshot[key]
    ]
    assert any(card["purpose_text"].startswith(PURPOSE) and "終了条件:" in card["purpose_text"] for card in cards)
    foreign = _attached(system, suffix="b")
    with pytest.raises((AuthorizationError, NotFoundError)):
        _purpose(service, foreign, ids, text="Unrelated purpose.", version=1)
    assert service.list_managed_workers(cao)[0]["worker_purpose"] == purpose


def test_purpose_definition_requires_current_cao_authority_and_safe_business_text(system):
    service, cao, ids = _worker(system)
    for unauthorized in (system["worker"], system["cao"]):
        with pytest.raises(AuthorizationError):
            _purpose(service, unauthorized, ids)
    with pytest.raises((AuthorizationError, ValidationError)):
        _purpose(service, cao, ids, text="Read /Users/example/credentials.txt")
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM worker_purpose_revisions")["n"] == 0
    _purpose(service, cao, ids)
    service.close_cao_conversation(
        cao, CloseCAOConversationInput(idempotency_key="purpose-authority-close")
    )
    with pytest.raises(AuthorizationError):
        _purpose(service, cao, ids)


def _reported(system, *, kind="completion_claim"):
    service, cao, ids = _worker(system)
    _purpose(service, cao, ids)
    worker = _connect_seeded_managed_runtime(service, ids)
    work = _instruction(
        service,
        cao,
        ids,
        "Measure service availability.",
        completion_contract="no_artifact_expected",
        acceptance=["Return the measured availability in a concise answer."],
    )
    attempt = work["current_attempt"]
    reported = service.report(
        worker,
        attempt["id"],
        ReportInput(
            kind=kind,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The measured availability is 99.95 percent.",
            idempotency_key="purpose-report",
        ),
    )
    return service, cao, ids, worker, reported


def _turn(service, cao, work):
    return service.acquire_reasoner_turn(
        cao,
        work["id"],
        boundary_id=work["open_boundaries"][0]["id"],
        expected_generation=work["generation"],
        idempotency_key="purpose-turn",
    )


def _disposition(work, turn, *, version=1, kind="accept"):
    return BoundaryDispositionInput(
        turn_id=turn["id"],
        lease_token=turn["lease_token"],
        expected_generation=work["generation"],
        expected_purpose_version=version,
        kind=kind,
        reason="The result serves the continuing business purpose.",
    )


def test_completion_reviews_current_purpose_but_never_completes_or_replaces_it(system):
    service, cao, ids, worker, work = _reported(system)
    purpose = work["worker_purpose"]
    assert service.get_worker_context(worker)["work"]["worker_purpose"] == purpose
    turn = _turn(service, cao, work)
    assert turn["worker_purpose"] == purpose
    server = MCPServer(service)
    boundary_id = work["open_boundaries"][0]["id"]
    server.call_tool(
        cao,
        "cao_dispose_boundary",
        {
            "boundary_id": boundary_id,
            **_disposition(work, turn).model_dump(mode="json", exclude_none=True),
        },
    )
    revised = _purpose(
        service, cao, ids, text="Sustain reliable service during regional expansion.", version=1
    )
    review = ReviewInput(
        attempt_id=work["current_attempt"]["id"],
        verdict="ok",
        summary="The measured result is verified and informs reliability decisions.",
        expected_purpose_version=1,
        expected_assignment_purpose_version=1,
        criterion_assessments=[{"criterion_id": "task_1", "verdict": "met", "evidence": "The availability measurement was verified."}],
        idempotency_key="purpose-review",
    )
    with pytest.raises(ConflictError) as raised:
        service.review(cao, review)
    assert raised.value.details["reason_code"] == "worker_purpose_version_conflict"
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM reviews")["n"] == 0
    result = service.review(cao, review.model_copy(update={"expected_purpose_version": 2}))
    current = service.get_work(work["id"], cao)
    decision = RequesterDecisionInput(
        review_id=current["reviews"][-1]["id"],
        verdict="accepted",
        summary="The requester accepts the measurement task.",
        conversation_evidence_id="requester-measurement-acceptance",
        expected_purpose_version=2,
        idempotency_key="purpose-requester-acceptance",
    )
    service.record_requester_decision(cao, decision)
    final = service.get_work(work["id"], cao)
    assert final["state"] == "completed"
    assert final["worker_purpose"] == revised
    assert final["assignment_purpose"] == purpose
    assert (
        _instruction(service, cao, ids, "Investigate the next reliability risk.", version=2)[
            "worker_purpose"
        ]
        == revised
    )
    checks = service.db.fetchall(
        "SELECT data_json FROM events WHERE event_type = 'work.purpose_confirmed'"
    )
    assert any(json.loads(row["data_json"])["worker_purpose"] == revised for row in checks)
    assert result


def test_stale_boundary_and_reply_cannot_commit_and_lease_replay_refreshes_purpose(system):
    service, cao, ids, _worker_actor, work = _reported(system, kind="question")
    turn = _turn(service, cao, work)
    revised = _purpose(
        service, cao, ids, text="Preserve service continuity as demand grows.", version=1
    )
    replay = _turn(service, cao, work)
    assert replay["id"] == turn["id"] and replay["lease_token"] == turn["lease_token"]
    assert replay["worker_purpose"] == revised
    before = service.db.fetchone("SELECT COUNT(*) AS n FROM boundary_dispositions")["n"]
    for version in (None, 1):
        with pytest.raises(ConflictError):
            service.reply(
                cao,
                work["id"],
                "Inspect the regional breakdown.",
                expected_purpose_version=version,
                idempotency_key="purpose-reply",
            )
        with pytest.raises(ConflictError):
            service.dispose_boundary(
                cao,
                work["open_boundaries"][0]["id"],
                _disposition(work, turn, version=version, kind="correct"),
            )
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM boundary_dispositions")["n"] == before
    # The same leased Boundary can be corrected with the freshly read purpose.
    result = service.dispose_boundary(
        cao,
        work["open_boundaries"][0]["id"],
        _disposition(work, turn, version=2, kind="correct").model_copy(
            update={"instruction": "Inspect the regional breakdown."}
        ),
    )
    assert result["kind"] == "correct"


def test_task_revision_alias_preserves_worker_purpose_and_confirms_version(system):
    service, cao, _ids, _worker_actor, work = _reported(system, kind="question")
    purpose = work["worker_purpose"]
    server = MCPServer(service)
    values = dict(
        work_item_id=work["id"],
        expected_version=1,
        instruction="Measure failures by region.",
        maturity="exploring",
        reason="The regional breakdown is the next useful checkpoint.",
        idempotency_key="revise-task",
    )
    with pytest.raises(ConflictError):
        server.call_tool(cao, "cao_revise_task", values)
    updated = server.call_tool(cao, "cao_revise_task", {**values, "expected_purpose_version": 1})
    assert updated["worker_purpose"] == purpose
    assert updated["instruction"] == values["instruction"]
    assert service.get_work(work["id"], cao)["goal_version"] == 2


def test_purpose_survives_archive_resume_and_delete_retains_audit_without_gate(system):
    service, cao, ids = _worker(system)
    purpose = _purpose(service, cao, ids)
    service.finish_worker_thread(
        cao,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"], idempotency_key="purpose-finish"
        ),
    )
    assert service.list_managed_workers(cao)[0]["worker_purpose"] == purpose
    service.resume_worker_thread(
        cao,
        ResumeWorkerThreadInput(
            worker_thread_id=ids["thread_id"], idempotency_key="purpose-resume"
        ),
    )
    assert service.list_managed_workers(cao)[0]["worker_purpose"] == purpose
    service.db.execute("UPDATE worker_purpose_revisions SET packet_json = ?", ("{broken",))
    listed = MCPServer(service).call_tool(cao, "cao_list_managed_workers", {})["workers"][0]
    assert listed["worker_purpose"] is None and listed["purpose_status"] == "invalid"
    assert any(v.code == "worker.purpose_invalid" for v in build_projection(service.db).violations)
    service.delete_worker_thread(
        cao,
        DeleteWorkerThreadInput(
            worker_thread_id=ids["thread_id"], idempotency_key="purpose-delete"
        ),
    )
    assert service.list_managed_workers(cao) == []
    retained = service.db.fetchone("SELECT purpose FROM worker_purpose_revisions")
    assert retained["purpose"] == PURPOSE


def test_schema46_upgrade_does_not_invent_a_purpose_or_change_legacy_packet_digests(system):
    service, cao, _ids = _worker(system)
    work = service.assign_work(
        cao,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Legacy checkpoint",
            objective="Do not attach a PDF.",
            maturity="exploring",
            idempotency_key="legacy-task",
        ),
    )
    before = (
        work["current_goal_revision"]["packet_digest"],
        work["current_attempt"]["task_packet_digest"],
    )
    with service.db.transaction() as connection:
        connection.execute("DROP TABLE work_purpose_bindings")
        connection.execute("DROP TABLE worker_purpose_revisions")
        connection.execute("DELETE FROM schema_migrations WHERE version > 46")
        connection.execute("PRAGMA user_version = 46")
    reopened = ControlPlane(Database(system["settings"]), system["settings"])
    restored = reopened.get_work(work["id"], cao)
    assert before == (
        restored["current_goal_revision"]["packet_digest"],
        restored["current_attempt"]["task_packet_digest"],
    )
    assert "worker_purpose" not in restored["current_goal_revision"]["packet"]
    assert restored["assignment_purpose"] is None
    assert reopened.list_managed_workers(cao)[0]["worker_purpose"] is None
    assert not build_projection(reopened.db).violations


def test_legacy_progress_handling_still_confirms_purpose(system, legacy_progress_wakes):
    service, cao, ids, _worker_actor, _work = _reported(system, kind="progress")
    message = next(m for m in service.get_inbox(cao)["items"] if m["kind"] == "progress")
    prompt = render_message(message)
    assert "cao_get_work.worker_purpose" in prompt
    service.acknowledge(cao, AckInput(message_ids=[message["id"]]))
    _purpose(service, cao, ids, text="Preserve service reliability during growth.", version=1)
    server = MCPServer(service)
    arguments = {
        "message_id": message["id"],
        "evidence": "The progress serves the current business purpose.",
    }
    for version in (None, 1):
        with pytest.raises(ConflictError):
            server.call_tool(
                cao,
                "cao_mark_handled",
                {
                    **arguments,
                    **({"expected_purpose_version": version} if version is not None else {}),
                },
            )
    result = server.call_tool(cao, "cao_mark_handled", {**arguments, "expected_purpose_version": 2})
    assert result["state"] == "handled"
    # Handling an already committed result is a replay, not another judgment.
    assert server.call_tool(cao, "cao_mark_handled", arguments)["state"] == "handled"


def test_complete_purpose_crosses_mcp_and_dashboard_edge_without_becoming_task_text(system):
    from cao_control_plane.dashboard_edge import _edge_snapshot, _edge_work_history
    from cao_control_plane.dashboard_history import history_reference
    from cao_control_plane.mcp import _conversation_purpose_projection

    service, cao, ids = _worker(system)
    text = (
        "Maintain service continuity.\n\n"
        + "Use reliability evidence to guide improvements. " * 30
        + "Retain this final business condition."
    )
    purpose = _purpose(service, cao, ids, text=text)
    work = _instruction(service, cao, ids, "Measure today's failure rate.")
    server = MCPServer(service)
    public = server.call_tool(cao, "cao_get_work", {"work_item_id": work["id"]})
    assert public["worker_purpose"]["text"] == text
    dashboard = DashboardReadModel(service)
    edge = _edge_snapshot(dashboard.snapshot())
    cards = [
        card
        for group in ("working", "ready", "cao_processing", "stopped_or_failed")
        for card in edge["operator"][group]
    ]
    assert any(card["purpose_text"].startswith(text) and purpose["contract"]["completion"] in card["purpose_text"] for card in cards)
    history = _edge_work_history(dashboard.work_history(work=history_reference("work", work["id"])))
    assert history["work"]["purpose_text"].startswith(text)
    assert purpose["contract"]["criteria"][0]["evidence"] in history["work"]["purpose_text"]
    assert history["work"]["instruction_text"] == work["instruction"]
    assert _conversation_purpose_projection({**purpose, "digest": "/private/fixture"}) is None
    assert (
        _conversation_purpose_projection({**purpose, "worker_thread_id": "/private/fixture"})
        is None
    )


def test_concurrent_purpose_revision_has_one_winner_and_old_request_cannot_rebind(system):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    service, cao, ids = _worker(system)
    _purpose(service, cao, ids)
    barrier = Barrier(2)

    def revise(index):
        barrier.wait(timeout=10)
        try:
            return service.set_worker_purpose(
                cao,
                SetWorkerPurposeInput(
                    contract=business_purpose_contract(),
                    worker_thread_id=ids["thread_id"],
                    expected_purpose_version=1,
                    purpose=f"Maintain service continuity during expansion phase {index}.",
                    reason="The requester changed the business direction.",
                    idempotency_key=f"concurrent-purpose-{index}",
                ),
            )
        except ConflictError as error:
            return error.details["reason_code"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(revise, (1, 2)))
    assert sum(isinstance(value, dict) for value in outcomes) == 1
    assert outcomes.count("worker_purpose_version_conflict") == 1
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM worker_purpose_revisions")["n"] == 2
    winner = next(value for value in outcomes if isinstance(value, dict))["worker_purpose"]
    # The initial setter replay preserves its initial receipt, not a fresh revision.
    assert _purpose(service, cao, ids)["version"] == 1
    assert service.list_managed_workers(cao)[0]["worker_purpose"] == winner


def test_explicit_worker_handoff_checks_both_purposes_and_delivers_successor_direction(system):
    service, cao, source, _source_actor, work = _reported(system, kind="question")
    turn = _turn(service, cao, work)
    service.dispose_boundary(
        cao,
        work["open_boundaries"][0]["id"],
        BoundaryDispositionInput(
            **{
                **_disposition(work, turn).model_dump(),
                "kind": "wait_user",
                "instruction": "Choose the Worker responsible for the next checkpoint.",
                "resume_condition": "The requester designates the successor Worker.",
            }
        ),
    )
    target = _seed_managed_thread(system, cao, ordinal=8102, with_purpose=False)
    _purpose(service, cao, target, text="Prepare reliable service capacity for expansion.")
    target_purpose = _purpose(
        service,
        cao,
        target,
        text="Prepare reliable service capacity for regional expansion.",
        version=1,
    )
    arguments = dict(
        worker_id=target["principal_id"],
        managed_worker_thread_id=target["thread_id"],
        managed_worker_thread_generation=1,
        expected_purpose_version=1,
        idempotency_key="purpose-handoff",
        reason="The requester designated this exact successor.",
    )
    with pytest.raises(ConflictError) as raised:
        service.create_attempt(cao, work["id"], expected_successor_purpose_version=1, **arguments)
    assert raised.value.details["reason_code"] == "worker_purpose_version_conflict"
    assert (
        service.get_work(work["id"], cao)["worker_purpose"]["worker_thread_id"]
        == source["thread_id"]
    )
    result = service.create_attempt(
        cao, work["id"], expected_successor_purpose_version=2, **arguments
    )
    current = service.get_work(work["id"], cao)
    assert current["worker_purpose"] == target_purpose
    assert current["assignment_purpose"]["worker_thread_id"] == source["thread_id"]
    assignment = service.db.fetchone(
        "SELECT payload_json FROM messages WHERE attempt_id = ? AND kind = 'assignment'",
        (current["current_attempt"]["id"],),
    )
    assert json.loads(assignment["payload_json"])["worker_purpose"] == target_purpose
    assert result
