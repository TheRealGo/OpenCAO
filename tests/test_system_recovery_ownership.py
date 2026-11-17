from __future__ import annotations

import json

import pytest
from test_supervision_continuity import _blocked_command
from test_worker_thread_lifecycle_core import (
    _attached,
    _seed_managed_thread,
    _seeded_managed_work_assignment,
)

from cao_control_plane.database import SCHEMA_VERSION, Database
from cao_control_plane.errors import ConflictError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import BoundaryDispositionInput, BoundaryInput
from cao_control_plane.projection import verify_projection
from cao_control_plane.runtime_recovery import (
    RuntimeRecoveryObservation,
    pending_runtime_recovery_holds,
    record_runtime_recovery_observation,
)
from cao_control_plane.service import ControlPlane


def _ready_recovery(system, tmp_path):
    service, actor, work, boundary, request, assignment = _blocked_command(system, tmp_path)
    assert service.dispose_boundary(actor, boundary["id"], request)["kind"] == "wait_system"
    # Test-only target-consumption evidence. Production must obtain it from
    # the exact provider/Delivery; elapsed time and requester approval are not proof.
    service.db.execute(
        "UPDATE message_deliveries SET state = 'handled' WHERE message_id = ?", (assignment,),
    )
    assert service.reconcile_cao_supervision_obligations() == 1
    current = service.get_work(work["id"])
    boundary = current["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        actor, work["id"], boundary_id=boundary["id"], expected_generation=current["generation"],
        idempotency_key="recovery-owner-decision",
    )
    return service, actor, current, boundary, turn, assignment


@pytest.mark.parametrize("requester_input_kind", [None, "private_information"])
def test_technical_wait_needs_concrete_new_requester_input(system, tmp_path, requester_input_kind):
    service, actor, work, boundary, turn, _ = _ready_recovery(system, tmp_path)
    request = BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
        expected_purpose_version=1, kind="wait_user", reason="Continuation needs a decision.",
        instruction="Supply the missing private source." if requester_input_kind else "Approve reconnecting.",
        resume_condition="The concrete input or safe continuation is available.",
        requester_input_kind=requester_input_kind,
    )
    decision = service.dispose_boundary(actor, boundary["id"], request)
    assert service.dispose_boundary(actor, boundary["id"], request) == decision
    current = service.get_work(work["id"])
    if requester_input_kind:
        assert decision["kind"] == "wait_user"
        assert current["state"] == "user_needed"
        assert current["system_recovery"] is None
        assert current["requester_escalation"]["decision"] == request.instruction
        assert current["requester_escalation"]["requester_input_kind"] == requester_input_kind
    else:
        assert decision["kind"] == "wait_system"
        assert current["state"] == "waiting_supervisor"
        assert current["attention_owner"] == "cao"
        assert current["requester_escalation"] is None
        assert current["system_recovery"]["requester_action_required"] is False
        projected = MCPServer(service).call_tool(actor, "cao_get_work", {"work_item_id": work["id"]})
        assert projected["system_recovery"]["owner"] == "cao"
        assert projected["system_recovery"]["requester_action_required"] is False
        notice = service.db.fetchone(
            "SELECT * FROM messages WHERE id = ?", (current["system_recovery"]["message_id"],),
        )
        [exact] = MCPServer(service).call_tool(actor, "cao_get_inbox", {"message_id": notice["id"]})["items"]
        rendered = exact["processing_instructions"]
        assert "No requester action or approval" in rendered
        assert "Approve reconnecting" not in rendered
        assert service.reconcile_cao_supervision_obligations() == 0
    assert verify_projection(service.db).healthy


def test_cao_can_continue_a_settled_system_hold_without_requester_reply(system, tmp_path):
    service, actor, work, boundary, turn, assignment = _ready_recovery(system, tmp_path)
    service.dispose_boundary(actor, boundary["id"], BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
        expected_purpose_version=1, kind="wait_system", reason="Diagnose the supervisor interruption.",
        resume_condition="CAO verifies the exact continuation route.",
    ))
    assert service.reconcile_cao_supervision_obligations() == 0
    resumed = service.reply(actor, work["id"],
        "Continue only the verified remaining outcome. Do not read files or run commands.",
        expected_purpose_version=1, idempotency_key="cao-owned-system-continuation")
    assert resumed["state"] == "active"
    assert resumed["id"] == work["id"]
    assert resumed["managed_worker_thread_id"] == work["managed_worker_thread_id"]
    assert resumed["requester_escalation"] is None
    assert resumed["system_recovery"] is None
    original = service.db.fetchone("SELECT state FROM message_deliveries WHERE message_id = ?", (assignment,))
    assert original["state"] == "handled"
    command = service.db.fetchone(
        "SELECT payload_json FROM messages WHERE work_item_id = ? AND kind = 'instruction' ORDER BY sequence DESC",
        (work["id"],),
    )
    instruction = json.loads(command["payload_json"])["instruction"]
    assert "First inspect the current task" in instruction
    assert "Report unresolved evidence back to me." in instruction
    assert "CAO" not in instruction
    # A recovery envelope must not add mandatory workspace inspection to an
    # otherwise no-file task. The original sealed restrictions still apply.
    assert "workspace" not in instruction
    assert "Do not read files or run commands." in instruction
    assert verify_projection(service.db).healthy


def _runtime_diagnostic_hold(system, tmp_path):
    service, actor, work, boundary, turn, assignment = _ready_recovery(system, tmp_path)
    service.dispose_boundary(actor, boundary["id"], BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
        expected_purpose_version=1, kind="wait_system", reason="Diagnose the failed runtime binding.",
        resume_condition="Authoritative runtime evidence changes.",
    ))
    current = service.get_work(work["id"])
    assert current["system_recovery"]["watch_kind"] == "runtime_diagnostics"
    return service, actor, current, assignment


def test_unavailable_probe_keeps_the_hold_until_a_usable_diagnosis_changes(system, tmp_path):
    service, _actor, work, assignment = _runtime_diagnostic_hold(system, tmp_path)
    [candidate] = pending_runtime_recovery_holds(service)
    unavailable = RuntimeRecoveryObservation("unavailable", "runtime_recovery_probe_unavailable", "a" * 64)
    assert not record_runtime_recovery_observation(service, candidate, unavailable)
    assert not record_runtime_recovery_observation(service, candidate, unavailable)
    current = service.get_work(work["id"])
    assert current["generation"] == work["generation"]
    assert current["open_boundaries"] == []
    assert current["system_recovery"]["owner"] == "cao"
    assert current["current_attempt"]["activity"]["provider_start_diagnosis"]["reason_code"] == unavailable.code
    assert service.db.fetchone("SELECT COUNT(*) n FROM messages WHERE "
        "json_extract(payload_json,'$.action')='worker_start_diagnosed'")["n"] == 0
    reloaded = ControlPlane(Database(system["settings"]), system["settings"])
    [held] = pending_runtime_recovery_holds(reloaded)
    events = reloaded.db.fetchone("SELECT MAX(sequence) n FROM events")["n"]
    assert not record_runtime_recovery_observation(reloaded, held, unavailable)
    assert reloaded.db.fetchone("SELECT MAX(sequence) n FROM events")["n"] == events
    ready = RuntimeRecoveryObservation("ready", "runtime_binding_diagnosed", "b" * 64)
    assert record_runtime_recovery_observation(reloaded, held, ready)
    assert not record_runtime_recovery_observation(reloaded, held, ready)
    assert reloaded.get_work(work["id"])["requester_escalation"] is None
    assert reloaded.db.fetchone("SELECT state FROM message_deliveries WHERE message_id = ?", (assignment,))["state"] == "handled"
    assert verify_projection(reloaded.db).healthy


def test_runtime_probe_cannot_rearm_a_finished_worker_or_forge_a_diagnostic(system, tmp_path):
    from cao_control_plane.models import WorkerThreadLifecycleInput

    service, actor, work, _ = _runtime_diagnostic_hold(system, tmp_path)
    [candidate] = pending_runtime_recovery_holds(service)
    service.finish_worker_thread(actor, WorkerThreadLifecycleInput(
        worker_thread_id=work["managed_worker_thread_id"], idempotency_key="finish-during-diagnosis",
    ))
    events = service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"]
    observation = RuntimeRecoveryObservation("ready", "runtime_binding_diagnosed", "c" * 64)
    assert not record_runtime_recovery_observation(service, candidate, observation)
    assert service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"] == events
    with pytest.raises(ValueError):
        record_runtime_recovery_observation(service, candidate,
            RuntimeRecoveryObservation("ready", "private-provider-body", "/private/path"))


def test_restart_preserves_unknown_delivery_and_cao_recovery_owner(system, tmp_path):
    service, actor, work, boundary, request, assignment = _blocked_command(system, tmp_path)
    service.dispose_boundary(actor, boundary["id"], request)
    unknown = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment,)))
    reloaded = ControlPlane(Database(system["settings"]), system["settings"])
    current = reloaded.get_work(work["id"])
    assert current["system_recovery"]["owner"] == "cao"
    assert current["requester_escalation"] is None
    assert reloaded.reconcile_cao_supervision_obligations() == 0
    assert dict(reloaded.db.fetchone("SELECT * FROM message_deliveries WHERE message_id = ?", (assignment,))) == unknown
    assert verify_projection(reloaded.db).healthy


def test_unsettled_admission_keeps_diagnosis_and_one_actionable_cao_wake(system, tmp_path):
    from cao_control_plane.mcp import _conversation_work_projection
    from cao_control_plane.runtime import Dispatcher

    service, actor, work, boundary, request, assignment = _blocked_command(system, tmp_path)
    service.dispose_boundary(actor, boundary["id"], request)
    current = service.get_work(work["id"])
    assert current["system_recovery"]["admission_blocker"] == "worker_command_outcome_unsettled"
    [candidate] = pending_runtime_recovery_holds(service)
    assert candidate["diagnostic_message_id"] == assignment
    assert candidate["recovery_blocker"] == "worker_command_outcome_unsettled"
    assert candidate["assignment_not_submitted_proven"] is False
    service.db.execute("UPDATE message_deliveries SET state='handled' WHERE recipient_id=?",
        (work["supervisor_id"],))
    original_delivery = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?", (assignment,)))
    input_count = service.db.fetchone("SELECT COUNT(*) n FROM messages WHERE work_item_id=? "
        "AND kind IN ('assignment','instruction')", (work["id"],))["n"]
    observation = RuntimeRecoveryObservation("blocked", "runtime_assignment_input_unmatched", "e" * 64,
        "unmatched", False, True)
    assert record_runtime_recovery_observation(service, candidate, observation)
    current = service.get_work(work["id"])
    assert current["generation"] == work["generation"] and current["state"] == "waiting_supervisor"
    assert current["system_recovery"]["watch_kind"] == "runtime_admission_and_diagnostics"
    assert current["requester_escalation"] is None
    assert _conversation_work_projection(current)["system_recovery"]["admission_blocker"] == "worker_command_outcome_unsettled"
    diagnosis = current["current_attempt"]["activity"]["provider_start_diagnosis"]
    assert diagnosis["input_state"] == "unmatched" and diagnosis["watch_active"]
    notice = service.db.fetchone("SELECT id FROM messages WHERE work_item_id=? "
        "AND json_extract(payload_json,'$.action')='worker_start_diagnosed'", (work["id"],))
    assert notice is not None
    assert Dispatcher(service, system["settings"])._claim_delivery()["message_id"] == notice["id"]
    reloaded = ControlPlane(Database(system["settings"]), system["settings"])
    sequence = reloaded.db.fetchone("SELECT MAX(sequence) n FROM events")["n"]
    assert not record_runtime_recovery_observation(reloaded, candidate, observation)
    assert reloaded.db.fetchone("SELECT MAX(sequence) n FROM events")["n"] == sequence
    assert dict(reloaded.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?", (assignment,))) == original_delivery
    assert reloaded.db.fetchone("SELECT COUNT(*) n FROM messages WHERE work_item_id=? "
        "AND kind IN ('assignment','instruction')", (work["id"],))["n"] == input_count

    # An independently authorized lifecycle action wins over an in-flight probe.
    from cao_control_plane.models import WorkerThreadLifecycleInput

    service.finish_worker_thread(actor, WorkerThreadLifecycleInput(
        worker_thread_id=work["managed_worker_thread_id"], idempotency_key="finish-held-diagnosis"))
    sequence = service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"]
    assert not record_runtime_recovery_observation(service, candidate,
        RuntimeRecoveryObservation("blocked", "runtime_assignment_input_unmatched", "f" * 64, "unmatched", False, True))
    assert service.db.fetchone("SELECT MAX(sequence) n FROM events")["n"] == sequence


def test_v50_upgrade_preserves_unknown_input_and_admits_empty_blocker_diagnosis(system, tmp_path):
    service, actor, work, boundary, request, assignment = _blocked_command(system, tmp_path)
    service.dispose_boundary(actor, boundary["id"], request)
    before = dict(service.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?", (assignment,)))
    # A legacy runtime-only hold had no admission-watch key at all.
    service.db.execute("UPDATE boundary_dispositions SET recovery_blocker='' WHERE boundary_id=?", (boundary["id"],))
    service.db.execute("ALTER TABLE boundary_dispositions DROP COLUMN recovery_probe_digest")
    service.db.execute("UPDATE metadata SET value='50' WHERE key='schema_version'")
    service.db.execute("PRAGMA user_version=50")
    service.db.execute("DELETE FROM schema_migrations WHERE version=51")
    upgraded = ControlPlane(Database(system["settings"]), system["settings"])
    [held] = pending_runtime_recovery_holds(upgraded)
    assert held["work_item_id"] == work["id"]
    assert upgraded.db.fetchone("PRAGMA user_version")[0] == SCHEMA_VERSION
    assert dict(upgraded.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?", (assignment,))) == before
    observation = RuntimeRecoveryObservation("ready", "runtime_binding_diagnosed", "d" * 64)
    # Unknown input still blocks execution, while CAO gets one actionable diagnosis.
    generation = upgraded.get_work(work["id"])["generation"]
    assert record_runtime_recovery_observation(upgraded, held, observation)
    assert not record_runtime_recovery_observation(upgraded, held, observation)
    assert upgraded.get_work(work["id"])["generation"] == generation
    assert dict(upgraded.db.fetchone("SELECT * FROM message_deliveries WHERE message_id=?", (assignment,))) == before
    assert upgraded.get_work(work["id"])["requester_escalation"] is None


def test_exact_recovery_decision_cannot_skip_another_boundary(system, tmp_path):
    service, actor, work, boundary, turn, _ = _ready_recovery(system, tmp_path)
    attempt = work["current_attempt"]
    other = service.record_boundary(actor, BoundaryInput(
        source_event_id="independent-system-fence", work_item_id=work["id"], attempt_id=attempt["id"],
        expected_goal_version=work["goal_version"], expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"], expected_generation=work["generation"],
        kind="failure", summary="Another exact recovery remains unresolved.", runtime_state="failed",
        metadata={"system_recovery": True, "reason": "independent_execution_fence"},
    ))
    with pytest.raises(ConflictError, match="superseded"):
        service.dispose_boundary(actor, boundary["id"], BoundaryDispositionInput(
            turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
            expected_purpose_version=1, kind="correct", reason="Attempt the admitted continuation.",
            instruction="Complete only verified remaining work.",
        ))
    assert other["id"] in {item["id"] for item in service.get_work(work["id"])["open_boundaries"]}
    assert service.db.fetchone(
        "SELECT COUNT(*) AS n FROM messages WHERE work_item_id = ? AND kind = 'instruction'", (work["id"],),
    )["n"] == 0


@pytest.mark.parametrize("followup_state", ["queued", "dispatched"])
def test_only_proven_unclaimed_followups_are_excluded_from_execution_ownership(system, followup_state):
    service = system["service"]
    actor = _attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=731)
    current, followup = [service.assign_work(actor, _seeded_managed_work_assignment(
        ids, title=f"Queued scope {index}", objective=f"Complete independent outcome {index}.",
        acceptance=["Only an actual handoff counts as execution ownership."],
        idempotency_key=f"queued-scope-{index}",
    )) for index in range(2)]
    message = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND kind = 'assignment'", (current["id"],),
    )
    service.db.execute(
        "UPDATE message_deliveries SET state = 'dispatched' WHERE message_id = ?", (message["id"],),
    )
    followup_message = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? AND kind = 'assignment'", (followup["id"],),
    )
    service.db.execute("UPDATE message_deliveries SET state = ? WHERE message_id = ?",
        (followup_state, followup_message["id"]))
    service.fail_runtime_enrollment(ids["runtime_id"], reason="runtime_unavailable")
    recovered = service.recover_terminal_worker_attempt(ids["runtime_id"], reason="runtime_unavailable")
    assert recovered is not None
    queued = service.get_work(followup["id"])
    if followup_state == "queued":
        assert recovered["work_item_id"] == current["id"]
        assert recovered["metadata"].get("ambiguous_runtime_ownership") is not True
        assert queued["state"] == "active"
        assert queued["generation"] == followup["generation"]
        assert queued["open_boundaries"] == []
    else:
        assert recovered["metadata"].get("ambiguous_runtime_ownership") is True
        assert queued["state"] == "waiting_supervisor"
        assert queued["attention_owner"] == "cao"
    assert queued["current_attempt"]["assignment_delivery"]["state"] == followup_state


@pytest.mark.parametrize("reported_as", ["blocker", "worker_output"])
def test_reported_capability_blocker_accepts_reviewed_repair_and_continues_without_requester_input(system, monkeypatch, reported_as):
    from test_worker_business_purpose import _reported, _turn

    if reported_as == "worker_output":
        # The same provider claim may arrive through automatic output instead
        # of the reporting tool. Preserve the normal Work/Boundary transitions.
        service = system["service"]
        original = service._record_boundary_tx

        def captured(connection, *, actor, request):
            if request.kind.value == "blocker":
                from cao_control_plane.models import BoundaryKind

                request = request.model_copy(update={"kind": BoundaryKind.WORKER_OUTPUT})
            return original(connection, actor=actor, request=request)

        monkeypatch.setattr(service, "_record_boundary_tx", captured)
    service, actor, _, _, reported = _reported(system, kind='blocker')
    baseline_violations = verify_projection(service.db).violations
    work = service.get_work(reported['id'])
    boundary = work['open_boundaries'][0]
    assert boundary['kind'] == reported_as
    turn = _turn(service, actor, work)
    request = BoundaryDispositionInput(
        turn_id=turn['id'], lease_token=turn['lease_token'], expected_generation=work['generation'],
        expected_purpose_version=1, kind='wait_system', reason='The supplied tool has no connected provider.',
        instruction='Inspect the authorized provider inventory and restore the existing permitted route; do not create new authority or replay an unknown operation.',
        resume_condition='The authorized provider is connected and the remaining task can run.',
    )
    decision = service.dispose_boundary(actor, boundary['id'], request)
    assert decision['kind'] == 'wait_system'
    held = service.get_work(work['id'])
    assert held['system_recovery']['requester_action_required'] is False
    assert held['system_recovery']['recovery_plan'] == request.instruction
    assert service.reconcile_cao_supervision_obligations() == 0
    repaired = service.reply(actor, work['id'],
        'Inspect the authorized provider inventory and continue the unmet task only through the verified existing route.',
        expected_purpose_version=1, idempotency_key='capability-repair-with-standing-authority')
    assert repaired['state'] == 'active' and repaired['id'] == work['id']
    assert verify_projection(service.db).violations == baseline_violations


@pytest.mark.parametrize("reported_as", ["blocker", "worker_output"])
@pytest.mark.parametrize("recovery_plan", ["", "Diagnose the existing authorized execution route before considering a correction."])
def test_managed_diagnostic_hold_needs_no_worker_instruction_or_review(system, monkeypatch, reported_as, recovery_plan):
    from test_worker_business_purpose import _reported, _turn

    service = system["service"]
    if reported_as == "worker_output":
        original = service._record_boundary_tx

        def captured(connection, *, actor, request):
            from cao_control_plane.models import BoundaryKind

            if request.kind.value == "blocker":
                request = request.model_copy(update={"kind": BoundaryKind.WORKER_OUTPUT})
            return original(connection, actor=actor, request=request)

        monkeypatch.setattr(service, "_record_boundary_tx", captured)
    service, actor, _, _, reported = _reported(system, kind="blocker")
    work = service.get_work(reported["id"])
    boundary = work["open_boundaries"][0]
    assert boundary["kind"] == reported_as
    turn = _turn(service, actor, work)
    baseline_violations = verify_projection(service.db).violations
    before = service.db.fetchone("SELECT COUNT(*) AS n FROM messages WHERE kind='instruction'")["n"]
    reviews = service.db.fetchone("SELECT COUNT(*) AS n FROM instruction_reviews")["n"]

    review = service.instruction_reviewer.review

    def forbidden_review(_proposal):
        raise AssertionError("A passive diagnostic hold sends no instruction to review")

    monkeypatch.setattr(service.instruction_reviewer, "review", forbidden_review)
    request = BoundaryDispositionInput(
        turn_id=turn["id"], lease_token=turn["lease_token"], expected_generation=work["generation"],
        expected_purpose_version=1, kind="wait_system",
        reason="The Worker returns unfinished output without execution evidence; the cause needs diagnosis.",
        instruction=recovery_plan,
        resume_condition="CAO verifies the cause and an admissible continuation route.",
    )
    decision = service.dispose_boundary(actor, boundary["id"], request)
    assert decision["kind"] == "wait_system"
    assert service.dispose_boundary(actor, boundary["id"], request) == decision
    held = service.get_work(work["id"])
    assert held["state"] == "waiting_supervisor"
    assert held["attention_owner"] == "cao"
    assert held["requester_escalation"] is None
    assert held["system_recovery"]["requester_action_required"] is False
    assert held["system_recovery"]["reason"] == request.reason
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM messages WHERE kind='instruction'")["n"] == before
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM instruction_reviews")["n"] == reviews
    assert service.reconcile_cao_supervision_obligations() == 0
    assert service.reconcile_cao_supervision_obligations() == 0
    assert verify_projection(service.db).violations == baseline_violations

    reviewed = []

    def review_correction(proposal):
        reviewed.append(proposal)
        return review(proposal)

    monkeypatch.setattr(service.instruction_reviewer, "review", review_correction)
    resumed = service.reply(
        actor, work["id"], "Continue only the verified unmet task through the existing authorized route.",
        expected_purpose_version=1, idempotency_key="diagnostic-hold-correction",
    )
    assert resumed["state"] == "active"
    assert resumed["system_recovery"] is None
    assert len(reviewed) == 1
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM messages WHERE kind='instruction'")["n"] == before + 1
    assert verify_projection(service.db).violations == baseline_violations
