"""Causal supervision memory and explicit, fenced Work pause/resumption."""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier
from typing import Any
from unittest.mock import patch

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_cao_notification_delivery_lane import _attached
from test_worker_output_delivery import (
    _begin_successor_capture,
    _capture_fixture,
    _event,
    _read_request,
    _terminal,
)

from cao_control_plane.database import Database, utc_after, utc_now
from cao_control_plane.errors import ConflictError, ControlPlaneError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    GoalRevision,
    MemoryReadInput,
    MemorySearchInput,
    MemoryWriteInput,
    MessageKind,
    ReviewInput,
    RuntimeHeartbeat,
    WorkAssignment,
    WorkHistoryReadInput,
    WorkResumeInput,
)
from cao_control_plane.runtime import Dispatcher
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS, ControlPlane

OUTPUT = "The available evidence does not satisfy the remaining acceptance condition."
INSTRUCTION = "Inspect the remaining acceptance condition using the current method."
REASON = "The exact result still requires additional work."
RESUME_CONDITION = "A different verified method or new evidence is available."


@pytest.fixture
def scenario(system: dict[str, Any]) -> dict[str, Any]:
    actor, work, binding = _capture_fixture(system)
    return {
        "service": system["service"],
        "settings": system["settings"],
        "actor": actor,
        "worker": system["worker"],
        "work_id": work["id"],
        "initial_attempt_id": work["current_attempt"]["id"],
        "initial_generation": work["generation"],
        "binding": binding,
        "round": 0,
    }


def _worker_instructions(case: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in case["service"].db.fetchall(
            "SELECT m.*, d.state AS delivery_state FROM messages m "
            "JOIN message_deliveries d ON d.message_id = m.id "
            "WHERE m.work_item_id = ? AND m.kind = 'instruction' ORDER BY m.sequence",
            (case["work_id"],),
        )
    ]


def _advance_capture(case: dict[str, Any]) -> None:
    service: ControlPlane = case["service"]
    source = service.db.fetchone(
        "SELECT m.id, m.attempt_id, d.runtime_session_id FROM messages m "
        "JOIN message_deliveries d ON d.message_id = m.id "
        "WHERE m.work_item_id = ? AND m.kind IN ('assignment', 'instruction') "
        "AND d.state = 'queued' ORDER BY m.sequence LIMIT 1",
        (case["work_id"],),
    )
    assert source is not None
    original_exchange = service.exchange_runtime_launch_ticket

    def remember_current_worker(*args: Any, **kwargs: Any) -> dict[str, Any]:
        exchanged = original_exchange(*args, **kwargs)
        case["worker"] = service.authenticate(str(exchanged["token"]))
        return exchanged

    with patch.object(service, "exchange_runtime_launch_ticket", remember_current_worker):
        case["binding"] = _begin_successor_capture(
            service,
            {
                **case["binding"],
                "attempt_id": source["attempt_id"],
                "runtime_id": source["runtime_session_id"],
            },
            source["id"],
        )


def _observe(
    case: dict[str, Any], *, text: str = OUTPUT, acknowledge: bool = True, review: bool = True
) -> dict[str, Any]:
    service: ControlPlane = case["service"]
    if case["round"]:
        _advance_capture(case)
    case["round"] += 1
    number = case["round"]
    turn_id = f"supervision-output-turn-{number}"
    event = _event(turn_id=turn_id, item_id=f"supervision-item-{number}", text=text)
    output = service.observe_worker_output(**case["binding"], event=event)
    terminal = _terminal(service, case["binding"], turn_id=turn_id)
    work = service.get_work(case["work_id"])
    assert output["capture_state"] == "available"
    assert output["digest"] == hashlib.sha256(text.encode()).hexdigest()
    assert work["state"] == "waiting_supervisor"
    boundary = work["open_boundaries"][0]
    assert boundary["id"] == terminal["boundary_id"]
    read = service.read_worker_output(case["actor"], _read_request(work, output))
    assert read["content"] == text and read["complete"] is True
    if acknowledge:
        for message_id in (output["notification_message_id"], terminal["notification_message_id"]):
            service.acknowledge(case["actor"], AckInput(message_ids=[message_id]))
    if review:
        service.review(
            case["actor"],
            ReviewInput(
                attempt_id=work["current_attempt"]["id"],
                verdict="needs_work",
                summary=REASON,
                evidence=[{"check": "acceptance", "result": "not_met"}],
                idempotency_key=f"supervision-review-{case['work_id']}-{number}",
            ),
        )
    return {
        "work": work,
        "boundary": boundary,
        "output": output,
        "terminal": terminal,
        "event": event,
        "acknowledged": acknowledge,
        "reviewed": review,
    }


def _decision_request(
    case: dict[str, Any],
    observed: dict[str, Any],
    *,
    kind: str = "correct",
    instruction: str | None = None,
    reason: str = REASON,
) -> BoundaryDispositionInput:
    turn = case["service"].acquire_reasoner_turn(
        case["actor"],
        case["work_id"],
        boundary_id=observed["boundary"]["id"],
        expected_generation=observed["work"]["generation"],
        idempotency_key=f"supervision-turn-{case['work_id']}-{case['round']}",
    )
    case["last_turn"] = turn
    return BoundaryDispositionInput(
        turn_id=turn["id"],
        lease_token=turn["lease_token"],
        expected_generation=observed["work"]["generation"],
        kind=kind,
        reason=reason,
        instruction=("" if kind == "pause" else INSTRUCTION)
        if instruction is None
        else instruction,
        resume_condition=RESUME_CONDITION if kind == "pause" else "",
    )


def _decide(case: dict[str, Any], observed: dict[str, Any], **values: Any) -> dict[str, Any]:
    result = case["service"].dispose_boundary(
        case["actor"],
        observed["boundary"]["id"],
        _decision_request(case, observed, **values),
    )
    observed["expected_decision"] = result
    if observed["acknowledged"]:
        case["service"].mark_message_handled(
            case["actor"],
            observed["output"]["notification_message_id"],
            evidence="The exact output was read and its supervision decision recorded.",
        )
    return result


def _assert_paused(case: dict[str, Any], observed: dict[str, Any]) -> dict[str, Any]:
    work = case["service"].get_work(case["work_id"])
    assert work["state"] == "suspended"
    assert work["attention_owner"] == "none"
    assert work["current_attempt"]["state"] == "suspended"
    assert work["paused_boundary_id"] == observed["boundary"]["id"]
    assert work["generation"] == observed["work"]["generation"] + 1
    assert work.get("suspended_by_work_item_id") is None
    assert work.get("user_needed_boundary_id") is None
    assert work["current_attempt"]["completion_claim"] == {}
    assert work["open_boundaries"] == []
    pause = work["supervision_pause"]
    assert pause["boundary_id"] == observed["boundary"]["id"]
    assert pause["source_generation"] == observed["work"]["generation"]
    assert pause["pause_generation"] == work["generation"]
    assert pause["reason"] and pause["resume_condition"]
    assert pause["paused_at"]
    assert set(pause) == {
        "boundary_id",
        "source_generation",
        "pause_generation",
        "reason",
        "resume_condition",
        "paused_at",
    }
    return work


def _read_history(case: dict[str, Any], **values: Any) -> dict[str, Any]:
    options = {"work_item_id": case["work_id"], "limit": 50, **values}
    return case["service"].read_work_history(case["actor"], WorkHistoryReadInput(**options))


def _assert_cycles(
    case: dict[str, Any], cycles: list[dict[str, Any]], history: list[dict[str, Any]]
) -> None:
    assert [cycle["boundary_id"] for cycle in cycles] == [
        item["boundary"]["id"] for item in history
    ]
    sequences = [cycle["sequence"] for cycle in cycles]
    assert sequences == sorted(set(sequences))
    assert all(type(sequence) is int and sequence > 0 for sequence in sequences)
    serialized = json.dumps(cycles)
    assert all(observed["event"].text not in serialized for observed in history)
    for cycle, observed in zip(cycles, history, strict=True):
        boundary = observed["boundary"]
        assert cycle["attempt_id"] == boundary["attempt_id"]
        assert cycle["goal_version"] == boundary["goal_version"]
        assert cycle["generation"] == boundary["generation"]
        assert cycle["boundary_kind"] == boundary["kind"]
        assert cycle["observed_summary"] == boundary["summary"]
        refs = {item["output_id"]: item for item in cycle["output_refs"]}
        assert set(refs) == {observed["output"]["id"], observed["terminal"]["id"]}
        for receipt in (observed["output"], observed["terminal"]):
            reference = refs[receipt["id"]]
            assert reference["digest"] == receipt["digest"]
            assert reference["capture_state"] == receipt["capture_state"]
            assert reference["event_kind"] == receipt["kind"]
            assert reference["phase"] == receipt["phase"]
        expected_review = (
            {"verdict": "needs_work", "summary": REASON} if observed["reviewed"] else None
        )
        assert cycle["review"] == expected_review
        decision = observed.get("expected_decision")
        assert cycle["decision"] == (
            {key: decision[key] for key in ("kind", "reason", "instruction", "resume_condition")}
            if decision is not None
            else None
        )
        expected_prior = observed.get("expected_prior")
        if expected_prior is not None:
            source = case["service"].db.fetchone(
                "SELECT kind FROM messages WHERE id=?",
                (observed["output"]["source_message_id"],),
            )
            assert cycle["prior_instruction"]["kind"] == source["kind"]
            assert cycle["prior_instruction"]["instruction"] == expected_prior["instruction"]
            assert cycle["prior_instruction"]["reason"] == expected_prior["reason"]


def _assert_history(
    case: dict[str, Any], page: dict[str, Any], history: list[dict[str, Any]]
) -> None:
    assert page["work_item_id"] == case["work_id"]
    assert page["total"] == len(history)
    assert page["before_sequence"] is None
    assert page["next_before_sequence"] is None
    assert page["untrusted"] is True
    _assert_cycles(case, page["cycles"], history)


def _assert_memory_context(
    case: dict[str, Any], bundle: dict[str, Any], history: list[dict[str, Any]]
) -> dict[str, Any]:
    assert "supervision_context" not in bundle
    memory = bundle["supervision_memory"]
    assert memory["history"]["work_item_id"] == case["work_id"]
    assert memory["history"]["boundary_count"] == len(history)
    goal_versions = memory["history"]["goal_versions"]
    assert goal_versions == sorted({goal["version"] for goal in bundle["goal_history"]})
    assert memory["guidance"]
    recall = memory["recall"]
    assert recall["query"] and recall["guidance"]
    assert recall["total"] >= 0 and recall["offset"] == 0
    assert recall["next_offset"] is None or recall["next_offset"] > 0
    assert isinstance(recall["memories"], list)
    assert all("value" not in item for item in recall["memories"])
    serialized = json.dumps(memory)
    assert all(observed["event"].text not in serialized for observed in history)
    return memory


def _pause(case: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    observed = _observe(case)
    decision = _decide(case, observed, kind="pause")
    _assert_paused(case, observed)
    return observed, decision


def _resume_request(case: dict[str, Any], **overrides: Any) -> WorkResumeInput:
    work = case["service"].get_work(case["work_id"])
    values = {
        "expected_generation": work["generation"],
        "pause_boundary_id": work["paused_boundary_id"],
        "reason": "A changed execution condition was independently checked.",
        "instruction": "Use the newly verified method for the remaining acceptance condition.",
        "resume_evidence": "An independent prerequisite check now confirms the new method is available.",
        "idempotency_key": "resume-supervision-pause",
    }
    values.update(overrides)
    return WorkResumeInput(**values)


def _snapshot(case: dict[str, Any]) -> dict[str, Any]:
    service: ControlPlane = case["service"]
    return {
        "work": dict(
            service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (case["work_id"],))
        ),
        **{
            table: [
                dict(row) for row in service.db.fetchall(f"SELECT * FROM {table} ORDER BY rowid")
            ]
            for table in (
                "attempts",
                "messages",
                "message_deliveries",
                "events",
                "directives",
                "goal_revisions",
                "work_pauses",
                "work_pause_resumptions",
                "effect_operations",
            )
        },
    }


def _seed_worker_input(case: dict[str, Any], kind: str) -> str:
    service: ControlPlane = case["service"]
    work = service.get_work(case["work_id"])
    with service.db.transaction() as connection:
        message = service._message(
            connection,
            sender_id=case["actor"]["id"],
            recipient_id=case["worker"]["id"],
            kind=MessageKind(kind),
            payload={"instruction": INSTRUCTION, "generation": work["generation"]},
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            goal_version=work["goal_version"],
            runtime_session_id=case["binding"]["runtime_id"],
            idempotency_key=f"retained-supervision-input-{kind}",
        )
    return str(message["id"])


@pytest.mark.parametrize("kind", ["correct", "continue"])
def test_ten_repeated_decisions_preserve_full_history_without_rewriting_authority(
    scenario: dict[str, Any], kind: str
) -> None:
    service = scenario["service"]
    history = []
    previous = None
    for number in range(1, 11):
        observed = _observe(scenario, review=kind != "continue")
        observed["expected_prior"] = previous
        history.append(observed)
        before = _read_history(scenario)
        _assert_history(scenario, before, history)
        assert before["cycles"][-1]["decision"] is None
        memory = _assert_memory_context(
            scenario, service.get_work(scenario["work_id"], actor=scenario["actor"]), history
        )
        decision = _decide(scenario, observed, kind=kind)
        assert scenario["last_turn"]["supervision_memory"] == memory
        work = service.get_work(scenario["work_id"])
        _assert_history(scenario, _read_history(scenario), history)
        assert decision["kind"] == kind
        assert work["state"] == "active" and work["attention_owner"] == "worker"
        assert work.get("supervision_pause") is None and work.get("paused_boundary_id") is None
        assert work["generation"] == scenario["initial_generation"]
        assert work["current_attempt"]["id"] == scenario["initial_attempt_id"]
        assert len(_worker_instructions(scenario)) == number
        assert len(work["boundaries"]) == number
        assert len(work["worker_outputs"]) == number * 2
        previous = decision
    assert len(work["reviews"]) == (10 if kind == "correct" else 0)
    page = _read_history(scenario, limit=3)
    _assert_cycles(scenario, page["cycles"], history[-3:])
    assert page["total"] == 10 and page["before_sequence"] is None
    assert page["next_before_sequence"] == page["cycles"][0]["sequence"]
    recovered = list(page["cycles"])
    while page["next_before_sequence"] is not None:
        cursor = page["next_before_sequence"]
        page = _read_history(scenario, limit=3, before_sequence=cursor)
        assert page["total"] == 10 and page["before_sequence"] == cursor
        assert len(page["cycles"]) <= 3 and page["untrusted"] is True
        assert all(cycle["sequence"] < cursor for cycle in page["cycles"])
        if page["next_before_sequence"] is not None:
            assert page["next_before_sequence"] == page["cycles"][0]["sequence"]
        recovered = page["cycles"] + recovered
    _assert_cycles(scenario, recovered, history)
    assert service.db.fetchone("SELECT COUNT(*) AS n FROM work_pauses")["n"] == 0
    assert len({output["digest"] for output in work["worker_outputs"] if output["digest"]}) == 1


def test_manual_pause_preserves_needs_work_review_and_exact_resume_condition(scenario) -> None:
    observed, decision = _pause(scenario)
    work = _assert_paused(scenario, observed)
    assert decision["kind"] == "pause"
    assert decision["reason"] == REASON
    assert decision["resume_condition"] == RESUME_CONDITION
    assert len(work["reviews"]) == 1 and work["reviews"][0]["verdict"] == "needs_work"
    assert _worker_instructions(scenario) == []
    assert (
        scenario["service"].read_worker_output(
            scenario["actor"], _read_request(work, observed["output"])
        )["content"]
        == OUTPUT
    )


def test_history_and_recall_references_do_not_fabricate_audited_content_reads(scenario) -> None:
    service = scenario["service"]
    event = _event(text=OUTPUT)
    output = service.observe_worker_output(**scenario["binding"], event=event)
    terminal = _terminal(service, scenario["binding"])
    work = service.get_work(scenario["work_id"])
    observed = {
        "work": work,
        "boundary": work["open_boundaries"][0],
        "output": output,
        "terminal": terminal,
        "event": event,
        "reviewed": False,
    }
    _assert_history(scenario, _read_history(scenario), [observed])
    memory = _assert_memory_context(
        scenario, service.get_work(scenario["work_id"], actor=scenario["actor"]), [observed]
    )
    assert "supervision_memory" not in work
    worker_work = service.get_work(scenario["work_id"], actor=scenario["worker"])
    assert "supervision_memory" not in worker_work
    _decision_request(scenario, observed, kind="pause")
    assert scenario["last_turn"]["supervision_memory"] == memory
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM events WHERE event_type='worker_output.read'"
        )["n"]
        == 0
    )
    read = service.read_worker_output(scenario["actor"], _read_request(work, output))
    assert read["content"] == OUTPUT and read["complete"] is True
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM events WHERE event_type='worker_output.read'"
        )["n"]
        == 1
    )


def test_cao_actor_can_read_prior_attempts_and_explicitly_choose_pause(scenario) -> None:
    """Script the supervisor's decision; do not claim to test model reasoning."""
    service = scenario["service"]
    history = []
    previous = None
    instructions = [INSTRUCTION] * 3
    for number, instruction in enumerate(instructions, 1):
        observed = _observe(scenario, review=number != 2)
        observed["expected_prior"] = previous
        history.append(observed)
        previous = _decide(
            scenario,
            observed,
            kind="continue" if number == 2 else "correct",
            instruction=instruction,
            reason=f"Independent rationale number {number}.",
        )
        assert previous["kind"] != "pause"
    observed = _observe(scenario)
    observed["expected_prior"] = previous
    history.append(observed)
    page = _read_history(scenario)
    _assert_history(scenario, page, history)
    assert page["cycles"][-1]["prior_instruction"]["instruction"] == instructions[-1]
    decision = _decide(
        scenario,
        observed,
        kind="pause",
        reason="The recorded attempts repeat one method without new verified evidence.",
    )
    _assert_paused(scenario, observed)
    assert decision["kind"] == "pause"
    assert len(_worker_instructions(scenario)) == 3
    quiet = _snapshot(scenario)
    for _ in range(3):
        service.reconcile_cao_supervision_obligations()
        service.reconcile_abandoned_worker_output_captures()
        assert Dispatcher(service, scenario["settings"])._claim_delivery() is None
    assert _snapshot(scenario) == quiet


@pytest.mark.parametrize("changed", ["instruction", "output"])
def test_full_history_distinguishes_changed_methods_and_results(scenario, changed) -> None:
    history = []
    previous = None
    for number in range(1, 5):
        different = number == 2
        observed = _observe(
            scenario,
            text="New independently verifiable intermediate evidence is available."
            if different and changed == "output"
            else OUTPUT,
        )
        observed["expected_prior"] = previous
        history.append(observed)
        previous = _decide(
            scenario,
            observed,
            instruction="Use the independent secondary method."
            if different and changed == "instruction"
            else INSTRUCTION,
        )
        assert previous["kind"] == "correct"
        _assert_history(scenario, _read_history(scenario), history)
    assert len(_worker_instructions(scenario)) == 4


def test_event_and_disposition_replays_do_not_duplicate_causal_history(scenario) -> None:
    service = scenario["service"]
    observed = _observe(scenario)
    request = _decision_request(scenario, observed)
    decision = service.dispose_boundary(scenario["actor"], observed["boundary"]["id"], request)
    observed["expected_decision"] = decision
    _assert_history(scenario, _read_history(scenario), [observed])
    snapshot = _snapshot(scenario)
    for _ in range(4):
        assert (
            service.observe_worker_output(**scenario["binding"], event=observed["event"])["id"]
            == observed["output"]["id"]
        )
        assert (
            service.dispose_boundary(scenario["actor"], observed["boundary"]["id"], request)
            == decision
        )
    assert _snapshot(scenario) == snapshot
    second = _observe(scenario)
    second["expected_prior"] = decision
    assert _decide(scenario, second)["kind"] == "correct"
    third = _observe(scenario)
    third["expected_prior"] = second["expected_decision"]
    assert _decide(scenario, third)["kind"] == "correct"
    _assert_history(scenario, _read_history(scenario), [observed, second, third])
    assert len(_worker_instructions(scenario)) == 3


def test_restart_and_authentic_reconnection_preserve_full_history(scenario) -> None:
    history = []
    previous = None
    for _ in range(2):
        observed = _observe(scenario)
        observed["expected_prior"] = previous
        history.append(observed)
        previous = _decide(scenario, observed)
    before = _read_history(scenario)
    scenario["service"] = ControlPlane(Database(scenario["settings"]), scenario["settings"])
    attachment = attach_cao_session_with_peer(
        scenario["service"],
        current_cao_session_attachment(
            native_thread_id="output-supervisor", project_digest="a" * 64
        ),
    )
    scenario["actor"] = scenario["service"].authenticate(str(attachment["context_token"]))
    assert _read_history(scenario) == before
    observed = _observe(scenario)
    observed["expected_prior"] = previous
    history.append(observed)
    assert _decide(scenario, observed)["kind"] == "correct"
    _assert_history(scenario, _read_history(scenario), history)
    assert len(_worker_instructions(scenario)) == 3


def test_history_cursor_stays_anchored_when_a_newer_cycle_arrives(scenario) -> None:
    first = _observe(scenario)
    first_decision = _decide(scenario, first)
    second = _observe(scenario)
    second["expected_prior"] = first_decision
    page = _read_history(scenario, limit=1)
    _assert_cycles(scenario, page["cycles"], [second])
    cursor = page["next_before_sequence"]
    assert cursor == page["cycles"][0]["sequence"]
    second_decision = _decide(scenario, second)
    third = _observe(scenario)
    third["expected_prior"] = second_decision
    older = _read_history(scenario, limit=1, before_sequence=cursor)
    assert older["total"] == 3
    assert older["before_sequence"] == cursor and older["next_before_sequence"] is None
    _assert_cycles(scenario, older["cycles"], [first])
    _assert_history(scenario, _read_history(scenario), [first, second, third])


@pytest.mark.parametrize("reader", ["worker", "foreign_conversation", "foreign_project"])
def test_full_history_cannot_expand_role_or_conversation_authority(scenario, reader) -> None:
    observed = _observe(scenario)
    service = scenario["service"]
    if reader == "worker":
        actor = scenario["worker"]
    else:
        attachment = attach_cao_session_with_peer(
            service,
            current_cao_session_attachment(
                native_thread_id=f"separate-history-{reader}",
                project_digest=("a" if reader == "foreign_conversation" else "b") * 64,
            ),
        )
        actor = service.authenticate(str(attachment["context_token"]))
    before = _snapshot(scenario)
    with pytest.raises(ControlPlaneError):
        service.read_work_history(actor, WorkHistoryReadInput(work_item_id=scenario["work_id"]))
    assert _snapshot(scenario) == before
    _assert_history(scenario, _read_history(scenario), [observed])


def test_full_history_is_scoped_to_the_exact_work_not_its_shared_worker(scenario) -> None:
    service = scenario["service"]
    observed, _ = _pause(scenario)
    original_history = _read_history(scenario)
    _assert_history(scenario, original_history, [observed])
    other = service.assign_work(
        scenario["actor"],
        WorkAssignment(
            worker_id=scenario["worker"]["id"],
            runtime_session_id=scenario["binding"]["runtime_id"],
            title="Separate supervision memory",
            objective="Return an independently reviewable bounded result.",
            acceptance=["The separate result is independently verified."],
            completion_contract="no_artifact_expected",
            idempotency_key="separate-supervision-memory",
        ),
    )
    other_case = {
        **scenario,
        "work_id": other["id"],
        "initial_attempt_id": other["current_attempt"]["id"],
        "initial_generation": other["generation"],
        "round": 0,
    }
    _assert_history(other_case, _read_history(other_case), [])
    _advance_capture(other_case)
    other_observed = _observe(other_case)
    _assert_history(other_case, _read_history(other_case), [other_observed])
    assert _read_history(scenario) == original_history
    _assert_paused(scenario, observed)


def test_full_history_retains_all_goal_versions_without_a_current_goal_cutoff(scenario) -> None:
    service = scenario["service"]
    first = _observe(scenario)
    decision = _decide(scenario, first)
    second = _observe(scenario)
    second["expected_prior"] = decision
    original = service.get_work(scenario["work_id"])
    _assert_history(scenario, _read_history(scenario), [first, second])
    revised = service.revise_goal(
        scenario["actor"],
        scenario["work_id"],
        GoalRevision(
            expected_version=original["goal_version"],
            objective="Verify a distinct replacement acceptance condition.",
            maturity="defined",
            acceptance=["The replacement condition has direct independent evidence."],
            reason="The intended observable result changed explicitly.",
            idempotency_key="replace-supervision-goal",
        ),
    )
    assert revised["goal_version"] == original["goal_version"] + 1
    _assert_history(scenario, _read_history(scenario), [first, second])
    observed = _observe(scenario)
    work = service.get_work(scenario["work_id"])
    page = _read_history(scenario)
    _assert_history(scenario, page, [first, second, observed])
    assert len({cycle["goal_version"] for cycle in page["cycles"]}) == 2
    _assert_memory_context(
        scenario,
        service.get_work(scenario["work_id"], actor=scenario["actor"]),
        [first, second, observed],
    )
    assert observed["boundary"]["goal_version"] == revised["goal_version"]
    assert {boundary["id"] for boundary in work["boundaries"]} == {
        first["boundary"]["id"],
        second["boundary"]["id"],
        observed["boundary"]["id"],
    }
    assert {goal["version"] for goal in work["goal_history"]} == {
        original["goal_version"],
        revised["goal_version"],
    }
    assert {output["id"] for output in work["worker_outputs"]} >= {
        first["output"]["id"],
        second["output"]["id"],
    }


def test_older_rich_memory_survives_later_noise_reconnect_goal_and_attempt_changes(
    scenario,
) -> None:
    """Exercise retrieval and scripted CAO authority, not a model-judgment oracle."""
    service = scenario["service"]
    first = _observe(scenario)
    value = (
        "Earlier independent verification found that repeating the same method did not "
        "change the available evidence. Preserve the observed result, identify a distinct "
        "prerequisite, and check it before choosing another instruction. "
    ) * 6 + "quartzvalueonlyneedle records the detailed comparison beyond the short abstraction."
    assert len(value) > 280 and "quartzvalueonlyneedle" not in value[:280]
    lesson = service.remember_memory(
        scenario["actor"],
        MemoryWriteInput(
            work_item_id=scenario["work_id"],
            primary_abstraction="Independent supervisor review of a bounded answer",
            cue_anchors=[
                "review-entry-anchor",
                "shared-method-evidence",
                "independent supervisor review",
            ],
            value=value,
            idempotency_key="older-detailed-supervision-lesson",
        ),
    )
    related = service.remember_memory(
        scenario["actor"],
        MemoryWriteInput(
            work_item_id=scenario["work_id"],
            primary_abstraction="Alternative verification experience",
            cue_anchors=["shared-method-evidence"],
            value="A second archived comparison explains a distinct verification method.",
            idempotency_key="related-supervision-lesson",
        ),
    )
    irrelevant_ids = set()
    for number in range(7):
        irrelevant = service.remember_memory(
            scenario["actor"],
            MemoryWriteInput(
                work_item_id=scenario["work_id"],
                primary_abstraction=f"Harbor buoy maintenance chronology {number}",
                cue_anchors=[f"anemometer-taxonomy-{number}"],
                value=f"A separate historical classification entry has ordinal {number}.",
                idempotency_key=f"later-unrelated-supervision-lesson-{number}",
            ),
        )
        irrelevant_ids.add(irrelevant["memory_id"])
    assert len(irrelevant_ids) == 7
    assert (
        lesson["scope"] == "conversation" and lesson["source_work_item_id"] == scenario["work_id"]
    )
    search = service.search_memories(
        scenario["actor"], MemorySearchInput(query="review-entry-anchor")
    )
    assert {item["memory_id"] for item in search["memories"]} == {lesson["memory_id"]}
    assert all("value" not in item for item in search["memories"])
    expanded = service.search_memories(
        scenario["actor"],
        MemorySearchInput(query="unmatched-navigation-token", related_to=lesson["memory_id"]),
    )
    related_hit = next(
        item for item in expanded["memories"] if item["memory_id"] == related["memory_id"]
    )
    assert "cue-related" in related_hit["matched_by"]
    assert "shared-method-evidence" in related_hit["matched_cues"]
    assert not irrelevant_ids.intersection(item["memory_id"] for item in expanded["memories"])
    assert (
        service.search_memories(
            scenario["actor"], MemorySearchInput(query="quartzvalueonlyneedle")
        )["total"]
        == 0
    )
    audits_before = service.db.fetchone(
        "SELECT COUNT(*) AS n FROM events WHERE event_type='worker_output.read'"
    )["n"]
    partial = service.read_memory(
        scenario["actor"],
        MemoryReadInput(
            memory_id=lesson["memory_id"], expected_revision=lesson["revision"], max_chars=128
        ),
    )
    assert partial["content"] == value[:128] and partial["complete"] is False
    assert partial["next_character_offset"] == 128 and partial["untrusted"] is True
    full_request = MemoryReadInput(
        memory_id=lesson["memory_id"], expected_revision=lesson["revision"], max_chars=16000
    )
    full = service.read_memory(scenario["actor"], full_request)
    assert full["content"] == value and full["complete"] is True and full["untrusted"] is True
    assert full["total_characters"] == len(value) and full["next_character_offset"] is None
    assert full["value_digest"] == hashlib.sha256(value.encode()).hexdigest()
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM events WHERE event_type='worker_output.read'"
        )["n"]
        == audits_before
    )
    memory = _assert_memory_context(
        scenario, service.get_work(scenario["work_id"], actor=scenario["actor"]), [first]
    )
    assert lesson["memory_id"] in {item["memory_id"] for item in memory["recall"]["memories"]}
    assert value not in json.dumps(memory)
    _decide(
        scenario,
        first,
        kind="pause",
        reason="The recalled comparison warrants checking a changed condition before continuing.",
    )
    assert scenario["last_turn"]["supervision_memory"] == memory
    _assert_paused(scenario, first)

    service = ControlPlane(Database(scenario["settings"]), scenario["settings"])
    scenario["service"] = service
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="output-supervisor", project_digest="a" * 64
        ),
    )
    scenario["actor"] = service.authenticate(str(attachment["context_token"]))
    assert service.read_memory(scenario["actor"], full_request)["content"] == value
    _assert_paused(scenario, first)
    resume_request = _resume_request(scenario)
    resumed = service.resume_work(scenario["actor"], scenario["work_id"], resume_request)
    assert resumed["current_attempt"]["id"] != scenario["initial_attempt_id"]
    second = _observe(scenario)
    second["expected_prior"] = {
        "instruction": resume_request.instruction,
        "reason": resume_request.reason,
    }
    revised = service.revise_goal(
        scenario["actor"],
        scenario["work_id"],
        GoalRevision(
            expected_version=resumed["goal_version"],
            objective="Use independent supervisor review to verify a changed bounded answer.",
            maturity="defined",
            acceptance=["The changed answer has independently verified evidence."],
            reason="The intended result now includes a distinct verification condition.",
            idempotency_key="revise-goal-with-retained-lesson",
        ),
    )
    third = _observe(scenario)
    history = [first, second, third]
    _assert_history(scenario, _read_history(scenario), history)
    assert third["boundary"]["goal_version"] == revised["goal_version"]
    assert len({item["boundary"]["attempt_id"] for item in history}) == 3
    memory = _assert_memory_context(
        scenario, service.get_work(scenario["work_id"], actor=scenario["actor"]), history
    )
    assert lesson["memory_id"] in {item["memory_id"] for item in memory["recall"]["memories"]}
    assert service.read_memory(scenario["actor"], full_request)["content"] == value
    _decide(
        scenario,
        third,
        kind="pause",
        reason="The preserved comparison requires another explicit prerequisite check.",
    )
    assert scenario["last_turn"]["supervision_memory"] == memory
    paused = _assert_paused(scenario, third)
    for _ in range(2):
        service.reconcile_abandoned_worker_output_captures()
        service.reconcile_completed_worker_turns()
        service.reconcile_cao_supervision_obligations()
    assert service.get_work(scenario["work_id"])["generation"] == paused["generation"]
    _assert_paused(scenario, third)
    assert service.read_memory(scenario["actor"], full_request)["content"] == value
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM messages m JOIN message_deliveries d ON d.message_id=m.id "
            "WHERE m.work_item_id=? AND m.kind IN ('assignment','instruction','status_request') "
            "AND d.state IN ('queued','leased','dispatched')",
            (scenario["work_id"],),
        )["n"]
        == 0
    )


@pytest.mark.parametrize("operation", ["reply", "retry", "revise"])
def test_ordinary_commands_cannot_consume_a_pause_or_partially_change_routes(
    scenario, operation
) -> None:
    _pause(scenario)
    service = scenario["service"]
    before = _snapshot(scenario)
    with pytest.raises(ConflictError):
        if operation == "reply":
            service.reply(
                scenario["actor"],
                scenario["work_id"],
                INSTRUCTION,
                idempotency_key="reply-to-paused",
            )
        elif operation == "retry":
            service.create_attempt(
                scenario["actor"],
                scenario["work_id"],
                reason=REASON,
                idempotency_key="retry-paused",
            )
        else:
            work = service.get_work(scenario["work_id"])
            service.revise_goal(
                scenario["actor"],
                scenario["work_id"],
                GoalRevision(
                    expected_version=work["goal_version"],
                    objective="A different exact goal.",
                    maturity="defined",
                    acceptance=["The new condition is verified."],
                    reason=REASON,
                    idempotency_key="revise-paused",
                ),
            )
    assert _snapshot(scenario) == before


def test_explicit_resume_preserves_work_and_goal_and_dispatches_once(scenario) -> None:
    observed, _ = _pause(scenario)
    service = scenario["service"]
    paused = service.get_work(scenario["work_id"])
    request = _resume_request(scenario)
    resumed = service.resume_work(scenario["actor"], scenario["work_id"], request)
    assert resumed["id"] == paused["id"]
    assert resumed["goal_version"] == paused["goal_version"]
    assert resumed["generation"] == paused["generation"] + 1
    assert resumed["state"] == "active" and resumed["attention_owner"] == "worker"
    assert resumed.get("paused_boundary_id") is None
    assert resumed.get("supervision_pause") is None
    assert resumed["current_attempt"]["id"] != paused["current_attempt"]["id"]
    assert resumed["current_attempt"]["completion_claim"] == {}
    assert resumed["current_attempt"]["worker_id"] == paused["current_attempt"]["worker_id"]
    replay_snapshot = _snapshot(scenario)
    assert service.resume_work(scenario["actor"], scenario["work_id"], request) == resumed
    assert _snapshot(scenario) == replay_snapshot
    receipt = service.db.fetchone(
        "SELECT * FROM work_pause_resumptions WHERE boundary_id = ?", (observed["boundary"]["id"],)
    )
    assert receipt is not None and receipt["resume_evidence"] == request.resume_evidence
    command = service.db.fetchone(
        "SELECT payload_json FROM messages WHERE id = ?", (receipt["message_id"],)
    )
    assert request.instruction in json.dumps(json.loads(command["payload_json"]))
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM messages m JOIN message_deliveries d ON d.message_id=m.id WHERE m.work_item_id=? AND m.kind IN ('assignment','instruction') AND d.state='queued'",
            (scenario["work_id"],),
        )["n"]
        == 1
    )


@pytest.mark.parametrize(
    "invalid", ["stale_generation", "wrong_boundary", "foreign_attachment", "worker_role"]
)
def test_resume_rejects_inexact_authority_without_side_effects(scenario, invalid) -> None:
    _pause(scenario)
    service = scenario["service"]
    request = _resume_request(scenario)
    actor = scenario["actor"]
    if invalid == "stale_generation":
        request = request.model_copy(
            update={"expected_generation": request.expected_generation - 1}
        )
    elif invalid == "wrong_boundary":
        request = request.model_copy(update={"pause_boundary_id": "bnd_unrelated_pause"})
    elif invalid == "foreign_attachment":
        actor = _attached(service, "foreign-pause")
    else:
        actor = scenario["worker"]
    before = _snapshot(scenario)
    with pytest.raises(ControlPlaneError):
        service.resume_work(actor, scenario["work_id"], request)
    assert _snapshot(scenario) == before


def test_changed_payload_cannot_reuse_a_committed_resume_key(scenario) -> None:
    _pause(scenario)
    service = scenario["service"]
    request = _resume_request(scenario)
    service.resume_work(scenario["actor"], scenario["work_id"], request)
    before = _snapshot(scenario)
    with pytest.raises(ConflictError):
        service.resume_work(
            scenario["actor"],
            scenario["work_id"],
            request.model_copy(update={"resume_evidence": "A different evidence claim."}),
        )
    assert _snapshot(scenario) == before


def test_resumption_keeps_prior_history_and_correlates_the_new_attempt_instruction(
    scenario,
) -> None:
    history = []
    previous = None
    for number in range(1, 4):
        observed = _observe(scenario)
        observed["expected_prior"] = previous
        history.append(observed)
        previous = _decide(scenario, observed, kind="pause" if number == 3 else "correct")
    _assert_paused(scenario, observed)
    request = _resume_request(scenario)
    scenario["service"].resume_work(scenario["actor"], scenario["work_id"], request)
    observed = _observe(scenario)
    observed["expected_prior"] = {"instruction": request.instruction, "reason": request.reason}
    history.append(observed)
    assert observed["work"]["current_attempt"]["id"] != scenario["initial_attempt_id"]
    page = _read_history(scenario)
    _assert_history(scenario, page, history)
    assert page["total"] == 4
    assert len({cycle["attempt_id"] for cycle in page["cycles"]}) == 2
    assert page["cycles"][-2]["decision"]["kind"] == "pause"
    assert page["cycles"][-1]["prior_instruction"]["kind"] == "assignment"
    assert page["cycles"][-1]["prior_instruction"]["instruction"] == request.instruction


@pytest.mark.parametrize("same_key", [True, False])
def test_concurrent_resumes_consume_one_pause_and_create_one_successor(scenario, same_key) -> None:
    _pause(scenario)
    service = scenario["service"]
    request = _resume_request(scenario)
    ready = Barrier(2)

    def resume(ordinal: int) -> dict[str, Any] | ControlPlaneError:
        selected = (
            request
            if same_key
            else request.model_copy(update={"idempotency_key": f"resume-race-{ordinal}"})
        )
        ready.wait(timeout=5)
        try:
            return service.resume_work(scenario["actor"], scenario["work_id"], selected)
        except ControlPlaneError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(resume, [0, 1]))
    successes = [outcome for outcome in outcomes if isinstance(outcome, dict)]
    failures = [outcome for outcome in outcomes if isinstance(outcome, ControlPlaneError)]
    assert len(successes) == (2 if same_key else 1)
    assert len(failures) == (0 if same_key else 1)
    if same_key:
        assert successes[0] == successes[1]
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM work_pause_resumptions WHERE work_item_id=?",
            (scenario["work_id"],),
        )["n"]
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS n FROM attempts WHERE work_item_id=?", (scenario["work_id"],)
        )["n"]
        == 2
    )


@pytest.mark.parametrize(
    "unsettled", ["busy", "starting", "dispatched", "leased", "effect_started", "effect_unknown"]
)
def test_pause_refuses_unsettled_execution_and_preserves_unknown_evidence(
    scenario, unsettled
) -> None:
    service = scenario["service"]
    observed = _observe(scenario)
    request = _decision_request(scenario, observed, kind="pause")
    if unsettled in {"busy", "starting"}:
        service.db.execute(
            "UPDATE runtime_sessions SET state=? WHERE id=?",
            (unsettled, scenario["binding"]["runtime_id"]),
        )
    elif unsettled in {"dispatched", "leased"}:
        message_id = _seed_worker_input(scenario, "instruction")
        service.db.execute(
            "UPDATE message_deliveries SET state=?, owner_token='pending-input-owner', lease_until=?, attempts=1 WHERE message_id=?",
            (unsettled, utc_after(300), message_id),
        )
    else:
        now = utc_now()
        service.db.execute(
            "INSERT INTO effect_operations(id,principal_id,kind,target,action,status,evidence,cleanup_work_item_id,created_at,updated_at) VALUES('eff_pause_pending',?,'external','opaque-target','opaque-action',?,'',?,?,?)",
            (
                scenario["worker"]["id"],
                unsettled.removeprefix("effect_"),
                scenario["work_id"],
                now,
                now,
            ),
        )
    before = _snapshot(scenario)
    with pytest.raises(ConflictError):
        service.dispose_boundary(scenario["actor"], observed["boundary"]["id"], request)
    assert _snapshot(scenario) == before


@pytest.mark.parametrize("kind", ["instruction", "status_request"])
def test_paused_worker_inputs_cannot_dispatch_after_restart_or_heartbeat(scenario, kind) -> None:
    observed, _ = _pause(scenario)
    service = scenario["service"]
    message_id = _seed_worker_input(scenario, kind)
    service = ControlPlane(Database(scenario["settings"]), scenario["settings"])
    scenario["service"] = service
    launch = service.issue_runtime_launch_ticket(scenario["binding"]["runtime_id"])
    exchange = service.exchange_runtime_launch_ticket(str(launch["ticket"]))
    worker = service.authenticate(str(exchange["token"]))
    service.record_mcp_tool_discovery(
        worker, protocol_version="2025-06-18", tool_names=WORKER_MCP_REQUIRED_TOOLS
    )
    service.heartbeat_runtime(
        worker,
        scenario["binding"]["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"], sequence=1
        ),
    )
    dispatcher = Dispatcher(service, scenario["settings"])
    for _ in range(3):
        service.expire_runtime_leases()
        service.reconcile_abandoned_worker_output_captures()
        service.reconcile_completed_worker_turns()
        service.reconcile_cao_supervision_obligations()
        assert dispatcher._claim_delivery() is None
    _assert_paused(scenario, observed)
    delivery = service.db.fetchone(
        "SELECT state, acknowledged_at, handled_at FROM message_deliveries WHERE message_id=?",
        (message_id,),
    )
    assert delivery["state"] == "queued"
    assert delivery["acknowledged_at"] is None and delivery["handled_at"] is None


def test_pause_does_not_ack_unread_terminal_notification(scenario) -> None:
    service = scenario["service"]
    observed = _observe(scenario, acknowledge=False)
    _decide(scenario, observed, kind="pause")
    _assert_paused(scenario, observed)
    message_id = observed["terminal"]["notification_message_id"]
    delivery = service.db.fetchone(
        "SELECT * FROM message_deliveries WHERE message_id=?", (message_id,)
    )
    assert delivery["state"] == "queued"
    assert delivery["acknowledged_at"] is None and delivery["handled_at"] is None
    assert message_id in {item["id"] for item in service.get_inbox(scenario["actor"])["items"]}
    service.acknowledge(scenario["actor"], AckInput(message_ids=[message_id]))
    assert (
        service.mark_message_handled(
            scenario["actor"], message_id, evidence="The exact pause was read after its decision."
        )["state"]
        == "handled"
    )


def test_late_output_cannot_restart_or_hide_a_pause(scenario) -> None:
    observed, _ = _pause(scenario)
    service = scenario["service"]
    before = _snapshot(scenario)
    with pytest.raises(ConflictError):
        service.observe_worker_output(
            **scenario["binding"], event=replace(observed["event"], item_id="late-unrelated-item")
        )
    assert _snapshot(scenario) == before
    _assert_paused(scenario, observed)


def test_an_independent_work_on_the_same_worker_does_not_consume_the_pause(scenario) -> None:
    observed, _ = _pause(scenario)
    service = scenario["service"]
    other = service.assign_work(
        scenario["actor"],
        WorkAssignment(
            worker_id=scenario["worker"]["id"],
            runtime_session_id=scenario["binding"]["runtime_id"],
            title="Independent exact Work",
            objective="Produce a separate bounded result.",
            acceptance=["The independent result is verified."],
            idempotency_key="independent-of-pause",
        ),
    )
    assert other["id"] != scenario["work_id"] and other["state"] == "active"
    _assert_paused(scenario, observed)
