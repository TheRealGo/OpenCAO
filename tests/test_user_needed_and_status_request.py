from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError as PydanticValidationError

import cao_control_plane.service as service_module
from cao_control_plane.database import SCHEMA_VERSION
from cao_control_plane.errors import ConflictError
from cao_control_plane.errors import ValidationError as ControlPlaneValidationError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    ReportInput,
    StatusRequestInput,
    WorkAssignment,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime import Dispatcher


def _assign(system: dict, suffix: str) -> dict:
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title=f"Supervisor state contract {suffix}",
            objective="Keep supervisor state durable and actionable.",
            acceptance=["The public Work projection explains the next action."],
            idempotency_key=f"supervisor-state:{suffix}",
        ),
    )


def _correct_with_worker_instruction(system: dict, suffix: str) -> tuple[dict, dict]:
    service = system["service"]
    work = _assign(system, suffix)
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="A bounded correction is required.",
            idempotency_key=f"{suffix}:question",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key=f"{suffix}:turn",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=waiting["generation"],
            kind="correct",
            reason="The Worker needs one exact correction.",
            instruction="Produce the corrected durable report.",
        ),
    )
    instruction = service.db.fetchone(
        "SELECT message.id, message.sequence FROM messages AS message "
        "WHERE message.work_item_id = ? AND message.attempt_id = ? "
        "AND message.kind = 'instruction' ORDER BY message.sequence DESC LIMIT 1",
        (work["id"], attempt["id"]),
    )
    assert instruction is not None
    service.acknowledge(
        system["worker"],
        AckInput(message_ids=[str(instruction["id"])]),
    )
    return work, dict(instruction)


def _report_corrected_progress(
    system: dict,
    work: dict,
    suffix: str,
    *,
    incorporated_message_ids: list[str] | None = None,
) -> dict:
    attempt = work["current_attempt"]
    return system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="progress",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The exact correction was incorporated.",
            stage="corrected",
            next_boundary="completion",
            incorporated_message_ids=incorporated_message_ids or [],
            idempotency_key=f"{suffix}:progress",
        ),
    )


def test_later_worker_report_handles_acknowledged_instruction_and_unblocks_lane(
    system: dict,
) -> None:
    service = system["service"]
    work, instruction = _correct_with_worker_instruction(
        system,
        "incorporated-instruction",
    )

    _report_corrected_progress(
        system,
        work,
        "incorporated-instruction",
        incorporated_message_ids=[str(instruction["id"])],
    )

    delivery = service.db.fetchone(
        "SELECT state, handled_at FROM message_deliveries WHERE message_id = ?",
        (instruction["id"],),
    )
    assert delivery is not None
    assert delivery["state"] == "handled"
    assert delivery["handled_at"]
    events = service.db.fetchall(
        "SELECT event_type, data_json FROM events WHERE aggregate_id = ? "
        "AND event_type = 'message.handled'",
        (instruction["id"],),
    )
    assert len(events) == 1
    assert json.loads(str(events[0]["data_json"]))["reason_code"] == (
        "worker_report_incorporated_instruction"
    )

    requested = service.request_status(
        system["cao"],
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            summary="Report the next observable result.",
            response_due_seconds=60,
            idempotency_key="incorporated-instruction:status",
        ),
    )
    inbox = service.get_inbox(system["worker"])["items"]
    assert [item["id"] for item in inbox] == [
        requested["current_attempt"]["activity"]["status_request"]["message_id"]
    ]


@pytest.mark.parametrize("report_kind", ["progress", "artifact"])
def test_unrelated_report_does_not_handle_acknowledged_instruction(
    system: dict,
    report_kind: str,
) -> None:
    service = system["service"]
    suffix = f"unrelated-instruction-{report_kind}"
    work, instruction = _correct_with_worker_instruction(
        system,
        suffix,
    )
    if report_kind == "progress":
        _report_corrected_progress(system, work, suffix)
    else:
        attempt = work["current_attempt"]
        content = b"unrelated artifact"
        service.report(
            system["worker"],
            attempt["id"],
            ReportInput(
                kind="artifact",
                expected_goal_version=work["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=work["generation"],
                summary="This artifact does not claim to incorporate the instruction.",
                artifacts=[
                    ArtifactInput(
                        name="unrelated.txt",
                        uri="data:text/plain,unrelated%20artifact",
                        media_type="text/plain",
                        digest=hashlib.sha256(content).hexdigest(),
                    )
                ],
                idempotency_key=f"{suffix}:artifact",
            ),
        )

    delivery = service.db.fetchone(
        "SELECT state, handled_at FROM message_deliveries WHERE message_id = ?",
        (instruction["id"],),
    )
    assert delivery is not None
    assert delivery["state"] == "acknowledged"
    assert delivery["handled_at"] is None

    requested = service.request_status(
        system["cao"],
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            summary="Report the next observable result.",
            response_due_seconds=60,
            idempotency_key=f"{suffix}:status",
        ),
    )
    assert service.get_inbox(system["worker"])["items"] == []
    assert requested["current_attempt"]["activity"]["status_request"]["state"] == ("pending")


def test_incorporated_instruction_validation_is_exact_and_rolls_back(
    system: dict,
) -> None:
    service = system["service"]
    work, instruction = _correct_with_worker_instruction(
        system,
        "exact-instruction-validation",
    )
    other = _assign(system, "other-instruction-validation")
    other_attempt = other["current_attempt"]

    with pytest.raises(ConflictError, match="not an instruction for this task packet"):
        service.report(
            system["worker"],
            other_attempt["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=other["goal_version"],
                expected_goal_packet_digest=other_attempt["goal_packet_digest"],
                expected_task_packet_digest=other_attempt["task_packet_digest"],
                expected_generation=other["generation"],
                summary="This report must not consume another Attempt's instruction.",
                incorporated_message_ids=[str(instruction["id"])],
                idempotency_key="other-instruction-validation:progress",
            ),
        )

    attempt = work["current_attempt"]
    with pytest.raises(
        ControlPlaneValidationError,
        match="inline artifact could not be staged",
    ):
        service.report(
            system["worker"],
            attempt["id"],
            ReportInput(
                kind="artifact",
                expected_goal_version=work["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=work["generation"],
                summary="A failed report must not consume the instruction.",
                artifacts=[
                    ArtifactInput(
                        name="mismatched.txt",
                        uri="data:text/plain,actual-content",
                        media_type="text/plain",
                        digest=hashlib.sha256(b"different-content").hexdigest(),
                    )
                ],
                incorporated_message_ids=[str(instruction["id"])],
                idempotency_key="exact-instruction-validation:failed-artifact",
            ),
        )

    delivery = service.db.fetchone(
        "SELECT state, handled_at FROM message_deliveries WHERE message_id = ?",
        (instruction["id"],),
    )
    assert delivery is not None
    assert delivery["state"] == "acknowledged"
    assert delivery["handled_at"] is None
    artifact_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM artifacts WHERE attempt_id = ?",
        (attempt["id"],),
    )
    assert artifact_count is not None and artifact_count["count"] == 0


@pytest.mark.parametrize(
    "updates",
    [
        {"reason": ""},
        {"instruction": ""},
        {"resume_condition": ""},
    ],
)
def test_wait_user_requires_reason_decision_and_resume_condition(
    updates: dict[str, str],
) -> None:
    values = {
        "turn_id": "turn_contract",
        "lease_token": "lease-contract",
        "expected_generation": 1,
        "kind": "wait_user",
        "reason": "Only the requester can choose the release policy.",
        "instruction": "Choose whether to publish the reviewed artifact.",
        "resume_condition": "Resume after the requester records publish or hold.",
        **updates,
    }

    with pytest.raises(PydanticValidationError):
        BoundaryDispositionInput.model_validate(values)


def test_wait_user_projects_the_exact_requester_decision(system: dict) -> None:
    service = system["service"]
    work = _assign(system, "structured-user-needed")
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The requester must choose the release policy.",
            idempotency_key="structured-user-needed:question",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key="structured-user-needed:turn",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=waiting["generation"],
            kind="wait_user",
            reason="Only the requester can choose the release policy.",
            instruction="Choose whether to publish the reviewed artifact.",
            resume_condition="Resume after the requester records publish or hold.",
        ),
    )

    projected = service.get_work(work["id"])

    assert projected["state"] == "user_needed"
    assert projected["attention_owner"] == "user"
    assert projected["current_attempt"]["state"] == "input_required"
    assert projected["user_needed"]["decision"] == (
        "Choose whether to publish the reviewed artifact."
    )
    assert projected["user_needed"]["reason"] == (
        "Only the requester can choose the release policy."
    )
    assert projected["user_needed"]["resume_condition"] == (
        "Resume after the requester records publish or hold."
    )


def test_non_wait_boundary_disposition_replays_a_pre_resume_condition_digest(
    system: dict,
) -> None:
    service = system["service"]
    work = _assign(system, "legacy-disposition-digest")
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="question",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Continue with the already sealed goal.",
            idempotency_key="legacy-disposition-digest:question",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key="legacy-disposition-digest:turn",
    )
    request = BoundaryDispositionInput(
        turn_id=turn["id"],
        lease_token=turn["lease_token"],
        expected_generation=waiting["generation"],
        kind="correct",
        reason="Continue the same sealed goal with this bounded instruction.",
        instruction="Continue with the next observable result.",
    )
    disposed = service.dispose_boundary(system["cao"], boundary["id"], request)
    legacy_fields = request.model_dump(mode="json")
    legacy_fields.pop("resume_condition", None)
    legacy_digest = hashlib.sha256(
        json.dumps(
            legacy_fields,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    updated = service.db.execute(
        "UPDATE boundary_dispositions SET request_digest = ? WHERE id = ?",
        (legacy_digest, disposed["id"]),
    )
    assert updated == 1

    replayed = service.dispose_boundary(system["cao"], boundary["id"], request)

    assert replayed["id"] == disposed["id"]


def test_schema_v25_reclassifies_incomplete_user_needed_to_cao_supervision(
    system: dict,
) -> None:
    service = system["service"]
    work = _assign(system, "v24-incomplete-user-needed")
    attempt_id = work["current_attempt"]["id"]
    with service.db.transaction() as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            ("work_items_user_needed_contract_update",),
        ).fetchone()
        assert trigger is not None and trigger["sql"]
        connection.execute("DROP TRIGGER work_items_user_needed_contract_update")
        connection.execute(
            "UPDATE work_items SET state = 'user_needed', attention_owner = 'user', "
            "user_needed_boundary_id = NULL WHERE id = ?",
            (work["id"],),
        )
        connection.execute(
            "UPDATE attempts SET state = 'input_required' WHERE id = ?",
            (attempt_id,),
        )
        connection.execute("UPDATE metadata SET value = '24' WHERE key = 'schema_version'")
        connection.execute("PRAGMA user_version = 24")
        connection.execute(str(trigger["sql"]))

    service.db.initialize()

    version = service.db.fetchone("SELECT value FROM metadata WHERE key = 'schema_version'")
    migration = service.db.fetchone("SELECT description FROM schema_migrations WHERE version = 25")
    projected = service.get_work(work["id"])
    assert version is not None and version["value"] == str(SCHEMA_VERSION)
    assert migration is not None
    assert projected["state"] == "waiting_supervisor"
    assert projected["attention_owner"] == "cao"
    assert projected["current_attempt"]["state"] == "waiting_supervisor"
    assert projected.get("user_needed") is None
    assert len(projected["open_boundaries"]) == 1
    assert projected["open_boundaries"][0]["metadata"] == {
        "next_action": "cao_continue_prior",
        "reason": "incomplete_user_needed_contract",
        "system_recovery": True,
    }


def test_status_request_is_boundary_free_durable_and_idempotent(system: dict) -> None:
    service = system["service"]
    work = _assign(system, "durable-status-request")
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="artifact",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Recorded the first observable result.",
            artifacts=[
                ArtifactInput(
                    name="status-evidence.json",
                    uri="artifact:status-evidence",
                    media_type="application/json",
                    digest="a" * 64,
                )
            ],
            idempotency_key="durable-status-request:artifact",
        ),
    )
    assert service.get_work(work["id"])["open_boundaries"] == []
    request = StatusRequestInput(
        expected_generation=work["generation"],
        summary="Report the current activity and next observable result.",
        response_due_seconds=60,
        idempotency_key="durable-status-request:request",
    )

    service.request_status(system["cao"], work["id"], request)
    service.request_status(system["cao"], work["id"], request)

    rows = service.db.fetchall(
        """
        SELECT message.id, message.attempt_id, message.payload_json,
               delivery.recipient_id, delivery.runtime_session_id, delivery.state
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ? AND message.kind = 'status_request'
        """,
        (work["id"],),
    )
    assert len(rows) == 1
    status_message = rows[0]
    payload = json.loads(status_message["payload_json"])
    assert status_message["attempt_id"] == attempt["id"]
    assert status_message["recipient_id"] == system["worker"]["id"]
    assert status_message["runtime_session_id"] == system["runtime"]["id"]
    assert status_message["state"] == "queued"
    assert payload["summary"] == request.summary
    assert payload["response_due_at"]

    projected = service.get_work(work["id"])
    activity = projected["current_attempt"]["activity"]
    # The fixture heartbeat belongs to the enrollment epoch before this
    # Attempt and must not be presented as current liveness.
    assert activity["runtime_heartbeat_at"] == ""
    assert activity["last_worker_activity_at"] == ""
    assert activity["last_artifact_at"]
    assert activity["status_request"]["message_id"] == status_message["id"]
    assert activity["status_request"]["state"] == "pending"
    assert activity["status_request"]["requested_at"]
    assert activity["status_request"]["response_due_at"]
    assert activity["status_request"]["responded_at"] == ""

    dashboard = build_projection(service.db).snapshot
    dashboard_item = next(item for item in dashboard["work_items"] if item["id"] == work["id"])[
        "operator_content"
    ]
    assert dashboard_item["runtime_heartbeat_at"] is None
    assert dashboard_item["last_worker_activity_at"] is None
    assert dashboard_item["last_artifact_at"]
    assert dashboard_item["status_request_state"] == "pending"
    assert dashboard_item["status_response_due_at"]

    with pytest.raises(ConflictError):
        service.request_status(
            system["cao"],
            work["id"],
            request.model_copy(update={"response_due_seconds": 61}),
        )

    tool = next(
        item
        for item in MCPServer(service).tools_for(system["cao"])
        if item["name"] == "cao_request_status"
    )
    assert {
        "work_item_id",
        "expected_generation",
        "idempotency_key",
    }.issubset(tool["inputSchema"]["required"])
    assert tool["inputSchema"]["properties"]["response_due_seconds"] == {
        "type": "integer",
        "minimum": 30,
        "maximum": 86400,
    }
    report_tool = next(
        item
        for item in MCPServer(service).tools_for(system["worker"])
        if item["name"] == "cao_report"
    )
    incorporated = report_tool["inputSchema"]["properties"]["incorporated_message_ids"]
    assert incorporated["type"] == "array"
    assert incorporated["maxItems"] == 1
    assert incorporated["uniqueItems"] is True
    assert incorporated["items"]["pattern"] == r"^msg_[A-Za-z0-9]{1,124}$"


def test_artifact_report_does_not_answer_status_request(system: dict) -> None:
    service = system["service"]
    work = _assign(system, "artifact-is-not-status-response")
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="progress",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Started the first observable step.",
            trajectory="advancing",
            stage="initial_activity",
            next_boundary="artifact",
            idempotency_key="artifact-is-not-status-response:initial-progress",
        ),
    )
    activity_before = service.get_work(work["id"])["current_attempt"]["activity"][
        "last_worker_activity_at"
    ]
    service.request_status(
        system["cao"],
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            summary="Report current activity and the next observable result.",
            response_due_seconds=3600,
            idempotency_key="artifact-is-not-status-response:request",
        ),
    )
    status_item = next(
        item
        for item in service.get_inbox(system["worker"])["items"]
        if item["kind"] == "status_request"
    )

    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="artifact",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Registered an artifact without answering the status request.",
            artifacts=[
                ArtifactInput(
                    name="artifact-only.json",
                    uri="artifact:artifact-only-status-evidence",
                    media_type="application/json",
                    digest="b" * 64,
                )
            ],
            idempotency_key="artifact-is-not-status-response:artifact",
        ),
    )

    projected = service.get_work(work["id"])
    activity = projected["current_attempt"]["activity"]
    assert activity["last_worker_activity_at"] == activity_before
    assert activity["last_artifact_at"]
    assert activity["status_request"]["state"] == "pending"
    assert activity["status_request"]["responded_at"] == ""
    dashboard_item = next(
        item
        for item in build_projection(service.db).snapshot["work_items"]
        if item["id"] == work["id"]
    )["operator_content"]
    assert dashboard_item["latest_report_kind"] == "artifact"
    assert dashboard_item["last_worker_activity_at"] == activity_before
    assert dashboard_item["last_artifact_at"]
    assert dashboard_item["status_request_state"] == "pending"
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (status_item["id"], system["worker"]["id"]),
    )
    assert delivery is not None and delivery["state"] != "handled"


@pytest.mark.parametrize(
    "summary",
    [
        "Read /Users/alice/private/token.txt before reporting status.",
        "Use api_key=sk-secret-example before reporting status.",
    ],
)
def test_status_request_rejects_private_worker_payload_before_persistence(
    system: dict, summary: str
) -> None:
    service = system["service"]
    work = _assign(system, "private-status-request")

    with pytest.raises(ControlPlaneValidationError):
        service.request_status(
            system["cao"],
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary=summary,
                response_due_seconds=60,
                idempotency_key=f"private-status-request:{len(summary)}",
            ),
        )

    count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM messages WHERE work_item_id = ? AND kind = 'status_request'",
        (work["id"],),
    )
    assert count is not None and count["count"] == 0


def _latest_status_request_id(service, attempt_id: str) -> str:
    row = service.db.fetchone(
        "SELECT id, payload_json FROM messages "
        "WHERE attempt_id = ? AND kind = 'status_request' "
        "ORDER BY sequence DESC LIMIT 1",
        (attempt_id,),
    )
    assert row is not None
    return str(row["id"])


def test_overdue_unclaimed_assignment_opens_one_runtime_recovery_boundary(
    system: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = system["service"]
    work = _assign(system, "overdue-unclaimed-assignment")
    attempt = work["current_attempt"]
    service.request_status(
        system["cao"],
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            summary="Confirm that the first Assignment reached a fresh runtime.",
            response_due_seconds=30,
            idempotency_key="overdue-unclaimed-assignment:status",
        ),
    )
    status_message_id = _latest_status_request_id(service, attempt["id"])
    monkeypatch.setattr(service_module, "utc_now", lambda: "2999-01-01T00:00:00Z")

    assert service.recover_overdue_unstarted_attempts() == 1
    assert service.recover_overdue_unstarted_attempts() == 0

    recovered = service.get_work(work["id"])
    assert recovered["state"] == "waiting_supervisor"
    assert recovered["attention_owner"] == "cao"
    assert recovered["current_attempt"]["state"] == "waiting_supervisor"
    assert recovered["current_attempt"]["trajectory"] == "stalled"
    assert recovered["current_attempt"]["stage"] == "system_reconciliation"
    assert recovered["current_attempt"]["next_boundary"] == "system_reconciliation"
    assert len(recovered["open_boundaries"]) == 1
    boundary = recovered["open_boundaries"][0]
    assert boundary["kind"] == "failure"
    assert boundary["recovery_action"] == "system_reconciliation"
    assert boundary["metadata"] == {
        "pre_dispatch_timeout": True,
        "reason": "runtime_dispatch_failed",
        "runtime_recovery": True,
    }
    deliveries = service.db.fetchall(
        """
        SELECT message.kind, delivery.state, delivery.last_error
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind IN ('assignment', 'status_request')
          AND delivery.recipient_id = ?
        ORDER BY message.sequence
        """,
        (attempt["id"], system["worker"]["id"]),
    )
    assert [(row["kind"], row["state"], row["last_error"]) for row in deliveries] == [
        ("assignment", "dead", "system_reconciliation_required"),
        ("status_request", "dead", "system_reconciliation_required"),
    ]
    assert status_message_id in {
        str(row["message_id"])
        for row in service.db.fetchall(
            "SELECT message_id FROM message_deliveries WHERE state = 'dead'"
        )
    }
    boundary_count = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM boundaries WHERE attempt_id = ?",
        (attempt["id"],),
    )
    assert boundary_count is not None and boundary_count["count"] == 1
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=recovered["generation"],
        idempotency_key="overdue-unclaimed-assignment:terminal-review",
    )
    disposition = service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=recovered["generation"],
            kind=BoundaryDispositionKind.FAIL,
            reason="The unclaimed Assignment exceeded its bounded delivery deadline.",
        ),
    )
    assert disposition["kind"] == "fail"
    assert service.get_work(work["id"])["state"] == "failed"

    # Defense in depth: even if a stale database writer re-queues the old
    # Assignment after recovery, the Dispatcher must bind launchability to the
    # exact active Work/Attempt generation rather than execute it.
    stale_assignment = service.db.fetchone(
        "SELECT delivery.message_id, delivery.recipient_id "
        "FROM messages AS message JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (attempt["id"],),
    )
    assert stale_assignment is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = 'queued', next_attempt_at = ?, "
        "last_error = '' WHERE message_id = ? AND recipient_id = ?",
        (
            "2000-01-01T00:00:00Z",
            stale_assignment["message_id"],
            stale_assignment["recipient_id"],
        ),
    )
    assert Dispatcher(service, system["settings"])._claim_delivery() is None


@pytest.mark.parametrize("claimed_state", ["leased", "dispatched"])
def test_overdue_status_never_reinterprets_a_claimed_assignment(
    system: dict, claimed_state: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    work = _assign(system, f"overdue-{claimed_state}-assignment")
    attempt = work["current_attempt"]
    service.request_status(
        system["cao"],
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            summary="Confirm the already claimed Assignment outcome.",
            response_due_seconds=30,
            idempotency_key=f"overdue-{claimed_state}-assignment:status",
        ),
    )
    _latest_status_request_id(service, attempt["id"])
    monkeypatch.setattr(service_module, "utc_now", lambda: "2999-01-01T00:00:00Z")
    assignment = service.db.fetchone(
        "SELECT delivery.message_id, delivery.recipient_id FROM messages AS message "
        "JOIN message_deliveries AS delivery ON delivery.message_id = message.id "
        "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
        (attempt["id"],),
    )
    assert assignment is not None
    service.db.execute(
        "UPDATE message_deliveries SET state = ? WHERE message_id = ? AND recipient_id = ?",
        (claimed_state, assignment["message_id"], assignment["recipient_id"]),
    )

    assert service.recover_overdue_unstarted_attempts() == 0

    unchanged = service.get_work(work["id"])
    assert unchanged["state"] == "active"
    assert unchanged["current_attempt"]["state"] == "assigned"
    assert unchanged["open_boundaries"] == []
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (assignment["message_id"], assignment["recipient_id"]),
    )
    assert delivery is not None and delivery["state"] == claimed_state


def test_status_request_becomes_overdue_then_responded(
    system: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = system["service"]
    work = _assign(system, "status-request-lifecycle")
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="progress",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The Worker started the first observable step.",
            trajectory="advancing",
            stage="initial_activity",
            next_boundary="status_check",
            idempotency_key="status-request-lifecycle:initial-progress",
        ),
    )
    service.request_status(
        system["cao"],
        work["id"],
        StatusRequestInput(
            expected_generation=work["generation"],
            response_due_seconds=30,
            idempotency_key="status-request-lifecycle:request",
        ),
    )
    status_item = next(
        item
        for item in service.get_inbox(system["worker"])["items"]
        if item["kind"] == "status_request"
    )

    with monkeypatch.context() as clock:
        clock.setattr(service_module, "_now_dt", lambda: datetime(9999, 1, 1, tzinfo=UTC))
        overdue = service.get_work(work["id"])
    assert overdue["current_attempt"]["activity"]["status_request"]["state"] == ("overdue")

    service.acknowledge(system["worker"], AckInput(message_ids=[status_item["id"]]))
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="progress",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The Worker is active and the next observable result is ready.",
            trajectory="advancing",
            stage="status_response",
            next_boundary="next_artifact",
            idempotency_key="status-request-lifecycle:response",
        ),
    )

    responded = service.get_work(work["id"])
    status = responded["current_attempt"]["activity"]["status_request"]
    assert status["message_id"] == status_item["id"]
    assert status["state"] == "responded"
    assert status["responded_at"]
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (status_item["id"], system["worker"]["id"]),
    )
    assert delivery is not None and delivery["state"] == "handled"
