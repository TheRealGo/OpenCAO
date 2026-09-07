from __future__ import annotations

import asyncio

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from enrollment_helpers import EnrollmentHandshakeAdapter, EnrollmentHandshakeRegistry
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.errors import StaleGenerationError, StaleGoalError
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
)
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryInput,
    DeliveryResolveInput,
    GoalRevision,
    IntentDisposition,
    ReportInput,
    RequesterDecisionInput,
    ReviewInput,
    RuntimeDispatchResult,
    SubmittedIntent,
    WorkAssignment,
)
from cao_control_plane.runtime import Dispatcher


def _assignment(system, title: str, runtime_id: str | None = None) -> WorkAssignment:
    return WorkAssignment(
        worker_id=system["worker"]["id"],
        title=title,
        objective=f"Complete {title}",
        acceptance=[f"{title} is verified"],
        runtime_session_id=runtime_id,
    )


def _received_task(system, source_id: str, title: str, runtime_id: str | None = None) -> dict:
    receipt = system["service"]._receive_canonical_intent_for_compat(
        system["cao"],
        SubmittedIntent(
            source_id=source_id,
            payload={"type": "canonical_cao_command", "title": title},
        ),
    )
    return system["service"]._classify_canonical_intent_for_compat(
        system["cao"],
        receipt["intent"]["id"],
        IntentDisposition(
            kind="task",
            relation="independent",
            assignment=_assignment(system, title, runtime_id),
            reason="authoritative request received",
        ),
    )["work"]


def _attached_cao_credential(system: dict) -> tuple[dict, str]:
    """Create the CAO capability that owns the requester conversation."""

    attachment = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id="autonomous-supervision-e2e",
            project_digest="a" * 64,
            model="gpt-5.6-terra",
            sandbox="workspace-write",
        ),
    )
    ticket = system["service"].issue_cao_runtime_launch_ticket(attachment["runtime_session_id"])
    credential = system["service"].exchange_cao_runtime_launch_ticket(ticket["ticket"])
    return attachment, credential["token"]


def _mcp_call(
    client: TestClient,
    token: str,
    name: str,
    arguments: dict,
    request_id: int,
) -> dict:
    response = client.post(
        "/mcp",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": MCP_LATEST_VERSION,
            "Mcp-Method": "tools/call",
            "Mcp-Name": name,
        },
        json={
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments,
                "_meta": {
                    PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                    CLIENT_CAPABILITIES_META_KEY: {},
                    CLIENT_INFO_META_KEY: {"name": "e2e", "version": "1"},
                },
            },
        },
    )
    assert response.status_code == 200, response.text
    assert "error" not in response.json(), response.text
    return response.json()["result"]["structuredContent"]


def test_raw_requester_http_ingress_is_absent_and_canonical_interrupt_resumes_once(system):
    """Only a post-reasoning CAO command may create the durable interrupt."""

    service = system["service"]
    runtime = system["runtime"]
    original = _received_task(system, "e2e:original", "Original objective", runtime["id"])
    adapter: EnrollmentHandshakeAdapter

    def incorporate_assignment(_, message):
        if message.get("kind") != "assignment":
            return
        worker_actor = adapter.actors[-1]
        assignment = [
            item
            for item in service.get_inbox(worker_actor)["items"]
            if item["kind"] == "assignment"
        ]
        service.acknowledge(worker_actor, AckInput(message_ids=[assignment[0]["id"]]))
        service.mark_message_handled(
            worker_actor,
            assignment[0]["id"],
            evidence=(
                "Original task incorporated"
                if message.get("work_item_id") == original["id"]
                else "Urgent task incorporated"
            ),
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=incorporate_assignment)
    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
    )
    assert asyncio.run(dispatcher.run_once()) == 1

    app = create_app(system["settings"])
    client = TestClient(app, base_url=system["settings"].public_base_url)
    rejected = client.post(
        "/api/v1/intents",
        headers={"Authorization": f"Bearer {system['user_token']}"},
        json={"source_id": "e2e:urgent", "payload": {"text": "Urgent correction"}},
    )
    client.close()
    assert rejected.status_code == 404
    assert (
        service.db.fetchone("SELECT id FROM source_receipts WHERE source_id = ?", ("e2e:urgent",))
        is None
    )

    received = service._receive_canonical_intent_for_compat(
        system["cao"],
        SubmittedIntent(
            source_id="e2e:urgent",
            payload={"type": "canonical_cao_command", "title": "Urgent correction"},
        ),
    )
    intent_id = received["intent"]["id"]
    wake_delivery = service.db.fetchone(
        """
        SELECT d.message_id, d.state FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE d.recipient_id = ? AND m.payload_json LIKE ?
        """,
        (system["cao"]["id"], f"%{intent_id}%"),
    )
    assert wake_delivery is not None
    assert wake_delivery["state"] == "queued"
    classified = service._classify_canonical_intent_for_compat(
        system["cao"],
        intent_id,
        IntentDisposition(
            kind="task",
            relation="interrupt",
            target_work_item_id=original["id"],
            assignment=_assignment(system, "Urgent correction", runtime["id"]),
            reason="the delegated correction must be handled first",
        ),
    )
    interrupt = classified["work"]
    assert (
        service.db.fetchone(
            "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (wake_delivery["message_id"], system["cao"]["id"]),
        )["state"]
        == "handled"
    )

    persisted = service.db.fetchone(
        "SELECT id FROM source_receipts WHERE source_id = ?", ("e2e:urgent",)
    )
    assert persisted is not None
    assert service.get_work(original["id"])["state"] == "suspended"
    assert service.get_work(original["id"])["suspended_by_work_item_id"] == interrupt["id"]
    assert asyncio.run(dispatcher.run_once()) == 1
    interrupt_delivery = service.db.fetchone(
        """
        SELECT d.message_id, d.state FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE d.recipient_id = ? AND m.work_item_id = ? AND m.kind = 'assignment'
        """,
        (system["worker"]["id"], interrupt["id"]),
    )
    assert interrupt_delivery["state"] == "handled"

    service.cancel_work(
        system["cao"], interrupt["id"], "Urgent boundary handled", idempotency_key="e2e-resume"
    )
    service.cancel_work(
        system["cao"], interrupt["id"], "Urgent boundary handled", idempotency_key="e2e-resume"
    )
    resumed = service.get_work(original["id"])
    assert resumed["state"] == "active"
    assert resumed["suspended_by_work_item_id"] is None
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events WHERE event_type = 'work.resumed' AND aggregate_id = ?",
            (original["id"],),
        )["count"]
        == 1
    )
    assert asyncio.run(dispatcher.run_once()) == 1


def test_goal_revision_keeps_history_immutable_and_rejects_stale_versions_and_generations(system):
    service = system["service"]
    work = _received_task(system, "e2e:versioned", "Version one")
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="e2e:versioned-boundary",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="Version one is at a safe reasoning boundary",
            runtime_state="ready",
        ),
    )
    stale_turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=work["generation"],
        idempotency_key="e2e-stale-turn",
    )
    revised = service.revise_goal(
        system["cao"],
        work["id"],
        GoalRevision(
            expected_version=1,
            objective="Version two",
            maturity="defined",
            acceptance=["Version two is verified"],
            reason="a new accepted requirement supersedes version one",
        ),
    )

    assert [(item["version"], item["objective"]) for item in revised["goal_revisions"]] == [
        (1, "Complete Version one"),
        (2, "Version two"),
    ]
    with pytest.raises(StaleGoalError):
        service.revise_goal(
            system["cao"],
            work["id"],
            GoalRevision(
                expected_version=1,
                objective="Illegitimate stale rewrite",
                maturity="defined",
                acceptance=["Never stored"],
                reason="stale sender",
            ),
        )
    with pytest.raises(StaleGenerationError):
        service.acquire_reasoner_turn(
            system["cao"],
            work["id"],
            expected_generation=stale_turn["generation"],
            idempotency_key="e2e-reused-generation",
        )
    assert service.get_work(work["id"])["goal_revisions"][0]["objective"] == "Complete Version one"


def test_worker_mcp_completion_drives_attached_cao_requester_decision(system):
    service = system["service"]
    attachment, attached_cao_token = _attached_cao_credential(system)
    app = create_app(system["settings"])
    client = TestClient(app, base_url=system["settings"].public_base_url)
    try:
        # The existing cao_assign tool schema does not yet carry attachment
        # binding fields, so create this CAO-owned work through the service
        # facade.  The product interaction under test begins at the Worker MCP
        # report and remains attached through the requester decision.
        work = service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                requester_id=system["user"]["id"],
                supervisor_attachment_id=attachment["id"],
                supervisor_project_digest=attachment["project_digest"],
                title="MCP completion",
                objective="Complete the attached CAO MCP workflow",
                acceptance=["The requester decision is durable"],
                idempotency_key="e2e-attached-mcp-assignment",
            ),
        )
        attempt = work["current_attempt"]
        reported = _mcp_call(
            client,
            system["worker_token"],
            "cao_report",
            {
                "attempt_id": attempt["id"],
                "kind": "completion_claim",
                "expected_goal_version": work["goal_version"],
                "expected_goal_packet_digest": attempt["goal_packet_digest"],
                "expected_task_packet_digest": attempt["task_packet_digest"],
                "expected_generation": work["generation"],
                "summary": "Worker completed the requested MCP-only path",
                "trajectory": "complete",
                "evidence": [{"check": "protocol-e2e", "result": "pass"}],
                "idempotency_key": "e2e-worker-mcp-completion",
            },
            1,
        )
        assert reported["state"] == "waiting_supervisor"
        boundary = reported["open_boundaries"][0]
        delivery = service.db.fetchone(
            """
            SELECT d.*
            FROM message_deliveries AS d
            JOIN messages AS m ON m.id = d.message_id
            WHERE d.recipient_id = ? AND m.work_item_id = ?
              AND json_extract(m.payload_json, '$.boundary_id') = ?
            """,
            (system["cao"]["id"], work["id"], boundary["id"]),
        )
        assert delivery is not None
        assert delivery["state"] == "queued"

        # The durable boundary delivery is pinned to the exact originating
        # CAO conversation runtime.  The attached credential below therefore
        # acts on the same conversation-scoped delivery rather than bypassing
        # a generic CAO inbox.
        assert delivery["runtime_session_id"] == attachment["runtime_session_id"]
        turn = _mcp_call(
            client,
            attached_cao_token,
            "cao_acquire_reasoner_turn",
            {
                "work_item_id": work["id"],
                "boundary_id": boundary["id"],
                "expected_generation": reported["generation"],
                "idempotency_key": "e2e-mcp-cao-turn",
            },
            2,
        )
        _mcp_call(
            client,
            attached_cao_token,
            "cao_dispose_boundary",
            {
                "boundary_id": boundary["id"],
                "turn_id": turn["id"],
                "lease_token": turn["lease_token"],
                "expected_generation": reported["generation"],
                "kind": "accept",
                "reason": "MCP report evidence is ready for CAO review",
            },
            3,
        )
        reviewed = _mcp_call(
            client,
            attached_cao_token,
            "cao_review",
            {
                "attempt_id": attempt["id"],
                "verdict": "ok",
                "summary": "CAO reviewed the Worker MCP completion evidence",
                "idempotency_key": "e2e-mcp-review",
            },
            4,
        )
        decided = _mcp_call(
            client,
            attached_cao_token,
            "cao_record_requester_decision",
            RequesterDecisionInput(
                review_id=reviewed["reviews"][0]["id"],
                verdict="accepted",
                summary="Requester accepts the reviewed result in this CAO conversation",
                evidence=[{"check": "protocol-e2e", "result": "pass"}],
                conversation_evidence_id="e2e-mcp-requester-decision",
                idempotency_key="e2e-mcp-requester-decision",
            ).model_dump(mode="json"),
            5,
        )
    finally:
        client.close()

    assert decided["state"] == "completed"
    assert decided["requester_decisions"][0]["verdict"] == "accepted"
    assert service.get_work(work["id"])["state"] == "completed"


def test_worker_claim_cao_review_and_attached_requester_decision_are_separate_stages(system):
    service = system["service"]
    attachment, attached_cao_token = _attached_cao_credential(system)
    attached_cao = service.authenticate(attached_cao_token)
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
            title="Separate authorities",
            objective="Keep CAO review and requester decision distinct",
            acceptance=["The attached CAO records the requester decision"],
            idempotency_key="e2e-attached-authority-split",
        ),
    )
    claimed = service.report(
        system["worker"],
        work["current_attempt"]["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Worker claims the requested evidence is complete",
            evidence=[{"check": "e2e", "result": "pass"}],
            idempotency_key="e2e-completion-claim",
        ),
    )
    assert claimed["state"] == "waiting_supervisor"
    completion = claimed["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        attached_cao,
        work["id"],
        expected_generation=claimed["generation"],
        idempotency_key="e2e-completion-disposition",
    )
    service.dispose_boundary(
        attached_cao,
        completion["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=claimed["generation"],
            kind="accept",
            reason="claim is ready for independent review",
        ),
    )
    waiting_review = service.get_work(work["id"])
    assert waiting_review["state"] == "waiting_review"
    reviewed = service.review(
        attached_cao,
        ReviewInput(
            attempt_id=work["current_attempt"]["id"],
            verdict="ok",
            summary="CAO independently verified the evidence",
            idempotency_key="e2e-cao-review",
        ),
    )
    assert reviewed["state"] == "waiting_user"
    assert reviewed["reviews"][0]["verdict"] == "ok"
    decided = service.record_requester_decision(
        attached_cao,
        RequesterDecisionInput(
            review_id=reviewed["reviews"][0]["id"],
            verdict="accepted",
            summary="Requester accepts the independently reviewed result",
            evidence=[{"check": "e2e", "result": "pass"}],
            conversation_evidence_id="e2e-attached-authority-decision",
            idempotency_key="e2e-attached-requester-decision",
        ),
    )
    assert decided["state"] == "completed"
    assert decided["requester_decisions"][0]["verdict"] == "accepted"


def test_boundary_delivery_is_durable_and_post_invocation_crash_requires_resolution(system):
    service = system["service"]
    runtime = system["runtime"]
    work = _received_task(system, "e2e:delivery", "Durable dispatch", runtime["id"])
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="e2e:worker-boundary",
            work_item_id=work["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=work["current_attempt"]["goal_packet_digest"],
            expected_task_packet_digest=work["current_attempt"]["task_packet_digest"],
            expected_generation=work["generation"],
            kind="idle",
            summary="Worker reached a durable supervision boundary",
            runtime_state="ready",
        ),
    )
    cao_delivery = service.db.fetchone(
        """
        SELECT d.state FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE d.recipient_id = ? AND m.work_item_id = ?
          AND json_extract(m.payload_json, '$.boundary_id') = ?
        """,
        (system["cao"]["id"], work["id"], boundary["id"]),
    )
    assert cao_delivery["state"] == "queued"

    class CrashAfterInvocationAdapter(EnrollmentHandshakeAdapter):
        async def dispatch(self, runtime, message):
            await super().dispatch(runtime, message)
            delivery = service.db.fetchone(
                """
                SELECT d.state FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                WHERE d.recipient_id = ? AND m.kind = 'assignment'
                """,
                (system["worker"]["id"],),
            )
            assert delivery["state"] == "dispatched"
            return RuntimeDispatchResult(success=False, state="failed", error="simulated crash")

    class CrashRegistry:
        def get(self, name):
            assert name == "claude"
            return CrashAfterInvocationAdapter(service)

    dispatcher = Dispatcher(service, system["settings"], registry=CrashRegistry())
    # An unattached CAO thread is a durable pending boundary, not a guessed
    # legacy dispatch target.  Only the pinned Worker delivery is invoked.
    assert asyncio.run(dispatcher.run_once()) == 1
    worker_delivery = service.db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE d.recipient_id = ? AND m.kind = 'assignment'
        """,
        (system["worker"]["id"],),
    )
    assert worker_delivery["state"] == "dispatched"
    assert asyncio.run(dispatcher.run_once()) == 0

    service.resolve_delivery(
        system["cao"],
        worker_delivery["message_id"],
        DeliveryResolveInput(
            recipient_id=system["worker"]["id"],
            outcome="not_delivered",
            evidence="runtime history has no matching delivery after simulated crash",
        ),
    )
    assert asyncio.run(Dispatcher(service, system["settings"]).run_once()) == 0
    assert dict(
        service.db.fetchone(
            "SELECT state, last_error FROM message_deliveries "
            "WHERE message_id = ? AND recipient_id = ?",
            (worker_delivery["message_id"], system["worker"]["id"]),
        )
    ) == {
        "state": "dead",
        "last_error": "system_reconciliation_required",
    }
    assert service.get_runtime(runtime["id"])["state"] == "failed"
