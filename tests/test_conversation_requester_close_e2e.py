"""Acceptance contract for requester decisions recorded in an attached CAO conversation.

These tests deliberately specify the cutover API rather than reaching into
SQLite.  The requester remains the authority for the decision, while the CAO
runtime that owns the existing conversation records that decision with durable
conversation evidence.  No separate User MCP client is part of this flow.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.close_contract import CleanupTargetKind
from cao_control_plane.dashboard import DashboardReadModel
from cao_control_plane.database import utc_now
from cao_control_plane.errors import AuthorizationError, ConflictError
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
)
from cao_control_plane.models import (
    AckInput,
    ArtifactInput,
    BoundaryDispositionInput,
    PrincipalCreate,
    PrincipalRole,
    ReportInput,
    ReportKind,
    ReviewInput,
    WorkAssignment,
    WorkClosePreparationInput,
)

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_DIGEST_C = hashlib.sha256(b"conversation-close-result").hexdigest()


def _mcp_request(method: str, *, request_id: int, **params: Any) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": {
            **params,
            "_meta": {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {"name": "conversation-close-e2e", "version": "1"},
            },
        },
    }


def _mcp_headers(token: str, method: str, *, name: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": MCP_LATEST_VERSION,
        "Mcp-Method": method,
    }
    if name:
        headers["Mcp-Name"] = name
    return headers


def _tools(client: TestClient, token: str) -> dict[str, dict[str, Any]]:
    response = client.post(
        "/mcp",
        headers=_mcp_headers(token, "tools/list"),
        json=_mcp_request("tools/list", request_id=1),
    )
    assert response.status_code == 200, response.text
    return {tool["name"]: tool for tool in response.json()["result"]["tools"]}


def _call(
    client: TestClient, token: str, name: str, arguments: dict[str, Any], request_id: int
) -> dict[str, Any]:
    response = client.post(
        "/mcp",
        headers=_mcp_headers(token, "tools/call", name=name),
        json=_mcp_request("tools/call", request_id=request_id, name=name, arguments=arguments),
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert "error" not in payload, payload
    return dict(payload["result"]["structuredContent"])


def _call_error(
    client: TestClient,
    token: str,
    name: str,
    arguments: dict[str, Any],
    request_id: int,
) -> dict[str, Any]:
    response = client.post(
        "/mcp",
        headers=_mcp_headers(token, "tools/call", name=name),
        json=_mcp_request("tools/call", request_id=request_id, name=name, arguments=arguments),
    )
    assert response.status_code == 400, response.text
    payload = response.json()
    assert "error" in payload, payload
    assert payload["error"]["code"] == -32602
    return dict(payload["error"])


def _call_tool_error(
    client: TestClient,
    token: str,
    name: str,
    arguments: dict[str, Any],
    request_id: int,
) -> dict[str, Any]:
    response = client.post(
        "/mcp",
        headers=_mcp_headers(token, "tools/call", name=name),
        json=_mcp_request("tools/call", request_id=request_id, name=name, arguments=arguments),
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert "error" not in payload, payload
    assert payload["result"]["isError"] is True
    return dict(payload["result"]["structuredContent"]["error"])


def _call_is_denied(
    client: TestClient, token: str, name: str, arguments: dict[str, Any], request_id: int
) -> None:
    response = client.post(
        "/mcp",
        headers=_mcp_headers(token, "tools/call", name=name),
        json=_mcp_request("tools/call", request_id=request_id, name=name, arguments=arguments),
    )
    assert response.status_code == 400, response.text
    assert response.json()["error"]["code"] == -32602


def _contract_type(name: str) -> Any:
    """Keep the test collectable until the public model is implemented."""

    import cao_control_plane.models as models

    value = getattr(models, name, None)
    assert value is not None, f"missing required public model: cao_control_plane.models.{name}"
    return value


def _attached_credential(
    system: dict[str, Any], *, thread_id: str = "conversation-thread"
) -> tuple[dict[str, Any], dict[str, Any], str]:
    attachment = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id=thread_id,
            project_digest=_DIGEST_A,
            model="gpt-5.6-terra",
            sandbox="workspace-write",
        ),
    )
    ticket = system["service"].issue_cao_runtime_launch_ticket(attachment["runtime_session_id"])
    credential = system["service"].exchange_cao_runtime_launch_ticket(ticket["ticket"])
    return attachment, system["service"].authenticate(credential["token"]), credential["token"]


def _reviewed_attached_work(
    system: dict[str, Any],
    attachment: dict[str, Any],
    cao_runtime: dict[str, Any],
    *,
    reviewer: dict[str, Any] | None = None,
    managed_thread: dict[str, str] | None = None,
    idempotency_prefix: str = "conversation-close",
    artifact_uri: str = "data:text/plain,conversation-close-result",
) -> dict[str, Any]:
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            runtime_session_id=system["runtime"]["id"],
            managed_worker_thread_id=(
                managed_thread["thread_id"] if managed_thread is not None else None
            ),
            managed_worker_thread_generation=(1 if managed_thread is not None else None),
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
            title="Conversation-close acceptance",
            objective="Finish through the attached CAO conversation",
            acceptance=["A requester decision can be recorded without a User MCP runtime"],
        ),
    )
    assignment_inbox = service.get_inbox(system["worker"])
    assert len(assignment_inbox["items"]) == 1
    assignment_message_id = str(assignment_inbox["items"][0]["id"])
    service.acknowledge(system["worker"], AckInput(message_ids=[assignment_message_id]))
    service.mark_message_handled(
        system["worker"],
        assignment_message_id,
        evidence="The enrolled Worker accepted the assignment before reporting completion.",
    )
    attempt = work["current_attempt"]
    reported = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Worker completion is ready for CAO review",
            artifacts=[
                ArtifactInput(
                    name="result",
                    uri=artifact_uri,
                    media_type="text/plain",
                    digest=_DIGEST_C,
                )
            ],
            idempotency_key=f"{idempotency_prefix}:completion",
        ),
    )
    boundary = reported["open_boundaries"][0]
    inbox = service.get_inbox(cao_runtime)
    assert len(inbox["items"]) == 1
    completion_message_id = str(inbox["items"][0]["id"])
    service.acknowledge(cao_runtime, AckInput(message_ids=[completion_message_id]))
    turn = service.acquire_reasoner_turn(
        cao_runtime,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key=f"{idempotency_prefix}:review-turn",
    )
    service.dispose_boundary(
        cao_runtime,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind="accept",
            reason="The attached CAO conversation reviewed the Worker boundary",
        ),
    )
    service.mark_message_handled(
        cao_runtime,
        completion_message_id,
        evidence="The attached CAO conversation disposed the completion boundary.",
    )
    reviewed = service.review(
        reviewer or cao_runtime,
        ReviewInput(
            attempt_id=attempt["id"],
            verdict="ok",
            summary="CAO review is OK; requester decision remains separate",
            idempotency_key=f"{idempotency_prefix}:review",
        ),
    )
    review_inbox = service.get_inbox(cao_runtime)
    assert len(review_inbox["items"]) == 1
    review_message_id = str(review_inbox["items"][0]["id"])
    service.acknowledge(cao_runtime, AckInput(message_ids=[review_message_id]))
    service.mark_message_handled(
        cao_runtime,
        review_message_id,
        evidence="The attached CAO conversation recorded its completed review.",
    )
    return reviewed


def _seed_managed_thread_for_work(
    system: dict[str, Any], attachment: dict[str, Any]
) -> dict[str, str]:
    """Bind the fixture Worker to one exact logical thread for public Finish."""

    service = system["service"]
    enrollment = service.db.fetchone(
        "SELECT id FROM worker_enrollments WHERE runtime_session_id = ?",
        (system["runtime"]["id"],),
    )
    assert enrollment is not None
    suffix = "e2eclose000000000000000000000001"
    ids = {
        "spec_id": f"mws_{suffix}",
        "thread_id": f"mwt_{suffix}",
        "epoch_id": f"mwe_{suffix}",
    }
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO managed_worker_specs(
                id, attachment_id, attachment_generation, principal_id,
                runtime_session_id, enrollment_id, worker_profile_id, adapter,
                workspace_ref, requested_model, effective_model,
                requested_reasoning_effort, effective_reasoning_effort,
                provider_scope_digest, catalog_target_id, state,
                policy_binding_digest, input_digest, idempotency_key,
                created_at, updated_at, stopped_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'test-profile', 'claude',
                     'workspace-close-e2e', 'test-model', 'test-model',
                     'high', 'high', '', '', 'enabled', ?, ?, ?, ?, ?, NULL)
            """,
            (
                ids["spec_id"],
                attachment["id"],
                attachment["generation"],
                system["worker"]["id"],
                system["runtime"]["id"],
                enrollment["id"],
                "a" * 64,
                "b" * 64,
                "seed-public-close-e2e",
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO managed_worker_threads(
                id, managed_spec_id, state, generation,
                created_at, updated_at, archived_at
            ) VALUES(?, ?, 'active', 1, ?, ?, NULL)
            """,
            (ids["thread_id"], ids["spec_id"], now, now),
        )
        connection.execute(
            """
            INSERT INTO managed_worker_thread_epochs(
                id, thread_id, generation, runtime_session_id,
                enrollment_id, created_at, retired_at
            ) VALUES(?, ?, 1, ?, ?, ?, NULL)
            """,
            (
                ids["epoch_id"],
                ids["thread_id"],
                system["runtime"]["id"],
                enrollment["id"],
                now,
            ),
        )
    return ids


def _close_input(
    service: Any, actor: dict[str, Any], work: dict[str, Any], decision: dict[str, Any], key: str
) -> Any:
    WorkCloseInput = _contract_type("WorkCloseInput")
    attempt = work["current_attempt"]
    review = work["reviews"][-1]
    for kind in (
        CleanupTargetKind.WORKSPACE,
        CleanupTargetKind.TEMPORARY,
        CleanupTargetKind.LOG,
        CleanupTargetKind.BRANCH,
    ):
        service.owner_private_close_inventory.declare_not_applicable(
            work_item_id=work["id"], target_kind=kind
        )
    preparation = service.prepare_work_close(
        actor,
        WorkClosePreparationInput(
            work_item_id=work["id"],
            retention_policy_evidence_id="retention-v1",
            artifact_manifest_evidence_id="artifact-manifest-v1",
            idempotency_key=f"{key}:prepare",
        ),
    )
    return WorkCloseInput(
        work_item_id=work["id"],
        attempt_id=attempt["id"],
        review_id=review["id"],
        requester_decision_id=decision["id"],
        expected_goal_version=work["goal_version"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        expected_generation=work["generation"],
        retention_policy_evidence_id="retention-v1",
        artifact_manifest_evidence_id="artifact-manifest-v1",
        cleanup_inventory_evidence_id=preparation["id"],
        close_preparation_id=preparation["id"],
        artifacts=[
            {
                "artifact_id": work["artifacts"][0]["id"],
                "digest": _DIGEST_C,
                "evidence_id": "artifact-copy-verified",
            }
        ],
        # Cleanup outcomes are derived from canonical terminal state and
        # trusted owner-private effect receipts, never caller attestation.
        cleanup=[],
        idempotency_key=key,
    )


def test_attached_cao_mcp_records_requester_decision_without_user_runtime(system):
    """The CAO can record an explicit conversation decision without impersonating the user."""

    attachment, cao_runtime, _runtime_token = _attached_credential(system)
    reviewed = _reviewed_attached_work(system, attachment, cao_runtime)
    review = reviewed["reviews"][-1]
    # A later user turn resumes the exact same conversation in a new
    # short-lived CAO runtime credential epoch.  The wake ticket/token identity
    # fences that epoch without rotating the base attachment generation or
    # invalidating the already-open conversation CSC.
    system["service"].db.execute(
        "UPDATE runtime_sessions SET state = 'waiting' WHERE id = ?",
        (cao_runtime["_runtime_session_id"],),
    )
    ticket = system["service"].issue_cao_runtime_launch_ticket(cao_runtime["_runtime_session_id"])
    renewed = system["service"].exchange_cao_runtime_launch_ticket(ticket["ticket"])
    assert renewed["generation"] == review["supervisor_attachment_generation"]
    token = str(renewed["token"])
    app = create_app(system["settings"])

    with TestClient(app, base_url="http://localhost") as client:
        attached_tools = _tools(client, token)
        assert "cao_record_requester_decision" in attached_tools
        assert "cao_close_conversation" not in attached_tools
        assert {
            "cao_stop_work_runtime",
            "cao_close_work",
            "cao_prepare_work_close",
            "cao_execute_prepared_cleanup",
        }.isdisjoint(attached_tools)
        assert "cao_user_acceptance" not in attached_tools
        assert "cao_record_requester_decision" not in _tools(client, system["user_token"])
        assert "cao_close_conversation" not in _tools(client, system["worker_token"])
        _call_is_denied(
            client,
            system["user_token"],
            "cao_record_requester_decision",
            {"review_id": reviewed["reviews"][-1]["id"]},
            3,
        )
        _call_is_denied(
            client,
            system["worker_token"],
            "cao_close_conversation",
            {"idempotency_key": "worker-cannot-close-conversation"},
            4,
        )

        RequesterDecisionInput = _contract_type("RequesterDecisionInput")
        decision_request = RequesterDecisionInput(
            review_id=reviewed["reviews"][-1]["id"],
            verdict="accepted",
            summary="Requester explicitly accepted in the existing CAO conversation",
            evidence=[{"kind": "conversation", "result": "confirmed"}],
            conversation_evidence_id="conversation-decision-v1",
            idempotency_key="conversation-close:requester-decision",
        ).model_dump(mode="json")
        result = _call(
            client,
            token,
            "cao_record_requester_decision",
            decision_request,
            2,
        )
        assert (
            _call(
                client,
                token,
                "cao_record_requester_decision",
                decision_request,
                5,
            )
            == result
        )

    decision = result["requester_decisions"][-1]
    assert "requester_id" not in decision
    assert "recorded_by" not in decision
    assert "idempotency_key" not in decision
    assert decision["conversation_evidence_id"] == "conversation-decision-v1"
    stored = system["service"].db.fetchone(
        "SELECT requester_id, recorded_by FROM requester_decisions WHERE id = ?",
        (decision["id"],),
    )
    assert stored is not None
    assert stored["requester_id"] == system["user"]["id"]
    assert stored["recorded_by"] == system["cao"]["id"]
    assert result["state"] == "completed"
    assert result["closure"]["open_delivery_count"] == 0


def test_public_conversation_finishes_reviewed_work_without_guessing_requester_or_work(
    system: dict[str, Any],
) -> None:
    """One attached-CAO Finish closes reviewed Work without a manual pipeline."""

    service = system["service"]
    # Build the HTTP facade first so its release-catalog reconciliation cannot
    # revoke a CSC issued by the fixture afterward.
    app = create_app(system["settings"])
    attachment, cao_runtime, _runtime_token = _attached_credential(
        system, thread_id="public-finish-close"
    )
    thread = _seed_managed_thread_for_work(system, attachment)
    reviewed = _reviewed_attached_work(
        system,
        attachment,
        cao_runtime,
        managed_thread=thread,
        idempotency_prefix="public-finish-close",
    )
    work_id = str(reviewed["id"])
    # The worker is quiescent, but its reviewed Work remains waiting for the
    # requester.  Explicit Finish itself is the requester-authorized Close;
    # no cleanup inventory, requester-decision, or Work-close IDs are inputs.
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE runtime_credentials SET state = 'revoked', revoked_at = ?, "
            "updated_at = ? WHERE enrollment_id = (SELECT id FROM worker_enrollments "
            "WHERE runtime_session_id = ?)",
            (now, now, system["runtime"]["id"]),
        )
        connection.execute(
            "UPDATE runtime_sessions SET state = 'waiting', updated_at = ? WHERE id = ?",
            (now, system["runtime"]["id"]),
        )

    csc_token = str(attachment["context_token"])
    other = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="public-finish-close-other",
            project_digest="d" * 64,
            model="gpt-5.6-terra",
            sandbox="workspace-write",
        ),
    )
    with TestClient(app, base_url="http://localhost") as client:
        tools = _tools(client, csc_token)
        assert "cao_finish_worker_thread" in tools
        assert {
            "cao_stop_work_runtime",
            "cao_prepare_work_close",
            "cao_execute_prepared_cleanup",
            "cao_close_work",
        }.isdisjoint(tools)
        assert {
            "cao_record_requester_decision",
            "cao_finish_worker_thread",
        } <= set(tools)
        hidden_before = (
            str(
                service.db.fetchone(
                    "SELECT state FROM runtime_sessions WHERE id = ?",
                    (system["runtime"]["id"],),
                )["state"]
            ),
            int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
        )
        for request_id, hidden_name in enumerate(
            (
                "cao_stop_work_runtime",
                "cao_prepare_work_close",
                "cao_execute_prepared_cleanup",
                "cao_close_work",
            ),
            start=620,
        ):
            hidden = _call_error(
                client,
                csc_token,
                hidden_name,
                {"work_item_id": work_id},
                request_id,
            )
            assert hidden["message"] == f"Unknown tool: {hidden_name}"
        assert (
            str(
                service.db.fetchone(
                    "SELECT state FROM runtime_sessions WHERE id = ?",
                    (system["runtime"]["id"],),
                )["state"]
            ),
            int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
        ) == hidden_before
        assert (
            "recorded_requester_id"
            not in tools["cao_record_requester_decision"]["inputSchema"]["properties"]
        )

        # An unbound administrator and another conversation receive no
        # authority over this exact Work.
        assert "cao_finish_worker_thread" not in _tools(client, system["cao_token"])
        _call_is_denied(
            client,
            system["cao_token"],
            "cao_finish_worker_thread",
            {
                "worker_thread_id": thread["thread_id"],
                "expected_generation": 1,
                "idempotency_key": "public-finish-close:unbound",
            },
            600,
        )
        cross = _call_tool_error(
            client,
            str(other["context_token"]),
            "cao_finish_worker_thread",
            {
                "worker_thread_id": thread["thread_id"],
                "expected_generation": 1,
                "idempotency_key": "public-finish-close:cross",
            },
            601,
        )
        assert cross["code"] == "not_found"

        artifact_before = tuple(
            service.db.fetchone("SELECT * FROM artifacts WHERE work_item_id = ?", (work_id,))
        )
        finish_arguments = {
            "worker_thread_id": thread["thread_id"],
            "expected_generation": 1,
            "idempotency_key": "public-finish-close:finish",
        }
        archived = _call(
            client,
            csc_token,
            "cao_finish_worker_thread",
            finish_arguments,
            602,
        )
        assert archived == {
            "worker_thread_id": thread["thread_id"],
            "state": "archived",
            "generation": 2,
        }
        assert service.get_work(work_id)["state"] == "canceled"
        assert (
            tuple(service.db.fetchone("SELECT * FROM artifacts WHERE work_item_id = ?", (work_id,)))
            == artifact_before
        )
        listed = _call(client, csc_token, "cao_list_managed_workers", {}, 603)
        retained = next(
            worker
            for worker in listed["workers"]
            if worker["worker_thread_id"] == thread["thread_id"]
        )
        assert retained["state"] == "archived"
        assert retained["generation"] == 2

        public_results = json.dumps([archived, retained], sort_keys=True)
        for private_fragment in (
            system["user"]["id"],
            system["runtime"]["id"],
            "enrollment_id",
            "attachment_id",
            "workspace-close-e2e",
            "grant_id",
        ):
            assert private_fragment not in public_results

        # Even an exact idempotent Finish replay cannot pass after its CSC is
        # revoked; the transaction-local freshness fence runs before cache use.
        service.db.execute(
            "UPDATE cao_conversation_credentials SET state = 'revoked', "
            "revoked_at = ?, updated_at = ? WHERE attachment_id = ?",
            (utc_now(), utc_now(), attachment["id"]),
        )
        stale = client.post(
            "/mcp",
            headers=_mcp_headers(csc_token, "tools/call", name="cao_finish_worker_thread"),
            json=_mcp_request(
                "tools/call",
                request_id=604,
                name="cao_finish_worker_thread",
                arguments=finish_arguments,
            ),
        )
        assert stale.status_code == 401


def test_attached_work_review_requires_its_authenticated_conversation(
    system: dict[str, Any],
) -> None:
    """A CAO administrator bearer cannot create an off-thread terminal review."""

    attachment, cao_runtime, _token = _attached_credential(system, thread_id="thread-denied")
    with pytest.raises(AuthorizationError, match="originating CAO conversation"):
        _reviewed_attached_work(
            system,
            attachment,
            cao_runtime,
            reviewer=system["cao"],
            idempotency_prefix="thread-denied",
        )

    attached, attached_runtime, _token = _attached_credential(system, thread_id="thread-accepted")
    reviewed = _reviewed_attached_work(
        system, attached, attached_runtime, idempotency_prefix="thread-accepted"
    )
    review = reviewed["reviews"][-1]
    assert review["supervisor_attachment_id"] == attached["id"]
    assert (
        review["supervisor_attachment_generation"] == attached_runtime["_cao_attachment_generation"]
    )


def test_cao_runtime_credential_is_confined_to_mcp(system: dict[str, Any]) -> None:
    """A runtime credential can resume its thread but cannot become an admin bearer."""

    attachment, cao_runtime, token = _attached_credential(system, thread_id="crc-scope")
    with pytest.raises(AuthorizationError, match="runtime credential cannot administer principals"):
        system["service"].create_principal(
            cao_runtime,
            PrincipalCreate(name="crc-must-not-create", role=PrincipalRole.WORKER),
        )

    app = create_app(system["settings"])
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(app, base_url="http://localhost") as client:
        tools = _tools(client, token)
        assert "cao_review" in tools
        assert _call(client, token, "cao_get_inbox", {}, 402)["items"] == []

        assert (
            client.post(
                "/api/v1/principals",
                headers=headers,
                json={"name": "crc-http-create", "role": "worker"},
            ).status_code
            == 403
        )
        assert (
            client.post(
                f"/api/v1/principals/{system['worker']['id']}:disable",
                headers=headers,
            ).status_code
            == 403
        )
        assert (
            client.post(
                f"/api/v1/runtimes/{attachment['runtime_session_id']}:stop",
                headers=headers,
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/v1/effects:check",
                headers=headers,
                json={
                    "principal_id": system["cao"]["id"],
                    "kind": "external",
                    "target": "crc-target",
                    "action": "crc-action",
                },
            ).status_code
            == 403
        )

        a2a_headers = {
            **headers,
            "A2A-Version": "1.0",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        assert (
            client.post(
                "/a2a",
                headers=a2a_headers,
                json={"jsonrpc": "2.0", "id": 403, "method": "GetTask", "params": {"id": "x"}},
            ).status_code
            == 403
        )
        assert client.get("/a2a/http/extendedAgentCard", headers=a2a_headers).status_code == 403

        assert (
            client.get(
                "/api/v1/principals",
                headers={"Authorization": f"Bearer {system['cao_token']}"},
            ).status_code
            == 200
        )


def test_close_is_bound_idempotent_and_visible_without_private_locators(system):
    """Only the exact accepted, cleaned work generation may be explicitly closed."""

    # Dashboard history is production-only.  The shared fixture is deliberately
    # unclassified so unrelated tests never become operator-visible by default.
    system["service"].db.execute(
        "UPDATE principals SET operator_scope = 'production', operator_label = ? "
        "WHERE id = ? AND operator_scope = 'unclassified'",
        ("Close contract Worker", system["worker"]["id"]),
    )
    attachment, cao_runtime, _token = _attached_credential(system)
    conversation = system["service"].authenticate(attachment["context_token"])
    reviewed = _reviewed_attached_work(system, attachment, cao_runtime)
    RequesterDecisionInput = _contract_type("RequesterDecisionInput")
    decision_result = system["service"].record_requester_decision(
        cao_runtime,
        RequesterDecisionInput(
            review_id=reviewed["reviews"][-1]["id"],
            verdict="accepted",
            summary="Requester accepted in the existing conversation",
            evidence=[],
            conversation_evidence_id="conversation-decision-v2",
            idempotency_key="conversation-close:decision-direct",
        ),
    )
    work = decision_result
    decision = work["requester_decisions"][-1]
    request = _close_input(
        system["service"], conversation, work, decision, "conversation-close:exact"
    )
    dashboard = DashboardReadModel(system["service"])
    before = dashboard.snapshot()["cursor"]

    other = system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            requester_id=system["user"]["id"],
            runtime_session_id=system["runtime"]["id"],
            supervisor_attachment_id=attachment["id"],
            supervisor_project_digest=attachment["project_digest"],
            title="Other work",
            objective="Must never be closed by another work's plan",
            acceptance=["Cross-work close plans are rejected"],
        ),
    )
    system["service"].cancel_work(
        cao_runtime,
        other["id"],
        "Release the shared runtime before the original WorkItem closes",
        "conversation-close:cancel-other",
    )

    # The close receipt records evidence after cleanup; it cannot turn a
    # self-attested cleanup record into canonical runtime state.
    system["service"].stop_work_runtime(conversation, work["id"])

    closed = system["service"].close_work(conversation, request)
    replay = system["service"].close_work(conversation, request)

    assert closed["closure"]["state"] == "closed"
    assert replay["closure"] == closed["closure"]
    assert closed["closure"]["plan_digest"]
    assert closed["closure"]["open_delivery_count"] == 0
    assert closed["closure"]["active_scoped_runtime_count"] == 0
    assert closed["closure"]["unresolved_effect_count"] == 0
    assert all("/" not in value for value in closed["closure"]["cleanup_target_fingerprints"])
    history = dashboard.history(after=before)
    assert any(item["event"]["type"] == "work.closed" for item in history["items"])

    changed = request.model_copy(update={"expected_generation": request.expected_generation + 1})
    with pytest.raises(ConflictError):
        system["service"].close_work(conversation, changed)

    with pytest.raises(ConflictError):
        system["service"].close_work(
            conversation,
            request.model_copy(update={"work_item_id": other["id"]}),
        )
