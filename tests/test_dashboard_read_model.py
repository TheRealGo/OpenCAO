from __future__ import annotations

import asyncio
import json
import os
from contextlib import closing
from dataclasses import replace

import pytest
from conftest import (
    CURRENT_CAO_CATALOG_DIGEST,
    CURRENT_CAO_PROXY_ABI_VERSION,
    attach_cao_session_with_peer,
    current_cao_session_attachment,
)
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.dashboard import (
    DashboardCursor,
    DashboardReadModel,
    _digest,
    build_operator_view,
)
from cao_control_plane.dashboard_history import history_reference
from cao_control_plane.errors import AuthorizationError, ConflictError, ValidationError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryInput,
    BoundaryKind,
    MessageKind,
    PrincipalCreate,
    PrincipalRole,
    QueryInput,
    ReportInput,
    ReviewInput,
    RuntimeRegistration,
    RuntimeState,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import build_projection
from cao_control_plane.runtime_enrollment import _process_identity


def _dashboard_app(system):
    settings = replace(system["settings"], enable_dashboard=True)
    app = create_app(settings)
    name = f"operator-dashboard-{len(app.state.service.list_principals())}"
    created = app.state.service.create_principal(
        system["cao"],
        PrincipalCreate(
            name=name,
            role=PrincipalRole.DASHBOARD,
            metadata={},
        ),
    )
    return app, str(created["token"])


def _production_worker(system, *, label: str = "Worker 1") -> None:
    """Promote only this Dashboard fixture before it creates scoped Work."""

    system["service"].db.execute(
        "UPDATE principals SET operator_scope = 'production', operator_label = ? WHERE id = ?",
        (label, system["worker"]["id"]),
    )


def _sensitive_transition(system) -> dict[str, object]:
    _production_worker(system)
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="private title",
            objective="private objective",
            maturity="defined",
            acceptance=["private acceptance"],
            metadata={"private": "metadata"},
            idempotency_key="dashboard-sensitive-work",
        ),
    )


def _managed_dashboard_item(
    system: dict[str, object],
    *,
    adapter: str,
    model: str,
    effort: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Bind a fixture Attempt to one canonical spec without exposing it to the DTO."""

    service = system["service"]
    cao = system["cao"]
    worker = system["worker"]
    runtime = system["runtime"]
    assert isinstance(service, object) and isinstance(cao, dict)
    assert isinstance(worker, dict) and isinstance(runtime, dict)
    _production_worker(system)
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"dashboard-{adapter}", project_digest="d" * 64
        ),
    )
    service.assign_work(  # type: ignore[union-attr]
        cao,
        WorkAssignment(
            worker_id=str(worker["id"]),
            title="Dashboard managed runner",
            objective="Project only canonical runner details",
            maturity="defined",
            acceptance=["Runner details are bounded"],
            supervisor_attachment_id=str(attachment["id"]),
            supervisor_project_digest="d" * 64,
            idempotency_key=f"dashboard-managed-{adapter}",
        ),
    )
    enrollment = service.db.fetchone(  # type: ignore[union-attr]
        "SELECT id FROM worker_enrollments WHERE runtime_session_id = ?", (runtime["id"],)
    )
    assert enrollment is not None
    service.db.execute(  # type: ignore[union-attr]
        "UPDATE runtime_sessions SET adapter = ? WHERE id = ?", (adapter, runtime["id"])
    )
    service.db.execute(  # type: ignore[union-attr]
        """
        INSERT INTO managed_worker_specs(
            id, attachment_id, attachment_generation, principal_id, runtime_session_id,
            enrollment_id, worker_profile_id, adapter, workspace_ref,
            requested_model, effective_model, requested_reasoning_effort,
            effective_reasoning_effort, state, policy_binding_digest, input_digest,
            idempotency_key, created_at, updated_at, stopped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'enabled', ?, ?, ?, ?, ?, NULL)
        """,
        (
            f"spec_dashboard_{adapter}",
            attachment["id"],
            attachment["generation"],
            worker["id"],
            runtime["id"],
            enrollment["id"],
            adapter,
            adapter,
            "opaque-workspace-ref",
            model,
            model,
            effort,
            effort,
            "binding-digest",
            "input-digest",
            f"dashboard-spec-{adapter}",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    service.db.execute(  # type: ignore[union-attr]
        """
        INSERT INTO managed_worker_threads(
            id, managed_spec_id, state, generation,
            created_at, updated_at, archived_at
        ) VALUES (?, ?, 'active', 1, ?, ?, NULL)
        """,
        (
            f"thread_dashboard_{adapter}",
            f"spec_dashboard_{adapter}",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    service.db.execute(  # type: ignore[union-attr]
        """
        INSERT INTO managed_worker_thread_epochs(
            id, thread_id, generation, runtime_session_id,
            enrollment_id, created_at, retired_at
        ) VALUES (?, ?, 1, ?, ?, ?, NULL)
        """,
        (
            f"epoch_dashboard_{adapter}",
            f"thread_dashboard_{adapter}",
            runtime["id"],
            enrollment["id"],
            "2026-01-01T00:00:00Z",
        ),
    )
    item = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]  # type: ignore[union-attr,index]
    assert isinstance(item, dict)
    return item, attachment


def _dashboard_mcp_call(client: TestClient, token: str, request_id: int, method: str, params: dict):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "MCP-Protocol-Version": "2026-07-28",
        "Mcp-Method": method,
    }
    if method == "resources/read":
        headers["Mcp-Name"] = str(params["uri"])
    if method == "tools/call":
        headers["Mcp-Name"] = str(params["name"])
    return client.post(
        "/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": {
                **params,
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {},
                },
            },
        },
    )


def test_dashboard_principal_has_one_sanitized_mcp_snapshot_and_no_mutation_capability(system):
    work = _sensitive_transition(system)
    app, token = _dashboard_app(system)
    headers = {"Authorization": f"Bearer {token}"}
    dashboard = app.state.service.authenticate(token)
    work_id = str(work["id"])

    with pytest.raises(AuthorizationError):
        app.state.service.get_work(work_id, dashboard)
    with pytest.raises(AuthorizationError):
        app.state.service.query_work(QueryInput(), dashboard)
    with pytest.raises(AuthorizationError):
        app.state.service.list_events(actor=dashboard)

    with closing(TestClient(app, base_url="http://localhost")) as client:
        responses = [
            client.get("/api/v1/work", headers=headers),
            client.get(f"/api/v1/work/{work_id}", headers=headers),
            client.get("/api/v1/events", headers=headers),
            client.get("/api/v1/dispatcher", headers=headers),
            client.post(
                f"/api/v1/work/{work_id}:cancel",
                headers=headers,
                json={"reason": "blocked", "idempotency_key": "dashboard-denied"},
            ),
        ]

    for response in responses:
        assert response.status_code == 403
        assert "private title" not in response.text
        assert "private objective" not in response.text
        assert "private acceptance" not in response.text
        assert "private" not in response.text
        assert "cao_assign" not in response.text

    assert app.state.mcp.tools_for(dashboard) == []
    assert app.state.mcp.resources_for(dashboard) == [
        {
            "uri": "cao://dashboard/v1/snapshot",
            "name": "CAO dashboard snapshot",
            "description": "Sanitized cao-dashboard-read-model/v1 operator snapshot.",
            "mimeType": "application/json",
        }
    ]
    assert app.state.mcp.discover(dashboard)["capabilities"]["resources"] == {
        "subscribe": True,
        "listChanged": False,
    }

    async def acknowledged_dashboard_filter() -> dict[str, object]:
        stream = app.state.mcp.subscription_messages(
            dashboard,
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "subscriptions/listen",
                "params": {
                    "notifications": {
                        "resourceSubscriptions": [
                            "cao://dashboard/v1/snapshot",
                            "cao://events",
                        ]
                    },
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                },
            },
        )
        acknowledged = await anext(stream)
        await stream.aclose()
        assert acknowledged is not None
        return acknowledged

    acknowledged = asyncio.run(acknowledged_dashboard_filter())
    assert acknowledged["params"]["notifications"] == {
        "resourceSubscriptions": ["cao://dashboard/v1/snapshot"]
    }
    with pytest.raises(ValidationError, match="dashboard principals"):
        app.state.mcp.read_resource(dashboard, "cao://self")

    bootstrap_token = app.state.service.issue_owner_local_attachment_bootstrap(
        _process_identity(os.getpid()),
        "dashboard-resource-isolation",
        "e" * 64,
        CURRENT_CAO_CATALOG_DIGEST,
        CURRENT_CAO_PROXY_ABI_VERSION,
    )
    attachment = app.state.service.attach_cao_session(
        app.state.service.authenticate(bootstrap_token),
        current_cao_session_attachment(
            native_thread_id="dashboard-resource-isolation", project_digest="e" * 64
        ),
    )
    csc = app.state.service.authenticate(attachment["context_token"])
    ticket = app.state.service.issue_cao_runtime_launch_ticket(attachment["runtime_session_id"])
    crc = app.state.service.authenticate(
        app.state.service.exchange_cao_runtime_launch_ticket(ticket["ticket"])["token"]
    )
    outsiders = (
        app.state.service.authenticate(system["cao_token"]),
        app.state.service.authenticate(system["worker_principal_token"]),
        app.state.service.authenticate(
            app.state.service.issue_owner_local_attachment_bootstrap(
                _process_identity(os.getpid()),
                "dashboard-bootstrap-outsider",
                "f" * 64,
                CURRENT_CAO_CATALOG_DIGEST,
                CURRENT_CAO_PROXY_ABI_VERSION,
            )
        ),
        csc,
        crc,
    )
    for outsider in outsiders:
        assert "cao://dashboard/v1/snapshot" not in {
            resource["uri"] for resource in app.state.mcp.resources_for(outsider)
        }
        with pytest.raises(ValidationError, match="unavailable"):
            app.state.mcp.read_resource(outsider, "cao://dashboard/v1/snapshot")

    with closing(TestClient(app, base_url="http://localhost")) as client:
        tools = _dashboard_mcp_call(client, token, 1, "tools/list", {})
        listed = _dashboard_mcp_call(client, token, 2, "resources/list", {})
        snapshot = _dashboard_mcp_call(
            client,
            token,
            3,
            "resources/read",
            {"uri": "cao://dashboard/v1/snapshot"},
        )
        denied = _dashboard_mcp_call(
            client,
            token,
            4,
            "tools/call",
            {"name": "cao_assign", "arguments": {}},
        )

    assert tools.status_code == listed.status_code == snapshot.status_code == 200
    assert denied.status_code == 400
    assert tools.json()["result"]["tools"] == []
    assert listed.json()["result"]["resources"] == app.state.mcp.resources_for(dashboard)
    contents = snapshot.json()["result"]["contents"]
    assert len(contents) == 1
    assert contents[0]["uri"] == "cao://dashboard/v1/snapshot"
    native_dto = json.loads(contents[0]["text"])
    canonical_dto = app.state.dashboard_read_model.snapshot()
    assert native_dto["snapshot_digest"] == canonical_dto["snapshot_digest"]
    native_operator = native_dto["operator"]
    canonical_operator = canonical_dto["operator"]
    native_item = native_operator["needs_attention"][0]["current_work_items"][0]
    canonical_item = canonical_operator["needs_attention"][0]["current_work_items"][0]
    assert native_item["latest_reported_at"] == canonical_item["latest_reported_at"]
    assert "latest_reported_at_display" in native_item
    assert "provider_condition" not in native_item
    assert "provider_retry_after_at" not in native_item
    assert "provider_retry_after_at_display" not in native_item
    assert "cooldown_until" not in native_item
    rendered = snapshot.text
    assert system["worker_token"] not in rendered
    assert str(system["worker"]["id"]) not in rendered
    assert denied.json()["error"]["code"] == -32602


def test_native_dashboard_resource_omits_internal_runtime_circuit_fields(
    system, monkeypatch: pytest.MonkeyPatch
) -> None:
    app, token = _dashboard_app(system)
    dashboard = app.state.service.authenticate(token)
    reported_at = "2026-08-12T00:00:00Z"
    internal_timestamp = "2099-12-31T23:59:59Z"

    class RawDashboardReadModel:
        def __init__(self, _service: object) -> None:
            pass

        def snapshot(self) -> dict[str, object]:
            return {
                "format": "cao-dashboard-read-model/v1",
                "snapshot_digest": "c" * 64,
                "operator": {
                    "working": [
                        {
                            "current_work_items": [
                                {
                                    "latest_reported_at": reported_at,
                                    "provider_condition": "rate_limited",
                                    "provider_retry_after_at": internal_timestamp,
                                    "cooldown_until": internal_timestamp,
                                }
                            ]
                        }
                    ],
                    "needs_attention": [],
                    "ready": [],
                    "inactive_workers": [],
                    "work_items": [],
                },
            }

    monkeypatch.setattr("cao_control_plane.mcp.DashboardReadModel", RawDashboardReadModel)

    resource = app.state.mcp.read_resource(dashboard, "cao://dashboard/v1/snapshot")
    item = resource["operator"]["working"][0]["current_work_items"][0]
    assert item["latest_reported_at"] == reported_at
    assert "latest_reported_at_display" in item
    assert resource["snapshot_digest"] == "c" * 64
    assert internal_timestamp not in json.dumps(resource, sort_keys=True)
    assert "rate_limited" not in json.dumps(resource, sort_keys=True)
    assert "provider_condition" not in item
    assert "provider_retry_after_at" not in item
    assert "cooldown_until" not in item


def test_dashboard_stream_wakes_on_an_in_process_successful_commit(system):
    app, _token = _dashboard_app(system)
    model = app.state.dashboard_read_model
    cursor = model.snapshot()["cursor"]

    async def receive_next_event():
        stream = model.stream(
            after=str(cursor),
            max_events=1,
            heartbeat_seconds=5.0,
        )
        synchronized = await anext(stream)
        assert synchronized["synced"]["cursor"] == cursor
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        app.state.service.create_principal(
            app.state.service.authenticate(system["cao_token"]),
            PrincipalCreate(
                name="commit-wakeup",
                role=PrincipalRole.WORKER,
                operator_scope="production",
                operator_label="Commit wakeup Worker",
                metadata={},
            ),
        )
        return await asyncio.wait_for(pending, timeout=1.0)

    item = asyncio.run(receive_next_event())
    assert item["event"]["event"]["type"] == "principal.created"


def test_dashboard_stream_emits_a_sync_boundary_after_hidden_progress(system):
    app, _token = _dashboard_app(system)
    model = app.state.dashboard_read_model
    cursor = model.snapshot()["cursor"]
    app.state.service.create_principal(
        system["cao"],
        PrincipalCreate(
            name="hidden-dashboard-progress",
            role=PrincipalRole.EXTERNAL,
            metadata={},
        ),
    )

    async def receive_sync_boundary():
        stream = model.stream(
            after=str(cursor),
            heartbeat_seconds=5.0,
        )
        return await asyncio.wait_for(anext(stream), timeout=1.0)

    item = asyncio.run(receive_sync_boundary())
    assert item["synced"]["format"] == "cao-dashboard-read-model/v1"
    assert item["synced"]["status"] == "synced"
    assert item["synced"]["cursor"] != cursor


def test_dashboard_is_feature_gated_and_requires_a_dashboard_principal(system):
    app = create_app(system["settings"])
    with closing(TestClient(app, base_url="http://localhost")) as client:
        assert client.get("/api/v1/dashboard/v1/snapshot").status_code == 404

    dashboard_app, dashboard_token = _dashboard_app(system)
    with closing(TestClient(dashboard_app, base_url="http://localhost")) as client:
        assert (
            client.get(
                "/api/v1/dashboard/v1/snapshot",
                headers={"Authorization": f"Bearer {system['cao_token']}"},
            ).status_code
            == 403
        )
        response = client.get(
            "/api/v1/dashboard/v1/snapshot",
            headers={"Authorization": f"Bearer {dashboard_token}"},
        )
        assert response.status_code == 200
        assert response.json()["format"] == "cao-dashboard-read-model/v1"


def test_dashboard_snapshot_and_history_are_sanitized_and_deterministic(system):
    seeded = _sensitive_transition(system)
    app, token = _dashboard_app(system)
    headers = {"Authorization": f"Bearer {token}"}
    # This assertion is about the immutable read-model projection. Entering
    # TestClient's lifespan would also start the commit-driven Dispatcher and
    # race the seeded pending Delivery into a real recovery transition.
    with closing(TestClient(app, base_url="http://localhost")) as client:
        first = client.get("/api/v1/dashboard/v1/snapshot", headers=headers)
        assert first.status_code == 200
        body = first.json()
        digest_input = {key: value for key, value in body.items() if key != "snapshot_digest"}
        assert body["snapshot_digest"] == _digest(digest_input)
        snapshot_text = first.text
        assert "private title" in snapshot_text
        assert "private objective" in snapshot_text
        assert "private acceptance" not in snapshot_text
        assert "metadata" not in snapshot_text
        assert system["worker_token"] not in snapshot_text
        assert str(system["worker"]["id"]) not in snapshot_text
        operator = body["operator"]
        assert operator["format"] == "cao-dashboard-operator/v1"
        assert operator["work_items"] == [
            {
                "display_label": "Work item 1",
                "worker_label": "Worker 1",
                "history_reference": history_reference("work", seeded["id"]),
                "objective_text": "private objective",
                "latest_report_text": None,
                "work_title": "private title",
                "objective_summary": "private objective",
                "state": "active",
                "attempt_state": "assigned",
                "progress_stage": None,
                "stage": "assigned",
                "trajectory": "untracked",
                "attention_owner": "worker",
                "supervision_pause": None,
                "next_boundary_summary": None,
                "pending_supervisor_boundary": False,
                "recovery_action": None,
                "recovery_waiting_since": None,
                "recovery_notification_state": None,
                "cao_supervision_state": None,
                "cao_supervision_updated_at": None,
                "latest_report_kind": None,
                "latest_report_summary": None,
                "latest_reported_at": None,
                "runtime_heartbeat_at": None,
                "last_worker_activity_at": None,
                "last_artifact_at": None,
                "status_request_state": None,
                "status_requested_at": None,
                "status_response_due_at": None,
                "status_responded_at": None,
                "completion_contract": "legacy_unclassified",
                "delivery_state": "pending",
                "next_observable_boundary": None,
                "latest_worker_report_summary": None,
                "runner_adapter": None,
                "runner_model": None,
                "runner_reasoning_effort": None,
                "runner_requested_model": None,
                "runner_effective_model": None,
                "runner_requested_reasoning_effort": None,
                "runner_effective_reasoning_effort": None,
                "runner_availability": "unavailable",
                "runner_state": "unavailable",
                "runner_connection_state": "unavailable",
                "latest_cao_review_decision": "pending",
                "requester_decision": "pending",
                "closure_state": "open",
                "closure_summary": {
                    "requester_decision": "pending",
                    "cao_review": "pending",
                    "artifact_preservation": "pending",
                    "cleanup": "pending",
                    "unresolved_deliveries": 1,
                    "unresolved_effects": None,
                    "active_runtimes": 1,
                },
                "completed_at": None,
                "availability": {
                    "work_title": "available",
                    "objective_summary": "available",
                    "attempt_state": "available",
                    "progress_stage": "unavailable",
                    "next_boundary_summary": "unavailable",
                    "pending_supervisor_boundary": "available",
                    "recovery_action": "unavailable",
                    "recovery_waiting_since": "unavailable",
                    "recovery_notification_state": "unavailable",
                    "latest_report_kind": "unavailable",
                    "latest_report_summary": "unavailable",
                    "latest_reported_at": "unavailable",
                    "runtime_heartbeat_at": "unavailable",
                    "last_worker_activity_at": "unavailable",
                    "last_artifact_at": "unavailable",
                    "status_request_state": "unavailable",
                    "status_requested_at": "unavailable",
                    "status_response_due_at": "unavailable",
                    "status_responded_at": "unavailable",
                    "completion_contract": "available",
                    "delivery_state": "available",
                    "next_observable_boundary": "unavailable",
                    "latest_worker_report_summary": "unavailable",
                    "runner_adapter": "unavailable",
                    "runner_model": "unavailable",
                    "runner_reasoning_effort": "unavailable",
                    "runner_requested_model": "unavailable",
                    "runner_effective_model": "unavailable",
                    "runner_requested_reasoning_effort": "unavailable",
                    "runner_effective_reasoning_effort": "unavailable",
                    "runner_availability": "available",
                    "runner_state": "available",
                    "runner_connection_state": "available",
                    "latest_cao_review_decision": "available",
                    "requester_decision": "available",
                    "closure_summary": "available",
                },
            }
        ]

        history = client.get("/api/v1/dashboard/v1/history?limit=100", headers=headers)
        assert history.status_code == 200
        body = history.json()
        assert body["items"]
        rendered = history.text
        assert "private title" not in rendered
        assert "private objective" not in rendered
        assert "private acceptance" not in rendered
        assert "metadata" not in rendered
        assert "data_json" not in rendered
        assert all(set(item) == {"cursor", "event"} for item in body["items"])
        assert all(
            set(item["event"]) == {"type", "aggregate_type", "occurred_at"}
            for item in body["items"]
        )


def test_operator_view_is_allowlisted_and_never_derives_text_or_private_locators() -> None:
    view = build_operator_view(
        {
            "work_items": [
                {
                    "id": "opaque-internal-locator",
                    "assigned_worker_id": "opaque-worker-locator",
                    "state": "active",
                    "current_attempt_state": "working",
                    "current_attempt_trajectory": "advancing",
                    "attention_owner": "worker",
                    "open_boundary_count": 1,
                    "title": "unapproved task text",
                    "objective": "unapproved objective text",
                    "summary": "unapproved report text",
                    "operator_content": {
                        "provider_condition": "rate_limited",
                        "provider_retry_after_at": "2099-12-31T23:59:59Z",
                        "cooldown_until": "2099-12-31T23:59:59Z",
                    },
                    "payload": {"arbitrary_key": "untrusted-value"},
                }
            ],
            "runtimes": [{"id": "opaque-runtime-locator"}],
            "scheduler": {"queued_due": 1, "unexpected": "untrusted-value"},
        }
    )
    rendered = str(view)
    assert "opaque-" not in rendered
    assert "unapproved" not in rendered
    assert "arbitrary_key" not in rendered
    assert "provider_condition" not in rendered
    assert "provider_retry_after_at" not in rendered
    assert "cooldown_until" not in rendered
    assert "rate_limited" not in rendered
    assert "2099-12-31T23:59:59Z" not in rendered
    item = view["work_items"][0]
    assert item["worker_label"] == "Worker 1"
    assert item["next_observable_boundary"] == "pending"
    assert item["pending_supervisor_boundary"] is True
    assert item["work_title"] is None and item["latest_worker_report_summary"] is None


def test_dashboard_names_system_reconciliation_and_notification_state(system) -> None:
    work = _sensitive_transition(system)
    service = system["service"]
    attempt = work["current_attempt"]
    boundary = service.record_boundary(
        system["worker"],
        BoundaryInput(
            source_event_id="dashboard-system-reconciliation",
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            kind=BoundaryKind.FAILURE,
            summary="Bounded recovery state",
            runtime_state=RuntimeState.FAILED,
            metadata={"runtime_recovery": True, "reason": "worker_inactive_timeout"},
        ),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE boundaries SET recovery_action = 'system_reconciliation' WHERE id = ?",
            (boundary["id"],),
        )
        connection.execute(
            "UPDATE work_items SET state = 'waiting_supervisor', "
            "attention_owner = 'cao' WHERE id = ?",
            (work["id"],),
        )
        connection.execute(
            "UPDATE attempts SET state = 'waiting_supervisor', "
            "stage = 'system_reconciliation', next_boundary = 'system_reconciliation' "
            "WHERE id = ?",
            (attempt["id"],),
        )
        copied = service._message(
            connection,
            sender_id=system["cao"]["id"],
            recipient_id=system["cao"]["id"],
            kind=MessageKind.SYSTEM,
            payload={
                "action": "review_runtime_boundary",
                "boundary_id": boundary["id"],
                "boundary_kind": boundary["kind"],
                "generation": work["generation"],
            },
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            goal_version=work["goal_version"],
            idempotency_key="dashboard-copied-recovery-notification",
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled', "
            "delivered_at = ?, acknowledged_at = ?, handled_at = ?, updated_at = ? "
            "WHERE message_id = ? AND recipient_id = ?",
            (
                copied["created_at"],
                copied["created_at"],
                copied["created_at"],
                copied["created_at"],
                copied["id"],
                system["cao"]["id"],
            ),
        )
        malformed = service._message(
            connection,
            sender_id=system["worker"]["id"],
            recipient_id=system["cao"]["id"],
            kind=MessageKind.SYSTEM,
            payload={
                "action": ["review_runtime_boundary"],
                "boundary_id": boundary["id"],
                "boundary_kind": boundary["kind"],
                "generation": work["generation"],
            },
            work_item_id=work["id"],
            attempt_id=attempt["id"],
            goal_version=work["goal_version"],
            idempotency_key="dashboard-malformed-recovery-notification",
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled', "
            "delivered_at = ?, acknowledged_at = ?, handled_at = ?, updated_at = ? "
            "WHERE message_id = ? AND recipient_id = ?",
            (
                malformed["created_at"],
                malformed["created_at"],
                malformed["created_at"],
                malformed["created_at"],
                malformed["id"],
                system["cao"]["id"],
            ),
        )

    before = build_operator_view(build_projection(service.db).snapshot)
    worker = before["needs_attention"][0]
    item = worker["current_work_items"][0]
    assert worker["attention_reason"] == "cao-processing"
    assert item["recovery_action"] == "system_reconciliation"
    assert item["recovery_waiting_since"] is not None
    assert item["recovery_notification_state"] == "queued"
    assert item["cao_supervision_state"] == "scheduled"
    assert before["cao_processing"][0]["worker_label"] == worker["worker_label"]

    message = service.db.fetchone(
        "SELECT id FROM messages WHERE work_item_id = ? "
        "AND json_extract(payload_json, '$.boundary_id') = ? "
        "AND sender_id = ? ORDER BY sequence LIMIT 1",
        (work["id"], boundary["id"], system["worker"]["id"]),
    )
    assert message is not None
    service.acknowledge(system["cao"], AckInput(message_ids=[message["id"]]))
    with pytest.raises(ConflictError, match="must be disposed"):
        service.mark_message_handled(
            system["cao"], message["id"], evidence="Observed the bounded system state."
        )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE message_deliveries SET state = 'handled', handled_at = updated_at "
            "WHERE message_id = ? AND recipient_id = ?",
            (message["id"], system["cao"]["id"]),
        )

    after = build_operator_view(build_projection(service.db).snapshot)
    worker = after["stopped_or_failed"][0]
    item = worker["current_work_items"][0]
    assert worker["attention_reason"] == "system-reconciliation"
    assert item["recovery_notification_state"] == "handled"
    assert item["cao_supervision_state"] == "unscheduled"
    assert item["pending_supervisor_boundary"] is True


def test_operator_view_groups_production_workers_once_and_hides_non_operator_scopes() -> None:
    def worker(
        worker_id: str,
        scope: str,
        label: str,
        *,
        state: str = "enabled",
        connection: str = "connected-idle",
    ) -> dict[str, object]:
        return {
            "principal_id": worker_id,
            "operator_scope": scope,
            "operator_label": label,
            "principal_enabled": True,
            "worker_state": state,
            "runner_availability": "available",
            "runner_connection_state": connection,
        }

    def work(
        work_id: str,
        worker_id: str,
        scope: str,
        title: str,
        *,
        state: str = "active",
        stage: str = "working",
    ) -> dict[str, object]:
        return {
            "id": work_id,
            "assigned_worker_id": worker_id,
            "operator_scope": scope,
            "state": state,
            "priority": 1,
            "attention_owner": "worker" if state == "active" else "none",
            "current_attempt_state": stage,
            "current_attempt_trajectory": "advancing" if state == "active" else "complete",
            "open_boundary_count": 0,
            "operator_content": {"work_title": title},
            "created_at": "2026-08-12T00:00:00Z",
            "updated_at": "2026-08-12T00:00:00Z",
        }

    view = build_operator_view(
        {
            "operator_workers": [
                worker("prod-attention", "production", "Review Worker"),
                worker("prod-working", "production", "Working Worker", connection="connected-busy"),
                worker("prod-ready", "production", "Ready Worker", connection="enrolling"),
                worker(
                    "prod-inactive",
                    "production",
                    "Stopped Worker",
                    state="stopped",
                    connection="stopped",
                ),
                worker("acceptance-hidden", "acceptance-test", "ACCEPTANCE MARKER"),
                worker("system-hidden", "system", "SYSTEM MARKER"),
                worker("unclassified-hidden", "unclassified", "UNCLASSIFIED MARKER"),
            ],
            "work_items": [
                work("prod-work-a", "prod-working", "production", "Production A"),
                work("prod-work-b", "prod-working", "production", "Production B"),
                {
                    **work(
                        "prod-settled",
                        "prod-attention",
                        "production",
                        "Supervisor-settled completion",
                        state="completed",
                        stage="completed",
                    ),
                    "closure_summary": {
                        "requester_decision": "pending",
                        "cao_review": "ok",
                    },
                },
                work(
                    "acceptance-work",
                    "acceptance-hidden",
                    "acceptance-test",
                    "ACCEPTANCE WORK MARKER",
                ),
                work(
                    "unclassified-work",
                    "unclassified-hidden",
                    "unclassified",
                    "UNCLASSIFIED WORK MARKER",
                ),
            ],
            "operator_runtime_delivery": {
                "runtime_count": 4,
                "queued_deliveries": 3,
                "unknown_delivery_outcomes": 2,
                "dead_deliveries": 1,
                "effects_by_state": {"pending": 2},
            },
            "runtimes": [{"id": "NONPRODUCTION RUNTIME MARKER"}],
            "scheduler": {"queued_due": 119},
        }
    )

    assert view["counts"] == {
        "needs_attention": 0,
        "working": 1,
        "ready": 2,
        "inactive_workers": 1,
        "current_work_items": 2,
    }
    assert view["needs_attention"] == []
    assert [item["worker_label"] for item in view["working"]] == ["Working Worker"]
    assert len(view["working"][0]["current_work_items"]) == 2
    ready = {item["worker_label"]: item for item in view["ready"]}
    assert ready["Ready Worker"]["runner_connection_state"] == "enrolling"
    assert ready["Ready Worker"]["current_work_items"] == []
    assert ready["Review Worker"]["current_work_items"] == []
    assert [item["worker_label"] for item in view["inactive_workers"]] == [
        "Stopped Worker",
    ]
    assert view["runtime_delivery"] == {
        "runtime_count": 4,
        "queued_deliveries": 3,
        "unknown_delivery_outcomes": 2,
        "dead_deliveries": 1,
        "effects_by_state": {"pending": 2},
    }
    rendered = str(view)
    for forbidden in (
        "ACCEPTANCE MARKER",
        "SYSTEM MARKER",
        "UNCLASSIFIED MARKER",
        "acceptance-hidden",
        "prod-working",
        "prod-work-a",
        "NONPRODUCTION RUNTIME MARKER",
        "119",
    ):
        assert forbidden not in rendered


def test_supervisor_settled_completion_is_not_mislabeled_as_requester_action() -> None:
    """Accepted completion bookkeeping is not a genuine requester-input lane."""

    def worker(worker_id: str, label: str) -> dict[str, object]:
        return {
            "principal_id": worker_id,
            "operator_scope": "production",
            "operator_label": label,
            "principal_enabled": True,
            "worker_state": "enabled",
            "runner_availability": "available",
            "runner_connection_state": "connected-idle",
        }

    common = {
        "operator_scope": "production",
        "priority": 1,
        "current_attempt_trajectory": "complete",
        "created_at": "2026-08-25T00:00:00Z",
        "updated_at": "2026-08-25T00:00:00Z",
    }
    view = build_operator_view(
        {
            "operator_workers": [
                worker("settled-worker", "Settled Worker"),
                worker("input-worker", "Input Worker"),
            ],
            "work_items": [
                {
                    **common,
                    "id": "settled-work",
                    "assigned_worker_id": "settled-worker",
                    "state": "waiting_user",
                    "attention_owner": "user",
                    "current_attempt_state": "completed",
                    "open_boundary_count": 0,
                    "closure_state": "open",
                    "closure_summary": {
                        "requester_decision": "pending",
                        "cao_review": "ok",
                    },
                    "operator_content": {"work_title": "Verified result"},
                },
                {
                    **common,
                    "id": "input-work",
                    "assigned_worker_id": "input-worker",
                    "state": "user_needed",
                    "attention_owner": "user",
                    "current_attempt_state": "input_required",
                    "open_boundary_count": 1,
                    "closure_state": "open",
                    "closure_summary": {
                        "requester_decision": "pending",
                        "cao_review": "pending",
                    },
                    "operator_content": {"work_title": "Actual requester question"},
                },
            ],
        }
    )

    assert view["counts"] == {
        "needs_attention": 1,
        "working": 0,
        "ready": 1,
        "inactive_workers": 0,
        "current_work_items": 1,
    }
    assert view["needs_attention"][0]["worker_label"] == "Input Worker"
    assert view["needs_attention"][0]["attention_reason"] == "user-action-required"
    assert view["needs_attention"][0]["current_work_items"][0]["work_title"] == (
        "Actual requester question"
    )
    assert view["ready"][0]["worker_label"] == "Settled Worker"
    assert view["ready"][0]["current_work_items"] == []
    assert [item["work_title"] for item in view["recently_completed"]] == [
        "Verified result"
    ]
    assert view["recently_completed"][0]["completed_at"] == "2026-08-25T00:00:00Z"


def test_open_completion_boundary_is_not_projected_as_system_reconciliation(system) -> None:
    """Ordinary CAO review work is not a runtime-recovery action."""

    service = system["service"]
    work = _sensitive_transition(system)
    attempt = work["current_attempt"]
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=attempt["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The bounded result is ready for ordinary CAO review.",
            evidence=[{"check": "representative", "result": "pass"}],
            idempotency_key="dashboard-open-completion-boundary",
        ),
    )

    view = build_operator_view(build_projection(service.db).snapshot)
    worker = view["needs_attention"][0]
    item = worker["current_work_items"][0]
    assert worker["attention_reason"] == "cao-processing"
    assert view["cao_processing"][0]["worker_label"] == worker["worker_label"]
    assert item["pending_supervisor_boundary"] is True
    assert item["cao_supervision_state"] == "scheduled"
    assert item["recovery_action"] is None
    assert item["recovery_waiting_since"] is None


def test_cao_supervision_is_active_only_with_a_live_reasoner_turn(system) -> None:
    service = system["service"]
    work = _sensitive_transition(system)
    attempt = work["current_attempt"]
    waiting = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=attempt["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The bounded result is ready for a leased CAO review.",
            evidence=[{"check": "representative", "result": "pass"}],
            idempotency_key="dashboard-active-cao-supervision",
        ),
    )
    boundary = waiting["open_boundaries"][0]
    scheduled = build_operator_view(build_projection(service.db).snapshot)
    assert scheduled["cao_processing"][0]["current_work_items"][0][
        "cao_supervision_state"
    ] == "scheduled"

    service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=waiting["generation"],
        idempotency_key="dashboard-active-cao-supervision-turn",
    )
    active = build_operator_view(build_projection(service.db).snapshot)
    assert active["cao_processing"][0]["current_work_items"][0][
        "cao_supervision_state"
    ] == "active"


def test_real_supervisor_accepted_completion_leaves_current_dashboard_work(system) -> None:
    """The durable completion path reproduces the production residual-card case."""

    service = system["service"]
    work = _sensitive_transition(system)
    attempt = work["current_attempt"]
    claimed = service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="completion_claim",
            expected_goal_version=attempt["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="The bounded result is complete.",
            evidence=[{"check": "representative", "result": "pass"}],
            idempotency_key="dashboard-settled-completion-report",
        ),
    )
    boundary = claimed["open_boundaries"][0]
    service.review(
        system["cao"],
        ReviewInput(
            attempt_id=attempt["id"],
            verdict="ok",
            summary="The completion evidence was independently verified.",
        ),
    )
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        expected_generation=claimed["generation"],
        idempotency_key="dashboard-settled-completion-turn",
    )
    service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=claimed["generation"],
            kind="accept",
            reason="The verified completion satisfies the bounded Work.",
        ),
    )

    persisted = service.get_work(work["id"], system["cao"])
    assert persisted["state"] == "waiting_user"
    assert persisted["current_attempt"]["state"] == "completed"
    assert persisted["open_boundaries"] == []

    view = build_operator_view(build_projection(service.db).snapshot)
    assert view["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 1,
        "inactive_workers": 0,
        "current_work_items": 0,
    }
    assert view["work_items"] == []
    assert view["ready"][0]["current_work_items"] == []


@pytest.mark.parametrize(
    ("work_state", "closure_state"),
    [
        ("completed", "closed"),
        ("canceled", "open"),
        ("failed", "open"),
    ],
)
def test_terminal_only_worker_history_cannot_return_to_attention(
    work_state: str,
    closure_state: str,
) -> None:
    view = build_operator_view(
        {
            "operator_workers": [
                {
                    "principal_id": "terminal-worker",
                    "operator_scope": "production",
                    "operator_label": "Terminal Worker",
                    "principal_enabled": True,
                    "worker_state": "enabled",
                    "runner_availability": "available",
                    "runner_connection_state": "missing",
                    "enrollment_state": "failed",
                }
            ],
            "work_items": [
                {
                    "assigned_worker_id": "terminal-worker",
                    "operator_scope": "production",
                    "state": work_state,
                    "closure_state": closure_state,
                    # Deliberately stale status fields must not resurrect a
                    # terminal Work or its dead headless runtime.
                    "attention_owner": "user",
                    "current_attempt_state": "input_required",
                    "current_attempt_trajectory": "stalled",
                    "open_boundary_count": 2,
                    "operator_content": {
                        "status_request_state": "overdue",
                        "delivery_state": "delivery_missing",
                    },
                    "closure_summary": {
                        "requester_decision": "accepted",
                        "cao_review": "ok",
                        "artifact_preservation": "preserved",
                        "cleanup": "verified",
                        "unresolved_deliveries": 1,
                        "unresolved_effects": 1,
                        "active_runtimes": 0,
                    },
                }
            ],
        }
    )

    assert view["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 1,
        "inactive_workers": 0,
        "current_work_items": 0,
    }
    ready = view["ready"][0]
    assert ready["attention_reason"] is None
    assert ready["current_work_items"] == []
    assert view["work_items"] == []


@pytest.mark.parametrize(
    ("worker_state", "connection_state", "expected_category", "expected_reason"),
    [
        ("enabled", "connected-busy", "working", None),
        ("stopped", "unavailable", "needs_attention", "runner-stopped"),
    ],
)
def test_current_work_takes_precedence_over_terminal_history_and_runner_inventory(
    worker_state: str,
    connection_state: str,
    expected_category: str,
    expected_reason: str | None,
) -> None:
    view = build_operator_view(
        {
            "operator_workers": [
                {
                    "principal_id": "reused-worker",
                    "operator_scope": "production",
                    "operator_label": "Reused Worker",
                    "principal_enabled": worker_state == "enabled",
                    "worker_state": worker_state,
                    "runner_availability": (
                        "available" if worker_state == "enabled" else "unavailable"
                    ),
                    "runner_connection_state": connection_state,
                    "enrollment_state": ("ready" if worker_state == "enabled" else "revoked"),
                }
            ],
            "work_items": [
                {
                    "assigned_worker_id": "reused-worker",
                    "operator_scope": "production",
                    "state": "completed",
                    "closure_state": "closed",
                    "attention_owner": "user",
                    "current_attempt_state": "submitted",
                    "current_attempt_trajectory": "stalled",
                    "open_boundary_count": 1,
                },
                {
                    "assigned_worker_id": "reused-worker",
                    "operator_scope": "production",
                    "state": "active",
                    "closure_state": "open",
                    "attention_owner": "worker",
                    "current_attempt_state": "working",
                    "current_attempt_trajectory": "advancing",
                    "open_boundary_count": 0,
                },
            ],
        }
    )

    assert len(view[expected_category]) == 1
    assert view["inactive_workers"] == []
    worker = view[expected_category][0]
    assert worker["attention_reason"] == expected_reason
    assert len(worker["current_work_items"]) == 1
    assert worker["current_work_items"][0]["state"] == "active"
    assert view["counts"]["current_work_items"] == 1


@pytest.mark.parametrize(
    ("work_state", "closure_state", "expected_category"),
    [
        ("active", "open", "needs_attention"),
        ("failed", "open", "ready"),
    ],
)
def test_stale_enrollment_is_not_reported_as_healthy_work(
    work_state: str,
    closure_state: str,
    expected_category: str,
) -> None:
    view = build_operator_view(
        {
            "operator_workers": [
                {
                    "principal_id": "stale-enrollment-worker",
                    "operator_scope": "production",
                    "operator_label": "Stale Enrollment Worker",
                    "principal_enabled": True,
                    "worker_state": "enabled",
                    "runner_availability": "available",
                    "runner_connection_state": "connected-busy",
                    "enrollment_state": "stale",
                }
            ],
            "work_items": [
                {
                    "assigned_worker_id": "stale-enrollment-worker",
                    "operator_scope": "production",
                    "state": work_state,
                    "closure_state": closure_state,
                    "attention_owner": "worker",
                    "current_attempt_state": "working",
                    "current_attempt_trajectory": "advancing",
                    "open_boundary_count": 0,
                }
            ],
        }
    )

    assert len(view[expected_category]) == 1
    worker = view[expected_category][0]
    if expected_category == "needs_attention":
        assert worker["attention_reason"] == "cao-action-required"
        assert view["counts"]["current_work_items"] == 1
    else:
        assert worker["attention_reason"] is None
        assert view["counts"]["current_work_items"] == 0


def test_projection_keeps_a_terminal_headless_runtime_out_of_attention(system) -> None:
    _managed_dashboard_item(
        system,
        adapter="codex-app-server",
        model="gpt-5.6-terra",
        effort="high",
    )
    service = system["service"]
    service.db.execute(
        "UPDATE work_items SET state = 'failed', attention_owner = 'cao' "
        "WHERE assigned_worker_id = ?",
        (system["worker"]["id"],),
    )
    service.db.execute(
        "UPDATE attempts SET state = 'failed', trajectory = 'stalled' WHERE worker_id = ?",
        (system["worker"]["id"],),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'missing' WHERE id = ?",
        (system["runtime"]["id"],),
    )
    service.db.execute(
        "UPDATE worker_enrollments SET state = 'failed' WHERE runtime_session_id = ?",
        (system["runtime"]["id"],),
    )

    view = build_operator_view(build_projection(service.db).snapshot)

    assert view["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 1,
        "inactive_workers": 0,
        "current_work_items": 0,
    }
    assert view["ready"][0]["attention_reason"] is None
    assert view["ready"][0]["current_work_items"] == []


def test_projection_excludes_archived_worker_thread_from_every_dashboard_group(
    system,
) -> None:
    _managed_dashboard_item(
        system,
        adapter="codex-app-server",
        model="gpt-5.6-terra",
        effort="high",
    )
    service = system["service"]
    before = build_operator_view(build_projection(service.db).snapshot)
    assert before["counts"]["current_work_items"] == 1

    def seed_historical_lifecycle_shape(trigger_name: str, statement: str) -> None:
        with service.db.transaction() as connection:
            trigger = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
                (trigger_name,),
            ).fetchone()
            assert trigger is not None and trigger["sql"]
            connection.execute(f"DROP TRIGGER {trigger_name}")
            connection.execute(statement)
            connection.execute(str(trigger["sql"]))

    # These deliberately impossible historical shapes exercise only the
    # Dashboard projection. Restore each production lifecycle guard before
    # reading the resulting snapshot.
    seed_historical_lifecycle_shape(
        "managed_worker_threads_nonterminal_work_update",
        """
        UPDATE managed_worker_threads
        SET state = 'archived', generation = 2,
            archived_at = '2026-01-02T00:00:00Z',
            updated_at = '2026-01-02T00:00:00Z'
        WHERE id = 'thread_dashboard_codex-app-server'
        """,
    )
    archived = build_operator_view(build_projection(service.db).snapshot)

    for category in (
        "needs_attention",
        "working",
        "ready",
        "inactive_workers",
    ):
        assert archived[category] == []
    assert archived["work_items"] == []
    assert archived["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 0,
        "inactive_workers": 0,
        "current_work_items": 0,
    }

    seed_historical_lifecycle_shape(
        "managed_worker_threads_nonterminal_work_update",
        """
        UPDATE managed_worker_threads
        SET state = 'active', generation = 3, archived_at = NULL,
            updated_at = '2026-01-03T00:00:00Z'
        WHERE id = 'thread_dashboard_codex-app-server'
        """,
    )
    resumed = build_operator_view(build_projection(service.db).snapshot)
    assert resumed["counts"]["current_work_items"] == 1

    seed_historical_lifecycle_shape(
        "managed_worker_threads_nonterminal_work_delete",
        "DELETE FROM managed_worker_threads WHERE id = 'thread_dashboard_codex-app-server'",
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM work_items WHERE assigned_worker_id = ?",
            (system["worker"]["id"],),
        )["count"]
        == 1
    )
    deleted = build_operator_view(build_projection(service.db).snapshot)
    assert deleted["work_items"] == []
    assert deleted["counts"]["current_work_items"] == 0


def test_worker_delete_event_remains_visible_after_the_thread_row_is_gone(system) -> None:
    _item, attachment = _managed_dashboard_item(
        system,
        adapter="codex-app-server",
        model="gpt-5.6-terra",
        effort="high",
    )
    service = system["service"]
    actor = service.authenticate(attachment["context_token"])
    model = DashboardReadModel(service)
    before = str(model.snapshot()["cursor"])
    thread_id = "thread_dashboard_codex-app-server"

    service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            expected_generation=1,
            idempotency_key="dashboard-finish-before-delete",
        ),
    )
    service.delete_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            expected_generation=2,
            idempotency_key="dashboard-delete-after-finish",
        ),
    )

    assert service.db.fetchone(
        "SELECT 1 FROM managed_worker_threads WHERE id = ?", (thread_id,)
    ) is None
    assert model.snapshot()["operator"]["counts"]["current_work_items"] == 0
    page = model.history(after=before, limit=100)
    assert isinstance(page, dict)
    event_types = [item["event"]["type"] for item in page["items"]]
    assert "managed_worker_thread.finished" in event_types
    assert "managed_worker_thread.deleted" in event_types
    deleted_event = service.db.fetchone(
        "SELECT operator_scope FROM events "
        "WHERE event_type = 'managed_worker_thread.deleted' "
        "AND aggregate_id = ? ORDER BY sequence DESC LIMIT 1",
        (thread_id,),
    )
    assert deleted_event is not None
    assert deleted_event["operator_scope"] == "production"


def test_projection_keeps_one_ready_worker_after_multiple_enrollments(system) -> None:
    _production_worker(system, label="Ready after reconnect")
    service = system["service"]
    service.db.execute(
        "UPDATE worker_enrollments SET state = 'revoked' WHERE principal_id = ?",
        (system["worker"]["id"],),
    )
    service.db.execute(
        "UPDATE runtime_sessions SET state = 'stopped' WHERE id = ?",
        (system["runtime"]["id"],),
    )
    service.register_runtime(
        system["cao"],
        system["worker"]["id"],
        RuntimeRegistration(adapter="claude", metadata={"command": ["/bin/cat"]}),
    )

    snapshot = build_projection(service.db).snapshot
    rows = [
        row for row in snapshot["operator_workers"] if row["principal_id"] == system["worker"]["id"]
    ]
    assert len(rows) == 1
    view = build_operator_view(snapshot)
    assert view["counts"] == {
        "needs_attention": 0,
        "working": 0,
        "ready": 1,
        "inactive_workers": 0,
        "current_work_items": 0,
    }
    assert view["ready"][0]["worker_label"] == "Ready after reconnect"
    assert view["ready"][0]["runner_connection_state"] == "unavailable"


def test_idle_failed_transport_remains_usable_without_exposing_diagnostics() -> None:
    view = build_operator_view(
        {
            "operator_workers": [
                {
                    "principal_id": "private-worker-id",
                    "operator_scope": "production",
                    "operator_label": "Visible Worker",
                    "principal_enabled": True,
                    "worker_state": "enabled",
                    "runner_availability": "available",
                    "runner_connection_state": "failed",
                    "diagnostic": "private failure detail",
                }
            ],
            "work_items": [],
        }
    )

    assert view["counts"]["ready"] == 1
    worker = view["ready"][0]
    assert worker["attention_reason"] is None
    assert "private failure detail" not in str(view)


def test_structured_report_dashboard_preserves_stage_next_boundary_and_redacts_content(
    system,
) -> None:
    service = system["service"]
    _production_worker(system)
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Ship /private/plan via https://private.example.test",
            objective=(
                "Keep bearer secret=not-for-dashboard and "
                "credential=also-not-for-dashboard out of the operator edge"
            ),
            maturity="defined",
            acceptance=["A visible dashboard title is present"],
            idempotency_key="dashboard-operator-content",
        ),
    )
    attempt = work["current_attempt"]
    private_report = ReportInput(
        kind="progress",
        expected_goal_version=work["goal_version"],
        expected_generation=work["generation"],
        expected_goal_packet_digest=attempt["goal_packet_digest"],
        expected_task_packet_digest=attempt["task_packet_digest"],
        summary="native thread id: internal-123; token=not-for-dashboard",
        stage="Inspect /private/stage",
        next_boundary="Open https://private.example.test/next",
    )
    with pytest.raises(ValidationError, match="private locator"):
        service.report(system["worker"], attempt["id"], private_report)
    service.report(
        system["worker"],
        attempt["id"],
        private_report.model_copy(
            update={
                "summary": "Structured progress is ready",
                "stage": "Inspect public result",
                "next_boundary": "Review public result",
            }
        ),
    )

    item = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]
    assert item["work_title"] == "Ship [path-redacted] via [uri-redacted]"
    assert item["objective_summary"] == (
        "Keep [secret-redacted] and [secret-redacted] out of the operator edge"
    )
    assert item["latest_worker_report_summary"] == "Structured progress is ready"
    assert item["attempt_state"] == "working"
    assert item["progress_stage"] == "Inspect public result"
    assert item["next_boundary_summary"] == "Review public result"
    assert item["pending_supervisor_boundary"] is False
    assert item["latest_report_kind"] == "progress"
    assert item["latest_report_summary"] == "Structured progress is ready"
    assert isinstance(item["latest_reported_at"], str)
    assert item["runner_adapter"] is None
    assert item["runner_availability"] == "unavailable"
    assert item["runner_state"] == "unavailable"
    assert item["runner_model"] is None and item["runner_reasoning_effort"] is None
    rendered = str(item)
    for forbidden in (
        "/private/plan",
        "private.example.test",
        "not-for-dashboard",
        "also-not-for-dashboard",
        "internal-123",
    ):
        assert forbidden not in rendered


def test_latest_artifact_report_remains_dashboard_visible(system) -> None:
    service = system["service"]
    _production_worker(system)
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Prepare review artifact",
            objective="Expose the latest structured artifact report",
            maturity="defined",
            acceptance=["The artifact report is visible on the Dashboard"],
            idempotency_key="dashboard-latest-artifact-report",
        ),
    )
    attempt = work["current_attempt"]
    common = {
        "expected_goal_version": work["goal_version"],
        "expected_generation": work["generation"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
    }
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="progress",
            summary="Older progress report",
            stage="Drafting",
            next_boundary="Produce the artifact",
            **common,
        ),
    )
    service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind="artifact",
            summary="Artifact is ready for review",
            stage="Artifact ready",
            next_boundary="Review the artifact",
            **common,
        ),
    )

    item = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]
    assert item["latest_report_kind"] == "artifact"
    assert item["latest_report_summary"] == "Artifact is ready for review"
    assert item["progress_stage"] == "Artifact ready"
    assert item["next_boundary_summary"] == "Review the artifact"
    assert isinstance(item["latest_reported_at"], str)


@pytest.mark.parametrize(
    ("adapter", "model", "effort"),
    [
        ("codex-app-server", "gpt-5.6-terra", "high"),
        ("claude", "claude-opus-4.1", "xhigh"),
    ],
)
def test_dashboard_projects_only_the_attachment_scoped_managed_runner_spec(
    system, adapter: str, model: str, effort: str
) -> None:
    item, _attachment = _managed_dashboard_item(system, adapter=adapter, model=model, effort=effort)
    assert item["runner_adapter"] == adapter
    assert item["runner_requested_model"] == model
    assert item["runner_effective_model"] == model
    assert item["runner_requested_reasoning_effort"] == effort
    assert item["runner_effective_reasoning_effort"] == effort
    assert item["runner_model"] == model
    assert item["runner_reasoning_effort"] == effort
    assert item["runner_availability"] == "available"
    assert item["runner_state"] == "enabled"
    assert item["runner_connection_state"] == "connected-idle"
    assert "opaque-workspace-ref" not in str(item)
    assert "input-digest" not in str(item)


def test_dashboard_fails_closed_for_a_cross_attachment_spec(system) -> None:
    item, attachment = _managed_dashboard_item(
        system, adapter="claude", model="claude-sonnet", effort="high"
    )
    assert item["runner_availability"] == "available"
    service = system["service"]
    other = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(native_thread_id="dashboard-other", project_digest="e" * 64),
    )
    service.db.execute(  # type: ignore[union-attr]
        "UPDATE managed_worker_specs SET attachment_id = ?, attachment_generation = ? WHERE attachment_id = ?",
        (other["id"], other["generation"], attachment["id"]),
    )
    tampered = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]  # type: ignore[union-attr,index]
    assert tampered["runner_adapter"] is None
    assert tampered["runner_requested_model"] is None
    assert tampered["runner_effective_reasoning_effort"] is None
    assert tampered["runner_availability"] == "unavailable"
    assert tampered["runner_state"] == "mismatched"
    assert tampered["runner_connection_state"] == "unavailable"


@pytest.mark.parametrize(
    ("runtime_state", "connection_state"),
    [
        ("starting", "enrolling"),
        ("ready", "connected-idle"),
        ("busy", "connected-busy"),
        ("waiting", "enrolled-reopenable"),
        ("stopped", "stopped"),
        ("failed", "failed"),
        ("missing", "missing"),
    ],
)
def test_dashboard_projects_durable_worker_connection_lifecycle(
    system, runtime_state: str, connection_state: str
) -> None:
    _item, _attachment = _managed_dashboard_item(
        system, adapter="codex-app-server", model="gpt-5.6-terra", effort="high"
    )
    service = system["service"]
    runtime = system["runtime"]
    service.db.execute(  # type: ignore[union-attr]
        "UPDATE runtime_sessions SET state = ? WHERE id = ?", (runtime_state, runtime["id"])
    )
    item = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]  # type: ignore[union-attr,index]
    assert item["runner_connection_state"] == connection_state


def test_dashboard_marks_missing_and_unsupported_runner_specs_unavailable(system) -> None:
    service = system["service"]
    _production_worker(system)
    work = service.assign_work(  # type: ignore[union-attr]
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Missing runner",
            objective="Do not infer a missing runtime configuration",
            maturity="defined",
            acceptance=["Runner details stay unavailable"],
            idempotency_key="dashboard-missing-runner-spec",
        ),
    )
    assert work["id"]
    missing = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]  # type: ignore[union-attr,index]
    assert missing["runner_availability"] == "unavailable"
    assert missing["runner_state"] == "unavailable"
    service.db.execute(  # type: ignore[union-attr]
        "UPDATE runtime_sessions SET adapter = 'retired' WHERE id = ?",
        (system["runtime"]["id"],),
    )
    unsupported = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]  # type: ignore[union-attr,index]
    assert unsupported["runner_adapter"] is None
    assert unsupported["runner_model"] is None
    assert unsupported["runner_availability"] == "unavailable"
    assert unsupported["runner_state"] == "unsupported"


def test_dashboard_runner_projection_never_reads_runtime_metadata_or_workspace_refs(system) -> None:
    _item, _attachment = _managed_dashboard_item(
        system, adapter="codex-app-server", model="gpt-5.6-terra", effort="high"
    )
    service = system["service"]
    service.db.execute(  # type: ignore[union-attr]
        "UPDATE runtime_sessions SET metadata_json = ? WHERE id = ?",
        (
            '{"command":["/private/runner"],"cwd":"/private/workspace",'
            '"token":"dashboard-must-not-leak"}',
            system["runtime"]["id"],
        ),
    )
    item = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]  # type: ignore[union-attr,index]
    assert item["runner_effective_model"] == "gpt-5.6-terra"
    rendered = str(item)
    for forbidden in (
        "/private/runner",
        "/private/workspace",
        "dashboard-must-not-leak",
        "opaque-workspace-ref",
    ):
        assert forbidden not in rendered


def test_operator_view_keeps_explicit_close_distinct_from_completed_and_sanitizes_evidence() -> (
    None
):
    """The read DTO is stable across snapshot/history/SSE-triggered reloads."""

    view = build_operator_view(
        {
            "work_items": [
                {
                    "assigned_worker_id": "worker-open",
                    "state": "active",
                    "closure_state": "open",
                },
                {
                    "assigned_worker_id": "worker-awaiting",
                    "state": "completed",
                    "current_attempt_state": "completed",
                },
                {
                    "assigned_worker_id": "worker-closed",
                    "state": "completed",
                    "current_attempt_state": "completed",
                    "closure_state": "closed",
                    "closure_summary": {
                        "requester_decision": "accepted",
                        "cao_review": "ok",
                        "artifact_preservation": "preserved",
                        "cleanup": "verified",
                        "unresolved_deliveries": 0,
                        "unresolved_effects": 0,
                        "active_runtimes": 0,
                        "summary": "private requester text",
                        "cleanup_path": "/private/worktree",
                        "token": "private-token",
                        "sqlite_rowid": 42,
                    },
                },
            ]
        }
    )

    assert [item["closure_state"] for item in view["work_items"]] == [
        "open",
        "awaiting-explicit-close",
        "closed",
    ]
    closed = view["work_items"][2]
    assert closed["requester_decision"] == "accepted"
    assert closed["latest_cao_review_decision"] == "ok"
    assert closed["closure_summary"] == {
        "requester_decision": "accepted",
        "cao_review": "ok",
        "artifact_preservation": "preserved",
        "cleanup": "verified",
        "unresolved_deliveries": 0,
        "unresolved_effects": 0,
        "active_runtimes": 0,
    }
    rendered = str(view)
    assert "/private/worktree" not in rendered
    assert "private requester text" not in rendered
    assert "private-token" not in rendered
    assert "sqlite_rowid" not in rendered


def test_dashboard_history_paginates_without_duplicates_and_sse_replays_last_event_id(system):
    _sensitive_transition(system)
    app, token = _dashboard_app(system)
    headers = {"Authorization": f"Bearer {token}"}
    with closing(TestClient(app, base_url="http://localhost")) as client:
        first = client.get("/api/v1/dashboard/v1/history?limit=1", headers=headers).json()
        second = client.get(
            f"/api/v1/dashboard/v1/history?limit=1&after={first['next_cursor']}",
            headers=headers,
        ).json()
        assert first["items"] and second["items"]
        assert first["items"][0]["cursor"] != second["items"][0]["cursor"]

        snapshot = client.get("/api/v1/dashboard/v1/snapshot", headers=headers).json()
        system["service"].create_principal(
            system["cao"],
            PrincipalCreate(
                name="stream-event",
                role=PrincipalRole.WORKER,
                operator_scope="production",
                operator_label="Stream event Worker",
                metadata={},
            ),
        )
        with client.stream(
            "GET",
            "/api/v1/dashboard/v1/stream?limit=1",
            headers={**headers, "Last-Event-ID": snapshot["cursor"]},
        ) as response:
            assert response.status_code == 200
            text = b"".join(response.iter_bytes()).decode("utf-8")
        assert "event: dashboard-update" in text
        assert "private" not in text
        assert "principal.created" in text


def test_dashboard_history_hides_nonproduction_events_but_advances_its_cursor(system):
    app, _token = _dashboard_app(system)
    model = app.state.dashboard_read_model
    initial_cursor = str(model.snapshot()["cursor"])
    app.state.service.create_principal(
        app.state.service.authenticate(system["cao_token"]),
        PrincipalCreate(name="hidden-external", role=PrincipalRole.EXTERNAL, metadata={}),
    )

    hidden_page = model.history(after=initial_cursor, limit=100)
    assert isinstance(hidden_page, dict)
    assert hidden_page["items"] == []
    assert DashboardCursor.decode(str(hidden_page["next_cursor"])).sequence > (
        DashboardCursor.decode(initial_cursor).sequence
    )

    _production_worker(system)
    system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Visible production boundary",
            objective="Prove filtering does not stall history",
            maturity="defined",
            acceptance=["Production event remains visible"],
            idempotency_key="dashboard-hidden-cursor-production-work",
        ),
    )
    visible_page = model.history(after=str(hidden_page["next_cursor"]), limit=100)
    assert isinstance(visible_page, dict)
    event_types = [item["event"]["type"] for item in visible_page["items"]]
    assert "work.assigned" in event_types
    assert "principal.created" not in event_types


def test_schema39_backfills_deleted_worker_event_scope_and_emits_one_resync_event(
    system,
) -> None:
    service = system["service"]
    _item, attachment = _managed_dashboard_item(
        system,
        adapter="codex-app-server",
        model="gpt-5.6-terra",
        effort="high",
    )
    actor = service.authenticate(attachment["context_token"])
    thread_id = "thread_dashboard_codex-app-server"
    bound_work = service.db.fetchone(
        "SELECT id FROM work_items WHERE assigned_worker_id = ? ORDER BY created_at DESC",
        (system["worker"]["id"],),
    )
    assert bound_work is not None
    bound_work_id = str(bound_work["id"])
    with service.db.transaction() as connection:
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
            "AND name = 'work_items_managed_thread_rebind_update'"
        ).fetchone()
        assert trigger is not None and trigger["sql"]
        connection.execute("DROP TRIGGER work_items_managed_thread_rebind_update")
        connection.execute(
            "UPDATE work_items SET managed_worker_thread_id = ?, "
            "managed_worker_thread_generation = 1 WHERE id = ?",
            (thread_id, bound_work_id),
        )
        connection.execute(str(trigger["sql"]))
    service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            expected_generation=1,
            idempotency_key="schema39-finish-before-delete",
        ),
    )
    service.delete_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=thread_id,
            expected_generation=2,
            idempotency_key="schema39-delete-worker",
        ),
    )
    assert service.db.fetchone(
        "SELECT 1 FROM managed_worker_threads WHERE id = ?", (thread_id,)
    ) is None
    retained_work = service.db.fetchone(
        "SELECT operator_scope FROM work_items WHERE id = ? "
        "AND managed_worker_thread_id = ?",
        (bound_work_id, thread_id),
    )
    assert retained_work is not None and retained_work["operator_scope"] == "production"
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE events SET operator_scope = 'unclassified' "
            "WHERE event_type = 'managed_worker_thread.deleted' AND aggregate_id = ?",
            (thread_id,),
        )
        connection.execute(
            "UPDATE metadata SET value = '38' WHERE key = 'schema_version'"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 39")
        connection.execute("PRAGMA user_version = 38")

    service.db.initialize()
    service.db.initialize()

    event = service.db.fetchone(
        "SELECT operator_scope FROM events "
        "WHERE event_type = 'managed_worker_thread.deleted' AND aggregate_id = ?",
        (thread_id,),
    )
    assert event is not None and event["operator_scope"] == "production"
    assert service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'dashboard.resync_requested'"
    )["count"] == 1
    assert service.db.fetchone(
        "SELECT operator_scope FROM events "
        "WHERE event_type = 'dashboard.resync_requested'"
    )["operator_scope"] == "production"


def test_dashboard_requires_resync_for_generation_change_or_pruned_history(system):
    app, token = _dashboard_app(system)
    headers = {"Authorization": f"Bearer {token}"}
    with closing(TestClient(app, base_url="http://localhost")) as client:
        snapshot = client.get("/api/v1/dashboard/v1/snapshot", headers=headers).json()
        system["service"].db.execute(
            "UPDATE control_authority SET generation = generation + 1 WHERE singleton = 1"
        )
        response = client.get(
            f"/api/v1/dashboard/v1/history?after={snapshot['cursor']}", headers=headers
        )
        assert response.status_code == 409
        assert response.json()["status"] == "resync-required"
        assert response.json()["reason"] == "authority-generation-changed"
        with client.stream(
            "GET",
            f"/api/v1/dashboard/v1/stream?after={snapshot['cursor']}&limit=1",
            headers=headers,
        ) as stream:
            assert stream.status_code == 200
            text = b"".join(stream.iter_bytes()).decode("utf-8")
        assert "event: resync-required" in text

    app, token = _dashboard_app(system)
    headers = {"Authorization": f"Bearer {token}"}
    system["service"].create_principal(
        system["cao"],
        PrincipalCreate(name="prune-event", role=PrincipalRole.EXTERNAL, metadata={}),
    )
    generation = int(
        system["service"].db.fetchone(
            "SELECT generation FROM control_authority WHERE singleton = 1"
        )["generation"]
    )
    system["service"].db.execute("DELETE FROM events WHERE sequence <= 2")
    stale = DashboardCursor(generation, 1).encode()
    with closing(TestClient(app, base_url="http://localhost")) as client:
        response = client.get(f"/api/v1/dashboard/v1/history?after={stale}", headers=headers)
        assert response.status_code == 409
        assert response.json()["reason"] == "cursor-pruned-or-invalid"
        invalid = client.get("/api/v1/dashboard/v1/history?after=not-a-cursor", headers=headers)
        assert invalid.status_code == 422
