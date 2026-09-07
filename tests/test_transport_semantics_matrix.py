from __future__ import annotations

import hashlib
import json
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from enrollment_helpers import enroll_ready_worker_runtime
from fastapi.testclient import TestClient

from cao_control_plane.api import create_app
from cao_control_plane.config import Settings
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
)
from cao_control_plane.models import PrincipalCreate

OBJECTIVE = "Implement the transport conformance matrix"
ACCEPTANCE = ["Canonical records agree"]
IDEMPOTENCY_KEY = "transport-semantics-matrix"


def _settings(
    tmp_path: Path,
    transport: str,
    *,
    require_cao_attachment_for_work: bool = True,
) -> Settings:
    settings = replace(
        Settings(),
        state_dir=tmp_path / transport,
        public_base_url="http://127.0.0.1:8768",
        dispatcher_recovery_scan_seconds=0.01,
        dispatcher_lease_seconds=1.0,
        require_cao_attachment_for_work=require_cao_attachment_for_work,
    )
    settings.ensure_directories()
    return settings


def _new_system(
    tmp_path: Path,
    transport: str,
    *,
    require_cao_attachment_for_work: bool = True,
) -> tuple[Any, Settings, str, str, str]:
    settings = _settings(
        tmp_path,
        transport,
        require_cao_attachment_for_work=require_cao_attachment_for_work,
    )
    app = create_app(settings)
    cao_token = app.state.bootstrap["tokens"]["cao"]["token"]
    cao = app.state.service.authenticate(cao_token)
    worker = app.state.service.create_principal(
        cao,
        PrincipalCreate(name="matrix-worker", role="worker", metadata={}),
    )
    enroll_ready_worker_runtime(app.state.service, cao, worker["principal"]["id"])
    attachment = attach_cao_session_with_peer(
        app.state.service,
        current_cao_session_attachment(
            native_thread_id=f"transport-thread-{transport}",
            project_digest=hashlib.sha256(transport.encode()).hexdigest(),
        ),
    )
    return (
        app,
        settings,
        str(attachment["context_token"]),
        str(cao_token),
        worker["principal"]["id"],
    )


def _modern_request(name: str, arguments: dict[str, Any], request_id: int = 1) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments,
            "_meta": {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {"name": "transport-matrix", "version": "1"},
            },
        },
    }


def _modern_headers(token: str, name: str = "cao_assign") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "MCP-Protocol-Version": MCP_LATEST_VERSION,
        "Mcp-Method": "tools/call",
        "Mcp-Name": name,
    }


def _assignment(worker_id: str) -> dict[str, Any]:
    return {
        "worker_id": worker_id,
        "title": "Transport conformance",
        "objective": OBJECTIVE,
        "maturity": "defined",
        "acceptance": ACCEPTANCE,
        "non_goals": ["Do not create a second authority"],
        "priority": 47,
    }


def _reopen_modern(settings: Settings, token: str, work_id: str) -> None:
    reopened = create_app(settings)
    # This matrix owns only transport-to-domain translation. Entering the app
    # lifespan would start the commit-driven Dispatcher and race the canonical
    # record snapshot with a downstream runtime attempt.
    with closing(TestClient(reopened, base_url="http://127.0.0.1:8768")) as client:
        response = client.post(
            "/mcp",
            headers=_modern_headers(token, "cao_get_work"),
            json=_modern_request("cao_get_work", {"work_item_id": work_id}, request_id=2),
        )
    assert response.status_code == 200
    assert response.json()["result"]["structuredContent"]["id"] == work_id


def _run_modern_mcp(tmp_path: Path) -> dict[str, Any]:
    app, settings, _, token, worker_id = _new_system(
        tmp_path, "modern", require_cao_attachment_for_work=False
    )
    baseline = _event_cursor(app)
    with closing(TestClient(app, base_url="http://127.0.0.1:8768")) as client:
        response = client.post(
            "/mcp",
            headers=_modern_headers(token),
            json=_modern_request("cao_assign", _assignment(worker_id)),
        )
    assert response.status_code == 200
    work_id = response.json()["result"]["structuredContent"]["id"]
    _reopen_modern(settings, token, work_id)
    return _canonical_records(create_app(settings), baseline)


def _run_a2a(tmp_path: Path) -> dict[str, Any]:
    # Conversation credentials are intentionally MCP-only.  A2A conformance
    # therefore exercises the explicit compatibility lane with the global CAO
    # principal and attachment enforcement disabled for this isolated system;
    # the release profile keeps attachment enforcement enabled.
    app, settings, _, token, worker_id = _new_system(
        tmp_path,
        "a2a",
        require_cao_attachment_for_work=False,
    )
    baseline = _event_cursor(app)
    headers = {
        "Authorization": f"Bearer {token}",
        "A2A-Version": "1.0",
        "Content-Type": "application/a2a+json",
    }
    with closing(TestClient(app, base_url="http://127.0.0.1:8768")) as client:
        response = client.post(
            "/a2a/http/message:send",
            headers=headers,
            json={
                "message": {
                    "messageId": IDEMPOTENCY_KEY,
                    "role": "ROLE_USER",
                    "parts": [{"text": OBJECTIVE}],
                    "metadata": {
                        "workerId": worker_id,
                        "title": "Transport conformance",
                        "objective": OBJECTIVE,
                        "maturity": "defined",
                        "acceptance": ACCEPTANCE,
                        "nonGoals": ["Do not create a second authority"],
                        "priority": 47,
                    },
                },
                "configuration": {"returnImmediately": True},
            },
        )
    assert response.status_code == 200
    task = response.json()["task"]

    reopened = create_app(settings)
    with closing(TestClient(reopened, base_url="http://127.0.0.1:8768")) as client:
        persisted = client.get(f"/a2a/http/tasks/{task['id']}", headers=headers)
    assert persisted.status_code == 200
    assert persisted.json()["metadata"]["workItemId"] == task["metadata"]["workItemId"]
    return _canonical_records(reopened, baseline)


def _event_cursor(app: Any) -> int:
    row = app.state.service.db.fetchone("SELECT COALESCE(MAX(sequence), 0) AS sequence FROM events")
    assert row is not None
    return int(row["sequence"])


def _semantic_assignment(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in ("title", "objective", "maturity", "acceptance", "non_goals", "priority")
    }


def _canonical_records(app: Any, after_event: int) -> dict[str, Any]:
    db = app.state.service.db
    roles = {row["id"]: row["role"] for row in db.fetchall("SELECT id, role FROM principals")}
    authority = db.fetchall("SELECT mode FROM control_authority ORDER BY singleton")
    assert [row["mode"] for row in authority] == ["canonical"]

    receipts = db.fetchall("SELECT * FROM source_receipts ORDER BY received_at, id")
    intents = db.fetchall("SELECT * FROM submitted_intents ORDER BY source_receipt_id, ordinal")
    dispositions = db.fetchall("SELECT * FROM intent_dispositions ORDER BY created_at, id")
    work_items = db.fetchall("SELECT * FROM work_items ORDER BY created_at, id")
    goals = db.fetchall("SELECT * FROM goal_revisions ORDER BY work_item_id, version")
    directives = db.fetchall("SELECT * FROM directives ORDER BY created_at, id")
    messages = db.fetchall(
        "SELECT * FROM messages WHERE work_item_id IS NOT NULL ORDER BY sequence"
    )
    events = db.fetchall(
        "SELECT * FROM events WHERE sequence > ? ORDER BY sequence", (after_event,)
    )
    assert all(
        len(records) == 1
        for records in (receipts, intents, dispositions, work_items, goals, directives, messages)
    )
    receipt, intent, disposition, work, goal, directive, message = (
        receipts[0],
        intents[0],
        dispositions[0],
        work_items[0],
        goals[0],
        directives[0],
        messages[0],
    )
    assert intent["source_receipt_id"] == receipt["id"]
    assert disposition["submitted_intent_id"] == intent["id"]
    assert disposition["source_receipt_id"] == receipt["id"]
    assert disposition["result_work_item_id"] == work["id"]
    assert disposition["result_directive_id"] == directive["id"]
    assert goal["work_item_id"] == work["id"]
    assert directive["submitted_intent_id"] == intent["id"]
    assert directive["source_receipt_id"] == receipt["id"]
    assert directive["created_work_item_id"] == work["id"]
    assert message["work_item_id"] == work["id"]

    return {
        "receipts": [(roles[row["source_principal_id"]],) for row in receipts],
        "intents": [(row["ordinal"], roles[row["submitter_id"]]) for row in intents],
        "dispositions": [
            (row["kind"], row["relation"], row["reason"], bool(row["result_work_item_id"]))
            for row in dispositions
        ],
        "work": [
            (
                row["title"],
                row["goal_version"],
                row["state"],
                row["priority"],
                row["attention_owner"],
                roles[row["created_by"]],
                roles[row["assigned_worker_id"]],
            )
            for row in work_items
        ],
        "goals": [
            (
                row["version"],
                row["objective"],
                row["maturity"],
                json.loads(row["acceptance_json"]),
                json.loads(row["non_goals_json"]),
                row["reason"],
                roles[row["created_by"]],
            )
            for row in goals
        ],
        "directives": [
            (
                row["relation"],
                row["expected_goal_version"],
                row["state"],
                row["content"],
                row["reason"],
                bool(row["created_work_item_id"]),
            )
            for row in directives
        ],
        "messages": [
            (
                row["sequence"],
                row["kind"],
                roles[row["sender_id"]],
                row["goal_version"],
                _semantic_assignment(json.loads(row["payload_json"])),
            )
            for row in messages
        ],
        "record_order": [
            (row["event_type"], row["aggregate_type"], roles.get(row["actor_id"], ""))
            for row in events
            if row["event_type"]
            in {
                "intent.received",
                "intent.submitted",
                "message.created",
                "work.assigned",
                "directive.created",
                "intent.classified",
            }
        ],
    }


def test_transport_semantics_matrix_preserves_one_canonical_domain(tmp_path: Path) -> None:
    matrix = {
        "mcp-current": _run_modern_mcp(tmp_path),
        "a2a-1.0": _run_a2a(tmp_path),
    }
    baseline = matrix["mcp-current"]
    assert matrix["a2a-1.0"] == baseline
    assert baseline["record_order"] == [
        ("intent.received", "source_receipt", "cao"),
        ("intent.submitted", "submitted_intent", "cao"),
        ("message.created", "message", "cao"),
        ("work.assigned", "work_item", "cao"),
        ("directive.created", "work_item", "cao"),
        ("intent.classified", "submitted_intent", "cao"),
    ]
