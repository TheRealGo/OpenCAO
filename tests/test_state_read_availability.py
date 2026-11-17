from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from typing import Any

import pytest
from conftest import (
    attach_cao_session_with_peer,
    business_purpose_contract,
    current_cao_session_attachment,
)
from test_worker_thread_lifecycle_core import _seed_managed_thread

from cao_control_plane.database import Database
from cao_control_plane.errors import AuthorizationError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import ReportInput, ReportKind, WorkAssignment
from cao_control_plane.service import ControlPlane


@pytest.fixture
def conversation(system: dict[str, Any]) -> dict[str, Any]:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="state-read-conversation",
            project_digest="a" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Bounded checkpoint",
            objective="Produce one inspectable checkpoint.",
            acceptance=["The checkpoint is inspectable"],
            idempotency_key="state-read-work",
        ),
    )
    return {
        **system,
        "actor": actor,
        "attachment": attachment,
        "server": MCPServer(service),
        "work": work,
    }


def read_work(c: dict[str, Any], server: MCPServer | None = None) -> Any:
    return (server or c["server"]).call_tool(
        c["actor"],
        "cao_get_work",
        {"work_item_id": c["work"]["id"]},
    )


def test_discarded_state_result_can_be_read_again(conversation: dict[str, Any]) -> None:
    first = read_work(conversation)
    expected_id = first["id"]
    del first
    recovered = read_work(conversation)
    assert recovered["id"] == expected_id


@pytest.mark.parametrize(
    "tool",
    [
        "cao_get_work",
        "cao_get_inbox",
        "cao_query",
        "cao_list_managed_workers",
        "cao_read_work_history",
    ],
)
def test_state_reads_are_repeatable_without_native_host(
    conversation: dict[str, Any],
    tool: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cao_control_plane.runtime import _JsonRpcDesktopSocket

    async def unavailable(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("state reads must not depend on native input observation")

    monkeypatch.setattr(_JsonRpcDesktopSocket, "connect", unavailable)
    c = conversation
    args = (
        {"work_item_id": c["work"]["id"]}
        if tool
        in {
            "cao_get_work",
            "cao_read_work_history",
        }
        else {}
    )
    for _ in range(3):
        assert c["server"].call_tool(c["actor"], tool, args) is not None


def test_rereads_and_purpose_change_allow_the_next_instruction(
    conversation: dict[str, Any],
) -> None:
    c = conversation
    worker = _seed_managed_thread(c, c["actor"], ordinal=62001)
    first = c["server"].call_tool(c["actor"], "cao_list_managed_workers", {})
    del first
    recovered = c["server"].call_tool(c["actor"], "cao_list_managed_workers", {})
    current = next(
        item for item in recovered["workers"] if item["worker_thread_id"] == worker["thread_id"]
    )
    purpose = c["server"].call_tool(
        c["actor"],
        "cao_set_worker_purpose",
        {
            "worker_thread_id": worker["thread_id"],
            "purpose": "Maintain service continuity during migration.",
            "contract": business_purpose_contract(),
            "expected_purpose_version": current["worker_purpose"]["version"],
            "reason": "The responsibility includes the replacement service.",
            "idempotency_key": "purpose-before-reread",
        },
    )
    c["server"].call_tool(c["actor"], "cao_list_managed_workers", {})
    assigned = c["server"].call_tool(
        c["actor"],
        "cao_instruct_worker_thread",
        {
            "worker_thread_id": worker["thread_id"],
            "instruction": "Measure current service availability.",
            "expected_purpose_version": purpose["worker_purpose"]["version"],
            "idempotency_key": "instruction-after-reread",
        },
    )
    work_id = assigned["task"]["work_item_id"]
    assert (
        c["server"].call_tool(
            c["actor"],
            "cao_get_work",
            {
                "work_item_id": work_id,
            },
        )["id"]
        == work_id
    )


@pytest.mark.parametrize("revoked", [False, True])
def test_rereads_preserve_attachment_and_work_authority(
    conversation: dict[str, Any],
    revoked: bool,
) -> None:
    c = conversation
    read_work(c)
    if revoked:
        c["service"].db.execute(
            "UPDATE cao_conversation_credentials SET state='revoked' WHERE id=?",
            (c["actor"]["_cao_conversation_credential_id"],),
        )
        actor = c["actor"]
    else:
        attachment = attach_cao_session_with_peer(
            c["service"],
            current_cao_session_attachment(
                native_thread_id="independent-conversation",
                project_digest="a" * 64,
            ),
        )
        actor = c["service"].authenticate(attachment["context_token"])
    with pytest.raises(AuthorizationError):
        c["server"].call_tool(actor, "cao_get_work", {"work_item_id": c["work"]["id"]})


def test_concurrent_state_reads_both_succeed(conversation: dict[str, Any]) -> None:
    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(lambda _: read_work(conversation)["id"], range(2)))
    assert ids == [conversation["work"]["id"]] * 2


def test_reread_does_not_require_a_database_writer_lock(conversation: dict[str, Any]) -> None:
    with closing(conversation["service"].db.connect()) as writer:
        writer.execute("BEGIN IMMEDIATE")
        try:
            read_work(conversation)
            read_work(conversation)
            assert writer.in_transaction
        finally:
            writer.rollback()


def test_failed_state_read_can_be_retried(
    conversation: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = conversation["service"].get_work

    def failing(*args: Any) -> None:
        raise ValueError("temporary read failure")

    monkeypatch.setattr(conversation["service"], "get_work", failing)
    with pytest.raises(ValueError, match="temporary read failure"):
        read_work(conversation)
    monkeypatch.setattr(conversation["service"], "get_work", original)
    assert read_work(conversation)["id"] == read_work(conversation)["id"]


def test_notifications_and_ack_do_not_consume_read_availability(
    conversation: dict[str, Any],
) -> None:
    c = conversation
    attempt = c["work"]["current_attempt"]
    c["service"].report(
        c["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            summary="Checkpoint prepared for review",
            expected_generation=c["work"]["generation"],
            expected_goal_version=c["work"]["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            idempotency_key="checkpoint-notification",
        ),
    )
    [notice] = c["server"].call_tool(c["actor"], "cao_get_inbox", {})["items"]
    args = {"message_id": notice["id"]}
    c["server"].call_tool(c["actor"], "cao_get_inbox", args)
    c["server"].call_tool(c["actor"], "cao_ack", {"message_ids": [notice["id"]]})
    reread = c["server"].call_tool(c["actor"], "cao_get_inbox", args)
    assert reread["items"][0]["id"] == notice["id"]
    assert "notification-only turn" in reread["supervision_guidance"]
    assert read_work(c)["id"] == c["work"]["id"]


def test_bridge_and_service_replacement_keep_reads_available(
    conversation: dict[str, Any],
) -> None:
    c = conversation
    read_work(c)
    attached = attach_cao_session_with_peer(
        c["service"],
        current_cao_session_attachment(
            native_thread_id="state-read-conversation",
            project_digest="a" * 64,
        ),
    )
    c["actor"] = c["service"].authenticate(attached["context_token"])
    restarted = ControlPlane(Database(c["settings"]), c["settings"])
    server = MCPServer(restarted)
    assert read_work(c, server)["id"] == read_work(c, server)["id"]


def test_http_result_can_be_recovered_without_another_input(
    conversation: dict[str, Any],
) -> None:
    from fastapi.testclient import TestClient
    from test_api import _modern_body, _modern_headers

    from cao_control_plane.api import create_app

    c = conversation
    client = TestClient(create_app(c["settings"]), base_url="http://localhost")
    try:
        for request_id in (1, 2):
            response = client.post(
                "/mcp",
                headers=_modern_headers(
                    c["attachment"]["context_token"], "tools/call", name="cao_get_work"
                ),
                json=_modern_body(
                    "tools/call",
                    {
                        "name": "cao_get_work",
                        "arguments": {"work_item_id": c["work"]["id"]},
                    },
                    request_id=request_id,
                ),
            )
            assert response.status_code == 200
            result = response.json()["result"]
            assert result["isError"] is False
            assert result["structuredContent"]["id"] == c["work"]["id"]
    finally:
        client.close()


def test_schema_upgrade_removes_only_legacy_read_counters(
    conversation: dict[str, Any],
) -> None:
    c = conversation
    db = c["service"].db
    for table in ("cao_state_read_inputs", "cao_state_read_cursors"):
        db.execute(f"CREATE TABLE {table}(counter TEXT)")
        db.execute(f"INSERT INTO {table} VALUES('old-read')")
    db.execute("UPDATE metadata SET value='52' WHERE key='schema_version'")
    db.execute("PRAGMA user_version=52")
    db.execute("DELETE FROM schema_migrations WHERE version=53")
    db.initialize()
    assert not db.fetchall(
        "SELECT name FROM sqlite_master WHERE name IN ('cao_state_read_inputs','cao_state_read_cursors')"
    )
    assert db.fetchone("PRAGMA user_version")[0] == 53
    assert read_work(c)["id"] == c["work"]["id"]
