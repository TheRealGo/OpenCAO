from __future__ import annotations

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_multi_connection_backend import (
    _CATALOG_A,
    _RELEASE_A,
    _attach_current,
    _identity,
    _install_process_identities,
)
from test_worker_thread_lifecycle_core import (
    _prepare_runtime_recovery,
    _seed_managed_thread,
    _seeded_managed_work_assignment,
)

from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.dashboard import DashboardReadModel
from cao_control_plane.database import utc_after, utc_now
from cao_control_plane.errors import AuthorizationError, ConflictError
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    CompletionContract,
    InstructWorkerThreadInput,
    MessageKind,
    ReportInput,
    ReportKind,
    RequesterDecisionInput,
    ResumeWorkerThreadInput,
    ReviewInput,
    ReviewVerdict,
    RuntimeHeartbeat,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.runtime import (
    RuntimeAdapterError,
    _communicate_limited,
    _ManagedWorkerActivityMonitor,
)
from cao_control_plane.service import WORKER_MCP_REQUIRED_TOOLS


def _two_connection_actors(
    system: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    pid_base: int,
    thread_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    service = system["service"]
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    root_one = _identity(pid_base + 1, parent_pid=1, signature=f"root-{pid_base}-one")
    bridge_one = _identity(
        pid_base + 101, parent_pid=root_one.pid, signature=f"bridge-{pid_base}-one"
    )
    root_two = _identity(pid_base + 2, parent_pid=1, signature=f"root-{pid_base}-two")
    bridge_two = _identity(
        pid_base + 102, parent_pid=root_two.pid, signature=f"bridge-{pid_base}-two"
    )
    _install_process_identities(monkeypatch, root_one, bridge_one, root_two, bridge_two)
    first = _attach_current(
        service,
        peer=bridge_one,
        catalog_digest=_CATALOG_A,
        thread_id=thread_id,
    )
    second = _attach_current(
        service,
        peer=bridge_two,
        catalog_digest=_CATALOG_A,
        thread_id=thread_id,
    )
    return (
        first,
        second,
        service.authenticate(first["context_token"]),
        service.authenticate(second["context_token"]),
    )


def _expire_connection(service: Any, actor: dict[str, Any]) -> None:
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE cao_attachment_connections "
            "SET state = 'expired', revoked_at = ?, updated_at = ? WHERE id = ?",
            (now, now, actor["_cao_connection_id"]),
        )
        connection.execute(
            "UPDATE cao_conversation_credentials "
            "SET state = 'expired', revoked_at = ?, updated_at = ? WHERE id = ?",
            (now, now, actor["_cao_conversation_credential_id"]),
        )


def _question_work(system: dict[str, Any], actor: dict[str, Any], key: str) -> dict[str, Any]:
    service = system["service"]
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Durable reply context",
            objective="Answer one exact Worker question exactly once.",
            acceptance=["The reply disposition and instruction are durable."],
            idempotency_key=f"{key}:assign",
        ),
    )
    attempt = work["current_attempt"]
    return service.report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=ReportKind.QUESTION,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary="Which exact option should the Worker use?",
            idempotency_key=f"{key}:question",
        ),
    )


def _completed_accepted_work(
    system: dict[str, Any], actor: dict[str, Any], key: str
) -> dict[str, Any]:
    service = system["service"]
    work = service.assign_work(
        actor,
        WorkAssignment(
            worker_id=system["worker"]["id"],
            runtime_session_id=system["runtime"]["id"],
            title="Idempotent Work runtime stop",
            objective="Reach one exact accepted terminal Work generation.",
            acceptance=["The runtime stop commits one terminal transition."],
            completion_contract=CompletionContract.NO_ARTIFACT_EXPECTED,
            idempotency_key=f"{key}:assign",
        ),
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
            summary="The no-artifact acceptance is satisfied.",
            idempotency_key=f"{key}:completion",
        ),
    )
    boundary = reported["open_boundaries"][-1]
    turn = service.acquire_reasoner_turn(
        actor,
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key=f"{key}:turn",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind=BoundaryDispositionKind.ACCEPT,
            reason="The exact completion claim is ready for review.",
        ),
    )
    reviewed = service.review(
        actor,
        ReviewInput(
            attempt_id=attempt["id"],
            verdict=ReviewVerdict.OK,
            summary="The declared no-artifact result is verified.",
            idempotency_key=f"{key}:review",
        ),
    )
    return service.record_requester_decision(
        actor,
        RequesterDecisionInput(
            review_id=reviewed["reviews"][-1]["id"],
            verdict="accepted",
            summary="The requester accepted this exact result.",
            conversation_evidence_id=f"{key}:requester-decision",
            idempotency_key=f"{key}:decision",
        ),
    )


def test_generic_runtime_stop_rejects_every_conversation_bearer_before_target_read(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    service = system["service"]
    root_one = _identity(8101, parent_pid=1, signature="generic-stop-root-one")
    bridge_one = _identity(8201, parent_pid=root_one.pid, signature="generic-stop-bridge-one")
    root_two = _identity(8102, parent_pid=1, signature="generic-stop-root-two")
    bridge_two = _identity(8202, parent_pid=root_two.pid, signature="generic-stop-bridge-two")
    root_bootstrap = _identity(8103, parent_pid=1, signature="generic-stop-root-bootstrap")
    bridge_bootstrap = _identity(
        8203, parent_pid=root_bootstrap.pid, signature="generic-stop-bridge-bootstrap"
    )
    _install_process_identities(
        monkeypatch,
        root_one,
        bridge_one,
        root_two,
        bridge_two,
        root_bootstrap,
        bridge_bootstrap,
    )
    service.reconcile_conversation_tool_catalog(_CATALOG_A, _RELEASE_A)
    first = _attach_current(
        service,
        peer=bridge_one,
        catalog_digest=_CATALOG_A,
        thread_id="generic-stop-scope",
    )
    second = _attach_current(
        service,
        peer=bridge_two,
        catalog_digest=_CATALOG_A,
        thread_id="generic-stop-scope",
    )
    stale_csc = service.authenticate(first["context_token"])
    live_csc = service.authenticate(second["context_token"])
    wake_ticket = service.issue_cao_runtime_launch_ticket(first["runtime_session_id"])
    crc = service.authenticate(
        service.exchange_cao_runtime_launch_ticket(wake_ticket["ticket"])["token"]
    )
    bootstrap = service.issue_owner_local_attachment_bootstrap(
        bridge_bootstrap,
        "generic-stop-scope",
        "c" * 64,
        _CATALOG_A,
        CAO_CONVERSATION_PROXY_ABI_VERSION,
    )
    cab = service.authenticate(bootstrap)
    _expire_connection(service, stale_csc)

    def snapshot() -> tuple[Any, ...]:
        return (
            tuple(
                service.db.fetchone(
                    "SELECT * FROM runtime_sessions WHERE id = ?",
                    (system["runtime"]["id"],),
                )
            ),
            tuple(
                service.db.fetchone(
                    "SELECT * FROM worker_enrollments WHERE runtime_session_id = ?",
                    (system["runtime"]["id"],),
                )
            ),
            int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
        )

    before = snapshot()
    for scoped_actor in (stale_csc, live_csc, crc, cab):
        with pytest.raises(AuthorizationError):
            service.stop_runtime(scoped_actor, system["runtime"]["id"])
        assert snapshot() == before


def test_reply_same_key_linearizes_once_and_conflicting_digest_is_non_mutating(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _first, _second, actor_one, actor_two = _two_connection_actors(
        system,
        monkeypatch,
        pid_base=8300,
        thread_id="reply-linearization",
    )
    service = system["service"]
    waiting = _question_work(system, actor_one, "reply-linearization")
    boundary_id = waiting["open_boundaries"][-1]["id"]
    ready = Barrier(2)

    def submit(actor: dict[str, Any]) -> dict[str, Any]:
        ready.wait(timeout=5)
        return service.reply(
            actor,
            waiting["id"],
            "Use the exact deterministic option A.",
            idempotency_key="reply-two-connections",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, (actor_one, actor_two)))

    assert results[0]["id"] == results[1]["id"] == waiting["id"]
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM boundary_dispositions WHERE boundary_id = ?",
                (boundary_id,),
            )["count"]
        )
        == 1
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM reasoner_turns WHERE boundary_id = ?",
                (boundary_id,),
            )["count"]
        )
        == 1
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM idempotency_results "
                "WHERE actor_id = ? AND operation = 'reply_context'",
                (actor_one["id"],),
            )["count"]
        )
        == 0
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM idempotency_results "
                "WHERE actor_id = ? AND operation = 'reply'",
                (actor_one["id"],),
            )["count"]
        )
        == 1
    )

    before = (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (waiting["id"],))),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"]),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM idempotency_results")["count"]),
    )
    with pytest.raises(ConflictError, match="idempotency key"):
        service.reply(
            actor_two,
            waiting["id"],
            "Use a conflicting option B.",
            idempotency_key="reply-two-connections",
        )
    after = (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (waiting["id"],))),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM messages")["count"]),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM idempotency_results")["count"]),
    )
    assert after == before


def test_reply_final_receipt_failure_rolls_back_and_retry_succeeds_atomically(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _first, _second, actor_one, actor_two = _two_connection_actors(
        system,
        monkeypatch,
        pid_base=8500,
        thread_id="reply-crash-resume",
    )
    service = system["service"]
    waiting = _question_work(system, actor_one, "reply-crash-resume")
    boundary_id = waiting["open_boundaries"][-1]["id"]
    original_put = service._idempotent_put
    crash_pending = True

    rollback_tables = (
        "work_items",
        "attempts",
        "boundaries",
        "boundary_dispositions",
        "reasoner_turns",
        "events",
        "managed_worker_thread_epochs",
        "messages",
        "message_deliveries",
        "idempotency_results",
    )

    def snapshot() -> dict[str, tuple[tuple[Any, ...], ...]]:
        with service.db.connect() as connection:
            return {
                table: tuple(
                    tuple(row)
                    for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
                )
                for table in rollback_tables
            }

    def crash_before_final_receipt(
        connection: Any,
        actor_id: str,
        operation: str,
        key: str,
        request_digest: str,
        result: dict[str, Any],
    ) -> None:
        nonlocal crash_pending
        if operation == "reply" and crash_pending:
            crash_pending = False
            raise RuntimeError("simulated reply receipt crash")
        original_put(connection, actor_id, operation, key, request_digest, result)

    before = snapshot()
    monkeypatch.setattr(service, "_idempotent_put", crash_before_final_receipt)
    with pytest.raises(RuntimeError, match="simulated reply receipt crash"):
        service.reply(
            actor_one,
            waiting["id"],
            "Resume this exact reply after the receipt crash.",
            idempotency_key="reply-crash-key",
        )
    monkeypatch.setattr(service, "_idempotent_put", original_put)
    assert snapshot() == before

    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM boundary_dispositions WHERE boundary_id = ?",
                (boundary_id,),
            )["count"]
        )
        == 0
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM idempotency_results "
                "WHERE operation = 'reply_context' AND idempotency_key = ?",
                ("provided:reply-crash-key",),
            )["count"]
        )
        == 0
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM idempotency_results "
            "WHERE operation = 'reply' AND idempotency_key = 'reply-crash-key'"
        )
        is None
    )

    resumed = service.reply(
        actor_two,
        waiting["id"],
        "Resume this exact reply after the receipt crash.",
        idempotency_key="reply-crash-key",
    )
    assert resumed["id"] == waiting["id"]
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM boundary_dispositions WHERE boundary_id = ?",
                (boundary_id,),
            )["count"]
        )
        == 1
    )
    turn = service.db.fetchone(
        "SELECT state FROM reasoner_turns WHERE boundary_id = ?", (boundary_id,)
    )
    assert turn is not None and turn["state"] == "completed"
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM messages "
                "WHERE work_item_id = ? AND kind = 'instruction'",
                (waiting["id"],),
            )["count"]
        )
        == 1
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM idempotency_results "
                "WHERE actor_id = ? AND operation = 'reply_context'",
                (actor_one["id"],),
            )["count"]
        )
        == 0
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM idempotency_results "
                "WHERE actor_id = ? AND operation = 'reply' "
                "AND idempotency_key = 'reply-crash-key'",
                (actor_one["id"],),
            )["count"]
        )
        == 1
    )


def test_stop_work_runtime_has_one_terminal_transition_across_two_connections(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _first, _second, actor_one, actor_two = _two_connection_actors(
        system,
        monkeypatch,
        pid_base=8700,
        thread_id="stop-work-runtime-linearization",
    )
    service = system["service"]
    completed = _completed_accepted_work(system, actor_one, "stop-work-linearization")
    runtime_id = str(completed["current_attempt"]["runtime_session_id"])
    enrollment_before = service.db.fetchone(
        "SELECT state, generation FROM worker_enrollments WHERE runtime_session_id = ?",
        (runtime_id,),
    )
    assert enrollment_before is not None
    event_count_before = int(
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'runtime.stopped' AND aggregate_id = ?",
            (runtime_id,),
        )["count"]
    )
    monkeypatch.setattr(service, "_cleanup_runtime_launch_artifacts", lambda _ticket_ids: None)
    ready = Barrier(2)

    def stop(actor: dict[str, Any]) -> dict[str, Any]:
        ready.wait(timeout=5)
        return service.stop_work_runtime(actor, completed["id"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(stop, (actor_one, actor_two)))

    assert {result["state"] for result in results} == {"completed"}
    enrollment_after = service.db.fetchone(
        "SELECT state, generation FROM worker_enrollments WHERE runtime_session_id = ?",
        (runtime_id,),
    )
    assert dict(enrollment_after) == {
        "state": "revoked",
        "generation": int(enrollment_before["generation"]) + 1,
    }
    assert (
        service.db.fetchone("SELECT state FROM runtime_sessions WHERE id = ?", (runtime_id,))[
            "state"
        ]
        == "stopped"
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM events "
                "WHERE event_type = 'runtime.stopped' AND aggregate_id = ?",
                (runtime_id,),
            )["count"]
        )
        == event_count_before + 1
    )

    service.stop_work_runtime(actor_two, completed["id"])
    stable_before = (
        tuple(
            service.db.fetchone(
                "SELECT * FROM worker_enrollments WHERE runtime_session_id = ?",
                (runtime_id,),
            )
        ),
        tuple(service.db.fetchone("SELECT * FROM runtime_sessions WHERE id = ?", (runtime_id,))),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
    )
    _expire_connection(service, actor_one)
    with pytest.raises(AuthorizationError):
        service.stop_work_runtime(actor_one, completed["id"])
    stable_after = (
        tuple(
            service.db.fetchone(
                "SELECT * FROM worker_enrollments WHERE runtime_session_id = ?",
                (runtime_id,),
            )
        ),
        tuple(service.db.fetchone("SELECT * FROM runtime_sessions WHERE id = ?", (runtime_id,))),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
    )
    assert stable_after == stable_before


def test_finish_one_public_call_archives_working_thread_and_preserves_unknown_evidence(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _first, _second, actor_one, actor_two = _two_connection_actors(
        system,
        monkeypatch,
        pid_base=8900,
        thread_id="finish-working-thread",
    )
    service = system["service"]
    ids = _seed_managed_thread(
        system,
        actor_one,
        ordinal=1901,
        native_session_id="retained-provider-resume-handle",
    )
    conflicting = _seed_managed_thread(system, actor_one, ordinal=1902)
    work = service.assign_work(
        actor_one,
        _seeded_managed_work_assignment(
            ids,
            title="Working thread explicit Close",
            objective="Close active supervision without erasing unknown outcomes.",
            acceptance=["The retained thread becomes resumably archived."],
            idempotency_key="finish-working-assignment",
        ),
    )
    attempt_id = work["current_attempt"]["id"]
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE runtime_sessions SET state = 'busy', updated_at = ? WHERE id = ?",
            (now, ids["runtime_id"]),
        )
        assignment = connection.execute(
            """
            SELECT delivery.message_id, delivery.recipient_id
            FROM message_deliveries AS delivery
            JOIN messages AS message ON message.id = delivery.message_id
            WHERE message.attempt_id = ? AND message.kind = 'assignment'
            """,
            (attempt_id,),
        ).fetchone()
        assert assignment is not None
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'dispatched', owner_token = 'finish-unknown-owner',
                lease_until = ?, last_error = 'outcome-unknown', updated_at = ?
            WHERE message_id = ? AND recipient_id = ?
            """,
            (
                utc_after(300),
                now,
                assignment["message_id"],
                assignment["recipient_id"],
            ),
        )
        queued = service._message(
            connection,
            sender_id=str(actor_one["id"]),
            recipient_id=ids["principal_id"],
            kind=MessageKind.INSTRUCTION,
            payload={"instruction": "unclaimed instruction before Close"},
            work_item_id=work["id"],
            attempt_id=attempt_id,
            goal_version=work["goal_version"],
            idempotency_key="finish-unclaimed-instruction",
        )
        connection.execute(
            """
            INSERT INTO artifacts(
                id, work_item_id, attempt_id, producer_id, name, uri,
                media_type, digest, metadata_json, created_at
            ) VALUES(
                'art_finish_unknown', ?, ?, ?, 'retained.txt',
                'artifact://retained-finish-evidence', 'text/plain', ?, '{}', ?
            )
            """,
            (work["id"], attempt_id, ids["principal_id"], "a" * 64, now),
        )
        connection.execute(
            """
            INSERT INTO effect_operations(
                id, principal_id, kind, target, action, status, evidence,
                cleanup_work_item_id, created_at, updated_at
            ) VALUES(
                'eff_finish_unknown', ?, 'external', 'opaque-target',
                'opaque-action', 'unknown', '', ?, ?, ?
            )
            """,
            (ids["principal_id"], work["id"], now, now),
        )

    dispatched_before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (assignment["message_id"], assignment["recipient_id"]),
        )
    )
    effect_before = dict(
        service.db.fetchone("SELECT * FROM effect_operations WHERE id = 'eff_finish_unknown'")
    )
    artifact_before = dict(
        service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_finish_unknown'")
    )
    dashboard = DashboardReadModel(service)
    assert any(
        worker["worker_label"] == "Managed lifecycle Worker 1901"
        for category in ("needs_attention", "working", "ready")
        for worker in dashboard.snapshot()["operator"][category]
    )

    request = WorkerThreadLifecycleInput(
        worker_thread_id=ids["thread_id"],
        expected_generation=1,
        idempotency_key="finish-working-public",
    )
    server = MCPServer(service)
    ready = Barrier(2)

    def finish(actor: dict[str, Any]) -> dict[str, Any]:
        ready.wait(timeout=5)
        return server.call_tool(
            actor,
            "cao_finish_worker_thread",
            request.model_dump(mode="json"),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(finish, (actor_one, actor_two)))

    assert results == [
        {
            "worker_thread_id": ids["thread_id"],
            "state": "archived",
            "generation": 2,
        },
        {
            "worker_thread_id": ids["thread_id"],
            "state": "archived",
            "generation": 2,
        },
    ]
    assert service.get_work(work["id"])["state"] == "canceled"
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
                (assignment["message_id"], assignment["recipient_id"]),
            )
        )
        == dispatched_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM effect_operations WHERE id = 'eff_finish_unknown'"))
        == effect_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_finish_unknown'"))
        == artifact_before
    )
    assert dict(
        service.db.fetchone(
            "SELECT state, last_error FROM message_deliveries WHERE message_id = ?",
            (queued["id"],),
        )
    ) == {"state": "dead", "last_error": "worker_thread_finished"}
    assert dict(
        service.db.fetchone(
            "SELECT state, generation FROM managed_worker_threads WHERE id = ?",
            (ids["thread_id"],),
        )
    ) == {"state": "archived", "generation": 2}
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],)
        )["state"]
        == "stopped"
    )
    assert (
        service.db.fetchone("SELECT enabled FROM principals WHERE id = ?", (ids["principal_id"],))[
            "enabled"
        ]
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT native_session_id FROM runtime_sessions WHERE id = ?", (ids["runtime_id"],)
        )["native_session_id"]
        == "retained-provider-resume-handle"
    )
    listed = {
        worker["worker_thread_id"]: worker for worker in service.list_managed_workers(actor_two)
    }
    assert listed[ids["thread_id"]]["thread_state"] == "archived"
    assert listed[ids["thread_id"]]["thread_generation"] == 2
    after_dashboard = dashboard.snapshot()["operator"]
    assert all(
        worker["worker_label"] != "Managed lifecycle Worker 1901"
        for category in ("needs_attention", "working", "ready", "inactive_workers")
        for worker in after_dashboard[category]
    )

    # Finish retains the dispatched outcome and unknown effect. Resume and a
    # distinct instruction preserve both records without retrying them.
    resumed = service.resume_worker_thread(
        actor_two,
        ResumeWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=2,
            idempotency_key="resume-retained-unknown-without-task",
        ),
    )
    assert resumed == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "active",
        "thread_generation": 3,
        "connection_state": "pending",
        "can_accept_instruction": True,
    }
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
                (assignment["message_id"], assignment["recipient_id"]),
            )
        )
        == dispatched_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM effect_operations WHERE id = 'eff_finish_unknown'"))
        == effect_before
    )
    instructed = service.instruct_worker_thread(
        actor_two,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=3,
            objective="Create a distinct Work without retrying retained evidence.",
            idempotency_key="instruction-after-retained-unknown",
        ),
    )
    assert instructed["task"]["work_item_id"] != work["id"]

    with pytest.raises(ConflictError, match="idempotency key"):
        service.finish_worker_thread(
            actor_two,
            WorkerThreadLifecycleInput(
                worker_thread_id=conflicting["thread_id"],
                expected_generation=1,
                idempotency_key=request.idempotency_key,
            ),
        )
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_threads WHERE id = ?",
            (conflicting["thread_id"],),
        )["state"]
        == "active"
    )

    _expire_connection(service, actor_one)
    stable_before = (
        tuple(
            service.db.fetchone(
                "SELECT * FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
            )
        ),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
    )
    with pytest.raises(AuthorizationError):
        service.finish_worker_thread(actor_one, request)
    stable_after = (
        tuple(
            service.db.fetchone(
                "SELECT * FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
            )
        ),
        int(service.db.fetchone("SELECT COUNT(*) AS count FROM events")["count"]),
    )
    assert stable_after == stable_before

    archived_again = service.finish_worker_thread(
        actor_two,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=3,
            idempotency_key="finish-retained-unknown-again",
        ),
    )
    assert archived_again["thread_generation"] == 4
    deleted = service.delete_worker_thread(
        actor_two,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=4,
            idempotency_key="delete-archived-retained-unknown",
        ),
    )
    assert deleted == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "deleted",
        "thread_generation": 5,
    }
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
                (assignment["message_id"], assignment["recipient_id"]),
            )
        )
        == dispatched_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM effect_operations WHERE id = 'eff_finish_unknown'"))
        == effect_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_finish_unknown'"))
        == artifact_before
    )


def test_finish_one_public_call_archives_system_reconciliation_without_erasing_unknowns(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="finish-system-reconciliation",
            project_digest="f" * 64,
        ),
    )
    attached = service.authenticate(attachment["context_token"])
    ids, failed, boundary, _request = _prepare_runtime_recovery(
        system,
        attached,
        ordinal=1903,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    assert failed["current_attempt"]["stage"] == "system_reconciliation"
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            """
            INSERT INTO artifacts(
                id, work_item_id, attempt_id, producer_id, name, uri,
                media_type, digest, metadata_json, created_at
            ) VALUES(
                'art_finish_reconciliation', ?, ?, ?, 'unknown.txt',
                'artifact://system-reconciliation-evidence', 'text/plain', ?, '{}', ?
            )
            """,
            (
                failed["id"],
                failed["current_attempt"]["id"],
                ids["principal_id"],
                "b" * 64,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO effect_operations(
                id, principal_id, kind, target, action, status, evidence,
                cleanup_work_item_id, created_at, updated_at
            ) VALUES(
                'eff_finish_reconciliation', ?, 'external', 'opaque-target',
                'opaque-action', 'unknown', '', ?, ?, ?
            )
            """,
            (ids["principal_id"], failed["id"], now, now),
        )
        connection.execute(
            "UPDATE events SET data_json = ? "
            "WHERE event_type = 'boundary.recorded' AND aggregate_id = ? "
            "AND json_extract(data_json, '$.boundary_id') = ?",
            ('{"boundary_id":"retained-anomaly"}', failed["id"], boundary["id"]),
        )
    boundary_before = dict(
        service.db.fetchone("SELECT * FROM boundaries WHERE id = ?", (boundary["id"],))
    )
    artifact_before = dict(
        service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_finish_reconciliation'")
    )
    effect_before = dict(
        service.db.fetchone(
            "SELECT * FROM effect_operations WHERE id = 'eff_finish_reconciliation'"
        )
    )
    boundary_event_before = dict(
        service.db.fetchone(
            "SELECT * FROM events WHERE event_type = 'boundary.recorded' AND aggregate_id = ?",
            (failed["id"],),
        )
    )

    result = MCPServer(service).call_tool(
        attached,
        "cao_finish_worker_thread",
        {
            "worker_thread_id": ids["thread_id"],
            "expected_generation": 1,
            "idempotency_key": "finish-system-reconciliation",
        },
    )

    assert result == {
        "worker_thread_id": ids["thread_id"],
        "state": "archived",
        "generation": 2,
    }
    current = service.get_work(failed["id"])
    assert current["state"] == "canceled"
    assert [item["id"] for item in current["open_boundaries"]] == [boundary["id"]]
    assert current["boundaries"][-1]["disposition"] is None
    assert current["boundaries"][-1]["supersession"] is None
    assert (
        dict(service.db.fetchone("SELECT * FROM boundaries WHERE id = ?", (boundary["id"],)))
        == boundary_before
    )
    assert (
        dict(service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_finish_reconciliation'"))
        == artifact_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM effect_operations WHERE id = 'eff_finish_reconciliation'"
            )
        )
        == effect_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM events WHERE event_type = 'boundary.recorded' AND aggregate_id = ?",
                (failed["id"],),
            )
        )
        == boundary_event_before
    )
    anomaly = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
        "AND aggregate_id = ?",
        (ids["thread_id"],),
    )
    assert anomaly is not None
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?", (boundary["id"],)
        )
        is None
    )
    archived = next(
        worker
        for worker in service.list_managed_workers(attached)
        if worker["worker_thread_id"] == ids["thread_id"]
    )
    assert (archived["thread_state"], archived["thread_generation"]) == ("archived", 2)


@pytest.mark.parametrize(
    "historical_delivery_state",
    ["queued", "expired_leased", "delivered", "acknowledged"],
)
def test_instruct_accepts_distinct_work_after_completed_waiting_user_history(
    system: dict[str, Any], historical_delivery_state: str
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"instruct-after-{historical_delivery_state}",
            project_digest="9" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=1910 if historical_delivery_state == "delivered" else 1911,
    )
    prior = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Historical completed Worker attempt",
            objective="Remain waiting for requester review without blocking distinct work.",
            acceptance=["The prior ledger remains unchanged."],
            idempotency_key=f"historical-{historical_delivery_state}-assignment",
        ),
    )
    prior_attempt_id = str(prior["current_attempt"]["id"])
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'completed', updated_at = ? WHERE id = ?",
            (now, prior_attempt_id),
        )
        connection.execute(
            "UPDATE work_items SET state = 'waiting_user', attention_owner = 'user', "
            "updated_at = ? WHERE id = ?",
            (now, prior["id"]),
        )
        status = service._message(
            connection,
            sender_id=str(actor["id"]),
            recipient_id=ids["principal_id"],
            kind=MessageKind.STATUS_REQUEST,
            payload={"status": "terminal historical status"},
            work_item_id=prior["id"],
            attempt_id=prior_attempt_id,
            goal_version=prior["goal_version"],
            idempotency_key=f"historical-{historical_delivery_state}-status",
        )
        stored_state = (
            "leased" if historical_delivery_state == "expired_leased" else historical_delivery_state
        )
        owner_token = "expired-historical-owner" if stored_state == "leased" else ""
        lease_until = "2000-01-01T00:00:00+00:00" if stored_state == "leased" else None
        connection.execute(
            "UPDATE message_deliveries SET state = ?, owner_token = ?, "
            "lease_until = ?, updated_at = ? WHERE message_id IN ("
            "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment') "
            "OR message_id = ?",
            (
                stored_state,
                owner_token,
                lease_until,
                now,
                prior_attempt_id,
                status["id"],
            ),
        )
        connection.execute(
            "UPDATE runtime_sessions SET state = 'stopped', updated_at = ? WHERE id = ?",
            (now, ids["runtime_id"]),
        )
    prior_work_before = tuple(
        service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (prior["id"],))
    )
    prior_attempt_before = tuple(
        service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (prior_attempt_id,))
    )
    prior_deliveries_before = [
        dict(row)
        for row in service.db.fetchall(
            "SELECT delivery.* FROM message_deliveries AS delivery "
            "JOIN messages AS message ON message.id = delivery.message_id "
            "WHERE message.attempt_id = ? ORDER BY delivery.message_id",
            (prior_attempt_id,),
        )
    ]
    listed = next(
        worker
        for worker in service.list_managed_workers(actor)
        if worker["worker_thread_id"] == ids["thread_id"]
    )
    assert listed["can_accept_instruction"] is True

    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            title="Distinct follow-up",
            objective="Queue one new, explicitly distinct instruction.",
            maturity="defined",
            acceptance=["A new Work exists without settling the prior Work."],
            idempotency_key=f"instruct-after-{historical_delivery_state}",
        ),
    )

    assert instructed["task"]["work_item_id"] != prior["id"]
    assert instructed["task"]["status"] == "active"
    assert (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (prior["id"],)))
        == prior_work_before
    )
    assert (
        tuple(service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (prior_attempt_id,)))
        == prior_attempt_before
    )
    prior_deliveries_after = [
        dict(row)
        for row in service.db.fetchall(
            "SELECT delivery.* FROM message_deliveries AS delivery "
            "JOIN messages AS message ON message.id = delivery.message_id "
            "WHERE message.attempt_id = ? ORDER BY delivery.message_id",
            (prior_attempt_id,),
        )
    ]
    assert prior_deliveries_after == prior_deliveries_before


@pytest.mark.parametrize(
    ("attempt_state", "work_state"),
    [
        ("suspended", "suspended"),
        ("waiting_supervisor", "waiting_supervisor"),
        ("input_required", "waiting_user"),
        ("blocked", "suspended"),
        ("submitted", "waiting_review"),
    ],
)
def test_instruct_accepts_distinct_work_after_coordination_owned_attempt(
    system: dict[str, Any], attempt_state: str, work_state: str
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"instruct-after-{attempt_state}",
            project_digest="7" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    ordinal = {
        "suspended": 1920,
        "waiting_supervisor": 1921,
        "input_required": 1922,
        "blocked": 1923,
        "submitted": 1924,
    }[attempt_state]
    ids = _seed_managed_thread(system, actor, ordinal=ordinal)
    prior = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title=f"Coordination-owned {attempt_state} history",
            objective="Keep this prior Work while accepting one distinct instruction.",
            acceptance=["The prior coordination ledger stays byte-identical."],
            idempotency_key=f"coordination-{attempt_state}-assignment",
        ),
    )
    attempt_id = str(prior["current_attempt"]["id"])
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = ?, updated_at = ? WHERE id = ?",
            (attempt_state, now, attempt_id),
        )
        connection.execute(
            "UPDATE work_items SET state = ?, attention_owner = 'cao', updated_at = ? WHERE id = ?",
            (work_state, now, prior["id"]),
        )
        connection.execute(
            "UPDATE message_deliveries SET state = 'delivered', owner_token = '', "
            "lease_until = NULL, updated_at = ? WHERE message_id IN ("
            "SELECT id FROM messages WHERE attempt_id = ?)",
            (now, attempt_id),
        )
        connection.execute(
            "UPDATE runtime_sessions SET state = 'stopped', updated_at = ? WHERE id = ?",
            (now, ids["runtime_id"]),
        )
    prior_rows = (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (prior["id"],))),
        tuple(service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (attempt_id,))),
        tuple(
            service.db.fetchone(
                "SELECT delivery.* FROM message_deliveries AS delivery "
                "JOIN messages AS message ON message.id = delivery.message_id "
                "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
                (attempt_id,),
            )
        ),
    )
    listed = next(
        worker
        for worker in service.list_managed_workers(actor)
        if worker["worker_thread_id"] == ids["thread_id"]
    )
    assert listed["can_accept_instruction"] is True

    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            objective=f"Create a distinct instruction after {attempt_state} history.",
            idempotency_key=f"coordination-{attempt_state}-instruction",
        ),
    )

    assert instructed["task"]["work_item_id"] != prior["id"]
    assert (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (prior["id"],))),
        tuple(service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (attempt_id,))),
        tuple(
            service.db.fetchone(
                "SELECT delivery.* FROM message_deliveries AS delivery "
                "JOIN messages AS message ON message.id = delivery.message_id "
                "WHERE message.attempt_id = ? AND message.kind = 'assignment'",
                (attempt_id,),
            )
        ),
    ) == prior_rows


@pytest.mark.parametrize("attempt_state", ["assigned", "accepted", "working"])
def test_instruct_accepts_distinct_work_during_execution_owned_attempt(
    system: dict[str, Any], attempt_state: str
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"instruct-blocks-{attempt_state}",
            project_digest="6" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal={"assigned": 1925, "accepted": 1926, "working": 1927}[attempt_state],
    )
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title=f"Execution-owned {attempt_state} Work",
            objective="Keep the execution lane exclusive.",
            acceptance=["A distinct instruction cannot overlap active execution."],
            idempotency_key=f"execution-{attempt_state}-assignment",
        ),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = ?, updated_at = ? WHERE id = ?",
            (attempt_state, utc_now(), work["current_attempt"]["id"]),
        )
    listed = next(
        worker
        for worker in service.list_managed_workers(actor)
        if worker["worker_thread_id"] == ids["thread_id"]
    )
    assert listed["can_accept_instruction"] is True
    prior = tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],)))
    instructed = MCPServer(service).call_tool(
        actor,
        "cao_instruct_worker_thread",
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            objective="Create one distinct Work while execution continues.",
            idempotency_key=f"execution-{attempt_state}-instruction",
        ).model_dump(mode="json", exclude_none=True),
    )
    assert instructed["task"]["work_item_id"] != work["id"]
    assert (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))) == prior
    )


@pytest.mark.parametrize(
    "unresolved_kind",
    ["unexpired_leased", "dispatched", "unknown_effect"],
)
def test_instruct_preserves_unknown_evidence_while_accepting_distinct_work(
    system: dict[str, Any], unresolved_kind: str
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id=f"instruct-blocked-{unresolved_kind}",
            project_digest="8" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal={"unexpired_leased": 1912, "dispatched": 1913, "unknown_effect": 1914}[
            unresolved_kind
        ],
    )
    prior = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Terminal history with a real unresolved outcome",
            objective="Keep exact claimed or unknown evidence fenced.",
            acceptance=["The next instruction is non-mutating while the outcome is unresolved."],
            idempotency_key=f"blocked-{unresolved_kind}-assignment",
        ),
    )
    attempt_id = str(prior["current_attempt"]["id"])
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'completed', updated_at = ? WHERE id = ?",
            (now, attempt_id),
        )
        connection.execute(
            "UPDATE work_items SET state = 'waiting_user', attention_owner = 'user', "
            "updated_at = ? WHERE id = ?",
            (now, prior["id"]),
        )
        if unresolved_kind == "unknown_effect":
            connection.execute(
                "UPDATE message_deliveries SET state = 'delivered', owner_token = '', "
                "lease_until = NULL, updated_at = ? WHERE message_id IN ("
                "SELECT id FROM messages WHERE attempt_id = ?)",
                (now, attempt_id),
            )
            connection.execute(
                """
                INSERT INTO effect_operations(
                    id, principal_id, kind, target, action, status, evidence,
                    cleanup_work_item_id, created_at, updated_at
                ) VALUES(?, ?, 'external', 'opaque-target', 'opaque-action',
                         'unknown', '', ?, ?, ?)
                """,
                (
                    f"eff_instruct_{unresolved_kind}",
                    ids["principal_id"],
                    prior["id"],
                    now,
                    now,
                ),
            )
        else:
            delivery_state = "leased" if unresolved_kind == "unexpired_leased" else "dispatched"
            connection.execute(
                "UPDATE message_deliveries SET state = ?, owner_token = ?, "
                "lease_until = ?, updated_at = ? WHERE message_id IN ("
                "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'assignment')",
                (
                    delivery_state,
                    f"owner-{unresolved_kind}",
                    utc_after(300),
                    now,
                    attempt_id,
                ),
            )

    evidence_before = (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (prior["id"],))),
        tuple(
            service.db.fetchall(
                "SELECT * FROM message_deliveries WHERE message_id IN (SELECT id FROM messages WHERE work_item_id = ?) ORDER BY message_id, recipient_id",
                (prior["id"],),
            )
        ),
        tuple(
            service.db.fetchall(
                "SELECT * FROM effect_operations WHERE cleanup_work_item_id = ? ORDER BY id",
                (prior["id"],),
            )
        ),
    )
    instructed = MCPServer(service).call_tool(
        actor,
        "cao_instruct_worker_thread",
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            objective="Create distinct Work while preserving unresolved evidence.",
            idempotency_key=f"blocked-{unresolved_kind}-instruction",
        ).model_dump(mode="json", exclude_none=True),
    )
    assert instructed["task"]["work_item_id"] != prior["id"]
    assert evidence_before == (
        tuple(service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (prior["id"],))),
        tuple(
            service.db.fetchall(
                "SELECT * FROM message_deliveries WHERE message_id IN (SELECT id FROM messages WHERE work_item_id = ?) ORDER BY message_id, recipient_id",
                (prior["id"],),
            )
        ),
        tuple(
            service.db.fetchall(
                "SELECT * FROM effect_operations WHERE cleanup_work_item_id = ? ORDER BY id",
                (prior["id"],),
            )
        ),
    )


def test_finish_is_the_only_public_close_step_for_active_and_waiting_user_workers(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _first, _second, actor, _other_actor = _two_connection_actors(
        system,
        monkeypatch,
        pid_base=9100,
        thread_id="finish-many-workers",
    )
    service = system["service"]
    active_ids = _seed_managed_thread(system, actor, ordinal=1905)
    waiting_ids = _seed_managed_thread(system, actor, ordinal=1906)
    active_work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            active_ids,
            title="Active Worker closed directly",
            objective="Close this active Worker with one lifecycle command.",
            acceptance=["No Work-close pipeline is required."],
            idempotency_key="finish-many-active-assignment",
        ),
    )
    waiting_work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            waiting_ids,
            title="Waiting-user Worker closed directly",
            objective="Close this reviewed Worker without a requester-decision pipeline.",
            acceptance=["One Finish call archives the retained handle."],
            completion_contract=CompletionContract.NO_ARTIFACT_EXPECTED,
            idempotency_key="finish-many-waiting-assignment",
        ),
    )
    waiting_attempt = waiting_work["current_attempt"]
    launch = service.issue_runtime_launch_ticket(
        waiting_ids["runtime_id"], attempt_id=waiting_attempt["id"]
    )
    worker = service.authenticate(service.exchange_runtime_launch_ticket(launch["ticket"])["token"])
    service.record_mcp_tool_discovery(
        worker,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker,
        waiting_ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker["_enrollment_generation"],
            sequence=1,
        ),
    )
    reported = service.report(
        worker,
        waiting_attempt["id"],
        ReportInput(
            kind=ReportKind.COMPLETION_CLAIM,
            expected_goal_version=waiting_work["goal_version"],
            expected_goal_packet_digest=waiting_attempt["goal_packet_digest"],
            expected_task_packet_digest=waiting_attempt["task_packet_digest"],
            expected_generation=waiting_work["generation"],
            summary="The no-artifact result is complete.",
            idempotency_key="finish-many-waiting-completion",
        ),
    )
    boundary = reported["open_boundaries"][-1]
    turn = service.acquire_reasoner_turn(
        actor,
        waiting_work["id"],
        boundary_id=boundary["id"],
        expected_generation=reported["generation"],
        idempotency_key="finish-many-waiting-turn",
    )
    service.dispose_boundary(
        actor,
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=reported["generation"],
            kind=BoundaryDispositionKind.ACCEPT,
            reason="The result is ready for exact CAO review.",
        ),
    )
    waiting_user = service.review(
        actor,
        ReviewInput(
            attempt_id=waiting_attempt["id"],
            verdict=ReviewVerdict.OK,
            summary="The result is verified and awaiting the requester.",
            idempotency_key="finish-many-waiting-review",
        ),
    )
    assert waiting_user["state"] == "waiting_user"

    server = MCPServer(service)
    public_tools = {str(tool["name"]) for tool in server.tools_for(actor)}
    assert "cao_finish_worker_thread" in public_tools
    assert {
        "cao_stop_work_runtime",
        "cao_prepare_work_close",
        "cao_execute_prepared_cleanup",
        "cao_close_work",
    }.isdisjoint(public_tools)

    results = [
        server.call_tool(
            actor,
            "cao_finish_worker_thread",
            {
                "worker_thread_id": ids["thread_id"],
                "expected_generation": 1,
                "idempotency_key": key,
            },
        )
        for ids, key in (
            (active_ids, "finish-many-active"),
            (waiting_ids, "finish-many-waiting-user"),
        )
    ]
    assert [result["state"] for result in results] == ["archived", "archived"]
    assert [result["generation"] for result in results] == [2, 2]
    assert service.get_work(active_work["id"])["state"] == "canceled"
    assert service.get_work(waiting_work["id"])["state"] == "canceled"
    listed = {worker["worker_thread_id"]: worker for worker in service.list_managed_workers(actor)}
    for ids in (active_ids, waiting_ids):
        assert listed[ids["thread_id"]]["thread_state"] == "archived"
        assert listed[ids["thread_id"]]["thread_generation"] == 2


def test_finish_enrollment_fence_drives_active_adapter_termination_path(
    system: dict[str, Any],
) -> None:
    service = system["service"]
    attachment = attach_cao_session_with_peer(
        service,
        current_cao_session_attachment(
            native_thread_id="finish-monitor-termination",
            project_digest="e" * 64,
        ),
    )
    actor = service.authenticate(attachment["context_token"])
    ids = _seed_managed_thread(system, actor, ordinal=1904)
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Active adapter termination",
            objective="Drive the exact managed adapter termination path on Close.",
            acceptance=["Enrollment revocation wakes the activity monitor."],
            idempotency_key="finish-monitor-assignment",
        ),
    )
    launch = service.issue_runtime_launch_ticket(
        ids["runtime_id"], attempt_id=work["current_attempt"]["id"]
    )
    worker_actor = service.authenticate(
        service.exchange_runtime_launch_ticket(launch["ticket"])["token"]
    )
    service.record_mcp_tool_discovery(
        worker_actor,
        protocol_version="2025-06-18",
        tool_names=WORKER_MCP_REQUIRED_TOOLS,
    )
    service.heartbeat_runtime(
        worker_actor,
        ids["runtime_id"],
        RuntimeHeartbeat(
            expected_enrollment_generation=worker_actor["_enrollment_generation"],
            sequence=1,
        ),
    )

    async def exercise() -> None:
        primed = asyncio.Event()

        class PrimedActivityMonitor(_ManagedWorkerActivityMonitor):
            def _snapshot(self) -> tuple[str, int, int, int]:
                snapshot = super()._snapshot()
                if self._authenticated(snapshot):
                    primed.set()
                return snapshot

        monitor = PrimedActivityMonitor(
            service.db,
            runtime_id=ids["runtime_id"],
            attempt_id=work["current_attempt"]["id"],
            expected_generation=worker_actor["_enrollment_generation"],
            startup_timeout_seconds=1,
            inactivity_timeout_seconds=10,
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        communicating = asyncio.create_task(
            _communicate_limited(
                process,
                stdin=None,
                timeout=None,
                limit=1024,
                activity_monitor=monitor,
                hard_timeout=5,
            )
        )
        await asyncio.wait_for(primed.wait(), timeout=1)
        service.finish_worker_thread(
            actor,
            WorkerThreadLifecycleInput(
                worker_thread_id=ids["thread_id"],
                expected_generation=1,
                idempotency_key="finish-monitor-termination",
            ),
        )
        with pytest.raises(RuntimeAdapterError, match="runtime_dispatch_failed"):
            await asyncio.wait_for(communicating, timeout=5)
        assert process.returncode is not None

    asyncio.run(exercise())
    assert (
        service.db.fetchone(
            "SELECT state FROM worker_enrollments WHERE id = ?", (ids["enrollment_id"],)
        )["state"]
        == "revoked"
    )
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )["state"]
        == "archived"
    )
