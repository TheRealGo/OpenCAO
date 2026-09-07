from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any

import pytest
from conftest import attach_cao_session_with_peer, current_cao_session_attachment
from test_worker_thread_lifecycle_core import (
    _attached as _lifecycle_attached,
)
from test_worker_thread_lifecycle_core import (
    _prepare_runtime_recovery,
    _revoke_and_reattach_cao,
    _run_after_role_check_gate,
    _seed_managed_thread,
    _seeded_managed_work_assignment,
)

import cao_control_plane.service as service_module
from cao_control_plane.database import utc_after, utc_now
from cao_control_plane.errors import (
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    NotFoundError,
)
from cao_control_plane.mcp import MCPServer
from cao_control_plane.models import (
    DeleteWorkerThreadInput,
    InstructWorkerThreadInput,
    ResumeWorkerThreadInput,
    WorkerThreadLifecycleInput,
)
from cao_control_plane.projection import verify_projection
from cao_control_plane.service import _digest

PROJECT_DIGEST = "a" * 64


def _attached(
    system: dict[str, Any], native_thread_id: str, project: str = PROJECT_DIGEST
) -> dict[str, Any]:
    attachment = attach_cao_session_with_peer(
        system["service"],
        current_cao_session_attachment(
            native_thread_id=native_thread_id,
            project_digest=project,
        ),
    )
    return system["service"].authenticate(attachment["context_token"])


def _delete_request(
    thread_id: str,
    generation: int,
    key: str,
) -> DeleteWorkerThreadInput:
    return DeleteWorkerThreadInput(
        worker_thread_id=thread_id,
        expected_generation=generation,
        idempotency_key=key,
    )


def _full_ledger_snapshot(service: Any) -> dict[str, tuple[tuple[object, ...], ...]]:
    with service.db.connect() as connection:
        table_names = tuple(
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
        return {
            table: tuple(
                tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
            )
            for table in table_names
        }


def test_delete_cross_attachment_dead_transport_terminalizes_recovery_and_replays(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _attached(system, "delete-source-conversation")
    ids, failed, boundary, _ = _prepare_runtime_recovery(
        system,
        source,
        ordinal=1801,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    current = _attached(system, "delete-current-conversation")
    service = system["service"]
    assignment = service.db.fetchone(
        """
        SELECT delivery.message_id, delivery.recipient_id
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (failed["current_attempt"]["id"],),
    )
    assert assignment is not None
    boundary_event = service.db.fetchone(
        "SELECT sequence FROM events WHERE event_type = 'boundary.recorded' "
        "AND json_extract(data_json, '$.boundary_id') = ?",
        (boundary["id"],),
    )
    assert boundary_event is not None
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            """
            UPDATE cao_session_attachments
            SET generation = 4, state = 'active', lease_expires_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (utc_after(3600), now, source["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_attachment_connections SET generation = 4, "
            "peer_pid = 2147483647, peer_start_signature = 'dead-peer-generation', "
            "updated_at = ? "
            "WHERE attachment_id = ? AND state = 'active'",
            (now, source["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_conversation_credentials SET generation = 4 "
            "WHERE attachment_id = ? AND state = 'active'",
            (source["_cao_attachment_id"],),
        )
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'dispatched', owner_token = 'unknown-dispatch-owner',
                lease_until = ?, last_error = 'worker_instruction_outcome_unknown',
                updated_at = ?
            WHERE message_id = ? AND recipient_id = ?
            """,
            (
                utc_after(300),
                now,
                assignment["message_id"],
                assignment["recipient_id"],
            ),
        )
        connection.execute(
            """
            INSERT INTO artifacts(
                id, work_item_id, attempt_id, producer_id, name, uri,
                media_type, digest, metadata_json, created_at
            ) VALUES(
                'art_delete_cross_attachment', ?, ?, ?, 'durable.txt',
                'artifact://durable-delete-evidence', 'text/plain', ?, '{}', ?
            )
            """,
            (
                failed["id"],
                failed["current_attempt"]["id"],
                ids["principal_id"],
                "1" * 64,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO effect_operations(
                id, principal_id, kind, target, action, status, evidence,
                cleanup_work_item_id, created_at, updated_at
            ) VALUES(
                'eff_delete_cross_attachment', ?, 'external', 'opaque-target',
                'opaque-action', 'unknown', '', ?, ?, ?
            )
            """,
            (ids["principal_id"], failed["id"], now, now),
        )
        connection.execute(
            """
            INSERT INTO provider_runtime_circuits(
                scope_digest, state, failure_code, cooldown_until,
                restart_authorized, source_runtime_session_id,
                source_boundary_id, source_work_item_id,
                source_boundary_sequence, probe_restart_override,
                probe_runtime_session_id, probe_attempt_id, probe_message_id,
                probe_instruction_sequence, probe_outcome_state,
                opened_at, updated_at
            ) VALUES(
                'delete-cross-provider-scope', 'open',
                'runtime_provider_rate_limited', ?, 0, ?, ?, ?, ?,
                0, NULL, NULL, '', 0, 'none', ?, ?
            )
            """,
            (
                utc_after(300),
                ids["runtime_id"],
                boundary["id"],
                failed["id"],
                boundary_event["sequence"],
                now,
                now,
            ),
        )

    dispatched_before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
            (assignment["message_id"], assignment["recipient_id"]),
        )
    )
    effect_before = dict(
        service.db.fetchone(
            "SELECT * FROM effect_operations WHERE id = 'eff_delete_cross_attachment'"
        )
    )
    artifact_before = dict(
        service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_delete_cross_attachment'")
    )
    circuit_before = dict(
        service.db.fetchone(
            "SELECT * FROM provider_runtime_circuits "
            "WHERE scope_digest = 'delete-cross-provider-scope'"
        )
    )

    monkeypatch.setattr(
        service,
        "_cleanup_runtime_launch_artifacts",
        lambda _ticket_ids: pytest.fail("Delete touched local launch artifacts"),
    )
    request = _delete_request(
        ids["thread_id"],
        1,
        "delete-cross-attachment-recovery",
    )
    result = service.delete_worker_thread(current, request)

    assert result == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "deleted",
        "thread_generation": 3,
    }
    assert service.delete_worker_thread(current, request) == result
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_thread_epochs WHERE thread_id = ?",
            (ids["thread_id"],),
        )
        is None
    )
    current_work = service.get_work(failed["id"])
    assert current_work["state"] == "canceled"
    assert current_work["open_boundaries"] == []
    assert current_work["boundaries"][-1]["disposition"] is None
    assert current_work["boundaries"][-1]["supersession"]["reason"] == "work_canceled"
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
        dict(
            service.db.fetchone(
                "SELECT * FROM effect_operations WHERE id = 'eff_delete_cross_attachment'"
            )
        )
        == effect_before
    )
    assert (
        dict(
            service.db.fetchone("SELECT * FROM artifacts WHERE id = 'art_delete_cross_attachment'")
        )
        == artifact_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM provider_runtime_circuits "
                "WHERE scope_digest = 'delete-cross-provider-scope'"
            )
        )
        == circuit_before
    )
    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_specs WHERE id = ?", (ids["spec_id"],)
        )["state"]
        == "revoked"
    )
    assert (
        service.db.fetchone("SELECT enabled FROM principals WHERE id = ?", (ids["principal_id"],))[
            "enabled"
        ]
        == 0
    )
    source_attachment = service.db.fetchone(
        "SELECT state, generation FROM cao_session_attachments WHERE id = ?",
        (source["_cao_attachment_id"],),
    )
    assert dict(source_attachment) == {"state": "active", "generation": 4}
    deletion_event = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'managed_worker_thread.deleted' AND aggregate_id = ?",
        (ids["thread_id"],),
    )
    assert deletion_event is not None
    deletion_data = json.loads(str(deletion_event["data_json"]))
    assert deletion_data["terminalized_work_count"] == 1
    assert deletion_data["preserved_unknown_delivery_count"] >= 1
    assert deletion_data["preserved_unknown_effect_count"] == 1
    assert deletion_data["conversation_evidence_digest"] == _digest(
        f"explicit-delete:{current['_cao_attachment_id']}:{request.worker_thread_id}:"
        f"{request.idempotency_key}"
    )
    before_rejected_replay = _full_ledger_snapshot(service)
    with pytest.raises(NotFoundError, match="managed Worker thread"):
        service.delete_worker_thread(
            current,
            request.model_copy(update={"expected_generation": 2}),
        )
    assert _full_ledger_snapshot(service) == before_rejected_replay


def test_delete_active_work_preserves_claimed_delivery_and_unknown_effects(
    system: dict[str, Any],
) -> None:
    actor = _attached(system, "delete-active-working")
    ids = _seed_managed_thread(
        system,
        actor,
        ordinal=1802,
        native_session_id="provider-native-unknown",
    )
    service = system["service"]
    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            title="Active Work deleted by explicit requester authority",
            objective="Preserve unknown outcomes while ending CAO supervision.",
            acceptance=["The logical Worker is deleted."],
            idempotency_key="assign-active-delete-authority",
        ),
    )
    work = service.get_work(str(instructed["task"]["work_item_id"]))
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
            SET state = 'dispatched', owner_token = 'claimed-dispatch',
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
            sender_id=str(actor["id"]),
            recipient_id=ids["principal_id"],
            kind=service_module.MessageKind.INSTRUCTION,
            payload={"instruction": "queued but never claimed"},
            work_item_id=work["id"],
            attempt_id=attempt_id,
            goal_version=work["goal_version"],
            idempotency_key="queued-before-delete",
        )
        claimed_queued = service._message(
            connection,
            sender_id=str(actor["id"]),
            recipient_id=ids["principal_id"],
            kind=service_module.MessageKind.INSTRUCTION,
            payload={"instruction": "queued row has contradictory claim evidence"},
            work_item_id=work["id"],
            attempt_id=attempt_id,
            goal_version=work["goal_version"],
            idempotency_key="claimed-queued-before-delete",
        )
        connection.execute(
            """
            UPDATE message_deliveries
            SET owner_token = 'queued-claim-owner', lease_until = ?, updated_at = ?
            WHERE message_id = ? AND recipient_id = ? AND state = 'queued'
            """,
            (utc_after(300), now, claimed_queued["id"], ids["principal_id"]),
        )
        leased = service._message(
            connection,
            sender_id=str(actor["id"]),
            recipient_id=ids["principal_id"],
            kind=service_module.MessageKind.INSTRUCTION,
            payload={"instruction": "leased with an unknown current owner"},
            work_item_id=work["id"],
            attempt_id=attempt_id,
            goal_version=work["goal_version"],
            idempotency_key="leased-before-delete",
        )
        connection.execute(
            """
            UPDATE message_deliveries
            SET state = 'leased', owner_token = 'active-lease-owner',
                lease_until = ?, updated_at = ?
            WHERE message_id = ? AND recipient_id = ?
            """,
            (utc_after(300), now, leased["id"], ids["principal_id"]),
        )
        for effect_id, status in (
            ("eff_delete_active_started", "started"),
            ("eff_delete_active_unknown", "unknown"),
        ):
            connection.execute(
                """
                INSERT INTO effect_operations(
                    id, principal_id, kind, target, action, status, evidence,
                    cleanup_work_item_id, created_at, updated_at
                ) VALUES(?, ?, 'external', 'opaque-target', 'opaque-action', ?, '', ?, ?, ?)
                """,
                (effect_id, ids["principal_id"], status, work["id"], now, now),
            )

    dispatched_before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ?",
            (assignment["message_id"],),
        )
    )
    leased_before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ?", (leased["id"],)
        )
    )
    claimed_queued_before = dict(
        service.db.fetchone(
            "SELECT * FROM message_deliveries WHERE message_id = ?",
            (claimed_queued["id"],),
        )
    )
    effects_before = {
        str(row["id"]): dict(row)
        for row in service.db.fetchall(
            "SELECT * FROM effect_operations WHERE id LIKE 'eff_delete_active_%' ORDER BY id"
        )
    }
    result = MCPServer(service).call_tool(
        actor,
        "cao_delete_worker_thread",
        _delete_request(ids["thread_id"], 1, "delete-active-working").model_dump(mode="json"),
    )

    assert result == {
        "worker_thread_id": ids["thread_id"],
        "state": "deleted",
        "generation": 3,
    }
    assert service.get_work(work["id"])["state"] == "canceled"
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ?",
                (assignment["message_id"],),
            )
        )
        == dispatched_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ?", (leased["id"],)
            )
        )
        == leased_before
    )
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM message_deliveries WHERE message_id = ?",
                (claimed_queued["id"],),
            )
        )
        == claimed_queued_before
    )
    queued_delivery = service.db.fetchone(
        "SELECT state, last_error FROM message_deliveries WHERE message_id = ?",
        (queued["id"],),
    )
    assert dict(queued_delivery) == {
        "state": "dead",
        "last_error": "worker_thread_deleted",
    }
    assert {
        str(row["id"]): dict(row)
        for row in service.db.fetchall(
            "SELECT * FROM effect_operations WHERE id LIKE 'eff_delete_active_%' ORDER BY id"
        )
    } == effects_before
    deletion_event = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'managed_worker_thread.deleted' AND aggregate_id = ?",
        (ids["thread_id"],),
    )
    assert json.loads(str(deletion_event["data_json"]))["preserved_unknown_delivery_count"] >= 3


def test_delete_cross_attachment_fences_stale_and_foreign_then_fences_live_source(
    system: dict[str, Any],
) -> None:
    source = _lifecycle_attached(system, suffix="a")
    current = _attached(system, "delete-scope-current")
    foreign_project = _attached(
        system,
        "delete-scope-foreign-project",
        project="b" * 64,
    )
    ids = _seed_managed_thread(system, source, ordinal=1803)
    service = system["service"]
    work = service.assign_work(
        source,
        _seeded_managed_work_assignment(
            ids,
            title="Scope-fenced Delete",
            objective="Only the exact same-project owner may delete this Worker.",
            acceptance=["Foreign identifiers remain indistinguishable."],
            idempotency_key="assign-delete-scope",
        ),
    )
    with pytest.raises(NotFoundError):
        service.delete_worker_thread(
            foreign_project,
            _delete_request(ids["thread_id"], 1, "delete-foreign-project"),
        )
    with pytest.raises(NotFoundError):
        service.delete_worker_thread(
            current,
            _delete_request("mwt_unknown_delete_target", 1, "delete-unknown-thread"),
        )
    with pytest.raises(ConflictError) as stale:
        service.delete_worker_thread(
            current,
            _delete_request(ids["thread_id"], 2, "delete-stale-generation"),
        )
    assert stale.value.details["reason_code"] == "worker_thread_generation_conflict"
    deleted = service.delete_worker_thread(
        current,
        DeleteWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            idempotency_key="delete-without-terminal-authority",
        ),
    )
    assert deleted == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "deleted",
        "thread_generation": 3,
    }
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert service.get_work(work["id"])["state"] == "canceled"
    assert (
        service.db.fetchone(
            "SELECT state FROM cao_session_attachments WHERE id = ?",
            (source["_cao_attachment_id"],),
        )["state"]
        == "active"
    )
    with pytest.raises(ControlPlaneError):
        service.assign_work(
            source,
            _seeded_managed_work_assignment(
                ids,
                title="Source loses the exact deleted Worker authority",
                objective="A later source command cannot revive the deleted thread.",
                acceptance=["The stale Worker command is rejected."],
                idempotency_key="assign-after-live-source-delete",
            ),
        )


def test_delete_removes_exact_legacy_stopped_terminal_ledger(system: dict[str, Any]) -> None:
    actor = _attached(system, "delete-legacy-stopped")
    ids = _seed_managed_thread(system, actor, ordinal=1804)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Legacy stopped Delete",
            objective="Remove only the terminal CAO supervision ledger.",
            acceptance=["The old thread cannot be resumed."],
            idempotency_key="assign-legacy-stopped-delete",
        ),
    )
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE work_items SET state = 'canceled', attention_owner = 'none', "
            "generation = generation + 1, updated_at = ? WHERE id = ?",
            (now, work["id"]),
        )
        connection.execute(
            "UPDATE attempts SET state = 'canceled', updated_at = ? WHERE id = ?",
            (now, work["current_attempt"]["id"]),
        )
        connection.execute(
            "UPDATE managed_worker_thread_epochs SET retired_at = ? WHERE thread_id = ?",
            (now, ids["thread_id"]),
        )
        connection.execute(
            "UPDATE managed_worker_threads SET state = 'legacy_stopped', generation = 2, "
            "updated_at = ? WHERE id = ?",
            (now, ids["thread_id"]),
        )
        connection.execute(
            "UPDATE managed_worker_specs SET state = 'stopped', stopped_at = ?, "
            "updated_at = ? WHERE id = ?",
            (now, now, ids["spec_id"]),
        )

    result = service.delete_worker_thread(
        actor,
        _delete_request(ids["thread_id"], 2, "delete-legacy-stopped-ledger"),
    )

    assert result == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "deleted",
        "thread_generation": 3,
    }
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert service.get_work(work["id"])["state"] == "canceled"


@pytest.mark.parametrize(
    ("legacy_stopped", "ordinal", "expected_generation"),
    ((False, 1805, 1), (True, 1806, 2)),
)
def test_delete_exact_worker_thread_without_any_work(
    system: dict[str, Any],
    legacy_stopped: bool,
    ordinal: int,
    expected_generation: int,
) -> None:
    actor = _attached(system, f"delete-no-work-{ordinal}")
    ids = _seed_managed_thread(system, actor, ordinal=ordinal)
    service = system["service"]
    if legacy_stopped:
        now = utc_now()
        with service.db.transaction() as connection:
            connection.execute(
                "UPDATE managed_worker_thread_epochs SET retired_at = ? WHERE thread_id = ?",
                (now, ids["thread_id"]),
            )
            connection.execute(
                "UPDATE managed_worker_threads SET state = 'legacy_stopped', generation = 2, "
                "updated_at = ? WHERE id = ?",
                (now, ids["thread_id"]),
            )
            connection.execute(
                "UPDATE managed_worker_specs SET state = 'stopped', stopped_at = ?, "
                "updated_at = ? WHERE id = ?",
                (now, now, ids["spec_id"]),
            )

    result = service.delete_worker_thread(
        actor,
        _delete_request(
            ids["thread_id"],
            expected_generation,
            f"delete-no-work-{ordinal}",
        ),
    )

    assert result == {
        "worker_thread_id": ids["thread_id"],
        "thread_state": "deleted",
        "thread_generation": 3,
    }
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert (
        int(
            service.db.fetchone(
                "SELECT COUNT(*) AS count FROM work_items WHERE assigned_worker_id = ?",
                (ids["principal_id"],),
            )["count"]
        )
        == 0
    )


def test_delete_waiting_user_preserves_completed_attempt_evidence(
    system: dict[str, Any],
) -> None:
    actor = _attached(system, "delete-waiting-user")
    ids = _seed_managed_thread(system, actor, ordinal=1807)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Waiting requester acceptance",
            objective="Preserve the completed Worker evidence during explicit Delete.",
            acceptance=["The completed Attempt remains unchanged."],
            idempotency_key="assign-waiting-user-delete",
        ),
    )
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE attempts SET state = 'completed', trajectory = 'complete', "
            "evidence_confidence = 'verified', updated_at = ? WHERE id = ?",
            (now, work["current_attempt"]["id"]),
        )
        connection.execute(
            "UPDATE work_items SET state = 'waiting_user', attention_owner = 'user', "
            "updated_at = ? WHERE id = ?",
            (now, work["id"]),
        )
    completed_attempt = dict(
        service.db.fetchone("SELECT * FROM attempts WHERE id = ?", (work["current_attempt"]["id"],))
    )

    result = service.delete_worker_thread(
        actor,
        _delete_request(ids["thread_id"], 1, "delete-waiting-user"),
    )

    assert result["thread_state"] == "deleted"
    assert service.get_work(work["id"])["state"] == "canceled"
    assert (
        dict(
            service.db.fetchone(
                "SELECT * FROM attempts WHERE id = ?", (work["current_attempt"]["id"],)
            )
        )
        == completed_attempt
    )
    assert "work.current_attempt_state_mismatch" not in {
        violation.code for violation in verify_projection(service.db).violations
    }
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events "
            "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )
        is None
    )


def test_delete_resumed_thread_accepts_historical_attachment_generations(
    system: dict[str, Any],
) -> None:
    actor = _attached(system, "delete-resumed-rotated")
    ids = _seed_managed_thread(system, actor, ordinal=1808)
    service = system["service"]
    first = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Historical Work before Resume",
            objective="Retain the original attachment-generation seal.",
            acceptance=["A later explicit Delete accepts this historical Work."],
            idempotency_key="assign-before-resumed-delete",
        ),
    )
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE runtime_sessions SET state = 'failed', updated_at = ? WHERE id = ?",
            (utc_now(), ids["runtime_id"]),
        )
    service.cancel_work(
        actor,
        first["id"],
        "Settle the historical Work before Finish.",
        idempotency_key="cancel-before-resumed-delete",
    )
    service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            idempotency_key="finish-before-resumed-delete",
        ),
    )
    resumed = service.resume_worker_thread(
        actor,
        ResumeWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=2,
            idempotency_key="resume-before-rotated-delete",
        ),
    )
    instructed = service.instruct_worker_thread(
        actor,
        InstructWorkerThreadInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=3,
            idempotency_key="instruction-before-rotated-delete",
            title="Resumed Work before attachment rotation",
            objective="Delete the same logical Worker after attachment rotation.",
            maturity="defined",
            acceptance=["Both Work generations remain correctly scoped."],
        ),
    )
    rotated_generation = int(actor["_cao_attachment_generation"]) + 1
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE cao_session_attachments SET generation = ?, updated_at = ? WHERE id = ?",
            (rotated_generation, now, actor["_cao_attachment_id"]),
        )
        connection.execute(
            "UPDATE cao_attachment_connections SET generation = ?, updated_at = ? WHERE id = ?",
            (rotated_generation, now, actor["_cao_connection_id"]),
        )
        connection.execute(
            "UPDATE cao_conversation_credentials SET generation = ?, updated_at = ? WHERE id = ?",
            (rotated_generation, now, actor["_cao_conversation_credential_id"]),
        )
        connection.execute(
            "UPDATE managed_worker_specs SET attachment_generation = ?, updated_at = ? "
            "WHERE id = ?",
            (rotated_generation, now, ids["spec_id"]),
        )
    current = dict(actor)
    current["_cao_attachment_generation"] = rotated_generation

    result = service.delete_worker_thread(
        current,
        _delete_request(ids["thread_id"], 3, "delete-resumed-rotated"),
    )

    assert result["thread_generation"] == 5
    assert service.get_work(first["id"])["state"] == "canceled"
    assert resumed["thread_generation"] == 3
    assert service.get_work(instructed["task"]["work_item_id"])["state"] == "canceled"


def test_delete_retains_terminal_work_with_open_boundary_as_anomaly(
    system: dict[str, Any],
) -> None:
    actor = _attached(system, "delete-terminal-open-boundary")
    ids, work, boundary, _ = _prepare_runtime_recovery(
        system,
        actor,
        ordinal=1809,
        reason="worker_inactive_timeout",
        acquire_turn=False,
    )
    service = system["service"]
    now = utc_now()
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE work_items SET state = 'canceled', attention_owner = 'none', "
            "generation = generation + 1, updated_at = ? WHERE id = ?",
            (now, work["id"]),
        )
        connection.execute(
            "UPDATE attempts SET state = 'canceled', updated_at = ? WHERE id = ?",
            (now, work["current_attempt"]["id"]),
        )

    boundary_before = dict(
        service.db.fetchone("SELECT * FROM boundaries WHERE id = ?", (boundary["id"],))
    )
    result = service.delete_worker_thread(
        actor,
        _delete_request(ids["thread_id"], 1, "delete-terminal-open-boundary"),
    )

    assert result["thread_state"] == "deleted"
    assert service.get_work(work["id"])["open_boundaries"][0]["id"] == boundary["id"]
    assert (
        dict(service.db.fetchone("SELECT * FROM boundaries WHERE id = ?", (boundary["id"],)))
        == boundary_before
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )
        is None
    )
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )["count"]
        == 1
    )


def test_finish_and_delete_accept_attachment_generation_zero_without_false_anomaly(
    system: dict[str, Any],
) -> None:
    actor = _attached(system, "finish-generation-zero")
    ids = _seed_managed_thread(system, actor, ordinal=1810)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Generation zero cleanup audit",
            objective="Settle this disposable Work without a false cleanup anomaly.",
            acceptance=["The Worker lifecycle command archives the exact disposable thread."],
            idempotency_key="assign-finish-generation-zero",
        ),
    )

    attachment = service.db.fetchone(
        "SELECT generation FROM cao_session_attachments WHERE id = ?",
        (actor["_cao_attachment_id"],),
    )
    goal = service.db.fetchone(
        "SELECT supervisor_attachment_generation FROM goal_revisions "
        "WHERE work_item_id = ? AND version = ?",
        (work["id"], work["goal_version"]),
    )
    assert attachment is not None and attachment["generation"] == 0
    assert goal is not None and goal["supervisor_attachment_generation"] == 0

    result = service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id=ids["thread_id"],
            expected_generation=1,
            idempotency_key="finish-generation-zero",
        ),
    )

    assert result["thread_state"] == "archived"
    assert service.get_work(work["id"])["state"] == "canceled"
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events "
            "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )
        is None
    )

    deleted = service.delete_worker_thread(
        actor,
        _delete_request(ids["thread_id"], 2, "delete-generation-zero"),
    )
    assert deleted["thread_state"] == "deleted"
    assert (
        service.db.fetchone(
            "SELECT COUNT(*) AS count FROM events "
            "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )["count"]
        == 0
    )


def test_delete_retains_tampered_goal_packet_seal_as_anomaly(system: dict[str, Any]) -> None:
    actor = _attached(system, "delete-tampered-goal")
    ids = _seed_managed_thread(system, actor, ordinal=1810)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Goal seal tamper fence",
            objective="Reject destructive scope when its requester seal is malformed.",
            acceptance=["Delete fails before mutation."],
            idempotency_key="assign-tampered-goal-delete",
        ),
    )
    goal = service.db.fetchone(
        "SELECT packet_json FROM goal_revisions WHERE work_item_id = ? AND version = ?",
        (work["id"], work["goal_version"]),
    )
    packet = json.loads(str(goal["packet_json"]))
    packet["requester_id"] = "prn_tampered_requester"
    with service.db.transaction() as connection:
        connection.execute(
            "UPDATE goal_revisions SET packet_json = ? WHERE work_item_id = ? AND version = ?",
            (json.dumps(packet, sort_keys=True), work["id"], work["goal_version"]),
        )

    tampered_packet = str(
        service.db.fetchone(
            "SELECT packet_json FROM goal_revisions WHERE work_item_id = ? AND version = ?",
            (work["id"], work["goal_version"]),
        )["packet_json"]
    )
    result = service.delete_worker_thread(
        actor,
        _delete_request(ids["thread_id"], 1, "delete-tampered-goal"),
    )

    assert result["thread_state"] == "deleted"
    assert (
        service.db.fetchone("SELECT state FROM work_items WHERE id = ?", (work["id"],))["state"]
        == "canceled"
    )
    assert (
        service.db.fetchone(
            "SELECT packet_json FROM goal_revisions WHERE work_item_id = ? AND version = ?",
            (work["id"], work["goal_version"]),
        )["packet_json"]
        == tampered_packet
    )
    anomaly = service.db.fetchone(
        "SELECT data_json FROM events "
        "WHERE event_type = 'managed_worker_thread.retained_anomaly' "
        "AND aggregate_id = ?",
        (ids["thread_id"],),
    )
    assert anomaly is not None
    assert json.loads(str(anomaly["data_json"]))["anomaly_kinds"]["goal_packet_seal"] >= 1


def test_delete_fail_closes_shadowed_shared_principal_binding(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = _attached(system, "delete-shared-principal")
    ids = _seed_managed_thread(system, actor, ordinal=1811)
    service = system["service"]
    connection = service.db.connect()
    try:
        connection.execute(
            "CREATE TEMP TABLE managed_worker_specs AS SELECT * FROM main.managed_worker_specs"
        )
        connection.execute(
            "CREATE TEMP TABLE managed_worker_threads AS SELECT * FROM main.managed_worker_threads"
        )
        connection.execute(
            "INSERT INTO managed_worker_specs SELECT * FROM main.managed_worker_specs WHERE id = ?",
            (ids["spec_id"],),
        )
        connection.execute(
            "UPDATE managed_worker_specs SET id = 'mws_shadow_shared_principal' "
            "WHERE rowid = (SELECT MAX(rowid) FROM managed_worker_specs)"
        )
        connection.execute(
            "INSERT INTO managed_worker_threads "
            "SELECT * FROM main.managed_worker_threads WHERE id = ?",
            (ids["thread_id"],),
        )
        connection.execute(
            "UPDATE managed_worker_threads "
            "SET id = 'mwt_shadow_shared_principal', "
            "managed_spec_id = 'mws_shadow_shared_principal' "
            "WHERE rowid = (SELECT MAX(rowid) FROM managed_worker_threads)"
        )

        @contextmanager
        def shadow_transaction(**_kwargs: Any) -> Any:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

        monkeypatch.setattr(service.db, "transaction", shadow_transaction)
        with pytest.raises(ConflictError) as conflict:
            service.delete_worker_thread(
                actor,
                _delete_request(ids["thread_id"], 1, "delete-shared-principal"),
            )
        assert conflict.value.details == {
            "reason_code": "worker_thread_delete_blocked",
            "blocker_kind": "shared_worker_binding",
            "blocker_count": 1,
        }
    finally:
        connection.close()

    assert (
        service.db.fetchone(
            "SELECT state FROM managed_worker_threads WHERE id = ?", (ids["thread_id"],)
        )["state"]
        == "active"
    )


def test_delete_rejects_reattach_after_role_precheck_without_mutation(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    actor = _lifecycle_attached(system, suffix="c")
    ids = _seed_managed_thread(system, actor, ordinal=1812)
    service = system["service"]
    work = service.assign_work(
        actor,
        _seeded_managed_work_assignment(
            ids,
            title="Stale CSC Delete fence",
            objective="A reattach linearized first prevents destructive mutation.",
            acceptance=["Work, thread, and Delivery remain byte-for-byte unchanged."],
            idempotency_key="assign-stale-csc-delete",
        ),
    )
    assignment = service.db.fetchone(
        """
        SELECT delivery.* FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        WHERE message.attempt_id = ? AND message.kind = 'assignment'
        """,
        (work["current_attempt"]["id"],),
    )
    assert assignment is not None

    def exact_state() -> dict[str, Any]:
        return {
            "work": dict(
                service.db.fetchone("SELECT * FROM work_items WHERE id = ?", (work["id"],))
            ),
            "attempt": dict(
                service.db.fetchone(
                    "SELECT * FROM attempts WHERE id = ?",
                    (work["current_attempt"]["id"],),
                )
            ),
            "thread": dict(
                service.db.fetchone(
                    "SELECT * FROM managed_worker_threads WHERE id = ?",
                    (ids["thread_id"],),
                )
            ),
            "delivery": dict(
                service.db.fetchone(
                    "SELECT * FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
                    (assignment["message_id"], assignment["recipient_id"]),
                )
            ),
        }

    before = exact_state()
    outcome = _run_after_role_check_gate(
        monkeypatch,
        service=service,
        actor=actor,
        operation=lambda: service.delete_worker_thread(
            actor,
            _delete_request(ids["thread_id"], 1, "delete-stale-csc-race"),
        ),
        linearize_first=lambda: _revoke_and_reattach_cao(
            system,
            actor,
            suffix="c",
        ),
    )

    assert "result" not in outcome
    assert isinstance(outcome.get("error"), AuthorizationError)
    assert exact_state() == before
    assert (
        service.db.fetchone(
            "SELECT 1 FROM events WHERE event_type = 'managed_worker_thread.deleted' "
            "AND aggregate_id = ?",
            (ids["thread_id"],),
        )
        is None
    )
