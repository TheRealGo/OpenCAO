"""Quiet runtime diagnosis for held supervision; never submit or replay input."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .connection_contract import cao_automatic_wake_ready_tx
from .database import _exact_assignment_not_submitted_is_proven, utc_after, utc_now
from .models import BoundaryInput, BoundaryKind, MessageKind, RuntimeState
from .requester_escalation import system_recovery_boundary_tx

if TYPE_CHECKING:
    import sqlite3

    from .service import ControlPlane


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryObservation:
    state: Literal["ready", "blocked", "unavailable"]
    code: str
    fingerprint: str
    input_state: Literal["", "pending", "running", "terminal", "unmatched", "ambiguous", "not_submitted"] = ""
    other_pending_input: bool = False
    history_complete: bool = False
    probe_stage: str = ""


DIAGNOSTIC_STAGES = frozenset({
    "", "spawn", "initialize", "thread_read", "queue_read", "turns_read", "input_correlation",
})


DIAGNOSTIC_CODES = frozenset({
    "runtime_binding_diagnosed", "runtime_thread_queue_blocked",
    "runtime_thread_source_unsettled", "runtime_thread_not_found", "runtime_thread_not_loaded", "runtime_thread_archived",
    "runtime_recovery_probe_unavailable", "runtime_recovery_adapter_unavailable",
    "runtime_recovery_launch_unavailable", "runtime_recovery_rpc_rejected", "runtime_recovery_timeout",
    "runtime_recovery_transport_unavailable",
    "runtime_assignment_pending", "runtime_assignment_running", "runtime_assignment_terminal",
    "runtime_assignment_input_unmatched", "runtime_assignment_evidence_conflict", "runtime_assignment_not_submitted",
})


def recover_unobserved_worker_starts(service: ControlPlane, *, dispatch_owner: str, limit: int = 100) -> int:
    """Surface an abandoned dispatch lease without deciding whether input ran.

    A different Dispatcher cannot consume the old stdio stream. An expired
    dispatch lease with no start proof gets one informational CAO notice, even
    while a bridge heartbeats. It changes no Work/Attempt generation or Worker
    authority, so a late authentic report remains admissible. Only terminal
    enrollment admits a reconciliation Boundary. Neither route replays input,
    activates a queue, or declares that the business task never ran.
    """
    if not service.db.is_canonical_authority():
        return 0
    due = utc_after(-service.settings.runtime_mcp_startup_timeout_seconds)
    recovered = 0
    with service.db.transaction() as connection:
        rows = connection.execute(
            """SELECT w.*, a.id AS attempt_id, a.runtime_session_id,
                      a.goal_packet_digest, a.task_packet_digest, m.id AS message_id,
                      d.generation AS delivery_generation, e.state AS enrollment_state
               FROM work_items w JOIN attempts a ON a.work_item_id = w.id
               JOIN messages m ON m.attempt_id = a.id AND m.kind = 'assignment'
               JOIN message_deliveries d ON d.message_id = m.id AND d.recipient_id = a.worker_id
               JOIN worker_enrollments e ON e.runtime_session_id = a.runtime_session_id AND e.managed = 1
               WHERE w.state = 'active' AND w.attention_owner = 'worker' AND a.state = 'assigned'
                 AND a.goal_version = w.goal_version AND d.state = 'dispatched'
                 AND d.owner_token <> ? AND d.updated_at <= ?
                 AND (d.lease_until IS NULL OR d.lease_until <= ?)
                 AND (e.state IN ('revoked', 'failed', 'stale') OR NOT EXISTS (
                     SELECT 1 FROM events notice WHERE notice.aggregate_id = a.id
                       AND notice.event_type = 'attempt.provider_start_unconfirmed'
                       AND json_extract(notice.data_json, '$.message_id') = m.id))
                 AND a.attempt_number = (SELECT MAX(latest.attempt_number) FROM attempts latest
                                        WHERE latest.work_item_id = w.id)
                 AND NOT EXISTS (SELECT 1 FROM events WHERE aggregate_id = a.id
                     AND event_type IN ('attempt.provider_turn_started', 'attempt.execution_started'))
                 AND NOT EXISTS (SELECT 1 FROM messages report WHERE report.attempt_id = a.id
                     AND report.sender_id = a.worker_id AND report.kind IN
                       ('progress', 'artifact', 'question', 'blocker', 'completion_claim'))
                 AND NOT EXISTS (SELECT 1 FROM worker_output_streams s WHERE s.attempt_id = a.id
                                 AND s.source_turn_digest <> '')
               ORDER BY d.updated_at, m.sequence LIMIT ?""", (dispatch_owner, due, utc_now(), limit),
        ).fetchall()
        for work in rows:
            if work["enrollment_state"] not in {"revoked", "failed", "stale"}:
                connection.execute(
                    "UPDATE message_deliveries SET last_error = 'runtime_assignment_start_timeout' "
                    "WHERE message_id = ? AND generation = ? AND state = 'dispatched'",
                    (work["message_id"], work["delivery_generation"]),
                )
                service._event(connection, "attempt.provider_start_unconfirmed", "attempt", work["attempt_id"], "", {
                    "message_id": work["message_id"], "delivery_generation": work["delivery_generation"],
                    "reason_code": "runtime_assignment_start_timeout",
                })
                service._message(connection, sender_id=work["supervisor_id"], recipient_id=work["supervisor_id"],
                    kind=MessageKind.SYSTEM, payload={"action": "worker_start_unconfirmed",
                        "reason": "runtime_assignment_start_timeout", "generation": work["generation"]},
                    work_item_id=work["id"], attempt_id=work["attempt_id"], goal_version=work["goal_version"],
                    idempotency_key=f"unobserved-live-worker-start:{work['message_id']}:{work['delivery_generation']}")
                recovered += 1
                continue
            generation = int(work["generation"]) + 1
            now = utc_now()
            connection.execute("UPDATE work_items SET state = 'waiting_supervisor', attention_owner = 'cao', "
                               "generation = ?, updated_at = ? WHERE id = ?", (generation, now, work["id"]))
            connection.execute("UPDATE attempts SET state = 'waiting_supervisor', stage = 'provider_start_unconfirmed', "
                               "next_boundary = 'cao_disposition', updated_at = ? WHERE id = ?", (now, work["attempt_id"]))
            connection.execute("UPDATE message_deliveries SET last_error = 'runtime_assignment_start_timeout' "
                               "WHERE message_id = ? AND generation = ? AND state = 'dispatched'",
                               (work["message_id"], work["delivery_generation"]))
            boundary = service._record_boundary_tx(connection, actor={"id": work["supervisor_id"], "role": "cao"},
                request=BoundaryInput(
                    source_event_id=f"unobserved-worker-start:{work['message_id']}:{work['delivery_generation']}",
                    work_item_id=work["id"], attempt_id=work["attempt_id"], expected_goal_version=work["goal_version"],
                    expected_generation=generation, expected_goal_packet_digest=work["goal_packet_digest"],
                    expected_task_packet_digest=work["task_packet_digest"], kind=BoundaryKind.FAILURE, runtime_state=RuntimeState.WAITING,
                    summary="The prior dispatch has no observed start. CAO owns reconciliation; the input outcome remains unknown.",
                    metadata={"system_recovery": True, "reason": "runtime_assignment_start_timeout"},
                ))
            service._message(connection, sender_id=work["supervisor_id"], recipient_id=work["supervisor_id"],
                kind=MessageKind.SYSTEM, payload={"action": "review_runtime_boundary", "boundary_id": boundary["id"],
                    "boundary_kind": "failure", "generation": generation}, work_item_id=work["id"],
                attempt_id=work["attempt_id"], goal_version=work["goal_version"],
                idempotency_key=f"unobserved-worker-start:{boundary['id']}")
            recovered += 1
    return recovered


def pending_runtime_recovery_holds(
    service: ControlPlane, *, limit: int = 8, after_disposition_id: str = "",
) -> list[dict[str, Any]]:
    """Diagnose every exact technical hold, including an unsettled Delivery fence."""
    if not service.db.is_canonical_authority():
        return []
    with service.db.connection_scope() as connection:
        rows = connection.execute(
            """SELECT w.id AS work_item_id, w.generation AS work_generation,
                  w.managed_worker_thread_id AS worker_thread_id,
                  b.id AS boundary_id, d.id AS disposition_id,
                  d.recovery_blocker, a.id AS attempt_id, a.worker_id,
                  COALESCE((SELECT source_message_id FROM worker_output_streams output
                     WHERE output.attempt_id = a.id ORDER BY output.created_at DESC, output.id DESC LIMIT 1),
                    (SELECT id FROM messages input WHERE input.attempt_id = a.id
                     AND input.kind = 'assignment' ORDER BY input.sequence DESC LIMIT 1)) AS diagnostic_message_id,
                  r.id, r.adapter, r.native_session_id, r.metadata_json
           FROM work_items w JOIN boundaries b ON b.work_item_id = w.id AND b.generation = w.generation
           JOIN boundary_dispositions d ON d.boundary_id = b.id AND d.kind = 'wait_system'
           JOIN attempts a ON a.id = b.attempt_id AND a.work_item_id = w.id
           JOIN runtime_sessions r ON r.id = a.runtime_session_id
           WHERE w.state = 'waiting_supervisor' AND w.attention_owner = 'cao'
             AND w.managed_worker_thread_id IS NOT NULL
             AND d.id > ?
           ORDER BY d.id LIMIT ?""", (after_disposition_id, limit),
        )
        result = []
        for row in rows:
            value = dict(row)
            value["assignment_not_submitted_proven"] = _exact_assignment_not_submitted_is_proven(
                connection, {"attempt_id": row["attempt_id"], "worker_id": row["worker_id"],
                             "runtime_session_id": row["id"]},
            )
            try:
                metadata = json.loads(value.pop("metadata_json"))
            except (TypeError, ValueError):
                metadata = {}
            value["metadata"] = metadata if isinstance(metadata, dict) else {}
            result.append(value)
        return result


def pending_runtime_start_diagnostics(
    service: ControlPlane, *, limit: int = 8, after_attempt_id: str = "",
    attempt_id: str = "", connection: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """Retain a read-only watch after a live unobserved-start notice is handled."""
    if not service.db.is_canonical_authority():
        return []
    sql = """SELECT w.id AS work_item_id, w.generation AS work_generation,
                  a.id AS attempt_id, m.id AS diagnostic_message_id,
                  d.generation AS delivery_generation, r.id, r.adapter,
                  r.native_session_id, r.metadata_json
           FROM work_items w JOIN attempts a ON a.work_item_id = w.id
           JOIN runtime_sessions r ON r.id = a.runtime_session_id
           JOIN messages m ON m.attempt_id = a.id AND m.kind = 'assignment'
           JOIN message_deliveries d ON d.message_id = m.id AND d.recipient_id = a.worker_id
           WHERE w.state = 'active' AND w.attention_owner = 'worker' AND a.state = 'assigned'
             AND a.id > ? AND (? = '' OR a.id = ?)
             AND d.state = 'dispatched' AND d.runtime_session_id = r.id
             AND a.goal_version = w.goal_version
             AND a.attempt_number = (SELECT MAX(latest.attempt_number) FROM attempts latest
                                    WHERE latest.work_item_id = w.id)
             AND EXISTS (SELECT 1 FROM events notice WHERE notice.aggregate_id = a.id
               AND notice.event_type = 'attempt.provider_start_unconfirmed'
               AND json_extract(notice.data_json, '$.message_id') = m.id)
             AND NOT EXISTS (SELECT 1 FROM events started WHERE started.aggregate_id = a.id
               AND started.event_type IN ('attempt.provider_turn_started', 'attempt.execution_started'))
             AND NOT EXISTS (SELECT 1 FROM messages report WHERE report.attempt_id = a.id
               AND report.sender_id = a.worker_id AND report.kind IN
                 ('progress', 'artifact', 'question', 'blocker', 'completion_claim'))
           ORDER BY a.id LIMIT ?"""
    params = (after_attempt_id, attempt_id, attempt_id, limit)
    rows = connection.execute(sql, params).fetchall() if connection else service.db.fetchall(sql, params)
    result = []
    for row in rows:
        value = dict(row)
        try:
            metadata = json.loads(value.pop("metadata_json"))
        except (TypeError, ValueError):
            metadata = {}
        value["metadata"] = metadata if isinstance(metadata, dict) else {}
        result.append(value)
    return result


def record_runtime_start_diagnosis(
    service: ControlPlane, candidate: dict[str, Any], observation: RuntimeRecoveryObservation,
) -> bool:
    """Publish exact input correlation, never acquire or replay Worker authority.

    Handling a notification cannot retire this watch. A changed fingerprint
    wakes CAO once; unchanged evidence stays quiet across process replacement.
    Native IDs and provider bodies remain outside all durable/public evidence.
    """
    if (observation.state not in {"ready", "blocked", "unavailable"}
            or observation.code not in DIAGNOSTIC_CODES
            or observation.probe_stage not in DIAGNOSTIC_STAGES
            or not re.fullmatch(r"[0-9a-f]{64}", observation.fingerprint)
            or observation.input_state not in {"", "pending", "running", "terminal", "unmatched", "ambiguous", "not_submitted"}
            or type(observation.other_pending_input) is not bool or type(observation.history_complete) is not bool):
        raise ValueError("invalid runtime start diagnosis")
    if not service.db.is_canonical_authority():
        return False
    with service.db.transaction() as connection:
        work = connection.execute("SELECT * FROM work_items WHERE id = ?", (candidate["work_item_id"],)).fetchone()
        current = next((row for row in pending_runtime_start_diagnostics(
            service, limit=1, attempt_id=candidate["attempt_id"], connection=connection,
        ) if row["attempt_id"] == candidate["attempt_id"]), None)
        if (work is None or work["generation"] != candidate["work_generation"] or current is None
                or any(current[key] != candidate[key] for key in
                       ("id", "native_session_id", "diagnostic_message_id", "delivery_generation"))):
            return False
        return _publish_start_diagnosis_tx(service, connection, work, candidate, observation)


def _publish_start_diagnosis_tx(
    service: ControlPlane, connection: sqlite3.Connection, work: sqlite3.Row,
    candidate: dict[str, Any], observation: RuntimeRecoveryObservation, *, notify: bool = True,
) -> bool:
    """One event-backed diagnosis/wake route for active dispatches and held recovery."""
    prior = connection.execute(
            "SELECT data_json FROM events WHERE aggregate_id = ? AND event_type = 'attempt.provider_start_diagnosed' "
            "ORDER BY sequence DESC LIMIT 1", (candidate["attempt_id"],),
    ).fetchone()
    if prior and json.loads(prior["data_json"]).get("fingerprint") == observation.fingerprint:
        return False
    data = {"message_id": candidate.get("diagnostic_message_id"),
            "delivery_generation": candidate.get("delivery_generation"), "fingerprint": observation.fingerprint,
            "state": observation.state, "reason_code": observation.code,
            "input_state": observation.input_state, "other_pending_input": observation.other_pending_input,
            "history_complete": observation.history_complete, "probe_stage": observation.probe_stage}
    sequence = service._event(connection, "attempt.provider_start_diagnosed", "attempt", candidate["attempt_id"], "", data)
    if notify:
        service._message(connection, sender_id=work["supervisor_id"], recipient_id=work["supervisor_id"],
            kind=MessageKind.SYSTEM, payload={"action": "worker_start_diagnosed", "generation": work["generation"]},
            work_item_id=work["id"], attempt_id=candidate["attempt_id"], goal_version=work["goal_version"],
            idempotency_key=f"worker-start-diagnosed:{candidate['attempt_id']}:{sequence}")
    return notify


def record_runtime_recovery_observation(
    service: ControlPlane, candidate: dict[str, Any], observation: RuntimeRecoveryObservation,
) -> bool:
    """Re-arm one exact decision only on changed evidence, including after restart.

    A healthy read is diagnostic evidence, not proof of successful binding or
    task completion. The next CAO decision still owns all admission/effect
    checks. An unchanged failed route neither wakes the model nor reports a
    no-change status. Observations contain no provider bodies or locators.
    """
    if (observation.state not in {"ready", "blocked", "unavailable"}
            or observation.code not in DIAGNOSTIC_CODES
            or observation.probe_stage not in DIAGNOSTIC_STAGES
            or not re.fullmatch(r"[0-9a-f]{64}", observation.fingerprint)):
        raise ValueError("invalid runtime recovery observation")
    if not service.db.is_canonical_authority():
        return False
    with service.db.transaction() as connection:
        work = connection.execute("SELECT * FROM work_items WHERE id = ?", (candidate["work_item_id"],)).fetchone()
        if work is None or work["generation"] != candidate["work_generation"]:
            return False
        held = system_recovery_boundary_tx(connection, work)
        if (held is None or held["id"] != candidate["boundary_id"]
                or held["disposition_id"] != candidate["disposition_id"]
                or held["recovery_blocker"] != candidate.get("recovery_blocker", "")):
            return False
        route = connection.execute(
            "SELECT r.id, r.native_session_id FROM attempts a JOIN runtime_sessions r "
            "ON r.id = a.runtime_session_id WHERE a.id = ?", (held["attempt_id"],),
        ).fetchone()
        if (route is None or route["id"] != candidate["id"]
                or route["native_session_id"] != candidate["native_session_id"]):
            return False
        prior = connection.execute(
            "SELECT e.data_json FROM events e JOIN boundaries b ON b.id=json_extract(e.data_json,'$.boundary_id') "
            "WHERE e.event_type = 'system_recovery.observed' AND e.aggregate_id = ? "
            "AND b.work_item_id = ? AND b.attempt_id = ? ORDER BY e.sequence DESC LIMIT 1",
            (candidate["worker_thread_id"], candidate["work_item_id"], held["attempt_id"]),
        ).fetchone()
        prior_digest = json.loads(prior["data_json"]).get("fingerprint") if prior else None
        if prior_digest != observation.fingerprint:
            service._event(connection, "system_recovery.observed", "managed_worker_thread",
                candidate["worker_thread_id"], "", {
                    "boundary_id": held["id"], "fingerprint": observation.fingerprint,
                    "state": observation.state, "reason_code": observation.code,
                    "input_state": observation.input_state, "other_pending_input": observation.other_pending_input,
                    "history_complete": observation.history_complete, "probe_stage": observation.probe_stage,
                })
        # A failed observation does not establish a newly usable route. Keep
        # its bounded diagnosis visible without manufacturing a new Boundary
        # and waking the same decision again. Real blocked/ready observations
        # remain actionable; none of these observations grant command authority.
        if observation.state == "unavailable":
            _publish_start_diagnosis_tx(service, connection, work, candidate, observation, notify=False)
            return False
        if held["recovery_probe_digest"] == observation.fingerprint:
            return False
        if service._system_recovery_blocker_tx(connection, work):
            # Admission forbids this exact Worker command, not CAO diagnosis
            # or a separately authorized lifecycle/handoff decision. Keep the
            # held Boundary, Work generation and unknown input/effects intact.
            return _publish_start_diagnosis_tx(service, connection, work, candidate, observation)
        if not cao_automatic_wake_ready_tx(connection, str(work["supervisor_attachment_id"] or "")):
            return False
        _publish_start_diagnosis_tx(service, connection, work, candidate, observation, notify=False)
        service._rearm_system_recovery_hold_tx(
            connection, work=work, held=held, diagnostic_changed=True, diagnostic_code=observation.code,
        )
        return True
