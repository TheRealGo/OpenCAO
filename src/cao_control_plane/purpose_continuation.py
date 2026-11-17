"""Keep the next purpose decision durable after an accepted task checkpoint.

The existing event ledger owns the obligation and decisions. Messages and
Deliveries remain replaceable wake transport, never business completion.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from .canonical import canonical_sha256
from .connection_contract import cao_automatic_wake_ready_tx
from .database import utc_now
from .errors import (
    AuthorizationError,
    ConflictError,
    ControlPlaneError,
    NotFoundError,
    ValidationError,
)
from .models import MessageKind, PurposeNextActionInput
from .worker_purpose import work_purpose_tx, worker_purpose_tx

if TYPE_CHECKING:
    from .service import ControlPlane

_REQUIRED = "worker.purpose_continuation_required"
_DECIDED = "worker.purpose_next_action_recorded"
_RECOVERY = "cao.purpose_continuation_recovery_attempted"
_RECOVERY_WAIT = "cao.purpose_continuation_recovery_waiting"
_RECOVERY_OBSERVED = "cao.purpose_continuation_source_observed"


def recovery_condition_tx(
    service: ControlPlane, connection: sqlite3.Connection, value: dict[str, Any], *,
    source_digest: str | None = None, source_status: str | None = None,
) -> dict[str, str]:
    """Only changed inputs/policy can make the same Next decision productive.

    Delivery IDs, native turn IDs, timestamps, review wording, assistant output
    and repeated rejected proposals are deliberately absent from this identity.
    """
    from .instruction_review import REVIEW_POLICY_DIGEST, requester_condition_digest

    row = connection.execute(
        "SELECT snapshot_json,snapshot_digest FROM instruction_reviews WHERE attachment_id=? "
        "AND json_type(snapshot_json, '$.requester_context')='array' ORDER BY rowid DESC LIMIT 1",
        (value["attachment_id"],),
    ).fetchone()
    retained: list[dict[str, Any]] = []
    if row is not None:
        snapshot = json.loads(row["snapshot_json"])
        if canonical_sha256(snapshot) != row["snapshot_digest"]:
            raise ConflictError("Requester source integrity is invalid", reason_code="instruction_review_source_integrity_invalid")
        retained = snapshot["requester_context"]
    artifacts = connection.execute(
        "SELECT DISTINCT artifact.digest FROM artifacts AS artifact JOIN work_items AS work "
        "ON work.id=artifact.work_item_id WHERE work.managed_worker_thread_id=? "
        "ORDER BY artifact.digest",
        (value["worker_thread_id"],),
    ).fetchall()
    diagnosis = connection.execute(
        "SELECT data_json FROM events WHERE aggregate_type='managed_worker_thread' AND aggregate_id=? "
        "AND event_type='system_recovery.observed' ORDER BY sequence DESC LIMIT 1",
        (value["worker_thread_id"],),
    ).fetchone()
    diagnostic = json.loads(diagnosis["data_json"]) if diagnosis else {}
    latest_source = connection.execute(
        "SELECT data_json FROM events WHERE event_type='cao.requester_source_observed' "
        "AND aggregate_id=? ORDER BY sequence DESC LIMIT 1", (value["attachment_id"],),
    ).fetchone()
    if latest_source is not None and source_digest is None:
        observed_source = json.loads(latest_source["data_json"])
        source_digest = observed_source["source_condition_digest"]
        source_status = observed_source["source_status"]
    capability = connection.execute(
        "SELECT data_json FROM events WHERE event_type='cao.native_capability_observed' "
        "AND aggregate_id=? ORDER BY sequence DESC LIMIT 1", (value["attachment_id"],),
    ).fetchone()
    condition = {
        "native_capability_digest": json.loads(capability["data_json"])["digest"] if capability else "",
        "purpose_version": str(value["purpose_version"]),
        "review_policy_digest": REVIEW_POLICY_DIGEST,
        "executable_release_digest": service._purpose_recovery_release_id,
        "source_condition_digest": source_digest if source_digest is not None else requester_condition_digest(retained),
        "source_status": source_status or ("available" if retained else "unavailable"),
        # Content registration is changed evidence to inspect, never acceptance
        # or effect permission. Repeated registration of the same bytes is not.
        "artifact_content_digest": canonical_sha256([row["digest"] for row in artifacts]),
        "runtime_diagnostic_digest": str(diagnostic.get("fingerprint", "")),
    }
    return {**condition, "condition_digest": canonical_sha256(condition)}


def _recover_wake_tx(
    service: ControlPlane, connection: sqlite3.Connection, value: dict[str, Any],
    condition: dict[str, str],
) -> int:
    latest = connection.execute(
        "SELECT data_json,sequence FROM events WHERE event_type=? AND aggregate_type='purpose_continuation' "
        "AND aggregate_id=? AND json_extract(data_json, '$.continuation_generation')=? ORDER BY sequence DESC LIMIT 1",
        (_RECOVERY, value["id"], value["generation"]),
    ).fetchone()
    if latest is not None and json.loads(latest["data_json"])["condition_digest"] == condition["condition_digest"]:
        waiting = connection.execute(
            "SELECT 1 FROM events WHERE event_type=? AND aggregate_id=? "
            "AND json_extract(data_json, '$.continuation_generation')=? "
            "AND json_extract(data_json, '$.recovery_attempt_sequence')=? LIMIT 1",
            (_RECOVERY_WAIT, value["id"], value["generation"], latest["sequence"]),
        ).fetchone()
        if waiting is None:
            service._event(connection, _RECOVERY_WAIT, "purpose_continuation", value["id"], "", {
                **condition, "continuation_generation": value["generation"],
                "recovery_attempt_sequence": latest["sequence"],
                "owner": "control_plane", "reason_code": "purpose_next_condition_unchanged",
                "recovery_action": "observe_native_source_or_policy_change",
            })
        return 0
    service._event(connection, _RECOVERY, "purpose_continuation", value["id"], "", {
        **condition, "continuation_generation": value["generation"],
    })
    service._message(
        connection, sender_id=value["supervisor_id"], recipient_id=value["supervisor_id"],
        kind=MessageKind.SYSTEM,
        payload={"action": "purpose_continuation", "purpose_continuation_id": value["id"],
                 "continuation_generation": value["generation"], "worker_thread_id": value["worker_thread_id"],
                 "purpose_version": value["purpose_version"], "recovery_condition_digest": condition["condition_digest"]},
        work_item_id=value["source_work_item_id"], attempt_id=value["source_attempt_id"],
        goal_version=value["source_goal_version"],
        idempotency_key=f"purpose-condition:{value['id']}:{value['generation']}:{latest['sequence'] if latest else 0}:{condition['condition_digest']}",
    )
    return 1


def pending_recovery_probes(service: ControlPlane) -> list[dict[str, Any]]:
    """Machine source repair runs outside transactions, without model wakeups."""
    rows = service.db.fetchall(
        "SELECT DISTINCT event.aggregate_id, attachment.native_thread_id FROM events AS event "
        "JOIN events AS required ON required.aggregate_id=event.aggregate_id AND required.event_type=? "
        "JOIN cao_session_attachments AS attachment ON attachment.id=json_extract(required.data_json, '$.attachment_id') "
        "WHERE event.event_type=? AND attachment.state='active' ORDER BY event.aggregate_id",
        (_REQUIRED, _RECOVERY_WAIT),
    )
    result = []
    with service.db.transaction() as connection:
        for row in rows:
            value = get_tx(connection, row["aggregate_id"])
            if (value["state"] == "pending"
                    and value.get("technical_recovery", {}).get("status") == "waiting_dependency_change"):
                result.append({"continuation_id": value["id"], "generation": value["generation"],
                               "native_thread_id": row["native_thread_id"]})
    return result


def record_recovery_probe(
    service: ControlPlane, candidate: dict[str, Any], *, source_digest: str, source_status: str,
) -> int:
    with service.db.transaction() as connection:
        value = get_tx(connection, candidate["continuation_id"])
        if value["state"] != "pending" or value["generation"] != candidate["generation"]:
            return 0
        observed = {"source_status": source_status, "source_condition_digest": source_digest,
                    "continuation_generation": value["generation"]}
        prior = connection.execute(
            "SELECT data_json FROM events WHERE event_type=? AND aggregate_id=? ORDER BY sequence DESC LIMIT 1",
            (_RECOVERY_OBSERVED, value["id"]),
        ).fetchone()
        if prior is None or json.loads(prior["data_json"]) != observed:
            service._event(connection, _RECOVERY_OBSERVED, "purpose_continuation", value["id"], "", observed)
        if source_status != "available":
            return 0
        if not cao_automatic_wake_ready_tx(connection, value["attachment_id"]):
            return 0
        # An admitted or unknown wake is never replaced by a background probe.
        active = connection.execute(
            "SELECT 1 FROM messages AS message JOIN message_deliveries AS delivery ON delivery.message_id=message.id "
            "WHERE json_extract(message.payload_json, '$.purpose_continuation_id')=? "
            "AND json_extract(message.payload_json, '$.continuation_generation')=? "
            "AND delivery.state NOT IN ('handled','dead') LIMIT 1",
            (value["id"], value["generation"]),
        ).fetchone()
        if active:
            return 0
        condition = recovery_condition_tx(service, connection, value, source_digest=source_digest, source_status=source_status)
        return _recover_wake_tx(service, connection, value, condition)


def _latest_purpose_state_tx(
    connection: sqlite3.Connection, thread_id: str, version: int
) -> sqlite3.Row | None:
    row: sqlite3.Row | None = connection.execute(
        "SELECT * FROM events WHERE aggregate_type='purpose_continuation' AND event_type=? "
        "AND json_extract(data_json, '$.worker_thread_id')=? "
        "AND json_extract(data_json, '$.purpose_version')=? "
        "AND json_extract(data_json, '$.kind') IN ('pause', 'resume', 'achieved') ORDER BY sequence DESC LIMIT 1",
        (_DECIDED, thread_id, version),
    ).fetchone()
    return row


def require_assignable_tx(connection: sqlite3.Connection, thread_id: str) -> None:
    purpose = worker_purpose_tx(connection, thread_id)
    if purpose is not None:
        pause = _latest_purpose_state_tx(connection, thread_id, int(purpose["version"]))
        if pause is not None and json.loads(pause["data_json"])["kind"] == "pause":
            raise ConflictError(
                "The requester explicitly paused this purpose; record requester-directed resumption first",
                reason_code="worker_purpose_paused",
            )
        if pause is not None and json.loads(pause["data_json"])["kind"] == "achieved":
            raise ConflictError(
                "This fixed purpose is achieved; record a requester-directed new purpose before assigning more Work",
                reason_code="worker_purpose_achieved",
            )


def get_tx(connection: sqlite3.Connection, continuation_id: str) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM events WHERE aggregate_type = 'purpose_continuation' "
        "AND aggregate_id = ? AND event_type = ? ORDER BY sequence LIMIT 1",
        (continuation_id, _REQUIRED),
    ).fetchone()
    if row is None:
        raise NotFoundError("purpose continuation", continuation_id)
    value: dict[str, Any] = json.loads(row["data_json"])
    value.update(id=continuation_id, generation=1, state="pending", created_at=row["created_at"])
    decision = connection.execute(
        "SELECT data_json, sequence FROM events WHERE aggregate_type = 'purpose_continuation' "
        "AND aggregate_id = ? AND event_type = ? ORDER BY sequence DESC LIMIT 1",
        (continuation_id, _DECIDED),
    ).fetchone()
    if decision is not None:
        value.update(json.loads(decision["data_json"]))
        value["_decision_sequence"] = int(decision["sequence"])
    if value["state"] in {"assigned", "achieved"}:
        return value
    try:
        purpose = worker_purpose_tx(connection, str(value["worker_thread_id"]))
    except ConflictError:
        value["state"] = "invalid"
        return value
    if purpose is None or purpose["version"] != value["purpose_version"]:
        value["state"] = "superseded"
    pause = _latest_purpose_state_tx(
        connection, value["worker_thread_id"], int(value["purpose_version"])
    )
    if value["state"] != "superseded" and pause is not None:
        final = json.loads(pause["data_json"])
        if final["kind"] == "achieved":
            value.update(
                state="achieved",
                purpose_achievement_id=pause["aggregate_id"],
                reason=final["reason"],
                evidence=final["evidence"],
                criterion_assessments=final["criterion_assessments"],
            )
            return value
    if (
        value["state"] != "superseded"
        and pause is not None
        and json.loads(pause["data_json"])["kind"] == "pause"
    ):
        value["state"] = "paused"
        return value
    if value["state"] == "paused" and pause is not None:
        value["state"] = "pending"
    attachment = connection.execute(
        "SELECT state FROM cao_session_attachments WHERE id = ?", (value["attachment_id"],)
    ).fetchone()
    if attachment is None or attachment["state"] != "active":
        value["state"] = "stopped"
    if value["state"] == "pending":
        recovery = connection.execute(
            "SELECT data_json FROM events WHERE aggregate_type='purpose_continuation' AND aggregate_id=? "
            "AND event_type IN (?,?) AND json_extract(data_json, '$.continuation_generation')=? "
            "ORDER BY sequence DESC LIMIT 1",
            (value["id"], _RECOVERY, _RECOVERY_WAIT, value["generation"]),
        ).fetchone()
        if recovery is not None:
            data = json.loads(recovery["data_json"])
            value["technical_recovery"] = {
                "owner": "control_plane", "condition_digest": data["condition_digest"],
                "status": "waiting_dependency_change" if "recovery_action" in data else "wake_scheduled",
                "next_action": "restore_native_source_or_correct_review_contract",
            }
    return value


def public_view(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value[key]
        for key in (
            "id",
            "generation",
            "state",
            "worker_thread_id",
            "purpose_version",
            "source_work_item_id",
            "source_review_id",
            "created_at",
            "next_work_item_id",
            "prior_next_work_item_id",
            "purpose_achievement_id",
            "resume_at",
            "resume_condition",
            "requester_input_kind",
            "reason",
            "evidence",
            "criterion_assessments",
            "technical_recovery",
        )
        if key in value
    }


def for_work_tx(connection: sqlite3.Connection, work_id: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT aggregate_id FROM events WHERE event_type = ? "
        "AND aggregate_type = 'purpose_continuation' "
        "AND json_extract(data_json, '$.source_work_item_id') = ? ORDER BY sequence DESC",
        (_REQUIRED, work_id),
    ).fetchall()
    return [public_view(get_tx(connection, str(row["aggregate_id"]))) for row in rows]


def worker_summary_tx(connection: sqlite3.Connection, thread_id: str) -> dict[str, Any]:
    try:
        purpose = worker_purpose_tx(connection, thread_id)
    except ConflictError:
        return {"state": "invalid", "count": 0, "resume_at": None}
    if purpose is not None:
        final = _latest_purpose_state_tx(connection, thread_id, int(purpose["version"]))
        if final is not None and json.loads(final["data_json"])["kind"] == "achieved":
            return {"state": "achieved", "count": 0, "resume_at": None}
    rows = connection.execute(
        "SELECT aggregate_id FROM events WHERE event_type=? "
        "AND aggregate_type='purpose_continuation' "
        "AND json_extract(data_json, '$.worker_thread_id')=? ORDER BY sequence DESC",
        (_REQUIRED, thread_id),
    ).fetchall()
    values = [get_tx(connection, str(row["aggregate_id"])) for row in rows]
    values = [
        value
        for value in values
        if purpose is not None and value["purpose_version"] == purpose["version"]
    ]
    for state in ("invalid", "paused", "pending", "user_needed", "waiting"):
        matching = [value for value in values if value["state"] == state]
        if matching:
            return {
                "state": state,
                "count": len(matching),
                "resume_at": min(
                    (str(value["resume_at"]) for value in matching if value.get("resume_at")),
                    default=None,
                ),
            }
    return {"state": values[0]["state"] if values else "none", "count": 0, "resume_at": None}


def pending_for_worker_tx(
    connection: sqlite3.Connection, thread_id: str, attachment_id: str
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT aggregate_id FROM events WHERE event_type=? AND aggregate_type='purpose_continuation' "
        "AND json_extract(data_json, '$.worker_thread_id')=? "
        "AND json_extract(data_json, '$.attachment_id')=? ORDER BY sequence",
        (_REQUIRED, thread_id, attachment_id),
    ).fetchall()
    values = [get_tx(connection, str(row["aggregate_id"])) for row in rows]
    return [
        value
        for value in values
        if value["state"] in {"pending", "waiting", "user_needed", "paused"}
    ]


def retire_worker_wakes_tx(connection: sqlite3.Connection, thread_id: str) -> None:
    """Retire old direction notices without touching an unknown handoff."""
    connection.execute(
        "UPDATE message_deliveries SET state='dead', reactivation_policy='terminal', "
        "last_error='purpose_continuation_superseded', updated_at=? "
        "WHERE state='queued' AND message_id IN (SELECT id FROM messages "
        "WHERE kind='system' AND json_valid(payload_json) "
        "AND json_extract(payload_json, '$.action')='purpose_continuation' "
        "AND json_extract(payload_json, '$.worker_thread_id')=?)",
        (utc_now(), thread_id),
    )


def _wake_tx(service: ControlPlane, connection: sqlite3.Connection, value: dict[str, Any]) -> int:
    if value["state"] != "pending" or not cao_automatic_wake_ready_tx(
        connection, value["attachment_id"]
    ):
        return 0
    deliveries = connection.execute(
        "SELECT delivery.state, delivery.last_error FROM messages AS message LEFT JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id WHERE message.kind = 'system' "
        "AND json_valid(message.payload_json) "
        "AND json_extract(message.payload_json, '$.purpose_continuation_id') = ? "
        "AND json_extract(message.payload_json, '$.continuation_generation') = ?",
        (value["id"], value["generation"]),
    ).fetchall()
    # Unknown or definitively dead transport remains evidence, never retry authority.
    if any(
        row["state"] is not None
        and row["state"] != "handled"
        and not (row["state"] == "dead" and row["last_error"] == "purpose_paused")
        for row in deliveries
    ):
        return 0
    if deliveries and all(row["state"] == "handled" for row in deliveries):
        return _recover_wake_tx(service, connection, value, recovery_condition_tx(service, connection, value))
    service._message(
        connection,
        sender_id=value["supervisor_id"],
        recipient_id=value["supervisor_id"],
        kind=MessageKind.SYSTEM,
        payload={
            "action": "purpose_continuation",
            "purpose_continuation_id": value["id"],
            "continuation_generation": value["generation"],
            "worker_thread_id": value["worker_thread_id"],
            "purpose_version": value["purpose_version"],
        },
        work_item_id=value["source_work_item_id"],
        attempt_id=value["source_attempt_id"],
        goal_version=value["source_goal_version"],
        idempotency_key=f"purpose-next:{value['id']}:{value['generation']}:{len(deliveries) + 1}",
    )
    return 1


def require_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    work: sqlite3.Row,
    *,
    review_id: str,
    cause_id: str | None = None,
    wake: bool = True,
) -> dict[str, Any] | None:
    thread_id = str(work["managed_worker_thread_id"] or "")
    if not thread_id or not work["supervisor_attachment_id"]:
        return None
    purpose = worker_purpose_tx(connection, thread_id)
    if purpose is None:
        return None
    existing = connection.execute(
        "SELECT aggregate_id FROM events WHERE event_type = ? AND causation_id = ? LIMIT 1",
        (_REQUIRED, cause_id or review_id),
    ).fetchone()
    if existing is not None:
        return get_tx(connection, str(existing["aggregate_id"]))
    continuation_id = f"pcn_{uuid.uuid4().hex}"
    review = connection.execute(
        "SELECT attempt_id, goal_version FROM reviews WHERE id = ?", (review_id,)
    ).fetchone()
    assert review is not None
    data = {
        "worker_thread_id": thread_id,
        "purpose_version": purpose["version"],
        "attachment_id": str(work["supervisor_attachment_id"]),
        "supervisor_id": str(work["supervisor_id"]),
        "source_work_item_id": str(work["id"]),
        "source_attempt_id": str(review["attempt_id"]),
        "source_goal_version": int(review["goal_version"]),
        "source_review_id": review_id,
        "operator_scope": str(work["operator_scope"]),
    }
    service._event(
        connection,
        _REQUIRED,
        "purpose_continuation",
        continuation_id,
        str(work["supervisor_id"]),
        data,
        causation_id=cause_id or review_id,
        correlation_id=str(work["id"]),
        operator_scope=str(work["operator_scope"]),
    )
    value = get_tx(connection, continuation_id)
    if wake:
        _wake_tx(service, connection, value)
    return value


def require_current_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    actor: dict[str, Any],
    continuation_id: str,
    generation: int,
    purpose_version: int | None,
) -> dict[str, Any]:
    value = get_tx(connection, continuation_id)
    if (
        str(actor.get("_cao_attachment_id") or "") != value["attachment_id"]
        or actor["id"] != value["supervisor_id"]
    ):
        raise AuthorizationError("purpose continuation belongs to its originating CAO conversation")
    work = service._require_authorized_work_tx(connection, actor, value["source_work_item_id"])
    service._confirm_work_purpose_tx(
        connection, str(actor["id"]), work, purpose_version, "purpose_next_action"
    )
    if value["generation"] != generation or value["state"] not in {
        "pending",
        "waiting",
        "user_needed",
        "paused",
    }:
        raise ConflictError(
            "Read the current purpose continuation before deciding",
            reason_code="purpose_continuation_state_conflict",
        )
    return value


def _decision_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    value: dict[str, Any],
    actor_id: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    service._event(
        connection,
        _DECIDED,
        "purpose_continuation",
        value["id"],
        actor_id,
        {
            **data,
            "generation": int(value["generation"]) + 1,
            "worker_thread_id": value["worker_thread_id"],
            "purpose_version": value["purpose_version"],
        },
        correlation_id=str(value["source_work_item_id"]),
        operator_scope=value["operator_scope"],
    )
    # Retire only an unclaimed wake. Accepted/unknown deliveries retain their proof.
    connection.execute(
        "UPDATE message_deliveries SET state = 'dead', reactivation_policy = 'terminal', "
        "last_error = 'purpose_continuation_decided', updated_at = ? "
        "WHERE state = 'queued' AND message_id IN (SELECT id FROM messages "
        "WHERE kind = 'system' AND json_valid(payload_json) "
        "AND json_extract(payload_json, '$.purpose_continuation_id') = ? "
        "AND json_extract(payload_json, '$.continuation_generation') = ?)",
        (utc_now(), value["id"], value["generation"]),
    )
    return public_view(get_tx(connection, value["id"]))


def prepare_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    actor: dict[str, Any],
    request: PurposeNextActionInput,
) -> tuple[dict[str, Any], dict[str, Any]]:
    value = require_current_tx(
        service,
        connection,
        actor,
        request.continuation_id,
        request.expected_generation,
        request.expected_purpose_version,
    )
    if value["state"] == "paused" and request.kind != "resume":
        raise ConflictError(
            "The purpose requires requester-directed resumption",
            reason_code="worker_purpose_paused",
        )
    if request.kind == "resume" and value["state"] != "paused":
        raise ConflictError("Only an explicitly paused purpose can be resumed")
    state = {
        "continue": "assigned",
        "achieved": "achieved",
        "wait_until": "waiting",
        "wait_user": "user_needed",
        "pause": "paused",
        "resume": "pending",
    }[request.kind]
    if request.kind == "continue":
        if not request.next_work_item_id:
            raise ValidationError("continuation requires the exact committed next Work")
        next_work = service._require_authorized_work_tx(
            connection, actor, request.next_work_item_id
        )
        next_purpose = work_purpose_tx(
            connection, str(next_work["id"]), int(next_work["goal_version"])
        )
        current_next_purpose = worker_purpose_tx(
            connection, str(next_work["managed_worker_thread_id"] or "")
        )
        source_purpose = worker_purpose_tx(connection, value["worker_thread_id"])
        same_purpose = bool(
            next_purpose
            and current_next_purpose == next_purpose
            and source_purpose
            and next_purpose["text"] == source_purpose["text"]
            and next_purpose.get("contract") == source_purpose.get("contract")
        )
        next_attempt = service._latest_attempt_tx(connection, str(next_work["id"]))
        if (
            not same_purpose
            or next_work["supervisor_attachment_id"] != value["attachment_id"]
            or next_attempt is None
            or next_attempt["id"] == value["source_attempt_id"]
            or next_work["state"]
            in {"completed", "waiting_user", "canceled", "failed", "suspended"}
        ):
            raise ConflictError(
                "next Work must be an unsettled execution of this exact purpose and attachment"
            )
    elif request.next_work_item_id is not None:
        raise ValidationError("next_work_item_id is valid only for continue")
    if request.kind == "achieved":
        purpose = worker_purpose_tx(connection, value["worker_thread_id"])
        assert purpose is not None
        if "contract" not in purpose:
            raise ValidationError("Register the fixed contract from the existing requester direction before declaring purpose achievement")
        expected = {item["id"] for item in purpose["contract"]["criteria"]}
        supplied = [item.criterion_id for item in request.criterion_assessments]
        if len(supplied) != len(set(supplied)) or set(supplied) != expected:
            raise ValidationError(
                "purpose achievement requires evidence for every fixed purpose criterion"
            )
    elif request.criterion_assessments:
        raise ValidationError("purpose criterion assessments are valid only for achieved")
    resume_at: str | None = None
    if request.kind == "wait_until":
        try:
            parsed = datetime.fromisoformat(str(request.resume_at).replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed <= datetime.now(UTC):
                raise ValueError
            resume_at = parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
        except (TypeError, ValueError):
            raise ValidationError(
                "wait_until requires an explicit future time with timezone"
            ) from None
        if not request.resume_condition.strip():
            raise ValidationError("wait_until requires a concrete resume condition")
    elif request.resume_at is not None:
        raise ValidationError("resume_at is valid only for wait_until")
    if request.kind == "wait_user":
        if not request.requester_input_kind or not request.resume_condition.strip():
            raise ValidationError(
                "wait_user requires the exact missing requester input and its kind"
            )
    elif request.requester_input_kind is not None:
        raise ValidationError("requester_input_kind is valid only for wait_user")
    data = request.model_dump(
        mode="json",
        exclude={
            "continuation_id",
            "expected_generation",
            "expected_purpose_version",
            "idempotency_key",
        },
    )
    data.update(state=state, resume_at=resume_at)
    return value, data


def apply_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    actor: dict[str, Any],
    value: dict[str, Any],
    data: dict[str, Any],
) -> dict[str, Any]:
    result = _decision_tx(service, connection, value, str(actor["id"]), data)
    if data["kind"] in {"pause", "achieved"}:
        connection.execute(
            "UPDATE message_deliveries SET state='dead', reactivation_policy='terminal', "
            "last_error=?, updated_at=? WHERE state='queued' "
            "AND message_id IN (SELECT id FROM messages WHERE kind='system' AND json_valid(payload_json) "
            "AND json_extract(payload_json, '$.action')='purpose_continuation' "
            "AND json_extract(payload_json, '$.worker_thread_id')=? "
            "AND json_extract(payload_json, '$.purpose_version')=?)",
            (
                "purpose_paused" if data["kind"] == "pause" else "purpose_achieved",
                utc_now(),
                value["worker_thread_id"],
                value["purpose_version"],
            ),
        )
    elif data["kind"] == "resume":
        _wake_tx(service, connection, get_tx(connection, value["id"]))
    return result


def resolve_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    actor: dict[str, Any],
    request: PurposeNextActionInput,
) -> dict[str, Any]:
    value, data = prepare_tx(service, connection, actor, request)
    return apply_tx(service, connection, actor, value, data)


def _ended_successor_tx(
    connection: sqlite3.Connection, value: dict[str, Any]
) -> sqlite3.Row | None:
    if value["state"] != "assigned" or not value.get("next_work_item_id"):
        return None
    purpose = worker_purpose_tx(connection, value["worker_thread_id"])
    if purpose is None or purpose["version"] != value["purpose_version"]:
        return None
    final = _latest_purpose_state_tx(
        connection, value["worker_thread_id"], value["purpose_version"]
    )
    if final is not None and json.loads(final["data_json"])["kind"] in {"pause", "achieved"}:
        return None
    attachment = connection.execute(
        "SELECT state FROM cao_session_attachments WHERE id=?", (value["attachment_id"],)
    ).fetchone()
    if attachment is None or attachment["state"] != "active":
        return None
    # An accepted successor already owns its own next decision. Only a successor
    # that ended before that handoff returns responsibility to the predecessor.
    row: sqlite3.Row | None = connection.execute(
        "SELECT work.id,work.state FROM work_items AS work WHERE work.id=? "
        "AND work.state IN ('canceled','failed') AND NOT EXISTS ("
        "SELECT 1 FROM events WHERE aggregate_type='purpose_continuation' AND event_type=? "
        "AND json_extract(data_json,'$.source_work_item_id')=work.id "
        "AND sequence>? AND causation_id=json_extract(data_json,'$.source_review_id'))",
        (value["next_work_item_id"], _REQUIRED, value["_decision_sequence"]),
    ).fetchone()
    return row


def recover_missing_checkpoints_tx(service: ControlPlane, connection: sqlite3.Connection) -> int:
    """Restore one omitted purpose decision, never replay an accepted Work.

    Pre-upgrade acceptance may predate the continuation ledger altogether. A
    currently executing/owned Work already carries the purpose forward. When
    none exists, its latest accepted checkpoint needs an explicit next decision.
    """
    rows = connection.execute("""
        SELECT work.*, review.id AS source_review_id FROM work_items AS work
        JOIN cao_session_attachments AS attachment ON attachment.id=work.supervisor_attachment_id
        JOIN reviews AS review ON review.work_item_id=work.id AND review.verdict='ok'
        JOIN worker_purpose_revisions AS purpose ON purpose.worker_thread_id=work.managed_worker_thread_id
          AND purpose.version=(SELECT MAX(current.version) FROM worker_purpose_revisions AS current
                               WHERE current.worker_thread_id=work.managed_worker_thread_id)
        WHERE work.managed_worker_thread_id IS NOT NULL AND attachment.state='active'
          AND work.state IN ('waiting_user','completed')
          AND (purpose.supervisor_attachment_id=work.supervisor_attachment_id OR EXISTS (
              SELECT 1 FROM work_purpose_bindings AS binding
              WHERE binding.work_item_id=work.id AND binding.goal_version=work.goal_version
                AND binding.worker_thread_id=purpose.worker_thread_id
                AND binding.purpose_version=purpose.version))
          AND review.id=(SELECT latest.id FROM reviews AS latest WHERE latest.work_item_id=work.id
                         AND latest.verdict='ok' ORDER BY latest.created_at DESC,latest.rowid DESC LIMIT 1)
          AND NOT EXISTS (SELECT 1 FROM work_items AS live
              WHERE live.managed_worker_thread_id=work.managed_worker_thread_id
                AND live.supervisor_attachment_id=work.supervisor_attachment_id
                AND live.state NOT IN ('waiting_user','completed','canceled','failed'))
          AND NOT EXISTS (SELECT 1 FROM work_items AS newer
              WHERE newer.managed_worker_thread_id=work.managed_worker_thread_id
                AND newer.supervisor_attachment_id=work.supervisor_attachment_id
                AND newer.state IN ('waiting_user','completed') AND newer.rowid>work.rowid)
        ORDER BY work.rowid
    """).fetchall()
    restored = 0
    for work in rows:
        connection.execute("SAVEPOINT missing_purpose_checkpoint")
        try:
            thread = str(work["managed_worker_thread_id"])
            purpose = worker_purpose_tx(connection, thread)
            if purpose is not None:
                final = _latest_purpose_state_tx(connection, thread, int(purpose["version"]))
                stopped = final is not None and json.loads(final["data_json"])["kind"] in {"pause", "achieved"}
                pending = pending_for_worker_tx(connection, thread, str(work["supervisor_attachment_id"]))
                if not stopped and not pending:
                    cause = f"retained-checkpoint:{thread}:{purpose['version']}:{work['id']}"
                    exists = connection.execute(
                        "SELECT 1 FROM events WHERE event_type=? "
                        "AND json_extract(data_json,'$.source_work_item_id')=? "
                        "AND json_extract(data_json,'$.source_review_id')=? "
                        "AND json_extract(data_json,'$.purpose_version')=?",
                        (_REQUIRED, work["id"], work["source_review_id"], purpose["version"]),
                    ).fetchone()
                    if exists is None:
                        require_tx(service, connection, work, review_id=work["source_review_id"], cause_id=cause)
                        restored += 1
        except (ConflictError, ValueError, TypeError, KeyError):
            connection.execute("ROLLBACK TO missing_purpose_checkpoint")
        finally:
            connection.execute("RELEASE missing_purpose_checkpoint")
    return restored


def reconcile_tx(service: ControlPlane, connection: sqlite3.Connection, *, limit: int) -> int:
    rows = connection.execute(
        "SELECT required.aggregate_id FROM events AS required "
        "WHERE required.event_type = ? AND required.aggregate_type = 'purpose_continuation' "
        "AND COALESCE((SELECT json_extract(decision.data_json, '$.state') FROM events AS decision "
        "WHERE decision.aggregate_type = required.aggregate_type AND decision.aggregate_id = required.aggregate_id "
        "AND decision.event_type = ? ORDER BY decision.sequence DESC LIMIT 1), 'pending') "
        "IN ('pending', 'waiting', 'paused', 'assigned') ORDER BY required.sequence",
        (_REQUIRED, _DECIDED),
    ).fetchall()
    rearmed = 0
    considered = 0
    for row in rows:
        connection.execute("SAVEPOINT purpose_continuation_wake")
        try:
            value = get_tx(connection, str(row["aggregate_id"]))
            ended = _ended_successor_tx(connection, value)
            if ended is not None:
                _decision_tx(
                    service,
                    connection,
                    value,
                    "",
                    {
                        "kind": "reconsider",
                        "state": "pending",
                        "prior_next_work_item_id": str(ended["id"]),
                        "reason": "The assigned successor ended before an accepted checkpoint.",
                        "evidence": f"Work {ended['id']} is {ended['state']}; prior Work, Delivery and effect evidence remains intact.",
                    },
                )
                value = get_tx(connection, value["id"])
            if value["state"] == "waiting" and datetime.fromisoformat(
                value["resume_at"].replace("Z", "+00:00")
            ) <= datetime.fromisoformat(utc_now().replace("Z", "+00:00")):
                _decision_tx(
                    service,
                    connection,
                    value,
                    "",
                    {
                        "state": "pending",
                        "reason": "The recorded observation time is due.",
                        "evidence": value["evidence"],
                    },
                )
                value = get_tx(connection, value["id"])
            count = _wake_tx(service, connection, value) if value["state"] == "pending" else 0
        except ControlPlaneError as error:
            connection.execute("ROLLBACK TO purpose_continuation_wake")
            # One damaged source or unavailable capability must not veto another purpose.
            code = error.code
            prior = connection.execute(
                "SELECT 1 FROM events WHERE aggregate_type='purpose_continuation' "
                "AND aggregate_id=? AND event_type='cao.purpose_continuation_wake_blocked' "
                "AND json_extract(data_json, '$.code')=? LIMIT 1",
                (row["aggregate_id"], code),
            ).fetchone()
            if prior is None:
                service._event(
                    connection,
                    "cao.purpose_continuation_wake_blocked",
                    "purpose_continuation",
                    str(row["aggregate_id"]),
                    "",
                    {"code": code},
                )
            count = 0
        finally:
            connection.execute("RELEASE purpose_continuation_wake")
        rearmed += count
        considered += count
        if considered >= limit:
            break
    return rearmed


def notification_tx(connection: sqlite3.Connection, message_id: str) -> sqlite3.Row | None:
    row: sqlite3.Row | None = connection.execute(
        "SELECT message.*, NULL AS boundary_id, delivery.recipient_id, delivery.recipient_attachment_id, "
        "delivery.generation AS delivery_generation, delivery.state AS delivery_state, "
        "delivery.generation AS generation, "
        "attachment.native_thread_id, attachment.generation AS attachment_generation, "
        "attachment.lease_expires_at FROM messages AS message "
        "JOIN message_deliveries AS delivery ON delivery.message_id = message.id "
        "JOIN work_items AS work ON work.id = message.work_item_id "
        "JOIN cao_session_attachments AS attachment ON attachment.id = delivery.recipient_attachment_id "
        "AND attachment.id = work.supervisor_attachment_id AND attachment.principal_id = delivery.recipient_id "
        "JOIN runtime_sessions AS runtime ON runtime.id = attachment.runtime_session_id "
        "AND runtime.principal_id = attachment.principal_id "
        "AND runtime.native_session_id = attachment.native_thread_id AND runtime.adapter = 'codex-app-server' "
        "AND runtime.state NOT IN ('stopped', 'failed', 'missing') "
        "WHERE message.id = ? AND work.supervisor_id = delivery.recipient_id "
        "AND message.kind = 'system' AND attachment.state = 'active' "
        "AND json_valid(message.payload_json) "
        "AND json_extract(message.payload_json, '$.action') = 'purpose_continuation'",
        (message_id,),
    ).fetchone()
    if row is not None:
        payload = json.loads(row["payload_json"])
        value = get_tx(connection, payload["purpose_continuation_id"])
        if (
            value["state"] != "pending"
            or value["generation"] != payload["continuation_generation"]
            or value["attachment_id"] != row["recipient_attachment_id"]
        ):
            return None
    return row


def recover_notification_tx(
    service: ControlPlane,
    connection: sqlite3.Connection,
    *,
    message_id: str,
    delivery_generation: int,
) -> str:
    row = notification_tx(connection, message_id)
    if (
        row is None
        or row["delivery_generation"] != delivery_generation
        or row["delivery_state"] not in {"delivered", "acknowledged"}
    ):
        return "not_required"
    payload = json.loads(row["payload_json"])
    value = get_tx(connection, payload["purpose_continuation_id"])
    if value["state"] != "pending" or value["generation"] != payload["continuation_generation"]:
        return "not_required"
    # Called only after the shared exact persisted-terminal-turn proof is corroborated.
    connection.execute(
        "UPDATE message_deliveries SET state = 'dead', reactivation_policy = 'terminal', "
        "last_error = 'purpose_continuation_turn_incomplete', updated_at = ? "
        "WHERE message_id = ? AND generation = ? AND state IN ('delivered', 'acknowledged')",
        (utc_now(), message_id, delivery_generation),
    )
    return "scheduled" if _recover_wake_tx(
        service, connection, value, recovery_condition_tx(service, connection, value)
    ) else "not_required"


def reactivate_tx(
    service: ControlPlane, connection: sqlite3.Connection, *, attachment_id: str, runtime_id: str
) -> int:
    """Replace only typed retryable wake transport on an authentic connection change."""
    candidates = connection.execute(
        "SELECT message.id FROM messages AS message JOIN message_deliveries AS delivery "
        "ON delivery.message_id = message.id WHERE delivery.recipient_attachment_id = ? "
        "AND delivery.state = 'dead' AND delivery.reactivation_policy = 'retryable' "
        "AND message.kind = 'system' AND json_valid(message.payload_json) "
        "AND json_extract(message.payload_json, '$.action') = 'purpose_continuation'",
        (attachment_id,),
    ).fetchall()
    count = 0
    for candidate in candidates:
        current = notification_tx(connection, str(candidate["id"]))
        if current is None:
            continue
        updated = connection.execute(
            "UPDATE message_deliveries SET state='queued', generation=generation+1, attempts=0, "
            "next_attempt_at=?, runtime_session_id=?, lease_until=NULL, owner_token='', last_error='', "
            "reactivation_policy='terminal', updated_at=? WHERE message_id=? AND generation=? "
            "AND state='dead' AND reactivation_policy='retryable'",
            (utc_now(), runtime_id, utc_now(), candidate["id"], current["delivery_generation"]),
        ).rowcount
        if updated:
            service._event(
                connection,
                "cao.purpose_continuation_wake_reactivated",
                "message",
                str(candidate["id"]),
                str(current["recipient_id"]),
                {"delivery_generation": int(current["delivery_generation"]) + 1},
            )
            count += 1
    return count
