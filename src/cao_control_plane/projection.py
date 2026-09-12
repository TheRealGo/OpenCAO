"""Deterministic, read-only control-plane projections.

The projection is intentionally built from the kernel SQLite database rather
than from a dispatcher, runtime adapter, cache, or transport session.  It is
safe to expose to a dashboard: user-provided text, endpoint values, metadata,
tokens, and JSON payloads are never copied into the result.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_preservation_edge import OwnerPrivateArtifactPreservation
from .canonical import canonical_sha256 as _digest
from .close_contract import (
    ArtifactPreservation,
    CleanupAction,
    CleanupOutcome,
    CleanupRecord,
    CleanupTargetKind,
    ClosePlan,
    CloseReadiness,
    evaluate_close,
)
from .close_inventory_edge import OwnerPrivateCloseInventory
from .database import Database, work_pause_record_binding_sql, work_pause_resumption_binding_sql
from .errors import ConflictError
from .goal_packets import (
    build_goal_packet,
    build_task_packet,
    goal_packet_digest,
    task_packet_digest,
)
from .provider_models import model_identifier
from .security import canonical_json, matches, redact_control_plane_secrets
from .supervision_control import pause_view_tx

_FORMAT = "cao-control-plane-projection/v1"
_TERMINAL_RUNTIME_STATES = ("stopped", "failed", "missing")
_WORK_ATTENTION = {
    "active": "worker",
    "waiting_supervisor": "cao",
    "waiting_review": "cao",
    "waiting_user": "user",
    "user_needed": "user",
    "suspended": "none",
    "completed": "none",
    "canceled": "none",
    "failed": "none",
}
_WORK_ATTEMPT_STATES = {
    "active": {"assigned", "accepted", "working"},
    "waiting_supervisor": {"waiting_supervisor"},
    "waiting_review": {"submitted"},
    "waiting_user": {"completed"},
    "user_needed": {"input_required"},
    "suspended": {"suspended"},
    "completed": {"completed"},
    # Lifecycle Finish/Delete cancels the remaining Work scope but preserves
    # an already terminal latest Attempt as immutable evidence.
    "canceled": {"completed", "canceled", "failed"},
    "failed": {"failed"},
}
_OPERATOR_TEXT_LIMIT = 280
_OPERATOR_URL = re.compile(r"(?i)\b(?:https?|file|ssh|s3|gs|data):[^\s<>()]+")
_OPERATOR_PATH = re.compile(r"(?<!\w)(?:~[\\/]|/[\w.~-]+|[A-Za-z]:[\\/])[^\s<>()]*")
_OPERATOR_SECRET = re.compile(
    r"(?i)\b(?:bearer|token|secret|password|credential|authorization|api[ _-]?key)"
    r"(?:\s*[:=]\s*|\s+)[^\s,;]+"
)
_OPERATOR_GITHUB_TOKEN = re.compile(r"\b(?:ghp_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{20,})\b")
_OPERATOR_INTERNAL_ID = re.compile(
    r"(?i)\b(?:native[ _-]?(?:thread|session)|thread|session|runtime|work(?:[ _-]?item)?|"
    r"attempt|review|principal|database|db)[ _-]?id\s*[:=#]\s*[A-Za-z0-9._-]+"
)
_OPERATOR_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
_OPERATOR_REPORT_KINDS = frozenset(
    {"progress", "question", "blocker", "artifact", "completion_claim", "worker_output"}
)
_MANAGED_RUNNER_ADAPTERS = frozenset({"codex-app-server", "claude"})
_MANAGED_REASONING_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max", "ultra"})
_RECOVERY_ACTIONS = frozenset(
    {
        "dispose_continue_or_correct",
        "reconcile_continue_same_thread",
        "system_reconciliation",
    }
)
_RECOVERY_NOTIFICATION_ACTIONS = frozenset(
    {
        "recover_terminal_worker_attempt",
        "reconcile_completed_worker_turn",
        "reconcile_continue_same_thread",
        "recover_expired_reasoner_turn",
        "recover_incomplete_reasoner_turn",
        "review_runtime_boundary",
        "rearm_supervision_boundary",
    }
)
_DELIVERY_STATES = frozenset(
    {
        "queued",
        "leased",
        "dispatched",
        "delivered",
        "acknowledged",
        "handled",
        "dead",
    }
)


def _canonical_artifact_digest(value: object) -> str:
    if not isinstance(value, str):
        return ""
    match = re.fullmatch(r"sha256:([0-9a-f]{64})", value)
    return match.group(1) if match is not None else value


def _conversation_close_receipt_contract_valid(
    *,
    attachment_id: str,
    close_generation: int,
    request_digest: str,
    idempotency_key: str,
    event_data: Mapping[str, Any],
    result: Mapping[str, Any],
    canceled_event_count: int,
    stopped_event_count: int,
    retired_spec_generations: list[int],
) -> bool:
    """Independently verify historical and current close-receipt shapes."""

    canceled_count = event_data.get("work_items_canceled")
    stopped_count = event_data.get("managed_workers_stopped")
    if not (
        isinstance(canceled_count, int)
        and not isinstance(canceled_count, bool)
        and canceled_count >= 0
        and canceled_count == canceled_event_count
        and isinstance(stopped_count, int)
        and not isinstance(stopped_count, bool)
        and stopped_count >= 0
        and stopped_count == stopped_event_count
        and event_data.get("idempotency_key_digest") == _digest(idempotency_key)
    ):
        return False
    base_result = {
        "status": "closed",
        "scope": "conversation",
        "work_items_canceled": canceled_count,
        "managed_workers_stopped": stopped_count,
    }
    legacy_fields = {
        "non_resumable_worker_threads",
        "resume_loss_acknowledged",
        "conversation_evidence_digest",
        "retired_spec_generation_count",
        "retired_spec_generations_digest",
    }
    if (
        request_digest
        == _digest(
            {
                "attachment_id": attachment_id,
                "attachment_generation": close_generation,
            }
        )
        and result == base_result
        and legacy_fields.isdisjoint(event_data)
    ):
        return True

    non_resumable_count = event_data.get("non_resumable_worker_threads")
    acknowledged = event_data.get("resume_loss_acknowledged")
    evidence_digest = str(event_data.get("conversation_evidence_digest") or "")
    generation_count = event_data.get("retired_spec_generation_count")
    generations_digest = str(event_data.get("retired_spec_generations_digest") or "")
    canonical_generations = sorted(set(retired_spec_generations))
    loss_count_valid = (
        isinstance(non_resumable_count, int)
        and not isinstance(non_resumable_count, bool)
        and isinstance(stopped_count, int)
        and not isinstance(stopped_count, bool)
        and 0 <= non_resumable_count <= stopped_count
    )
    acknowledgment_valid = loss_count_valid and (
        (acknowledged is False and non_resumable_count == 0 and evidence_digest == "")
        or (acknowledged is True and re.fullmatch(r"[0-9a-f]{64}", evidence_digest) is not None)
    )
    return bool(
        acknowledgment_valid
        and isinstance(generation_count, int)
        and not isinstance(generation_count, bool)
        and generation_count == len(canonical_generations)
        and canonical_generations == retired_spec_generations
        and all(0 <= generation <= close_generation for generation in canonical_generations)
        and generations_digest == _digest(canonical_generations)
        and result
        == {
            **base_result,
            "non_resumable_worker_threads": non_resumable_count,
            "resume_loss_acknowledged": acknowledged,
            "conversation_evidence_digest": evidence_digest,
            "retired_spec_generation_count": generation_count,
            "retired_spec_generations_digest": generations_digest,
        }
        and request_digest
        == _digest(
            {
                "attachment_id": attachment_id,
                "attachment_generation": close_generation,
                "accept_non_resumable_worker_threads": acknowledged,
                "conversation_evidence_digest": evidence_digest,
            }
        )
    )


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class ProjectionViolation:
    """A sanitized invariant failure suitable for a dashboard or alert."""

    code: str
    subject_type: str
    subject_ids: tuple[str, ...] = ()
    count: int = 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "subject_type": self.subject_type,
            "subject_ids": list(self.subject_ids),
            "count": self.count,
        }


@dataclass(frozen=True)
class ProjectionSnapshot:
    """A consistent kernel-only projection and its verification result."""

    snapshot: Mapping[str, Any]
    watermark: Mapping[str, Any]
    canonical_digest: str
    violations: tuple[ProjectionViolation, ...]

    @property
    def healthy(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, Any]:
        return {
            "format": _FORMAT,
            "snapshot": dict(self.snapshot),
            "watermark": dict(self.watermark),
            "canonical_digest": self.canonical_digest,
            "healthy": self.healthy,
            "violations": [violation.as_dict() for violation in self.violations],
        }


def build_projection(database: Database, *, as_of: str | None = None) -> ProjectionSnapshot:
    """Build a single-snapshot dashboard projection from ``database`` only.

    ``as_of`` is the comparison instant for lease checks.  Pass it explicitly
    when reproducing a projection; this makes the output byte-for-byte
    deterministic for a fixed SQLite snapshot and instant.
    """

    return _build_projection(database, as_of or _utc_now(), None)


def _build_projection(
    database: Database,
    comparison_time: str,
    owner_private_state_dir: Path | None,
) -> ProjectionSnapshot:
    with database.connection_scope() as connection:
        connection.execute("BEGIN")
        try:
            return _build_projection_from_connection(
                connection,
                as_of=comparison_time,
                owner_private_state_dir=owner_private_state_dir,
            )
        finally:
            connection.rollback()


def build_projection_from_connection(
    connection: sqlite3.Connection, *, as_of: str
) -> ProjectionSnapshot:
    """Build from the caller's already-open SQLite snapshot.

    This is the cutover-safe primitive: the caller owns the transaction, so
    verification and the authority transition can observe exactly the same
    rows without a time-of-check/time-of-use gap.
    """

    return _build_projection_from_connection(
        connection,
        as_of=as_of,
        owner_private_state_dir=None,
    )


def project_work_items_from_connection(
    connection: sqlite3.Connection,
    *,
    work_ids: Sequence[str],
    as_of: str | None = None,
) -> list[dict[str, Any]]:
    """Project exact Works inside the caller's read transaction.

    Reuse the snapshot's canonical Work and close-proof semantics without
    rebuilding unrelated inventory, event watermarks or global health audits.
    """
    if not work_ids:
        return []
    close_semantics = _close_receipt_semantics(
        connection, None, work_ids=frozenset(work_ids)
    )
    return _work_items_projection(
        connection, as_of or _utc_now(), close_semantics.verified_receipts_by_work, work_ids
    )


def _build_projection_from_connection(
    connection: sqlite3.Connection,
    *,
    as_of: str,
    owner_private_state_dir: Path | None,
) -> ProjectionSnapshot:
    close_semantics = _close_receipt_semantics(
        connection,
        owner_private_state_dir,
    )
    snapshot = _snapshot(
        connection,
        as_of,
        close_semantics.verified_receipts_by_work,
    )
    watermark = _watermark(connection)
    violations = _violations(
        connection,
        as_of,
        close_semantics.violations,
    )
    ordered_violations = tuple(
        sorted(
            violations,
            key=lambda item: (item.code, item.subject_type, item.subject_ids, item.count),
        )
    )
    canonical_digest = _digest(
        {
            "format": _FORMAT,
            "snapshot": snapshot,
            "watermark": watermark,
            "violations": [item.as_dict() for item in ordered_violations],
        }
    )
    return ProjectionSnapshot(
        snapshot=snapshot,
        watermark=watermark,
        canonical_digest=canonical_digest,
        violations=ordered_violations,
    )


def verify_projection(
    database: Database,
    *,
    as_of: str | None = None,
    owner_private_state_dir: Path | None = None,
) -> ProjectionSnapshot:
    """Rebuild the kernel projection without writing database projection state.

    Normal callers leave ``owner_private_state_dir`` unset, binding close
    evidence to the database directory. A migration verifier may point a
    disposable database copy at the original owner-private evidence directory;
    canonical rows are still read exclusively from the copied database.
    """

    return _build_projection(database, as_of or _utc_now(), owner_private_state_dir)


@dataclass(frozen=True)
class _CloseReceiptSemantics:
    """The receipt ids that remain safe to project as closed.

    This internal result intentionally carries only opaque receipt/work ids and
    sanitized violation codes.  In particular, it never copies the raw effect
    target, evidence text, artifact URI, or workspace locator from SQLite.
    """

    verified_receipts_by_work: Mapping[str, str]
    violations: tuple[ProjectionViolation, ...]


def _close_inventory_digest(value: object) -> str:
    """Match the server-side canonical digest used for close preparations."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _json_array(value: object) -> list[Any] | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, list) else None


def _completion_claim_artifact_rows(
    connection: sqlite3.Connection,
    *,
    attempt_id: str,
    completion_claim_json: str,
) -> list[sqlite3.Row] | None:
    """Resolve the exact canonical artifact manifest without exposing locators."""

    try:
        claim = json.loads(completion_claim_json)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(claim, Mapping) or not claim:
        return None
    manifest = claim.get("artifacts", [])
    if not isinstance(manifest, list):
        return None
    manifest_scope = claim.get("artifact_manifest_scope")
    if manifest_scope not in {None, "verified_attempt_artifacts_v2"}:
        return None
    is_verified_v2 = manifest_scope == "verified_attempt_artifacts_v2"
    if not manifest:
        if is_verified_v2:
            return []
        return list(
            connection.execute(
                "SELECT * FROM artifacts WHERE attempt_id = ? ORDER BY created_at, id",
                (attempt_id,),
            ).fetchall()
        )
    rows: list[sqlite3.Row] = []
    seen: set[str] = set()
    for item in manifest:
        if not isinstance(item, Mapping):
            return None
        artifact_id = item.get("id")
        if not isinstance(artifact_id, str) or not artifact_id or artifact_id in seen:
            return None
        row = connection.execute(
            "SELECT * FROM artifacts WHERE id = ? AND attempt_id = ?",
            (artifact_id, attempt_id),
        ).fetchone()
        item_digest = item.get("digest", "")
        item_uri = item.get("uri", "")
        row_digest = str(row["digest"]) if row is not None else ""
        row_uri = str(row["uri"]) if row is not None else ""
        digest_matches = isinstance(item_digest, str) and (
            item_digest == row_digest
            or (not is_verified_v2 and _canonical_artifact_digest(item_digest) == row_digest)
        )
        if (
            row is None
            or not digest_matches
            or not isinstance(item_uri, str)
            or item_uri != row_uri
            or (
                is_verified_v2
                and (
                    re.fullmatch(r"[0-9a-f]{64}", row_digest) is None
                    or row_uri != f"owner-private-artifact:{row_digest}"
                )
            )
        ):
            return None
        seen.add(artifact_id)
        rows.append(row)
    return rows


def _string_record(item: object, fields: frozenset[str]) -> dict[str, str] | None:
    """Require the exact persisted close-record shape without exposing it."""

    if not isinstance(item, Mapping) or set(item) != fields:
        return None
    result: dict[str, str] = {}
    for field in fields:
        value = item.get(field)
        if not isinstance(value, str):
            return None
        result[field] = value
    return result


def _close_receipt_semantics(
    connection: sqlite3.Connection,
    owner_private_state_dir: Path | None,
    *,
    work_ids: frozenset[str] | None = None,
) -> _CloseReceiptSemantics:
    """Recompute close semantics from canonical rows without changing them.

    A receipt is a proof, not merely a link.  This verifier deliberately
    repeats the service-side reconstruction so an imported/corrupted database
    cannot make the read model claim a close that would no longer pass the
    close gate.
    """

    verified: dict[str, str] = {}
    violations: list[ProjectionViolation] = []
    receipts = connection.execute("SELECT * FROM work_close_receipts ORDER BY id").fetchall()
    receipts_per_work: dict[str, int] = {}
    receipts_per_decision: dict[str, int] = {}
    # Count globally even for a selected read: a duplicate decision on another
    # Work must still invalidate the selected receipt.
    for receipt in receipts:
        work_id = str(receipt["work_item_id"])
        decision_id = str(receipt["requester_decision_id"])
        receipts_per_work[work_id] = receipts_per_work.get(work_id, 0) + 1
        receipts_per_decision[decision_id] = receipts_per_decision.get(decision_id, 0) + 1
    for receipt in receipts:
        receipt_id = str(receipt["id"])
        work_id = str(receipt["work_item_id"])
        if work_ids is not None and work_id not in work_ids:
            continue
        if (
            receipts_per_work[work_id] == 1
            and receipts_per_decision[str(receipt["requester_decision_id"])] == 1
            and _verify_close_receipt_semantics(
                connection,
                receipt,
                owner_private_state_dir,
            )
        ):
            verified[work_id] = receipt_id
        else:
            violations.append(
                ProjectionViolation(
                    "close_receipt.semantic_invalid",
                    "work_close_receipt",
                    (receipt_id,),
                )
            )
    return _CloseReceiptSemantics(verified, tuple(violations))


def _verify_close_receipt_semantics(
    connection: sqlite3.Connection,
    receipt: sqlite3.Row,
    owner_private_state_dir: Path | None,
) -> bool:
    """Return whether one persisted receipt still satisfies the close gate."""

    work_id = str(receipt["work_item_id"])
    preparation_id = str(receipt["close_preparation_id"] or "")
    if not preparation_id or str(receipt["cleanup_inventory_evidence_id"] or "") != preparation_id:
        return False
    preparation = connection.execute(
        "SELECT * FROM work_close_preparations WHERE id = ? AND work_item_id = ?",
        (preparation_id, work_id),
    ).fetchone()
    if preparation is None:
        return False
    if (
        str(preparation["final_attempt_id"]) != str(receipt["attempt_id"])
        or int(preparation["work_generation"]) != int(receipt["work_generation"])
        or int(preparation["goal_version"]) != int(receipt["goal_version"])
        or str(preparation["goal_packet_digest"]) != str(receipt["goal_packet_digest"])
        or str(preparation["task_packet_digest"]) != str(receipt["task_packet_digest"])
        or str(preparation["supervisor_attachment_id"] or "")
        != str(receipt["supervisor_attachment_id"])
        or int(preparation["supervisor_attachment_generation"] or 0)
        != int(receipt["supervisor_attachment_generation"])
        or str(preparation["retention_policy_evidence_id"])
        != str(receipt["retention_policy_evidence_id"])
        or str(preparation["artifact_manifest_evidence_id"])
        != str(receipt["artifact_manifest_evidence_id"])
    ):
        return False

    prepared_artifacts = _json_array(preparation["artifacts_json"])
    prepared_preservations = _json_array(preparation["artifact_preservations_json"])
    prepared_inventory = _json_array(preparation["inventory_json"])
    receipt_artifacts = _json_array(receipt["artifacts_json"])
    receipt_cleanup = _json_array(receipt["cleanup_json"])
    if (
        prepared_artifacts is None
        or prepared_preservations is None
        or prepared_inventory is None
        or receipt_artifacts is None
        or receipt_cleanup is None
        or _close_inventory_digest(prepared_inventory) != str(preparation["inventory_digest"])
    ):
        return False

    prepared_artifact_records = [
        _string_record(item, frozenset({"id", "digest"})) for item in prepared_artifacts
    ]
    artifact_records = [
        _string_record(item, frozenset({"artifact_id", "digest", "evidence_id"}))
        for item in receipt_artifacts
    ]
    coverage_records = [
        _string_record(
            item,
            frozenset({"target_kind", "coverage", "inventory_set_digest", "provider_evidence_id"}),
        )
        for item in prepared_inventory
        if isinstance(item, Mapping) and item.get("coverage") == "not-applicable"
    ]
    prepared_cleanup_records = [
        _string_record(
            item,
            frozenset(
                {
                    "target_kind",
                    "coverage",
                    "target_fingerprint",
                    "action",
                    "inventory_set_digest",
                    "provider_evidence_id",
                    "execution_digest",
                }
            ),
        )
        for item in prepared_inventory
        if isinstance(item, Mapping) and item.get("coverage") == "enumerated"
    ] + [
        _string_record(item, frozenset({"target_kind", "target_fingerprint", "action"}))
        for item in prepared_inventory
        if isinstance(item, Mapping) and "coverage" not in item
    ]
    cleanup_records = [
        _string_record(
            item,
            frozenset(
                {
                    "target_kind",
                    "target_fingerprint",
                    "action",
                    "outcome",
                    "evidence_id",
                    "effect_operation_id",
                    "destructive_authority_evidence_id",
                }
            ),
        )
        for item in receipt_cleanup
    ]
    if any(
        item is None
        for item in (
            *prepared_artifact_records,
            *artifact_records,
            *coverage_records,
            *prepared_cleanup_records,
            *cleanup_records,
        )
    ):
        return False
    prepared_artifact_values = [item for item in prepared_artifact_records if item is not None]
    artifact_values = [item for item in artifact_records if item is not None]
    prepared_cleanup_values = [item for item in prepared_cleanup_records if item is not None]
    coverage_values = [item for item in coverage_records if item is not None]
    cleanup_values = [item for item in cleanup_records if item is not None]

    has_external_cleanup = any(
        item["target_kind"] in {"workspace", "temporary", "log", "branch"}
        for item in prepared_cleanup_values
    )
    preservation_values: list[dict[str, str]] = []
    if has_external_cleanup and (
        not preparation["cleanup_executed_at"]
        or _close_inventory_digest(prepared_preservations)
        != str(preparation["artifact_preservations_digest"])
    ):
        return False
    if has_external_cleanup:
        preservation_records = [
            _string_record(
                item,
                frozenset(
                    {
                        "artifact_id",
                        "digest",
                        "preservation",
                        "preservation_set_digest",
                        "provider_evidence_id",
                    }
                ),
            )
            for item in prepared_preservations
        ]
        if any(item is None for item in preservation_records):
            return False
        preservation_values = [item for item in preservation_records if item is not None]
        if [
            {
                "artifact_id": item["artifact_id"],
                "digest": item["digest"],
                "evidence_id": item["provider_evidence_id"],
            }
            for item in preservation_values
        ] != artifact_values:
            return False

    canonical_artifacts = {
        (str(row["id"]), str(row["digest"]))
        for row in connection.execute(
            "SELECT id, digest FROM artifacts WHERE work_item_id = ?", (work_id,)
        ).fetchall()
    }
    manifest_scope = str(preparation["artifact_manifest_scope"])
    if manifest_scope == "final_completion_claim_v2":
        goal = connection.execute(
            "SELECT packet_json FROM goal_revisions WHERE work_item_id = ? AND version = ?",
            (work_id, receipt["goal_version"]),
        ).fetchone()
        final_attempt = connection.execute(
            "SELECT completion_claim_json FROM attempts WHERE id = ? AND work_item_id = ?",
            (receipt["attempt_id"], work_id),
        ).fetchone()
        if goal is None or final_attempt is None:
            return False
        packet = _json_object(str(goal["packet_json"] or ""))
        contract = (
            str(packet.get("completion_contract") or "legacy_unclassified")
            if packet is not None
            else ""
        )
        if contract not in {
            "completion_required",
            "no_artifact_expected",
            "legacy_unclassified",
        }:
            return False
        claimed_rows = _completion_claim_artifact_rows(
            connection,
            attempt_id=str(receipt["attempt_id"]),
            completion_claim_json=str(final_attempt["completion_claim_json"]),
        )
        if claimed_rows is None:
            return False
        if contract == "completion_required" and (
            not claimed_rows
            or any(
                re.fullmatch(r"[0-9a-f]{64}", str(row["digest"])) is None
                or str(row["uri"]) != f"owner-private-artifact:{row['digest']}"
                for row in claimed_rows
            )
        ):
            return False
        claimed_artifacts = {(str(row["id"]), str(row["digest"])) for row in claimed_rows}
        required_artifacts = claimed_artifacts
    elif manifest_scope == "work_history_v1":
        required_artifacts = canonical_artifacts
    else:
        return False
    prepared_artifact_set = {(item["id"], item["digest"]) for item in prepared_artifact_values}
    receipt_artifact_set = {(item["artifact_id"], item["digest"]) for item in artifact_values}
    if (
        len(prepared_artifact_set) != len(prepared_artifact_values)
        or len(receipt_artifact_set) != len(artifact_values)
        or prepared_artifact_set != receipt_artifact_set
        or prepared_artifact_set != required_artifacts
    ):
        return False

    prepared_cleanup_set = {
        (item["target_kind"], item["target_fingerprint"], item["action"])
        for item in prepared_cleanup_values
    }
    receipt_cleanup_set = {
        (item["target_kind"], item["target_fingerprint"], item["action"]) for item in cleanup_values
    }
    if (
        len(prepared_cleanup_set) != len(prepared_cleanup_values)
        or len(receipt_cleanup_set) != len(cleanup_values)
        or prepared_cleanup_set != receipt_cleanup_set
        or {
            item["target_kind"]
            for item in coverage_values + prepared_cleanup_values
            if item["target_kind"] in {"workspace", "temporary", "log", "branch"}
        }
        != {"workspace", "temporary", "log", "branch"}
    ):
        return False
    try:
        if owner_private_state_dir is None:
            database_file = next(
                str(row["file"])
                for row in connection.execute("PRAGMA database_list").fetchall()
                if str(row["name"]) == "main"
            )
            private_state = Path(database_file).parent
        else:
            private_state = owner_private_state_dir
        provider = OwnerPrivateCloseInventory(private_state)
        provider_records = [
            item for item in prepared_inventory if isinstance(item, Mapping) and "coverage" in item
        ]
        if not provider.verify_public_inventory(
            preparation_id=preparation_id,
            work_item_id=work_id,
            generation=int(receipt["work_generation"]),
            attachment_id=str(receipt["supervisor_attachment_id"]),
            public_inventory=provider_records,
        ):
            return False
        if has_external_cleanup:
            artifact_provider = OwnerPrivateArtifactPreservation(private_state)
            if not artifact_provider.verify_public_receipt_set(
                work_item_id=work_id,
                preparation_id=preparation_id,
                attachment_id=str(receipt["supervisor_attachment_id"]),
                attachment_generation=int(receipt["supervisor_attachment_generation"]),
                public_receipts=preservation_values,
            ):
                return False
    except (OSError, StopIteration):
        return False

    try:
        plan = ClosePlan(
            work_item_id=work_id,
            attempt_id=str(receipt["attempt_id"]),
            review_id=str(receipt["review_id"]),
            requester_decision_id=str(receipt["requester_decision_id"]),
            expected_goal_version=int(receipt["goal_version"]),
            expected_goal_packet_digest=str(receipt["goal_packet_digest"]),
            expected_task_packet_digest=str(receipt["task_packet_digest"]),
            expected_generation=int(receipt["work_generation"]),
            retention_policy_evidence_id=str(receipt["retention_policy_evidence_id"]),
            artifact_manifest_evidence_id=str(receipt["artifact_manifest_evidence_id"]),
            cleanup_inventory_evidence_id=str(receipt["cleanup_inventory_evidence_id"]),
            artifacts=tuple(
                ArtifactPreservation(item["artifact_id"], item["digest"], item["evidence_id"])
                for item in artifact_values
            ),
            cleanup=tuple(
                CleanupRecord(
                    CleanupTargetKind(item["target_kind"]),
                    item["target_fingerprint"],
                    CleanupAction(item["action"]),
                    CleanupOutcome(item["outcome"]),
                    item["evidence_id"],
                    item["effect_operation_id"],
                    item["destructive_authority_evidence_id"],
                )
                for item in cleanup_values
            ),
            close_preparation_id=preparation_id,
        )
    except (TypeError, ValueError):
        return False
    if plan.digest() != str(receipt["plan_digest"]):
        return False

    work = connection.execute("SELECT * FROM work_items WHERE id = ?", (work_id,)).fetchone()
    attempt = connection.execute(
        "SELECT * FROM attempts WHERE id = ? AND work_item_id = ?",
        (receipt["attempt_id"], work_id),
    ).fetchone()
    review = connection.execute(
        "SELECT verdict FROM reviews WHERE id = ? AND work_item_id = ? AND attempt_id = ?",
        (receipt["review_id"], work_id, receipt["attempt_id"]),
    ).fetchone()
    decision = connection.execute(
        "SELECT verdict FROM requester_decisions WHERE id = ? AND work_item_id = ?",
        (receipt["requester_decision_id"], work_id),
    ).fetchone()
    if work is None or attempt is None or review is None or decision is None:
        return False

    open_deliveries = _count_where(
        connection,
        "message_deliveries AS d JOIN messages AS m ON m.id = d.message_id",
        "m.work_item_id = ? AND d.state IN ('queued', 'leased', 'dispatched', 'delivered', 'acknowledged')",
        (work_id,),
    )
    active_runtimes = _count_where(
        connection,
        "runtime_sessions",
        "id IN (SELECT DISTINCT runtime_session_id FROM attempts WHERE work_item_id = ? AND runtime_session_id IS NOT NULL) "
        "AND state NOT IN ('stopped', 'failed', 'missing')",
        (work_id,),
    )
    nonterminal_enrollments = _count_where(
        connection,
        "worker_enrollments",
        "runtime_session_id IN (SELECT DISTINCT runtime_session_id FROM attempts WHERE work_item_id = ? AND runtime_session_id IS NOT NULL) "
        "AND state NOT IN ('revoked', 'failed')",
        (work_id,),
    )
    effects = connection.execute(
        "SELECT * FROM effect_operations WHERE cleanup_work_item_id = ? AND cleanup_generation = ? ORDER BY id",
        (work_id, int(receipt["work_generation"])),
    ).fetchall()
    if not _cleanup_effects_match_receipt(connection, effects, cleanup_values, receipt):
        return False
    unresolved_effects = sum(
        str(effect["status"]) not in {"succeeded", "not_applied"} for effect in effects
    )
    readiness = CloseReadiness(
        work_state=str(work["state"]),
        attempt_state=str(attempt["state"]),
        review_verdict=str(review["verdict"]),
        requester_decision_verdict=str(decision["verdict"]),
        goal_version=int(work["goal_version"]),
        goal_packet_digest=str(attempt["goal_packet_digest"]),
        task_packet_digest=str(attempt["task_packet_digest"]),
        generation=int(work["generation"]),
        artifact_ids=tuple(
            sorted(artifact_id for artifact_id, _digest_value in receipt_artifact_set)
        ),
        open_delivery_count=open_deliveries,
        active_runtime_count=active_runtimes + nonterminal_enrollments,
        unresolved_effect_count=unresolved_effects,
    )
    return evaluate_close(plan, readiness).ready


def _cleanup_effects_match_receipt(
    connection: sqlite3.Connection,
    effects: list[sqlite3.Row],
    cleanup: list[dict[str, str]],
    receipt: sqlite3.Row,
) -> bool:
    """Require every cleanup-bound effect to prove one exact cleanup record."""

    cleanup_by_effect = {
        item["effect_operation_id"]: item for item in cleanup if item["effect_operation_id"]
    }
    if len(cleanup_by_effect) != sum(bool(item["effect_operation_id"]) for item in cleanup):
        return False
    internal_kinds = {
        CleanupTargetKind.RUNTIME.value,
        CleanupTargetKind.SUPERVISION_REGISTRATION.value,
    }
    for item in cleanup:
        if item["target_kind"] in internal_kinds:
            if item["effect_operation_id"]:
                return False
            continue
        effect = next(
            (row for row in effects if str(row["id"]) == item["effect_operation_id"]), None
        )
        if effect is None:
            return False
        if (
            str(effect["principal_id"]) != str(receipt["closed_by"])
            or str(effect["action"]) != item["action"]
            or str(effect["cleanup_work_item_id"] or "") != str(receipt["work_item_id"])
            or int(effect["cleanup_generation"] or 0) != int(receipt["work_generation"])
            or str(effect["cleanup_preparation_id"] or "") != str(receipt["close_preparation_id"])
            or str(effect["cleanup_target_kind"] or "") != item["target_kind"]
            or str(effect["cleanup_target_fingerprint"] or "") != item["target_fingerprint"]
            or str(effect["status"]) not in {"succeeded", "not_applied"}
            or str(effect["content_digest"] or "") != str(effect["cleanup_execution_digest"] or "")
        ):
            return False
        if item["action"] in {"trash", "delete"}:
            grant_id = str(effect["grant_id"] or "")
            if (
                str(effect["kind"]) != "destructive"
                or not grant_id
                or item["destructive_authority_evidence_id"] != grant_id
                or _destructive_grant_invalid(connection, effect, grant_id)
            ):
                return False
        elif item["destructive_authority_evidence_id"]:
            return False
    for effect in effects:
        bound_cleanup = cleanup_by_effect.get(str(effect["id"]))
        if bound_cleanup is None:
            return False
        if (
            str(effect["cleanup_target_kind"]),
            str(effect["cleanup_target_fingerprint"]),
            str(effect["action"]),
        ) != (
            bound_cleanup["target_kind"],
            bound_cleanup["target_fingerprint"],
            bound_cleanup["action"],
        ):
            return False
    return len(cleanup_by_effect) == len(effects)


def _destructive_grant_invalid(
    connection: sqlite3.Connection, effect: sqlite3.Row, grant_id: str
) -> bool:
    """Verify the opaque destructive grant still covers the sealed effect."""

    grant = connection.execute(
        """
        SELECT principal_id, kind, target_pattern, action_pattern, content_digest,
               argv_digest, workdir_digest
        FROM effect_grants WHERE id = ?
        """,
        (grant_id,),
    ).fetchone()
    return (
        grant is None
        or str(grant["principal_id"]) != str(effect["principal_id"])
        or str(grant["kind"]) != "destructive"
        or not matches(str(grant["target_pattern"]), str(effect["target"]))
        or not matches(str(grant["action_pattern"]), str(effect["action"]))
        or (
            bool(grant["content_digest"])
            and str(grant["content_digest"]) != str(effect["content_digest"])
        )
        or (bool(grant["argv_digest"]) and str(grant["argv_digest"]) != str(effect["argv_digest"]))
        or (
            bool(grant["workdir_digest"])
            and str(grant["workdir_digest"]) != str(effect["workdir_digest"])
        )
    )


def _work_items_projection(
    connection: sqlite3.Connection,
    comparison_time: str,
    semantic_receipts: Mapping[str, str],
    work_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    if work_ids is not None and not work_ids:
        return []
    query = """
        WITH valid_requester_decisions AS (
            SELECT d.id, d.work_item_id, d.review_id, d.verdict
            FROM requester_decisions d
            JOIN work_items decision_work ON decision_work.id = d.work_item_id
            JOIN attempts decision_attempt
              ON decision_attempt.id = d.attempt_id
             AND decision_attempt.work_item_id = decision_work.id
            JOIN reviews decision_review ON decision_review.id = d.review_id
                                          AND decision_review.work_item_id = decision_work.id
                                          AND decision_review.attempt_id = decision_attempt.id
            JOIN cao_session_attachments decision_attachment
              ON decision_attachment.id = d.supervisor_attachment_id
            WHERE decision_work.requester_id = d.requester_id
              AND decision_work.supervisor_id = d.recorded_by
              AND decision_work.supervisor_attachment_id = d.supervisor_attachment_id
              AND decision_work.generation = d.work_generation
              AND decision_attachment.principal_id = d.recorded_by
              AND decision_attachment.generation >= d.supervisor_attachment_generation
              AND decision_review.reviewer_role = 'cao'
              AND decision_review.verdict = 'ok'
              AND decision_review.supervisor_attachment_id = d.supervisor_attachment_id
              AND decision_review.supervisor_attachment_generation
                  <= d.supervisor_attachment_generation
              AND decision_review.work_generation <= d.work_generation
              AND decision_attempt.goal_version = d.goal_version
              AND decision_attempt.goal_packet_digest = d.goal_packet_digest
              AND decision_attempt.task_packet_digest = d.task_packet_digest
              AND decision_review.goal_version = d.goal_version
              AND decision_review.goal_packet_digest = d.goal_packet_digest
              AND decision_review.task_packet_digest = d.task_packet_digest
        ), valid_close_receipts AS (
            SELECT c.id, c.work_item_id
            FROM work_close_receipts c
            JOIN work_items close_work ON close_work.id = c.work_item_id
            JOIN attempts close_attempt ON close_attempt.id = c.attempt_id
                                      AND close_attempt.work_item_id = close_work.id
            JOIN reviews close_review ON close_review.id = c.review_id
                                      AND close_review.work_item_id = close_work.id
                                      AND close_review.attempt_id = close_attempt.id
            JOIN valid_requester_decisions decision
              ON decision.id = c.requester_decision_id
             AND decision.work_item_id = close_work.id
             AND decision.verdict = 'accepted'
            JOIN requester_decisions decision_row ON decision_row.id = decision.id
            JOIN cao_session_attachments close_attachment
              ON close_attachment.id = c.supervisor_attachment_id
            WHERE c.cleanup_inventory_evidence_id <> ''
              AND close_work.state = 'completed'
              AND close_work.supervisor_id = c.closed_by
              AND close_work.supervisor_attachment_id = c.supervisor_attachment_id
              AND close_work.generation = c.work_generation
              AND close_attachment.principal_id = c.closed_by
              AND close_attachment.generation >= c.supervisor_attachment_generation
              AND decision_row.attempt_id = close_attempt.id
              AND decision_row.review_id = close_review.id
              AND decision_row.recorded_by = c.closed_by
              AND decision_row.supervisor_attachment_id = c.supervisor_attachment_id
              AND decision_row.supervisor_attachment_generation
                  <= c.supervisor_attachment_generation
              AND decision_row.work_generation = c.work_generation
              AND close_attempt.goal_version = c.goal_version
              AND close_attempt.goal_packet_digest = c.goal_packet_digest
              AND close_attempt.task_packet_digest = c.task_packet_digest
              AND close_review.goal_version = c.goal_version
              AND close_review.goal_packet_digest = c.goal_packet_digest
              AND close_review.task_packet_digest = c.task_packet_digest
              AND decision_row.goal_version = c.goal_version
              AND decision_row.goal_packet_digest = c.goal_packet_digest
              AND decision_row.task_packet_digest = c.task_packet_digest
        )
        SELECT w.id, w.goal_version, w.state, w.priority, w.assigned_worker_id,
               w.attention_owner, w.generation, w.operator_scope,
               w.paused_boundary_id, w.supervisor_id, w.suspended_by_work_item_id,
               w.user_needed_boundary_id,
               w.created_at AS work_created_at, w.updated_at AS work_updated_at,
               a.id AS current_attempt_id, a.state AS current_attempt_state,
               a.trajectory AS current_attempt_trajectory,
               a.stage AS current_attempt_progress_stage,
               a.next_boundary AS current_attempt_next_boundary,
               a.evidence_confidence AS current_attempt_evidence_confidence,
               runtime.adapter AS current_runtime_adapter,
               runtime.state AS current_runtime_state,
               (
                   SELECT COUNT(*)
                   FROM managed_worker_specs candidate_spec
                   WHERE candidate_spec.principal_id = a.worker_id
                     AND candidate_spec.runtime_session_id = a.runtime_session_id
               ) AS managed_spec_candidate_count,
               (
                   SELECT COUNT(*)
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
               ) AS managed_spec_scoped_count,
               (
                   SELECT scoped_spec.adapter
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS managed_spec_adapter,
               (
                   SELECT scoped_spec.requested_model
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS managed_spec_requested_model,
               (
                   SELECT scoped_spec.effective_model
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS managed_spec_effective_model,
               (
                   SELECT scoped_spec.requested_reasoning_effort
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS managed_spec_requested_reasoning_effort,
               (
                   SELECT scoped_spec.effective_reasoning_effort
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS managed_spec_effective_reasoning_effort,
               (
                   SELECT scoped_spec.state
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS managed_spec_state,
               (
                   SELECT CASE circuit.state
                              WHEN 'open' THEN 'rate_limited'
                              WHEN 'half_open' THEN 'probing'
                              WHEN 'blocked' THEN 'rate_limited'
                          END
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   JOIN provider_runtime_circuits circuit
                     ON circuit.scope_digest = scoped_spec.provider_scope_digest
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS provider_condition,
               (
                   SELECT CASE WHEN circuit.state = 'open'
                               THEN circuit.cooldown_until END
                   FROM managed_worker_specs scoped_spec
                   JOIN cao_session_attachments scoped_attachment
                     ON scoped_attachment.id = scoped_spec.attachment_id
                   JOIN provider_runtime_circuits circuit
                     ON circuit.scope_digest = scoped_spec.provider_scope_digest
                   WHERE scoped_spec.principal_id = a.worker_id
                     AND scoped_spec.runtime_session_id = a.runtime_session_id
                     AND scoped_attachment.principal_id = work_attachment.principal_id
                     AND scoped_attachment.project_scope_digest = work_attachment.project_scope_digest
                     AND scoped_spec.attachment_generation <= scoped_attachment.generation
                   LIMIT 1
               ) AS provider_retry_after_at,
               goal.title AS operator_work_title,
               goal.objective AS operator_objective,
               goal.packet_json AS operator_goal_packet_json,
               a.completion_claim_json AS current_completion_claim_json,
               (
                   SELECT message.payload_json
                   FROM messages message
                   WHERE message.work_item_id = w.id
                     AND message.attempt_id = a.id
                     AND message.sender_id = w.assigned_worker_id
                     AND (message.kind IN ('progress', 'question', 'blocker', 'artifact', 'completion_claim')
                          OR (message.kind = 'system' AND json_extract(message.payload_json, '$.action') = 'worker_output'
                              AND EXISTS (SELECT 1 FROM worker_output_receipts output WHERE output.notification_message_id = message.id)))
                   ORDER BY message.sequence DESC
                   LIMIT 1
               ) AS operator_worker_report_payload,
               (
                   SELECT CASE WHEN message.kind = 'system' AND json_extract(message.payload_json, '$.action') = 'worker_output'
                               AND EXISTS (SELECT 1 FROM worker_output_receipts output WHERE output.notification_message_id = message.id)
                               THEN 'worker_output' ELSE message.kind END
                   FROM messages message
                   WHERE message.work_item_id = w.id
                     AND message.attempt_id = a.id
                     AND message.sender_id = w.assigned_worker_id
                     AND (message.kind IN ('progress', 'question', 'blocker', 'artifact', 'completion_claim')
                          OR (message.kind = 'system' AND json_extract(message.payload_json, '$.action') = 'worker_output'
                              AND EXISTS (SELECT 1 FROM worker_output_receipts output WHERE output.notification_message_id = message.id)))
                   ORDER BY message.sequence DESC
                   LIMIT 1
               ) AS operator_worker_report_kind,
               (
                   SELECT message.created_at
                   FROM messages message
                   WHERE message.work_item_id = w.id
                     AND message.attempt_id = a.id
                     AND message.sender_id = w.assigned_worker_id
                     AND (message.kind IN ('progress', 'question', 'blocker', 'artifact', 'completion_claim')
                          OR (message.kind = 'system' AND json_extract(message.payload_json, '$.action') = 'worker_output'
                              AND EXISTS (SELECT 1 FROM worker_output_receipts output WHERE output.notification_message_id = message.id)))
                   ORDER BY message.sequence DESC
                   LIMIT 1
               ) AS operator_worker_report_created_at,
               (SELECT COUNT(*) FROM attempts WHERE work_item_id = w.id) AS attempt_count,
               (SELECT COUNT(*)
                  FROM boundaries b
                  LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
                  LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
                 WHERE b.work_item_id = w.id AND d.id IS NULL
                   AND s.boundary_id IS NULL) AS open_boundary_count,
               (SELECT CASE
                           WHEN SUM(
                               CASE WHEN b.recovery_action IN (
                                            'dispose_continue_or_correct',
                                            'reconcile_continue_same_thread',
                                            'system_reconciliation'
                                        )
                                    THEN 1 ELSE 0 END
                           ) = 0 THEN ''
                           WHEN SUM(
                               CASE WHEN b.recovery_action IN (
                                            'dispose_continue_or_correct',
                                            'reconcile_continue_same_thread',
                                            'system_reconciliation'
                                        )
                                    THEN 1 ELSE 0 END
                           ) > 1 THEN 'system_reconciliation'
                           ELSE MAX(
                               CASE WHEN b.recovery_action IN (
                                            'dispose_continue_or_correct',
                                            'reconcile_continue_same_thread',
                                            'system_reconciliation'
                                        )
                                    THEN b.recovery_action
                                    ELSE 'system_reconciliation'
                               END
                           )
                       END
                  FROM boundaries b
                  LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
                  LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
                 WHERE b.work_item_id = w.id AND d.id IS NULL
                   AND s.boundary_id IS NULL) AS open_boundary_recovery_action,
               (SELECT MIN(b.created_at)
                  FROM boundaries b
                  LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
                  LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
                 WHERE b.work_item_id = w.id AND d.id IS NULL
                   AND s.boundary_id IS NULL
                   AND b.recovery_action IN (
                       'dispose_continue_or_correct',
                       'reconcile_continue_same_thread',
                       'system_reconciliation'
                   )) AS open_boundary_created_at,
               (SELECT COUNT(*)
                  FROM directives d
                 WHERE d.target_work_item_id = w.id OR d.created_work_item_id = w.id) AS directive_count,
               (SELECT COUNT(*) FROM messages m WHERE m.work_item_id = w.id) AS message_count,
               (SELECT id FROM valid_close_receipts c WHERE c.work_item_id = w.id)
                   AS close_receipt_id,
               (SELECT verdict FROM valid_requester_decisions decision
                 WHERE decision.work_item_id = w.id
                 ORDER BY decision.id DESC LIMIT 1) AS requester_decision_verdict,
               (SELECT r.verdict
                  FROM valid_requester_decisions decision
                  JOIN reviews r ON r.id = decision.review_id
                 WHERE decision.work_item_id = w.id
                 ORDER BY decision.id DESC LIMIT 1) AS requester_decision_review_verdict,
               (SELECT review.verdict
                  FROM reviews review
                  JOIN attempts reviewed_attempt ON reviewed_attempt.id = review.attempt_id
                 WHERE review.work_item_id = w.id
                   AND reviewed_attempt.work_item_id = w.id
                   AND review.reviewer_role = 'cao'
                   AND review.work_generation = w.generation
                   AND review.goal_version = w.goal_version
                 ORDER BY review.created_at DESC, review.id DESC
                 LIMIT 1) AS latest_cao_review_verdict,
               (SELECT COUNT(*)
                  FROM message_deliveries delivery
                  JOIN messages message ON message.id = delivery.message_id
                 WHERE message.work_item_id = w.id
                   AND delivery.state IN (
                       'queued', 'leased', 'dispatched', 'delivered', 'acknowledged'
                   )) AS open_delivery_count,
               CASE
                   WHEN a.runtime_session_id IS NULL THEN 0
                   WHEN EXISTS (
                       SELECT 1 FROM runtime_sessions runtime
                       WHERE runtime.id = a.runtime_session_id
                         AND runtime.state NOT IN ('stopped', 'failed', 'missing')
                   ) THEN 1
                   ELSE 0
               END AS active_runtime_count,
               CASE
                   WHEN EXISTS (
                       SELECT 1 FROM valid_requester_decisions decision
                       WHERE decision.work_item_id = w.id AND decision.verdict = 'accepted'
                   ) THEN 'conversation'
                   ELSE ''
               END AS requester_acceptance_source
        FROM work_items w
        LEFT JOIN cao_session_attachments work_attachment
          ON work_attachment.id = w.supervisor_attachment_id
        JOIN goal_revisions goal ON goal.work_item_id = w.id
                               AND goal.version = w.goal_version
        LEFT JOIN attempts a ON a.work_item_id = w.id
                            AND a.attempt_number = (
                                SELECT MAX(attempt_number)
                                FROM attempts current_attempt
                                WHERE current_attempt.work_item_id = w.id
                            )
        LEFT JOIN runtime_sessions runtime ON runtime.id = a.runtime_session_id
        """
    if work_ids is not None:
        query += " WHERE w.id IN (" + ",".join("?" for _ in work_ids) + ")"
    query += " ORDER BY w.id"
    work_rows = connection.execute(query, tuple(work_ids or ())).fetchall()
    work_items = []
    for row in work_rows:
        runner = _managed_runner_projection(row)
        completion_contract, delivery_state = _operator_completion_delivery_projection(
            connection, row
        )
        attempt_activity = _operator_attempt_activity_projection(
            connection,
            attempt_id=str(row["current_attempt_id"] or ""),
            worker_id=str(row["assigned_worker_id"] or ""),
            as_of=comparison_time,
        )
        cao_supervision = _operator_cao_supervision_projection(
            connection,
            work_item_id=str(row["id"]),
            as_of=comparison_time,
        )
        verified_receipt_id = semantic_receipts.get(str(row["id"]), "")
        if verified_receipt_id != str(row["close_receipt_id"] or ""):
            verified_receipt_id = ""
        work_items.append(
            {
                "id": str(row["id"]),
                "goal_version": int(row["goal_version"]),
                "state": str(row["state"]),
                "priority": int(row["priority"]),
                "assigned_worker_id": str(row["assigned_worker_id"]),
                "attention_owner": str(row["attention_owner"]),
                "generation": int(row["generation"]),
                "supervision_pause": _supervision_pause_projection(connection, row),
                "operator_scope": str(row["operator_scope"]),
                "created_at": str(row["work_created_at"]),
                "updated_at": str(row["work_updated_at"]),
                "current_attempt_id": str(row["current_attempt_id"] or ""),
                "current_attempt_state": str(row["current_attempt_state"] or ""),
                "current_attempt_trajectory": str(row["current_attempt_trajectory"] or ""),
                "current_attempt_evidence_confidence": str(
                    row["current_attempt_evidence_confidence"] or ""
                ),
                # This deliberately contains only the current CAO-authored Goal
                # and the Worker MCP report summary.  It is a bounded, redacted
                # operator display value, not a general text projection.
                "operator_content": {
                    "work_title": sanitize_operator_text(row["operator_work_title"]),
                    "objective_summary": sanitize_operator_text(row["operator_objective"]),
                    "objective_text": sanitize_operator_text(
                        row["operator_objective"], limit=None, collapse_whitespace=False
                    ),
                    "progress_stage": sanitize_operator_text(row["current_attempt_progress_stage"]),
                    "next_boundary_summary": sanitize_operator_text(
                        row["current_attempt_next_boundary"]
                    ),
                    "recovery_action": str(row["open_boundary_recovery_action"] or ""),
                    "recovery_waiting_since": _operator_timestamp(row["open_boundary_created_at"]),
                    "recovery_notification_state": (
                        _operator_recovery_notification_state(
                            connection,
                            work_item_id=str(row["id"]),
                        )
                    ),
                    **cao_supervision,
                    "latest_report_kind": _operator_report_kind(row["operator_worker_report_kind"]),
                    "latest_worker_report_summary": _operator_report_summary(
                        row["operator_worker_report_payload"]
                    ),
                    "latest_report_text": _operator_report_summary(
                        row["operator_worker_report_payload"], full=True
                    ),
                    "latest_reported_at": _operator_timestamp(
                        row["operator_worker_report_created_at"]
                    ),
                    "completion_contract": completion_contract,
                    "delivery_state": delivery_state,
                    **attempt_activity,
                    "provider_condition": _operator_provider_condition(row["provider_condition"]),
                    "provider_retry_after_at": _operator_timestamp(row["provider_retry_after_at"]),
                    **runner,
                },
                "attempt_count": int(row["attempt_count"]),
                "open_boundary_count": int(row["open_boundary_count"]),
                "directive_count": int(row["directive_count"]),
                "message_count": int(row["message_count"]),
                "requester_acceptance_source": str(row["requester_acceptance_source"]),
                "close_receipt_id": verified_receipt_id,
                "closure_state": (
                    "closed"
                    if verified_receipt_id
                    else "awaiting-explicit-close"
                    if str(row["state"]) == "completed"
                    else "open"
                ),
                "closure_summary": {
                    "requester_decision": (
                        str(row["requester_decision_verdict"])
                        if row["requester_decision_verdict"]
                        else "pending"
                    ),
                    "cao_review": (
                        "needs-work"
                        if row["latest_cao_review_verdict"] == "needs_work"
                        else str(
                            row["latest_cao_review_verdict"]
                            or row["requester_decision_review_verdict"]
                            or "pending"
                        )
                    ),
                    "artifact_preservation": ("preserved" if verified_receipt_id else "pending"),
                    "cleanup": "verified" if verified_receipt_id else "pending",
                    "unresolved_deliveries": int(row["open_delivery_count"]),
                    "unresolved_effects": 0 if verified_receipt_id else None,
                    "active_runtimes": int(row["active_runtime_count"]),
                },
            }
        )
    return work_items


def _snapshot(
    connection: sqlite3.Connection,
    comparison_time: str,
    semantic_receipts: Mapping[str, str],
) -> dict[str, Any]:
    authority_row = connection.execute(
        """
        SELECT mode, generation, activated_at, updated_at
        FROM control_authority WHERE singleton = 1
        """
    ).fetchone()
    authority = (
        {
            "mode": str(authority_row["mode"]),
            "generation": int(authority_row["generation"]),
            "activated_at": str(authority_row["activated_at"] or ""),
            "updated_at": str(authority_row["updated_at"]),
        }
        if authority_row is not None
        else {"mode": "missing", "generation": 0}
    )
    work_items = _work_items_projection(connection, comparison_time, semantic_receipts)
    operator_workers = _operator_workers_projection(connection)
    operator_runtime_delivery = _operator_runtime_delivery_projection(
        connection,
        comparison_time,
    )
    runtime_rows = connection.execute(
        """
        SELECT id, principal_id, adapter, state, lease_expires_at
        FROM runtime_sessions
        ORDER BY id
        """
    ).fetchall()
    runtimes = [
        {
            "id": str(row["id"]),
            "principal_id": str(row["principal_id"]),
            "adapter": str(row["adapter"]),
            "state": str(row["state"]),
            "lease_expires_at": str(row["lease_expires_at"]),
        }
        for row in runtime_rows
    ]
    records = {
        "source_receipts": _count(connection, "source_receipts"),
        "submitted_intents": _count(connection, "submitted_intents"),
        "intent_dispositions": _count(connection, "intent_dispositions"),
        "directives": _count(connection, "directives"),
        "work_items": len(work_items),
        "goal_revisions": _count(connection, "goal_revisions"),
        "attempts": _count(connection, "attempts"),
        "boundaries": _count(connection, "boundaries"),
        "boundary_dispositions": _count(connection, "boundary_dispositions"),
        "boundary_supersessions": _count(connection, "boundary_supersessions"),
        "requester_decisions": _count(connection, "requester_decisions"),
        "work_close_receipts": _count(connection, "work_close_receipts"),
        "messages": _count(connection, "messages"),
        "message_deliveries": _count(connection, "message_deliveries"),
        "reasoner_turns": _count(connection, "reasoner_turns"),
        "runtime_sessions": len(runtimes),
        "worker_enrollments": _count(connection, "worker_enrollments"),
        "runtime_enrollment_tickets": _count(connection, "runtime_enrollment_tickets"),
        "runtime_credentials": _count(connection, "runtime_credentials"),
        "events": _count(connection, "events"),
    }
    pending_intents = _count_where(
        connection,
        "submitted_intents",
        "NOT EXISTS (SELECT 1 FROM intent_dispositions d "
        "WHERE d.submitted_intent_id = submitted_intents.id)",
    )
    scheduler = {
        "deliveries_by_state": _group_counts(connection, "message_deliveries", "state"),
        "queued_due": _count_where(
            connection,
            "message_deliveries",
            "state = 'queued' AND next_attempt_at <= ?",
            (comparison_time,),
        ),
        "leased": _count_where(connection, "message_deliveries", "state = 'leased'"),
        "outcome_unknown": _count_where(connection, "message_deliveries", "state = 'dispatched'"),
        "expired_delivery_leases": _count_where(
            connection,
            "message_deliveries",
            "state = 'leased' AND lease_until IS NOT NULL AND lease_until < ?",
            (comparison_time,),
        ),
        "dead": _count_where(connection, "message_deliveries", "state = 'dead'"),
    }
    return {
        "authority": authority,
        "records": records,
        "pending_intents": pending_intents,
        "work_items": work_items,
        "operator_workers": operator_workers,
        "operator_runtime_delivery": operator_runtime_delivery,
        "intents_by_kind": _group_counts(connection, "intent_dispositions", "kind"),
        "directives_by_relation": _group_counts(connection, "directives", "relation"),
        "directives_by_state": _group_counts(connection, "directives", "state"),
        "attempts_by_state": _group_counts(connection, "attempts", "state"),
        "boundaries_by_kind": _group_counts(connection, "boundaries", "kind"),
        "boundary_dispositions_by_kind": _group_counts(connection, "boundary_dispositions", "kind"),
        "boundary_supersessions_by_reason": _group_counts(
            connection, "boundary_supersessions", "reason"
        ),
        "reasoner_turns_by_state": _group_counts(connection, "reasoner_turns", "state"),
        "runtimes": runtimes,
        "runtimes_by_state": _group_counts(connection, "runtime_sessions", "state"),
        "enrollments_by_state": _group_counts(connection, "worker_enrollments", "state"),
        "scheduler": scheduler,
    }


def _operator_workers_projection(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Project every Worker as a first-class, scope-bearing inventory row.

    This projection is still internal to the Control Plane and therefore keeps
    opaque relationship keys for the Dashboard assembler.  The public
    Dashboard DTO removes them.  A managed specification is the only source
    of runner/model detail; runtime metadata and workspace references are
    deliberately absent.
    """

    rows = connection.execute(
        """
        SELECT principal.id AS principal_id,
               principal.enabled AS principal_enabled,
               principal.operator_scope,
               principal.operator_label,
               spec.id AS managed_spec_id,
               spec.state AS managed_spec_state,
               spec.adapter AS managed_spec_adapter,
               spec.requested_model AS managed_spec_requested_model,
               spec.effective_model AS managed_spec_effective_model,
               spec.requested_reasoning_effort AS managed_spec_requested_reasoning_effort,
               spec.effective_reasoning_effort AS managed_spec_effective_reasoning_effort,
               runtime.state AS runtime_state,
               enrollment.state AS enrollment_state
        FROM principals AS principal
        LEFT JOIN managed_worker_specs AS spec
          ON spec.principal_id = principal.id
        LEFT JOIN managed_worker_threads AS worker_thread
          ON worker_thread.managed_spec_id = spec.id
        LEFT JOIN runtime_sessions AS runtime
          ON runtime.id = COALESCE(
              spec.runtime_session_id,
              (
                  SELECT candidate.id
                  FROM runtime_sessions AS candidate
                  LEFT JOIN worker_enrollments AS candidate_enrollment
                    ON candidate_enrollment.runtime_session_id = candidate.id
                  WHERE candidate.principal_id = principal.id
                  ORDER BY CASE
                               WHEN candidate_enrollment.state IN (
                                   'awaiting_handshake', 'ready', 'stale'
                               ) THEN 0
                               ELSE 1
                           END,
                           candidate.updated_at DESC,
                           candidate.created_at DESC,
                           candidate.id DESC
                  LIMIT 1
              )
          )
        LEFT JOIN worker_enrollments AS enrollment
          ON enrollment.id = COALESCE(
              spec.enrollment_id,
              (
                  SELECT candidate.id
                  FROM worker_enrollments AS candidate
                  WHERE candidate.principal_id = principal.id
                    AND candidate.runtime_session_id = runtime.id
                  ORDER BY CASE
                               WHEN candidate.state IN (
                                   'awaiting_handshake', 'ready', 'stale'
                               ) THEN 0
                               ELSE 1
                           END,
                           candidate.generation DESC,
                           candidate.updated_at DESC,
                           candidate.created_at DESC,
                           candidate.id DESC
                  LIMIT 1
              )
          )
        WHERE principal.role = 'worker'
          AND (
              spec.id IS NULL
              OR worker_thread.state = 'active'
          )
        ORDER BY principal.id
        """
    ).fetchall()
    workers: list[dict[str, Any]] = []
    for row in rows:
        scope = str(row["operator_scope"] or "unclassified")
        raw_label = sanitize_operator_text(row["operator_label"])
        label = raw_label if raw_label else None
        workers.append(
            {
                "principal_id": str(row["principal_id"]),
                "operator_scope": scope,
                "operator_label": label,
                "principal_enabled": bool(row["principal_enabled"]),
                "enrollment_state": str(row["enrollment_state"] or ""),
                **_managed_worker_inventory_projection(row, scope=scope),
            }
        )
    return workers


def _managed_worker_inventory_projection(
    row: sqlite3.Row,
    *,
    scope: str,
) -> dict[str, str | None]:
    unavailable: dict[str, str | None] = {
        "worker_state": "unavailable",
        "runner_adapter": None,
        "runner_model": None,
        "runner_reasoning_effort": None,
        "runner_requested_model": None,
        "runner_effective_model": None,
        "runner_requested_reasoning_effort": None,
        "runner_effective_reasoning_effort": None,
        "runner_availability": "unavailable",
        "runner_connection_state": "unavailable",
    }
    if not row["managed_spec_id"]:
        if bool(row["principal_enabled"]):
            unavailable["worker_state"] = "enabled"
        return unavailable

    state = row["managed_spec_state"]
    if not isinstance(state, str) or state not in {"enabled", "stopped", "revoked"}:
        return unavailable
    unavailable["worker_state"] = state
    if state != "enabled" or not bool(row["principal_enabled"]):
        return unavailable

    adapter = _operator_adapter(row["managed_spec_adapter"])
    requested_model = _operator_model(row["managed_spec_requested_model"])
    effective_model = _operator_model(row["managed_spec_effective_model"])
    requested_effort = row["managed_spec_requested_reasoning_effort"]
    effective_effort = row["managed_spec_effective_reasoning_effort"]
    if (
        adapter not in _MANAGED_RUNNER_ADAPTERS
        or requested_model is None
        or effective_model is None
        or requested_effort not in _MANAGED_REASONING_EFFORTS
        or effective_effort not in _MANAGED_REASONING_EFFORTS
    ):
        return unavailable
    return {
        "worker_state": "enabled",
        "runner_adapter": adapter,
        "runner_model": effective_model,
        "runner_reasoning_effort": str(effective_effort),
        "runner_requested_model": requested_model,
        "runner_effective_model": effective_model,
        "runner_requested_reasoning_effort": str(requested_effort),
        "runner_effective_reasoning_effort": str(effective_effort),
        "runner_availability": "available",
        "runner_connection_state": _runner_connection_state(row["runtime_state"]),
    }


def _operator_runtime_delivery_projection(
    connection: sqlite3.Connection,
    comparison_time: str,
) -> dict[str, Any]:
    """Return production-only runtime, delivery, and effect aggregates."""

    runtime = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM runtime_sessions AS runtime
        JOIN principals AS principal ON principal.id = runtime.principal_id
        WHERE principal.role = 'worker' AND principal.operator_scope = 'production'
        """
    ).fetchone()
    delivery_rows = connection.execute(
        """
        SELECT delivery.state, COUNT(*) AS count
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        JOIN work_items AS work ON work.id = message.work_item_id
        WHERE work.operator_scope = 'production'
        GROUP BY delivery.state
        ORDER BY delivery.state
        """
    ).fetchall()
    deliveries = {str(row["state"]): int(row["count"]) for row in delivery_rows}
    queued = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        JOIN work_items AS work ON work.id = message.work_item_id
        WHERE work.operator_scope = 'production'
          AND delivery.state = 'queued' AND delivery.next_attempt_at <= ?
        """,
        (comparison_time,),
    ).fetchone()
    effect_rows = connection.execute(
        """
        SELECT effect.status, COUNT(*) AS count
        FROM effect_operations AS effect
        JOIN work_items AS work ON work.id = effect.cleanup_work_item_id
        WHERE work.operator_scope = 'production'
        GROUP BY effect.status
        ORDER BY effect.status
        """
    ).fetchall()
    return {
        "runtime_count": int(runtime["count"] if runtime else 0),
        "queued_deliveries": int(queued["count"] if queued else 0),
        "unknown_delivery_outcomes": deliveries.get("dispatched", 0),
        "dead_deliveries": deliveries.get("dead", 0),
        "effects_by_state": {str(row["status"]): int(row["count"]) for row in effect_rows},
    }


def _operator_report_summary(payload: object, *, full: bool = False) -> str | None:
    """Extract only a Worker report's canonical ``summary`` property."""

    if not isinstance(payload, str):
        return None
    try:
        raw = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return sanitize_operator_text(
        raw.get("summary") if isinstance(raw, Mapping) else None,
        limit=None if full else _OPERATOR_TEXT_LIMIT,
        collapse_whitespace=not full,
    )


def _operator_report_kind(value: object) -> str | None:
    """Keep only a canonical Worker handoff kind."""

    return value if isinstance(value, str) and value in _OPERATOR_REPORT_KINDS else None


def _operator_completion_delivery_projection(
    connection: sqlite3.Connection, row: sqlite3.Row
) -> tuple[str | None, str | None]:
    """Expose only the bounded delivery gate, never artifact locators or claims."""

    packet = _json_object(str(row["operator_goal_packet_json"] or ""))
    contract = (
        str(packet.get("completion_contract") or "legacy_unclassified")
        if packet is not None
        else ""
    )
    if contract not in {
        "completion_required",
        "no_artifact_expected",
        "legacy_unclassified",
    }:
        return None, None
    claim = _json_object(str(row["current_completion_claim_json"] or ""))
    if not claim:
        return contract, "pending"
    if contract == "no_artifact_expected":
        return contract, "not_required"
    if contract == "legacy_unclassified":
        return contract, "legacy_unclassified"
    artifacts = _completion_claim_artifact_rows(
        connection,
        attempt_id=str(row["current_attempt_id"]),
        completion_claim_json=str(row["current_completion_claim_json"]),
    )
    if not artifacts or any(
        str(artifact["uri"]) != f"owner-private-artifact:{artifact['digest']}"
        or re.fullmatch(r"[0-9a-f]{64}", str(artifact["digest"])) is None
        for artifact in artifacts
    ):
        return contract, "delivery_missing"
    return contract, "ready"


def _operator_timestamp(value: object) -> str | None:
    """Keep only a bounded canonical timestamp, never arbitrary message text."""

    return value if isinstance(value, str) and _OPERATOR_TIMESTAMP.fullmatch(value) else None


def _operator_recovery_notification_payload_valid(
    message: sqlite3.Row,
    boundary: sqlite3.Row,
    payload: object,
) -> bool:
    """Verify the exact canonical notification for one recovery Boundary."""

    if not isinstance(payload, Mapping):
        return False
    action = payload.get("action")
    if not isinstance(action, str) or action not in _RECOVERY_NOTIFICATION_ACTIONS:
        return False

    common_payload_keys = {
        "action",
        "boundary_id",
        "boundary_kind",
        "generation",
    }
    packet_payload_keys = {"goal_packet_digest", "task_packet_digest"}
    if action == "recover_terminal_worker_attempt":
        accepted_payload_key_sets = {
            frozenset(common_payload_keys | packet_payload_keys | {"reason"})
        }
        try:
            metadata = json.loads(str(boundary["metadata_json"]))
        except (json.JSONDecodeError, TypeError, ValueError):
            return False
        if not isinstance(metadata, Mapping) or str(payload.get("reason") or "") != str(
            metadata.get("reason") or ""
        ):
            return False
    elif action in {
        "recover_expired_reasoner_turn",
        "recover_incomplete_reasoner_turn",
    }:
        action_keys = common_payload_keys | {"prior_turn_id"}
        accepted_payload_key_sets = {
            frozenset(action_keys),
            frozenset(action_keys | packet_payload_keys),
        }
        if not isinstance(payload.get("prior_turn_id"), str):
            return False
    else:
        accepted_payload_key_sets = {
            frozenset(common_payload_keys),
            frozenset(common_payload_keys | packet_payload_keys),
        }

    payload_keys = frozenset(payload)
    generation = payload.get("generation")
    has_packet_binding = packet_payload_keys.issubset(payload_keys)
    return bool(
        payload_keys in accepted_payload_key_sets
        and str(message["kind"]) == "system"
        and str(message["sender_id"] or "") == str(boundary["source_principal_id"])
        and str(message["work_item_id"] or "") == str(boundary["work_item_id"])
        and str(message["attempt_id"] or "") == str(boundary["attempt_id"])
        and message["goal_version"] == boundary["goal_version"]
        and str(message["goal_packet_digest"] or "") == str(boundary["goal_packet_digest"])
        and str(message["task_packet_digest"] or "") == str(boundary["task_packet_digest"])
        and str(payload.get("boundary_id") or "") == str(boundary["id"])
        and str(payload.get("boundary_kind") or "") == str(boundary["kind"])
        and not isinstance(generation, bool)
        and generation == int(boundary["generation"])
        and (
            not has_packet_binding
            or (
                str(payload.get("goal_packet_digest") or "") == str(boundary["goal_packet_digest"])
                and str(payload.get("task_packet_digest") or "")
                == str(boundary["task_packet_digest"])
            )
        )
        and str(message["payload_digest"] or "") == _digest(payload)
    )


def _operator_recovery_notification_state(
    connection: sqlite3.Connection,
    *,
    work_item_id: str,
) -> str:
    """Project notification state only from one exact open recovery Boundary."""

    boundaries = connection.execute(
        """
        SELECT boundary.*, work.supervisor_id
        FROM boundaries AS boundary
        JOIN work_items AS work ON work.id = boundary.work_item_id
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        WHERE boundary.work_item_id = ?
          AND disposition.id IS NULL
          AND supersession.boundary_id IS NULL
        ORDER BY boundary.created_at, boundary.id
        """,
        (work_item_id,),
    ).fetchall()
    if len(boundaries) != 1:
        return ""
    boundary = boundaries[0]
    if (
        str(boundary["kind"]) != "failure"
        or str(boundary["recovery_action"] or "") not in _RECOVERY_ACTIONS
    ):
        return ""

    messages = connection.execute(
        """
        SELECT message.*, delivery.state AS delivery_state
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE delivery.recipient_id = ?
          AND message.kind = 'system'
          AND message.sender_id = ?
          AND message.work_item_id = ?
          AND message.attempt_id = ?
          AND message.goal_version = ?
          AND message.goal_packet_digest = ?
          AND message.task_packet_digest = ?
        ORDER BY CASE delivery.state
                     WHEN 'queued' THEN 0
                     WHEN 'leased' THEN 0
                     WHEN 'dispatched' THEN 0
                     WHEN 'delivered' THEN 0
                     WHEN 'acknowledged' THEN 0
                     ELSE 1
                 END,
                 message.sequence DESC, message.id DESC
        """,
        (
            boundary["supervisor_id"],
            boundary["source_principal_id"],
            boundary["work_item_id"],
            boundary["attempt_id"],
            boundary["goal_version"],
            boundary["goal_packet_digest"],
            boundary["task_packet_digest"],
        ),
    ).fetchall()
    for message in messages:
        try:
            payload = json.loads(str(message["payload_json"]))
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if not _operator_recovery_notification_payload_valid(message, boundary, payload):
            continue
        state = str(message["delivery_state"] or "")
        return state if state in _DELIVERY_STATES else ""
    return ""


def _operator_cao_supervision_projection(
    connection: sqlite3.Connection,
    *,
    work_item_id: str,
    as_of: str,
) -> dict[str, str | None]:
    """Project whether one durable CAO obligation is scheduled or executing."""

    boundaries = connection.execute(
        """
        SELECT boundary.*, work.supervisor_id, work.supervisor_attachment_id
        FROM boundaries AS boundary
        JOIN work_items AS work ON work.id = boundary.work_item_id
        LEFT JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
        LEFT JOIN boundary_supersessions AS supersession
          ON supersession.boundary_id = boundary.id
        WHERE boundary.work_item_id = ?
          AND boundary.generation = work.generation
          AND work.attention_owner = 'cao'
          AND disposition.id IS NULL
          AND supersession.boundary_id IS NULL
        ORDER BY boundary.created_at, boundary.id
        """,
        (work_item_id,),
    ).fetchall()
    if not boundaries:
        return {
            "cao_supervision_state": None,
            "cao_supervision_updated_at": None,
        }
    if len(boundaries) != 1:
        return {
            "cao_supervision_state": "unscheduled",
            "cao_supervision_updated_at": _operator_timestamp(boundaries[0]["created_at"]),
        }
    boundary = boundaries[0]
    active_turn = connection.execute(
        """
        SELECT updated_at FROM reasoner_turns
        WHERE boundary_id = ? AND supervisor_id = ?
          AND state = 'leased' AND lease_expires_at >= ?
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (boundary["id"], boundary["supervisor_id"], as_of),
    ).fetchone()
    if active_turn is not None:
        return {
            "cao_supervision_state": "active",
            "cao_supervision_updated_at": _operator_timestamp(active_turn["updated_at"]),
        }
    delivery = connection.execute(
        """
        SELECT delivery.state, delivery.updated_at
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.work_item_id = ?
          AND message.attempt_id = ?
          AND message.goal_version = ?
          AND message.goal_packet_digest = ?
          AND message.task_packet_digest = ?
          AND delivery.recipient_id = ?
          AND delivery.recipient_attachment_id IS ?
          AND json_valid(message.payload_json)
          AND json_extract(message.payload_json, '$.boundary_id') = ?
        ORDER BY CASE delivery.state
                     WHEN 'queued' THEN 0
                     WHEN 'leased' THEN 0
                     WHEN 'dispatched' THEN 0
                     WHEN 'delivered' THEN 0
                     WHEN 'acknowledged' THEN 0
                     ELSE 1
                 END,
                 message.sequence DESC, message.id DESC
        LIMIT 1
        """,
        (
            boundary["work_item_id"],
            boundary["attempt_id"],
            boundary["goal_version"],
            boundary["goal_packet_digest"],
            boundary["task_packet_digest"],
            boundary["supervisor_id"],
            boundary["supervisor_attachment_id"],
            boundary["id"],
        ),
    ).fetchone()
    state = str(delivery["state"] or "") if delivery is not None else ""
    if state in {
        "queued",
        "leased",
        "dispatched",
        "delivered",
        "acknowledged",
    }:
        supervision_state = "scheduled"
    else:
        supervision_state = "unscheduled"
    return {
        "cao_supervision_state": supervision_state,
        "cao_supervision_updated_at": _operator_timestamp(
            delivery["updated_at"] if delivery is not None else boundary["created_at"]
        ),
    }


def _operator_attempt_activity_projection(
    connection: sqlite3.Connection,
    *,
    attempt_id: str,
    worker_id: str,
    as_of: str,
) -> dict[str, str | None]:
    """Project independent liveness, work, artifact, and status-request clocks."""

    runtime_heartbeat = (
        connection.execute(
            """
            SELECT enrollment.heartbeat_at
            FROM attempts AS attempt
            JOIN worker_enrollments AS enrollment
              ON enrollment.runtime_session_id = attempt.runtime_session_id
            WHERE attempt.id = ? AND enrollment.managed = 1
              AND enrollment.heartbeat_at IS NOT NULL
              AND enrollment.heartbeat_at >= attempt.created_at
              AND EXISTS (
                  SELECT 1 FROM runtime_enrollment_tickets AS ticket
                  WHERE ticket.enrollment_id = enrollment.id
                    AND ticket.attempt_id = attempt.id
                    AND ticket.state = 'consumed'
                    AND ticket.generation = enrollment.generation
                    AND ticket.created_at >= attempt.created_at
                    AND ticket.consumed_at >= attempt.created_at
              )
            """,
            (attempt_id,),
        ).fetchone()
        if attempt_id
        else None
    )

    worker_activity = (
        connection.execute(
            "SELECT created_at FROM messages "
            "WHERE attempt_id = ? AND sender_id = ? "
            "AND kind IN ('progress', 'question', 'blocker', 'completion_claim') "
            "ORDER BY sequence DESC LIMIT 1",
            (attempt_id, worker_id),
        ).fetchone()
        if attempt_id and worker_id
        else None
    )
    artifact = (
        connection.execute(
            "SELECT created_at FROM artifacts WHERE attempt_id = ? "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (attempt_id,),
        ).fetchone()
        if attempt_id
        else None
    )
    status = (
        connection.execute(
            "SELECT sequence, payload_json, created_at FROM messages "
            "WHERE attempt_id = ? AND kind = 'status_request' "
            "ORDER BY sequence DESC LIMIT 1",
            (attempt_id,),
        ).fetchone()
        if attempt_id
        else None
    )
    status_requested_at: str | None = None
    status_response_due_at: str | None = None
    status_responded_at: str | None = None
    status_request_state: str | None = None
    if status is not None:
        try:
            payload = json.loads(str(status["payload_json"]))
        except json.JSONDecodeError:
            payload = {}
        status_requested_at = _operator_timestamp(status["created_at"])
        status_response_due_at = _operator_timestamp(
            payload.get("response_due_at") if isinstance(payload, Mapping) else None
        )
        response = connection.execute(
            """
            SELECT created_at FROM messages
            WHERE attempt_id = ? AND sender_id = ? AND sequence > ?
              AND kind IN ('progress', 'question', 'blocker', 'completion_claim')
            ORDER BY sequence ASC LIMIT 1
            """,
            (attempt_id, worker_id, status["sequence"]),
        ).fetchone()
        status_responded_at = _operator_timestamp(
            response["created_at"] if response is not None else None
        )
        if status_responded_at:
            status_request_state = "responded"
        elif status_response_due_at:
            try:
                due = datetime.fromisoformat(status_response_due_at.replace("Z", "+00:00"))
                observed = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            except ValueError:
                status_request_state = None
            else:
                status_request_state = "overdue" if due <= observed else "pending"
    return {
        "runtime_heartbeat_at": _operator_timestamp(
            runtime_heartbeat["heartbeat_at"] if runtime_heartbeat is not None else None
        ),
        "last_worker_activity_at": _operator_timestamp(
            worker_activity["created_at"] if worker_activity is not None else None
        ),
        "last_artifact_at": _operator_timestamp(
            artifact["created_at"] if artifact is not None else None
        ),
        "status_request_state": status_request_state,
        "status_requested_at": status_requested_at,
        "status_response_due_at": status_response_due_at,
        "status_responded_at": status_responded_at,
    }


def _operator_provider_condition(value: object) -> str | None:
    """Keep only the fixed circuit state, never a provider response body."""

    return value if value in {"rate_limited", "probing", "recovered"} else None


def sanitize_operator_text(
    value: object,
    *,
    limit: int | None = _OPERATOR_TEXT_LIMIT,
    collapse_whitespace: bool = True,
) -> str | None:
    """Produce a deterministic, bounded, locator-free display string."""

    if not isinstance(value, str):
        return None
    normalized = unicodedata.normalize("NFKC", value)
    if collapse_whitespace:
        normalized = " ".join(normalized.split())
    normalized = str(redact_control_plane_secrets(normalized))
    normalized = _OPERATOR_URL.sub("[uri-redacted]", normalized)
    normalized = _OPERATOR_PATH.sub("[path-redacted]", normalized)
    normalized = _OPERATOR_SECRET.sub("[secret-redacted]", normalized)
    normalized = _OPERATOR_GITHUB_TOKEN.sub("[secret-redacted]", normalized)
    normalized = _OPERATOR_INTERNAL_ID.sub("[internal-id-redacted]", normalized)
    if not normalized:
        return None
    return (
        normalized
        if limit is None or len(normalized) <= limit
        else normalized[: limit - 1].rstrip() + "…"
    )


def _operator_adapter(value: object) -> str | None:
    allowed = {"codex-app-server", "claude", "subprocess", "webhook"}
    return value if isinstance(value, str) and value in allowed else None


def _operator_model(value: object) -> str | None:
    """Allow only the bounded model identifier contract, never free text."""

    return model_identifier(value)


def _managed_runner_projection(row: sqlite3.Row) -> dict[str, str | None]:
    """Project one attachment-scoped managed Worker specification, fail closed.

    A managed specification is authoritative only when exactly one spec binds
    the current Attempt's Worker/runtime pair to the WorkItem's owning CAO
    attachment *and* that attachment's current generation.  The dashboard must
    not repair a missing relationship from runtime metadata, native state, or
    a similarly named Worker specification.
    """

    candidate_count = int(row["managed_spec_candidate_count"] or 0)
    scoped_count = int(row["managed_spec_scoped_count"] or 0)
    runtime_adapter = _operator_adapter(row["current_runtime_adapter"])
    unavailable: dict[str, str | None] = {
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
    }
    if candidate_count != 1:
        if candidate_count == 0:
            unavailable["runner_state"] = (
                "unavailable" if runtime_adapter in _MANAGED_RUNNER_ADAPTERS else "unsupported"
            )
        else:
            unavailable["runner_state"] = "invalid"
        return unavailable
    if scoped_count != 1:
        unavailable["runner_state"] = "mismatched"
        return unavailable

    state = row["managed_spec_state"]
    if not isinstance(state, str) or state not in {"enabled", "stopped", "revoked"}:
        unavailable["runner_state"] = "invalid"
        return unavailable
    if state != "enabled":
        unavailable["runner_state"] = state
        return unavailable

    adapter = row["managed_spec_adapter"]
    requested_model = _operator_model(row["managed_spec_requested_model"])
    effective_model = _operator_model(row["managed_spec_effective_model"])
    requested_effort = row["managed_spec_requested_reasoning_effort"]
    effective_effort = row["managed_spec_effective_reasoning_effort"]
    if (
        adapter not in _MANAGED_RUNNER_ADAPTERS
        or runtime_adapter != adapter
        or requested_model is None
        or effective_model is None
        or requested_effort not in _MANAGED_REASONING_EFFORTS
        or effective_effort not in _MANAGED_REASONING_EFFORTS
    ):
        unavailable["runner_state"] = "invalid"
        return unavailable
    return {
        "runner_adapter": adapter,
        # Compatibility aliases intentionally reflect the effective settings.
        "runner_model": effective_model,
        "runner_reasoning_effort": effective_effort,
        "runner_requested_model": requested_model,
        "runner_effective_model": effective_model,
        "runner_requested_reasoning_effort": requested_effort,
        "runner_effective_reasoning_effort": effective_effort,
        "runner_availability": "available",
        "runner_state": "enabled",
        "runner_connection_state": _runner_connection_state(row["current_runtime_state"]),
    }


def _runner_connection_state(value: object) -> str:
    """Map durable runtime lifecycle to an operator-safe connection label.

    This deliberately says nothing about a PID or terminal.  ``waiting`` is
    an enrolled Worker whose native thread can be reopened with a fresh
    process-bound MCP credential; it is not an assertion that a process is
    still running.
    """

    states = {
        "starting": "enrolling",
        "ready": "connected-idle",
        "busy": "connected-busy",
        "waiting": "enrolled-reopenable",
        "stopped": "stopped",
        "failed": "failed",
        "missing": "missing",
    }
    return states.get(value, "unavailable") if isinstance(value, str) else "unavailable"


def _watermark(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute(
        "SELECT sequence, id, created_at FROM events ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    schema = connection.execute(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        return {
            "schema_version": str(schema["value"] if schema else ""),
            "event_sequence": 0,
            "event_id_digest": "",
            "event_created_at": "",
        }
    return {
        "schema_version": str(schema["value"] if schema else ""),
        "event_sequence": int(row["sequence"]),
        "event_id_digest": _digest(str(row["id"])),
        "event_created_at": str(row["created_at"]),
    }


def _supervision_pause_projection(
    connection: sqlite3.Connection, work: sqlite3.Row
) -> dict[str, Any] | None:
    try:
        pause = pause_view_tx(connection, work)
    except ConflictError:
        # Corrupt authority is visible through fixed violation codes, never
        # through raw legacy text or a failed whole-Dashboard read.
        return None
    if pause is None:
        return None
    return {
        "boundary_id": pause["boundary_id"],
        "source_generation": pause["source_generation"],
        "pause_generation": pause["pause_generation"],
        "reason": sanitize_operator_text(pause["reason"]),
        "resume_condition": sanitize_operator_text(pause["resume_condition"]),
        "paused_at": pause["paused_at"],
    }


def _supervision_pause_violations(
    connection: sqlite3.Connection, violations: list[ProjectionViolation]
) -> None:
    _add_rows(
        violations,
        connection,
        "work_pause.binding_invalid",
        "work_pause",
        f"SELECT pause.boundary_id AS id FROM work_pauses AS pause "
        f"WHERE NOT {work_pause_record_binding_sql()}",
    )
    _add_rows(
        violations,
        connection,
        "work_pause_resumption.binding_invalid",
        "work_pause_resumption",
        f"SELECT resumption.boundary_id AS id FROM work_pause_resumptions AS resumption "
        f"WHERE NOT {work_pause_resumption_binding_sql()}",
    )
    _add_rows(
        violations,
        connection,
        "work.supervision_pause_binding_invalid",
        "work_item",
        f"""
        SELECT work.id FROM work_items AS work
        WHERE work.paused_boundary_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM work_pauses AS pause JOIN attempts AS attempt ON attempt.id = pause.attempt_id
            WHERE pause.boundary_id = work.paused_boundary_id AND pause.work_item_id = work.id
              AND pause.pause_generation = work.generation AND pause.paused_by = work.supervisor_id
              AND work.state = 'suspended' AND work.attention_owner = 'none'
              AND work.user_needed_boundary_id IS NULL AND work.suspended_by_work_item_id IS NULL
              AND attempt.work_item_id = work.id AND attempt.worker_id = work.assigned_worker_id
              AND attempt.goal_version = work.goal_version AND attempt.state = 'suspended'
              AND {work_pause_record_binding_sql()}
              AND NOT EXISTS (SELECT 1 FROM work_pause_resumptions AS resumption WHERE resumption.boundary_id = pause.boundary_id)
              AND NOT EXISTS (SELECT 1 FROM attempts AS later WHERE later.work_item_id = work.id
                              AND later.attempt_number > attempt.attempt_number)
        )
        """,
    )
    _add_rows(
        violations,
        connection,
        "work_pause.unconsumed_not_current",
        "work_pause",
        """
        SELECT pause.boundary_id AS id FROM work_pauses AS pause
        JOIN work_items AS work ON work.id = pause.work_item_id
        JOIN attempts AS attempt ON attempt.id = pause.attempt_id
        WHERE work.state NOT IN ('completed', 'canceled', 'failed')
          AND attempt.state = 'suspended'
          AND work.paused_boundary_id IS NOT pause.boundary_id
          AND NOT EXISTS (SELECT 1 FROM work_pause_resumptions AS resumption WHERE resumption.boundary_id = pause.boundary_id)
        """,
    )


def _violations(
    connection: sqlite3.Connection,
    comparison_time: str,
    close_receipt_violations: tuple[ProjectionViolation, ...],
) -> list[ProjectionViolation]:
    violations: list[ProjectionViolation] = []
    integrity = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    if integrity != ["ok"]:
        violations.append(
            ProjectionViolation("sqlite.integrity_check_failed", "database", count=len(integrity))
        )
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        violations.append(
            ProjectionViolation(
                "sqlite.foreign_key_check_failed", "database", count=len(foreign_keys)
            )
        )
    authority_rows = connection.execute(
        "SELECT singleton, mode, generation FROM control_authority"
    ).fetchall()
    if len(authority_rows) != 1 or int(authority_rows[0]["singleton"]) != 1:
        violations.append(
            ProjectionViolation(
                "authority.singleton_invalid", "control_authority", count=len(authority_rows)
            )
        )
    elif str(authority_rows[0]["mode"]) != "canonical":
        violations.append(ProjectionViolation("authority.mode_invalid", "control_authority"))

    _add_rows(
        violations,
        connection,
        "intent.receipt_without_submitted_intent",
        "source_receipt",
        """
        SELECT r.id FROM source_receipts r
        LEFT JOIN submitted_intents i ON i.source_receipt_id = r.id
        WHERE i.id IS NULL
        """,
    )
    _add_rows(
        violations,
        connection,
        "intent.without_disposition",
        "submitted_intent",
        """
        SELECT i.id FROM submitted_intents i
        LEFT JOIN intent_dispositions d ON d.submitted_intent_id = i.id
        WHERE d.id IS NULL
          AND (
            EXISTS (
              SELECT 1 FROM directives linked
              WHERE linked.submitted_intent_id = i.id
            )
          )
        """,
    )
    _add_rows(
        violations,
        connection,
        "intent.without_supervisor_delivery",
        "submitted_intent",
        """
        SELECT i.id FROM submitted_intents i
        LEFT JOIN intent_dispositions disposition
          ON disposition.submitted_intent_id = i.id
        WHERE disposition.id IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM messages m
            JOIN message_deliveries delivery ON delivery.message_id = m.id
            JOIN principals recipient ON recipient.id = delivery.recipient_id
            WHERE m.causation_id = i.id
              AND m.kind = 'system'
              AND recipient.role = 'cao'
          )
        """,
    )
    _add_rows(
        violations,
        connection,
        "intent.disposition_receipt_mismatch",
        "intent_disposition",
        """
        SELECT d.id FROM intent_dispositions d
        JOIN submitted_intents i ON i.id = d.submitted_intent_id
        WHERE d.source_receipt_id <> i.source_receipt_id
        """,
    )
    _add_rows(
        violations,
        connection,
        "work.current_goal_missing_or_wrong",
        "work_item",
        """
        SELECT w.id FROM work_items w
        LEFT JOIN goal_revisions g
          ON g.work_item_id = w.id AND g.version = w.goal_version
        WHERE g.work_item_id IS NULL
        """,
    )
    _add_rows(
        violations,
        connection,
        "work.goal_revision_sequence_invalid",
        "work_item",
        """
        SELECT w.id FROM work_items w
        LEFT JOIN goal_revisions g ON g.work_item_id = w.id
        GROUP BY w.id
        HAVING COUNT(g.version) = 0
            OR MIN(g.version) <> 1
            OR COUNT(g.version) <> MAX(g.version)
            OR MAX(g.version) <> w.goal_version
        """,
    )
    _packet_violations(connection, violations)
    _close_lifecycle_violations(
        connection,
        violations,
        close_receipt_violations,
    )
    _add_rows(
        violations,
        connection,
        "work.without_attempt",
        "work_item",
        """
        SELECT w.id FROM work_items w
        LEFT JOIN attempts a ON a.work_item_id = w.id
        WHERE a.id IS NULL
        """,
    )
    _add_rows(
        violations,
        connection,
        "attempt.sequence_invalid",
        "work_item",
        """
        SELECT work_item_id AS id FROM attempts
        GROUP BY work_item_id
        HAVING MIN(attempt_number) <> 1 OR COUNT(*) <> MAX(attempt_number)
        """,
    )
    _add_rows(
        violations,
        connection,
        "work.assigned_worker_differs_from_current_attempt",
        "work_item",
        """
        SELECT w.id FROM work_items w
        JOIN attempts a ON a.work_item_id = w.id
        WHERE a.attempt_number = (
            SELECT MAX(current_attempt.attempt_number)
            FROM attempts current_attempt
            WHERE current_attempt.work_item_id = w.id
        )
          AND a.worker_id <> w.assigned_worker_id
        """,
    )
    _work_state_violations(connection, violations)
    _supervision_pause_violations(connection, violations)
    _add_rows(
        violations,
        connection,
        "review.boundary_binding_invalid",
        "review",
        """
        SELECT review.id
        FROM reviews AS review
        LEFT JOIN boundaries AS boundary ON boundary.id = review.boundary_id
        WHERE review.boundary_id IS NOT NULL
          AND (
              boundary.id IS NULL
              OR boundary.kind NOT IN ('completion', 'worker_output')
              OR boundary.work_item_id <> review.work_item_id
              OR boundary.attempt_id <> review.attempt_id
              OR boundary.generation <> review.work_generation
              OR boundary.goal_version <> review.goal_version
              OR boundary.goal_packet_digest <> review.goal_packet_digest
              OR boundary.task_packet_digest <> review.task_packet_digest
          )
        """,
    )
    _add_rows(
        violations,
        connection,
        "boundary.supersession_invalid",
        "boundary",
        """
        SELECT supersession.boundary_id AS id
        FROM boundary_supersessions AS supersession
        WHERE NOT (
          EXISTS (
            SELECT 1
            FROM boundaries AS boundary
            JOIN work_items AS work ON work.id = boundary.work_item_id
            JOIN events AS boundary_event
              ON boundary_event.sequence = supersession.boundary_event_sequence
             AND boundary_event.event_type = 'boundary.recorded'
             AND boundary_event.aggregate_type = 'work_item'
             AND boundary_event.aggregate_id = boundary.work_item_id
             AND json_extract(boundary_event.data_json, '$.boundary_id') = boundary.id
            JOIN events AS superseding_event
              ON superseding_event.sequence = supersession.superseding_event_sequence
             AND superseding_event.event_type = 'work.canceled'
             AND superseding_event.aggregate_type = 'work_item'
             AND superseding_event.aggregate_id = boundary.work_item_id
             AND superseding_event.sequence > boundary_event.sequence
            LEFT JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
            WHERE boundary.id = supersession.boundary_id
              AND work.state = 'canceled'
              AND boundary.generation < work.generation
              AND disposition.id IS NULL
              AND supersession.reason = 'work_canceled'
              AND (
                  SELECT COUNT(*) FROM events AS exact_boundary_event
                  WHERE exact_boundary_event.event_type = 'boundary.recorded'
                    AND exact_boundary_event.aggregate_type = 'work_item'
                    AND exact_boundary_event.aggregate_id = boundary.work_item_id
                    AND json_extract(
                          exact_boundary_event.data_json, '$.boundary_id'
                        ) = boundary.id
              ) = 1
          ) OR EXISTS (
            SELECT 1
            FROM boundaries AS boundary
            JOIN events AS boundary_event
              ON boundary_event.sequence = supersession.boundary_event_sequence
             AND boundary_event.event_type = 'boundary.recorded'
             AND boundary_event.aggregate_type = 'work_item'
             AND boundary_event.aggregate_id = boundary.work_item_id
             AND json_extract(boundary_event.data_json, '$.boundary_id') = boundary.id
            JOIN events AS superseding_event
              ON superseding_event.sequence = supersession.superseding_event_sequence
             AND superseding_event.event_type = 'boundary.recorded'
             AND superseding_event.aggregate_type = 'work_item'
             AND superseding_event.aggregate_id = boundary.work_item_id
             AND superseding_event.sequence > boundary_event.sequence
            JOIN boundaries AS replacement
              ON replacement.id = json_extract(
                   superseding_event.data_json, '$.boundary_id'
                 )
             AND replacement.work_item_id = boundary.work_item_id
             AND replacement.attempt_id = boundary.attempt_id
             AND replacement.generation = boundary.generation
            LEFT JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
            WHERE boundary.id = supersession.boundary_id
              AND disposition.id IS NULL
              AND supersession.reason = 'recovery_boundary_replaced'
              AND (
                json_extract(boundary.metadata_json, '$.runtime_recovery') = 1
                OR json_extract(boundary.metadata_json, '$.system_recovery') = 1
              )
              AND (
                json_extract(replacement.metadata_json, '$.runtime_recovery') = 1
                OR json_extract(replacement.metadata_json, '$.system_recovery') = 1
              )
              AND (
                  SELECT COUNT(*) FROM events AS exact_boundary_event
                  WHERE exact_boundary_event.event_type = 'boundary.recorded'
                    AND exact_boundary_event.aggregate_type = 'work_item'
                    AND exact_boundary_event.aggregate_id = boundary.work_item_id
                    AND json_extract(
                          exact_boundary_event.data_json, '$.boundary_id'
                        ) = boundary.id
              ) = 1
              AND (
                  SELECT COUNT(*) FROM events AS exact_replacement_event
                  WHERE exact_replacement_event.event_type = 'boundary.recorded'
                    AND exact_replacement_event.aggregate_type = 'work_item'
                    AND exact_replacement_event.aggregate_id = replacement.work_item_id
                    AND json_extract(
                          exact_replacement_event.data_json, '$.boundary_id'
                        ) = replacement.id
              ) = 1
          ) OR EXISTS (
            SELECT 1
            FROM boundaries AS boundary
            JOIN work_items AS work ON work.id = boundary.work_item_id
            JOIN events AS boundary_event
              ON boundary_event.sequence = supersession.boundary_event_sequence
             AND boundary_event.event_type = 'boundary.recorded'
             AND boundary_event.aggregate_type = 'work_item'
             AND boundary_event.aggregate_id = boundary.work_item_id
             AND json_extract(boundary_event.data_json, '$.boundary_id') = boundary.id
            JOIN events AS superseding_event
              ON superseding_event.sequence = supersession.superseding_event_sequence
             AND superseding_event.event_type = 'work.goal_replaced'
             AND superseding_event.aggregate_type = 'work_item'
             AND superseding_event.aggregate_id = boundary.work_item_id
             AND superseding_event.sequence > boundary_event.sequence
            JOIN goal_revisions AS replacement_goal
              ON replacement_goal.work_item_id = boundary.work_item_id
             AND replacement_goal.version = CAST(json_extract(
                   superseding_event.data_json, '$.version'
                 ) AS INTEGER)
             AND replacement_goal.source_directive_id = json_extract(
                   superseding_event.data_json, '$.directive_id'
                 )
            JOIN attempts AS successor_attempt
              ON successor_attempt.id = json_extract(
                   superseding_event.data_json, '$.attempt_id'
                 )
             AND successor_attempt.work_item_id = boundary.work_item_id
             AND successor_attempt.goal_version = replacement_goal.version
            LEFT JOIN boundary_dispositions AS disposition
              ON disposition.boundary_id = boundary.id
            WHERE boundary.id = supersession.boundary_id
              AND disposition.id IS NULL
              AND supersession.reason = 'goal_replaced'
              AND replacement_goal.version = boundary.goal_version + 1
              AND CAST(json_extract(
                    superseding_event.data_json, '$.generation'
                  ) AS INTEGER) = boundary.generation + 1
              AND work.goal_version >= replacement_goal.version
              AND work.generation >= CAST(json_extract(
                    superseding_event.data_json, '$.generation'
                  ) AS INTEGER)
              AND (
                  SELECT COUNT(*) FROM events AS exact_boundary_event
                  WHERE exact_boundary_event.event_type = 'boundary.recorded'
                    AND exact_boundary_event.aggregate_type = 'work_item'
                    AND exact_boundary_event.aggregate_id = boundary.work_item_id
                    AND json_extract(
                          exact_boundary_event.data_json, '$.boundary_id'
                        ) = boundary.id
              ) = 1
              AND (
                  SELECT COUNT(*) FROM events AS exact_replacement_event
                  WHERE exact_replacement_event.event_type = 'work.goal_replaced'
                    AND exact_replacement_event.aggregate_type = 'work_item'
                    AND exact_replacement_event.aggregate_id = boundary.work_item_id
                    AND CAST(json_extract(
                          exact_replacement_event.data_json, '$.version'
                        ) AS INTEGER) = replacement_goal.version
                    AND CAST(json_extract(
                          exact_replacement_event.data_json, '$.generation'
                        ) AS INTEGER) = boundary.generation + 1
                    AND json_extract(
                          exact_replacement_event.data_json, '$.attempt_id'
                        ) = successor_attempt.id
                    AND json_extract(
                          exact_replacement_event.data_json, '$.directive_id'
                        ) = replacement_goal.source_directive_id
              ) = 1
          )
        )
        """,
    )
    _add_rows(
        violations,
        connection,
        "boundary.more_than_one_open",
        "work_item",
        """
        SELECT b.work_item_id AS id FROM boundaries b
        LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
        LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
        WHERE d.id IS NULL AND s.boundary_id IS NULL
        GROUP BY b.work_item_id
        HAVING COUNT(*) > 1
        """,
    )
    _add_rows(
        violations,
        connection,
        "boundary.open_boundary_is_stale",
        "boundary",
        """
        SELECT b.id FROM boundaries b
        JOIN work_items w ON w.id = b.work_item_id
        LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
        LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
        WHERE d.id IS NULL
          AND s.boundary_id IS NULL
          AND (b.goal_version <> w.goal_version OR b.generation <> w.generation)
        """,
    )
    _add_rows(
        violations,
        connection,
        "boundary.open_boundary_on_terminal_work",
        "boundary",
        """
        SELECT b.id FROM boundaries b
        JOIN work_items w ON w.id = b.work_item_id
        LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
        LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
        WHERE d.id IS NULL
          AND s.boundary_id IS NULL
          AND w.state IN ('completed', 'canceled', 'failed')
        """,
    )
    _add_rows(
        violations,
        connection,
        "boundary.disposition_fence_mismatch",
        "boundary_disposition",
        """
        SELECT d.id FROM boundary_dispositions d
        JOIN boundaries b ON b.id = d.boundary_id
        JOIN reasoner_turns t ON t.id = d.reasoner_turn_id
        WHERE d.generation <> b.generation
           OR t.work_item_id <> b.work_item_id
           OR t.boundary_id <> b.id
           OR t.generation <> b.generation
        """,
    )
    _add_rows(
        violations,
        connection,
        "boundary.accepts_non_completion",
        "boundary_disposition",
        """
        SELECT d.id FROM boundary_dispositions d
        JOIN boundaries b ON b.id = d.boundary_id
        WHERE d.kind = 'accept' AND (
            b.kind NOT IN ('completion', 'worker_output')
            OR (b.kind = 'worker_output' AND NOT EXISTS (
                SELECT 1 FROM reviews r WHERE r.boundary_id = b.id AND r.verdict = 'ok'
            ))
        )
        """,
    )
    _add_rows(
        violations,
        connection,
        "reasoner.leased_turn_without_exact_open_boundary",
        "reasoner_turn",
        """
        SELECT t.id FROM reasoner_turns t
        LEFT JOIN boundaries b ON b.id = t.boundary_id
        LEFT JOIN boundary_dispositions d ON d.boundary_id = b.id
        LEFT JOIN boundary_supersessions s ON s.boundary_id = b.id
        WHERE t.state = 'leased'
          AND (
            b.id IS NULL OR d.id IS NOT NULL OR s.boundary_id IS NOT NULL
            OR b.work_item_id <> t.work_item_id
            OR b.goal_version <> t.goal_version OR b.generation <> t.generation
          )
        """,
    )
    _add_rows(
        violations,
        connection,
        "reasoner.leased_turn_is_stale",
        "reasoner_turn",
        """
        SELECT t.id FROM reasoner_turns t
        JOIN work_items w ON w.id = t.work_item_id
        WHERE t.state = 'leased'
          AND (t.generation <> w.generation OR w.state IN ('completed', 'canceled', 'failed', 'suspended'))
        """,
    )
    _add_rows(
        violations,
        connection,
        "reasoner.lease_expired",
        "reasoner_turn",
        "SELECT id FROM reasoner_turns WHERE state = 'leased' AND lease_expires_at < ?",
        (comparison_time,),
    )
    _delivery_violations(connection, violations, comparison_time)
    _runtime_violations(connection, violations, comparison_time)
    _add_rows(
        violations,
        connection,
        "work.completed_without_requester_acceptance",
        "work_item",
        """
        SELECT w.id FROM work_items w
        WHERE w.state = 'completed'
          AND NOT EXISTS (
              SELECT 1
              FROM requester_decisions d
              JOIN attempts a ON a.id = d.attempt_id AND a.work_item_id = w.id
              JOIN reviews r ON r.id = d.review_id
                             AND r.work_item_id = w.id
                             AND r.attempt_id = a.id
              JOIN cao_session_attachments attachment
                ON attachment.id = d.supervisor_attachment_id
              WHERE d.work_item_id = w.id
                AND d.verdict = 'accepted'
                AND w.requester_id = d.requester_id
                AND w.supervisor_id = d.recorded_by
                AND w.supervisor_attachment_id = d.supervisor_attachment_id
                AND w.generation = d.work_generation
                AND attachment.principal_id = d.recorded_by
                AND attachment.generation >= d.supervisor_attachment_generation
                AND r.reviewer_role = 'cao'
                AND r.verdict = 'ok'
                AND r.supervisor_attachment_id = d.supervisor_attachment_id
                AND r.supervisor_attachment_generation
                    <= d.supervisor_attachment_generation
                AND r.work_generation <= d.work_generation
                AND a.goal_version = d.goal_version
                AND a.goal_packet_digest = d.goal_packet_digest
                AND a.task_packet_digest = d.task_packet_digest
                AND r.goal_version = d.goal_version
                AND r.goal_packet_digest = d.goal_packet_digest
                AND r.task_packet_digest = d.task_packet_digest
          )
        """,
    )
    return violations


def _close_lifecycle_violations(
    connection: sqlite3.Connection,
    violations: list[ProjectionViolation],
    close_receipt_violations: tuple[ProjectionViolation, ...],
) -> None:
    """Verify the attachment-scoped decision/receipt chain independently.

    These checks intentionally duplicate the durable trigger predicates.  The
    projection must still fail closed for an imported database, a historic
    writer that lacked the trigger, or corruption discovered after commit.
    """

    _add_rows(
        violations,
        connection,
        "requester_decision.binding_invalid",
        "requester_decision",
        """
        SELECT d.id
        FROM requester_decisions d
        WHERE NOT EXISTS (
            SELECT 1
            FROM work_items w
            JOIN attempts a ON a.id = d.attempt_id AND a.work_item_id = w.id
            JOIN reviews r ON r.id = d.review_id
                           AND r.work_item_id = w.id
                           AND r.attempt_id = a.id
            JOIN cao_session_attachments attachment
              ON attachment.id = d.supervisor_attachment_id
            WHERE w.id = d.work_item_id
              AND w.requester_id = d.requester_id
              AND w.supervisor_id = d.recorded_by
              AND w.supervisor_attachment_id = d.supervisor_attachment_id
              AND w.generation = d.work_generation
              AND attachment.principal_id = d.recorded_by
              AND attachment.generation >= d.supervisor_attachment_generation
              AND r.reviewer_role = 'cao'
              AND r.verdict = 'ok'
              AND r.supervisor_attachment_id = d.supervisor_attachment_id
              AND r.supervisor_attachment_generation <= d.supervisor_attachment_generation
              AND r.work_generation <= d.work_generation
              AND a.goal_version = d.goal_version
              AND a.goal_packet_digest = d.goal_packet_digest
              AND a.task_packet_digest = d.task_packet_digest
              AND r.goal_version = d.goal_version
              AND r.goal_packet_digest = d.goal_packet_digest
              AND r.task_packet_digest = d.task_packet_digest
        )
        """,
    )
    _add_rows(
        violations,
        connection,
        "close_receipt.binding_invalid",
        "work_close_receipt",
        """
        SELECT c.id
        FROM work_close_receipts c
        WHERE c.cleanup_inventory_evidence_id = ''
           OR NOT EXISTS (
                SELECT 1
                FROM work_items w
                JOIN attempts a ON a.id = c.attempt_id AND a.work_item_id = w.id
                JOIN reviews r ON r.id = c.review_id
                               AND r.work_item_id = w.id
                               AND r.attempt_id = a.id
                JOIN requester_decisions d ON d.id = c.requester_decision_id
                                          AND d.work_item_id = w.id
                                          AND d.attempt_id = a.id
                                          AND d.review_id = r.id
                JOIN cao_session_attachments attachment
                  ON attachment.id = c.supervisor_attachment_id
                WHERE w.id = c.work_item_id
                  AND w.state = 'completed'
                  AND w.supervisor_id = c.closed_by
                  AND w.supervisor_attachment_id = c.supervisor_attachment_id
                  AND w.generation = c.work_generation
                  AND attachment.principal_id = c.closed_by
                  AND attachment.generation >= c.supervisor_attachment_generation
                  AND d.verdict = 'accepted'
                  AND d.recorded_by = c.closed_by
                  AND d.supervisor_attachment_id = c.supervisor_attachment_id
                  AND d.supervisor_attachment_generation <= c.supervisor_attachment_generation
                  AND d.work_generation = c.work_generation
                  AND a.goal_version = c.goal_version
                  AND a.goal_packet_digest = c.goal_packet_digest
                  AND a.task_packet_digest = c.task_packet_digest
                  AND r.goal_version = c.goal_version
                  AND r.goal_packet_digest = c.goal_packet_digest
                  AND r.task_packet_digest = c.task_packet_digest
                  AND d.goal_version = c.goal_version
                  AND d.goal_packet_digest = c.goal_packet_digest
                  AND d.task_packet_digest = c.task_packet_digest
           )
        """,
    )
    _add_rows(
        violations,
        connection,
        "close_receipt.duplicate_work_item",
        "work_item",
        """
        SELECT work_item_id AS id
        FROM work_close_receipts
        GROUP BY work_item_id
        HAVING COUNT(*) > 1
        """,
    )
    _add_rows(
        violations,
        connection,
        "close_receipt.duplicate_requester_decision",
        "requester_decision",
        """
        SELECT requester_decision_id AS id
        FROM work_close_receipts
        GROUP BY requester_decision_id
        HAVING COUNT(*) > 1
        """,
    )
    violations.extend(close_receipt_violations)


def _supervisor_attachment(
    connection: sqlite3.Connection, work_item_id: str, goal_version: int
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT a.id, a.principal_id, a.native_thread_id,
               a.project_digest, g.supervisor_attachment_generation,
               g.supervisor_runtime_session_id
        FROM work_items AS w
        JOIN cao_session_attachments AS a ON a.id = w.supervisor_attachment_id
        JOIN goal_revisions AS g
          ON g.work_item_id = w.id AND g.version = ?
        JOIN runtime_sessions AS supervisor_runtime
          ON supervisor_runtime.id = g.supervisor_runtime_session_id
         AND supervisor_runtime.principal_id = a.principal_id
        WHERE w.id = ?
          AND g.supervisor_attachment_generation IS NOT NULL
          AND g.supervisor_runtime_session_id IS NOT NULL
        """,
        (goal_version, work_item_id),
    ).fetchone()
    if row is None:
        return None
    return {
        "attachment_id": str(row["id"]),
        "supervisor_id": str(row["principal_id"]),
        "runtime_session_id": str(row["supervisor_runtime_session_id"]),
        "native_thread_id": str(row["native_thread_id"]),
        "project_digest": str(row["project_digest"]),
        "generation": int(row["supervisor_attachment_generation"]),
    }


def _packet_violations(
    connection: sqlite3.Connection, violations: list[ProjectionViolation]
) -> None:
    """Verify the immutable Goal/Task packet chain against canonical rows."""

    goal_rows = connection.execute(
        """
        SELECT work_item_id, version, title, objective, maturity, acceptance_json,
               non_goals_json, priority, requester_id, supervisor_id, metadata_json,
               packet_json, packet_digest, reason, created_by, source_intent_id,
               source_directive_id, correlation_id, prior_version,
               supervisor_attachment_generation
        FROM goal_revisions
        """
    ).fetchall()
    goal_digests: dict[tuple[str, int], str] = {}
    goal_dependencies: dict[tuple[str, int], list[str]] = {}
    managed_task_policies: dict[tuple[str, int], dict[str, Any]] = {}
    for row in goal_rows:
        identity = (str(row["work_item_id"]), int(row["version"]))
        packet = _json_object(str(row["packet_json"]))
        row_digest = str(row["packet_digest"])
        dependencies: list[str] | None = None
        invalid = packet is None
        if packet is not None:
            completion_contract = str(packet.get("completion_contract", "legacy_unclassified"))
            completion_contract_valid = completion_contract in {
                "legacy_unclassified",
                "completion_required",
                "no_artifact_expected",
            }
            dependencies_value = packet.get("dependencies", [])
            dependencies = (
                list(dependencies_value)
                if dependencies_value in ([], ["docker_api_ping"])
                else None
            )
            acceptance = _json_list(str(row["acceptance_json"]))
            non_goals = _json_list(str(row["non_goals_json"]))
            metadata = _json_object(str(row["metadata_json"]))
            managed_task_policy_value = packet.get("managed_task_policy")
            managed_task_policy = (
                dict(managed_task_policy_value)
                if isinstance(managed_task_policy_value, Mapping)
                else None
            )
            managed_task_policy_valid = managed_task_policy_value is None
            if managed_task_policy is not None:
                task_policy = managed_task_policy.get("task_policy")
                task_policy_valid = task_policy is None or (
                    isinstance(task_policy, Mapping)
                    and set(task_policy)
                    == {
                        "scope",
                        "external_writes",
                        "allowed_without_approval",
                        "instruction_language",
                        "do_not_generalize",
                    }
                    and all(
                        isinstance(task_policy.get(field), str)
                        for field in (
                            "scope",
                            "external_writes",
                            "allowed_without_approval",
                            "instruction_language",
                        )
                    )
                    and isinstance(task_policy.get("do_not_generalize"), bool)
                )
                policy_digest = _digest(task_policy)
                managed_task_policy_valid = (
                    set(managed_task_policy)
                    == {
                        "format",
                        "catalog_target_id",
                        "task_policy",
                        "task_policy_digest",
                    }
                    and managed_task_policy.get("format")
                    == "cao-managed-worker-task-policy-binding/v1"
                    and isinstance(managed_task_policy.get("catalog_target_id"), str)
                    and bool(managed_task_policy.get("catalog_target_id"))
                    and task_policy_valid
                    and managed_task_policy.get("task_policy_digest") == f"sha256:{policy_digest}"
                )
            try:
                expected_packet = build_goal_packet(
                    work_item_id=identity[0],
                    version=identity[1],
                    title=str(row["title"]),
                    objective=str(row["objective"]),
                    maturity=str(row["maturity"]),
                    acceptance=acceptance or [],
                    non_goals=non_goals or [],
                    priority=int(row["priority"]),
                    requester_id=(
                        str(row["requester_id"]) if row["requester_id"] is not None else None
                    ),
                    supervisor_id=(
                        str(row["supervisor_id"]) if row["supervisor_id"] is not None else None
                    ),
                    metadata=metadata or {},
                    reason=str(row["reason"]),
                    created_by=str(row["created_by"]),
                    source_intent_id=(
                        str(row["source_intent_id"])
                        if row["source_intent_id"] is not None
                        else None
                    ),
                    source_directive_id=(
                        str(row["source_directive_id"])
                        if row["source_directive_id"] is not None
                        else None
                    ),
                    correlation_id=str(row["correlation_id"]),
                    prior_version=(identity[1] - 1 if identity[1] > 1 else None),
                    completion_contract=completion_contract,
                    supervisor_attachment=_supervisor_attachment(
                        connection, identity[0], identity[1]
                    ),
                    dependencies=dependencies or [],
                    managed_task_policy=managed_task_policy,
                )
                computed = goal_packet_digest(expected_packet)
            except (TypeError, ValueError):
                expected_packet = {}
                computed = ""
            invalid = (
                acceptance is None
                or non_goals is None
                or metadata is None
                or dependencies is None
                or not completion_contract_valid
                or not managed_task_policy_valid
                or row["prior_version"] != (identity[1] - 1 if identity[1] > 1 else None)
                or packet != expected_packet
                or computed != row_digest
            )
        if invalid:
            violations.append(
                ProjectionViolation(
                    "goal.packet_invalid", "goal_revision", (f"{identity[0]}:{identity[1]}",)
                )
            )
        else:
            goal_digests[identity] = row_digest
            goal_dependencies[identity] = dependencies or []
            if managed_task_policy is not None:
                managed_task_policies[identity] = managed_task_policy

    for identity, policy in managed_task_policies.items():
        row = connection.execute(
            """
            SELECT goal.source_intent_id, intent.content_json,
                   prior.packet_json AS prior_packet_json
            FROM goal_revisions AS goal
            LEFT JOIN submitted_intents AS intent
              ON intent.id = goal.source_intent_id
            LEFT JOIN goal_revisions AS prior
              ON prior.work_item_id = goal.work_item_id
             AND prior.version = goal.version - 1
            WHERE goal.work_item_id = ? AND goal.version = ?
            """,
            identity,
        ).fetchone()
        valid_policy_provenance = row is not None
        if row is not None and identity[1] == 1:
            content = _json_object(str(row["content_json"]))
            valid_policy_provenance = (
                content is not None
                and isinstance(content.get("goal"), Mapping)
                and content["goal"].get("managed_task_policy") == policy
            )
        elif row is not None:
            prior_packet = _json_object(str(row["prior_packet_json"]))
            valid_policy_provenance = (
                prior_packet is not None and prior_packet.get("managed_task_policy") == policy
            )
        assignment_rows = connection.execute(
            """
            SELECT payload_json FROM messages
            WHERE work_item_id = ? AND goal_version = ? AND kind = 'assignment'
            """,
            identity,
        ).fetchall()
        valid_assignment_policy = bool(assignment_rows) and all(
            (payload := _json_object(str(assignment["payload_json"]))) is not None
            and payload.get("managed_task_policy") == policy
            for assignment in assignment_rows
        )
        if not valid_policy_provenance or not valid_assignment_policy:
            violations.append(
                ProjectionViolation(
                    "goal.managed_task_policy_invalid",
                    "goal_revision",
                    (f"{identity[0]}:{identity[1]}",),
                )
            )

    current_goal_rows = connection.execute(
        """
        SELECT w.id, w.title, w.priority, w.requester_id, w.supervisor_id,
               w.metadata_json, g.title AS goal_title, g.priority AS goal_priority,
               g.requester_id AS goal_requester_id,
               g.supervisor_id AS goal_supervisor_id,
               g.metadata_json AS goal_metadata_json
        FROM work_items w
        JOIN goal_revisions g
          ON g.work_item_id = w.id AND g.version = w.goal_version
        """
    ).fetchall()
    for row in current_goal_rows:
        if (
            str(row["title"]) != str(row["goal_title"])
            or int(row["priority"]) != int(row["goal_priority"])
            or row["requester_id"] != row["goal_requester_id"]
            or row["supervisor_id"] != row["goal_supervisor_id"]
            or _json_object(str(row["metadata_json"]))
            != _json_object(str(row["goal_metadata_json"]))
        ):
            violations.append(
                ProjectionViolation(
                    "work.current_goal_semantics_mismatch",
                    "work_item",
                    (str(row["id"]),),
                )
            )

    attempt_rows = connection.execute(
        """
        SELECT id, work_item_id, attempt_number, worker_id, runtime_session_id,
               goal_version, goal_packet_digest, task_packet_digest,
               completion_claim_json, state
        FROM attempts
        """
    ).fetchall()
    attempt_packets: dict[str, tuple[int, str, str]] = {}
    for row in attempt_rows:
        attempt_id = str(row["id"])
        goal_version = int(row["goal_version"])
        goal_digest = str(row["goal_packet_digest"])
        task_digest = str(row["task_packet_digest"])
        task_packet = build_task_packet(
            goal_packet_digest_value=goal_digest,
            work_item_id=str(row["work_item_id"]),
            goal_version=goal_version,
            attempt_id=attempt_id,
            attempt_number=int(row["attempt_number"]),
            worker_id=str(row["worker_id"]),
            runtime_session_id=(
                str(row["runtime_session_id"]) if row["runtime_session_id"] is not None else None
            ),
            supervisor_attachment=_supervisor_attachment(
                connection, str(row["work_item_id"]), goal_version
            ),
            dependencies=goal_dependencies.get((str(row["work_item_id"]), goal_version), []),
        )
        invalid = (
            goal_digests.get((str(row["work_item_id"]), goal_version)) != goal_digest
            or task_packet_digest(task_packet) != task_digest
        )
        claim = _json_object(str(row["completion_claim_json"]))
        if claim:
            invalid = invalid or (
                claim.get("goal_packet_digest") != goal_digest
                or claim.get("task_packet_digest") != task_digest
            )
        elif str(row["state"]) == "submitted":
            invalid = True
        if invalid:
            violations.append(
                ProjectionViolation("attempt.packet_invalid", "attempt", (attempt_id,))
            )
        else:
            attempt_packets[attempt_id] = (goal_version, goal_digest, task_digest)

    for table, subject_type, code in (
        ("boundaries", "boundary", "boundary.packet_mismatch"),
        ("reviews", "review", "review.packet_mismatch"),
    ):
        rows = connection.execute(
            f"SELECT id, attempt_id, goal_version, "
            f"goal_packet_digest, task_packet_digest FROM {table}"
        ).fetchall()
        for row in rows:
            expected = attempt_packets.get(str(row["attempt_id"]))
            actual = (
                int(row["goal_version"]),
                str(row["goal_packet_digest"]),
                str(row["task_packet_digest"]),
            )
            if expected != actual:
                violations.append(ProjectionViolation(code, subject_type, (str(row["id"]),)))

    reasoner_packet_rows = connection.execute(
        """
        SELECT t.id, b.attempt_id, t.goal_version,
               t.goal_packet_digest, t.task_packet_digest
        FROM reasoner_turns t
        LEFT JOIN boundaries b ON b.id = t.boundary_id
        """
    ).fetchall()
    for row in reasoner_packet_rows:
        reasoner_expected = (
            attempt_packets.get(str(row["attempt_id"])) if row["attempt_id"] is not None else None
        )
        reasoner_actual = (
            int(row["goal_version"]),
            str(row["goal_packet_digest"]),
            str(row["task_packet_digest"]),
        )
        if reasoner_expected != reasoner_actual:
            violations.append(
                ProjectionViolation("reasoner.packet_mismatch", "reasoner_turn", (str(row["id"]),))
            )

    directive_rows = connection.execute(
        """
        SELECT id, target_work_item_id, created_work_item_id, expected_goal_version,
               expected_goal_packet_digest
        FROM directives
        """
    ).fetchall()
    for row in directive_rows:
        version = row["expected_goal_version"]
        work_item_id = row["target_work_item_id"] or row["created_work_item_id"]
        directive_expected_digest = (
            goal_digests.get((str(work_item_id), int(version)))
            if work_item_id is not None and version is not None
            else ""
        )
        if str(row["expected_goal_packet_digest"]) != directive_expected_digest:
            violations.append(
                ProjectionViolation(
                    "directive.goal_packet_mismatch", "directive", (str(row["id"]),)
                )
            )

    continuation_rows = connection.execute(
        """
        SELECT directive.id, directive.target_work_item_id,
               directive.created_work_item_id, directive.submitted_intent_id,
               directive.source_receipt_id, directive.expected_goal_version,
               directive.expected_goal_packet_digest, directive.issuer_id,
               directive.state AS directive_state,
               directive.content AS directive_content,
               directive.reason AS directive_reason,
               directive.handled_by AS directive_handled_by,
               prior.supervisor_id AS prior_supervisor_id,
               prior.requester_id AS prior_requester_id,
               prior.supervisor_attachment_id AS prior_attachment_id,
               prior.state AS prior_state,
               successor.supervisor_id AS successor_supervisor_id,
               successor.requester_id AS successor_requester_id,
               successor.supervisor_attachment_id AS successor_attachment_id,
               successor_attachment.generation AS successor_attachment_generation,
               prior_attachment.project_digest AS prior_project_digest,
               prior_attachment.project_scope_digest AS prior_project_scope,
               successor_attachment.project_digest AS successor_project_digest,
               successor_attachment.project_scope_digest AS successor_project_scope,
               prior_goal.packet_json AS prior_packet_json,
               successor_goal.packet_json AS successor_packet_json,
               successor_goal.source_intent_id AS successor_source_intent_id,
               successor_goal.correlation_id AS successor_correlation_id,
               prior_attempt.id AS prior_attempt_id,
               prior_attempt.runtime_session_id AS prior_attempt_runtime_id,
               prior_attempt.state AS prior_attempt_state,
               prior_attempt.evidence_confidence AS prior_evidence_confidence,
               successor_attempt.worker_id AS successor_worker_id,
               successor_attempt.runtime_session_id AS successor_runtime_id,
               successor_attempt.task_packet_digest AS successor_task_packet_digest,
               prior_spec.id AS prior_spec_id,
               prior_spec.catalog_target_id AS prior_target_id,
               prior_spec.workspace_ref AS prior_workspace_ref,
               prior_spec.provider_scope_digest AS prior_provider_scope,
               prior_spec.attachment_generation AS prior_spec_generation,
               prior_spec.state AS prior_spec_state,
               prior_runtime.state AS prior_runtime_state,
               prior_enrollment.state AS prior_enrollment_state,
               successor_spec.id AS successor_spec_id,
               successor_spec.catalog_target_id AS successor_target_id,
               successor_spec.workspace_ref AS successor_workspace_ref,
               successor_spec.provider_scope_digest AS successor_provider_scope,
               successor_spec.attachment_generation AS successor_spec_generation,
               close_event.sequence AS close_sequence,
               close_event.data_json AS close_event_data_json,
               CAST(json_extract(
                   close_event.data_json, '$.closed_generation'
               ) AS INTEGER) AS close_generation,
               close_result.idempotency_key AS close_idempotency_key,
               close_result.request_digest AS close_request_digest,
               close_result.result_json AS close_result_json,
               (
                   SELECT COUNT(*) FROM events AS counted_cancel
                   JOIN work_items AS counted_work
                     ON counted_work.id = counted_cancel.aggregate_id
                   JOIN goal_revisions AS counted_goal
                     ON counted_goal.work_item_id = counted_work.id
                    AND counted_goal.version = counted_work.goal_version
                   WHERE counted_cancel.event_type = 'work.canceled'
                     AND counted_cancel.aggregate_type = 'work_item'
                     AND counted_cancel.actor_id = prior.supervisor_id
                     AND counted_cancel.sequence < close_event.sequence
                     AND json_extract(
                           counted_cancel.data_json, '$.reason'
                         ) = 'cao_conversation_closed'
                     AND json_extract(
                           counted_cancel.data_json, '$.close_request_digest'
                         ) = close_result.request_digest
                     AND json_extract(
                           counted_cancel.data_json, '$.closed_generation'
                         ) = json_extract(
                               close_event.data_json, '$.closed_generation'
                             )
                     AND counted_work.supervisor_attachment_id =
                         prior_attachment.id
                     AND counted_goal.supervisor_attachment_generation <=
                         CAST(json_extract(
                             close_event.data_json, '$.closed_generation'
                         ) AS INTEGER)
               ) AS close_canceled_event_count,
               (
                   SELECT COUNT(*) FROM events AS counted_stop
                   JOIN managed_worker_specs AS counted_spec
                     ON counted_spec.id = counted_stop.aggregate_id
                   WHERE counted_stop.event_type = 'managed_worker.stopped'
                     AND counted_stop.aggregate_type = 'managed_worker_spec'
                     AND counted_stop.actor_id = prior.supervisor_id
                     AND counted_stop.sequence < close_event.sequence
                     AND json_extract(
                           counted_stop.data_json, '$.reason_code'
                         ) = 'cao_conversation_closed'
                     AND json_extract(
                           counted_stop.data_json, '$.close_request_digest'
                         ) = close_result.request_digest
                     AND json_extract(
                           counted_stop.data_json, '$.closed_generation'
                         ) = json_extract(
                               close_event.data_json, '$.closed_generation'
                             )
                     AND counted_spec.attachment_id = prior_attachment.id
                     AND counted_spec.attachment_generation <=
                         CAST(json_extract(
                             close_event.data_json, '$.closed_generation'
                         ) AS INTEGER)
               ) AS close_stopped_event_count,
               (
                   SELECT COUNT(*) FROM events AS exact_prior_stop
                   WHERE exact_prior_stop.event_type = 'managed_worker.stopped'
                     AND exact_prior_stop.aggregate_type = 'managed_worker_spec'
                     AND exact_prior_stop.aggregate_id = prior_spec.id
                     AND exact_prior_stop.actor_id = prior.supervisor_id
                     AND exact_prior_stop.sequence < close_event.sequence
                     AND json_extract(
                           exact_prior_stop.data_json, '$.reason_code'
                         ) = 'cao_conversation_closed'
                     AND json_extract(
                           exact_prior_stop.data_json, '$.close_request_digest'
                         ) = close_result.request_digest
                     AND json_extract(
                           exact_prior_stop.data_json, '$.closed_generation'
                         ) = json_extract(
                               close_event.data_json, '$.closed_generation'
                             )
                     AND EXISTS (
                         SELECT 1 FROM events AS prior_cancel_before_stop
                         WHERE prior_cancel_before_stop.event_type = 'work.canceled'
                           AND prior_cancel_before_stop.aggregate_type = 'work_item'
                           AND prior_cancel_before_stop.aggregate_id = prior.id
                           AND prior_cancel_before_stop.actor_id = prior.supervisor_id
                           AND prior_cancel_before_stop.sequence <
                               exact_prior_stop.sequence
                           AND json_extract(
                                 prior_cancel_before_stop.data_json, '$.reason'
                               ) = 'cao_conversation_closed'
                           AND json_extract(
                                 prior_cancel_before_stop.data_json,
                                 '$.close_request_digest'
                               ) = close_result.request_digest
                           AND json_extract(
                                 prior_cancel_before_stop.data_json,
                                 '$.closed_generation'
                               ) = json_extract(
                                     close_event.data_json,
                                     '$.closed_generation'
                                   )
                     )
               ) AS prior_spec_stop_proof_count,
               NOT EXISTS (
                   SELECT duplicate_cancel.aggregate_id
                   FROM events AS duplicate_cancel
                   JOIN work_items AS duplicate_work
                     ON duplicate_work.id = duplicate_cancel.aggregate_id
                   JOIN goal_revisions AS duplicate_goal
                     ON duplicate_goal.work_item_id = duplicate_work.id
                    AND duplicate_goal.version = duplicate_work.goal_version
                   WHERE duplicate_cancel.event_type = 'work.canceled'
                     AND duplicate_cancel.aggregate_type = 'work_item'
                     AND duplicate_cancel.actor_id = prior.supervisor_id
                     AND duplicate_cancel.sequence < close_event.sequence
                     AND json_extract(
                           duplicate_cancel.data_json, '$.reason'
                         ) = 'cao_conversation_closed'
                     AND json_extract(
                           duplicate_cancel.data_json, '$.close_request_digest'
                         ) = close_result.request_digest
                     AND json_extract(
                           duplicate_cancel.data_json, '$.closed_generation'
                         ) = json_extract(
                               close_event.data_json, '$.closed_generation'
                             )
                     AND duplicate_work.supervisor_attachment_id =
                         prior_attachment.id
                     AND duplicate_goal.supervisor_attachment_generation <=
                         CAST(json_extract(
                             close_event.data_json, '$.closed_generation'
                         ) AS INTEGER)
                   GROUP BY duplicate_cancel.aggregate_id
                   HAVING COUNT(*) <> 1
               ) AS close_cancel_events_unique,
               NOT EXISTS (
                   SELECT duplicate_stop.aggregate_id
                   FROM events AS duplicate_stop
                   JOIN managed_worker_specs AS duplicate_spec
                     ON duplicate_spec.id = duplicate_stop.aggregate_id
                   WHERE duplicate_stop.event_type = 'managed_worker.stopped'
                     AND duplicate_stop.aggregate_type = 'managed_worker_spec'
                     AND duplicate_stop.actor_id = prior.supervisor_id
                     AND duplicate_stop.sequence < close_event.sequence
                     AND json_extract(
                           duplicate_stop.data_json, '$.reason_code'
                         ) = 'cao_conversation_closed'
                     AND json_extract(
                           duplicate_stop.data_json, '$.close_request_digest'
                         ) = close_result.request_digest
                     AND json_extract(
                           duplicate_stop.data_json, '$.closed_generation'
                         ) = json_extract(
                               close_event.data_json, '$.closed_generation'
                             )
                     AND duplicate_spec.attachment_id = prior_attachment.id
                     AND duplicate_spec.attachment_generation <=
                         CAST(json_extract(
                             close_event.data_json, '$.closed_generation'
                         ) AS INTEGER)
                   GROUP BY duplicate_stop.aggregate_id
                   HAVING COUNT(*) <> 1
               ) AS close_stop_events_unique,
               receipt.source_principal_id AS receipt_principal_id,
               receipt.source_id, receipt.payload_json AS receipt_payload_json,
               receipt.payload_digest AS receipt_payload_digest,
               intent.submitter_id AS intent_submitter_id,
               intent.content_json AS intent_content_json,
               intent.content_digest AS intent_content_digest,
               intent.correlation_id AS intent_correlation_id,
               disposition.id AS disposition_id,
               disposition.decided_by AS disposition_decided_by,
               disposition.kind AS disposition_kind,
               disposition.relation AS disposition_relation,
               disposition.target_work_item_id AS disposition_target_id,
               disposition.result_work_item_id AS disposition_result_id,
               disposition.result_directive_id AS disposition_directive_id,
               disposition.reason AS disposition_reason,
               disposition.request_digest AS disposition_request_digest,
               disposition.result_json AS disposition_result_json,
               (
                   SELECT COUNT(*) FROM reviews AS accepted_review
                   JOIN requester_decisions AS accepted_decision
                     ON accepted_decision.review_id = accepted_review.id
                    AND accepted_decision.work_item_id = accepted_review.work_item_id
                    AND accepted_decision.attempt_id = accepted_review.attempt_id
                    AND accepted_decision.goal_version = accepted_review.goal_version
                    AND accepted_decision.goal_packet_digest =
                        accepted_review.goal_packet_digest
                    AND accepted_decision.task_packet_digest =
                        accepted_review.task_packet_digest
                    AND accepted_decision.verdict = 'accepted'
                    AND accepted_decision.conversation_evidence_id <> ''
                   WHERE accepted_review.work_item_id = prior.id
                     AND accepted_review.attempt_id = prior_attempt.id
                     AND accepted_review.reviewer_id = prior.supervisor_id
                     AND accepted_review.reviewer_role = 'cao'
                     AND accepted_review.verdict = 'ok'
                     AND accepted_review.goal_version =
                         directive.expected_goal_version
                     AND accepted_review.goal_packet_digest =
                         directive.expected_goal_packet_digest
                     AND accepted_review.task_packet_digest =
                         prior_attempt.task_packet_digest
               ) AS accepted_proof_count,
               (
                   SELECT COUNT(*)
                   FROM boundaries AS recovery_boundary
                   JOIN boundary_dispositions AS recovery_disposition
                     ON recovery_disposition.boundary_id = recovery_boundary.id
                    AND recovery_disposition.decided_by = prior.supervisor_id
                    AND recovery_disposition.generation = recovery_boundary.generation
                    AND recovery_disposition.kind = 'cancel'
                    AND recovery_disposition.reason =
                        'Continued by an explicit system-recovery task instruction.'
                   JOIN reasoner_turns AS recovery_turn
                     ON recovery_turn.id = recovery_disposition.reasoner_turn_id
                    AND recovery_turn.supervisor_id = prior.supervisor_id
                    AND recovery_turn.work_item_id = prior.id
                    AND recovery_turn.boundary_id = recovery_boundary.id
                    AND recovery_turn.generation = recovery_boundary.generation
                    AND recovery_turn.goal_version = recovery_boundary.goal_version
                    AND recovery_turn.goal_packet_digest =
                        recovery_boundary.goal_packet_digest
                    AND recovery_turn.task_packet_digest =
                        recovery_boundary.task_packet_digest
                    AND recovery_turn.state = 'completed'
                    AND recovery_turn.completed_at IS NOT NULL
                   WHERE recovery_boundary.work_item_id = prior.id
                     AND recovery_boundary.attempt_id = prior_attempt.id
                     AND recovery_boundary.source_principal_id = prior_attempt.worker_id
                     AND recovery_boundary.kind = 'failure'
                     AND recovery_boundary.generation + 1 = prior.generation
                     AND recovery_boundary.goal_version =
                         directive.expected_goal_version
                     AND recovery_boundary.goal_packet_digest =
                         directive.expected_goal_packet_digest
                     AND recovery_boundary.task_packet_digest =
                         prior_attempt.task_packet_digest
                     AND json_extract(
                           recovery_boundary.metadata_json, '$.system_recovery'
                         ) = 1
                     AND json_extract(
                           recovery_boundary.metadata_json, '$.reason'
                         ) = 'incomplete_user_needed_contract'
                     AND json_extract(
                           recovery_boundary.metadata_json, '$.next_action'
                         ) = 'cao_continue_prior'
               ) AS system_recovery_proof_count,
               (
                   SELECT COUNT(*) FROM events AS exact_close
                   WHERE exact_close.event_type = 'cao.conversation_closed'
                     AND exact_close.aggregate_type = 'cao_session_attachment'
                     AND exact_close.aggregate_id = prior_attachment.id
                     AND exact_close.actor_id = prior.supervisor_id
                     AND json_extract(
                           exact_close.data_json, '$.closed_generation'
                         ) = json_extract(
                               close_event.data_json, '$.closed_generation'
                             )
                     AND json_extract(
                           exact_close.data_json, '$.idempotency_key_digest'
                         ) <> ''
                     AND EXISTS (
                         SELECT 1 FROM events AS exact_cancel
                         WHERE exact_cancel.event_type = 'work.canceled'
                           AND exact_cancel.aggregate_type = 'work_item'
                           AND exact_cancel.aggregate_id = prior.id
                           AND exact_cancel.actor_id = prior.supervisor_id
                           AND exact_cancel.sequence < exact_close.sequence
                           AND json_extract(
                                 exact_cancel.data_json, '$.reason'
                               ) = 'cao_conversation_closed'
                           AND json_extract(
                                 exact_cancel.data_json,
                                 '$.close_request_digest'
                               ) = close_result.request_digest
                           AND json_extract(
                                 exact_cancel.data_json, '$.closed_generation'
                               ) = json_extract(
                                     close_event.data_json,
                                     '$.closed_generation'
                                   )
                     )
               ) AS close_proof_count
        FROM directives AS directive
        LEFT JOIN work_items AS prior
          ON prior.id = directive.target_work_item_id
        LEFT JOIN goal_revisions AS prior_goal
          ON prior_goal.work_item_id = prior.id
         AND prior_goal.version = directive.expected_goal_version
         AND prior_goal.packet_digest = directive.expected_goal_packet_digest
        LEFT JOIN work_items AS successor
          ON successor.id = directive.created_work_item_id
        LEFT JOIN goal_revisions AS successor_goal
          ON successor_goal.work_item_id = successor.id
         AND successor_goal.version = 1
        LEFT JOIN cao_session_attachments AS prior_attachment
          ON prior_attachment.id = prior.supervisor_attachment_id
        LEFT JOIN cao_session_attachments AS successor_attachment
          ON successor_attachment.id = successor.supervisor_attachment_id
        LEFT JOIN attempts AS prior_attempt
          ON prior_attempt.id = (
              SELECT latest_prior.id FROM attempts AS latest_prior
              WHERE latest_prior.work_item_id = prior.id
              ORDER BY latest_prior.attempt_number DESC LIMIT 1
          )
        LEFT JOIN attempts AS successor_attempt
          ON successor_attempt.work_item_id = successor.id
         AND successor_attempt.attempt_number = 1
        LEFT JOIN managed_worker_thread_epochs AS prior_epoch
          ON prior_epoch.runtime_session_id = prior_attempt.runtime_session_id
        LEFT JOIN managed_worker_threads AS prior_thread
          ON prior_thread.id = prior_epoch.thread_id
        LEFT JOIN managed_worker_specs AS prior_spec
          ON prior_spec.id = prior_thread.managed_spec_id
         AND prior_spec.principal_id = prior_attempt.worker_id
         AND prior_spec.attachment_id = prior.supervisor_attachment_id
        LEFT JOIN managed_worker_specs AS successor_spec
          ON successor_spec.principal_id = successor_attempt.worker_id
         AND successor_spec.runtime_session_id = successor_attempt.runtime_session_id
         AND EXISTS (
             SELECT 1 FROM cao_session_attachments AS successor_spec_attachment
             WHERE successor_spec_attachment.id = successor_spec.attachment_id
               AND successor_spec_attachment.principal_id = successor.supervisor_id
               AND successor_spec_attachment.project_scope_digest =
                   successor_attachment.project_scope_digest
         )
        LEFT JOIN runtime_sessions AS prior_runtime
          ON prior_runtime.id = prior_spec.runtime_session_id
        LEFT JOIN worker_enrollments AS prior_enrollment
          ON prior_enrollment.id = prior_spec.enrollment_id
        LEFT JOIN events AS close_event
          ON close_event.event_type = 'cao.conversation_closed'
         AND close_event.aggregate_type = 'cao_session_attachment'
         AND close_event.aggregate_id = prior_attachment.id
         AND close_event.actor_id = prior.supervisor_id
         AND CAST(json_extract(
               close_event.data_json, '$.closed_generation'
             ) AS INTEGER) >= prior_spec.attachment_generation
         AND EXISTS (
             SELECT 1
             FROM events AS binding_cancel
             JOIN idempotency_results AS binding_result
               ON binding_result.actor_id = prior.supervisor_id
              AND binding_result.operation =
                  'close_cao_conversation:' || prior_attachment.id || ':' ||
                  CAST(json_extract(
                      close_event.data_json, '$.closed_generation'
                  ) AS INTEGER)
              AND binding_result.request_digest = json_extract(
                    binding_cancel.data_json, '$.close_request_digest'
                  )
             WHERE binding_cancel.event_type = 'work.canceled'
               AND binding_cancel.aggregate_type = 'work_item'
               AND binding_cancel.aggregate_id = prior.id
               AND binding_cancel.actor_id = prior.supervisor_id
               AND binding_cancel.sequence < close_event.sequence
               AND json_extract(binding_cancel.data_json, '$.reason') =
                   'cao_conversation_closed'
               AND json_extract(
                     binding_cancel.data_json, '$.closed_generation'
                   ) = json_extract(
                         close_event.data_json, '$.closed_generation'
                       )
         )
        LEFT JOIN idempotency_results AS close_result
          ON close_result.actor_id = prior.supervisor_id
         AND close_result.operation =
             'close_cao_conversation:' || prior_attachment.id || ':' ||
             CAST(json_extract(
                 close_event.data_json, '$.closed_generation'
             ) AS INTEGER)
        LEFT JOIN source_receipts AS receipt
          ON receipt.id = directive.source_receipt_id
        LEFT JOIN submitted_intents AS intent
          ON intent.id = directive.submitted_intent_id
         AND intent.source_receipt_id = receipt.id
        LEFT JOIN intent_dispositions AS disposition
          ON disposition.submitted_intent_id = directive.submitted_intent_id
         AND disposition.source_receipt_id = directive.source_receipt_id
        WHERE directive.relation = 'continue'
        """
    ).fetchall()
    provider_continuation_rows = connection.execute(
        """
        SELECT directive.id AS directive_id,
               circuit.source_boundary_id, circuit.source_boundary_sequence,
               boundary.input_digest AS boundary_input_digest,
               boundary.goal_packet_digest AS boundary_goal_packet_digest,
               boundary.task_packet_digest AS boundary_task_packet_digest,
               boundary.generation AS boundary_generation,
               disposition.id AS disposition_id,
               disposition.request_digest AS disposition_request_digest,
               turn.id AS turn_id, turn.generation AS turn_generation,
               turn.input_digest AS turn_input_digest,
               turn.result_digest AS turn_result_digest,
               turn.lease_token_digest AS turn_lease_token_digest,
               turn.idempotency_key AS turn_idempotency_key,
               boundary_event.sequence AS boundary_event_sequence,
               disposed_event.sequence AS disposed_event_sequence,
               canceled_event.sequence AS canceled_event_sequence,
               retired_event.sequence AS retired_event_sequence,
               instruction_event.sequence AS instruction_event_sequence,
               prior.id AS prior_work_item_id,
               prior_goal.packet_digest AS prior_goal_packet_digest
        FROM directives AS directive
        JOIN work_items AS prior ON prior.id = directive.target_work_item_id
        JOIN goal_revisions AS prior_goal
          ON prior_goal.work_item_id = prior.id
         AND prior_goal.version = directive.expected_goal_version
         AND prior_goal.packet_digest = directive.expected_goal_packet_digest
        JOIN attempts AS prior_attempt
          ON prior_attempt.id = (
              SELECT latest_prior.id FROM attempts AS latest_prior
              WHERE latest_prior.work_item_id = prior.id
              ORDER BY latest_prior.attempt_number DESC LIMIT 1
          )
        JOIN managed_worker_thread_epochs AS prior_epoch
          ON prior_epoch.runtime_session_id = prior_attempt.runtime_session_id
        JOIN managed_worker_threads AS prior_thread
          ON prior_thread.id = prior_epoch.thread_id
        JOIN managed_worker_specs AS prior_spec
          ON prior_spec.id = prior_thread.managed_spec_id
         AND prior_spec.principal_id = prior_attempt.worker_id
         AND prior_spec.attachment_id = prior.supervisor_attachment_id
        JOIN provider_runtime_circuits AS circuit
          ON circuit.scope_digest = prior_spec.provider_scope_digest
         AND circuit.failure_code = 'runtime_provider_rate_limited'
         AND circuit.source_runtime_session_id = prior_attempt.runtime_session_id
         AND circuit.source_work_item_id = prior.id
         AND circuit.source_boundary_id <> ''
         AND circuit.source_boundary_sequence > 0
        JOIN boundaries AS boundary
          ON boundary.id = circuit.source_boundary_id
         AND boundary.work_item_id = prior.id
         AND boundary.attempt_id = prior_attempt.id
         AND boundary.source_principal_id = prior_attempt.worker_id
         AND boundary.kind = 'failure'
         AND boundary.goal_version = directive.expected_goal_version
         AND boundary.goal_packet_digest = directive.expected_goal_packet_digest
         AND boundary.task_packet_digest = prior_attempt.task_packet_digest
         AND json_extract(boundary.metadata_json, '$.runtime_recovery') = 1
         AND json_extract(boundary.metadata_json, '$.reason') =
             'runtime_provider_rate_limited'
        JOIN events AS boundary_event
          ON boundary_event.sequence = circuit.source_boundary_sequence
         AND boundary_event.event_type = 'boundary.recorded'
         AND boundary_event.aggregate_type = 'work_item'
         AND boundary_event.aggregate_id = prior.id
         AND boundary_event.actor_id = prior_attempt.worker_id
         AND json_extract(boundary_event.data_json, '$.boundary_id') = boundary.id
         AND json_extract(boundary_event.data_json, '$.generation') =
             boundary.generation
         AND json_extract(boundary_event.data_json, '$.kind') = 'failure'
        JOIN boundary_dispositions AS disposition
          ON disposition.boundary_id = boundary.id
         AND disposition.decided_by = prior.supervisor_id
         AND disposition.generation = boundary.generation
         AND disposition.kind = 'cancel'
         AND disposition.reason =
             'Continued by an explicit target task instruction.'
         AND disposition.instruction = ''
        JOIN reasoner_turns AS turn
          ON turn.id = disposition.reasoner_turn_id
         AND turn.supervisor_id = prior.supervisor_id
         AND turn.work_item_id = prior.id
         AND turn.boundary_id = boundary.id
         AND turn.generation = boundary.generation
         AND turn.goal_version = boundary.goal_version
         AND turn.goal_packet_digest = boundary.goal_packet_digest
         AND turn.task_packet_digest = boundary.task_packet_digest
         AND turn.state = 'completed'
         AND turn.completed_at IS NOT NULL
        JOIN events AS disposed_event
          ON disposed_event.event_type = 'boundary.disposed'
         AND disposed_event.aggregate_type = 'work_item'
         AND disposed_event.aggregate_id = prior.id
         AND disposed_event.actor_id = prior.supervisor_id
         AND disposed_event.sequence > boundary_event.sequence
         AND json_extract(disposed_event.data_json, '$.boundary_id') = boundary.id
         AND json_extract(disposed_event.data_json, '$.disposition_id') =
             disposition.id
         AND json_extract(disposed_event.data_json, '$.generation') =
             boundary.generation
         AND json_extract(disposed_event.data_json, '$.kind') = 'cancel'
         AND json_extract(disposed_event.data_json, '$.reason_code') =
             'continued_by_explicit_target_delegation'
        JOIN events AS canceled_event
          ON canceled_event.event_type = 'work.canceled'
         AND canceled_event.aggregate_type = 'work_item'
         AND canceled_event.aggregate_id = prior.id
         AND canceled_event.actor_id = prior.supervisor_id
         AND canceled_event.sequence > disposed_event.sequence
         AND json_extract(canceled_event.data_json, '$.reason') =
             'continued_by_explicit_target_delegation'
        JOIN events AS retired_event
          ON retired_event.event_type IN (
             'managed_worker.retired_for_target_delegation',
             'managed_worker.provider_boundary_consumed'
          )
         AND retired_event.aggregate_type = 'managed_worker_spec'
         AND retired_event.aggregate_id = prior_spec.id
         AND retired_event.actor_id = prior.supervisor_id
         AND retired_event.sequence > canceled_event.sequence
         AND json_extract(retired_event.data_json, '$.mode') = 'continue_prior'
         AND json_extract(retired_event.data_json, '$.source_work_item_id') = prior.id
        JOIN work_items AS successor
          ON successor.id = directive.created_work_item_id
         AND successor.supervisor_id = prior.supervisor_id
        JOIN attempts AS successor_attempt
          ON successor_attempt.work_item_id = successor.id
         AND successor_attempt.attempt_number = 1
        JOIN messages AS assignment_message
          ON assignment_message.work_item_id = successor.id
         AND assignment_message.attempt_id = successor_attempt.id
         AND assignment_message.kind = 'assignment'
         AND assignment_message.sender_id = prior.supervisor_id
        JOIN message_deliveries AS assignment_delivery
          ON assignment_delivery.message_id = assignment_message.id
         AND assignment_delivery.recipient_id = successor_attempt.worker_id
         AND assignment_delivery.runtime_session_id =
             successor_attempt.runtime_session_id
        JOIN events AS instruction_event
          ON instruction_event.event_type = 'work.assigned'
         AND instruction_event.aggregate_type = 'work_item'
         AND instruction_event.aggregate_id = successor.id
         AND instruction_event.actor_id = prior.supervisor_id
         AND instruction_event.causation_id = assignment_message.id
         AND instruction_event.sequence > retired_event.sequence
         AND json_extract(instruction_event.data_json, '$.attempt_id') =
             successor_attempt.id
         AND json_extract(instruction_event.data_json, '$.relation') = 'continue'
         AND json_extract(instruction_event.data_json, '$.source_receipt_id') =
             directive.source_receipt_id
         AND json_extract(instruction_event.data_json, '$.worker_id') =
             successor_attempt.worker_id
         AND json_extract(
               instruction_event.data_json, '$.provider_source_boundary_id'
             ) = boundary.id
        WHERE directive.relation = 'continue'
          AND prior.state = 'canceled'
          AND prior_attempt.state = 'canceled'
          AND circuit.state IN ('open', 'half_open', 'blocked', 'closed')
          AND boundary_event.sequence < disposed_event.sequence
          AND disposed_event.sequence < canceled_event.sequence
          AND canceled_event.sequence < retired_event.sequence
          AND retired_event.sequence < instruction_event.sequence
        """
    ).fetchall()
    provider_continuation_candidates: dict[str, list[sqlite3.Row]] = {}
    for provider_row in provider_continuation_rows:
        provider_continuation_candidates.setdefault(str(provider_row["directive_id"]), []).append(
            provider_row
        )
    provider_continuation_valid: dict[str, bool] = {}
    for directive_id, candidates in provider_continuation_candidates.items():
        if len(candidates) != 1:
            provider_continuation_valid[directive_id] = False
            continue
        candidate = candidates[0]
        idempotency_match = re.fullmatch(
            r"delegate-target:([0-9a-f]{64}):" + re.escape(str(candidate["source_boundary_id"])),
            str(candidate["turn_idempotency_key"] or ""),
        )
        decision = (
            {
                "kind": "cancel",
                "reason_code": "continued_by_explicit_target_delegation",
                "work_item_id": str(candidate["prior_work_item_id"]),
                "goal_packet_digest": str(candidate["prior_goal_packet_digest"]),
                "idempotency_key_digest": idempotency_match.group(1),
            }
            if idempotency_match is not None
            else None
        )
        expected_request_digest = _digest(decision) if decision is not None else ""
        expected_input_digest = _digest(
            {
                "boundary_id": str(candidate["source_boundary_id"]),
                "boundary_input_digest": str(candidate["boundary_input_digest"]),
                "goal_packet_digest": str(candidate["boundary_goal_packet_digest"]),
                "task_packet_digest": str(candidate["boundary_task_packet_digest"]),
                "generation": candidate["turn_generation"],
            }
        )
        expected_result_digest = _digest(
            {
                "boundary_id": str(candidate["source_boundary_id"]),
                "disposition_id": str(candidate["disposition_id"]),
                "request_digest": expected_request_digest,
                "goal_packet_digest": str(candidate["boundary_goal_packet_digest"]),
                "task_packet_digest": str(candidate["boundary_task_packet_digest"]),
            }
        )
        provider_continuation_valid[directive_id] = (
            decision is not None
            and candidate["turn_generation"] == candidate["boundary_generation"]
            and candidate["disposition_request_digest"] == expected_request_digest
            and candidate["turn_input_digest"] == expected_input_digest
            and candidate["turn_result_digest"] == expected_result_digest
            and candidate["turn_lease_token_digest"]
            == _digest(f"delegate-target:{candidate['turn_id']}")
        )
    for row in continuation_rows:
        prior_packet = _json_object(str(row["prior_packet_json"]))
        successor_packet = _json_object(str(row["successor_packet_json"]))
        receipt_payload = _json_object(str(row["receipt_payload_json"]))
        intent_content = _json_object(str(row["intent_content_json"]))
        disposition_result = _json_object(str(row["disposition_result_json"]))
        close_event_data = _json_object(str(row["close_event_data_json"]))
        close_result = _json_object(str(row["close_result_json"]))
        close_counts_valid = False
        if (
            close_event_data is not None
            and close_result is not None
            and row["close_generation"] is not None
            and row["close_sequence"] is not None
        ):
            close_generation = int(row["close_generation"])
            retired_spec_generations = [
                int(generation["attachment_generation"])
                for generation in connection.execute(
                    """
                    SELECT DISTINCT spec.attachment_generation
                    FROM events AS stopped
                    JOIN managed_worker_specs AS spec
                      ON spec.id = stopped.aggregate_id
                    WHERE stopped.event_type = 'managed_worker.stopped'
                      AND stopped.aggregate_type = 'managed_worker_spec'
                      AND stopped.actor_id = ?
                      AND stopped.sequence < ?
                      AND json_extract(stopped.data_json, '$.reason_code') =
                          'cao_conversation_closed'
                      AND json_extract(
                            stopped.data_json, '$.close_request_digest'
                          ) = ?
                      AND json_extract(
                            stopped.data_json, '$.closed_generation'
                          ) = ?
                      AND spec.attachment_id = ?
                      AND spec.attachment_generation <= ?
                    ORDER BY spec.attachment_generation
                    """,
                    (
                        row["prior_supervisor_id"],
                        row["close_sequence"],
                        row["close_request_digest"],
                        close_generation,
                        row["prior_attachment_id"],
                        close_generation,
                    ),
                ).fetchall()
            ]
            close_counts_valid = bool(
                row["close_cancel_events_unique"]
                and row["close_stop_events_unique"]
                and _conversation_close_receipt_contract_valid(
                    attachment_id=str(row["prior_attachment_id"] or ""),
                    close_generation=close_generation,
                    request_digest=str(row["close_request_digest"] or ""),
                    idempotency_key=str(row["close_idempotency_key"] or ""),
                    event_data=close_event_data,
                    result=close_result,
                    canceled_event_count=int(row["close_canceled_event_count"] or 0),
                    stopped_event_count=int(row["close_stopped_event_count"] or 0),
                    retired_spec_generations=retired_spec_generations,
                )
            )
        semantic_fields = (
            "title",
            "objective",
            "maturity",
            "acceptance",
            "non_goals",
            "priority",
            "completion_contract",
            "requester_id",
            "metadata",
            "dependencies",
            "managed_task_policy",
        )
        source_goal = receipt_payload.get("goal") if isinstance(receipt_payload, dict) else None
        source_id = str(row["source_id"] or "")
        scoped_key = source_id.removeprefix("delegate-target:")
        expected_assignment: dict[str, Any] | None = None
        expected_disposition_request: dict[str, Any] | None = None
        expected_disposition_request_digests: set[str] = set()
        if successor_packet is not None and re.fullmatch(r"[0-9a-f]{64}", scoped_key):
            expected_assignment = {
                "worker_id": str(row["successor_worker_id"] or ""),
                "title": successor_packet.get("title"),
                "objective": successor_packet.get("objective"),
                "maturity": successor_packet.get("maturity"),
                "acceptance": successor_packet.get("acceptance", []),
                "non_goals": successor_packet.get("non_goals", []),
                "priority": successor_packet.get("priority"),
                "completion_contract": successor_packet.get(
                    "completion_contract", "legacy_unclassified"
                ),
                "runtime_session_id": row["successor_runtime_id"],
                "supervisor_attachment_id": row["successor_attachment_id"],
                "supervisor_project_digest": row["successor_project_digest"],
                "requester_id": successor_packet.get("requester_id"),
                "metadata": successor_packet.get("metadata", {}),
                "idempotency_key": f"delegate:{scoped_key}",
            }
            dependencies = successor_packet.get("dependencies", [])
            if dependencies:
                expected_assignment["dependencies"] = dependencies
            expected_disposition_request = {
                "kind": "task",
                "relation": "continue",
                "target_work_item_id": row["target_work_item_id"],
                "assignment": expected_assignment,
                "directive": "",
                "reason": "explicit exact-goal continuation",
            }
            # WorkAssignment gained the nullable logical-thread pair in schema
            # 36.  Target continuation still resolves that pair server-side,
            # so both fields are semantically absent from this sealed request.
            # Accept only the two exact encodings emitted before and after the
            # model change; a partial or non-null pair remains invalid.
            expected_disposition_request_digests.add(_digest(expected_disposition_request))
            current_assignment = {
                **expected_assignment,
                "managed_worker_thread_id": None,
                "managed_worker_thread_generation": None,
            }
            expected_disposition_request_digests.add(
                _digest(
                    {
                        **expected_disposition_request,
                        "assignment": current_assignment,
                    }
                )
            )
            if (
                row["successor_spec_id"] == row["prior_spec_id"]
                and row["prior_attempt_runtime_id"] is not None
            ):
                historical_route_assignment = {
                    **expected_assignment,
                    "runtime_session_id": row["prior_attempt_runtime_id"],
                    "managed_worker_thread_id": None,
                    "managed_worker_thread_generation": None,
                }
                expected_disposition_request_digests.add(
                    _digest(
                        {
                            **expected_disposition_request,
                            "assignment": historical_route_assignment,
                        }
                    )
                )
        expected_source_goal = (
            {
                "title": successor_packet.get("title"),
                "objective": successor_packet.get("objective"),
                "maturity": successor_packet.get("maturity"),
                "acceptance": successor_packet.get("acceptance", []),
                "non_goals": successor_packet.get("non_goals", []),
                "priority": successor_packet.get("priority"),
                "completion_contract": successor_packet.get(
                    "completion_contract", "legacy_unclassified"
                ),
                "dependencies": successor_packet.get("dependencies", []),
                "managed_task_policy": successor_packet.get("managed_task_policy"),
            }
            if successor_packet is not None
            else None
        )
        expected_result = {
            "kind": "task",
            "relation": "continue",
            "work_item_id": row["created_work_item_id"],
            "disposition_id": row["disposition_id"],
            "reason": "explicit exact-goal continuation",
        }
        valid = (
            row["target_work_item_id"] is not None
            and row["created_work_item_id"] is not None
            and str(row["target_work_item_id"]) != str(row["created_work_item_id"])
            and prior_packet is not None
            and successor_packet is not None
            and all(
                prior_packet.get(field) == successor_packet.get(field) for field in semantic_fields
            )
            and row["prior_supervisor_id"] == row["successor_supervisor_id"]
            and row["prior_requester_id"] == row["successor_requester_id"]
            and row["prior_project_scope"] == row["successor_project_scope"]
            and row["prior_target_id"] == row["successor_target_id"]
            and row["prior_workspace_ref"] == row["successor_workspace_ref"]
            and row["prior_provider_scope"] == row["successor_provider_scope"]
            and prior_packet.get("supervisor_attachment", {}).get("generation")
            == row["prior_spec_generation"]
            and successor_packet.get("supervisor_attachment", {}).get("generation")
            == row["successor_attachment_generation"]
            and row["receipt_principal_id"] == row["prior_supervisor_id"]
            and row["intent_submitter_id"] == row["prior_supervisor_id"]
            and row["issuer_id"] == row["prior_supervisor_id"]
            and row["disposition_decided_by"] == row["prior_supervisor_id"]
            and row["intent_correlation_id"] == row["source_receipt_id"]
            and receipt_payload is not None
            and intent_content == receipt_payload
            and row["receipt_payload_digest"] == _digest(receipt_payload)
            and row["intent_content_digest"] == _digest(intent_content)
            and receipt_payload
            == {
                "type": "target_task_delegation",
                "target": row["prior_target_id"],
                "mode": "continue_prior",
                "goal": expected_source_goal,
                "prior_work_item_id": row["target_work_item_id"],
                "prior_goal_packet_digest": row["expected_goal_packet_digest"],
            }
            and source_goal == expected_source_goal
            and row["successor_source_intent_id"] == row["submitted_intent_id"]
            and row["successor_correlation_id"] == row["source_receipt_id"]
            and row["disposition_kind"] == "task"
            and row["disposition_relation"] == "continue"
            and row["disposition_target_id"] == row["target_work_item_id"]
            and row["disposition_result_id"] == row["created_work_item_id"]
            and row["disposition_directive_id"] == row["id"]
            and row["directive_state"] == "handled"
            and row["directive_handled_by"] == row["prior_supervisor_id"]
            and row["directive_content"] == prior_packet.get("objective")
            and row["directive_reason"] == "explicit exact-goal continuation"
            and row["disposition_reason"] == "explicit exact-goal continuation"
            and expected_disposition_request is not None
            and row["disposition_request_digest"] in expected_disposition_request_digests
            and disposition_result == expected_result
            and (
                (
                    row["prior_state"] == "completed"
                    and row["prior_attempt_state"] == "completed"
                    and row["prior_evidence_confidence"] == "verified"
                    and int(row["accepted_proof_count"] or 0) == 1
                )
                or (
                    row["prior_state"] == "canceled"
                    and int(row["close_proof_count"] or 0) == 1
                    and (
                        int(row["prior_spec_stop_proof_count"] or 0) == 1
                        or (
                            int(row["close_stopped_event_count"] or 0) == 0
                            and row["prior_spec_state"] == "enabled"
                            and row["successor_spec_id"] == row["prior_spec_id"]
                        )
                    )
                    and close_counts_valid
                )
                or provider_continuation_valid.get(str(row["id"]), False)
                or (
                    row["prior_state"] == "canceled"
                    and row["prior_attempt_state"] == "canceled"
                    and int(row["system_recovery_proof_count"] or 0) == 1
                    and row["prior_spec_state"] == "revoked"
                    and row["prior_runtime_state"] == "stopped"
                    and row["prior_enrollment_state"] == "revoked"
                    and row["successor_spec_id"] is not None
                )
            )
        )
        if not valid:
            violations.append(
                ProjectionViolation(
                    "directive.continuation_invalid",
                    "directive",
                    (str(row["id"]),),
                )
            )

    message_rows = connection.execute(
        """
        SELECT id, work_item_id, attempt_id, goal_version,
               goal_packet_digest, task_packet_digest
        FROM messages
        """
    ).fetchall()
    current_goals = {
        str(row["id"]): (
            int(row["goal_version"]),
            goal_digests.get((str(row["id"]), int(row["goal_version"])), ""),
        )
        for row in connection.execute("SELECT id, goal_version FROM work_items")
    }
    for row in message_rows:
        message_expected: tuple[int | None, str, str] | None
        attempt_id = row["attempt_id"]
        work_item_id = row["work_item_id"]
        if attempt_id is not None:
            expected_attempt = attempt_packets.get(str(attempt_id))
            message_expected = expected_attempt
        elif work_item_id is not None:
            current = current_goals.get(str(work_item_id))
            message_expected = (current[0], current[1], "") if current is not None else None
        else:
            message_expected = (None, "", "")
        message_actual = (
            int(row["goal_version"]) if row["goal_version"] is not None else None,
            str(row["goal_packet_digest"]),
            str(row["task_packet_digest"]),
        )
        if message_expected != message_actual:
            violations.append(
                ProjectionViolation("message.packet_mismatch", "message", (str(row["id"]),))
            )

    reasoner_rows = connection.execute(
        "SELECT id, state, input_digest, result_digest FROM reasoner_turns"
    ).fetchall()
    for row in reasoner_rows:
        if not str(row["input_digest"]) or (
            str(row["state"]) == "completed" and not str(row["result_digest"])
        ):
            violations.append(
                ProjectionViolation(
                    "reasoner.digest_evidence_missing", "reasoner_turn", (str(row["id"]),)
                )
            )


def _json_object(value: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _json_list(value: str) -> list[Any] | None:
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, list) else None


def _work_state_violations(
    connection: sqlite3.Connection, violations: list[ProjectionViolation]
) -> None:
    rows = connection.execute(
        """
        SELECT w.id, w.state, w.attention_owner, a.state AS attempt_state
        FROM work_items w
        LEFT JOIN attempts a ON a.work_item_id = w.id
                            AND a.attempt_number = (
                                SELECT MAX(attempt_number) FROM attempts current_attempt
                                WHERE current_attempt.work_item_id = w.id
                            )
        """
    ).fetchall()
    for row in rows:
        work_state = str(row["state"])
        expected_attention = _WORK_ATTENTION.get(work_state)
        if expected_attention is not None and str(row["attention_owner"]) != expected_attention:
            violations.append(
                ProjectionViolation("work.attention_owner_mismatch", "work_item", (str(row["id"]),))
            )
        allowed_attempt_states = _WORK_ATTEMPT_STATES.get(work_state)
        attempt_state = str(row["attempt_state"] or "")
        if allowed_attempt_states is not None and attempt_state not in allowed_attempt_states:
            violations.append(
                ProjectionViolation(
                    "work.current_attempt_state_mismatch", "work_item", (str(row["id"]),)
                )
            )


def _delivery_violations(
    connection: sqlite3.Connection,
    violations: list[ProjectionViolation],
    comparison_time: str,
) -> None:
    _add_rows(
        violations,
        connection,
        "message.without_delivery",
        "message",
        """
        SELECT m.id
        FROM messages m
        LEFT JOIN message_deliveries d ON d.message_id = m.id
        LEFT JOIN attempts a ON a.id = m.attempt_id
        WHERE d.message_id IS NULL
          AND NOT (
            m.kind IN ('progress', 'artifact')
            AND m.work_item_id IS NOT NULL
            AND m.attempt_id IS NOT NULL
            AND a.id IS NOT NULL
            AND m.work_item_id = a.work_item_id
            AND m.sender_id = a.worker_id
          )
        """,
    )
    _add_rows(
        violations,
        connection,
        "delivery.runtime_recipient_mismatch",
        "message_delivery",
        """
        SELECT d.message_id || ':' || d.recipient_id AS id
        FROM message_deliveries d
        JOIN runtime_sessions r ON r.id = d.runtime_session_id
        WHERE r.principal_id <> d.recipient_id
        """,
    )
    _add_rows(
        violations,
        connection,
        "delivery.attachment_binding_mismatch",
        "message_delivery",
        """
        SELECT delivery.message_id || ':' || delivery.recipient_id AS id
        FROM message_deliveries AS delivery
        JOIN messages AS message ON message.id = delivery.message_id
        JOIN principals AS recipient ON recipient.id = delivery.recipient_id
        LEFT JOIN work_items AS work ON work.id = message.work_item_id
        LEFT JOIN cao_session_attachments AS attachment
          ON attachment.id = delivery.recipient_attachment_id
        WHERE (
            delivery.recipient_attachment_id IS NOT NULL
            AND (
                recipient.role <> 'cao'
                OR attachment.id IS NULL
                OR attachment.principal_id <> delivery.recipient_id
                OR message.work_item_id IS NULL
                OR delivery.recipient_attachment_id IS NOT work.supervisor_attachment_id
            )
        ) OR (
            recipient.role = 'cao'
            AND work.supervisor_attachment_id IS NOT NULL
            AND delivery.recipient_attachment_id IS NOT work.supervisor_attachment_id
        )
        """,
    )
    _add_rows(
        violations,
        connection,
        "delivery.leased_without_fence",
        "message_delivery",
        """
        SELECT message_id || ':' || recipient_id AS id
        FROM message_deliveries
        WHERE state = 'leased' AND (lease_until IS NULL OR owner_token = '')
        """,
    )
    _add_rows(
        violations,
        connection,
        "delivery.lease_expired",
        "message_delivery",
        """
        SELECT message_id || ':' || recipient_id AS id
        FROM message_deliveries
        WHERE state = 'leased' AND lease_until IS NOT NULL AND lease_until < ?
        """,
        (comparison_time,),
    )
    _add_rows(
        violations,
        connection,
        "delivery.recipient_order_bypassed",
        "message_delivery",
        """
        SELECT later.message_id || ':' || later.recipient_id AS id
        FROM message_deliveries later
        JOIN messages later_message ON later_message.id = later.message_id
        JOIN principals recipient ON recipient.id = later.recipient_id
        WHERE later.state = 'leased'
          AND EXISTS (
              SELECT 1
              FROM message_deliveries earlier
              JOIN messages earlier_message ON earlier_message.id = earlier.message_id
              WHERE earlier.recipient_id = later.recipient_id
                AND earlier_message.sequence < later_message.sequence
                AND earlier.state IN ('queued', 'leased', 'dispatched', 'delivered', 'acknowledged')
                AND (
                    (
                        recipient.role = 'cao'
                        AND earlier.recipient_attachment_id IS later.recipient_attachment_id
                    )
                    OR (
                        recipient.role <> 'cao'
                        AND (
                            (
                                earlier.runtime_session_id IS NULL
                                AND later.runtime_session_id IS NULL
                            )
                            OR earlier.runtime_session_id = later.runtime_session_id
                        )
                    )
                )
          )
        """,
    )


def _runtime_violations(
    connection: sqlite3.Connection,
    violations: list[ProjectionViolation],
    comparison_time: str,
) -> None:
    placeholders = ",".join("?" for _ in _TERMINAL_RUNTIME_STATES)
    _add_rows(
        violations,
        connection,
        "runtime.lease_expired",
        "runtime_session",
        f"SELECT id FROM runtime_sessions WHERE state NOT IN ({placeholders}) AND lease_expires_at < ?",
        (*_TERMINAL_RUNTIME_STATES, comparison_time),
    )
    _add_rows(
        violations,
        connection,
        "runtime.unavailable_delivery_lease",
        "message_delivery",
        """
        SELECT d.message_id || ':' || d.recipient_id AS id
        FROM message_deliveries d
        JOIN runtime_sessions r ON r.id = d.runtime_session_id
        WHERE d.state = 'leased' AND r.state IN ('stopped', 'failed', 'missing')
        """,
    )
    _add_rows(
        violations,
        connection,
        "runtime.enrollment_identity_mismatch",
        "worker_enrollment",
        """
        SELECT e.id
        FROM worker_enrollments e
        JOIN runtime_sessions r ON r.id = e.runtime_session_id
        WHERE e.principal_id <> r.principal_id
        """,
    )
    _add_rows(
        violations,
        connection,
        "runtime.ready_enrollment_invalid",
        "worker_enrollment",
        # A ready enrollment describes a verified, reopenable Worker identity.
        # Its bearer exists only for a live launch epoch (runtime ready/busy).
        # A clean process exit deliberately leaves the runtime waiting and
        # revokes every bearer until the next one-use ticket is exchanged.
        """
        SELECT e.id
        FROM worker_enrollments e
        JOIN runtime_sessions r ON r.id = e.runtime_session_id
        WHERE e.state = 'ready'
          AND (
            e.required_tools_digest = ''
            OR e.discovered_tools_digest <> e.required_tools_digest
            OR e.protocol_version = ''
            OR e.lease_expires_at IS NULL
            OR e.lease_expires_at < ?
            OR r.lease_expires_at < ?
            OR r.state NOT IN ('ready', 'busy', 'waiting')
            OR (
              r.state IN ('ready', 'busy')
              AND (SELECT COUNT(*) FROM runtime_credentials c
                   WHERE c.enrollment_id = e.id AND c.principal_id = e.principal_id
                     AND c.generation = e.generation AND c.state = 'active'
                     AND c.expires_at >= ?) <> 1
            )
            OR (
              r.state = 'waiting'
              AND (SELECT COUNT(*) FROM runtime_credentials c
                   WHERE c.enrollment_id = e.id AND c.state = 'active') <> 0
            )
          )
        """,
        (comparison_time, comparison_time, comparison_time),
    )
    _add_rows(
        violations,
        connection,
        "runtime.credential_outlives_enrollment",
        "runtime_credential",
        """
        SELECT c.id
        FROM runtime_credentials c
        JOIN worker_enrollments e ON e.id = c.enrollment_id
        WHERE c.state = 'active'
          AND (c.generation <> e.generation OR e.state IN ('stale', 'revoked', 'failed'))
        """,
    )
    _add_rows(
        violations,
        connection,
        "runtime.enrollment_ticket_expired_pending",
        "runtime_enrollment_ticket",
        """
        SELECT id FROM runtime_enrollment_tickets
        WHERE state = 'pending' AND expires_at < ?
        """,
        (comparison_time,),
    )
    _provider_runtime_circuit_violations(connection, violations, comparison_time)


def _provider_scope_digest(adapter: str, model: str) -> str:
    """Rebuild the opaque provider scope without exposing owner credentials."""

    return _digest(
        {
            "adapter": adapter,
            "auth_scope": "owner-local",
            "model": model,
        }
    )


def _provider_closed_source_takeover_sequence(
    connection: sqlite3.Connection,
    *,
    source_runtime_id: str,
    source_boundary_id: str,
    source_work_id: str,
    source_boundary_sequence: int,
    candidate_attachment_id: str,
    comparison_time: str,
) -> int | None:
    """Independently verify the exact safe-close chain for a later CAO instruction."""

    rows = connection.execute(
        """
        SELECT close_event.sequence AS close_sequence,
               close_event.data_json AS close_event_data_json,
               CAST(json_extract(
                   close_event.data_json, '$.closed_generation'
               ) AS INTEGER) AS close_generation,
               source_attachment.id AS source_attachment_id,
               source_work.supervisor_id AS source_supervisor_id,
               source_spec.id AS source_spec_id,
               close_result.idempotency_key AS close_idempotency_key,
               close_result.request_digest AS close_request_digest,
               close_result.result_json AS close_result_json
        FROM managed_worker_specs AS source_spec
        JOIN cao_session_attachments AS source_attachment
          ON source_attachment.id = source_spec.attachment_id
        JOIN runtime_sessions AS source_cao_runtime
          ON source_cao_runtime.id = source_attachment.runtime_session_id
         AND source_cao_runtime.principal_id = source_attachment.principal_id
        JOIN runtime_sessions AS source_worker_runtime
          ON source_worker_runtime.id = source_spec.runtime_session_id
         AND source_worker_runtime.principal_id = source_spec.principal_id
         AND source_worker_runtime.state = 'stopped'
        JOIN boundaries AS source_boundary
          ON source_boundary.id = ?
         AND source_boundary.work_item_id = ?
         AND source_boundary.source_principal_id = source_spec.principal_id
        JOIN attempts AS source_attempt
          ON source_attempt.id = source_boundary.attempt_id
         AND source_attempt.work_item_id = source_boundary.work_item_id
         AND source_attempt.worker_id = source_spec.principal_id
         AND source_attempt.runtime_session_id = source_spec.runtime_session_id
        JOIN work_items AS source_work
          ON source_work.id = source_boundary.work_item_id
         AND source_work.supervisor_attachment_id = source_attachment.id
         AND source_work.supervisor_id = source_attachment.principal_id
         AND source_work.state = 'canceled'
        JOIN events AS source_boundary_event
          ON source_boundary_event.sequence = ?
         AND source_boundary_event.event_type = 'boundary.recorded'
         AND source_boundary_event.aggregate_type = 'work_item'
         AND source_boundary_event.aggregate_id = source_work.id
         AND source_boundary_event.actor_id = source_attempt.worker_id
         AND json_extract(source_boundary_event.data_json, '$.boundary_id') =
             source_boundary.id
         AND json_extract(source_boundary_event.data_json, '$.kind') =
             source_boundary.kind
         AND json_extract(source_boundary_event.data_json, '$.runtime_state') =
             source_boundary.runtime_state
         AND json_extract(source_boundary_event.data_json, '$.generation') =
             source_boundary.generation
        JOIN events AS close_disposition
          ON close_disposition.event_type = 'boundary.disposed'
         AND close_disposition.aggregate_type = 'work_item'
         AND close_disposition.aggregate_id = source_work.id
         AND close_disposition.actor_id = source_work.supervisor_id
         AND close_disposition.sequence > source_boundary_event.sequence
         AND json_extract(close_disposition.data_json, '$.boundary_id') =
             source_boundary.id
         AND json_extract(close_disposition.data_json, '$.kind') = 'cancel'
         AND json_extract(close_disposition.data_json, '$.reason_code') =
             'cao_conversation_closed'
         AND json_extract(close_disposition.data_json, '$.attachment_id') =
             source_attachment.id
        JOIN events AS cancel_event
          ON cancel_event.event_type = 'work.canceled'
         AND cancel_event.aggregate_type = 'work_item'
         AND cancel_event.aggregate_id = source_work.id
         AND cancel_event.actor_id = source_work.supervisor_id
         AND cancel_event.sequence > close_disposition.sequence
         AND json_extract(cancel_event.data_json, '$.reason') =
             'cao_conversation_closed'
        JOIN events AS close_event
          ON close_event.event_type = 'cao.conversation_closed'
         AND close_event.aggregate_type = 'cao_session_attachment'
         AND close_event.aggregate_id = source_attachment.id
         AND close_event.actor_id = source_work.supervisor_id
         AND close_event.sequence > cancel_event.sequence
         AND CAST(json_extract(
               close_event.data_json, '$.closed_generation'
             ) AS INTEGER) >= source_spec.attachment_generation
        JOIN idempotency_results AS close_result
          ON close_result.actor_id = source_work.supervisor_id
         AND close_result.operation =
             'close_cao_conversation:' || source_attachment.id || ':' ||
             CAST(json_extract(
                 close_event.data_json, '$.closed_generation'
             ) AS INTEGER)
        JOIN cao_session_attachments AS candidate_attachment
          ON candidate_attachment.id = ?
         AND candidate_attachment.principal_id = source_attachment.principal_id
         AND candidate_attachment.project_scope_digest = source_attachment.project_scope_digest
         AND candidate_attachment.state = 'active'
         AND candidate_attachment.lease_expires_at > ?
        JOIN runtime_sessions AS candidate_runtime
          ON candidate_runtime.id = candidate_attachment.runtime_session_id
         AND candidate_runtime.principal_id = candidate_attachment.principal_id
         AND candidate_runtime.state IN ('ready', 'waiting', 'busy')
         AND candidate_runtime.lease_expires_at > ?
        WHERE source_spec.runtime_session_id = ?
          AND source_spec.state = 'stopped'
          AND source_spec.catalog_target_id <> ''
          AND source_spec.attachment_generation = (
              SELECT source_goal.supervisor_attachment_generation
              FROM goal_revisions AS source_goal
              WHERE source_goal.work_item_id = source_work.id
                AND source_goal.version = source_work.goal_version
          )
          AND source_boundary.kind = 'failure'
          AND json_extract(source_boundary.metadata_json, '$.runtime_recovery') = 1
          AND json_extract(source_boundary.metadata_json, '$.reason') =
              'runtime_provider_rate_limited'
          AND json_extract(
                close_disposition.data_json, '$.attachment_generation'
              ) = json_extract(
                    close_event.data_json, '$.closed_generation'
                  )
          AND json_extract(
                cancel_event.data_json, '$.close_request_digest'
              ) = close_result.request_digest
          AND json_extract(cancel_event.data_json, '$.closed_generation') =
              json_extract(close_event.data_json, '$.closed_generation')
          AND (
              (candidate_attachment.id <> source_attachment.id
               AND source_attachment.state = 'revoked'
               AND source_attachment.generation =
                   CAST(json_extract(
                       close_event.data_json, '$.closed_generation'
                   ) AS INTEGER)
               AND source_cao_runtime.state IN ('stopped', 'failed', 'missing'))
              OR
              (candidate_attachment.id = source_attachment.id
               AND source_attachment.state = 'active'
               AND source_attachment.generation =
                   CAST(json_extract(
                       close_event.data_json, '$.closed_generation'
                   ) AS INTEGER) + 1
               AND source_cao_runtime.state IN ('ready', 'waiting', 'busy'))
          )
          AND NOT EXISTS (
              SELECT 1 FROM cao_attachment_bootstrap_credentials AS bootstrap
              WHERE bootstrap.attachment_id = source_attachment.id
                AND bootstrap.state = 'active'
                AND bootstrap.expires_at > ?
          )
          AND NOT EXISTS (
              SELECT 1 FROM cao_conversation_credentials AS credential
              WHERE credential.attachment_id = source_attachment.id
                AND credential.state = 'active'
                AND (candidate_attachment.id <> source_attachment.id
                     OR credential.generation <= CAST(json_extract(
                         close_event.data_json, '$.closed_generation'
                     ) AS INTEGER))
          )
          AND NOT EXISTS (
              SELECT 1 FROM cao_runtime_credentials AS credential
              WHERE credential.attachment_id = source_attachment.id
                AND credential.state = 'active'
                AND (candidate_attachment.id <> source_attachment.id
                     OR credential.generation <= CAST(json_extract(
                         close_event.data_json, '$.closed_generation'
                     ) AS INTEGER))
          )
          AND NOT EXISTS (
              SELECT 1 FROM cao_runtime_tickets AS ticket
              WHERE ticket.attachment_id = source_attachment.id
                AND ticket.state = 'pending' AND ticket.expires_at > ?
                AND (candidate_attachment.id <> source_attachment.id
                     OR ticket.generation <= CAST(json_extract(
                         close_event.data_json, '$.closed_generation'
                     ) AS INTEGER))
          )
          AND NOT EXISTS (
              SELECT 1
              FROM message_deliveries AS delivery
              JOIN messages AS message ON message.id = delivery.message_id
              LEFT JOIN attempts AS attempt ON attempt.id = message.attempt_id
              WHERE delivery.state = 'dispatched' AND (
                  message.work_item_id IN (
                      SELECT old_work.id FROM work_items AS old_work
                      WHERE old_work.supervisor_attachment_id = source_attachment.id
                        AND EXISTS (
                            SELECT 1 FROM events AS old_instruction
                            WHERE old_instruction.event_type = 'work.assigned'
                              AND old_instruction.aggregate_type = 'work_item'
                              AND old_instruction.aggregate_id = old_work.id
                              AND old_instruction.sequence < close_event.sequence))
                  OR delivery.runtime_session_id = source_attachment.runtime_session_id
                  OR delivery.recipient_id IN (
                      SELECT principal_id FROM managed_worker_specs
                      WHERE attachment_id = source_attachment.id
                        AND attachment_generation <= CAST(json_extract(
                            close_event.data_json, '$.closed_generation'
                        ) AS INTEGER))
                  OR message.sender_id IN (
                      SELECT principal_id FROM managed_worker_specs
                      WHERE attachment_id = source_attachment.id
                        AND attachment_generation <= CAST(json_extract(
                            close_event.data_json, '$.closed_generation'
                        ) AS INTEGER))
                  OR attempt.worker_id IN (
                      SELECT principal_id FROM managed_worker_specs
                      WHERE attachment_id = source_attachment.id
                        AND attachment_generation <= CAST(json_extract(
                            close_event.data_json, '$.closed_generation'
                        ) AS INTEGER))
              )
          )
          AND NOT EXISTS (
              SELECT 1 FROM effect_operations AS effect
              WHERE effect.status IN ('started', 'unknown') AND (
                  effect.principal_id IN (
                      SELECT principal_id FROM managed_worker_specs
                      WHERE attachment_id = source_attachment.id
                        AND attachment_generation <= CAST(json_extract(
                            close_event.data_json, '$.closed_generation'
                        ) AS INTEGER))
                  OR effect.cleanup_work_item_id IN (
                      SELECT old_work.id FROM work_items AS old_work
                      WHERE old_work.supervisor_attachment_id = source_attachment.id
                        AND EXISTS (
                            SELECT 1 FROM events AS old_instruction
                            WHERE old_instruction.event_type = 'work.assigned'
                              AND old_instruction.aggregate_type = 'work_item'
                              AND old_instruction.aggregate_id = old_work.id
                              AND old_instruction.sequence < close_event.sequence))
              )
          )
          AND (SELECT COUNT(*) FROM events AS exact_disposition
               WHERE exact_disposition.event_type = 'boundary.disposed'
                 AND exact_disposition.aggregate_type = 'work_item'
                 AND exact_disposition.aggregate_id = source_work.id
                 AND exact_disposition.actor_id = source_work.supervisor_id
                 AND exact_disposition.sequence > source_boundary_event.sequence
                 AND json_extract(exact_disposition.data_json, '$.boundary_id') =
                     source_boundary.id
                 AND json_extract(exact_disposition.data_json, '$.kind') = 'cancel'
                 AND json_extract(exact_disposition.data_json, '$.reason_code') =
                     'cao_conversation_closed'
                 AND json_extract(
                       exact_disposition.data_json, '$.attachment_id'
                     ) = source_attachment.id
                 AND json_extract(
                       exact_disposition.data_json, '$.attachment_generation'
                     ) = json_extract(
                           close_event.data_json, '$.closed_generation'
                         )) = 1
          AND (SELECT COUNT(*) FROM events AS exact_cancel
               WHERE exact_cancel.event_type = 'work.canceled'
                 AND exact_cancel.aggregate_type = 'work_item'
                 AND exact_cancel.aggregate_id = source_work.id
                 AND exact_cancel.actor_id = source_work.supervisor_id
                 AND exact_cancel.sequence > source_boundary_event.sequence
                 AND json_extract(exact_cancel.data_json, '$.reason') =
                     'cao_conversation_closed'
                 AND json_extract(
                       exact_cancel.data_json, '$.close_request_digest'
                     ) = close_result.request_digest
                 AND json_extract(
                       exact_cancel.data_json, '$.closed_generation'
                     ) = json_extract(
                           close_event.data_json, '$.closed_generation'
                         )) = 1
          AND (SELECT COUNT(*) FROM events AS exact_stop
               WHERE exact_stop.event_type = 'managed_worker.stopped'
                 AND exact_stop.aggregate_type = 'managed_worker_spec'
                 AND exact_stop.aggregate_id = source_spec.id
                 AND exact_stop.actor_id = source_work.supervisor_id
                 AND exact_stop.sequence > cancel_event.sequence
                 AND exact_stop.sequence < close_event.sequence
                 AND json_extract(exact_stop.data_json, '$.reason_code') =
                     'cao_conversation_closed'
                 AND json_extract(
                       exact_stop.data_json, '$.close_request_digest'
                     ) = close_result.request_digest
                 AND json_extract(
                       exact_stop.data_json, '$.closed_generation'
                     ) = json_extract(
                           close_event.data_json, '$.closed_generation'
                         )) = 1
          AND (SELECT COUNT(*) FROM events AS exact_close
               WHERE exact_close.event_type = 'cao.conversation_closed'
                 AND exact_close.aggregate_type = 'cao_session_attachment'
                 AND exact_close.aggregate_id = source_attachment.id
                 AND exact_close.actor_id = source_work.supervisor_id
                 AND exact_close.sequence > source_boundary_event.sequence
                 AND json_extract(exact_close.data_json, '$.closed_generation') =
                     json_extract(
                         close_event.data_json, '$.closed_generation'
                     )) = 1
        """,
        (
            source_boundary_id,
            source_work_id,
            source_boundary_sequence,
            candidate_attachment_id,
            comparison_time,
            comparison_time,
            source_runtime_id,
            comparison_time,
            comparison_time,
        ),
    ).fetchall()
    if len(rows) != 1:
        return None
    row = rows[0]
    sequence = int(row["close_sequence"] or 0)
    close_generation = int(row["close_generation"])
    counts = connection.execute(
        """
        SELECT
          (
            SELECT COUNT(*) FROM events AS canceled
            JOIN work_items AS work ON work.id = canceled.aggregate_id
            JOIN goal_revisions AS goal
              ON goal.work_item_id = work.id
             AND goal.version = work.goal_version
            WHERE canceled.event_type = 'work.canceled'
              AND canceled.aggregate_type = 'work_item'
              AND canceled.actor_id = ?
              AND canceled.sequence < ?
              AND json_extract(canceled.data_json, '$.reason') =
                  'cao_conversation_closed'
              AND json_extract(
                    canceled.data_json, '$.close_request_digest'
                  ) = ?
              AND json_extract(canceled.data_json, '$.closed_generation') = ?
              AND work.supervisor_attachment_id = ?
              AND goal.supervisor_attachment_generation <= ?
          ) AS canceled_count,
          (
            SELECT COUNT(*) FROM events AS stopped
            JOIN managed_worker_specs AS spec ON spec.id = stopped.aggregate_id
            WHERE stopped.event_type = 'managed_worker.stopped'
              AND stopped.aggregate_type = 'managed_worker_spec'
              AND stopped.actor_id = ?
              AND stopped.sequence < ?
              AND json_extract(stopped.data_json, '$.reason_code') =
                  'cao_conversation_closed'
              AND json_extract(
                    stopped.data_json, '$.close_request_digest'
                  ) = ?
              AND json_extract(stopped.data_json, '$.closed_generation') = ?
              AND spec.attachment_id = ?
              AND spec.attachment_generation <= ?
          ) AS stopped_count
        """,
        (
            row["source_supervisor_id"],
            sequence,
            row["close_request_digest"],
            close_generation,
            row["source_attachment_id"],
            close_generation,
            row["source_supervisor_id"],
            sequence,
            row["close_request_digest"],
            close_generation,
            row["source_attachment_id"],
            close_generation,
        ),
    ).fetchone()
    assert counts is not None
    retired_spec_generations = [
        int(generation["attachment_generation"])
        for generation in connection.execute(
            """
            SELECT DISTINCT spec.attachment_generation
            FROM events AS stopped
            JOIN managed_worker_specs AS spec ON spec.id = stopped.aggregate_id
            WHERE stopped.event_type = 'managed_worker.stopped'
              AND stopped.aggregate_type = 'managed_worker_spec'
              AND stopped.actor_id = ?
              AND stopped.sequence < ?
              AND json_extract(stopped.data_json, '$.reason_code') =
                  'cao_conversation_closed'
              AND json_extract(
                    stopped.data_json, '$.close_request_digest'
                  ) = ?
              AND json_extract(stopped.data_json, '$.closed_generation') = ?
              AND spec.attachment_id = ?
              AND spec.attachment_generation <= ?
            ORDER BY spec.attachment_generation
            """,
            (
                row["source_supervisor_id"],
                sequence,
                row["close_request_digest"],
                close_generation,
                row["source_attachment_id"],
                close_generation,
            ),
        ).fetchall()
    ]
    close_event_data = _json_object(str(row["close_event_data_json"]))
    close_result = _json_object(str(row["close_result_json"]))
    if (
        close_event_data is None
        or close_result is None
        or not _conversation_close_receipt_contract_valid(
            attachment_id=str(row["source_attachment_id"]),
            close_generation=close_generation,
            request_digest=str(row["close_request_digest"]),
            idempotency_key=str(row["close_idempotency_key"]),
            event_data=close_event_data,
            result=close_result,
            canceled_event_count=int(counts["canceled_count"]),
            stopped_event_count=int(counts["stopped_count"]),
            retired_spec_generations=retired_spec_generations,
        )
    ):
        return None
    return sequence if sequence > source_boundary_sequence else None


def _provider_runtime_circuit_violations(
    connection: sqlite3.Connection,
    violations: list[ProjectionViolation],
    comparison_time: str,
) -> None:
    """Verify every active provider circuit from exact durable bindings.

    The Dashboard may display an open or half-open circuit only when its
    fixed failure code is backed by the exact managed runtime, Work, Attempt,
    and typed recovery Boundary.  A half-open circuit additionally owns one
    exact assignment Delivery on a fresh same-scope runtime.  These checks are
    deliberately independent from the service writer so imported or corrupted
    rows fail closed during projection verification.
    """

    for spec in connection.execute(
        "SELECT id, adapter, effective_model, provider_scope_digest FROM managed_worker_specs"
    ).fetchall():
        if str(spec["provider_scope_digest"]) != _provider_scope_digest(
            str(spec["adapter"]), str(spec["effective_model"])
        ):
            violations.append(
                ProjectionViolation(
                    "runtime.provider_scope_digest_mismatch",
                    "managed_worker_spec",
                    (str(spec["id"]),),
                )
            )

    closed_rows = connection.execute(
        "SELECT scope_digest FROM provider_runtime_circuits "
        "WHERE state = 'closed' "
        "AND (restart_authorized <> 0 OR probe_restart_override <> 0 "
        "OR probe_outcome_state <> 'none')"
    ).fetchall()
    for circuit in closed_rows:
        violations.append(
            ProjectionViolation(
                "runtime.provider_circuit_closed_authorization_invalid",
                "provider_runtime_circuit",
                (_digest(str(circuit["scope_digest"])),),
            )
        )

    rows = connection.execute(
        """
        SELECT *
        FROM provider_runtime_circuits
        WHERE state IN ('open', 'half_open', 'blocked')
        ORDER BY scope_digest
        """
    ).fetchall()
    for circuit in rows:
        circuit_id = str(circuit["scope_digest"])
        circuit_subject_id = _digest(circuit_id)
        source_runtime_id = str(circuit["source_runtime_session_id"] or "")
        source_boundary_id = str(circuit["source_boundary_id"] or "")
        source_work_id = str(circuit["source_work_item_id"] or "")
        source = connection.execute(
            """
            SELECT boundary.id, boundary.source_principal_id,
                   boundary.work_item_id AS boundary_work_item_id,
                   boundary.kind, boundary.metadata_json,
                   attempt.id AS attempt_id,
                   attempt.work_item_id AS attempt_work_item_id,
                   attempt.worker_id AS attempt_worker_id,
                   attempt.runtime_session_id AS attempt_runtime_session_id,
                   attempt.attempt_number AS attempt_number,
                   work.state AS source_work_state,
                   work.supervisor_id AS source_supervisor_id,
                   work.supervisor_attachment_id AS source_supervisor_attachment_id,
                   boundary_event.sequence AS boundary_sequence,
                   (SELECT COUNT(*) FROM events AS close_cancel
                    WHERE close_cancel.event_type = 'work.canceled'
                      AND close_cancel.aggregate_type = 'work_item'
                      AND close_cancel.aggregate_id = work.id
                      AND json_extract(close_cancel.data_json, '$.reason') =
                          'cao_conversation_closed'
                      AND close_cancel.sequence > boundary_event.sequence) AS close_cancel_count,
                   (SELECT COUNT(*) FROM events AS conversation_close
                    WHERE conversation_close.event_type = 'cao.conversation_closed'
                      AND conversation_close.aggregate_type = 'cao_session_attachment'
                      AND conversation_close.aggregate_id = work.supervisor_attachment_id
                      AND conversation_close.sequence > boundary_event.sequence) AS conversation_close_count,
                   (SELECT MAX(conversation_close.sequence)
                    FROM events AS conversation_close
                    WHERE conversation_close.event_type = 'cao.conversation_closed'
                      AND conversation_close.aggregate_type = 'cao_session_attachment'
                      AND conversation_close.aggregate_id = work.supervisor_attachment_id
                      AND conversation_close.sequence > boundary_event.sequence) AS conversation_close_sequence,
                   (SELECT COUNT(*) FROM message_deliveries AS unresolved_delivery
                    LEFT JOIN messages AS unresolved_message
                      ON unresolved_message.id = unresolved_delivery.message_id
                    WHERE unresolved_delivery.state = 'dispatched'
                      AND (unresolved_message.work_item_id = work.id
                           OR unresolved_delivery.runtime_session_id =
                              attempt.runtime_session_id)) AS unresolved_delivery_count,
                   (SELECT COUNT(*) FROM effect_operations AS unresolved_effect
                    WHERE unresolved_effect.status IN ('started', 'unknown')
                      AND (unresolved_effect.principal_id = attempt.worker_id
                           OR unresolved_effect.cleanup_work_item_id = work.id))
                       AS unresolved_effect_count,
                   (SELECT COUNT(*) FROM cao_conversation_credentials AS credential
                    WHERE credential.attachment_id = work.supervisor_attachment_id
                      AND credential.state = 'active') AS active_conversation_credential_count,
                   (SELECT COUNT(*) FROM events AS duplicate_event
                    WHERE duplicate_event.event_type = 'boundary.recorded'
                      AND json_extract(duplicate_event.data_json, '$.boundary_id') =
                          boundary.id) AS boundary_event_count
                   ,(SELECT COUNT(*)
                     FROM messages AS source_message
                     JOIN message_deliveries AS source_delivery
                       ON source_delivery.message_id = source_message.id
                      AND source_delivery.recipient_id = attempt.worker_id
                      AND source_delivery.runtime_session_id =
                          attempt.runtime_session_id
                     JOIN events AS source_instruction
                       ON source_instruction.causation_id = source_message.id
                      AND source_instruction.actor_id = work.supervisor_id
                     WHERE source_message.kind = 'assignment'
                       AND source_message.attempt_id = attempt.id
                       AND source_message.work_item_id = work.id
                       AND source_message.sender_id = work.supervisor_id
                       AND (
                            (source_instruction.event_type = 'work.assigned'
                             AND source_instruction.aggregate_type = 'work_item'
                             AND source_instruction.aggregate_id = work.id
                             AND json_extract(
                                   source_instruction.data_json, '$.attempt_id'
                                 ) = attempt.id
                             AND json_extract(
                                   source_instruction.data_json, '$.worker_id'
                                 ) = attempt.worker_id)
                         OR (source_instruction.event_type = 'work.goal_replaced'
                             AND source_instruction.aggregate_type = 'work_item'
                             AND source_instruction.aggregate_id = work.id
                             AND json_extract(
                                   source_instruction.data_json, '$.attempt_id'
                                 ) = attempt.id)
                         OR (source_instruction.event_type = 'attempt.created'
                             AND source_instruction.aggregate_type = 'attempt'
                             AND source_instruction.aggregate_id = attempt.id
                             AND json_extract(
                                   source_instruction.data_json, '$.work_item_id'
                                 ) = work.id)
                       )) AS source_instruction_count
            FROM boundaries AS boundary
            LEFT JOIN attempts AS attempt ON attempt.id = boundary.attempt_id
            LEFT JOIN work_items AS work ON work.id = boundary.work_item_id
            LEFT JOIN events AS boundary_event
              ON boundary_event.event_type = 'boundary.recorded'
             AND boundary_event.aggregate_type = 'work_item'
             AND boundary_event.aggregate_id = boundary.work_item_id
             AND json_extract(boundary_event.data_json, '$.boundary_id') = boundary.id
            WHERE boundary.id = ?
            """,
            (source_boundary_id,),
        ).fetchone()
        source_metadata = _json_object(str(source["metadata_json"])) if source is not None else None
        source_valid = bool(
            source_runtime_id
            and source_boundary_id
            and source_work_id
            and source is not None
            and str(source["boundary_work_item_id"]) == source_work_id
            and str(source["attempt_work_item_id"] or "") == source_work_id
            and str(source["attempt_runtime_session_id"] or "") == source_runtime_id
            and str(source["source_principal_id"]) == str(source["attempt_worker_id"] or "")
            and str(source["kind"]) == "failure"
            and int(circuit["source_boundary_sequence"] or 0) > 0
            and int(source["boundary_event_count"] or 0) == 1
            and int(source["source_instruction_count"] or 0) == 1
            and int(source["boundary_sequence"] or 0) == int(circuit["source_boundary_sequence"])
            and str(circuit["failure_code"]) == "runtime_provider_rate_limited"
            and source_metadata is not None
            and source_metadata.get("runtime_recovery") is True
            and source_metadata.get("reason") == "runtime_provider_rate_limited"
        )
        if not source_valid:
            violations.append(
                ProjectionViolation(
                    "runtime.provider_circuit_source_invalid",
                    "provider_runtime_circuit",
                    (circuit_subject_id,),
                )
            )

        source_spec = connection.execute(
            """
            SELECT spec.id AS spec_id, spec.principal_id, spec.adapter,
                   spec.effective_model, spec.state,
                   spec.provider_scope_digest, spec.attachment_id,
                   spec.attachment_generation, spec.catalog_target_id,
                   spec.workspace_ref,
                   attachment.principal_id AS attachment_principal_id,
                   attachment.project_scope_digest AS project_digest, attachment.generation,
                   attachment.state AS attachment_state,
                   runtime.principal_id AS runtime_principal_id,
                   cao_runtime.state AS cao_runtime_state
            FROM managed_worker_specs AS spec
            JOIN runtime_sessions AS runtime
              ON runtime.id = spec.runtime_session_id
            JOIN cao_session_attachments AS attachment
              ON attachment.id = spec.attachment_id
            JOIN runtime_sessions AS cao_runtime
              ON cao_runtime.id = attachment.runtime_session_id
            WHERE spec.runtime_session_id = ?
               OR EXISTS (
                    SELECT 1
                    FROM managed_worker_thread_epochs AS source_epoch
                    JOIN managed_worker_threads AS source_thread
                      ON source_thread.id = source_epoch.thread_id
                    WHERE source_epoch.runtime_session_id = ?
                      AND source_thread.managed_spec_id = spec.id
               )
            """,
            (source_runtime_id, source_runtime_id),
        ).fetchone()
        source_scope_valid = bool(
            source_spec is not None
            and str(source_spec["adapter"]) == "claude"
            and source is not None
            and str(source_spec["principal_id"]) == str(source["attempt_worker_id"] or "")
            and str(source_spec["runtime_principal_id"]) == str(source_spec["principal_id"])
            and str(source_spec["attachment_id"])
            == str(source["source_supervisor_attachment_id"] or "")
            and str(source_spec["attachment_principal_id"])
            == str(source["source_supervisor_id"] or "")
            and str(source_spec["provider_scope_digest"]) == circuit_id
            and str(source_spec["catalog_target_id"]) != ""
            and str(source_spec["provider_scope_digest"])
            == _provider_scope_digest(
                str(source_spec["adapter"]), str(source_spec["effective_model"])
            )
        )
        if not source_scope_valid:
            violations.append(
                ProjectionViolation(
                    "runtime.provider_circuit_scope_mismatch",
                    "provider_runtime_circuit",
                    (circuit_subject_id,),
                )
            )

        if str(circuit["state"]) == "open":
            if (
                str(circuit["probe_restart_override"]) != "0"
                or str(circuit["probe_outcome_state"]) != "none"
                or circuit["probe_runtime_session_id"] is not None
                or circuit["probe_attempt_id"] is not None
                or str(circuit["probe_message_id"]) != ""
                or int(circuit["probe_instruction_sequence"] or 0) != 0
            ):
                violations.append(
                    ProjectionViolation(
                        "runtime.provider_circuit_open_probe_invalid",
                        "provider_runtime_circuit",
                        (circuit_subject_id,),
                    )
                )
            continue

        probe_runtime_id = str(circuit["probe_runtime_session_id"] or "")
        probe_attempt_id = str(circuit["probe_attempt_id"] or "")
        probe_message_id = str(circuit["probe_message_id"] or "")
        probe = connection.execute(
            """
            SELECT attempt.id, attempt.work_item_id, attempt.worker_id,
                   attempt.runtime_session_id, attempt.attempt_number,
                   message.kind AS message_kind,
                   message.sender_id AS message_sender_id,
                   message.work_item_id AS message_work_item_id,
                   message.attempt_id AS message_attempt_id,
                   delivery.recipient_id AS delivery_recipient_id,
                   delivery.runtime_session_id AS delivery_runtime_session_id,
                   delivery.state AS delivery_state,
                   runtime.state AS runtime_state,
                   enrollment.state AS enrollment_state,
                   work.assigned_worker_id, work.supervisor_id,
                   work.supervisor_attachment_id,
                   supervisor_attachment.project_scope_digest AS supervisor_project_digest,
                   supervisor_attachment.generation AS supervisor_attachment_generation,
                   supervisor_attachment.state AS supervisor_attachment_state,
                   supervisor_attachment.lease_expires_at AS supervisor_attachment_lease_expires_at,
                   instruction.sequence AS instruction_sequence,
                   instruction.event_type AS instruction_event_type,
                   instruction.aggregate_type AS instruction_aggregate_type,
                   instruction.aggregate_id AS instruction_aggregate_id,
                   instruction.data_json AS instruction_data_json,
                   (SELECT COUNT(*) FROM events AS exact_instruction
                    WHERE exact_instruction.causation_id = message.id
                      AND exact_instruction.actor_id = work.supervisor_id
                      AND json_extract(
                            exact_instruction.data_json,
                            '$.provider_source_boundary_id'
                          ) = ?
                      AND (
                           (exact_instruction.event_type = 'work.assigned'
                            AND exact_instruction.aggregate_type = 'work_item'
                            AND exact_instruction.aggregate_id = work.id)
                        OR (exact_instruction.event_type = 'work.goal_replaced'
                            AND exact_instruction.aggregate_type = 'work_item'
                            AND exact_instruction.aggregate_id = work.id)
                        OR (exact_instruction.event_type = 'attempt.created'
                            AND exact_instruction.aggregate_type = 'attempt'
                            AND exact_instruction.aggregate_id = attempt.id)
                      )) AS instruction_count
            FROM attempts AS attempt
            LEFT JOIN messages AS message ON message.id = ?
            LEFT JOIN work_items AS work ON work.id = attempt.work_item_id
            LEFT JOIN cao_session_attachments AS supervisor_attachment
              ON supervisor_attachment.id = work.supervisor_attachment_id
             AND supervisor_attachment.principal_id = work.supervisor_id
            LEFT JOIN message_deliveries AS delivery
              ON delivery.message_id = message.id
             AND delivery.recipient_id = attempt.worker_id
            LEFT JOIN runtime_sessions AS runtime
              ON runtime.id = attempt.runtime_session_id
            LEFT JOIN worker_enrollments AS enrollment
              ON enrollment.runtime_session_id = runtime.id
            LEFT JOIN events AS instruction
             ON instruction.sequence = ?
             AND instruction.causation_id = message.id
             AND instruction.actor_id = work.supervisor_id
             AND json_extract(
                   instruction.data_json,
                   '$.provider_source_boundary_id'
                 ) = ?
            WHERE attempt.id = ?
            """,
            (
                source_boundary_id,
                probe_message_id,
                circuit["probe_instruction_sequence"],
                source_boundary_id,
                probe_attempt_id,
            ),
        ).fetchone()
        probe_spec = connection.execute(
            """
            SELECT spec.id AS spec_id, spec.principal_id, spec.adapter, spec.effective_model,
                   spec.state AS spec_state,
                   spec.provider_scope_digest, spec.attachment_id,
                   spec.attachment_generation, spec.catalog_target_id,
                   spec.workspace_ref,
                   runtime.principal_id AS runtime_principal_id,
                   enrollment.principal_id AS enrollment_principal_id,
                   attachment.principal_id AS attachment_principal_id,
                   attachment.project_scope_digest AS project_digest,
                   attachment.generation AS current_attachment_generation,
                   attachment.state AS attachment_state,
                   attachment.lease_expires_at AS attachment_lease_expires_at
            FROM managed_worker_specs AS spec
            JOIN runtime_sessions AS runtime
              ON runtime.id = spec.runtime_session_id
            JOIN worker_enrollments AS enrollment
              ON enrollment.id = spec.enrollment_id
             AND enrollment.runtime_session_id = runtime.id
            JOIN cao_session_attachments AS attachment
              ON attachment.id = spec.attachment_id
            WHERE spec.runtime_session_id = ?
            """,
            (probe_runtime_id,),
        ).fetchone()
        source_attempt_number = (
            int(source["attempt_number"])
            if source is not None and source["attempt_number"] is not None
            else None
        )
        instruction_data = (
            _json_object(str(probe["instruction_data_json"]))
            if probe is not None and probe["instruction_data_json"] is not None
            else None
        )
        same_work_retry = bool(
            probe is not None
            and source_attempt_number is not None
            and str(probe["work_item_id"]) == source_work_id
            and int(probe["attempt_number"]) > source_attempt_number
        )
        fresh_work_instruction = bool(
            probe is not None
            and str(probe["work_item_id"]) != source_work_id
            and int(probe["attempt_number"]) == 1
        )
        same_worker_lineage = bool(
            source_spec is not None
            and probe_spec is not None
            and str(probe_spec["attachment_id"]) == str(source_spec["attachment_id"])
            and int(probe_spec["attachment_generation"])
            == int(source_spec["attachment_generation"])
            and (
                str(probe_spec["spec_id"]) == str(source_spec["spec_id"])
                or (
                    str(source_spec["attachment_state"]) == "active"
                    and int(source_spec["generation"]) == int(source_spec["attachment_generation"])
                )
            )
        )
        takeover_sequence = (
            None
            if same_worker_lineage or probe_spec is None
            else _provider_closed_source_takeover_sequence(
                connection,
                source_runtime_id=source_runtime_id,
                source_boundary_id=source_boundary_id,
                source_work_id=source_work_id,
                source_boundary_sequence=int(circuit["source_boundary_sequence"] or 0),
                candidate_attachment_id=str(probe_spec["attachment_id"]),
                comparison_time=comparison_time,
            )
        )
        instruction_floor = (
            int(circuit["source_boundary_sequence"] or 0)
            if same_worker_lineage
            else int(takeover_sequence or 0)
        )
        attachment_lineage_valid = same_worker_lineage or takeover_sequence is not None
        instruction_event_valid = bool(
            probe is not None
            and instruction_data is not None
            and instruction_data.get("provider_source_boundary_id") == source_boundary_id
            and int(probe["instruction_count"] or 0) == 1
            and int(probe["instruction_sequence"] or 0)
            == int(circuit["probe_instruction_sequence"] or 0)
            and int(probe["instruction_sequence"] or 0) > instruction_floor
            and (
                (
                    str(probe["instruction_event_type"] or "")
                    in {"work.assigned", "work.goal_replaced"}
                    and str(probe["instruction_aggregate_type"] or "") == "work_item"
                    and str(probe["instruction_aggregate_id"] or "") == str(probe["work_item_id"])
                )
                or (
                    str(probe["instruction_event_type"] or "") == "attempt.created"
                    and str(probe["instruction_aggregate_type"] or "") == "attempt"
                    and str(probe["instruction_aggregate_id"] or "") == probe_attempt_id
                )
            )
        )
        probe_outcome_valid = bool(
            probe is not None
            and (
                (
                    str(circuit["state"]) == "half_open"
                    and str(circuit["probe_outcome_state"]) == "active"
                    and str(probe["delivery_state"] or "") in {"dispatched", "acknowledged"}
                    and str(probe["runtime_state"] or "") == "busy"
                    and str(probe["enrollment_state"] or "") in {"awaiting_handshake", "ready"}
                )
                or (
                    str(circuit["state"]) == "blocked"
                    and str(circuit["probe_outcome_state"]) == "unknown"
                    and str(probe["delivery_state"] or "")
                    in {"dispatched", "delivered", "acknowledged"}
                    and str(probe["runtime_state"] or "")
                    in {"busy", "waiting", "failed", "missing"}
                    and str(probe["enrollment_state"] or "")
                    in {"awaiting_handshake", "ready", "failed", "stale"}
                )
            )
        )
        probe_valid = bool(
            source_valid
            and source_scope_valid
            and probe_runtime_id
            and probe_attempt_id
            and probe_message_id
            and int(circuit["restart_authorized"]) == 0
            and probe_runtime_id != source_runtime_id
            and probe is not None
            and str(probe["runtime_session_id"] or "") == probe_runtime_id
            and (same_work_retry or fresh_work_instruction)
            and attachment_lineage_valid
            and instruction_event_valid
            and str(probe["message_kind"] or "") == "assignment"
            and str(probe["message_work_item_id"] or "") == str(probe["work_item_id"])
            and str(probe["message_attempt_id"] or "") == probe_attempt_id
            and str(probe["message_sender_id"] or "") == str(probe["supervisor_id"] or "")
            and str(probe["assigned_worker_id"] or "") == str(probe["worker_id"])
            and str(probe["delivery_recipient_id"] or "") == str(probe["worker_id"])
            and str(probe["delivery_runtime_session_id"] or "") == probe_runtime_id
            and probe_outcome_valid
            and probe_spec is not None
            and str(probe_spec["spec_state"] or "") == "enabled"
            and str(probe_spec["principal_id"]) == str(probe["worker_id"])
            and str(probe_spec["runtime_principal_id"]) == str(probe_spec["principal_id"])
            and str(probe_spec["enrollment_principal_id"]) == str(probe_spec["principal_id"])
            and str(probe_spec["adapter"]) == "claude"
            and str(probe_spec["provider_scope_digest"]) == circuit_id
            and str(probe_spec["provider_scope_digest"])
            == _provider_scope_digest(
                str(probe_spec["adapter"]), str(probe_spec["effective_model"])
            )
            and str(probe_spec["attachment_principal_id"]) == str(probe["supervisor_id"] or "")
            and str(probe["supervisor_attachment_state"] or "") == "active"
            and str(probe["supervisor_attachment_lease_expires_at"] or "") > comparison_time
            and str(probe_spec["project_digest"]) == str(probe["supervisor_project_digest"] or "")
            and str(probe_spec["project_digest"])
            == str(source_spec["project_digest"] if source_spec is not None else "")
            and str(probe_spec["catalog_target_id"]) != ""
            and str(probe_spec["catalog_target_id"])
            == str(source_spec["catalog_target_id"] if source_spec is not None else "")
            and str(probe_spec["workspace_ref"])
            == str(source_spec["workspace_ref"] if source_spec is not None else "")
        )
        if not probe_valid:
            violations.append(
                ProjectionViolation(
                    "runtime.provider_circuit_half_open_probe_invalid",
                    "provider_runtime_circuit",
                    (circuit_subject_id,),
                )
            )


def _add_rows(
    violations: list[ProjectionViolation],
    connection: sqlite3.Connection,
    code: str,
    subject_type: str,
    sql: str,
    params: tuple[object, ...] = (),
) -> None:
    rows = connection.execute(sql, params).fetchall()
    for row in rows:
        violations.append(ProjectionViolation(code, subject_type, (str(row["id"]),)))


def _count(connection: sqlite3.Connection, table: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
    assert row is not None
    return int(row["count"])


def _count_where(
    connection: sqlite3.Connection,
    table: str,
    clause: str,
    params: tuple[object, ...] = (),
) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM {table} WHERE {clause}", params
    ).fetchone()
    assert row is not None
    return int(row["count"])


def _group_counts(connection: sqlite3.Connection, table: str, column: str) -> dict[str, int]:
    rows = connection.execute(
        f"SELECT {column}, COUNT(*) AS count FROM {table} GROUP BY {column} ORDER BY {column}"
    ).fetchall()
    return {str(row[column]): int(row["count"]) for row in rows}
