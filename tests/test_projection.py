from __future__ import annotations

import json

from cao_control_plane.database import utc_now
from cao_control_plane.models import ReportInput, SubmittedIntent, WorkAssignment
from cao_control_plane.projection import build_projection, verify_projection

AS_OF = utc_now()


def _assign(system):
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Sensitive title must not appear",
            objective="Sensitive objective must not appear",
            acceptance=["Sensitive acceptance must not appear"],
            metadata={"secret": "sensitive metadata must not appear"},
        ),
    )


def test_projection_is_deterministic_kernel_only_and_sanitized(system):
    work = _assign(system)

    first = build_projection(system["service"].db, as_of=AS_OF)
    second = verify_projection(system["service"].db, as_of=AS_OF)

    assert first.as_dict() == second.as_dict()
    assert first.healthy is True
    assert first.snapshot["records"]["work_items"] == 1
    assert first.snapshot["records"]["submitted_intents"] == 1
    assert first.snapshot["records"]["intent_dispositions"] == 1
    assert first.snapshot["records"]["directives"] == 1
    assert first.snapshot["work_items"][0]["id"] == work["id"]
    assert first.watermark["event_sequence"] > 0
    assert len(first.canonical_digest) == 64

    rendered = json.dumps(first.as_dict(), sort_keys=True)
    assert "Sensitive title must not appear" in rendered
    assert "Sensitive objective must not appear" in rendered
    assert "Sensitive acceptance must not appear" not in rendered
    assert "sensitive metadata must not appear" not in rendered
    assert system["worker_token"] not in rendered


def test_projection_marks_domain_and_scheduler_drift_unhealthy(system):
    work = _assign(system)
    database = system["service"].db
    database.execute(
        "UPDATE work_items SET attention_owner = 'user' WHERE id = ?", (work["id"],)
    )
    database.execute(
        """
        UPDATE message_deliveries
        SET state = 'leased', lease_until = '2000-01-01T00:00:00Z', owner_token = 'lease'
        WHERE message_id = (SELECT id FROM messages ORDER BY sequence LIMIT 1)
        """
    )

    projection = build_projection(database, as_of=AS_OF)
    codes = {violation.code for violation in projection.violations}

    assert projection.healthy is False
    assert "work.attention_owner_mismatch" in codes
    assert "delivery.lease_expired" in codes
    assert projection.snapshot["scheduler"]["expired_delivery_leases"] == 1


def test_projection_detects_unclassified_intent_conservation_failure(system):
    _assign(system)
    database = system["service"].db
    database.execute("DELETE FROM intent_dispositions")

    projection = build_projection(database, as_of=AS_OF)

    assert projection.healthy is False
    assert any(
        violation.code == "intent.without_disposition" for violation in projection.violations
    )


def test_pristine_received_intent_is_visible_pending_not_integrity_drift(system):
    system["service"]._receive_canonical_intent_for_compat(
        system["cao"],
        SubmittedIntent(
            source_id="pending-1",
            payload={"type": "canonical_cao_command", "title": "queued"},
        ),
    )
    projection = build_projection(system["service"].db, as_of=AS_OF)

    assert projection.healthy is True
    assert projection.snapshot["pending_intents"] == 1


def test_worker_progress_wakes_supervisor_but_artifact_only_does_not(system):
    work = _assign(system)
    attempt = work["current_attempt"]
    common = {
        "expected_goal_version": work["goal_version"],
        "expected_generation": work["generation"],
        "expected_goal_packet_digest": attempt["goal_packet_digest"],
        "expected_task_packet_digest": attempt["task_packet_digest"],
    }
    for kind in ("progress", "artifact"):
        system["service"].report(
            system["worker"],
            attempt["id"],
            ReportInput(
                kind=kind,
                summary=f"Structured {kind} update",
                stage="Working",
                next_boundary="Continue work",
                **common,
            ),
        )

    report_deliveries = system["service"].db.fetchall(
        """
        SELECT message.kind, COUNT(delivery.message_id) AS count
        FROM messages message
        LEFT JOIN message_deliveries delivery ON message.id = delivery.message_id
        WHERE message.attempt_id = ?
          AND message.kind IN ('progress', 'artifact')
        GROUP BY message.kind
        ORDER BY message.kind
        """,
        (attempt["id"],),
    )
    projection = verify_projection(system["service"].db)

    assert {row["kind"]: row["count"] for row in report_deliveries} == {
        "artifact": 0,
        "progress": 1,
    }
    assert projection.healthy is True
    assert all(
        violation.code != "message.without_delivery"
        for violation in projection.violations
    )
    system["service"].db.execute(
        """
        DELETE FROM message_deliveries
        WHERE message_id IN (
            SELECT id FROM messages
            WHERE attempt_id = ? AND kind = 'progress'
        )
        """,
        (attempt["id"],),
    )
    historical_projection = verify_projection(system["service"].db)
    assert historical_projection.healthy is True
    assert all(
        violation.code != "message.without_delivery"
        for violation in historical_projection.violations
    )


def test_projection_exposes_only_canonical_authority(system):
    database = system["service"].db
    projection = build_projection(database, as_of=AS_OF)
    assert projection.healthy is True
    assert projection.snapshot["authority"]["mode"] == "canonical"
    assert "legacy_snapshot_bound" not in projection.snapshot["authority"]
    assert "fence_evidence_bound" not in projection.snapshot["authority"]


def test_projection_reports_attempt_sequence_violation_without_raw_data(system):
    work = _assign(system)
    database = system["service"].db
    database.execute(
        "UPDATE attempts SET attempt_number = 2 WHERE work_item_id = ?", (work["id"],)
    )

    projection = build_projection(database, as_of=AS_OF)
    violation = next(
        item for item in projection.violations if item.code == "attempt.sequence_invalid"
    )

    assert projection.healthy is False
    assert violation.subject_ids == (work["id"],)


def test_projection_rejects_goal_packet_with_different_semantic_columns(system):
    work = _assign(system)
    database = system["service"].db
    database.execute(
        "UPDATE goal_revisions SET title = 'different semantic task' "
        "WHERE work_item_id = ? AND version = 1",
        (work["id"],),
    )

    projection = build_projection(database, as_of=AS_OF)
    codes = {violation.code for violation in projection.violations}

    assert projection.healthy is False
    assert "goal.packet_invalid" in codes
    assert "work.current_goal_semantics_mismatch" in codes
