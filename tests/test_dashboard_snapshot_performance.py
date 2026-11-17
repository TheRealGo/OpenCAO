from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from conftest import enroll_worker_runtime
from test_dashboard_reading import _assign, _report

from cao_control_plane import projection
from cao_control_plane.dashboard import DashboardReadModel, build_operator_view
from cao_control_plane.models import (
    BoundaryDispositionInput,
    PrincipalCreate,
    ReviewInput,
    WorkAssignment,
)


@pytest.mark.parametrize("kind", ["progress", "blocker", "completion_claim"])
def test_live_snapshot_matches_canonical_operator_fields(system, monkeypatch, kind):
    work = _assign(system)
    _report(system, work, "Full current report\nSecond paragraph", kind=kind)
    if kind == "completion_claim":
        system["service"].review(
            system["cao"],
            ReviewInput(attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Verified"),
        )
    monkeypatch.setattr(projection, "_utc_now", lambda: "2026-01-01T00:00:00Z")
    expected = build_operator_view(projection.build_projection(system["service"].db).snapshot)
    actual = DashboardReadModel(system["service"]).snapshot()
    assert actual["operator"] == expected
    assert actual["projection"]["healthy"] is None
    assert actual["projection"]["integrity_check"] == "not-performed"


@pytest.mark.parametrize("hidden_count", [0, 24])
def test_snapshot_projects_only_visible_work_and_never_runs_global_audits(
    system, monkeypatch, hidden_count
):
    service = system["service"]
    selected = _assign(system)
    created = service.create_principal(
        system["cao"], PrincipalCreate(name="hidden-worker", role="worker", metadata={})
    )
    hidden_worker = service.authenticate(created["token"])
    enroll_worker_runtime(service, system["cao"], hidden_worker["id"])
    for index in range(hidden_count):
        service.assign_work(
            system["cao"],
            WorkAssignment(worker_id=hidden_worker["id"], title=f"Hidden {index}",
                           objective="Unrelated retained work", acceptance=["Retained"]),
        )
    projected = []
    original = projection.project_work_items_from_connection

    def observe(connection, **kwargs):
        projected.extend(kwargs["work_ids"])
        return original(connection, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("A live Dashboard read ran a global audit or full-state reconstruction")

    monkeypatch.setattr(projection, "project_work_items_from_connection", observe)
    for name in ("build_projection", "_snapshot", "_violations"):
        monkeypatch.setattr(projection, name, forbidden)
    original_connect = service.db.connect

    def authorize(action, name, *_):
        if action == sqlite3.SQLITE_PRAGMA and name in {
            "integrity_check", "quick_check", "foreign_key_check"
        }:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def connect():
        connection = original_connect()
        connection.set_authorizer(authorize)
        return connection

    monkeypatch.setattr(service.db, "connect", connect)
    body = DashboardReadModel(service).snapshot()
    assert projected == [selected["id"]]
    assert body["operator"]["counts"]["current_work_items"] == 1


def test_snapshot_with_no_production_workers_stays_available(system, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("An empty Dashboard must not need a full integrity projection")

    monkeypatch.setattr(projection, "build_projection", forbidden)
    body = DashboardReadModel(system["service"]).snapshot()
    assert body["operator"]["counts"]["current_work_items"] == 0
    for group in ("cao_processing", "user_confirmation", "stopped_or_failed", "working",
                  "ready", "inactive_workers", "recently_completed", "work_items"):
        assert body["operator"][group] == []


def test_snapshot_state_and_cursor_share_one_read_and_next_read_is_fresh(system, monkeypatch):
    work = _assign(system)
    _report(system, work, "Before read")
    model = DashboardReadModel(system["service"])
    before = model.snapshot()
    original = projection.project_work_items_from_connection
    committed = False

    def concurrent_report(connection, **kwargs):
        nonlocal committed
        result = original(connection, **kwargs)
        if not committed:
            committed = True
            _report(system, work, "Committed during read")
        return result

    monkeypatch.setattr(projection, "project_work_items_from_connection", concurrent_report)
    first = model.snapshot()
    assert committed
    assert first["operator"]["work_items"][0]["latest_report_text"] == "Before read"
    assert first["cursor"] == before["cursor"]
    second = model.snapshot()
    assert second["operator"]["work_items"][0]["latest_report_text"] == "Committed during read"
    assert second["cursor"] != first["cursor"]


def test_visibility_does_not_claim_health_when_authority_is_invalid(system):
    system["service"].db.execute("DELETE FROM control_authority")
    body = DashboardReadModel(system["service"]).snapshot()
    assert body["projection"]["healthy"] is False
    assert body["projection"]["violations"][0]["code"] == "authority.mode_invalid"


def _settle(system, work):
    service = system["service"]
    reported = _report(system, work, "Verified result", kind="completion_claim")
    service.review(
        system["cao"],
        ReviewInput(attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Verified"),
    )
    boundary = reported["open_boundaries"][0]
    turn = service.acquire_reasoner_turn(
        system["cao"], work["id"], expected_generation=reported["generation"],
        idempotency_key=f"snapshot-turn-{work['id']}",
    )
    service.dispose_boundary(
        system["cao"], boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"], lease_token=turn["lease_token"],
            expected_generation=reported["generation"], kind="accept", reason="Verified result",
        ),
    )


@pytest.mark.parametrize("history_count", [25, 80])
def test_visible_worker_history_enriches_only_current_and_recent_with_stable_labels(
    system, monkeypatch, history_count
):
    service = system["service"]
    for index in range(history_count):
        work = _assign(system, title=f"Completed {index}")
        _settle(system, work)
    canceled = _assign(system, title="Canceled retained work")
    service.db.execute("UPDATE work_items SET state = 'canceled' WHERE id = ?", (canceled["id"],))
    service.db.execute(
        "UPDATE attempts SET state = 'canceled' WHERE id = ?", (canceled["current_attempt"]["id"],)
    )
    current = _assign(system, title="Current work")
    _report(system, current, "Still advancing")
    # Identical timestamps exercise the canonical lexical ordinal tie break.
    service.db.execute("UPDATE work_items SET updated_at = '2026-01-01T00:00:00Z'")
    monkeypatch.setattr(projection, "_utc_now", lambda: "2026-01-01T00:00:00Z")
    expected = build_operator_view(projection.build_projection(service.db).snapshot)
    projected = []
    original = projection.project_work_items_from_connection

    def observe(connection, **kwargs):
        projected.extend(kwargs["work_ids"])
        return original(connection, **kwargs)

    monkeypatch.setattr(projection, "project_work_items_from_connection", observe)
    actual = DashboardReadModel(service).snapshot()["operator"]
    assert actual == expected
    assert len(actual["recently_completed"]) == 20
    assert len(projected) == 21
    assert current["id"] in projected
    assert canceled["id"] not in projected
    assert actual["work_items"][0]["work_title"] == "Current work"


@pytest.mark.parametrize("review", ["missing", "stale", "pending-boundary"])
def test_unsettled_completion_remains_current(system, monkeypatch, review):
    service = system["service"]
    work = _assign(system)
    if review == "pending-boundary":
        _report(system, work, "Needs disposition", kind="completion_claim")
        service.review(
            system["cao"],
            ReviewInput(attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Verified"),
        )
    else:
        if review == "stale":
            _settle(system, work)
            service.db.execute(
                "UPDATE work_items SET generation = generation + 1 WHERE id = ?", (work["id"],)
            )
        service.db.execute(
            "UPDATE work_items SET state = 'waiting_user' WHERE id = ?", (work["id"],)
        )
        service.db.execute(
            "UPDATE attempts SET state = 'completed' WHERE id = ?", (work["current_attempt"]["id"],)
        )
    monkeypatch.setattr(projection, "_utc_now", lambda: "2026-01-01T00:00:00Z")
    expected = build_operator_view(projection.build_projection(service.db).snapshot)
    actual = DashboardReadModel(service).snapshot()["operator"]
    assert actual == expected
    assert len(actual["work_items"]) == 1
    assert actual["recently_completed"] == []


def test_selected_close_read_does_not_load_unselected_receipt_bodies(monkeypatch):
    bodies = []

    def row_factory(cursor, values):
        row = sqlite3.Row(cursor, values)
        if any(column[0] == "payload" for column in cursor.description):
            bodies.append(row["work_item_id"])
        return row

    monkeypatch.setattr(projection, "_verify_close_receipt_semantics", lambda *args: True)
    with closing(sqlite3.connect(":memory:")) as connection, connection:
        connection.row_factory = row_factory
        connection.execute(
            "CREATE TABLE work_close_receipts(id, work_item_id, requester_decision_id, payload)"
        )
        connection.executemany(
            "INSERT INTO work_close_receipts VALUES (?, ?, ?, ?)",
            [(f"receipt-{i}", f"work-{i}", f"decision-{i}", "retained payload") for i in range(500)],
        )
        result = projection._close_receipt_semantics(
            connection, None, work_ids=frozenset({"work-0"})
        )
        assert result.verified_receipts_by_work == {"work-0": "receipt-0"}
        assert bodies == ["work-0"]
        connection.execute(
            "UPDATE work_close_receipts SET requester_decision_id = 'decision-0' WHERE id = 'receipt-1'"
        )
        result = projection._close_receipt_semantics(
            connection, None, work_ids=frozenset({"work-0"})
        )
        assert result.verified_receipts_by_work == {}
        assert bodies == ["work-0", "work-0"]


def test_completion_selection_preserves_valid_requester_review_fallback(system, monkeypatch):
    from test_close_persistence_invariants import _completed_accepted_work
    from test_dashboard_read_model import _production_worker

    service = system["service"]
    _production_worker(system)
    work, _actor = _completed_accepted_work(system)
    # A valid requester decision can retain its earlier review after a Work
    # generation changes. The canonical closure DTO uses that bound review.
    service.db.execute(
        "UPDATE work_items SET generation = generation + 1 WHERE id = ?", (work["id"],)
    )
    service.db.execute(
        "UPDATE requester_decisions SET work_generation = work_generation + 1 WHERE work_item_id = ?",
        (work["id"],),
    )
    monkeypatch.setattr(projection, "_utc_now", lambda: "2026-01-01T00:00:00Z")
    expected = build_operator_view(projection.build_projection(service.db).snapshot)
    actual = DashboardReadModel(service).snapshot()["operator"]
    assert actual == expected
    assert actual["work_items"] == []
    assert len(actual["recently_completed"]) == 1
