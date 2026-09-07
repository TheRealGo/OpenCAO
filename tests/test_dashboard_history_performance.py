from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from threading import Event

import httpx
import pytest
from test_dashboard_read_model import _dashboard_app
from test_dashboard_reading import _assign, _report

from cao_control_plane import api, dashboard, projection
from cao_control_plane.dashboard import DashboardReadModel
from cao_control_plane.dashboard_history import history_reference
from cao_control_plane.models import ReviewInput


@pytest.mark.parametrize("kind", ["progress", "blocker", "completion_claim"])
def test_selected_work_matches_the_canonical_snapshot(system, monkeypatch, kind):
    work = _assign(system)
    _report(system, work, "Complete report\nWith another paragraph", kind=kind)
    if kind == "completion_claim":
        system["service"].review(
            system["cao"],
            ReviewInput(
                attempt_id=work["current_attempt"]["id"], verdict="ok", summary="Verified"
            ),
        )
    _assign(system, title="Unrelated Work")
    monkeypatch.setattr(projection, "_utc_now", lambda: "2026-01-01T00:00:00Z")
    model = DashboardReadModel(system["service"])
    full = model.snapshot()["operator"]
    reference = history_reference("work", work["id"])
    expected = next(
        item for item in full["work_items"] + full["recently_completed"]
        if item["history_reference"] == reference
    )
    selected = model.work_history(work=reference)["work"]
    assert selected == expected | {"display_label": "Work item 1"}


@pytest.mark.parametrize("unrelated_count", [0, 24])
def test_history_projects_only_the_requested_page_without_global_audits(
    system, monkeypatch, unrelated_count
):
    selected = _assign(system, title="Selected")
    for index in range(unrelated_count):
        _assign(system, title=f"Unrelated {index}")
    projected_attempts = []
    original = projection._operator_attempt_activity_projection

    def observe(connection, **kwargs):
        projected_attempts.append(kwargs["attempt_id"])
        return original(connection, **kwargs)

    def forbidden(*args, **kwargs):
        pytest.fail("A history read rebuilt global state")

    monkeypatch.setattr(projection, "_operator_attempt_activity_projection", observe)
    for name in ("_snapshot", "_violations", "_watermark"):
        monkeypatch.setattr(projection, name, forbidden)
    model = DashboardReadModel(system["service"])
    model.work_history(work=history_reference("work", selected["id"]))
    assert projected_attempts == [selected["current_attempt"]["id"]]
    projected_attempts.clear()
    page = model.work_history(limit=2)
    assert len(projected_attempts) == len(page["items"]) == min(unrelated_count + 1, 2)


def test_work_and_exchanges_share_one_snapshot_and_next_read_is_fresh(system, monkeypatch):
    work = _assign(system)
    _report(system, work, "Before the read")
    original = dashboard.project_work_items_from_connection
    updated = False

    def concurrent_report(connection, **kwargs):
        nonlocal updated
        result = original(connection, **kwargs)
        if not updated:
            updated = True
            _report(system, work, "Committed during the read")
        return result

    monkeypatch.setattr(dashboard, "project_work_items_from_connection", concurrent_report)
    model = DashboardReadModel(system["service"])
    reference = history_reference("work", work["id"])
    first = model.work_history(work=reference)
    assert first["work"]["latest_report_text"] == "Before the read"
    assert first["entries"][-1]["text"] == "Before the read"
    second = model.work_history(work=reference)
    assert second["work"]["latest_report_text"] == "Committed during the read"
    assert second["entries"][-1]["text"] == "Committed during the read"


def test_selected_close_proof_checks_uniqueness_across_unselected_work(monkeypatch):
    verified = []

    def verify(connection, receipt, private_state):
        verified.append(receipt["id"])
        return True

    monkeypatch.setattr(projection, "_verify_close_receipt_semantics", verify)
    with closing(sqlite3.connect(":memory:")) as connection, connection:
        connection.row_factory = sqlite3.Row
        connection.execute(
            "CREATE TABLE work_close_receipts(id, work_item_id, requester_decision_id)"
        )
        connection.executemany(
            "INSERT INTO work_close_receipts VALUES (?, ?, ?)",
            [("receipt-a", "work-a", "decision-a"), ("receipt-b", "work-b", "decision-b")],
        )
        result = projection._close_receipt_semantics(
            connection, None, work_ids=frozenset({"work-a"})
        )
        assert result.verified_receipts_by_work == {"work-a": "receipt-a"}
        assert verified == ["receipt-a"]
        connection.execute(
            "UPDATE work_close_receipts SET requester_decision_id = 'decision-a' "
            "WHERE id = 'receipt-b'"
        )
        result = projection._close_receipt_semantics(
            connection, None, work_ids=frozenset({"work-a"})
        )
        assert result.verified_receipts_by_work == {}
        assert result.violations[0].subject_ids == ("receipt-a",)
        assert verified == ["receipt-a"]


@pytest.mark.parametrize("surface", ["ready", "snapshot", "history", "stream"])
def test_slow_global_reads_do_not_hold_up_work_selection(system, monkeypatch, surface):
    work = _assign(system)
    app, token = _dashboard_app(system)
    model = DashboardReadModel(app.state.service)
    cursor = model.snapshot()["cursor"]
    entered, release, expired = Event(), Event(), Event()
    target = api if surface == "ready" else DashboardReadModel
    method = "build_projection" if surface == "ready" else (
        "history" if surface == "stream" else surface
    )
    original = getattr(target, method)

    def held_read(*args, **kwargs):
        entered.set()
        if not release.wait(3):
            expired.set()
        return original(*args, **kwargs)

    monkeypatch.setattr(target, method, held_read)

    async def scenario():
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost", headers=headers
        ) as client:
            stream = model.stream(after=cursor, heartbeat_seconds=60)
            path = "/ready" if surface == "ready" else f"/api/v1/dashboard/v1/{surface}"
            slow = asyncio.create_task(anext(stream) if surface == "stream" else client.get(path))
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                selected = await asyncio.wait_for(
                    client.get(
                        "/api/v1/dashboard/v1/work-history",
                        params={"work": history_reference("work", work["id"])},
                    ),
                    timeout=2,
                )
                assert selected.status_code == 200
                assert selected.json()["work"]["history_reference"] == history_reference(
                    "work", work["id"]
                )
                # Complete the selected read while the competing full read
                # is still deliberately held, rather than asserting a speed.
                assert not expired.is_set()
                assert not slow.done()
            finally:
                release.set()
                await slow
                await stream.aclose()

    asyncio.run(scenario())
