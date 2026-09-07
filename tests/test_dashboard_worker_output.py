from __future__ import annotations

import json
from typing import Any

import pytest
from test_worker_output_delivery import _capture_fixture, _event, _read_request, _terminal

from cao_control_plane.dashboard import DashboardReadModel, build_operator_view
from cao_control_plane.dashboard_edge import _edge_snapshot, render_dashboard_snapshot
from cao_control_plane.models import ReportInput
from cao_control_plane.projection import build_projection

_PRIVATE_OUTPUT = "PRIVATE_OUTPUT_BODY_SENTINEL: the retained result needs independent review."


def _production_capture(system: dict[str, Any]):
    system["service"].db.execute(
        "UPDATE principals SET operator_scope = 'production', operator_label = ? WHERE id = ?",
        ("Output fixture Worker", system["worker"]["id"]),
    )
    return _capture_fixture(system)


def _report(system: dict[str, Any], work: dict[str, Any], *, kind: str, summary: str):
    attempt = work["current_attempt"]
    return system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=kind,
            expected_goal_version=work["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            expected_generation=work["generation"],
            summary=summary,
            trajectory="complete" if kind == "completion_claim" else "advancing",
            idempotency_key=f"dashboard:structured:{kind}",
        ),
    )


@pytest.mark.parametrize("settled", [False, True], ids=["available", "terminal"])
def test_dashboard_projects_automatic_output_metadata_without_any_model_report(
    system, settled: bool
) -> None:
    service = system["service"]
    actor, work, binding = _production_capture(system)
    output = service.observe_worker_output(**binding, event=_event(text=_PRIVATE_OUTPUT))
    assert output["capture_state"] == "available"
    assert (
        service.read_worker_output(actor, _read_request(work, output))["content"] == _PRIVATE_OUTPUT
    )
    latest = _terminal(service, binding) if settled else output
    notification = service.db.fetchone(
        "SELECT payload_json, created_at FROM messages WHERE id = ?",
        (latest["notification_message_id"],),
    )
    assert notification is not None
    expected_summary = json.loads(notification["payload_json"])["summary"]

    projection = build_projection(service.db).snapshot
    operator = build_operator_view(projection)
    dashboard = DashboardReadModel(service).snapshot()
    edge = _edge_snapshot(dashboard)
    for view in (operator, dashboard["operator"], edge["operator"]):
        item = view["work_items"][0]
        assert item["latest_report_kind"] == "worker_output"
        assert item["latest_report_summary"] == expected_summary
        assert item["latest_reported_at"] == notification["created_at"]
        assert item["pending_supervisor_boundary"] is settled
        assert item["state"] == ("waiting_supervisor" if settled else "active")
        assert item["progress_stage"] is None
        assert item["next_boundary_summary"] is None

    assert _PRIVATE_OUTPUT not in json.dumps(projection)
    for public_view in (operator, dashboard, edge):
        rendered = json.dumps(public_view)
        for forbidden in (
            _PRIVATE_OUTPUT,
            output["id"],
            output["digest"],
            "output-provider-thread",
            "output-provider-turn",
            "owner-private-artifact:",
        ):
            assert forbidden not in rendered
    assert _PRIVATE_OUTPUT not in render_dashboard_snapshot(dashboard)
    current = service.get_work(work["id"])
    assert current["current_attempt"]["completion_claim"] == {}
    assert current["reviews"] == []
    assert (
        service.db.fetchone(
            "SELECT 1 FROM messages WHERE attempt_id = ? AND kind IN "
            "('progress', 'question', 'blocker', 'artifact', 'completion_claim')",
            (binding["attempt_id"],),
        )
        is None
    )


def test_later_structured_report_keeps_ordering_after_automatic_output(system) -> None:
    service = system["service"]
    _, work, binding = _production_capture(system)
    service.observe_worker_output(**binding, event=_event(text=_PRIVATE_OUTPUT))
    summary = "A later explicit progress report is available."
    _report(system, work, kind="progress", summary=summary)

    item = build_operator_view(build_projection(service.db).snapshot)["work_items"][0]

    assert item["latest_report_kind"] == "progress"
    assert item["latest_report_summary"] == summary
    assert _PRIVATE_OUTPUT not in json.dumps(item)


def test_reused_structured_completion_notification_keeps_its_original_report_kind(system) -> None:
    service = system["service"]
    _, work, binding = _production_capture(system)
    service.observe_worker_output(**binding, event=_event(text=_PRIVATE_OUTPUT))
    summary = "The optional structured result is ready for independent review."
    reported = _report(system, work, kind="completion_claim", summary=summary)
    terminal = _terminal(service, binding)
    notification = service.db.fetchone(
        "SELECT kind FROM messages WHERE id = ?", (terminal["notification_message_id"],)
    )
    assert notification["kind"] == "completion_claim"
    assert terminal["boundary_id"] == reported["open_boundaries"][0]["id"]

    dashboard = DashboardReadModel(service).snapshot()
    for view in (dashboard["operator"], _edge_snapshot(dashboard)["operator"]):
        item = view["work_items"][0]
        assert item["latest_report_kind"] == "completion_claim"
        assert item["latest_report_summary"] == summary
        assert item["pending_supervisor_boundary"] is True
        assert item["state"] == "waiting_supervisor"
    assert _PRIVATE_OUTPUT not in json.dumps(dashboard)
