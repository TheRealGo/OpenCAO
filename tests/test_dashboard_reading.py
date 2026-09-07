from __future__ import annotations

import json
from contextlib import closing

import httpx
import pytest
from fastapi.testclient import TestClient
from test_dashboard_edge import _bootstrap, _settings
from test_dashboard_read_model import _dashboard_app, _managed_dashboard_item, _production_worker

from cao_control_plane.dashboard import DashboardReadModel
from cao_control_plane.dashboard_edge import (
    _edge_snapshot,
    _edge_work_history,
    create_dashboard_edge,
)
from cao_control_plane.dashboard_history import full_operator_text, history_reference
from cao_control_plane.models import (
    ReportInput,
    ReviewInput,
    WorkAssignment,
    WorkerThreadLifecycleInput,
)


def _assign(system, title="Reading example", objective="Understand the complete result"):
    _production_worker(system, label="Reading Worker")
    return system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title=title,
            objective=objective,
            acceptance=["Result is readable"],
        ),
    )


def _report(system, work, summary, kind="progress"):
    attempt = work["current_attempt"]
    return system["service"].report(
        system["worker"],
        attempt["id"],
        ReportInput(
            kind=kind,
            expected_generation=work["generation"],
            expected_goal_version=attempt["goal_version"],
            expected_goal_packet_digest=attempt["goal_packet_digest"],
            expected_task_packet_digest=attempt["task_packet_digest"],
            summary=summary,
        ),
    )


def test_full_content_survives_projection_api_and_edge_without_private_tail(system):
    objective = "第一段落です。\n\n" + "目的の詳細。" * 140 + "\n目的の最終段落。"
    report = "報告の冒頭。\n\n" + "検証結果です。" * 300 + "\n最終結論を保持する。"
    private_tail = "\nhttps://private.example.test/token /owner/private/file api_key=DO_NOT_EXPOSE"
    work = _assign(system, objective=objective + private_tail)
    _report(system, work, report)
    app, token = _dashboard_app(system)
    with closing(TestClient(app, base_url="http://localhost")) as client:
        response = client.get(
            "/api/v1/dashboard/v1/snapshot", headers={"Authorization": f"Bearer {token}"}
        )
    assert response.status_code == 200
    snapshot = response.json()
    for body in (snapshot, _edge_snapshot(snapshot)):
        item = body["operator"]["work_items"][0]
        assert objective in item["objective_text"]
        assert report in item["latest_report_text"]
        assert len(item["objective_summary"]) <= 280
        assert item["history_reference"] == history_reference("work", work["id"])
        serialized = json.dumps(body)
        for private in ("DO_NOT_EXPOSE", "private.example.test", "/owner/private/file", work["id"]):
            assert private not in serialized
    # The former 512 KiB edge ceiling must not turn a long report into an outage.
    large = snapshot["operator"]["work_items"][0].copy()
    large["latest_report_text"] = "長い報告\n" * 40000 + "末尾まで読む"
    snapshot["operator"]["working"] = [
        {"worker_label": "Reading Worker", "current_work_items": [large]}
    ]
    edge = create_dashboard_edge(
        _settings(
            [], transport=httpx.MockTransport(lambda request: httpx.Response(200, json=snapshot))
        )
    )
    with TestClient(edge, base_url="http://localhost") as client:
        _bootstrap(client)
        response = client.get("/dashboard/api/snapshot")
    assert response.status_code == 200
    assert response.json()["operator"]["working"][0]["current_work_items"][0][
        "latest_report_text"
    ].endswith("末尾まで読む")


def test_work_reader_pages_all_exchanges_and_preserves_prior_reports(system):
    work = _assign(system)
    for index in range(4):
        _report(system, work, f"Progress {index}\n" + "Detail " * 70)
    _report(system, work, "The final report", kind="completion_claim")
    system["service"].review(
        system["cao"],
        ReviewInput(
            attempt_id=work["current_attempt"]["id"],
            verdict="ok",
            summary="Independent review reasoning",
        ),
    )
    model = DashboardReadModel(system["service"])
    reference = history_reference("work", work["id"])
    newest = model.work_history(work=reference, limit=2)
    entries = newest["entries"]
    page = newest
    while page["has_more"]:
        page = model.work_history(work=reference, before=page["next_before"], limit=2)
        entries = page["entries"] + entries
    assert [entry["kind"] for entry in entries] == [
        "goal",
        *["progress"] * 4,
        "completion_claim",
        "review",
    ]
    assert len({entry["reference"] for entry in entries}) == 7
    assert all(f"Progress {index}" in entries[index + 1]["text"] for index in range(4))
    assert entries[-1]["text"] == "Independent review reasoning"
    edge = _edge_work_history(newest | {"metadata": "not forwarded"})
    assert edge["entries"] == newest["entries"]
    assert "metadata" not in edge


def test_history_retains_archived_work_without_reactivating_worker(system):
    _item, attachment = _managed_dashboard_item(
        system, adapter="claude", model="opus", effort="high"
    )
    model = DashboardReadModel(system["service"])
    reference = model.work_history()["items"][0]["history_reference"]
    service = system["service"]
    actor = service.authenticate(attachment["context_token"])
    service.finish_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id="thread_dashboard_claude",
            expected_generation=1,
            idempotency_key="reader-finish",
        ),
    )
    assert model.snapshot()["operator"]["work_items"] == []
    assert model.work_history()["items"][0]["history_reference"] == reference
    assert model.work_history(work=reference)["entries"][0]["kind"] == "goal"
    service.delete_worker_thread(
        actor,
        WorkerThreadLifecycleInput(
            worker_thread_id="thread_dashboard_claude",
            expected_generation=2,
            idempotency_key="reader-delete",
        ),
    )
    assert model.work_history(work=reference)["entries"][0]["kind"] == "goal"


def test_work_index_has_stable_pagination_and_exact_selection(system):
    first = _assign(system, title="First")
    second = _assign(system, title="Second")
    model = DashboardReadModel(system["service"])
    page = model.work_history(limit=1)
    next_page = model.work_history(before=page["next_before"], limit=1)
    references = [item["history_reference"] for item in page["items"] + next_page["items"]]
    assert set(references) == {history_reference("work", work["id"]) for work in (first, second)}
    assert not next_page["has_more"]
    for work in (first, second):
        selected = model.work_history(work=history_reference("work", work["id"]))
        assert selected["work"]["work_title"] == work["title"]


def test_work_history_auth_scope_and_input_validation(system):
    # Scope is sealed at assignment; a non-production Work is never promoted.
    work = system["service"].assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Hidden acceptance work",
            objective="Remain outside the Dashboard",
            acceptance=["Remains hidden"],
        ),
    )
    app, token = _dashboard_app(system)
    path = "/api/v1/dashboard/v1/work-history"
    with closing(TestClient(app, base_url="http://localhost")) as client:
        assert client.get(path).status_code == 401
        assert (
            client.get(
                path, headers={"Authorization": f"Bearer {system['worker_token']}"}
            ).status_code
            == 403
        )
        headers = {"Authorization": f"Bearer {token}"}
        assert client.get(path, headers=headers).status_code == 200
        assert client.get(path, params={"work": "invalid"}, headers=headers).status_code == 422
        assert client.get(path, params={"limit": 101}, headers=headers).status_code == 422
        reference = history_reference("work", work["id"])
        assert client.get(path, headers=headers).json()["items"] == []
        assert client.get(path, params={"work": reference}, headers=headers).status_code == 404
    calls = []
    edge = create_dashboard_edge(
        _settings(
            calls,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"format": "cao-dashboard-read-model/v1", "items": []}
                )
            ),
        )
    )
    with TestClient(edge, base_url="http://localhost") as client:
        assert client.get("/dashboard/api/work-history").status_code == 401
        _bootstrap(client)
        assert client.get("/dashboard/api/work-history", params={"work": "bad"}).status_code == 400
        assert client.get("/dashboard/api/work-history").status_code == 200


@pytest.mark.parametrize("configured_timeout", [12.0, 30.0, 45.0])
def test_retained_history_has_a_separate_bounded_read_budget(configured_timeout):
    observed = {}

    def respond(request):
        observed[request.url.path] = request.extensions["timeout"]["read"]
        return httpx.Response(200, json={"format": "cao-dashboard-read-model/v1", "items": []})

    edge = create_dashboard_edge(_settings(
        [], read_timeout_seconds=configured_timeout, transport=httpx.MockTransport(respond)
    ))
    with TestClient(edge, base_url="http://localhost") as client:
        _bootstrap(client)
        assert client.get("/dashboard/api/work-history").status_code == 200
        assert client.get("/dashboard/api/history").status_code == 200
    assert observed["/api/v1/dashboard/v1/work-history"] == min(configured_timeout, 30.0)
    assert observed["/api/v1/dashboard/v1/history"] == min(configured_timeout, 15.0)


def test_history_edge_rejects_untyped_content_and_preserves_safe_lines():
    body = {
        "format": "cao-dashboard-read-model/v1",
        "entries": [
            {
                "reference": "a" * 64,
                "kind": "worker_output",
                "text": "Safe\n" + "Detail " * 100 + "secret=HIDDEN",
                "occurred_at": "2026-09-01T00:00:00Z",
                "raw_output": "HIDDEN",
                "outcome": "unapproved",
            },
            {"reference": "b" * 64, "kind": "runtime_transcript", "text": "HIDDEN"},
            {"reference": "c" * 64, "kind": ["progress"], "text": "HIDDEN"},
        ],
    }
    result = _edge_work_history(body)
    assert len(result["entries"]) == 1
    assert result["entries"][0]["text"] == full_operator_text(body["entries"][0]["text"])
    assert result["entries"][0]["outcome"] is None
    assert "HIDDEN" not in json.dumps(result)
