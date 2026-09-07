from __future__ import annotations

import asyncio
import copy
import hashlib
import stat
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient

from cao_control_plane.cloudflare_access import CloudflareAccessSettings
from cao_control_plane.dashboard_edge import (
    DashboardEdgeSettings,
    DashboardTextClient,
    _ActiveStreamCounter,
    _relay_sse_chunks,
    _tracked_stream,
    create_dashboard_edge,
    render_dashboard_event,
    render_dashboard_snapshot,
)


class _OpenSSEStream(httpx.AsyncByteStream):
    """Yield one complete event while deliberately keeping the stream open."""

    def __init__(self, first_chunk: bytes) -> None:
        self.first_chunk = first_chunk
        self.closed = asyncio.Event()

    async def __aiter__(self):
        yield self.first_chunk
        await self.closed.wait()

    async def aclose(self) -> None:
        self.closed.set()


def _snapshot() -> dict[str, object]:
    def work(
        label: str,
        worker_label: str,
        *,
        title: str | None = None,
        state: str = "active",
        stage: str = "working",
        closure_state: str = "open",
    ) -> dict[str, object]:
        return {
            "display_label": label,
            "worker_label": worker_label,
            "work_title": title,
            "objective_summary": None,
            "state": state,
            "attempt_state": stage,
            "progress_stage": "候補を検証中",
            "stage": stage,
            "trajectory": "advancing",
            "attention_owner": "worker",
            "next_boundary_summary": "検証結果を報告",
            "pending_supervisor_boundary": True,
            "recovery_action": None,
            "recovery_waiting_since": None,
            "recovery_notification_state": None,
            "latest_report_kind": "progress",
            "latest_report_summary": None,
            "latest_reported_at": "2026-08-12T00:00:00Z",
            "runtime_heartbeat_at": "2026-08-12T00:00:01Z",
            "last_worker_activity_at": "2026-08-12T00:00:00Z",
            "last_artifact_at": None,
            "status_request_state": "pending",
            "status_requested_at": "2026-08-12T00:00:02Z",
            "status_response_due_at": "2026-08-12T00:05:02Z",
            "status_responded_at": None,
            "completion_contract": "completion_required",
            "delivery_state": "ready",
            "provider_condition": "rate_limited",
            "provider_retry_after_at": "2099-12-31T23:59:59Z",
            "cooldown_until": "2099-12-31T23:59:59Z",
            "next_observable_boundary": "pending",
            "latest_worker_report_summary": None,
            "runner_adapter": "codex-app-server",
            "runner_model": "gpt-5.6-codex",
            "runner_reasoning_effort": "high",
            "runner_requested_model": "gpt-5.6-codex",
            "runner_effective_model": "gpt-5.6-codex",
            "runner_requested_reasoning_effort": "high",
            "runner_effective_reasoning_effort": "high",
            "runner_availability": "available",
            "runner_state": "enabled",
            "runner_connection_state": "connected-busy",
            "latest_cao_review_decision": None,
            "requester_decision": None,
            "closure_state": closure_state,
            "closure_summary": {
                "requester_decision": None,
                "cao_review": None,
                "artifact_preservation": None,
                "cleanup": None,
                "unresolved_deliveries": None,
                "unresolved_effects": None,
                "active_runtimes": None,
            },
            "availability": {
                "work_title": "available" if title else "unavailable",
                "objective_summary": "unavailable",
                "attempt_state": "available",
                "progress_stage": "available",
                "next_boundary_summary": "available",
                "pending_supervisor_boundary": "available",
                "recovery_action": "unavailable",
                "recovery_waiting_since": "unavailable",
                "recovery_notification_state": "unavailable",
                "latest_report_kind": "available",
                "latest_report_summary": "unavailable",
                "latest_reported_at": "available",
                "runtime_heartbeat_at": "available",
                "last_worker_activity_at": "available",
                "last_artifact_at": "unavailable",
                "status_request_state": "available",
                "status_requested_at": "available",
                "status_response_due_at": "available",
                "status_responded_at": "unavailable",
                "completion_contract": "available",
                "delivery_state": "available",
                "provider_condition": "available",
                "provider_retry_after_at": "available",
                "cooldown_until": "available",
                "next_observable_boundary": "available",
                "latest_worker_report_summary": "unavailable",
                "runner_adapter": "available",
                "runner_model": "available",
                "runner_reasoning_effort": "available",
                "runner_requested_model": "available",
                "runner_effective_model": "available",
                "runner_requested_reasoning_effort": "available",
                "runner_effective_reasoning_effort": "available",
                "runner_availability": "available",
                "runner_state": "available",
                "runner_connection_state": "available",
                "latest_cao_review_decision": "unavailable",
                "requester_decision": "unavailable",
                "closure_summary": "unavailable",
            },
            "raw_payload": {"arbitrary_key": "untrusted-value"},
            "worker_locator": "opaque-internal-locator",
        }

    def worker(
        label: str,
        items: list[dict[str, object]],
        *,
        worker_state: str = "enabled",
        connection: str = "connected-busy",
        attention_reason: str | None = None,
    ) -> dict[str, object]:
        return {
            "worker_label": label,
            "attention_reason": attention_reason,
            "worker_state": worker_state,
            "runner_adapter": "codex-app-server",
            "runner_model": "gpt-5.6-codex",
            "runner_reasoning_effort": "high",
            "runner_requested_model": "gpt-5.6-codex",
            "runner_effective_model": "gpt-5.6-codex",
            "runner_requested_reasoning_effort": "high",
            "runner_effective_reasoning_effort": "high",
            "runner_availability": "available",
            "runner_connection_state": connection,
            "current_work_items": items,
            "private_locator": "opaque-internal-locator",
        }

    attention_work = work(
        "Work item 1",
        "Worker Alpha",
        title="提出物を確認",
        state="completed",
        stage="completed",
        closure_state="awaiting-explicit-close",
    )
    working_work = work("Work item 2", "Worker Beta")
    return {
        "format": "cao-dashboard-read-model/v1",
        "authority": {"mode": "canonical", "generation": 3},
        "cursor": "cursor-1",
        "snapshot_digest": "d" * 64,
        "untrusted_top_level": "untrusted-value",
        "projection": {
            "healthy": True,
            "snapshot": {
                "pending_intents": 1,
                "work_items": [
                    {
                        "state": "active",
                        "current_attempt_trajectory": "advancing",
                        "attention_owner": "worker",
                        "closure": "open",
                    }
                ],
                "runtimes": [{"state": "ready"}],
                "scheduler": {"queued_due": 2, "outcome_unknown": 0, "dead": 0},
                "effects_by_state": {"completed": 1},
            },
        },
        "operator": {
            "format": "cao-dashboard-operator/v1",
            "counts": {
                "needs_attention": 1,
                "working": 1,
                "ready": 1,
                "inactive_workers": 1,
                "current_work_items": 2,
            },
            "needs_attention": [
                worker(
                    "Worker Alpha",
                    [copy.deepcopy(attention_work)],
                    attention_reason="awaiting-explicit-close",
                )
            ],
            "working": [worker("Worker Beta", [copy.deepcopy(working_work)])],
            "ready": [worker("Worker Gamma", [], connection="enrolled-reopenable")],
            "inactive_workers": [
                worker("Worker 4", [], worker_state="stopped", connection="unavailable")
            ],
            "work_items": [copy.deepcopy(attention_work), copy.deepcopy(working_work)],
            "runtime_delivery": {
                "runtime_count": 1,
                "queued_deliveries": 2,
                "unknown_delivery_outcomes": 0,
                "dead_deliveries": 0,
                "effects_by_state": {"completed": 1, "unexpected": "untrusted-value"},
            },
            "acceptance_test_workers": [
                {"worker_label": "test-worker-residue", "worker_state": "enabled"}
            ],
        },
    }


def _transport(calls: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/snapshot"):
            return httpx.Response(200, json=_snapshot())
        if request.url.path.endswith("/history"):
            return httpx.Response(
                200,
                json={
                    "format": "cao-dashboard-read-model/v1",
                    "items": [
                        {
                            "event": {
                                "type": "work.changed",
                                "aggregate_type": "work",
                                "occurred_at": "now",
                            }
                        }
                    ],
                    "next_cursor": "cursor-2",
                    "has_more": False,
                },
            )
        if request.url.path.endswith("/stream"):
            return httpx.Response(
                200,
                content=b'id: cursor-2\nevent: dashboard-update\ndata: {"event":{"type":"work.changed","occurred_at":"now"}}\n\n',
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def _settings(calls: list[httpx.Request], **changes: object) -> DashboardEdgeSettings:
    values: dict[str, object] = {
        "upstream_base_url": "http://127.0.0.1:8768",
        "dashboard_bearer": "dashboard-bearer-credential-value",
        "bootstrap_secrets": {"b" * 32: time.time() + 60},
        "public_origin": "http://localhost",
        "transport": _transport(calls),
    }
    values.update(changes)
    return DashboardEdgeSettings(**values)  # type: ignore[arg-type]


def _bootstrap(client: TestClient, secret: str = "b" * 32) -> None:
    response = client.post("/dashboard/session", json={"secret": secret})
    assert response.status_code == 204


def test_sse_relay_forwards_a_small_event_while_upstream_remains_open() -> None:
    frame = (
        b"id: cursor-2\nevent: dashboard-update\n"
        b'data: {"event":{"type":"work.changed","occurred_at":"now"}}\n\n'
    )

    async def receive_first_chunk() -> bytes:
        stream = _OpenSSEStream(frame)
        response = httpx.Response(
            200,
            stream=stream,
            headers={"content-type": "text/event-stream"},
        )
        relay = _relay_sse_chunks(response)
        try:
            return await asyncio.wait_for(anext(relay), timeout=0.25)
        finally:
            await relay.aclose()
            await response.aclose()

    assert asyncio.run(receive_first_chunk()) == frame


def test_tracked_stream_counts_only_while_response_is_active() -> None:
    observed: list[int] = []
    counter = _ActiveStreamCounter(observed.append)
    release = asyncio.Event()

    async def source():
        yield b": heartbeat\n\n"
        await release.wait()

    async def exercise() -> tuple[int, int]:
        stream = _tracked_stream(counter, source())
        assert await anext(stream) == b": heartbeat\n\n"
        active = counter.value()
        await stream.aclose()
        return active, counter.value()

    assert asyncio.run(exercise()) == (1, 0)
    assert observed == [1, 0]


def test_stream_count_callback_failure_never_breaks_sse() -> None:
    def fail(_count: int) -> None:
        raise RuntimeError("observer unavailable")

    counter = _ActiveStreamCounter(fail)

    async def source():
        yield b"data: ok\n\n"

    async def exercise() -> tuple[bytes, int]:
        stream = _tracked_stream(counter, source())
        first = await anext(stream)
        await stream.aclose()
        return first, counter.value()

    assert asyncio.run(exercise()) == (b"data: ok\n\n", 0)


def test_edge_startup_emits_exactly_one_initial_zero_stream_count() -> None:
    calls: list[httpx.Request] = []
    observed: list[int] = []
    app = create_dashboard_edge(
        _settings(calls, on_authenticated_stream_count=observed.append)
    )
    assert observed == []

    with TestClient(app, base_url="http://localhost"):
        assert observed == [0]

    assert observed == [0]


def test_internal_visibility_requires_owner_bearer_and_reports_active_streams() -> None:
    calls: list[httpx.Request] = []
    app = create_dashboard_edge(_settings(calls))
    counter = app.state.dashboard_active_streams
    with TestClient(app, base_url="http://localhost") as client:
        assert client.get("/dashboard/internal/visibility").status_code == 401
        assert client.get(
            "/dashboard/internal/visibility",
            headers={"Authorization": "Bearer wrong-dashboard-credential"},
        ).status_code == 401
        assert client.post(
            "/dashboard/session/rendered",
            json={"cursor": "cursor-1", "snapshot_digest": "d" * 64},
        ).status_code == 401
        _bootstrap(client)
        assert client.post(
            "/dashboard/session/rendered",
            json={"cursor": "cursor-1", "snapshot_digest": "not-a-digest"},
        ).status_code == 400
        assert client.post(
            "/dashboard/session/rendered",
            json={"cursor": "cursor-1", "snapshot_digest": "d" * 64},
        ).status_code == 204
        counter.acquire()
        try:
            visible = client.get(
                "/dashboard/internal/visibility",
                headers={
                    "Authorization": "Bearer dashboard-bearer-credential-value"
                },
            )
        finally:
            counter.release()
    assert visible.status_code == 200
    assert visible.json() == {
        "format": "cao-dashboard-visibility/v2",
        "active_streams": 1,
        "rendered_cursor": "cursor-1",
        "rendered_snapshot_digest": "d" * 64,
    }
    assert "dashboard-bearer-credential-value" not in visible.text


def test_edge_only_proxies_official_dashboard_routes_and_keeps_bearer_server_side() -> None:
    calls: list[httpx.Request] = []
    settings = _settings(calls)
    app = create_dashboard_edge(settings)
    with TestClient(app, base_url="http://localhost") as client:
        assert client.get("/dashboard/api/snapshot").status_code == 401
        _bootstrap(client)
        snapshot = client.get("/dashboard/api/snapshot")
        history = client.get("/dashboard/api/history?after=cursor-1&limit=10")
        stream = client.get("/dashboard/api/stream", headers={"Last-Event-ID": "cursor-1"})
        assert snapshot.status_code == history.status_code == stream.status_code == 200
        reconnect = client.get(
            "/dashboard/api/stream?after=cursor-1",
            headers={"Last-Event-ID": "cursor-2"},
        )
        assert reconnect.status_code == 200
        assert "dashboard-bearer-credential-value" not in snapshot.text
        assert "untrusted-value" not in snapshot.text
        assert "opaque-internal-locator" not in snapshot.text
        assert "test-worker-residue" not in snapshot.text
        assert "provider_condition" not in snapshot.text
        assert "provider_retry_after_at" not in snapshot.text
        assert "cooldown_until" not in snapshot.text
        assert "rate_limited" not in snapshot.text
        assert "2099-12-31T23:59:59Z" not in snapshot.text
        assert set(snapshot.json()) == {
            "format",
            "authority",
            "cursor",
            "operator",
            "snapshot_digest",
        }
        assert set(snapshot.json()["operator"]) == {
            "format",
                "counts",
                "needs_attention",
                "cao_processing",
                "user_confirmation",
                "stopped_or_failed",
                "working",
                "ready",
                "inactive_workers",
                "work_items",
                "recently_completed",
                "runtime_delivery",
        }
        assert "dashboard-bearer-credential-value" not in history.text
        assert client.get("/dashboard/api/work").status_code == 404

    assert {request.url.path for request in calls} == {
        "/api/v1/dashboard/v1/snapshot",
        "/api/v1/dashboard/v1/history",
        "/api/v1/dashboard/v1/stream",
    }
    assert all(
        request.headers["authorization"] == "Bearer dashboard-bearer-credential-value"
        for request in calls
    )
    # Native EventSource reconnects keep the original URL but send the newest
    # delivered cursor in Last-Event-ID.  The edge must resume from that header
    # exactly once rather than rejecting the stale query cursor.
    assert calls[-1].headers["last-event-id"] == "cursor-2"
    assert all("redirect" not in str(request.url) for request in calls)


def test_cloudflare_access_assertion_authorizes_without_local_bootstrap_cookie() -> None:
    calls: list[httpx.Request] = []
    assertions: list[str] = []

    class Validator:
        async def authorized(self, token: str) -> bool:
            assertions.append(token)
            return token == "valid-access-assertion"

    access = CloudflareAccessSettings(
        team_domain="cao-team.cloudflareaccess.com",
        application_audience="a" * 64,
        allowed_email="owner@example.test",
    )
    app = create_dashboard_edge(
        _settings(
            calls,
            public_origin="https://cao.example.test",
            cloudflare_access=access,
            cloudflare_access_validator=Validator(),
        )
    )
    with TestClient(app, base_url="https://cao.example.test") as client:
        assert client.get("/dashboard/api/snapshot").status_code == 401
        assert client.get(
            "/dashboard/api/snapshot",
            headers={"Cf-Access-Jwt-Assertion": "invalid-access-assertion"},
        ).status_code == 401
        accepted = client.get(
            "/dashboard/api/snapshot",
            headers={"Cf-Access-Jwt-Assertion": "valid-access-assertion"},
        )
    assert accepted.status_code == 200
    assert assertions == ["", "invalid-access-assertion", "valid-access-assertion"]


def test_bootstrap_secret_is_one_time_expiring_and_not_reflected() -> None:
    calls: list[httpx.Request] = []
    secret = "s" * 32
    app = create_dashboard_edge(_settings(calls, bootstrap_secrets={secret: time.time() + 60}))
    with TestClient(app, base_url="http://localhost") as client:
        first = client.post("/dashboard/session", json={"secret": secret})
        replay = client.post("/dashboard/session", json={"secret": secret})
        assert first.status_code == 204
        assert replay.status_code == 401
        assert secret not in replay.text
        cookie = first.headers["set-cookie"].lower()
        assert "httponly" in cookie and "samesite=strict" in cookie
        assert "secure" not in cookie

    expired = create_dashboard_edge(_settings(calls, bootstrap_secrets={"e" * 32: time.time() - 1}))
    with TestClient(expired, base_url="http://localhost") as client:
        assert client.post("/dashboard/session", json={"secret": "e" * 32}).status_code == 401


def test_https_edge_session_is_secure_and_session_expiry_is_enforced() -> None:
    calls: list[httpx.Request] = []
    now = [100.0]
    settings = _settings(
        calls,
        public_origin="https://dashboard.example.test",
        bootstrap_secrets={"h" * 32: 150.0},
        session_ttl_seconds=10,
    )
    app = create_dashboard_edge(settings, clock=lambda: now[0])
    with TestClient(app, base_url="https://dashboard.example.test") as client:
        response = client.post("/dashboard/session", json={"secret": "h" * 32})
        assert response.status_code == 204
        assert "secure" in response.headers["set-cookie"].lower()
        assert client.get("/dashboard/api/snapshot").status_code == 200
        now[0] = 111.0
        assert client.get("/dashboard/api/snapshot").status_code == 401


def test_hash_only_session_survives_edge_restart_and_expires(
    tmp_path: Path,
) -> None:
    calls: list[httpx.Request] = []
    now = [100.0]
    session_records = tmp_path / "sessions"
    session_records.mkdir(mode=0o700)
    session_records.chmod(0o700)
    settings = _settings(
        calls,
        bootstrap_secrets={"r" * 32: 150.0},
        session_record_dir=session_records,
        session_ttl_seconds=10,
    )

    first_app = create_dashboard_edge(settings, clock=lambda: now[0])
    with TestClient(first_app, base_url="http://localhost") as client:
        response = client.post("/dashboard/session", json={"secret": "r" * 32})
        assert response.status_code == 204
        raw_session = response.cookies.get("cao_dashboard_session")
    assert isinstance(raw_session, str) and raw_session

    records = list(session_records.iterdir())
    assert len(records) == 1
    record = records[0]
    assert record.name == hashlib.sha256(raw_session.encode("utf-8")).hexdigest()
    assert stat.S_IMODE(record.stat().st_mode) == 0o600
    assert raw_session not in record.read_text(encoding="utf-8")

    restarted_app = create_dashboard_edge(settings, clock=lambda: now[0])
    with TestClient(restarted_app, base_url="http://localhost") as client:
        response = client.get(
            "/dashboard/api/snapshot",
            headers={"Cookie": f"cao_dashboard_session={raw_session}"},
        )
        assert response.status_code == 200

    now[0] = 111.0
    expired_app = create_dashboard_edge(settings, clock=lambda: now[0])
    with TestClient(expired_app, base_url="http://localhost") as client:
        response = client.get(
            "/dashboard/api/snapshot",
            headers={"Cookie": f"cao_dashboard_session={raw_session}"},
        )
        assert response.status_code == 401
    assert not record.exists()


def test_html_has_no_inline_script_or_secret_storage_and_is_mobile_semantic() -> None:
    calls: list[httpx.Request] = []
    app = create_dashboard_edge(_settings(calls))
    with TestClient(app, base_url="http://localhost") as client:
        page = client.get("/dashboard/")
        script = client.get("/dashboard/static/dashboard.js")
    assert page.status_code == script.status_code == 200
    assert "viewport" in page.text
    assert "<main" in page.text and "<section" in page.text and "aria-live" in page.text
    main_opening_tag = page.text.split("<main", 1)[1].split(">", 1)[0]
    assert "aria-live" not in main_opening_tag
    assert "現在稼働中の本番Workerはありません" in page.text
    assert all(
        label in page.text
        for label in (
            "CAOが処理中",
            "あなたの確認待ち",
            "停止・異常",
            "Worker作業中",
            "待機・再開可能",
            "作業の履歴",
        )
    )
    assert "停止済み・移行Worker" in page.text and "最新のWorker報告" in script.text
    assert "<script src=" in page.text and "<script>" not in page.text
    assert "localStorage" not in script.text
    assert "operator.work_items" not in script.text
    assert all(
        key in script.text
        for key in (
            "cao_processing",
            "user_confirmation",
            "stopped_or_failed",
            "working",
            "ready",
            "inactive_workers",
            "recently_completed",
        )
    )
    assert "dashboard_bearer" not in page.text + script.text
    assert "Provider状態" not in script.text
    assert "再試行可能時刻" not in script.text
    assert "provider_condition" not in script.text
    assert "provider_retry_after_at" not in script.text
    assert "同じ更新を再取得しています" in script.text
    assert "次の更新を待っています" not in script.text
    assert "window.setTimeout" in script.text
    assert 'fetchWithDeadline("/dashboard/session/rendered"' in script.text
    csp = page.headers["content-security-policy"]
    assert "script-src 'self'" in csp and "frame-ancestors 'none'" in csp


def test_resync_and_upstream_failures_are_sanitized() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/history"):
            return httpx.Response(
                409,
                json={
                    "format": "cao-dashboard-read-model/v1",
                    "status": "resync-required",
                    "reason": "cursor-pruned",
                },
            )
        return httpx.Response(500, text="private upstream failure token")

    settings = _settings([], transport=httpx.MockTransport(handler))
    app = create_dashboard_edge(settings)
    with TestClient(app, base_url="http://localhost") as client:
        _bootstrap(client)
        resync = client.get("/dashboard/api/history?after=cursor-1")
        unavailable = client.get("/dashboard/api/snapshot")
    assert resync.status_code == 409 and resync.json()["status"] == "resync-required"
    assert unavailable.status_code == 502
    assert "private upstream failure token" not in unavailable.text


def test_text_client_renders_sanitized_snapshot_and_follow_updates() -> None:
    calls: list[httpx.Request] = []
    client = DashboardTextClient(
        "http://127.0.0.1:8768",
        "dashboard-bearer-credential-value",
        transport=httpx.MockTransport(lambda request: _transport(calls).handle_request(request)),
    )
    rendered = client.snapshot()
    assert all(
        label in rendered for label in ("停止・異常", "Worker作業中", "待機・再開可能")
    )
    assert "停止済み・移行Worker" in rendered
    assert "trajectory advancing" in rendered
    assert "closure open" in rendered and "Effects: completed: 1" in rendered
    assert "title unavailable" in rendered and "next boundary 検証結果を報告" in rendered
    assert "Work items:" not in rendered
    assert "untrusted-value" not in rendered and "opaque-internal-locator" not in rendered
    assert "provider condition" not in rendered
    assert "provider retry after" not in rendered
    assert "rate_limited" not in rendered
    assert "2099-12-31T23:59:59Z" not in rendered
    assert list(client.follow(after="cursor-1", limit=1)) == [
        "Update: work.changed at unavailable"
    ]
    assert all(request.url.path.startswith("/api/v1/dashboard/v1/") for request in calls)


def test_text_output_has_operator_field_parity_and_ignores_unknown_payload_keys() -> None:
    rendered = render_dashboard_snapshot(_snapshot(), timezone=ZoneInfo("Asia/Tokyo"))
    for label in (
        "title unavailable",
        "objective unavailable",
        "state active",
        "attempt state working",
        "progress stage 候補を検証中",
        "trajectory advancing",
        "attention worker",
        "next boundary 検証結果を報告",
        "pending supervisor boundary yes",
        "latest report unavailable",
        "report kind progress",
        "reported at 2026-08-12 09:00:00 JST",
        "CAO review unavailable",
        "requester decision unavailable",
        "closure open",
    ):
        assert label in rendered
    assert "untrusted-value" not in rendered
    assert "opaque-internal-locator" not in rendered
    assert (
        render_dashboard_event(
            "dashboard-update",
            {"event": {"type": "untrusted-value", "occurred_at": "opaque-internal-locator"}},
        )
        == "Dashboard update received."
    )


def test_server_categories_are_rendered_as_supplied_without_client_reclassification() -> None:
    body = _snapshot()
    operator = body["operator"]
    assert isinstance(operator, dict)
    # This deliberately puts a connected-busy Worker in the ready category.
    # The edge must preserve server membership instead of inferring from state.
    operator["ready"] = operator["working"]
    operator["working"] = []
    rendered = render_dashboard_snapshot(body, timezone=ZoneInfo("Asia/Tokyo"))
    assert "作業中\n" not in rendered
    assert rendered.index("待機・再開可能") < rendered.index("- Worker Beta")

    operator["needs_attention"] = []
    operator["ready"] = []
    operator["work_items"] = []
    operator["counts"] = {
        "needs_attention": 0,
        "working": 0,
        "ready": 0,
        "inactive_workers": 1,
        "current_work_items": 0,
    }
    empty = render_dashboard_snapshot(body)
    assert "現在稼働中の本番Workerはありません" in empty
    assert "Work items:" not in empty


def test_edge_preserves_exact_worker_and_nested_work_allowlists() -> None:
    calls: list[httpx.Request] = []
    app = create_dashboard_edge(_settings(calls))
    with TestClient(app, base_url="http://localhost") as client:
        _bootstrap(client)
        operator = client.get("/dashboard/api/snapshot").json()["operator"]

    worker = operator["working"][0]
    assert set(worker) == {
        "worker_label",
        "attention_reason",
        "worker_state",
        "runner_adapter",
        "runner_model",
        "runner_reasoning_effort",
        "runner_requested_model",
        "runner_effective_model",
        "runner_requested_reasoning_effort",
        "runner_effective_reasoning_effort",
        "runner_availability",
        "runner_connection_state",
        "current_work_items",
    }
    work = worker["current_work_items"][0]
    assert set(work) == {
        "display_label",
        "worker_label",
        "work_title",
        "objective_summary",
        "objective_text",
        "latest_report_text",
        "history_reference",
        "state",
        "attempt_state",
        "progress_stage",
        "stage",
        "trajectory",
        "attention_owner",
        "next_boundary_summary",
        "pending_supervisor_boundary",
        "recovery_action",
        "recovery_waiting_since",
            "recovery_notification_state",
            "cao_supervision_state",
            "cao_supervision_updated_at",
        "latest_report_kind",
        "latest_report_summary",
        "latest_reported_at",
        "runtime_heartbeat_at",
        "last_worker_activity_at",
        "last_artifact_at",
        "status_request_state",
        "status_requested_at",
        "status_response_due_at",
        "status_responded_at",
        "completion_contract",
        "delivery_state",
        "next_observable_boundary",
        "latest_worker_report_summary",
        "runner_adapter",
        "runner_model",
        "runner_reasoning_effort",
        "runner_requested_model",
        "runner_effective_model",
        "runner_requested_reasoning_effort",
        "runner_effective_reasoning_effort",
        "runner_availability",
        "runner_state",
        "runner_connection_state",
        "latest_cao_review_decision",
        "requester_decision",
        "closure_state",
            "closure_summary",
            "completed_at",
            "availability",
    }
    assert operator["needs_attention"][0]["attention_reason"] == (
        "awaiting-explicit-close"
    )
    assert "private_locator" not in worker and "raw_payload" not in work


def test_text_and_mobile_browser_keep_the_same_operator_content_fields() -> None:
    body = _snapshot()
    operator = body["operator"]
    assert isinstance(operator, dict)
    working = operator["working"]
    assert isinstance(working, list) and isinstance(working[0], dict)
    worker = working[0]
    worker.update(
        {
            "runner_adapter": "claude",
            "runner_model": "claude-opus-4.1",
            "runner_reasoning_effort": "high",
            "runner_requested_model": "claude-opus-4.1",
            "runner_effective_model": "claude-opus-4.1",
            "runner_requested_reasoning_effort": "high",
            "runner_effective_reasoning_effort": "high",
            "runner_availability": "available",
            "worker_state": "enabled",
            "runner_connection_state": "connected-idle",
        }
    )
    current = worker["current_work_items"]
    assert isinstance(current, list) and isinstance(current[0], dict)
    item = current[0]
    item.update(
        {
            "work_title": "Canonical title",
            "objective_summary": "Canonical objective",
            "progress_stage": "Canonical stage",
            "next_boundary_summary": "Canonical next boundary",
            "latest_report_kind": "artifact",
            "latest_report_summary": "Worker reports progress",
            "latest_reported_at": "2026-08-12T01:02:03Z",
            "latest_worker_report_summary": "Worker reports progress",
            "recovery_action": "system_reconciliation",
            "recovery_waiting_since": "2026-08-12T01:00:00Z",
            "recovery_notification_state": "handled",
        }
    )
    availability = item["availability"]
    assert isinstance(availability, dict)
    availability.update(
        {
            "recovery_action": "available",
            "recovery_waiting_since": "available",
            "recovery_notification_state": "available",
        }
    )
    alias = operator["work_items"]
    assert isinstance(alias, list) and isinstance(alias[1], dict)
    alias[1].update(item)
    rendered = render_dashboard_snapshot(body, timezone=ZoneInfo("Asia/Tokyo"))
    for expected in (
        "title Canonical title",
        "objective Canonical objective",
        "progress stage Canonical stage",
        "latest report Worker reports progress",
        "report kind artifact",
        "next boundary Canonical next boundary",
        "reported at 2026-08-12 10:02:03 JST",
        "recovery action system_reconciliation",
        "recovery waiting since 2026-08-12 10:00:00 JST",
        "recovery notification handled",
        "completion contract completion_required",
        "delivery state ready",
        "runner claude",
        "model claude-opus-4.1",
        "reasoning effort high",
        "requested model claude-opus-4.1",
        "effective model claude-opus-4.1",
        "requested reasoning effort high",
        "effective reasoning effort high",
        "runner availability available",
        "worker state enabled",
        "runner connection connected-idle",
    ):
        assert expected in rendered

    calls: list[httpx.Request] = []
    app = create_dashboard_edge(
        _settings(
            calls, transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=body))
        )
    )
    with TestClient(app, base_url="http://localhost") as client:
        _bootstrap(client)
        edge_operator = client.get("/dashboard/api/snapshot").json()["operator"]
        edge_worker = edge_operator["working"][0]
        edge_item = edge_worker["current_work_items"][0]
        script = client.get("/dashboard/static/dashboard.js").text
        page = client.get("/dashboard/").text
    assert {key: edge_item[key] for key in item if key in edge_item and key != "availability"} == {
        key: item[key] for key in item if key in edge_item and key != "availability"
    }
    raw_availability = item["availability"]
    assert isinstance(raw_availability, dict)
    assert edge_item["availability"] == {
        key: raw_availability[key] for key in edge_item["availability"]
    }
    assert edge_worker["runner_adapter"] == "claude"
    assert edge_worker["runner_effective_model"] == "claude-opus-4.1"
    assert "Runner" in script and "推論Effort" in script
    assert "接続状態" in script and "item.runner_connection_state" in script
    assert "進捗" in script and "最新のWorker報告" in script and "次の予定" in script
    assert "recovery-notice" in script and "復旧通知の配送" in script
    assert 'system_reconciliation: "システム整合化が必要"' in script
    assert 'artifact: "成果物報告"' in script
    assert 'delivery_missing: "成果物が未引き渡し"' in script
    assert script.index("workItems.forEach") < script.index("technical.append", script.index("function workerCard"))
    assert "接続・モデル詳細" in script
    assert "viewport" in page and "<main" in page


def test_edge_and_text_keep_explicit_closure_and_safe_close_summary_on_reload() -> None:
    body = _snapshot()
    operator = body["operator"]
    assert isinstance(operator, dict)
    needs_attention = operator["needs_attention"]
    assert isinstance(needs_attention, list) and isinstance(needs_attention[0], dict)
    current = needs_attention[0]["current_work_items"]
    assert isinstance(current, list) and isinstance(current[0], dict)
    item = current[0]
    close_update = {
        "state": "completed",
        "stage": "completed",
        "closure_state": "closed",
        "closure_summary": {
            "requester_decision": "accepted",
            "cao_review": "ok",
            "artifact_preservation": "preserved",
            "cleanup": "verified",
            "unresolved_deliveries": 0,
            "unresolved_effects": 0,
            "active_runtimes": 0,
            "decision_text": "private requester text",
            "artifact_uri": "file:///private/artifact",
            "cleanup_target": "/private/worktree",
            "token": "private-token",
        },
    }
    item.update(close_update)
    alias = operator["work_items"]
    assert isinstance(alias, list) and isinstance(alias[0], dict)
    alias[0].update(copy.deepcopy(close_update))

    rendered = render_dashboard_snapshot(body)
    for label in (
        "CAO review ok",
        "requester decision accepted",
        "closure closed",
        "artifacts preserved",
        "cleanup verified",
        "unresolved deliveries 0",
        "unresolved effects 0",
        "active runtimes 0",
    ):
        assert label in rendered
    assert "private requester text" not in rendered
    assert "file:///private/artifact" not in rendered
    assert "/private/worktree" not in rendered
    assert "private-token" not in rendered

    # A browser refresh after an SSE/history event sees the same edge-filtered
    # DTO, rather than reclassifying a completed item as implicitly closed.
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path.endswith("/snapshot"):
            return httpx.Response(200, json=body)
        if request.url.path.endswith("/history"):
            return httpx.Response(
                200,
                json={
                    "format": "cao-dashboard-read-model/v1",
                    "items": [
                        {
                            "event": {
                                "type": "work.closed",
                                "aggregate_type": "work",
                                "occurred_at": "now",
                            }
                        }
                    ],
                    "next_cursor": "cursor-2",
                    "has_more": False,
                },
            )
        if request.url.path.endswith("/stream"):
            return httpx.Response(
                200,
                content=(
                    b"id: cursor-2\nevent: dashboard-update\n"
                    b'data: {"event":{"type":"work.closed","occurred_at":"now"}}\n\n'
                ),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(404)

    app = create_dashboard_edge(_settings(calls, transport=httpx.MockTransport(handler)))
    with TestClient(app, base_url="http://localhost") as client:
        _bootstrap(client)
        snapshot = client.get("/dashboard/api/snapshot")
        history = client.get("/dashboard/api/history?after=cursor-1")
        stream = client.get("/dashboard/api/stream", headers={"Last-Event-ID": "cursor-1"})
    assert snapshot.status_code == history.status_code == 200
    assert stream.status_code == 200 and "event: dashboard-update" in stream.text
    reloaded = snapshot.json()["operator"]["needs_attention"][0]["current_work_items"][0]
    assert reloaded["closure_state"] == "closed"
    assert reloaded["closure_summary"]["cleanup"] == "verified"
    assert "private requester text" not in snapshot.text


@pytest.mark.parametrize(
    "url",
    ["https://public.example.test", "https://user:pass@127.0.0.1", "http://127.0.0.1/?next=/mcp"],
)
def test_edge_rejects_non_private_or_ambiguous_upstream_urls(url: str) -> None:
    with pytest.raises(ValueError):
        _settings([], upstream_base_url=url)
