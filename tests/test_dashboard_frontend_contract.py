from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).parents[1] / "src" / "cao_control_plane" / "static" / "dashboard.js"
).read_text(encoding="utf-8")


def _function_body(name: str) -> str:
    marker = f"function {name}("
    start = _SCRIPT.index(marker)
    opening = _SCRIPT.index("{", start)
    depth = 0
    for index in range(opening, len(_SCRIPT)):
        if _SCRIPT[index] == "{":
            depth += 1
        elif _SCRIPT[index] == "}":
            depth -= 1
            if depth == 0:
                return _SCRIPT[opening + 1 : index]
    raise AssertionError(f"unterminated function: {name}")


def test_dashboard_has_no_periodic_or_idle_refresh_path() -> None:
    for periodic_api in ("setInterval(", "requestAnimationFrame("):
        assert periodic_api not in _SCRIPT
    retry = _function_body("scheduleRetry")
    assert _SCRIPT.count("setTimeout(") == 2
    assert "state.completedRevision >= state.pendingRevision" in retry
    assert "Math.min(state.retryDelayMs * 2, 5000)" in retry
    assert "enqueueRefreshAttempt(state.pendingRevision)" in retry

    refresh = _function_body("refresh")
    connect = _function_body("connect")
    assert "connect()" not in refresh
    assert 'addEventListener("dashboard-update", enqueueUpdate)' in connect
    assert 'addEventListener("dashboard-synced", enqueueSync)' in connect
    assert "source.onopen" in connect
    assert "reportRendered()" not in connect


def test_durable_sse_events_are_deduplicated_and_serially_applied() -> None:
    update = _function_body("enqueueUpdate")
    synced = _function_body("enqueueSync")
    assert "event.lastEventId" in update
    assert "eventId === state.lastEventId" in update
    assert "state.lastEventId = eventId" in update
    assert "state.history.length > 20" in update
    assert update.index("requestRefresh()") < update.index("renderHistory()")
    assert "event.lastEventId" in synced
    assert "eventId !== state.cursor" in synced
    assert "requestRefresh()" in synced
    enqueue = _function_body("enqueueRefreshAttempt")
    assert "state.updateQueue = state.updateQueue" in enqueue
    assert "await refresh(shouldReconnect)" in enqueue


def test_user_facing_timestamps_use_browser_intl_and_never_echo_invalid_values() -> None:
    formatter = _function_body("localTimestamp")
    report = _function_body("reportMeta")
    history = _function_body("renderHistory")
    assert "new Intl.DateTimeFormat(undefined" in formatter
    assert 'timeZoneName: "short"' in formatter
    assert "localTimestamp(item.latest_reported_at)" in report
    assert "localTimestamp(event.occurred_at)" in history


@pytest.mark.parametrize(
    ("timezone", "mode", "language"),
    [
        ("Asia/Tokyo", "tokyo", "ja_JP.UTF-8"),
        ("America/New_York", "new-york", "en_US.UTF-8"),
    ],
)
def test_browser_timestamp_runtime_uses_current_timezone_rules(
    timezone: str, mode: str, language: str
) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is not available for the browser timestamp contract test")
    harness = Path(__file__).with_name("dashboard_timestamp_runtime.mjs")
    environment = dict(os.environ)
    environment.update({"TZ": timezone, "LANG": language})
    result = subprocess.run(
        [
            node,
            str(harness),
            str(Path(__file__).parents[1] / "src/cao_control_plane/static/dashboard.js"),
            mode,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env=environment,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_normal_updates_keep_one_event_source_and_resync_restarts_from_snapshot_cursor() -> None:
    deadline = _function_body("fetchWithDeadline")
    update = _function_body("enqueueUpdate")
    resync = _function_body("enqueueResync")
    connect = _function_body("connect")
    attempt = _function_body("enqueueRefreshAttempt")
    assert ".close()" not in update
    assert "connect()" not in update
    assert "source.close()" in resync
    assert "requestRefresh({ reconnect: true })" in resync
    assert "source.onerror" in connect
    assert "enqueueResync(source)" in connect
    assert "new AbortController()" in deadline
    assert "controller.abort()" in deadline
    assert "window.clearTimeout(deadline)" in deadline
    assert attempt.index("await refresh(shouldReconnect)") < attempt.index("connect()")


def test_dashboard_event_runtime_contract() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is not available for the browser contract test")
    harness = Path(__file__).with_name("dashboard_frontend_runtime.mjs")
    result = subprocess.run(
        [node, str(harness), str(Path(__file__).parents[1] / "src/cao_control_plane/static/dashboard.js")],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_dashboard_reading_interaction_contract() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is not available for the Dashboard interaction test")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("dashboard_reading_runtime.mjs")),
         str(Path(__file__).parents[1] / "src/cao_control_plane/static/dashboard.js")],
        capture_output=True, text=True, timeout=10, check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout
