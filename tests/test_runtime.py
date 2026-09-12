from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace

import pytest
from enrollment_helpers import (
    EnrollmentHandshakeAdapter,
    EnrollmentHandshakeRegistry,
    enroll_ready_worker_runtime,
)

from cao_control_plane.config import Settings
from cao_control_plane.errors import ConflictError
from cao_control_plane.models import (
    AckInput,
    BoundaryDispositionInput,
    BoundaryDispositionKind,
    DeliveryResolveInput,
    PrincipalCreate,
    ReportInput,
    RuntimeDispatchResult,
    RuntimeHeartbeat,
    StatusRequestInput,
    WorkAssignment,
)
from cao_control_plane.runtime import (
    AdapterRegistry,
    Dispatcher,
    RuntimeAdapterError,
    _ManagedWorkerActivityMonitor,
    render_message,
)


def test_default_runtime_registry_rejects_retired_tmux_transport() -> None:
    with pytest.raises(RuntimeAdapterError, match="unsupported runtime adapter"):
        AdapterRegistry(Settings()).get("tmux")


def test_database_commit_notifications_skip_read_only_transactions(system):
    database = system["service"].db
    before = database.commit_generation()

    with database.transaction() as connection:
        connection.execute("SELECT id FROM principals LIMIT 1").fetchone()

    assert database.commit_generation() == before
    database.execute(
        "INSERT INTO metadata(key, value) VALUES(?, ?)",
        ("test-commit-notification", "changed"),
    )
    assert database.commit_generation() == before + 1


def test_dispatcher_recovery_config_defaults(tmp_path, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text("[runtime]\ndispatcher_recovery_scan_seconds = 2.5\n")
    monkeypatch.setenv("CAO_A2A_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("CAO_A2A_RUNTIME_LAUNCH_DIR", str(tmp_path / "state" / "runtime-launches"))
    loaded = Settings.load(config)
    assert loaded.dispatcher_recovery_scan_seconds == 2.5

    monkeypatch.setenv("CAO_A2A_DISPATCHER_RECOVERY_SCAN_SECONDS", "7.5")
    overridden = Settings.load(config)
    assert overridden.dispatcher_recovery_scan_seconds == 7.5
    assert Settings().dispatcher_recovery_scan_seconds == 30.0


def test_dashboard_access_probe_loads_only_explicit_owner_private_paths(tmp_path, monkeypatch):
    state = tmp_path / "state"
    credentials = state / "dashboard-credentials.json"
    bootstrap = state / "dashboard-bootstrap"
    sessions = state / "dashboard-sessions"
    config = tmp_path / "config.toml"
    config.write_text(
        "\n".join(
            (
                "[server]",
                f'state_dir = "{state}"',
                "enable_dashboard = true",
                "[runtime]",
                f'runtime_launch_dir = "{state / "runtime-launches"}"',
                "[dashboard]",
                "enable_dashboard_access_probe = true",
                f'dashboard_credentials_file = "{credentials}"',
                f'dashboard_bootstrap_record_dir = "{bootstrap}"',
                f'dashboard_session_record_dir = "{sessions}"',
                'dashboard_edge_base_url = "http://127.0.0.1:8769"',
                "dashboard_access_timeout_seconds = 7.5",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("CAO_A2A_STATE_DIR", raising=False)
    monkeypatch.delenv("CAO_A2A_RUNTIME_LAUNCH_DIR", raising=False)

    loaded = Settings.load(config)

    assert loaded.enable_dashboard_access_probe is True
    assert loaded.dashboard_access_timeout_seconds == 7.5
    assert loaded.dashboard_credentials_file == credentials
    assert loaded.dashboard_bootstrap_record_dir == bootstrap
    assert loaded.dashboard_session_record_dir == sessions
    assert bootstrap.is_dir() and sessions.is_dir()
    assert bootstrap.stat().st_mode & 0o777 == 0o700
    assert sessions.stat().st_mode & 0o777 == 0o700


def test_dashboard_access_probe_rejects_incomplete_configuration(tmp_path):
    with pytest.raises(ValueError, match="needs an owner-private credentials file"):
        Settings(
            state_dir=tmp_path / "state",
            runtime_launch_dir=tmp_path / "launches",
            enable_dashboard=True,
            enable_dashboard_access_probe=True,
        )


def test_removed_dashboard_presentation_config_is_rejected(tmp_path, monkeypatch):
    config = tmp_path / "config.toml"
    credentials = tmp_path / "credentials.json"
    config.write_text(
        "\n".join(
            (
                "[server]",
                "enable_dashboard = true",
                "[dashboard]",
                "require_dashboard_visibility_for_supervision = true",
                f'dashboard_credentials_file = "{credentials}"',
                'dashboard_presenter = "safari"',
                "dashboard_visibility_timeout_seconds = 4.5",
            )
        )
    )
    monkeypatch.setenv("CAO_A2A_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("CAO_A2A_RUNTIME_LAUNCH_DIR", str(tmp_path / "state" / "runtime-launches"))

    with pytest.raises(ValueError, match="unsupported configuration keys"):
        Settings.load(config)


def test_dispatcher_mcp_commit_wakes_idle_dispatcher_without_interval_scanning(system, monkeypatch):
    async def exercise() -> None:
        service = system["service"]
        settings = replace(
            system["settings"],
            dispatcher_recovery_scan_seconds=60.0,
        )
        wait_started = asyncio.Event()
        delivered = asyncio.Event()
        loop = asyncio.get_running_loop()
        original_wait = service.db.wait_for_commit

        def observe_wait(*args, **kwargs):
            loop.call_soon_threadsafe(wait_started.set)
            return original_wait(*args, **kwargs)

        monkeypatch.setattr(service.db, "wait_for_commit", observe_wait)

        class SignallingAdapter(EnrollmentHandshakeAdapter):
            async def dispatch(self, runtime, message):
                result = await super().dispatch(runtime, message)
                delivered.set()
                return result

        dispatcher = Dispatcher(
            service,
            settings,
            registry=EnrollmentHandshakeRegistry(SignallingAdapter(service)),
        )
        await dispatcher.start()
        try:
            await asyncio.wait_for(wait_started.wait(), timeout=1.0)
            service.assign_work(
                system["cao"],
                WorkAssignment(
                    worker_id=system["worker"]["id"],
                    title="MCP wake",
                    objective="Wake from a durable service commit",
                    acceptance=["Delivered without interval polling"],
                    runtime_session_id=system["runtime"]["id"],
                ),
            )
            # Keep this well below the disabled 60-second recovery scan while
            # allowing a loaded CI runner to schedule the dispatcher thread.
            await asyncio.wait_for(delivered.wait(), timeout=5.0)
        finally:
            await dispatcher.stop()

    asyncio.run(exercise())


def test_dispatcher_no_commit_does_not_start_another_cycle(system, monkeypatch):
    async def exercise() -> None:
        service = system["service"]
        settings = replace(
            system["settings"],
            dispatcher_recovery_scan_seconds=60.0,
        )
        wait_started = asyncio.Event()
        loop = asyncio.get_running_loop()
        original_wait = service.db.wait_for_commit

        def observe_wait(*args, **kwargs):
            loop.call_soon_threadsafe(wait_started.set)
            return original_wait(*args, **kwargs)

        monkeypatch.setattr(service.db, "wait_for_commit", observe_wait)

        class CountingDispatcher(Dispatcher):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.cycles = 0

            def _start_background_jobs(self):
                self.cycles += 1
                return 0

        dispatcher = CountingDispatcher(service, settings)
        await dispatcher.start()
        try:
            await asyncio.wait_for(wait_started.wait(), timeout=1.0)
            assert dispatcher.cycles == 1
        finally:
            await dispatcher.stop()

    asyncio.run(exercise())


def test_dispatcher_commit_between_scan_and_wait_is_not_missed(system):
    async def exercise() -> None:
        service = system["service"]
        settings = replace(
            system["settings"],
            dispatcher_recovery_scan_seconds=60.0,
        )
        first_scan = asyncio.Event()
        allow_first_scan_to_finish = asyncio.Event()
        second_scan = asyncio.Event()

        class RaceDispatcher(Dispatcher):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.cycles = 0

            def _start_background_jobs(self):
                self.cycles += 1
                if self.cycles == 1:
                    first_scan.set()
                    # Commit exactly between scan and condition wait.
                    service.db.execute(
                        "INSERT INTO metadata(key, value) VALUES(?, ?)",
                        ("test-raced-dispatcher-commit", "changed"),
                    )
                    allow_first_scan_to_finish.set()
                else:
                    second_scan.set()
                return 0

        dispatcher = RaceDispatcher(service, settings)
        await dispatcher.start()
        try:
            await asyncio.wait_for(first_scan.wait(), timeout=1.0)
            await asyncio.wait_for(allow_first_scan_to_finish.wait(), timeout=1.0)
            await asyncio.wait_for(second_scan.wait(), timeout=1.0)
            assert dispatcher.cycles >= 2
        finally:
            await dispatcher.stop()

    asyncio.run(exercise())


def test_dispatcher_stop_releases_commit_wait_executor_promptly(system, monkeypatch):
    async def exercise() -> None:
        service = system["service"]
        settings = replace(
            system["settings"],
            dispatcher_recovery_scan_seconds=60.0,
        )
        wait_started = asyncio.Event()
        wait_finished = asyncio.Event()
        loop = asyncio.get_running_loop()
        original_wait = service.db.wait_for_commit

        def observe_wait(*args, **kwargs):
            loop.call_soon_threadsafe(wait_started.set)
            try:
                return original_wait(*args, **kwargs)
            finally:
                loop.call_soon_threadsafe(wait_finished.set)

        monkeypatch.setattr(service.db, "wait_for_commit", observe_wait)
        dispatcher = Dispatcher(service, settings)
        await dispatcher.start()
        await asyncio.wait_for(wait_started.wait(), timeout=1.0)
        await asyncio.wait_for(dispatcher.stop(), timeout=1.0)
        await asyncio.wait_for(wait_finished.wait(), timeout=1.0)
        assert dispatcher.running is False

    asyncio.run(exercise())


def test_dispatcher_recovery_deadline_runs_expiry_recovery(system, monkeypatch):
    async def exercise() -> None:
        service = system["service"]
        settings = replace(
            system["settings"],
            dispatcher_recovery_scan_seconds=0.01,
        )
        recovery_cycle = asyncio.Event()
        original_expire = service.expire_runtime_leases
        original_recover = service.recover_expired_reasoner_turns
        calls = 0

        def expire():
            nonlocal calls
            calls += 1
            result = original_expire()
            if calls >= 2:
                recovery_cycle.set()
            return result

        monkeypatch.setattr(service, "expire_runtime_leases", expire)
        monkeypatch.setattr(service, "recover_expired_reasoner_turns", original_recover)
        dispatcher = Dispatcher(service, settings)
        await dispatcher.start()
        try:
            await asyncio.wait_for(recovery_cycle.wait(), timeout=1.0)
            assert calls >= 2
        finally:
            await dispatcher.stop()

    asyncio.run(exercise())


def test_dispatcher_cycle_does_not_erase_a_completed_job_failure(system, monkeypatch) -> None:
    async def exercise() -> None:
        dispatcher = Dispatcher(system["service"], system["settings"])

        async def fail_in_background() -> None:
            raise RuntimeError("private background failure detail")

        failed = asyncio.create_task(fail_in_background())
        await asyncio.gather(failed, return_exceptions=True)
        dispatcher._active_jobs.add(failed)
        monkeypatch.setattr(dispatcher, "_start_background_jobs", lambda: 0)

        dispatcher._run_background_cycle()

        assert dispatcher.last_error
        assert "private background failure detail" not in dispatcher.last_error
        assert dispatcher.last_cycle_at

        dispatcher._run_background_cycle()
        assert dispatcher.last_error == ""

    asyncio.run(exercise())


def test_managed_worker_activity_renews_on_heartbeat_and_progress_then_times_out(system):
    async def exercise() -> None:
        service = system["service"]
        work = service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title="Activity lease",
                objective="Keep a managed turn alive only while MCP evidence advances",
                acceptance=["Fresh heartbeat and progress renew inactivity"],
                runtime_session_id=system["runtime"]["id"],
            ),
        )
        attempt = work["current_attempt"]
        monitor = _ManagedWorkerActivityMonitor(
            service.db,
            runtime_id=system["runtime"]["id"],
            attempt_id=attempt["id"],
            expected_generation=system["worker"]["_enrollment_generation"],
            startup_timeout_seconds=0.1,
            inactivity_timeout_seconds=0.15,
        )
        initial_activity = monitor._snapshot()
        waiting = asyncio.create_task(monitor.wait_for_failure())
        try:
            await asyncio.sleep(0.06)
            service.heartbeat_runtime(
                system["worker"],
                system["runtime"]["id"],
                RuntimeHeartbeat(
                    expected_enrollment_generation=system["worker"]["_enrollment_generation"],
                    sequence=2,
                ),
            )
            heartbeat_activity = monitor._snapshot()
            assert heartbeat_activity[2] > initial_activity[2]
            await asyncio.sleep(0.06)
            service.report(
                system["worker"],
                attempt["id"],
                ReportInput(
                    kind="progress",
                    expected_goal_version=work["goal_version"],
                    expected_goal_packet_digest=attempt["goal_packet_digest"],
                    expected_task_packet_digest=attempt["task_packet_digest"],
                    expected_generation=work["generation"],
                    summary="Validated the first milestone",
                    stage="validation",
                    next_boundary="complete remaining check",
                ),
            )
            report_activity = monitor._snapshot()
            assert report_activity[3] > heartbeat_activity[3]
            await asyncio.sleep(0.08)
            assert not waiting.done()
            assert await asyncio.wait_for(waiting, timeout=0.15) == "worker_inactive_timeout"
        finally:
            monitor.close()
            if not waiting.done():
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)

    asyncio.run(exercise())


def test_managed_worker_activity_renews_on_exact_bound_codex_turn_events(system):
    async def exercise() -> None:
        service = system["service"]
        work = service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title="Bound turn activity lease",
                objective="Keep a long managed turn alive while its exact App Server events advance",
                acceptance=["Only the current bound turn renews inactivity"],
                runtime_session_id=system["runtime"]["id"],
            ),
        )
        monitor = _ManagedWorkerActivityMonitor(
            service.db,
            runtime_id=system["runtime"]["id"],
            attempt_id=work["current_attempt"]["id"],
            expected_generation=system["worker"]["_enrollment_generation"],
            startup_timeout_seconds=0.1,
            inactivity_timeout_seconds=0.12,
        )
        waiting = asyncio.create_task(monitor.wait_for_failure())
        try:
            await asyncio.sleep(0.07)
            monitor.observe_bound_turn_activity()
            await asyncio.sleep(0.08)
            assert not waiting.done()
            assert await asyncio.wait_for(waiting, timeout=0.1) == "worker_inactive_timeout"
        finally:
            monitor.close()
            if not waiting.done():
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)

    asyncio.run(exercise())


def test_background_dispatcher_refills_while_a_worker_turn_is_still_running(system):
    async def exercise() -> None:
        service = system["service"]
        created = service.create_principal(
            system["cao"], PrincipalCreate(name="worker-refill", role="worker")
        )
        second_runtime, second_worker, _ = enroll_ready_worker_runtime(
            service, system["cao"], created["principal"]["id"]
        )
        first_started = asyncio.Event()
        second_delivered = asyncio.Event()
        release_first = asyncio.Event()

        class BlockingAdapter(EnrollmentHandshakeAdapter):
            async def dispatch(self, runtime, message):
                result = await super().dispatch(runtime, message)
                if str(runtime["id"]) == str(system["runtime"]["id"]):
                    first_started.set()
                    await release_first.wait()
                else:
                    second_delivered.set()
                return result

        dispatcher = Dispatcher(
            service,
            replace(system["settings"], dispatcher_concurrency=2),
            registry=EnrollmentHandshakeRegistry(BlockingAdapter(service)),
        )
        service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title="Long turn",
                objective="Remain active while later work is delivered",
                acceptance=["Second Worker is not blocked"],
                runtime_session_id=system["runtime"]["id"],
            ),
        )
        await dispatcher.start()
        try:
            await asyncio.wait_for(first_started.wait(), timeout=1.0)
            service.assign_work(
                system["cao"],
                WorkAssignment(
                    worker_id=second_worker["id"],
                    title="Later turn",
                    objective="Deliver while the first turn is active",
                    acceptance=["Delivered before first turn exits"],
                    runtime_session_id=second_runtime["id"],
                ),
            )
            await asyncio.wait_for(second_delivered.wait(), timeout=1.0)
            assert not release_first.is_set()
        finally:
            release_first.set()
            await dispatcher.stop()

    asyncio.run(exercise())


def test_assignment_render_uses_provider_owned_output_and_optional_structured_reports() -> None:
    rendered = render_message(
        {
            "kind": "assignment",
            "payload": {
                "title": "Report contract",
                "objective": "Expose progress",
                "completion_contract": "no_artifact_expected",
            },
        }
    )
    assert "Completion contract: no_artifact_expected" in rendered
    assert "Before acting, call cao_get_context" in rendered
    assert "acknowledge the Assignment" in rendered
    assert "automatically" in rendered
    assert "cao_report" in rendered
    assert "always submit one terminal cao_report" not in rendered
    assert "A normal assistant response is not a durable Work report" not in rendered
    assert "first confirm that the terminal cao_report call succeeded" not in rendered


@pytest.mark.parametrize("reason", ["runtime_unavailable", "runtime_dispatch_failed"])
def test_runtime_recovery_render_only_disposes_proven_pre_mcp_failures(
    reason: str,
) -> None:
    rendered = render_message(
        {
            "id": "msg_recovery",
            "kind": "system",
            "work_item_id": "wrk_recovery",
            "attempt_id": "att_recovery",
            "recovery_action": "dispose_continue_or_correct",
            "payload": {
                "action": "recover_terminal_worker_attempt",
                "boundary_id": "bnd_recovery",
                "boundary_kind": "failure",
                "generation": 2,
                "reason": reason,
            },
        }
    )

    assert "dispose this Boundary exactly once with Continue or Correct" in rendered
    assert "same logical Worker thread" in rendered
    assert "prior CAO turn ended" not in rendered
    assert "different target, or New" in rendered


def test_incomplete_reasoner_recovery_render_forbids_same_state_polling() -> None:
    rendered = render_message(
        {
            "id": "msg_incomplete",
            "kind": "system",
            "work_item_id": "wrk_incomplete",
            "attempt_id": "att_incomplete",
            "payload": {
                "action": "recover_incomplete_reasoner_turn",
                "boundary_id": "bnd_incomplete",
                "boundary_kind": "completion_claim",
                "generation": 1,
            },
        }
    )

    assert "Do not wait, sleep, or poll" in rendered
    assert "cao_start" in rendered
    assert "periodic no-change updates" in rendered
    assert "end this turn" in rendered


def test_normal_completion_boundary_render_requires_supervisor_disposition() -> None:
    """A normal Worker claim must never be projected as runtime recovery."""

    rendered = render_message(
        {
            "id": "msg_completion",
            "kind": "completion_claim",
            "work_item_id": "wrk_completion",
            "attempt_id": "att_completion",
            "goal_version": 1,
            "payload": {
                "boundary_id": "bnd_completion",
                "goal_packet_digest": "a" * 64,
                "task_packet_digest": "b" * 64,
                "summary": "No-effect completion marker",
                "trajectory": "complete",
            },
        }
    )

    assert "Supervisor boundary ready for disposition" in rendered
    assert "dispose this boundary exactly once" in rendered
    assert "system-owned recovery Boundary" not in rendered
    assert "do not acquire or dispose this Boundary" not in rendered


@pytest.mark.parametrize(
    ("reason", "recovery_action", "expected"),
    (
        (
            "worker_report_missing",
            "reconcile_continue_same_thread",
            "inspect the current task and workspace state first",
        ),
        (
            "worker_inactive_timeout",
            "system_reconciliation",
            "dispose it as Fail",
        ),
    ),
)
def test_recovery_render_uses_only_the_selected_current_action(
    reason: str, recovery_action: str, expected: str
) -> None:
    rendered = render_message(
        {
            "id": "msg_recovery",
            "kind": "system",
            "work_item_id": "wrk_recovery",
            "attempt_id": "att_recovery",
            "recovery_action": recovery_action,
            "payload": {
                "action": "recover_terminal_worker_attempt",
                "boundary_id": "bnd_recovery",
                "boundary_kind": "failure",
                "generation": 2,
                "reason": reason,
            },
        }
    )

    assert expected in rendered
    if recovery_action == "system_reconciliation":
        assert "another Worker" in rendered
        assert "convert it to requester input" in rendered
    else:
        assert "dispose this boundary exactly once" in rendered.lower()
    assert "Delivery message ID: msg_recovery" in rendered
    assert "owns only the Delivery message ID above" in rendered
    assert "do not drain" in rendered.lower()
    assert "repeatedly read" not in rendered
    assert "prior CAO turn ended" not in rendered


def test_progress_wake_owns_only_its_exact_delivery() -> None:
    rendered = render_message(
        {
            "id": "msg_progress",
            "kind": "progress",
            "work_item_id": "wrk_progress",
            "attempt_id": "att_progress",
            "goal_version": 1,
            "payload": {
                "summary": "One material milestone was committed.",
                "stage": "validation",
                "next_boundary": "completion",
            },
        }
    )

    assert "Delivery message ID: msg_progress" in rendered
    assert "owns only the Delivery message ID above" in rendered
    assert "do not drain" in rendered.lower()
    assert "repeatedly read" not in rendered


def test_instruction_render_binds_report_to_exact_delivery_message() -> None:
    rendered = render_message(
        {
            "id": "msg_0123456789abcdef0123456789abcdef",
            "kind": "instruction",
            "work_item_id": "wrk_example",
            "attempt_id": "att_example",
            "goal_version": 2,
            "payload": {
                "instruction": "Apply the bounded correction.",
                "goal_packet_digest": "a" * 64,
                "task_packet_digest": "b" * 64,
            },
        }
    )

    assert "Delivery message ID: msg_0123456789abcdef0123456789abcdef" in rendered
    assert "incorporated_message_ids" in rendered
    assert "Do not include an instruction that was not incorporated" in rendered


def test_subprocess_dispatcher_delivers_assignment(system):
    service = system["service"]
    runtime = system["runtime"]
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Dispatch",
            objective="Receive prompt",
            acceptance=["Delivered"],
            runtime_session_id=runtime["id"],
        ),
    )
    adapter = EnrollmentHandshakeAdapter(service)
    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
    )
    assert asyncio.run(dispatcher.run_once()) == 1
    delivery = service.db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.kind = 'assignment' ORDER BY d.created_at LIMIT 1
        """
    )
    assert delivery["state"] == "delivered"
    assert len(adapter.deliveries) == 1
    assert adapter.deliveries[0]["runtime_id"] == runtime["id"]
    assert adapter.deliveries[0]["message_id"] == delivery["message_id"]
    persisted = service.get_runtime(runtime["id"])["metadata"]
    assert "cao.ent_" not in str(persisted)
    assert adapter.deliveries[0]["capability_path"] not in str(persisted)


def test_missing_runtime_retries_without_losing_message(system):
    service = system["service"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="No runtime",
            objective="Queue",
            acceptance=["Remain durable"],
        ),
    )
    service.stop_runtime(system["cao"], work["current_attempt"]["runtime_session_id"])
    dispatcher = Dispatcher(service, system["settings"])
    assert asyncio.run(dispatcher.run_once()) == 1
    delivery = service.db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.kind = 'assignment'
        """
    )
    assert delivery["state"] == "queued"
    assert delivery["attempts"] == 1
    assert delivery["last_error"] == "runtime_unavailable"


def test_runtime_lease_expiry(system):
    service = system["service"]
    runtime = system["runtime"]
    service.db.execute(
        "UPDATE runtime_sessions SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?",
        (runtime["id"],),
    )
    assert service.expire_runtime_leases() == 1
    assert service.get_runtime(runtime["id"])["state"] == "missing"


def test_subprocess_output_is_bounded(system):
    from dataclasses import replace

    from cao_control_plane.runtime import SubprocessAdapter

    settings = replace(system["settings"], max_runtime_output_bytes=1024)
    adapter = SubprocessAdapter(settings)
    runtime = {
        "endpoint": "",
        "native_session_id": "",
        "metadata": {
            "command": [
                sys.executable,
                "-c",
                "import sys; sys.stdout.write('x' * 1000000)",
            ]
        },
    }
    result = asyncio.run(
        adapter.dispatch(runtime, {"kind": "instruction", "payload": {"message": "go"}})
    )
    assert result.success is True
    assert len(result.output.encode()) <= 1100
    assert "output truncated" in result.output


def test_dispatcher_uses_configured_concurrency(system):
    service = system["service"]
    workers = [(system["worker"], system["runtime"])]
    for index in range(2, 4):
        created = service.create_principal(
            system["cao"], PrincipalCreate(name=f"worker-{index}", role="worker")
        )
        runtime, worker, _ = enroll_ready_worker_runtime(
            service,
            system["cao"],
            created["principal"]["id"],
        )
        workers.append((worker, runtime))
    for index, (worker, runtime) in enumerate(workers):
        service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=worker["id"],
                title=f"Concurrent {index}",
                objective="Deliver concurrently",
                acceptance=["Delivered"],
                runtime_session_id=runtime["id"],
                idempotency_key=f"concurrent-{index}",
            ),
        )
    adapter = EnrollmentHandshakeAdapter(service)
    dispatcher = Dispatcher(
        service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
    )
    assert asyncio.run(dispatcher.run_once()) == 3
    delivered = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM message_deliveries WHERE state = 'delivered'"
    )
    assert delivered["count"] == 3
    assert {item["runtime_id"] for item in adapter.deliveries} == {
        runtime["id"] for _, runtime in workers
    }


def test_runtime_adapter_output_cannot_claim_worker_completion(system):
    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Delivery is not completion",
            objective="Require a Worker MCP completion claim",
            acceptance=["Adapter output cannot close work"],
            runtime_session_id=runtime["id"],
        ),
    )

    assert (
        asyncio.run(
            Dispatcher(
                service,
                system["settings"],
                registry=EnrollmentHandshakeRegistry(EnrollmentHandshakeAdapter(service)),
            ).run_once()
        )
        == 1
    )
    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert len(current["open_boundaries"]) == 1
    assert (
        current["open_boundaries"][0]["metadata"]["reason"]
        == "worker_turn_completed_without_terminal_report"
    )
    assert current["open_boundaries"][0]["recovery_action"] == "system_reconciliation"
    assert current["current_attempt"]["completion_claim"] == {}

    boundary = current["open_boundaries"][0]
    notification = next(
        item
        for item in service.get_inbox(system["cao"], include_acknowledged=True)["items"]
        if item["payload"].get("boundary_id") == boundary["id"]
    )
    assert notification["payload"]["action"] == "reconcile_completed_worker_turn"
    assert notification["payload"]["generation"] == current["generation"]
    service.acknowledge(
        system["cao"],
        AckInput(message_ids=[notification["id"]]),
    )
    with pytest.raises(ConflictError, match="must be disposed"):
        service.mark_message_handled(
            system["cao"],
            notification["id"],
            evidence="The exact system reconciliation state was observed.",
        )
    turn = service.acquire_reasoner_turn(
        system["cao"],
        work["id"],
        boundary_id=boundary["id"],
        expected_generation=current["generation"],
        idempotency_key="adapter-output-system-reconciliation",
    )
    disposition = service.dispose_boundary(
        system["cao"],
        boundary["id"],
        BoundaryDispositionInput(
            turn_id=turn["id"],
            lease_token=turn["lease_token"],
            expected_generation=current["generation"],
            kind=BoundaryDispositionKind.FAIL,
            reason="The provider turn ended without a terminal Worker report.",
        ),
    )

    assert disposition["kind"] == "fail"
    observed = service.get_work(work["id"])
    assert observed["state"] == "failed"
    assert observed["generation"] == current["generation"] + 1
    assert observed["open_boundaries"] == []
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_dispositions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is not None
    )
    assert (
        service.db.fetchone(
            "SELECT 1 FROM boundary_supersessions WHERE boundary_id = ?",
            (boundary["id"],),
        )
        is None
    )
    delivery = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (notification["id"], system["cao"]["id"]),
    )
    assert delivery is not None and delivery["state"] == "handled"


def test_handled_status_request_does_not_hide_completed_worker_turn(system):
    """Provider dispatch lineage, not later Message sequence, closes the turn."""

    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Status response inside completed turn",
            objective="Report progress after incorporating a later status request",
            acceptance=["The completed provider turn is reconciled immediately"],
            runtime_session_id=runtime["id"],
        ),
    )
    attempt = work["current_attempt"]
    adapter: EnrollmentHandshakeAdapter

    def report_after_later_status(_runtime, message) -> None:
        actor = adapter.actors[-1]
        service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
        service.request_status(
            system["cao"],
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="Report the current observable activity.",
                response_due_seconds=60,
                idempotency_key="status-inside-provider-turn",
            ),
        )
        service.report(
            actor,
            attempt["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=work["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=work["generation"],
                summary="The status request was incorporated before this turn ended.",
                idempotency_key="progress-after-status-request",
            ),
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=report_after_later_status)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
    )

    assert asyncio.run(dispatcher.run_once()) == 1

    current = service.get_work(work["id"])
    assert current["state"] == "waiting_supervisor"
    assert current["current_attempt"]["stage"] == "system_reconciliation"
    assert current["open_boundaries"][0]["metadata"]["reason"] == (
        "worker_turn_completed_without_terminal_report"
    )
    status_delivery = service.db.fetchone(
        """
        SELECT delivery.state
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'status_request'
        """,
        (attempt["id"],),
    )
    assert status_delivery is not None and status_delivery["state"] == "handled"
    persisted_runtime = service.get_runtime(runtime["id"])
    assert persisted_runtime["state"] == "waiting"
    assert (
        persisted_runtime["metadata"]["last_dispatch_message_id"]
        == (adapter.deliveries[0]["message_id"])
    )


def test_unsettled_later_worker_command_blocks_completed_turn_reconciliation(system):
    """A real later command remains Worker-owned until its own provider turn."""

    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Unsettled command after provider dispatch",
            objective="Preserve a later queued status request",
            acceptance=["The later command is not skipped by reconciliation"],
            runtime_session_id=runtime["id"],
        ),
    )
    adapter: EnrollmentHandshakeAdapter

    def queue_later_status(_runtime, message) -> None:
        actor = adapter.actors[-1]
        service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
        service.request_status(
            system["cao"],
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="This request must retain its own execution turn.",
                response_due_seconds=60,
                idempotency_key="unsettled-status-after-provider-turn",
            ),
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=queue_later_status)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
    )

    assert asyncio.run(dispatcher.run_once()) == 1

    current = service.get_work(work["id"])
    assert current["state"] == "active"
    assert current["open_boundaries"] == []
    status_delivery = service.db.fetchone(
        """
        SELECT delivery.state
        FROM messages AS message
        JOIN message_deliveries AS delivery ON delivery.message_id = message.id
        WHERE message.attempt_id = ? AND message.kind = 'status_request'
        """,
        (work["current_attempt"]["id"],),
    )
    assert status_delivery is not None and status_delivery["state"] == "queued"
    assert service.reconcile_completed_worker_turns() == 0


def test_completed_unacknowledged_turn_terminalizes_head_before_successor(system):
    """A completed native turn cannot strand a later durable Worker command."""

    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Unacknowledged turn with successor",
            objective="Advance the durable command lane without replaying the prior turn.",
            acceptance=["The successor becomes the exact FIFO head."],
            runtime_session_id=runtime["id"],
        ),
    )
    successor_message_id = ""

    def queue_successor(_runtime, _message) -> None:
        nonlocal successor_message_id
        service.request_status(
            system["cao"],
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="Inspect state after the completed predecessor turn.",
                response_due_seconds=60,
                idempotency_key="successor-after-unacknowledged-turn",
            ),
        )
        status_message = service.db.fetchone(
            "SELECT id FROM messages WHERE attempt_id = ? AND kind = 'status_request' "
            "ORDER BY sequence DESC LIMIT 1",
            (work["current_attempt"]["id"],),
        )
        assert status_message is not None
        successor_message_id = str(status_message["id"])

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=queue_successor)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    predecessor_message_id = str(adapter.deliveries[0]["message_id"])
    predecessor = service.db.fetchone(
        "SELECT state, last_error, reactivation_policy FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (predecessor_message_id, system["worker"]["id"]),
    )
    assert predecessor is not None
    assert dict(predecessor) == {
        "state": "dead",
        "last_error": "provider_turn_completed_without_instruction_ack",
        "reactivation_policy": "terminal",
    }
    successor = service.db.fetchone(
        "SELECT state FROM message_deliveries WHERE message_id = ? AND recipient_id = ?",
        (successor_message_id, system["worker"]["id"]),
    )
    assert successor is not None and successor["state"] == "queued"

    claimed = dispatcher._claim_delivery()
    assert claimed is not None
    assert claimed["message_id"] == successor_message_id


def test_background_reconciliation_uses_persisted_provider_dispatch_lineage(system, monkeypatch):
    """Upgrade/background repair must not guess the turn from MAX(message.sequence)."""

    service = system["service"]
    runtime = system["runtime"]
    work = service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Persisted provider dispatch lineage",
            objective="Repair a completed turn after its immediate reconciliation was skipped",
            acceptance=["Background repair selects the actual provider-dispatched Message"],
            runtime_session_id=runtime["id"],
        ),
    )
    attempt = work["current_attempt"]
    adapter: EnrollmentHandshakeAdapter

    def report_after_later_status(_runtime, message) -> None:
        actor = adapter.actors[-1]
        service.acknowledge(actor, AckInput(message_ids=[str(message["id"])]))
        service.request_status(
            system["cao"],
            work["id"],
            StatusRequestInput(
                expected_generation=work["generation"],
                summary="Record a later handled control Message.",
                response_due_seconds=60,
                idempotency_key="persisted-lineage-status",
            ),
        )
        service.report(
            actor,
            attempt["id"],
            ReportInput(
                kind="progress",
                expected_goal_version=work["goal_version"],
                expected_goal_packet_digest=attempt["goal_packet_digest"],
                expected_task_packet_digest=attempt["task_packet_digest"],
                expected_generation=work["generation"],
                summary="The later status request was handled in this completed turn.",
                idempotency_key="persisted-lineage-progress",
            ),
        )

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=report_after_later_status)
    dispatcher = Dispatcher(
        service,
        system["settings"],
        registry=EnrollmentHandshakeRegistry(adapter),
    )
    immediate_reconcile = service._reconcile_completed_worker_turn_tx
    monkeypatch.setattr(
        service,
        "_reconcile_completed_worker_turn_tx",
        lambda *_args, **_kwargs: None,
    )

    assert asyncio.run(dispatcher.run_once()) == 1
    assert service.get_work(work["id"])["state"] == "active"
    monkeypatch.setattr(
        service,
        "_reconcile_completed_worker_turn_tx",
        immediate_reconcile,
    )

    assert service.reconcile_completed_worker_turns() == 1
    repaired = service.get_work(work["id"])
    assert repaired["state"] == "waiting_supervisor"
    assert repaired["open_boundaries"][0]["metadata"]["reason"] == (
        "worker_turn_completed_without_terminal_report"
    )


def test_dispatch_result_persistence_allowlists_diagnostics_and_discards_conversation_data(system):
    """Adapter responses are transport-only, never a durable transcript channel."""

    service = system["service"]
    runtime = system["runtime"]
    sentinels = {
        "prompt": "runtime-raw-prompt-sentinel",
        "model_output": "runtime-model-output-sentinel",
        "account_limits": "runtime-account-limit-sentinel",
        "native_id": "runtime-native-notification-id-sentinel",
        "tokens": "runtime-token-usage-sentinel",
        "path": "/owner/private/runtime-path-sentinel",
        "raw_event": "runtime-raw-event-payload-sentinel",
    }
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Minimal durable runtime diagnostics",
            objective="Keep adapter conversation output out of durable state.",
            acceptance=["Only the allowlisted dispatch summary is persisted."],
            runtime_session_id=runtime["id"],
        ),
    )

    class NoisyAdapter(EnrollmentHandshakeAdapter):
        async def dispatch(self, launch, message):
            await super().dispatch(launch, message)
            return RuntimeDispatchResult(
                success=True,
                native_session_id="runtime-native-session-core-field",
                state="ready",
                output=json.dumps(sentinels),
                metadata={
                    "event_count": 17,
                    "turn_status": "completed",
                    "delivery_method": "thread_queue",
                    "delivery_acceptance": "completed",
                    "server": dict(sentinels),
                    "turn_id": sentinels["native_id"],
                    "server_request_methods": [sentinels["raw_event"]],
                },
            )

    adapter = NoisyAdapter(service)
    assert (
        asyncio.run(
            Dispatcher(
                service,
                system["settings"],
                registry=EnrollmentHandshakeRegistry(adapter),
            ).run_once()
        )
        == 1
    )

    runtime_metadata = service.get_runtime(runtime["id"])["metadata"]
    assert runtime_metadata["last_dispatch"] == {
        "success": True,
        "state": "ready",
        "diagnostics": {
            "event_count": 17,
            "turn_status": "completed",
            "delivery_method": "thread_queue",
            "delivery_acceptance": "completed",
        },
    }
    assert "last_output" not in runtime_metadata
    event = service.db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'runtime.message_delivered' "
        "ORDER BY sequence DESC LIMIT 1"
    )
    assert event is not None
    event_payload = json.loads(str(event["data_json"]))
    assert event_payload["result"] == runtime_metadata["last_dispatch"]

    # Check all result/error persistence sinks rather than merely the public
    # runtime view.  The core native-session pointer is deliberately outside
    # this summary contract; no notification-provided native ID is retained.
    sinks = (
        ("runtime_sessions", "metadata_json"),
        ("events", "data_json"),
        ("message_deliveries", "last_error"),
        ("idempotency_results", "result_json"),
    )
    for sentinel in sentinels.values():
        for table, column in sinks:
            row = service.db.fetchone(
                f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
                (f"%{sentinel}%",),
            )
            assert row is not None
            assert row["count"] == 0, (table, column, sentinel)


def test_dispatch_failure_persists_only_a_fixed_mcp_startup_code(system, monkeypatch):
    service = system["service"]
    runtime = system["runtime"]
    # Second-resolution timestamps can tie across the dispatch and its CAO wake.
    import cao_control_plane.runtime as runtime_module
    import cao_control_plane.service as service_module

    fixed_now = service_module.utc_now()
    monkeypatch.setattr(service_module, "utc_now", lambda: fixed_now)
    monkeypatch.setattr(runtime_module, "utc_now", lambda: fixed_now)
    dispatched_messages = []
    raw_failure = (
        "managed Codex MCP server readiness timed out: cao_control_plane; "
        "last status: runtime-raw-mcp-startup-payload-sentinel"
    )
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="MCP failure code",
            objective="Persist a fixed diagnostic code, not app-server failure text.",
            acceptance=["A raw startup payload cannot enter durable error state."],
            runtime_session_id=runtime["id"],
        ),
    )

    class FailedAdapter:
        async def dispatch(self, _runtime, _message):
            dispatched_messages.append(_message["id"])
            return RuntimeDispatchResult(
                success=False,
                state="failed",
                output="runtime-raw-model-output-on-failure-sentinel",
                error=raw_failure,
                metadata={
                    "delivery_acceptance": "not_submitted",
                    "dispatch_phase": "mcp_startup",
                    "raw": "runtime-raw-event-on-failure-sentinel",
                },
            )

    class Registry:
        def get(self, _name):
            return FailedAdapter()

    assert asyncio.run(Dispatcher(service, system["settings"], registry=Registry()).run_once()) == 1
    assert len(dispatched_messages) == 1
    delivery = service.db.fetchone(
        "SELECT message_id, last_error FROM message_deliveries "
        "WHERE message_id = ? AND recipient_id = ?",
        (dispatched_messages[0], system["worker"]["id"]),
    )
    assert delivery is not None
    assert delivery["last_error"] == "runtime_dispatch_pre_submit_failed"
    runtime_metadata = service.get_runtime(runtime["id"])["metadata"]
    assert runtime_metadata["last_dispatch_message_id"] == delivery["message_id"]
    assert runtime_metadata["last_dispatch"] == {
        "success": False,
        "state": "failed",
        "diagnostics": {
            "delivery_acceptance": "not_submitted",
            "dispatch_phase": "mcp_startup",
            "failure_code": "mcp_startup_timeout",
            "mcp_startup_failure_code": "mcp_startup_timeout",
        },
    }
    not_submitted = service.db.fetchone(
        "SELECT data_json FROM events WHERE event_type = 'runtime.message_not_submitted' "
        "AND aggregate_id = ?",
        (delivery["message_id"],),
    )
    assert not_submitted is not None
    not_submitted_data = json.loads(str(not_submitted["data_json"]))
    assert not_submitted_data["dispatch_phase"] == "mcp_startup"
    assert not_submitted_data["failure_code"] == "mcp_startup_timeout"
    for sentinel in (
        "runtime-raw-mcp-startup-payload-sentinel",
        "runtime-raw-model-output-on-failure-sentinel",
        "runtime-raw-event-on-failure-sentinel",
    ):
        for table, column in (
            ("runtime_sessions", "metadata_json"),
            ("events", "data_json"),
            ("message_deliveries", "last_error"),
        ):
            row = service.db.fetchone(
                f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
                (f"%{sentinel}%",),
            )
            assert row is not None
            assert row["count"] == 0, (table, column, sentinel)


def test_delivery_failure_helpers_reject_arbitrary_diagnostics_from_durable_sinks(system):
    """Retry/dead and unknown paths share a fixed-code-only audit boundary."""

    service = system["service"]
    dispatcher = Dispatcher(service, system["settings"])
    sentinel = (
        "delivery-raw-prompt=/private/worker/path token=cao.ent_delivery_failure_sink_sentinel"
    )

    def assign(title: str) -> None:
        service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title=title,
                objective="Exercise the durable delivery failure boundary.",
                acceptance=["Fixed code only"],
                runtime_session_id=system["runtime"]["id"],
            ),
        )

    assign("Retry/dead sink")
    leased = dispatcher._claim_delivery()
    assert leased is not None
    dispatcher._finish_delivery(
        leased,
        success=False,
        error_code=sentinel,
        dead=True,
    )

    assign("Unknown sink")
    dispatched = dispatcher._claim_delivery()
    assert dispatched is not None
    service.db.execute(
        """
        UPDATE message_deliveries
        SET state = 'dispatched'
        WHERE message_id = ? AND recipient_id = ? AND generation = ?
          AND state = 'leased' AND owner_token = ?
        """,
        (
            dispatched["message_id"],
            dispatched["recipient_id"],
            dispatched["generation"],
            dispatcher.owner_token,
        ),
    )
    dispatcher._mark_delivery_unknown(dispatched, error_code=sentinel)

    rows = service.db.fetchall("SELECT last_error FROM message_deliveries WHERE last_error <> ''")
    assert rows
    assert {str(row["last_error"]) for row in rows} == {"runtime_dispatch_failed"}
    event_rows = service.db.fetchall(
        """
        SELECT data_json FROM events
        WHERE event_type IN (
            'runtime.message_dead',
            'runtime.message_delivery_unknown'
        )
        """
    )
    assert len(event_rows) == 2
    for event in event_rows:
        payload = json.loads(str(event["data_json"]))
        assert payload["failure_code"] == "runtime_dispatch_failed"
        assert "error" not in payload
        assert "error_digest" not in payload
    for table, column in (
        ("message_deliveries", "last_error"),
        ("events", "data_json"),
        ("runtime_sessions", "metadata_json"),
    ):
        row = service.db.fetchone(
            f"SELECT COUNT(*) AS count FROM {table} WHERE {column} LIKE ?",
            (f"%{sentinel}%",),
        )
        assert row is not None
        assert row["count"] == 0, (table, column)


def test_codex_adapter_discards_raw_notification_payloads_before_returning() -> None:
    """The app-server adapter itself must not build a transcript summary."""

    from cao_control_plane.runtime import CodexAppServerAdapter

    sentinel = "raw-app-server-notification-transcript-and-usage-sentinel"
    server_program = (
        r'''
import json
import sys

for raw in sys.stdin:
    request = json.loads(raw)
    method = request.get("method")
    request_id = request.get("id")
    if request_id is None:
        continue
    if method == "initialize":
        result = {}
    elif method == "thread/start":
        result = {"thread": {"id": "native-thread-id-not-for-diagnostics"}}
    elif method == "turn/start":
        print(json.dumps({"id": request_id, "result": {"turn": {"id": "native-turn-id-not-for-diagnostics"}}}), flush=True)
        print(json.dumps({"method": "item/completed", "params": {"item": {"text": "'''
        + sentinel
        + r'''", "usage": {"tokens": 99999}, "rateLimit": "private"}}}), flush=True)
        print(json.dumps({"method": "turn/completed", "params": {"turn": {"id": "native-turn-id-not-for-diagnostics", "status": "completed", "output": "'''
        + sentinel
        + r""""}}}), flush=True)
        continue
    else:
        result = {}
    print(json.dumps({"id": request_id, "result": result}), flush=True)
"""
    )
    settings = Settings(runtime_timeout_seconds=2.0)
    result = asyncio.run(
        CodexAppServerAdapter(settings).dispatch(
            {
                "native_session_id": "",
                "metadata": {
                    "command": [sys.executable, "-u", "-c", server_program],
                    "timeout_seconds": 2.0,
                },
            },
            {"kind": "instruction", "payload": {"message": "private prompt is transport-only"}},
        )
    )
    assert result.success is True
    assert result.output == ""
    assert result.error == ""
    assert result.metadata == {
        "turn_status": "completed",
        "event_count": 2,
        "delivery_method": "direct_turn",
        "delivery_acceptance": "completed",
    }
    assert sentinel not in str(result.model_dump(mode="json"))
    assert "native-turn-id-not-for-diagnostics" not in str(result.metadata)


def test_dispatch_failure_after_reservation_is_unknown_until_verified(system):
    service = system["service"]
    runtime = system["runtime"]
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Unknown delivery",
            objective="Do not duplicate delivery",
            acceptance=["Unknown is verified before retry"],
            runtime_session_id=runtime["id"],
        ),
    )

    class FailedAdapter(EnrollmentHandshakeAdapter):
        async def dispatch(self, runtime, message):
            await super().dispatch(runtime, message)
            return RuntimeDispatchResult(success=False, state="failed", error="connection lost")

    class FailedRegistry:
        def get(self, name):
            assert name == "claude"
            return FailedAdapter(service)

    dispatcher = Dispatcher(service, system["settings"], registry=FailedRegistry())
    assert asyncio.run(dispatcher.run_once()) == 1
    delivery = service.db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.kind = 'assignment'
        """
    )
    assert delivery["state"] == "dispatched"
    assert dispatcher.status()["unknown_delivery_outcomes"] == 1
    assert asyncio.run(dispatcher.run_once()) == 0

    service.resolve_delivery(
        system["cao"],
        delivery["message_id"],
        DeliveryResolveInput(
            recipient_id=delivery["recipient_id"],
            outcome="not_delivered",
            evidence="target history contains no matching message digest",
        ),
    )
    assert asyncio.run(Dispatcher(service, system["settings"]).run_once()) == 0
    retried = service.db.fetchone(
        """
        SELECT d.* FROM message_deliveries AS d
        JOIN messages AS m ON m.id = d.message_id
        WHERE m.kind = 'assignment'
        """
    )
    assert retried["state"] == "dead"
    assert retried["last_error"] == "system_reconciliation_required"
    superseded = service.db.fetchone(
        "SELECT COUNT(*) AS count FROM events "
        "WHERE event_type = 'message.delivery_superseded' "
        "AND aggregate_id = ? "
        "AND json_extract(data_json, '$.reason_code') = "
        "'system_reconciliation_required'",
        (delivery["message_id"],),
    )
    assert superseded is not None and superseded["count"] == 1
    assert service.get_runtime(runtime["id"])["state"] == "failed"


def test_delivery_is_durably_dispatched_before_adapter_crosses_runtime_boundary(system):
    service = system["service"]
    runtime = system["runtime"]
    service.assign_work(
        system["cao"],
        WorkAssignment(
            worker_id=system["worker"]["id"],
            title="Dispatch fence",
            objective="Commit before invoking the adapter",
            acceptance=["Dispatch is fenced"],
            runtime_session_id=runtime["id"],
        ),
    )

    def assert_dispatched(runtime, message):
        del runtime, message
        row = service.db.fetchone("SELECT state FROM message_deliveries")
        assert row["state"] == "dispatched"

    adapter = EnrollmentHandshakeAdapter(service, after_handshake=assert_dispatched)

    assert (
        asyncio.run(
            Dispatcher(
                service, system["settings"], registry=EnrollmentHandshakeRegistry(adapter)
            ).run_once()
        )
        == 1
    )
    assert (
        service.db.fetchone(
            "SELECT delivery.state FROM message_deliveries AS delivery "
            "JOIN messages AS message ON message.id = delivery.message_id "
            "WHERE message.kind = 'assignment'"
        )["state"]
        == "delivered"
    )
