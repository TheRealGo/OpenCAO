from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from test_dashboard_lifecycle import (
    FakeProbe,
    FakeRunner,
    FakeUpgradeProbe,
    FakeUpgradeProcessController,
    _upgrade_plan,
)

from cao_control_plane import dashboard_lifecycle as lifecycle_module
from cao_control_plane.config import Settings
from cao_control_plane.connection_contract import CAO_CONVERSATION_PROXY_ABI_VERSION
from cao_control_plane.dashboard_cli import main
from cao_control_plane.dashboard_lifecycle import (
    CommandResult,
    DashboardUpgradePreflightResult,
    HttpResult,
    HttpxDashboardUpgradeProbe,
    LifecycleUnknownOutcome,
    build_dashboard_upgrade_plan,
    upgrade_dashboard_lifecycle,
)
from cao_control_plane.database import Database, backup_sqlite_database, inspect_sqlite_database


def _services_plan(tmp_path, monkeypatch):
    settings, lifecycle, previous = _upgrade_plan(tmp_path)
    database = Database(Settings(state_dir=tmp_path, runtime_launch_dir=tmp_path / "launches"))
    with database.transaction() as connection:
        connection.executemany(
            "INSERT OR REPLACE INTO metadata(key,value) VALUES(?,?)",
            [("conversation_mcp_catalog_digest", previous.target_identity.mcp_catalog_digest),
             ("conversation_mcp_proxy_abi_version", str(CAO_CONVERSATION_PROXY_ABI_VERSION))],
        )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("A services-only update must not depend on or operate Codex")

    for name in ("discover_codex_desktop_launchagent_binding", "canonical_codex_app_server_executable",
                 "_checked_codex_socket", "preflight_codex_app_server_mcp"):
        monkeypatch.setattr(lifecycle_module, name, forbidden)
    plan = build_dashboard_upgrade_plan(
        lifecycle, database_path=database.path,
        backup_destination=Path(previous.backup_destination), credentials_file=settings.credentials_file,
        source_identity=inspect_sqlite_database(database.path), target_identity=previous.target_identity,
        control_plane_plist_path=Path(previous.control_plane.plist_path),
        control_plane_plist_sha256=previous.control_plane.plist_sha256,
        edge_plist_path=Path(previous.edge.plist_path), edge_plist_sha256=previous.edge.plist_sha256,
        services_only=True,
    )
    return database, lifecycle, plan


def test_services_only_update_preserves_controlled_service_lifecycle_without_codex(tmp_path, monkeypatch):
    _, _, plan = _services_plan(tmp_path, monkeypatch)
    assert plan.codex_mcp_reload_scope == "none"
    assert plan.codex_app_server_socket == ""
    trace = []
    responses = {}
    for binding in (plan.control_plane, plan.edge):
        for action in ("bootout", "bootstrap"):
            command = ("launchctl", action, f"gui/{lifecycle_module.os.getuid()}", binding.plist_path)
            responses[command] = CommandResult(command, 0)
        command = ("launchctl", "print", f"gui/{lifecycle_module.os.getuid()}/{binding.label}")
        responses[command] = CommandResult(command, 1)
    runner = FakeRunner(responses, trace=trace)

    def backup(source, destination, *, replace=True):
        assert not replace
        trace.append("backup:final-offline")
        return backup_sqlite_database(source, destination, replace=False)

    result = upgrade_dashboard_lifecycle(
        plan, runner, probe=FakeUpgradeProbe(trace), process_controller=FakeUpgradeProcessController(trace),
        preflight=lambda *_: DashboardUpgradePreflightResult(
            plan.source_identity, plan.source_identity, True,
            plan.control_plane.plist_sha256, plan.edge.plist_sha256,
        ),
        backup_database=backup,
        reload_codex_mcp=lambda *_: pytest.fail("no Codex reload is in this scope"),
        codex_host_preflight=lambda *_: pytest.fail("no Codex lifecycle is in this scope"),
        execute=True, owner_confirmed=True,
    )
    assert result.status == "services_ready"
    assert result.backup_retained is False
    assert not Path(plan.backup_destination).exists()
    assert result.codex_app_server_restart == "not_performed"
    assert result.codex_mcp_reload == "not_requested"
    assert result.current_conversation_verification == "not_requested"
    assert trace.index("process:fenced") < trace.index("backup:final-offline") < trace.index("probe:control-plane")
    assert trace[-1] == "probe:edge"


def _native_unavailable_readiness(plan):
    return {
        "ready": False,
        **plan.target_identity.to_dict(),
        "database": {"ok": True, "integrity": ["ok"], "foreign_key_errors": []},
        "authority": {"mode": "canonical"},
        "projection": {"healthy": True, "violations": []},
        "dispatcher": {
            "healthy": False, "running": True, "last_error": None,
            "issues": ["native_change_subscription_unavailable"],
        },
    }


def _readiness_probe(plan, body, *, status_code=503, health_identity=None):
    return HttpxDashboardUpgradeProbe(FakeProbe({
        plan.control_plane_origin + "/health": HttpResult(
            200, {"content-type": "application/json"},
            {"status": "ok", **(health_identity or plan.target_identity.to_dict())},
        ),
        plan.control_plane_origin + "/ready": HttpResult(
            status_code, {"content-type": "application/json"}, body,
        ),
    }), sleeper=lambda _: None)


@pytest.mark.parametrize("services_only", [True, False])
def test_only_explicit_service_scope_can_finish_with_native_subscription_unavailable(
    tmp_path, monkeypatch, services_only,
):
    if services_only:
        _, _, plan = _services_plan(tmp_path, monkeypatch)
    else:
        _, _, plan = _upgrade_plan(tmp_path)
    plan = replace(plan, readiness_attempts=1)
    body = _native_unavailable_readiness(plan)
    before = json.dumps(body, sort_keys=True)
    if services_only:
        _readiness_probe(plan, body).wait_for_control_plane(plan)
    else:
        with pytest.raises(LifecycleUnknownOutcome, match="Control Plane"):
            _readiness_probe(plan, body).wait_for_control_plane(plan)
    assert json.dumps(body, sort_keys=True) == before  # global readiness remains degraded


@pytest.mark.parametrize("section,field,value", [
    (None, "release_id", "other-release"),
    (None, "schema_version", 0),
    (None, "mcp_catalog_digest", "0" * 64),
    (None, "ready", True),
    ("database", "ok", False),
    ("database", "integrity", ["corrupt"]),
    ("database", "foreign_key_errors", ["violation"]),
    ("authority", "mode", "legacy"),
    ("projection", "healthy", False),
    ("projection", "violations", ["violation"]),
    ("dispatcher", "running", False),
    ("dispatcher", "last_error", "cycle_failed"),
    ("dispatcher", "issues", []),
    ("dispatcher", "issues", ["native_change_subscription_unavailable", "dispatcher_stale"]),
])
def test_service_scope_does_not_hide_cao_service_faults_or_target_identity_drift(
    tmp_path, monkeypatch, section, field, value,
):
    _, _, plan = _services_plan(tmp_path, monkeypatch)
    plan = replace(plan, readiness_attempts=1)
    body = _native_unavailable_readiness(plan)
    target = body if section is None else body[section]
    target[field] = value
    with pytest.raises(LifecycleUnknownOutcome, match="Control Plane"):
        _readiness_probe(plan, body).wait_for_control_plane(plan)


def test_service_scope_still_requires_health_identity_and_truthful_ready_status(tmp_path, monkeypatch):
    _, _, plan = _services_plan(tmp_path, monkeypatch)
    plan = replace(plan, readiness_attempts=1)
    body = _native_unavailable_readiness(plan)
    with pytest.raises(LifecycleUnknownOutcome, match="Control Plane"):
        _readiness_probe(plan, body, status_code=200).wait_for_control_plane(plan)
    with pytest.raises(LifecycleUnknownOutcome, match="Control Plane"):
        _readiness_probe(plan, body, health_identity={"release_id": "other-release"}).wait_for_control_plane(plan)


@pytest.mark.parametrize("key,value", [
    ("conversation_mcp_catalog_digest", "d" * 64),
    ("conversation_mcp_proxy_abi_version", "0"),
])
def test_services_only_update_rechecks_catalog_and_proxy_abi_before_effects(tmp_path, monkeypatch, key, value):
    database, _, plan = _services_plan(tmp_path, monkeypatch)
    database.execute("UPDATE metadata SET value=? WHERE key=?", (value, key))
    runner = FakeRunner({})
    with pytest.raises(ValueError, match=r"unchanged.*catalog.*proxy ABI"):
        upgrade_dashboard_lifecycle(
            plan, runner, probe=FakeUpgradeProbe([]), execute=True, owner_confirmed=True,
            preflight=lambda *_: pytest.fail("contract drift must stop before migration or effects"),
        )
    assert runner.calls == []


def test_services_only_scope_is_bound_to_the_reviewed_plan(tmp_path, monkeypatch):
    _, _, plan = _services_plan(tmp_path, monkeypatch)
    runner = FakeRunner({})
    with pytest.raises(ValueError, match="reviewed digest"):
        upgrade_dashboard_lifecycle(
            replace(plan, codex_mcp_reload_scope="all_loaded_codex_threads"), runner,
            probe=FakeUpgradeProbe([]), execute=True, owner_confirmed=True,
        )
    assert runner.calls == []


@pytest.mark.parametrize("command", ["upgrade", "restart"])
def test_services_only_cli_plan_needs_no_codex_host(tmp_path, monkeypatch, capsys, command):
    database, lifecycle, plan = _services_plan(tmp_path, monkeypatch)
    monkeypatch.setattr(lifecycle_module, "current_release_identity", lambda: plan.target_identity)
    before = database.path.read_bytes()
    arguments = [
        "lifecycle", command,
        "--application-support-dir", str(tmp_path / "support"),
        "--working-directory", lifecycle.control_plane.working_directory,
        "--control-plane-command-json", json.dumps(list(lifecycle.control_plane.program_arguments[
            :lifecycle.control_plane.program_arguments.index("serve")
        ])),
        "--credentials-file", plan.credentials_file,
        "--bootstrap-record-dir", str(tmp_path / "bootstrap"),
        "--session-record-dir", str(tmp_path / "sessions"),
        "--database-path", plan.database_path,
        "--backup-destination", plan.backup_destination,
        "--control-plane-plist-path", plan.control_plane.plist_path,
        "--control-plane-plist-sha256", plan.control_plane.plist_sha256,
        "--edge-plist-path", plan.edge.plist_path,
        "--edge-plist-sha256", plan.edge.plist_sha256,
        "--services-only", "--plan-only",
    ]
    assert main(arguments) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["codex_mcp_reload_scope"] == "none"
    assert rendered["codex_mcp_reload_application"] == "not_requested"
    assert database.path.read_bytes() == before


def test_services_only_rechecks_contract_after_migration_before_stopping_services(tmp_path, monkeypatch):
    database, _, plan = _services_plan(tmp_path, monkeypatch)
    runner = FakeRunner({})

    def preflight(*_args):
        database.execute(
            "UPDATE metadata SET value=? WHERE key='conversation_mcp_catalog_digest'", ("d" * 64,),
        )
        return DashboardUpgradePreflightResult(
            plan.source_identity, plan.source_identity, True,
            plan.control_plane.plist_sha256, plan.edge.plist_sha256,
        )

    with pytest.raises(ValueError, match=r"unchanged.*catalog.*proxy ABI"):
        upgrade_dashboard_lifecycle(
            plan, runner, probe=FakeUpgradeProbe([]), preflight=preflight,
            execute=True, owner_confirmed=True,
        )
    assert runner.calls == []
