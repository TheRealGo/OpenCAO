from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

from enrollment_helpers import enroll_ready_worker_runtime

from cao_control_plane.database import Database
from cao_control_plane.effects import EffectExecutor, effect_argv_digest, effect_workdir_digest
from cao_control_plane.models import (
    DeliveryResolveInput,
    EffectCheckInput,
    PrincipalCreate,
    WorkAssignment,
)
from cao_control_plane.runtime import Dispatcher
from cao_control_plane.service import ControlPlane


def _child_environment(project_root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    source_root = str(project_root / "src")
    current_pythonpath = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        f"{source_root}{os.pathsep}{current_pythonpath}" if current_pythonpath else source_root
    )
    return environment


def _stop_process_group(process_id: int) -> None:
    try:
        os.killpg(process_id, signal.SIGTERM)
    except ProcessLookupError:
        return


def test_dispatch_crash_reopen_needs_explicit_not_delivered_resolution(
    system, tmp_path: Path
):
    """A dispatcher death after durable DISPATCHED never creates a blind retry."""

    service = system["service"]
    project_root = Path(__file__).resolve().parents[1]
    marker_path = tmp_path / "adapter.pid"
    child: subprocess.Popen[str] | None = None
    adapter_pid: int | None = None
    try:
        launch_program = "\n".join(
            (
                "import json",
                "import asyncio",
                "import os",
                "import pathlib",
                "import signal",
                "import sys",
                "from cao_control_plane.config import Settings",
                "from cao_control_plane.database import Database",
                "from cao_control_plane.models import RuntimeHeartbeat",
                "from cao_control_plane.runtime_enrollment import receive_enrollment_capability",
                "from cao_control_plane.service import ControlPlane, WORKER_MCP_REQUIRED_TOOLS",
                "config = json.loads(sys.argv[sys.argv.index('--mcp-config') + 1])",
                "broker_path = config['mcpServers']['cao_control_plane']['args'][-1]",
                "settings = Settings(state_dir=pathlib.Path(os.environ['CAO_TEST_STATE_DIR']), public_base_url='http://127.0.0.1:8768')",
                "service = ControlPlane(Database(settings), settings)",
                "exchange = asyncio.run(receive_enrollment_capability(broker_path, timeout_seconds=10))",
                "actor = service.authenticate(exchange['token'])",
                "service.record_mcp_tool_discovery(actor, protocol_version='2025-06-18', tool_names=WORKER_MCP_REQUIRED_TOOLS)",
                "service.heartbeat_runtime(actor, exchange['runtime_id'], RuntimeHeartbeat(expected_enrollment_generation=actor['_enrollment_generation'], sequence=1))",
                "pathlib.Path(os.environ['CAO_TEST_CRASH_MARKER']).write_text(f'{os.getpid()}\\n', encoding='utf-8')",
                "signal.pause()",
            )
        )
        service.stop_runtime(system["cao"], system["runtime"]["id"])
        runtime, _, _ = enroll_ready_worker_runtime(
            service,
            system["cao"],
            system["worker"]["id"],
            adapter="claude",
            metadata={
                "command": [sys.executable, "-c", launch_program],
                "environment": {
                    "CAO_TEST_CRASH_MARKER": str(marker_path),
                    "CAO_TEST_STATE_DIR": str(system["settings"].state_dir),
                },
                "timeout_seconds": 60,
            },
        )
        service.assign_work(
            system["cao"],
            WorkAssignment(
                worker_id=system["worker"]["id"],
                title="Crash fence",
                objective="Preserve an unknown delivery across dispatcher loss",
                acceptance=["No automatic replay after DISPATCHED"],
                runtime_session_id=runtime["id"],
            ),
        )
        dispatcher_program = "\n".join(
            (
                "import asyncio",
                "import sys",
                "from pathlib import Path",
                "from cao_control_plane.config import Settings",
                "from cao_control_plane.database import Database",
                "from cao_control_plane.runtime import Dispatcher",
                "from cao_control_plane.service import ControlPlane",
                "settings = Settings(state_dir=Path(sys.argv[1]), runtime_launch_dir=Path(sys.argv[2]), dispatcher_lease_seconds=60)",
                "service = ControlPlane(Database(settings), settings)",
                "asyncio.run(Dispatcher(service, settings).run_once())",
            )
        )
        child = subprocess.Popen(
            [
                sys.executable,
                "-c",
                dispatcher_program,
                str(system["settings"].state_dir),
                str(system["settings"].runtime_launch_dir),
            ],
            cwd=project_root,
            env=_child_environment(project_root),
            text=True,
        )
        deadline = time.monotonic() + 10
        while not marker_path.exists():
            if child.poll() is not None:
                raise AssertionError("dispatcher exited before the adapter started")
            if time.monotonic() >= deadline:
                raise AssertionError("adapter did not reach the dispatch boundary")
            time.sleep(0.01)
        adapter_pid = int(marker_path.read_text(encoding="utf-8").strip())

        dispatched = service.db.fetchone(
            """
            SELECT d.* FROM message_deliveries AS d
            JOIN messages AS m ON m.id = d.message_id
            WHERE m.kind = 'assignment'
            """
        )
        assert dispatched is not None
        assert dispatched["state"] == "dispatched"
        generation_before_resolution = dispatched["generation"]

        child.terminate()
        assert child.wait(timeout=10) != 0
        reopened = ControlPlane(Database(system["settings"]), system["settings"])
        persisted = reopened.db.fetchone(
            """
            SELECT d.* FROM message_deliveries AS d
            JOIN messages AS m ON m.id = d.message_id
            WHERE m.kind = 'assignment'
            """
        )
        assert persisted is not None
        assert persisted["state"] == "dispatched"
        assert persisted["generation"] == generation_before_resolution
        assert Dispatcher(reopened, system["settings"]).status()["unknown_delivery_outcomes"] == 1
        assert asyncio.run(Dispatcher(reopened, system["settings"]).run_once()) == 0

        resolved = reopened.resolve_delivery(
            system["cao"],
            persisted["message_id"],
            DeliveryResolveInput(
                recipient_id=persisted["recipient_id"],
                outcome="not_delivered",
                evidence="the killed dispatcher did not record an adapter result",
            ),
        )
        assert resolved["state"] == "queued"
        assert resolved["generation"] == generation_before_resolution + 1
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait(timeout=10)
        if adapter_pid is not None:
            _stop_process_group(adapter_pid)


def test_effect_reservation_crash_is_unknown_after_control_plane_reopen(system, tmp_path: Path):
    """A STARTED effect reservation is fenced as UNKNOWN after a real process exit."""

    settings = replace(system["settings"], state_dir=tmp_path / "effect-state")
    settings.ensure_directories()
    service = ControlPlane(Database(settings), settings)
    bootstrap = service.bootstrap()
    cao = service.authenticate(bootstrap["tokens"]["cao"]["token"])
    worker = service.authenticate(
        service.create_principal(
            cao,
            PrincipalCreate(name="effect-worker", role="worker"),
        )["token"]
    )
    project_root = Path(__file__).resolve().parents[1]
    effect_program = "\n".join(
        (
            "import os",
            "import sys",
            "from pathlib import Path",
            "from cao_control_plane.config import Settings",
            "from cao_control_plane.database import Database",
            "from cao_control_plane.effects import effect_argv_digest, effect_workdir_digest",
            "from cao_control_plane.models import EffectCheckInput",
            "from cao_control_plane.service import ControlPlane",
            "state_dir, worker_id, workdir = map(Path, sys.argv[1:])",
            "settings = Settings(state_dir=state_dir)",
            "service = ControlPlane(Database(settings), settings)",
            "service.start_effect({'id': str(worker_id), 'role': 'worker'}, EffectCheckInput("
            "principal_id=str(worker_id), kind='local', target='local:crash-reopen', action='run', "
            "argv_digest=effect_argv_digest(('/usr/bin/true',)), "
            "workdir_digest=effect_workdir_digest(workdir)))",
            "os._exit(0)",
        )
    )
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            effect_program,
            str(settings.state_dir),
            worker["id"],
            str(tmp_path),
        ],
        cwd=project_root,
        env=_child_environment(project_root),
        check=True,
        text=True,
    )
    assert child.returncode == 0

    reopened = ControlPlane(Database(settings), settings)
    assert EffectExecutor(reopened).recover_incomplete(cao) == 1
    operation = reopened.db.fetchone("SELECT * FROM effect_operations")
    assert operation is not None
    assert operation["status"] == "unknown"
    decision = reopened.check_effect(
        EffectCheckInput(
            principal_id=worker["id"],
            kind="local",
            target="local:crash-reopen",
            action="run",
            argv_digest=effect_argv_digest(("/usr/bin/true",)),
            workdir_digest=effect_workdir_digest(tmp_path),
        )
    )
    assert decision["decision"] == "verify_remote"
