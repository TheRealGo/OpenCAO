"""Opt-in release check with a real Codex Worker and the documented local install."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from test_local_setup import _ports
from test_vendor_model_mcp_e2e import (
    LIVE_RUN_ENV,
    _create_persisted_native_cao_thread,
    _logical_control_plane_secrets,
    _require_subscription_auth,
)
from test_worker_mcp_live_e2e import _close_process, _request, _send, _structured, _tool_call

from cao_control_plane.config import Settings
from cao_control_plane.local_setup import local_dashboard_link, setup_local
from cao_control_plane.runtime import _default_codex_app_server_command, _JsonRpcProcess
from cao_control_plane.security import CONTROL_PLANE_SECRET_PATTERN


async def _archive_disposable_thread(thread_id: str) -> None:
    process = await asyncio.create_subprocess_exec(
        *_default_codex_app_server_command(),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    rpc = _JsonRpcProcess(process, trusted_mcp_servers=frozenset())
    try:
        await rpc.request(
            "initialize", {"clientInfo": {"name": "cao-release-cleanup", "version": "1"}}
        )
        await rpc.send({"method": "initialized", "params": {}})
        await rpc.request("thread/archive", {"threadId": thread_id})
    finally:
        if process.returncode is None:
            process.terminate()
        await process.wait()


@pytest.mark.skipif(
    os.environ.get(LIVE_RUN_ENV) != "1",
    reason="real Codex subscription turns require explicit opt-in",
)
def test_fresh_install_real_codex_worker_report_review_and_dashboard(tmp_path: Path) -> None:
    """The supervisor is a deterministic MCP client; Worker inference is never mocked.

    The fixture's requester decision is authorized only for the exact no-effect
    marker below. It does not claim to test a CAO model's independent judgment.
    """
    _require_subscription_auth()
    cp_port, edge_port = _ports()
    config = tmp_path / "config.toml"
    state = tmp_path / "state"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    setup_local(config_path=config, state_dir=state, port=cp_port, dashboard_port=edge_port)
    settings = Settings.load(config)
    native_thread = asyncio.run(_create_persisted_native_cao_thread(cwd=workspace))
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CAO_", "CODEX_THREAD"))
    }
    environment["CODEX_THREAD_ID"] = native_thread
    captured: list[str] = []
    bridge = None
    worker_id = None
    request_id = 10

    def call(name: str, arguments: dict) -> dict:
        nonlocal request_id
        request_id += 1
        assert bridge is not None
        response = _send(bridge, _tool_call(request_id, name, arguments), captured)
        assert not response["result"].get("isError"), response
        return _structured(response)

    with (tmp_path / "launcher.log").open("w") as log:
        launcher = subprocess.Popen(
            [sys.executable, "-m", "cao_control_plane.cli", "--config", str(config), "run-local"],
            env=environment,
            cwd=workspace,
            stdout=log,
            stderr=log,
        )
        try:
            with httpx.Client(timeout=2, trust_env=False) as client:
                deadline = time.monotonic() + 30
                while True:
                    assert launcher.poll() is None, "local launcher exited"
                    try:
                        if client.get(f"http://127.0.0.1:{cp_port}/ready").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    assert time.monotonic() < deadline, "local startup timed out"
                    time.sleep(0.1)
                bridge = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "cao_control_plane.cli",
                        "--config",
                        str(config),
                        "mcp-stdio",
                    ],
                    env=environment,
                    cwd=workspace,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=log,
                    text=True,
                )
                _send(
                    bridge,
                    _request(
                        1,
                        "initialize",
                        {
                            "protocolVersion": "2026-07-28",
                            "capabilities": {},
                            "clientInfo": {"name": "cao-public-release", "version": "1"},
                        },
                    ),
                    captured,
                )
                attached = call("cao_start", {"native_thread_id": native_thread})
                assert attached["status"] in {"ready", "attached"}, attached
                assert call("cao_list_managed_workers", {})["workers"] == []
                created = call(
                    "cao_new_worker_thread",
                    {
                        "working_directory": str(workspace),
                        "runner": "codex",
                        "model": "gpt-5.6-terra",
                        "reasoning_effort": "low",
                        "idempotency_key": "public-live-new",
                    },
                )
                worker_id = created["worker_thread_id"]
                assigned = call(
                    "cao_instruct_worker_thread",
                    {
                        "worker_thread_id": worker_id,
                        "objective": "Reply with exactly PUBLIC_CODEX_INSTALL_OK. Do not read or write files, run commands, or access external services.",
                        "acceptance": ["The entire final answer is PUBLIC_CODEX_INSTALL_OK."],
                        "completion_contract": "no_artifact_expected",
                        "idempotency_key": "public-live-instruct",
                    },
                )
                work_id = assigned["task"]["work_item_id"]
                deadline = time.monotonic() + 180
                while True:
                    work = call("cao_get_work", {"work_item_id": work_id})
                    if any(
                        item["kind"] in {"completion", "worker_output"}
                        for item in work["open_boundaries"]
                    ):
                        break
                    assert not work["open_boundaries"], {
                        "state": work["state"],
                        "boundaries": work["open_boundaries"],
                    }
                    assert time.monotonic() < deadline, {
                        "state": work["state"],
                        "attempt": work["current_attempt"]["state"],
                    }
                    time.sleep(0.5)
                outputs = work["worker_outputs"]
                assert outputs, "real provider output was not captured"
                finals = []
                for output in outputs:
                    if output["capture_state"] == "empty":
                        continue
                    read = call(
                        "cao_read_worker_output",
                        {
                            "work_item_id": work_id,
                            "attempt_id": work["current_attempt"]["id"],
                            "output_id": output["id"],
                            "expected_digest": output["digest"],
                            "max_bytes": 65536,
                            "idempotency_key": f"public-live-read-{output['id']}",
                        },
                    )
                    finals.append(read["content"])
                assert any(value.strip() == "PUBLIC_CODEX_INSTALL_OK" for value in finals)
                reviewed = call(
                    "cao_review",
                    {
                        "attempt_id": work["current_attempt"]["id"],
                        "verdict": "ok",
                        "summary": "The complete real Worker output matches the exact fixture marker.",
                        "evidence": [{"check": "real_codex_output", "result": "pass"}],
                        "idempotency_key": "public-live-review",
                    },
                )
                boundary = next(
                    item
                    for item in work["open_boundaries"]
                    if item["kind"] in {"completion", "worker_output"}
                )
                turn = call(
                    "cao_acquire_reasoner_turn",
                    {
                        "work_item_id": work_id,
                        "boundary_id": boundary["id"],
                        "expected_generation": work["generation"],
                        "idempotency_key": "public-live-turn",
                    },
                )
                call(
                    "cao_dispose_boundary",
                    {
                        "boundary_id": boundary["id"],
                        "turn_id": turn["id"],
                        "lease_token": turn["lease_token"],
                        "expected_generation": work["generation"],
                        "kind": "accept",
                        "reason": "Exact fixture marker independently read and verified.",
                    },
                )
                accepted = call(
                    "cao_record_requester_decision",
                    {
                        "review_id": reviewed["reviews"][-1]["id"],
                        "verdict": "accepted",
                        "summary": "Apply the fixture's preauthorized exact-marker acceptance.",
                        "evidence": [{"check": "exact_fixture_marker", "result": "pass"}],
                        "conversation_evidence_id": "public-live-fixture-acceptance",
                        "idempotency_key": "public-live-acceptance",
                    },
                )
                assert accepted["state"] == "completed", accepted
                edge = f"http://127.0.0.1:{edge_port}"
                secret = parse_qs(urlparse(local_dashboard_link(settings)).fragment)[
                    "dashboard_bootstrap"
                ][0]
                assert (
                    client.post(edge + "/dashboard/session", json={"secret": secret}).status_code
                    == 204
                )
                snapshot = client.get(edge + "/dashboard/api/snapshot")
                assert snapshot.status_code == 200
                completed = snapshot.json()["operator"]["recently_completed"]
                assert len(completed) == 1 and completed[0]["state"] == "completed"
                history = client.get(edge + "/dashboard/api/history")
                assert history.status_code == 200
                index = client.get(edge + "/dashboard/api/work-history").json()
                assert len(index["items"]) == 1
                selected = client.get(
                    edge + "/dashboard/api/work-history",
                    params={"work": index["items"][0]["history_reference"]},
                )
                assert selected.status_code == 200
                detail = selected.json()
                assert detail["work"]["latest_cao_review_decision"] == "ok"
                assert detail["work"]["requester_decision"] == "accepted"
                assert {"worker_output", "review", "requester_decision"} <= {
                    entry["kind"] for entry in detail["entries"]
                }
                assert not _logical_control_plane_secrets(settings.database_path)
                assert not CONTROL_PLANE_SECRET_PATTERN.search("".join(captured))
                print(
                    "Real Codex: attachment, New, instruction, managed output, review, acceptance and Dashboard passed."
                )
        finally:
            try:
                if worker_id and bridge and bridge.poll() is None:
                    call(
                        "cao_delete_worker_thread",
                        {"worker_thread_id": worker_id, "idempotency_key": "public-live-delete"},
                    )
                    assert call("cao_list_managed_workers", {})["workers"] == []
            finally:
                _close_process(bridge)
                _close_process(launcher)
                asyncio.run(_archive_disposable_thread(native_thread))
