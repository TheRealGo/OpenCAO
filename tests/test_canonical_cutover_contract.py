from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pytest
from pydantic import ValidationError as PydanticValidationError

from cao_control_plane.api import create_app
from cao_control_plane.cli import build_parser
from cao_control_plane.config import Settings
from cao_control_plane.mcp import conversation_server_tools
from cao_control_plane.models import DeleteWorkerThreadInput
from cao_control_plane.runtime import render_message

REMOVED_TOOLS = {
    "cao_delegate_target",
    "cao_delegate_new_worker",
    "cao_abandon_system_reconciliation",
    "cao_provision_managed_worker",
    "cao_stop_managed_worker",
}

CANONICAL_WORKER_TOOLS = {
    "cao_list_managed_workers",
    "cao_new_worker_thread",
    "cao_instruct_worker_thread",
    "cao_finish_worker_thread",
    "cao_resume_worker_thread",
    "cao_delete_worker_thread",
}

REMOVED_CONFIG_KEYS = {
    "enable_mcp_2026",
    "enable_mcp_2025_compat",
    "managed_worker_catalog_file",
}


def test_attached_catalog_exposes_only_the_canonical_worker_route() -> None:
    tools = {str(tool["name"]) for tool in conversation_server_tools()}

    assert tools >= CANONICAL_WORKER_TOOLS
    assert REMOVED_TOOLS.isdisjoint(tools)


def test_removed_settings_and_direct_stdio_mode_cannot_be_reenabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setting_names = {item.name for item in fields(Settings)}
    assert REMOVED_CONFIG_KEYS.isdisjoint(setting_names)

    config = tmp_path / "config.toml"
    config.write_text(
        "[protocols]\nenable_mcp_2025_compat = true\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CAO_A2A_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv(
        "CAO_A2A_RUNTIME_LAUNCH_DIR",
        str(tmp_path / "state" / "runtime-launches"),
    )
    with pytest.raises(ValueError, match="unsupported configuration keys"):
        Settings.load(config)

    with pytest.raises(SystemExit):
        build_parser().parse_args(["mcp-stdio", "--operator", "--direct"])

    with pytest.raises(PydanticValidationError):
        DeleteWorkerThreadInput.model_validate(
            {
                "worker_thread_id": "wth_old-delete-shape",
                "idempotency_key": "old-delete-shape",
                "acknowledge_delete": True,
                "conversation_evidence_id": "old-second-acknowledgment",
            }
        )


def test_http_mcp_has_one_post_only_route(settings: Settings) -> None:
    app = create_app(settings)
    routes = [route for route in app.routes if getattr(route, "path", None) == "/mcp"]

    assert len(routes) == 1
    assert routes[0].methods == {"POST"}


def test_historical_removed_recovery_value_is_inert_evidence() -> None:
    rendered = render_message(
        {
            "kind": "system",
            "work_item_id": "wrk_historical",
            "attempt_id": "att_historical",
            "recovery_action": "delegate_continue_prior",
            "payload": {
                "action": "recover_terminal_worker_attempt",
                "boundary_id": "bnd_historical",
                "boundary_kind": "failure",
                "generation": 1,
            },
        }
    )

    assert "non-executable system recovery Boundary requires a CAO disposition" in rendered
    assert "same logical Worker thread to a fresh fenced epoch" not in rendered
    assert "same provider-native Worker thread" not in rendered
