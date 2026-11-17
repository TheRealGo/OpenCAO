from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from cao_control_plane.errors import ConflictError, ControlPlaneError
from cao_control_plane.mcp import MCPServer, conversation_server_tools
from cao_control_plane.models import (
    InstructWorkerThreadInput,
    NewWorkerThreadInput,
    ResumeWorkerThreadInput,
)
from cao_control_plane.service import ControlPlane
from cao_control_plane.worker_purpose import PURPOSE_GUIDANCE

PROTOCOLS = Path(__file__).resolve().parents[1] / "docs" / "protocols.md"


def _actor() -> dict[str, Any]:
    return {
        "id": "cao_public",
        "role": "cao",
        "_cao_conversation_credential_id": "csc_public",
        "_cao_attachment_id": "csa_public",
        "_cao_attachment_generation": 3,
    }


def _tool_map() -> dict[str, dict[str, Any]]:
    return {str(tool["name"]): tool for tool in conversation_server_tools()}


class WorkerControlContractService:
    def __init__(self) -> None:
        self.settings = SimpleNamespace(
            server_name="test",
            server_version="test",
        )
        self.new_request: NewWorkerThreadInput | None = None
        self.instruction_request: InstructWorkerThreadInput | None = None
        self.resume_request: ResumeWorkerThreadInput | None = None

    def new_worker_thread(
        self, actor: dict[str, Any], request: NewWorkerThreadInput
    ) -> dict[str, Any]:
        del actor
        self.new_request = request
        return {
            "worker_thread_id": "wth_public",
            "thread_state": "active",
            "thread_generation": 1,
            "operator_label": "Codex Worker",
            "adapter": "codex-app-server",
            "assignment_readiness": "not_connected",
            "connection_state": "disconnected",
            "can_accept_instruction": True,
            "workspace_ref": "private-workspace",
            "runtime_session_id": "private-runtime",
            "enrollment_id": "private-enrollment",
        }

    def instruct_worker_thread(
        self, actor: dict[str, Any], request: InstructWorkerThreadInput
    ) -> dict[str, Any]:
        del actor
        self.instruction_request = request
        return {
            "worker_thread_id": request.worker_thread_id,
            "thread_state": "active",
            "thread_generation": request.expected_generation,
            "delivery_state": "queued",
            "connection_state": "disconnected",
            "can_accept_instruction": True,
            "task": {
                "work_item_id": "wrk_public",
                "status": "active",
                "title": request.title or "Worker instruction",
                "goal_version": 1,
                "goal_packet_digest": "a" * 64,
            },
            "runtime_session_id": "private-runtime",
            "native_session_id": "private-native-session",
            "workspace_ref": "private-workspace",
        }

    def resume_worker_thread(
        self, actor: dict[str, Any], request: ResumeWorkerThreadInput
    ) -> dict[str, Any]:
        del actor
        self.resume_request = request
        return {
            "worker_thread_id": request.worker_thread_id,
            "thread_state": "active",
            "thread_generation": request.expected_generation + 1,
            "runtime_session_id": "private-runtime",
        }

    def list_managed_workers(self, actor: dict[str, Any]) -> list[dict[str, Any]]:
        del actor
        return [
            {
                "worker_thread_id": "wth_public",
                "thread_state": "active",
                "thread_generation": 1,
                "operator_label": "Codex Worker",
                "adapter": "codex-app-server",
                "effective_model": "gpt-test",
                "effective_reasoning_effort": "high",
                "assignment_readiness": "not_connected",
                "connection_state": "disconnected",
                "can_accept_instruction": True,
                "workspace_ref": "private-workspace",
                "runtime_session_id": "private-runtime",
                "enrollment_id": "private-enrollment",
            }
        ]


def test_models_keep_three_goal_maturities_and_resume_has_no_task_shape() -> None:
    pure = ResumeWorkerThreadInput.model_validate(
        {
            "worker_thread_id": "wth_public",
            "expected_generation": 2,
            "idempotency_key": "resume-pure",
        }
    )
    assert pure.worker_thread_id == "wth_public"

    with pytest.raises(PydanticValidationError):
        ResumeWorkerThreadInput.model_validate(
            {
                "worker_thread_id": "wth_public",
                "expected_generation": 2,
                "idempotency_key": "resume-with-obsolete-task",
                "objective": "Task assignment uses cao_instruct_worker_thread.",
            }
        )

    for maturity in ("unset", "exploring"):
        instruction = InstructWorkerThreadInput.model_validate(
            {
                "worker_thread_id": "wth_public",
                "expected_generation": 1,
                "idempotency_key": f"instruct-{maturity}",
                "title": "Bounded instruction",
                "objective": "Record one exact instruction.",
                "maturity": maturity,
            }
        )
        assert instruction.maturity.value == maturity

    minimal = InstructWorkerThreadInput.model_validate(
        {
            "worker_thread_id": "wth_public",
            "expected_generation": 1,
            "idempotency_key": "instruct-without-goal-metadata",
            "objective": "Send one bounded local instruction.",
        }
    )
    assert minimal.title is None
    assert minimal.maturity.value == "unset"
    assert minimal.acceptance == []

    with pytest.raises(PydanticValidationError):
        InstructWorkerThreadInput.model_validate(
            {
                "worker_thread_id": "wth_public",
                "expected_generation": 1,
                "idempotency_key": "instruct-defined-without-acceptance",
                "title": "Defined instruction",
                "objective": "Require observable completion.",
                "maturity": "defined",
            }
        )


def test_new_instruction_resume_and_list_projections_are_privacy_safe() -> None:
    service = WorkerControlContractService()
    server = MCPServer(service)  # type: ignore[arg-type]
    created = server.call_tool(
        _actor(),
        "cao_new_worker_thread",
        {
            "working_directory": "/private/requester-selected-directory",
            "idempotency_key": "new-empty-worker",
        },
    )
    assert created == {
        "worker_thread_id": "wth_public",
        "state": "active",
        "generation": 1,
        "name": "Codex Worker",
        "runner": "codex",
        "connection_state": "disconnected",
        "can_accept_instruction": True,
    }
    assert service.new_request is not None
    assert service.new_request.working_directory == "/private/requester-selected-directory"
    assert service.new_request.runner == "codex"
    assert service.new_request.reasoning_effort is None
    assert service.new_request.name is None

    instructed = server.call_tool(
        _actor(),
        "cao_instruct_worker_thread",
        {
            "worker_thread_id": "wth_public",
            "expected_generation": 1,
            "idempotency_key": "instruct-empty-worker",
            "objective": "Queue one sealed instruction while disconnected.",
        },
    )
    assert instructed == {
        "worker_thread_id": "wth_public",
        "state": "active",
        "generation": 1,
        "task": {
            "work_item_id": "wrk_public",
            "status": "active",
            "title": "Worker instruction",
            "goal_version": 1,
            "goal_packet_digest": "a" * 64,
        },
        "delivery_state": "queued",
        "connection_state": "disconnected",
        "can_accept_instruction": True,
    }
    assert service.instruction_request is not None
    assert service.instruction_request.worker_thread_id == "wth_public"
    assert service.instruction_request.title is None
    assert service.instruction_request.maturity.value == "unset"

    resumed = server.call_tool(
        _actor(),
        "cao_resume_worker_thread",
        {
            "worker_thread_id": "wth_public",
            "expected_generation": 2,
            "idempotency_key": "resume-without-work",
        },
    )
    assert resumed == {
        "worker_thread_id": "wth_public",
        "state": "active",
        "generation": 3,
    }
    assert service.resume_request is not None
    assert service.resume_request.worker_thread_id == "wth_public"

    listed = server.call_tool(_actor(), "cao_list_managed_workers", {})
    assert listed == {
        "workers": [
            {
                "name": "Codex Worker",
                "runner": "codex",
                "model": "gpt-test",
                "reasoning_effort": "high",
                "state": "active",
                "worker_thread_id": "wth_public",
                "worker_purpose": None,
                "purpose_supervision": {"state": "none", "count": 0, "resume_at": None},
                "supervision_guidance": PURPOSE_GUIDANCE,
                "generation": 1,
                "assignment_readiness": "not_connected",
                "connection_state": "disconnected",
                "can_accept_instruction": True,
                "instruction_queue": {
                    "pending_count": 0,
                    "head_state": "empty",
                    "ordering": "durable_fifo",
                },
            }
        ]
    }
    rendered = json.dumps(
        {"created": created, "instructed": instructed, "listed": listed},
        sort_keys=True,
    )
    for private_value in (
        "private-workspace",
        "private-runtime",
        "private-native-session",
        "private-enrollment",
        "/private/requester-selected-directory",
    ):
        assert private_value not in rendered


@pytest.mark.parametrize("reason_code", [
    "worker_purpose_paused", "worker_purpose_achieved", "private_admission_failure"
])
def test_instruction_refusal_exposes_only_known_purpose_state(
    monkeypatch: pytest.MonkeyPatch, reason_code: str
) -> None:
    service = WorkerControlContractService()

    def refuse(_actor: dict[str, Any], _request: InstructWorkerThreadInput) -> None:
        raise ConflictError(
            "private-runtime at /private/worker-directory",
            reason_code=reason_code,
            native_session_id="private-native-session",
            runtime_session_id="private-runtime",
            next_action="private-recovery-action",
        )

    monkeypatch.setattr(service, "instruct_worker_thread", refuse)
    with pytest.raises(ControlPlaneError) as caught:
        MCPServer(service).call_tool(  # type: ignore[arg-type]
            _actor(), "cao_instruct_worker_thread",
            {
                "worker_thread_id": "wth_public",
                "instruction": "Perform one bounded verification.",
                "idempotency_key": "refused-instruction",
            },
        )
    error = caught.value
    assert error.code == "conflict" and error.status_code == 409
    if reason_code.startswith("worker_purpose_"):
        assert error.details == {"reason_code": reason_code, "retryable": False}
        assert "requester-directed" in error.message
    else:
        assert error.details is None
    rendered = json.dumps({"message": error.message, "details": error.details})
    for private_value in (
        "private-runtime", "/private/worker-directory", "private-native-session",
        "private-recovery-action", "private_admission_failure",
    ):
        assert private_value not in rendered


def test_control_plane_exposes_worker_thread_control_hooks() -> None:
    assert callable(getattr(ControlPlane, "new_worker_thread", None))
    assert callable(getattr(ControlPlane, "instruct_worker_thread", None))
