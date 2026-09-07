from __future__ import annotations

from contextlib import closing
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError as ModelValidationError

from cao_control_plane.api import create_app
from cao_control_plane.mcp import (
    CLIENT_CAPABILITIES_META_KEY,
    CLIENT_INFO_META_KEY,
    MCP_LATEST_VERSION,
    PROTOCOL_VERSION_META_KEY,
    MCPServer,
    _conversation_work_projection,
    conversation_proxy_tools,
    conversation_server_tools,
)
from cao_control_plane.models import BoundaryDispositionInput, WorkResumeInput

_TEXT_LIMITS = {
    "pause_boundary_id": 128,
    "reason": 4000,
    "instruction": 16000,
    "resume_evidence": 4000,
    "idempotency_key": 256,
}


def _resume_arguments() -> dict[str, Any]:
    return {
        "expected_generation": 2,
        "pause_boundary_id": "bnd_paused",
        "reason": "The recorded dependency is now available",
        "instruction": "Verify the dependency and continue only the unmet acceptance conditions",
        "resume_evidence": "The scoped dependency check now passes",
        "idempotency_key": "explicit-pause-resumption",
    }


def _pause_arguments() -> dict[str, Any]:
    return {
        "turn_id": "turn_decision",
        "lease_token": "fixture-decision-capability",
        "expected_generation": 1,
        "kind": "pause",
        "reason": "The current dependency cannot support another useful attempt",
        "resume_condition": "The scoped dependency check passes",
    }


def _tool_call(arguments: dict[str, Any], *, name: str = "cao_resume_work") -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments,
            "_meta": {
                PROTOCOL_VERSION_META_KEY: MCP_LATEST_VERSION,
                CLIENT_CAPABILITIES_META_KEY: {},
                CLIENT_INFO_META_KEY: {"name": "pytest", "version": "1"},
            },
        },
    }


def test_explicit_pause_is_not_a_worker_instruction_or_requester_question() -> None:
    request = BoundaryDispositionInput.model_validate(_pause_arguments())
    assert request.kind.value == "pause"
    assert request.instruction == ""
    assert request.resume_condition == "The scoped dependency check passes"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("reason", ""),
        ("reason", " \n\t"),
        ("resume_condition", ""),
        ("resume_condition", " \n\t"),
        ("instruction", "Try again"),
        ("instruction", " "),
        ("worker_id", "worker_alternate"),
        ("runtime_session_id", "runtime_alternate"),
    ],
)
def test_pause_rejects_missing_condition_or_execution_arguments(field: str, value: str) -> None:
    with pytest.raises(ModelValidationError):
        BoundaryDispositionInput.model_validate({**_pause_arguments(), field: value})


def test_wait_user_retains_its_distinct_requester_decision_contract() -> None:
    arguments = {**_pause_arguments(), "kind": "wait_user"}
    with pytest.raises(ModelValidationError):
        BoundaryDispositionInput.model_validate(arguments)
    arguments["instruction"] = "Choose the permitted scope"
    assert BoundaryDispositionInput.model_validate(arguments).kind.value == "wait_user"
    with pytest.raises(ModelValidationError):
        BoundaryDispositionInput.model_validate({**arguments, "kind": "correct"})


@pytest.mark.parametrize("field", tuple(_TEXT_LIMITS))
@pytest.mark.parametrize("invalid", ["", " \n\t"])
def test_resume_requires_nonblank_exact_authority_and_changed_condition(
    field: str, invalid: str
) -> None:
    with pytest.raises(ModelValidationError):
        WorkResumeInput.model_validate({**_resume_arguments(), field: invalid})


@pytest.mark.parametrize("field", tuple(_TEXT_LIMITS))
def test_resume_text_limits_are_bounded_and_advertised(field: str) -> None:
    arguments = _resume_arguments()
    arguments[field] = "x" * _TEXT_LIMITS[field]
    assert WorkResumeInput.model_validate(arguments).model_dump()[field] == arguments[field]
    arguments[field] += "x"
    with pytest.raises(ModelValidationError):
        WorkResumeInput.model_validate(arguments)


@pytest.mark.parametrize("generation", [0, -1, True, False, 1.0, "1", 2**63])
def test_resume_generation_is_a_strict_positive_bounded_integer(generation: object) -> None:
    with pytest.raises(ModelValidationError):
        WorkResumeInput.model_validate({**_resume_arguments(), "expected_generation": generation})


def test_resume_requires_every_field_and_rejects_unsealed_extra_authority() -> None:
    for omitted in _resume_arguments():
        arguments = _resume_arguments()
        del arguments[omitted]
        with pytest.raises(ModelValidationError):
            WorkResumeInput.model_validate(arguments)
    with pytest.raises(ModelValidationError):
        WorkResumeInput.model_validate({**_resume_arguments(), "worker_id": "worker_other"})


def test_resume_catalog_is_cao_only_and_uses_the_typed_flat_contract(
    system: dict[str, Any],
) -> None:
    server = MCPServer(system["service"])
    catalogs = [
        conversation_server_tools(),
        conversation_proxy_tools(),
        server.tools_for(system["cao"]),
    ]
    model_schema = WorkResumeInput.model_json_schema()
    for catalog in catalogs:
        definitions = {tool["name"]: tool for tool in catalog}
        schema = definitions["cao_resume_work"]["inputSchema"]
        assert set(schema["required"]) == set(model_schema["required"]) | {"work_item_id"}
        assert schema["additionalProperties"] is False
        assert {"allOf", "anyOf", "oneOf", "$ref", "$defs"}.isdisjoint(schema)
        for field, maximum in _TEXT_LIMITS.items():
            assert schema["properties"][field]["maxLength"] == maximum
            assert schema["properties"][field]["pattern"] == r"\S"
        assert definitions["cao_resume_work"]["annotations"] == {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        }
        disposition = definitions["cao_dispose_boundary"]
        assert "pause" in disposition["inputSchema"]["properties"]["kind"]["enum"]
        assert (
            "pause" in disposition["inputSchema"]["properties"]["resume_condition"]["description"]
        )
    for actor in (
        system["worker"],
        system["user"],
        {**system["cao"], "_cao_runtime_credential_id": "runtime-decision-capability"},
    ):
        assert "cao_resume_work" not in {tool["name"] for tool in server.tools_for(actor)}


def test_resume_mcp_and_http_delegate_the_same_typed_command(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app(system["settings"])
    service = app.state.service
    calls: list[tuple[str, str, dict[str, Any]]] = []
    result = {"id": "wrk_paused", "state": "active", "generation": 3, "supervision_pause": None}

    def resume(actor: dict[str, Any], work_id: str, request: WorkResumeInput) -> dict[str, Any]:
        assert isinstance(request, WorkResumeInput)
        calls.append((str(actor["id"]), work_id, request.model_dump()))
        return result

    # The spy isolates transport mapping; domain-state tests cover the actual
    # pause transition, attachment authority, replay, and execution fences.
    monkeypatch.setattr(service, "resume_work", resume, raising=False)
    mcp = MCPServer(service).handle_modern(
        system["cao"], _tool_call({"work_item_id": "wrk_paused", **_resume_arguments()})
    )
    assert mcp["result"]["structuredContent"] == result
    with closing(TestClient(app, base_url="http://localhost")) as client:
        response = client.post(
            "/api/v1/work/wrk_paused:resume",
            json=_resume_arguments(),
            headers={"Authorization": f"Bearer {system['cao_token']}"},
        )
    assert response.status_code == 200
    assert response.json() == result
    assert calls == [(system["cao"]["id"], "wrk_paused", _resume_arguments())] * 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("expected_generation", True),
        ("expected_generation", "2"),
        ("pause_boundary_id", " "),
        ("reason", ""),
        ("instruction", "\n"),
        ("resume_evidence", ""),
        ("idempotency_key", " "),
        ("resume_evidence", "x" * 4001),
        ("worker_id", "worker_alternate"),
    ],
    ids=[
        "boolean-generation",
        "string-generation",
        "blank-pause-id",
        "blank-reason",
        "blank-instruction",
        "blank-evidence",
        "blank-idempotency-key",
        "oversized-evidence",
        "unsealed-worker-override",
    ],
)
def test_invalid_resume_never_reaches_the_domain_on_either_transport(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, field: str, value: object
) -> None:
    app = create_app(system["settings"])
    calls: list[object] = []
    monkeypatch.setattr(
        app.state.service, "resume_work", lambda *args: calls.append(args), raising=False
    )
    arguments = {**_resume_arguments(), field: value}
    result = MCPServer(app.state.service).handle_modern(
        system["cao"], _tool_call({"work_item_id": "wrk_paused", **arguments})
    )
    assert result["result"]["isError"] is True
    with closing(TestClient(app, base_url="http://localhost")) as client:
        response = client.post(
            "/api/v1/work/wrk_paused:resume",
            json=arguments,
            headers={"Authorization": f"Bearer {system['cao_token']}"},
        )
    assert response.status_code == 422
    assert calls == []


@pytest.mark.parametrize("role", ["worker", "user"])
def test_resume_http_rejects_non_cao_before_domain_execution(
    system: dict[str, Any], monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    app = create_app(system["settings"])
    calls: list[object] = []
    monkeypatch.setattr(
        app.state.service, "resume_work", lambda *args: calls.append(args), raising=False
    )
    with closing(TestClient(app, base_url="http://localhost")) as client:
        response = client.post(
            "/api/v1/work/wrk_paused:resume",
            json=_resume_arguments(),
            headers={"Authorization": f"Bearer {system[f'{role}_token']}"},
        )
    assert response.status_code == 403
    assert calls == []


def test_pause_projection_exposes_only_the_current_public_resume_contract() -> None:
    pause = {
        "boundary_id": "bnd_paused",
        "source_generation": 1,
        "pause_generation": 2,
        "reason": "The dependency requires a changed condition.\n" * 14,
        "resume_condition": (
            "Do not repeat the earlier dependency check unchanged.\n" * 12
            + "Resume only after the separate safety check explicitly succeeds."
        ),
        "paused_at": "2000-01-01T00:00:00Z",
    }
    projected = _conversation_work_projection(
        {
            "id": "wrk_paused",
            "state": "suspended",
            "generation": 2,
            "supervision_pause": {
                **pause,
                "runtime_session_id": "runtime_private",
                "native_thread_id": "native_private",
                "owner_token": "opaque-private-value",
            },
        }
    )
    assert projected["supervision_pause"] == pause
    assert len(projected["supervision_pause"]) == 6
    assert len(projected["supervision_pause"]["reason"]) > 280
    assert projected["supervision_pause"]["resume_condition"].endswith(
        "Resume only after the separate safety check explicitly succeeds."
    )
    assert "user_needed" not in projected


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("boundary_id", {"private": "nested identity"}),
        ("source_generation", True),
        ("pause_generation", "2"),
        ("reason", ["nested reason"]),
        ("resume_condition", " "),
        ("paused_at", {"private": "nested timestamp"}),
    ],
)
def test_malformed_pause_is_not_projected_as_resumption_authority(field: str, invalid: Any) -> None:
    pause = {
        "boundary_id": "bnd_paused",
        "source_generation": 1,
        "pause_generation": 2,
        "reason": "The dependency requires a changed condition",
        "resume_condition": "The dependency check passes",
        "paused_at": "2000-01-01T00:00:00Z",
        field: invalid,
    }
    assert "supervision_pause" not in _conversation_work_projection({"supervision_pause": pause})
