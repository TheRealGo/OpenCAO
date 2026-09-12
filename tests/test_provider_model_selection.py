from pathlib import Path

import pytest
from pydantic import ValidationError

from cao_control_plane.config import Settings
from cao_control_plane.dashboard import _model_label
from cao_control_plane.dashboard_edge import _model_label as edge_model_label
from cao_control_plane.models import NewWorkerThreadInput
from cao_control_plane.projection import _operator_model
from cao_control_plane.service import ControlPlane


@pytest.mark.parametrize("legacy", [False, True])
def test_configured_models_supply_a_default_not_an_allowlist(
    tmp_path: Path, legacy: bool
) -> None:
    config = tmp_path / "config.toml"
    model_setting = (
        'models = ["existing-default", "another-old-model"]'
        if legacy
        else 'default_model = "existing-default"'
    )
    config.write_text(
        '[runtime.managed_worker_profiles.codex]\n'
        'adapter = "codex-app-server"\n'
        f'{model_setting}\n'
        'reasoning_efforts = ["medium", "xhigh"]\n'
    )
    service = ControlPlane.__new__(ControlPlane)
    service.settings = Settings.load(config)
    _, model, effort = service._resolve_new_worker_profile(
        runner="codex", model="provider-next-release", reasoning_effort="xhigh"
    )
    assert (model, effort) == ("provider-next-release", "xhigh")
    _, model, effort = service._resolve_new_worker_profile(
        runner="codex", model=None, reasoning_effort=None
    )
    assert (model, effort) == ("existing-default", "medium")


@pytest.mark.parametrize(
    "model", ["opus[1m]", "organization/model:release", "provider.model@release"]
)
def test_provider_selectors_survive_request_and_public_projection(model: str) -> None:
    request = NewWorkerThreadInput(
        working_directory="/workspace", model=model, idempotency_key="selector"
    )
    assert request.model == model
    assert _operator_model(model) == model
    assert _model_label(model) == model
    assert edge_model_label(model) == model


@pytest.mark.parametrize(
    "model",
    [
        "", "--model=other", "/private/model", "../model", "model\nother",
        "https://host/model", "C:/private/model", "namespace/../model",
        "cao.rtc_fixture.synthetic-credential", "sk-" + "x" * 24,
    ],
)
def test_model_transport_rejects_options_paths_and_control_text(model: str) -> None:
    with pytest.raises(ValidationError):
        NewWorkerThreadInput(
            working_directory="/workspace", model=model, idempotency_key="invalid-selector"
        )
    assert _operator_model(model) is None
    assert _model_label(model) is None
    assert edge_model_label(model) is None
