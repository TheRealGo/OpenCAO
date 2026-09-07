from __future__ import annotations

import pytest

from cao_control_plane.effects import EffectExecutor, effect_argv_digest, effect_workdir_digest
from cao_control_plane.errors import AuthenticationError, AuthorizationError, ValidationError
from cao_control_plane.models import EffectCheckInput, EffectGrantInput, EffectResolveInput
from cao_control_plane.security import (
    contains_generic_credential_text,
    require_loopback_url,
    sha256_json,
)


def test_invalid_token_fails(system):
    with pytest.raises(AuthenticationError):
        system["service"].authenticate("invalid")


def test_loopback_callback_policy():
    require_loopback_url("http://127.0.0.1:9000/callback")
    require_loopback_url("http://localhost:9000/callback")
    with pytest.raises(ValidationError):
        require_loopback_url("https://example.com/callback")


@pytest.mark.parametrize(
    "credential",
    (
        "github_pat_" + "A" * 40,
        "Authorization: Bearer " + "B" * 40,
        "-----BEGIN PRIVATE KEY-----",
        "https://alice:s3cretpassword123@example.com/data",
    ),
)
def test_generic_credential_text_detector_rejects_concrete_values(
    credential: str,
) -> None:
    assert contains_generic_credential_text(credential)


@pytest.mark.parametrize(
    "description",
    (
        "Document the credential rotation policy and token scope.",
        "Describe the password length and private key format.",
        "Explain the Authorization bearer scheme without including a value.",
    ),
)
def test_generic_credential_text_detector_allows_descriptive_words(
    description: str,
) -> None:
    assert not contains_generic_credential_text(description)


def test_exact_one_time_effect_authority(system):
    service = system["service"]
    digest = sha256_json({"ref": "abc"})
    grant = service.grant_effect(
        system["cao"],
        EffectGrantInput(
            principal_id=system["worker"]["id"],
            kind="external",
            target_pattern="github:example-owner/example-project",
            action_pattern="push",
            content_digest=digest,
            standing=False,
        ),
    )
    request = EffectCheckInput(
        principal_id=system["worker"]["id"],
        kind="external",
        target="github:example-owner/example-project",
        action="push",
        content_digest=digest,
    )
    assert service.check_effect(request)["decision"] == "allow"
    operation = service.start_effect(system["worker"], request)
    assert operation["grant_id"] == grant["id"]
    assert service.check_effect(request)["decision"] == "verify_remote"
    resolved = service.resolve_effect(
        system["worker"],
        operation["id"],
        EffectResolveInput(status="succeeded", evidence="remote ref verified"),
    )
    assert resolved["status"] == "succeeded"


def test_effect_does_not_match_different_content(system):
    service = system["service"]
    service.grant_effect(
        system["cao"],
        EffectGrantInput(
            principal_id=system["worker"]["id"],
            kind="external",
            target_pattern="github:*",
            action_pattern="push",
            content_digest=sha256_json("allowed"),
        ),
    )
    decision = service.check_effect(
        EffectCheckInput(
            principal_id=system["worker"]["id"],
            kind="external",
            target="github:example-owner/example-project",
            action="push",
            content_digest=sha256_json("different"),
        )
    )
    assert decision["decision"] == "approval_required"


def test_worker_cannot_create_principal(system):
    from cao_control_plane.models import PrincipalCreate

    with pytest.raises(AuthorizationError):
        system["service"].create_principal(
            system["worker"], PrincipalCreate(name="intruder", role="worker")
        )


def test_only_cao_can_manage_effect_authority(system):
    service = system["service"]
    request = EffectGrantInput(
        principal_id=system["worker"]["id"],
        kind="external",
        target_pattern="remote:*",
        action_pattern="publish",
        standing=True,
    )

    with pytest.raises(AuthorizationError):
        service.grant_effect(system["user"], request)

    grant = service.grant_effect(system["cao"], request)
    with pytest.raises(AuthorizationError):
        service.revoke_effect_grant(system["user"], grant["id"])
    assert service.get_effect_grant(grant["id"])["id"] == grant["id"]


def test_standing_effect_cannot_be_reserved_twice_while_unresolved(system):
    service = system["service"]
    request = EffectCheckInput(
        principal_id=system["worker"]["id"],
        kind="external",
        target="github:example-owner/example-project",
        action="push",
        content_digest=sha256_json({"commit": "abc"}),
    )
    service.grant_effect(
        system["cao"],
        EffectGrantInput(
            principal_id=system["worker"]["id"],
            kind="external",
            target_pattern=request.target,
            action_pattern=request.action,
            content_digest=request.content_digest,
            standing=True,
        ),
    )
    first = service.start_effect(system["worker"], request)
    with pytest.raises(AuthorizationError) as caught:
        service.start_effect(system["worker"], request)
    assert caught.value.details["decision"] == "verify_remote"
    assert caught.value.details["operation_id"] == first["id"]


def test_effect_executor_binds_exact_argv_and_workdir_without_a_shell(system, tmp_path):
    service = system["service"]
    argv = ("/usr/bin/printf", "%s", "safe;not-a-shell")
    request = EffectCheckInput(
        principal_id=system["worker"]["id"],
        kind="local",
        target="local:test",
        action="render",
        argv_digest=effect_argv_digest(argv),
        workdir_digest=effect_workdir_digest(tmp_path),
    )
    result = EffectExecutor(service).run(
        system["worker"], request, argv, workdir=tmp_path
    )
    assert result["status"] == "succeeded"
    assert "safe;not-a-shell" not in result["evidence"]


def test_effect_executor_marks_crashed_reservations_unknown_and_never_retries(system, tmp_path):
    service = system["service"]
    argv = ("/usr/bin/true",)
    request = EffectCheckInput(
        principal_id=system["worker"]["id"],
        kind="local",
        target="local:recovery",
        action="run",
        argv_digest=effect_argv_digest(argv),
        workdir_digest=effect_workdir_digest(tmp_path),
    )
    started = service.start_effect(system["worker"], request)
    executor = EffectExecutor(service)
    assert executor.recover_incomplete(system["cao"]) == 1
    assert service.get_effect_operation(started["id"])["status"] == "unknown"
    assert service.check_effect(request)["decision"] == "verify_remote"
