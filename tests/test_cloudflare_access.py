from __future__ import annotations

import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from cao_control_plane.cloudflare_access import (
    CloudflareAccessJWTValidator,
    CloudflareAccessSettings,
)


def _fixture() -> tuple[object, dict[str, object]]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    jwk.update({"kid": "cao-key", "alg": "RS256", "use": "sig"})
    return private_key, jwk


def _token(
    private_key: object,
    *,
    email: str = "owner@example.test",
    audience: str = "a" * 64,
    issuer: str = "https://cao-team.cloudflareaccess.com",
) -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "aud": [audience],
            "email": email,
            "exp": now + 300,
            "iat": now,
            "iss": issuer,
            "sub": "owner",
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "cao-key"},
    )


def test_access_validator_requires_exact_signed_application_and_email() -> None:
    private_key, jwk = _fixture()
    requests: list[httpx.Request] = []

    def certs(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"keys": [jwk]})

    settings = CloudflareAccessSettings(
        team_domain="cao-team.cloudflareaccess.com",
        application_audience="a" * 64,
        allowed_email="owner@example.test",
    )
    validator = CloudflareAccessJWTValidator(
        settings,
        transport=httpx.MockTransport(certs),
    )

    async def exercise() -> tuple[bool, bool, bool, bool, bool]:
        accepted = await validator.authorized(_token(private_key))
        wrong_email = await validator.authorized(
            _token(private_key, email="other@example.test")
        )
        wrong_audience = await validator.authorized(
            _token(private_key, audience="b" * 64)
        )
        wrong_issuer = await validator.authorized(
            _token(private_key, issuer="https://other.cloudflareaccess.com")
        )
        malformed = await validator.authorized("not-a-jwt")
        return accepted, wrong_email, wrong_audience, wrong_issuer, malformed

    import asyncio

    assert asyncio.run(exercise()) == (True, False, False, False, False)
    assert len(requests) == 1
    assert requests[0].url == settings.certs_url


def test_access_validator_refreshes_cached_keys_once_for_rotation() -> None:
    _old_private_key, old_jwk = _fixture()
    new_private_key, new_jwk = _fixture()
    old_jwk["kid"] = "old-key"
    new_jwk["kid"] = "cao-key"
    responses = iter(({"keys": [old_jwk]}, {"keys": [new_jwk]}))
    requests: list[httpx.Request] = []

    def certs(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=next(responses))

    settings = CloudflareAccessSettings(
        team_domain="cao-team.cloudflareaccess.com",
        application_audience="a" * 64,
        allowed_email="owner@example.test",
    )
    validator = CloudflareAccessJWTValidator(
        settings,
        transport=httpx.MockTransport(certs),
    )

    import asyncio

    assert asyncio.run(validator.authorized(_token(new_private_key))) is True
    assert len(requests) == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("team_domain", "https://cao-team.cloudflareaccess.com"),
        ("application_audience", "audience with spaces"),
        ("allowed_email", "not-an-email"),
    ],
)
def test_access_settings_reject_ambiguous_identity(field: str, value: str) -> None:
    values = {
        "team_domain": "cao-team.cloudflareaccess.com",
        "application_audience": "a" * 64,
        "allowed_email": "owner@example.test",
    }
    values[field] = value
    with pytest.raises(ValueError):
        CloudflareAccessSettings(**values)
