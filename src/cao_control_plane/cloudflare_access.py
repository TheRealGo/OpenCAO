"""Fail-closed Cloudflare Access JWT verification for the Dashboard edge."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import httpx
import jwt

_TEAM_DOMAIN = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cloudflareaccess\.com$"
)
_AUDIENCE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
_EMAIL = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_MAX_TOKEN_BYTES = 16 * 1024
_MAX_JWKS_BYTES = 256 * 1024
_MAX_JWKS_KEYS = 64


@dataclass(frozen=True, slots=True)
class CloudflareAccessSettings:
    """Exact Access application identity accepted by one Dashboard edge."""

    team_domain: str
    application_audience: str
    allowed_email: str

    def __post_init__(self) -> None:
        team_domain = self.team_domain.lower().rstrip(".")
        allowed_email = self.allowed_email.casefold()
        if _TEAM_DOMAIN.fullmatch(team_domain) is None:
            raise ValueError("Cloudflare Access team domain is invalid")
        if _AUDIENCE.fullmatch(self.application_audience) is None:
            raise ValueError("Cloudflare Access application audience is invalid")
        if len(allowed_email) > 320 or _EMAIL.fullmatch(allowed_email) is None:
            raise ValueError("Cloudflare Access allowed email is invalid")
        object.__setattr__(self, "team_domain", team_domain)
        object.__setattr__(self, "allowed_email", allowed_email)

    @property
    def issuer(self) -> str:
        return f"https://{self.team_domain}"

    @property
    def certs_url(self) -> str:
        return f"{self.issuer}/cdn-cgi/access/certs"


class CloudflareAccessJWTValidator:
    """Validate Access assertions against Cloudflare's bounded JWKS endpoint."""

    def __init__(
        self,
        settings: CloudflareAccessSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
        cache_ttl_seconds: float = 300.0,
    ) -> None:
        if cache_ttl_seconds <= 0:
            raise ValueError("Cloudflare Access key cache TTL must be positive")
        self._settings = settings
        self._transport = transport
        self._clock = clock
        self._cache_ttl_seconds = float(cache_ttl_seconds)
        self._keys: dict[str, jwt.PyJWK] = {}
        self._keys_expire_at = 0.0
        self._refresh_lock = asyncio.Lock()

    async def authorized(self, token: str) -> bool:
        if not token or len(token.encode("utf-8")) > _MAX_TOKEN_BYTES or token.count(".") != 2:
            return False
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError:
            return False
        kid = header.get("kid") if isinstance(header, Mapping) else None
        if (
            not isinstance(kid, str)
            or not kid
            or len(kid) > 256
            or header.get("alg") != "RS256"
        ):
            return False
        key = await self._key(kid)
        if key is None:
            return False
        try:
            claims = jwt.decode(
                token,
                key=key,
                algorithms=["RS256"],
                audience=self._settings.application_audience,
                issuer=self._settings.issuer,
                leeway=30,
                options={"require": ["aud", "email", "exp", "iat", "iss"]},
            )
        except jwt.PyJWTError:
            return False
        email = claims.get("email") if isinstance(claims, Mapping) else None
        return isinstance(email, str) and email.casefold() == self._settings.allowed_email

    async def _key(self, kid: str) -> jwt.PyJWK | None:
        if self._clock() >= self._keys_expire_at or not self._keys:
            async with self._refresh_lock:
                if self._clock() >= self._keys_expire_at or not self._keys:
                    await self._refresh_keys()
        key = self._keys.get(kid)
        if key is not None:
            return key
        # Cloudflare may rotate signing keys before this edge's bounded cache
        # expires. Refresh once for an unknown key id so a legitimate rotation
        # does not create a five-minute authentication outage.
        async with self._refresh_lock:
            key = self._keys.get(kid)
            if key is None:
                await self._refresh_keys()
                key = self._keys.get(kid)
        return key

    async def _refresh_keys(self) -> None:
        try:
            timeout = httpx.Timeout(5.0)
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=False,
                transport=self._transport,
            ) as client:
                response = await client.get(self._settings.certs_url)
        except httpx.HTTPError:
            self._keys = {}
            self._keys_expire_at = 0.0
            return
        if response.status_code != 200 or len(response.content) > _MAX_JWKS_BYTES:
            self._keys = {}
            self._keys_expire_at = 0.0
            return
        try:
            value = response.json()
        except ValueError:
            self._keys = {}
            self._keys_expire_at = 0.0
            return
        raw_keys = value.get("keys") if isinstance(value, Mapping) else None
        if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= _MAX_JWKS_KEYS:
            self._keys = {}
            self._keys_expire_at = 0.0
            return
        keys: dict[str, jwt.PyJWK] = {}
        for item in raw_keys:
            if not isinstance(item, Mapping):
                continue
            kid = item.get("kid")
            if (
                not isinstance(kid, str)
                or not kid
                or len(kid) > 256
                or item.get("kty") != "RSA"
                or item.get("alg") not in {None, "RS256"}
            ):
                continue
            try:
                keys[kid] = jwt.PyJWK.from_dict(dict(item), algorithm="RS256")
            except (jwt.PyJWTError, ValueError):
                continue
        if not keys:
            self._keys = {}
            self._keys_expire_at = 0.0
            return
        self._keys = keys
        self._keys_expire_at = self._clock() + self._cache_ttl_seconds
