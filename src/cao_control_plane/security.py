from __future__ import annotations

import base64
import fnmatch
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import unicodedata
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlsplit

from .errors import ValidationError

SCRYPT_N = 2**14
SCRYPT_R = 8
SCRYPT_P = 1


# These are bearer credentials minted for principals, runtime credentials, and
# one-use enrollment tickets.  They must never cross into durable opaque
# metadata, including as a nested mapping key.  Keep this pattern in the
# security module so every launch and persistence boundary applies the same
# definition rather than growing local variants.
CONTROL_PLANE_SECRET_PATTERN = re.compile(
    r"cao\.(?:prn_|rtc_|ent_|crc_|catk_|csc_|cab_)[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"
)
CONTROL_PLANE_SECRET_REDACTION = "[control-plane-credential-redacted]"
_GENERIC_CREDENTIAL_PATTERN = re.compile(
    r"(?:\bgithub_pat_[A-Za-z0-9_]{20,}\b|"
    r"\bgh[oprsu]_[A-Za-z0-9]{30,}\b|"
    r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b|"
    r"\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b|"
    r"-----BEGIN(?: [A-Z0-9]+)* PRIVATE KEY-----|"
    r"\bAuthorization\s*:\s*(?:Bearer|Basic)\s+[^\s,;]+|"
    r"\bBearer\s+[A-Za-z0-9._~+/=-]{20,}|"
    r"\bAKIA[A-Z0-9]{16}\b|"
    r"\b(?:[A-Za-z0-9]+[_-])*(?:api[ _-]?key|access[ _-]?token|"
    r"auth[ _-]?token|refresh[ _-]?token|client[ _-]?secret|private[ _-]?key|"
    r"secret[ _-]?access[ _-]?key|password)"
    r"\s*[:=]\s*[^\s,;]{12,})",
    re.IGNORECASE,
)
_GENERIC_CREDENTIAL_HTTP_URL_PATTERN = re.compile(
    r"\bhttps?://[^\s<>\"']+", re.IGNORECASE
)


def contains_control_plane_secret(value: Any) -> bool:
    """Return whether JSON-like data contains a control-plane bearer secret.

    Opaque metadata is allowed to be arbitrarily nested, so check both mapping
    keys and values.  A substring match is intentional: command arguments,
    paths, and environment values can embed a credential alongside other text.
    """

    if isinstance(value, str):
        return CONTROL_PLANE_SECRET_PATTERN.search(value) is not None
    if isinstance(value, Mapping):
        return any(
            contains_control_plane_secret(key) or contains_control_plane_secret(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(contains_control_plane_secret(item) for item in value)
    return False


def contains_generic_credential_text(value: str) -> bool:
    """Detect concrete non-CAO credential material in one text value.

    This is the shared string-level boundary for task packets, reports, and
    verified artifact text. Descriptive credential words remain valid; only
    concrete token/key patterns, credential assignments, and URL userinfo are
    rejected.
    """

    normalized = unicodedata.normalize("NFKC", value)
    if _GENERIC_CREDENTIAL_PATTERN.search(normalized) is not None:
        return True
    for match in _GENERIC_CREDENTIAL_HTTP_URL_PATTERN.finditer(normalized):
        candidate = match.group(0).rstrip(".,;:!?)]}")
        parsed = urlsplit(candidate)
        if parsed.username is not None or parsed.password is not None:
            return True
    return False


def redact_control_plane_secrets(value: Any) -> Any:
    """Recursively redact canonical bearer credentials from JSON-like output.

    This is deliberately safe for mapping keys too: launch diagnostics often
    serialize entire environment/configuration objects.  It is a display/log
    helper, not a persistence transform; callers must reject secrets before
    writing durable metadata.
    """

    if isinstance(value, str):
        return CONTROL_PLANE_SECRET_PATTERN.sub(CONTROL_PLANE_SECRET_REDACTION, value)
    if isinstance(value, Mapping):
        return {
            redact_control_plane_secrets(key): redact_control_plane_secrets(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_control_plane_secrets(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_control_plane_secrets(item) for item in value)
    if isinstance(value, set):
        return {redact_control_plane_secrets(item) for item in value}
    if isinstance(value, frozenset):
        return frozenset(redact_control_plane_secrets(item) for item in value)
    return value


def safe_validation_details(
    errors: Iterable[Mapping[str, Any]],
    *,
    allowed_location_roots: frozenset[str] = frozenset(),
    default_location: str | None = None,
) -> list[dict[str, Any]]:
    """Serialize validation failures without reflecting submitted values.

    Pydantic error dictionaries may contain the complete invalid ``input`` and
    attacker-controlled mapping keys in ``loc``.  Retain only stable schema
    diagnostics, collapse locations to an explicitly trusted transport root,
    and defensively redact credential-shaped text from validator messages.
    """

    details: list[dict[str, Any]] = []
    for item in errors:
        detail: dict[str, Any] = {}
        message = item.get("msg")
        error_type = item.get("type")
        if isinstance(message, str):
            detail["msg"] = redact_control_plane_secrets(message)
        if isinstance(error_type, str):
            detail["type"] = redact_control_plane_secrets(error_type)
        location = item.get("loc")
        if (
            isinstance(location, (list, tuple))
            and location
            and location[0] in allowed_location_roots
        ):
            detail["loc"] = [location[0]]
        elif default_location is not None:
            detail["loc"] = [default_location]
        details.append(detail)
    return details


def new_token(prefix: str = "cao") -> str:
    return f"{prefix}_{secrets.token_urlsafe(32)}"


def hash_token(token: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(
        token.encode("utf-8"), salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P, dklen=32
    )
    return "scrypt${}${}${}${}${}".format(
        SCRYPT_N,
        SCRYPT_R,
        SCRYPT_P,
        base64.urlsafe_b64encode(salt).decode("ascii"),
        base64.urlsafe_b64encode(digest).decode("ascii"),
    )


def verify_token(token: str, encoded: str) -> bool:
    try:
        algorithm, n, r, p, salt_b64, digest_b64 = encoded.split("$", 5)
        if algorithm != "scrypt":
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
        expected = base64.urlsafe_b64decode(digest_b64.encode("ascii"))
        actual = hashlib.scrypt(
            token.encode("utf-8"),
            salt=salt,
            n=int(n),
            r=int(r),
            p=int(p),
            dklen=len(expected),
        )
        return hmac.compare_digest(expected, actual)
    except (ValueError, TypeError):
        return False


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def sha256_json(value: Any) -> str:
    return sha256_text(canonical_json(value))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def matches(pattern: str, value: str) -> bool:
    return bool(pattern) and fnmatch.fnmatchcase(value, pattern)


def is_loopback_host(host: str) -> bool:
    value = host.strip().lower().strip("[]").rstrip(".")
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def canonical_origin(raw_origin: str) -> str:
    parsed = urlparse(raw_origin)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValidationError("Origin must be an absolute http(s) origin", origin=raw_origin)
    if parsed.username or parsed.password or parsed.path not in {"", "/"}:
        raise ValidationError("Origin must not contain credentials or a path", origin=raw_origin)
    if parsed.params or parsed.query or parsed.fragment:
        raise ValidationError("Origin must not contain params, query, or fragment", origin=raw_origin)
    host = parsed.hostname.lower().rstrip(".")
    port = parsed.port or (80 if parsed.scheme == "http" else 443)
    default_port = 80 if parsed.scheme == "http" else 443
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{parsed.scheme}://{rendered_host}{'' if port == default_port else f':{port}'}"


def origin_allowed(raw_origin: str, allowed_origins: tuple[str, ...]) -> bool:
    if not raw_origin:
        return True
    if raw_origin.strip().lower() == "null":
        return False
    try:
        candidate = canonical_origin(raw_origin)
    except ValidationError:
        return False
    allowed: set[str] = set()
    for value in allowed_origins:
        try:
            allowed.add(canonical_origin(value))
        except ValidationError:
            continue
    return candidate in allowed


def require_loopback_url(raw_url: str, *, allow_remote: bool = False) -> None:
    """Validate a callback URL without DNS resolution.

    Resolving arbitrary hostnames and then trusting the result introduces a DNS
    rebinding/TOCTOU gap. Local mode therefore accepts only localhost or a
    literal loopback IP. Remote callbacks are an explicit deployment option.
    """

    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValidationError("URL must use http or https", url=raw_url)
    if not parsed.hostname:
        raise ValidationError("URL hostname is required", url=raw_url)
    if parsed.username or parsed.password:
        raise ValidationError("callback URL must not embed credentials", url=raw_url)
    if allow_remote:
        return
    if not is_loopback_host(parsed.hostname):
        raise ValidationError(
            "remote URL is disabled; use localhost/a literal loopback IP or enable remote callbacks",
            url=raw_url,
        )


def load_or_create_fernet_key(path: Path) -> bytes:
    from cryptography.fernet import Fernet

    if path.exists():
        value = path.read_bytes().strip()
        os.chmod(path, 0o600)
        return value
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    key = Fernet.generate_key()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(key + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    return key


def encrypt_secret(value: str, key_path: Path) -> str:
    if not value:
        return ""
    from cryptography.fernet import Fernet

    key = load_or_create_fernet_key(key_path)
    return Fernet(key).encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_secret(value: str, key_path: Path) -> str:
    if not value:
        return ""
    from cryptography.fernet import Fernet, InvalidToken

    key = load_or_create_fernet_key(key_path)
    try:
        return Fernet(key).decrypt(value.encode("ascii")).decode("utf-8")
    except InvalidToken as error:
        raise ValidationError("stored callback credential cannot be decrypted") from error
