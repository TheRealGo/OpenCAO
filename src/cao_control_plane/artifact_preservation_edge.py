"""Owner-private, fail-closed artifact-preservation evidence.

This edge is deliberately separate from the database and cleanup executor.
Before a destructive explicit-close operation, its trusted owner-side caller
copies each regular artifact into a content-addressed archive and seals the
exact receipt set for one close preparation.  Callers can pass only sanitized
receipts onward; source and archive locators never appear in those receipts.

The private ledger detects accidental corruption and substitution between the
control plane and this edge.  It is *not* a security boundary against code
already running as this same operating-system UID: that code can read or
replace owner-private files and their signing key.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import hmac
import json
import os
import secrets
import stat
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import canonical_json_bytes as _canonical
from .security import contains_control_plane_secret, contains_generic_credential_text


class ArtifactPreservationProviderError(RuntimeError):
    """A path-free failure from the owner-private preservation edge."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


_VERSION = 1
_IDENTIFIER_MAX = 127
_STAGED_REFERENCE_PREFIX = "owner-private-artifact:"
ARTIFACT_CONTENT_MAX_BYTES = 1024 * 1024
ARTIFACT_CONTENT_CHUNK_MAX_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class VerifiedArtifactTextChunk:
    """Path-free result of revalidating and reading one UTF-8 archive chunk."""

    content: str
    byte_offset: int
    byte_count: int
    total_bytes: int
    next_byte_offset: int | None
    complete: bool
    chunk_digest: str


def _hmac_hex(key: bytes, value: object) -> str:
    return hmac.new(key, _canonical(value), hashlib.sha256).hexdigest()


def _evidence_id(key: bytes, value: object) -> str:
    return "hmac-sha256:" + _hmac_hex(key, value)


class OwnerPrivateArtifactPreservation:
    """Archive and seal artifacts for exactly one explicit-close binding.

    ``stage_file`` is the trusted observation step: it opens a regular,
    non-linked source and verifies its requested SHA-256 while copying it into
    the private archive.  The only return value is an opaque content reference.
    ``register_staged_file`` binds that retained content to one close
    preparation without receiving a raw source locator.  ``seal_receipt_set``
    accepts the canonical artifact id/digest set, checks every archive entry
    again, and creates an immutable public receipt set.  Cleanup integrations
    should require a successful ``verify_public_receipt_set``.
    """

    def __init__(self, state_dir: Path, *, archive_root: Path | None = None) -> None:
        self._state_dir = Path(state_dir)
        self._archive_root = (
            Path(archive_root) if archive_root is not None else self._state_dir / "artifact-archive-v1"
        )
        self._ledger_path = self._state_dir / "artifact-preservation-owner-private-v1.json"
        self._lock_path = self._state_dir / "artifact-preservation-v1.lock"
        self._thread_lock = threading.RLock()

    def register_file(
        self,
        *,
        work_item_id: str,
        preparation_id: str,
        attachment_id: str,
        attachment_generation: int,
        artifact_id: str,
        digest: str,
        locator: Path,
    ) -> dict[str, str]:
        """Compatibility helper: stage, then bind one file without persistence.

        New integrations should use ``stage_file`` at artifact creation time
        and then ``register_staged_file`` during close preparation.  This
        helper intentionally does not persist the raw source locator either.
        """

        reference = self.stage_file(digest=digest, locator=locator)
        return self.register_staged_file(
            work_item_id=work_item_id,
            preparation_id=preparation_id,
            attachment_id=attachment_id,
            attachment_generation=attachment_generation,
            artifact_id=artifact_id,
            digest=digest,
            staged_ref=reference,
        )

    def stage_file(self, *, digest: str, locator: Path) -> str:
        """Copy and verify a source file, returning only an opaque reference."""

        canonical_digest = self._digest(digest)
        with self._locked():
            self._observe_and_archive(locator, canonical_digest)
        return self._staged_reference(canonical_digest)

    def stage_bytes(self, *, digest: str, content: bytes) -> str:
        """Verify bounded in-memory content and retain it behind an opaque ref."""

        canonical_digest = self._digest(digest)
        if not isinstance(content, bytes):
            raise ArtifactPreservationProviderError(
                "artifact_preservation_inline_content_invalid"
            )
        if not hmac.compare_digest(hashlib.sha256(content).hexdigest(), canonical_digest):
            raise ArtifactPreservationProviderError(
                "artifact_preservation_source_digest_mismatch"
            )
        with self._locked():
            archive = self._archive_path(canonical_digest)
            self._ensure_private_directory(archive.parent)
            self._write_or_validate_archive_bytes(content, archive, canonical_digest)
        return self._staged_reference(canonical_digest)

    def read_verified_text_chunk(
        self,
        *,
        digest: str,
        byte_offset: int,
        max_bytes: int,
    ) -> VerifiedArtifactTextChunk:
        """Rehash one retained artifact and return a bounded UTF-8 chunk.

        The caller supplies only a verified content digest, never an archive
        locator.  Total content and each returned chunk are independently
        bounded.  Byte offsets must be exact UTF-8 boundaries so pagination
        cannot silently replace or skip bytes.
        """

        canonical_digest = self._digest(digest)
        if (
            not isinstance(byte_offset, int)
            or isinstance(byte_offset, bool)
            or byte_offset < 0
            or byte_offset > ARTIFACT_CONTENT_MAX_BYTES
        ):
            raise ArtifactPreservationProviderError(
                "artifact_content_offset_out_of_range"
            )
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes < 4
            or max_bytes > ARTIFACT_CONTENT_CHUNK_MAX_BYTES
        ):
            raise ArtifactPreservationProviderError(
                "artifact_content_chunk_size_invalid"
            )

        with self._locked():
            encoded = self._read_staged_archive_bounded(canonical_digest)
        try:
            decoded = encoded.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ArtifactPreservationProviderError(
                "artifact_content_not_utf8"
            ) from error
        if contains_control_plane_secret(decoded):
            raise ArtifactPreservationProviderError(
                "artifact_content_contains_control_plane_secret"
            )
        if contains_generic_credential_text(decoded):
            raise ArtifactPreservationProviderError(
                "artifact_content_contains_generic_credential"
            )
        if byte_offset > len(encoded):
            raise ArtifactPreservationProviderError(
                "artifact_content_offset_out_of_range"
            )
        try:
            encoded[:byte_offset].decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ArtifactPreservationProviderError(
                "artifact_content_offset_not_utf8_boundary"
            ) from error

        end = min(len(encoded), byte_offset + max_bytes)
        while end > byte_offset:
            try:
                content = encoded[byte_offset:end].decode("utf-8", errors="strict")
                break
            except UnicodeDecodeError:
                end -= 1
        else:
            content = ""
        if end == byte_offset and byte_offset < len(encoded):
            # A valid UTF-8 scalar is at most four bytes, and max_bytes is at
            # least four.  Reaching this state means an invariant changed.
            raise ArtifactPreservationProviderError(
                "artifact_content_chunk_boundary_unavailable"
            )
        chunk = encoded[byte_offset:end]
        complete = end == len(encoded)
        return VerifiedArtifactTextChunk(
            content=content,
            byte_offset=byte_offset,
            byte_count=len(chunk),
            total_bytes=len(encoded),
            next_byte_offset=None if complete else end,
            complete=complete,
            chunk_digest=hashlib.sha256(chunk).hexdigest(),
        )

    def stage_workspace_file(
        self,
        *,
        digest: str,
        workspace_fd: int,
        relative_locator: str,
    ) -> str:
        """Stage a relative file through an already verified workspace FD.

        Every path component is opened relative to the pinned Directory and
        with symlink following disabled. The raw relative locator never enters
        the archive ledger or the returned reference.
        """

        canonical_digest = self._digest(digest)
        if (
            not isinstance(workspace_fd, int)
            or workspace_fd < 0
            or not isinstance(relative_locator, str)
            or not relative_locator
            or len(relative_locator) > 4096
            or relative_locator.startswith("/")
            or "\\" in relative_locator
            or "\x00" in relative_locator
        ):
            raise ArtifactPreservationProviderError(
                "artifact_preservation_workspace_locator_invalid"
            )
        parts = relative_locator.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ArtifactPreservationProviderError(
                "artifact_preservation_workspace_locator_invalid"
            )

        directory_fd: int | None = None
        source_fd: int | None = None
        try:
            directory_fd = os.dup(workspace_fd)
            directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            file_flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                directory_flags |= os.O_NOFOLLOW
                file_flags |= os.O_NOFOLLOW
            if hasattr(os, "O_NONBLOCK"):
                # A special file must not be able to block the Control Plane
                # before the regular-file fence below can reject it.
                directory_flags |= os.O_NONBLOCK
                file_flags |= os.O_NONBLOCK
            for component in parts[:-1]:
                next_fd = os.open(component, directory_flags, dir_fd=directory_fd)
                os.close(directory_fd)
                directory_fd = next_fd
            source_fd = os.open(parts[-1], file_flags, dir_fd=directory_fd)
            source_info = os.fstat(source_fd)
            self._require_regular_single_link(
                source_info, "artifact_preservation_source_unsafe"
            )
            with self._locked():
                archive = self._archive_path(canonical_digest)
                self._ensure_private_directory(archive.parent)
                self._copy_or_validate_archive(
                    source_fd, archive, canonical_digest
                )
        except ArtifactPreservationProviderError:
            raise
        except OSError as error:
            raise ArtifactPreservationProviderError(
                "artifact_preservation_workspace_source_unavailable"
            ) from error
        finally:
            if source_fd is not None:
                os.close(source_fd)
            if directory_fd is not None:
                os.close(directory_fd)
        return self._staged_reference(canonical_digest)

    def register_staged_file(
        self,
        *,
        work_item_id: str,
        preparation_id: str,
        attachment_id: str,
        attachment_generation: int,
        artifact_id: str,
        digest: str,
        staged_ref: str,
    ) -> dict[str, str]:
        """Bind an already archived artifact without accepting a source path.

        The staged reference is intentionally only a digest-scoped opaque
        handle.  The private archive is revalidated by its deterministic path,
        inode, link count, and content digest before any public receipt exists.
        """

        binding = self._binding(
            work_item_id, preparation_id, attachment_id, attachment_generation
        )
        artifact = self._artifact(artifact_id, digest)
        if staged_ref != self._staged_reference(artifact["digest"]):
            raise ArtifactPreservationProviderError("artifact_preservation_staged_reference_invalid")
        with self._locked():
            ledger = self._read_or_create_ledger_locked()
            key = self._key(ledger)
            preparation = self._preparation(ledger, binding, create=True)
            sealed = preparation.get("sealed")
            if sealed is not None:
                frozen = self._validate_sealed_locked(sealed, key, binding)
                for receipt in frozen["public_receipts"]:
                    if receipt["artifact_id"] == artifact_id and receipt["digest"] == digest:
                        return dict(receipt)
                raise ArtifactPreservationProviderError("artifact_preservation_set_already_sealed")

            registrations = preparation["registrations"]
            existing = registrations.get(artifact_id)
            if existing is not None:
                self._validate_registration_locked(existing, key, binding, artifact)
                self._validate_archive_record(existing, digest)
                return dict(existing["public_receipt"])

            archive, archive_info = self._staged_archive(artifact["digest"])
            receipt = self._public_registration_receipt(key, binding, artifact)
            record: dict[str, Any] = {
                "binding": binding,
                "artifact": artifact,
                "archive_locator": str(archive),
                "archive_dev": int(archive_info.st_dev),
                "archive_ino": int(archive_info.st_ino),
                "public_receipt": receipt,
            }
            record["seal"] = _evidence_id(
                key, {"domain": "cao-artifact-preservation-registration-v1", "record": record}
            )
            registrations[artifact_id] = record
            self._atomic_write_locked(self._ledger_path, ledger)
            return dict(receipt)

    def seal_receipt_set(
        self,
        *,
        work_item_id: str,
        preparation_id: str,
        attachment_id: str,
        attachment_generation: int,
        artifacts: Sequence[Mapping[str, object]],
    ) -> list[dict[str, str]]:
        """Seal the exact canonical artifact id/digest set, fail closed.

        The supplied values contain no locators and are compared with trusted
        registrations.  Once sealed, an identical replay returns the same
        receipts; any substitution, omission, or later registration fails.
        """

        binding = self._binding(
            work_item_id, preparation_id, attachment_id, attachment_generation
        )
        expected = self._canonical_artifact_set(artifacts)
        with self._locked():
            ledger = self._read_or_create_ledger_locked()
            key = self._key(ledger)
            preparation = self._preparation(ledger, binding, create=True)
            sealed = preparation.get("sealed")
            if sealed is not None:
                frozen = self._validate_sealed_locked(sealed, key, binding)
                if _canonical(expected) != _canonical(frozen["artifacts"]):
                    raise ArtifactPreservationProviderError("artifact_preservation_set_mismatch")
                return [dict(receipt) for receipt in frozen["public_receipts"]]

            registrations = preparation["registrations"]
            if set(registrations) != {item["artifact_id"] for item in expected}:
                raise ArtifactPreservationProviderError("artifact_preservation_registration_incomplete")
            for artifact in expected:
                record = registrations[artifact["artifact_id"]]
                self._validate_registration_locked(record, key, binding, artifact)
                self._validate_archive_record(record, artifact["digest"])

            receipt_bases = [
                dict(registrations[item["artifact_id"]]["public_receipt"])
                for item in expected
            ]
            set_digest = hashlib.sha256(_canonical(receipt_bases)).hexdigest()
            public_receipts = [
                {
                    **receipt,
                    "preservation_set_digest": set_digest,
                    "provider_evidence_id": _evidence_id(
                        key,
                        {
                            "domain": "cao-artifact-preservation-receipt-v1",
                            "binding": binding,
                            "receipt": receipt,
                            "preservation_set_digest": set_digest,
                        },
                    ),
                }
                for receipt in receipt_bases
            ]
            sealed_record: dict[str, Any] = {
                "binding": binding,
                "artifacts": expected,
                "public_receipts": public_receipts,
            }
            sealed_record["seal"] = _evidence_id(
                key, {"domain": "cao-artifact-preservation-set-v1", "sealed": sealed_record}
            )
            preparation["sealed"] = sealed_record
            self._atomic_write_locked(self._ledger_path, ledger)
            return [dict(receipt) for receipt in public_receipts]

    def verify_public_receipt_set(
        self,
        *,
        work_item_id: str,
        preparation_id: str,
        attachment_id: str,
        attachment_generation: int,
        public_receipts: Sequence[Mapping[str, object]],
    ) -> bool:
        """Return true only for the exact immutable, still-retained set."""

        try:
            binding = self._binding(
                work_item_id, preparation_id, attachment_id, attachment_generation
            )
            with self._locked():
                ledger = self._read_or_create_ledger_locked(create=False)
                key = self._key(ledger)
                preparation = self._preparation(ledger, binding, create=False)
                sealed = preparation.get("sealed")
                if sealed is None:
                    return False
                frozen = self._validate_sealed_locked(sealed, key, binding)
                for artifact in frozen["artifacts"]:
                    record = preparation["registrations"].get(artifact["artifact_id"])
                    if record is None:
                        return False
                    self._validate_registration_locked(record, key, binding, artifact)
                    self._validate_archive_record(record, artifact["digest"])
                return hmac.compare_digest(
                    _canonical([dict(item) for item in public_receipts]),
                    _canonical(frozen["public_receipts"]),
                )
        except (ArtifactPreservationProviderError, OSError, TypeError, ValueError):
            return False

    # ------------------------------------------------------------------
    # Private ledger and binding validation
    # ------------------------------------------------------------------
    def _binding(
        self,
        work_item_id: str,
        preparation_id: str,
        attachment_id: str,
        attachment_generation: int,
    ) -> dict[str, object]:
        return {
            "work_item_id": self._identifier(work_item_id, "artifact_preservation_invalid_work"),
            "preparation_id": self._identifier(
                preparation_id, "artifact_preservation_invalid_preparation"
            ),
            "attachment_id": self._identifier(
                attachment_id, "artifact_preservation_invalid_attachment"
            ),
            "attachment_generation": self._nonnegative_int(
                attachment_generation, "artifact_preservation_invalid_attachment_generation"
            ),
        }

    def _artifact(self, artifact_id: str, digest: str) -> dict[str, str]:
        return {
            "artifact_id": self._identifier(
                artifact_id, "artifact_preservation_invalid_artifact"
            ),
            "digest": self._digest(digest),
        }

    @staticmethod
    def _staged_reference(digest: str) -> str:
        return _STAGED_REFERENCE_PREFIX + digest

    @staticmethod
    def _identifier(value: object, error: str) -> str:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > _IDENTIFIER_MAX
            or not value[0].isalnum()
            or any(not (character.isalnum() or character in "._:-") for character in value)
        ):
            raise ArtifactPreservationProviderError(error)
        return value

    @staticmethod
    def _digest(value: object) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_invalid_digest")
        return value

    @staticmethod
    def _nonnegative_int(value: object, error: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ArtifactPreservationProviderError(error)
        return value

    def _canonical_artifact_set(
        self, artifacts: Sequence[Mapping[str, object]]
    ) -> list[dict[str, str]]:
        if isinstance(artifacts, (str, bytes)):
            raise ArtifactPreservationProviderError("artifact_preservation_set_invalid")
        values: list[dict[str, str]] = []
        for raw in artifacts:
            if not isinstance(raw, Mapping) or set(raw) != {"artifact_id", "digest"}:
                raise ArtifactPreservationProviderError("artifact_preservation_set_invalid")
            artifact_id, digest = raw["artifact_id"], raw["digest"]
            if not isinstance(artifact_id, str) or not isinstance(digest, str):
                raise ArtifactPreservationProviderError("artifact_preservation_set_invalid")
            values.append(self._artifact(artifact_id, digest))
        values.sort(key=lambda item: item["artifact_id"])
        if len({item["artifact_id"] for item in values}) != len(values):
            raise ArtifactPreservationProviderError("artifact_preservation_set_invalid")
        return values

    def _preparation(
        self, ledger: dict[str, Any], binding: Mapping[str, object], *, create: bool
    ) -> dict[str, Any]:
        preparations = ledger.get("preparations")
        if not isinstance(preparations, dict):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        key = str(binding["preparation_id"])
        value = preparations.get(key)
        if value is None:
            if not create:
                raise ArtifactPreservationProviderError("artifact_preservation_observation_missing")
            value = {"binding": dict(binding), "registrations": {}}
            preparations[key] = value
        if (
            not isinstance(value, dict)
            or value.get("binding") != dict(binding)
            or not isinstance(value.get("registrations"), dict)
            or set(value).difference({"binding", "registrations", "sealed"})
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        return value

    def _read_or_create_ledger_locked(self, *, create: bool = True) -> dict[str, Any]:
        self._ensure_private_directory(self._state_dir)
        self._ensure_private_directory(self._archive_root)
        if not self._ledger_path.exists() and not self._ledger_path.is_symlink():
            if not create:
                raise ArtifactPreservationProviderError("artifact_preservation_ledger_missing")
            return {
                "version": _VERSION,
                "key": base64.b64encode(secrets.token_bytes(32)).decode("ascii"),
                "preparations": {},
            }
        info = self._safe_private_file(self._ledger_path)
        try:
            raw = json.loads(self._read_regular_file(self._ledger_path, info))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed") from error
        if (
            not isinstance(raw, dict)
            or set(raw) != {"version", "key", "preparations"}
            or raw.get("version") != _VERSION
            or not isinstance(raw.get("key"), str)
            or not isinstance(raw.get("preparations"), dict)
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        self._key(raw)
        return raw

    @staticmethod
    def _key(ledger: Mapping[str, object]) -> bytes:
        try:
            key = base64.b64decode(str(ledger["key"]), validate=True)
        except (KeyError, ValueError) as error:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed") from error
        if len(key) != 32:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        return key

    def _validate_registration_locked(
        self,
        record: object,
        key: bytes,
        binding: Mapping[str, object],
        artifact: Mapping[str, object],
    ) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        expected_fields = {
            "binding",
            "artifact",
            "archive_locator",
            "archive_dev",
            "archive_ino",
            "public_receipt",
            "seal",
        }
        if set(record) != expected_fields or record.get("binding") != dict(binding) or record.get("artifact") != dict(artifact):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        unsigned = {name: value for name, value in record.items() if name != "seal"}
        if not hmac.compare_digest(
            str(record["seal"]),
            _evidence_id(
                key, {"domain": "cao-artifact-preservation-registration-v1", "record": unsigned}
            ),
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_tampered")
        receipt = record.get("public_receipt")
        if not isinstance(receipt, dict) or receipt != self._public_registration_receipt(key, binding, artifact):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_tampered")
        return record

    def _validate_sealed_locked(
        self, sealed: object, key: bytes, binding: Mapping[str, object]
    ) -> dict[str, Any]:
        if not isinstance(sealed, dict) or set(sealed) != {"binding", "artifacts", "public_receipts", "seal"}:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        unsigned = {name: value for name, value in sealed.items() if name != "seal"}
        if (
            sealed.get("binding") != dict(binding)
            or not hmac.compare_digest(
                str(sealed["seal"]),
                _evidence_id(key, {"domain": "cao-artifact-preservation-set-v1", "sealed": unsigned}),
            )
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_tampered")
        artifacts = sealed.get("artifacts")
        receipts = sealed.get("public_receipts")
        if not isinstance(artifacts, list) or not isinstance(receipts, list):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        canonical_artifacts = self._canonical_artifact_set(artifacts)
        if artifacts != canonical_artifacts or len(receipts) != len(artifacts):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        bases = [self._public_registration_receipt(key, binding, artifact) for artifact in artifacts]
        set_digest = hashlib.sha256(_canonical(bases)).hexdigest()
        expected_receipts = [
            {
                **receipt,
                "preservation_set_digest": set_digest,
                "provider_evidence_id": _evidence_id(
                    key,
                    {
                        "domain": "cao-artifact-preservation-receipt-v1",
                        "binding": binding,
                        "receipt": receipt,
                        "preservation_set_digest": set_digest,
                    },
                ),
            }
            for receipt in bases
        ]
        if receipts != expected_receipts:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_tampered")
        return sealed

    @staticmethod
    def _public_registration_receipt(
        key: bytes, binding: Mapping[str, object], artifact: Mapping[str, object]
    ) -> dict[str, str]:
        receipt = {
            "artifact_id": str(artifact["artifact_id"]),
            "digest": str(artifact["digest"]),
            "preservation": "content-addressed-archive",
        }
        receipt["observation_evidence_id"] = _evidence_id(
            key,
            {
                "domain": "cao-artifact-preservation-observation-v1",
                "binding": binding,
                "artifact": artifact,
                "preservation": receipt["preservation"],
            },
        )
        return receipt

    # ------------------------------------------------------------------
    # File observation and atomic private storage
    # ------------------------------------------------------------------
    def _observe_and_archive(self, locator: Path, digest: str) -> tuple[Path, os.stat_result]:
        source = self._safe_source(locator)
        source_info = source.lstat()
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(source, flags)
        except OSError as error:
            raise ArtifactPreservationProviderError("artifact_preservation_source_unavailable") from error
        try:
            opened = os.fstat(descriptor)
            self._require_regular_single_link(opened, "artifact_preservation_source_unsafe")
            if (opened.st_dev, opened.st_ino) != (source_info.st_dev, source_info.st_ino):
                raise ArtifactPreservationProviderError("artifact_preservation_source_changed")
            archive = self._archive_path(digest)
            self._ensure_private_directory(archive.parent)
            archive_info = self._copy_or_validate_archive(descriptor, archive, digest)
        finally:
            os.close(descriptor)
        return archive, archive_info

    def _staged_archive(self, digest: str) -> tuple[Path, os.stat_result]:
        archive = self._archive_path(digest)
        try:
            info = archive.lstat()
            self._require_regular_single_link(info, "artifact_preservation_archive_unsafe")
            if hashlib.sha256(self._read_fd_path(archive)).hexdigest() != digest:
                raise ArtifactPreservationProviderError("artifact_preservation_archive_digest_mismatch")
            return archive, info
        except ArtifactPreservationProviderError:
            raise
        except OSError as error:
            raise ArtifactPreservationProviderError("artifact_preservation_archive_unavailable") from error

    def _read_staged_archive_bounded(self, digest: str) -> bytes:
        """Read one content-addressed archive through a single verified FD."""

        archive = self._archive_path(digest)
        try:
            expected = archive.lstat()
            self._require_regular_single_link(
                expected, "artifact_preservation_archive_unsafe"
            )
            if expected.st_size > ARTIFACT_CONTENT_MAX_BYTES:
                raise ArtifactPreservationProviderError(
                    "artifact_content_too_large"
                )
            flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            if hasattr(os, "O_NONBLOCK"):
                flags |= os.O_NONBLOCK
            descriptor = os.open(archive, flags)
        except ArtifactPreservationProviderError:
            raise
        except OSError as error:
            raise ArtifactPreservationProviderError(
                "artifact_preservation_archive_unavailable"
            ) from error
        try:
            opened = os.fstat(descriptor)
            self._require_regular_single_link(
                opened, "artifact_preservation_archive_unsafe"
            )
            if (opened.st_dev, opened.st_ino) != (expected.st_dev, expected.st_ino):
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_identity_changed"
                )
            if opened.st_size > ARTIFACT_CONTENT_MAX_BYTES:
                raise ArtifactPreservationProviderError(
                    "artifact_content_too_large"
                )
            parts: list[bytes] = []
            remaining = ARTIFACT_CONTENT_MAX_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                parts.append(chunk)
                remaining -= len(chunk)
            content = b"".join(parts)
            final = os.fstat(descriptor)
            if (
                (final.st_dev, final.st_ino, final.st_nlink, final.st_size)
                != (opened.st_dev, opened.st_ino, opened.st_nlink, opened.st_size)
            ):
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_identity_changed"
                )
            if len(content) > ARTIFACT_CONTENT_MAX_BYTES:
                raise ArtifactPreservationProviderError(
                    "artifact_content_too_large"
                )
            if not hmac.compare_digest(hashlib.sha256(content).hexdigest(), digest):
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_digest_mismatch"
                )
            return content
        except ArtifactPreservationProviderError:
            raise
        except OSError as error:
            raise ArtifactPreservationProviderError(
                "artifact_preservation_archive_unavailable"
            ) from error
        finally:
            os.close(descriptor)

    def _copy_or_validate_archive(
        self, source_fd: int, archive: Path, digest: str
    ) -> os.stat_result:
        try:
            existing = archive.lstat()
        except FileNotFoundError:
            existing = None
        if existing is not None:
            self._require_regular_single_link(
                existing, "artifact_preservation_archive_unsafe"
            )
            digestor = hashlib.sha256()
            os.lseek(source_fd, 0, os.SEEK_SET)
            while True:
                chunk = os.read(source_fd, 1024 * 1024)
                if not chunk:
                    break
                digestor.update(chunk)
            if digestor.hexdigest() != digest:
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_source_digest_mismatch"
                )
            if hashlib.sha256(self._read_fd_path(archive)).hexdigest() != digest:
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_digest_mismatch"
                )
            return existing

        temp = archive.parent / f".{archive.name}.{secrets.token_hex(16)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            destination_fd = os.open(temp, flags, 0o600)
        except OSError as error:
            raise ArtifactPreservationProviderError(
                "artifact_preservation_archive_unavailable"
            ) from error
        try:
            digestor = hashlib.sha256()
            os.lseek(source_fd, 0, os.SEEK_SET)
            while True:
                chunk = os.read(source_fd, 1024 * 1024)
                if not chunk:
                    break
                digestor.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(destination_fd, view)
                    view = view[written:]
            if digestor.hexdigest() != digest:
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_source_digest_mismatch"
                )
            os.fsync(destination_fd)
            destination_info = os.fstat(destination_fd)
            self._require_regular_single_link(
                destination_info, "artifact_preservation_archive_unsafe"
            )
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(temp)
            raise
        finally:
            os.close(destination_fd)
        try:
            os.replace(temp, archive)
            self._fsync_directory(archive.parent)
            info = archive.lstat()
            self._require_regular_single_link(
                info, "artifact_preservation_archive_unsafe"
            )
            if hashlib.sha256(self._read_fd_path(archive)).hexdigest() != digest:
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_digest_mismatch"
                )
            return info
        except OSError as error:
            raise ArtifactPreservationProviderError(
                "artifact_preservation_archive_unavailable"
            ) from error

    def _write_or_validate_archive_bytes(
        self, content: bytes, archive: Path, digest: str
    ) -> os.stat_result:
        """Create one content-addressed archive without materializing a locator."""

        try:
            existing = archive.lstat()
        except FileNotFoundError:
            existing = None
        if existing is not None:
            self._require_regular_single_link(
                existing, "artifact_preservation_archive_unsafe"
            )
            if hashlib.sha256(self._read_fd_path(archive)).hexdigest() != digest:
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_digest_mismatch"
                )
            return existing

        temporary = archive.parent / f".{archive.name}.{secrets.token_hex(16)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(temporary, flags, 0o600)
        except OSError as error:
            raise ArtifactPreservationProviderError(
                "artifact_preservation_archive_unavailable"
            ) from error
        try:
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            os.fsync(descriptor)
            info = os.fstat(descriptor)
            self._require_regular_single_link(
                info, "artifact_preservation_archive_unsafe"
            )
        except BaseException:
            with suppress(FileNotFoundError):
                os.unlink(temporary)
            raise
        finally:
            os.close(descriptor)
        try:
            os.replace(temporary, archive)
            self._fsync_directory(archive.parent)
            info = archive.lstat()
            self._require_regular_single_link(
                info, "artifact_preservation_archive_unsafe"
            )
            if hashlib.sha256(self._read_fd_path(archive)).hexdigest() != digest:
                raise ArtifactPreservationProviderError(
                    "artifact_preservation_archive_digest_mismatch"
                )
            return info
        except OSError as error:
            with suppress(FileNotFoundError):
                temporary.unlink()
            raise ArtifactPreservationProviderError(
                "artifact_preservation_archive_unavailable"
            ) from error

    def _validate_archive_record(self, record: Mapping[str, object], digest: str) -> None:
        try:
            archive = Path(str(record["archive_locator"]))
            expected = self._archive_path(digest)
            if archive != expected:
                raise ArtifactPreservationProviderError("artifact_preservation_archive_identity_changed")
            _archive, info = self._staged_archive(digest)
            if (
                self._stored_int(record, "archive_dev"),
                self._stored_int(record, "archive_ino"),
            ) != (info.st_dev, info.st_ino):
                raise ArtifactPreservationProviderError("artifact_preservation_archive_identity_changed")
        except (KeyError, TypeError, ValueError, OSError) as error:
            if isinstance(error, ArtifactPreservationProviderError):
                raise
            raise ArtifactPreservationProviderError("artifact_preservation_archive_unavailable") from error

    @staticmethod
    def _stored_int(record: Mapping[str, object], field: str) -> int:
        value = record.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_malformed")
        return value

    def _archive_path(self, digest: str) -> Path:
        root = self._private_canonical_directory(self._archive_root, "artifact_preservation_archive_unavailable")
        return root / digest[:2] / digest

    @staticmethod
    def _safe_source(locator: Path) -> Path:
        raw = Path(os.path.abspath(os.fspath(locator)))
        try:
            info = raw.lstat()
            canonical = raw.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise ArtifactPreservationProviderError("artifact_preservation_source_unavailable") from error
        if canonical != raw or stat.S_ISLNK(info.st_mode):
            raise ArtifactPreservationProviderError("artifact_preservation_source_symlink_forbidden")
        OwnerPrivateArtifactPreservation._require_regular_single_link(
            info, "artifact_preservation_source_unsafe"
        )
        return canonical

    @staticmethod
    def _require_regular_single_link(info: os.stat_result, error: str) -> None:
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ArtifactPreservationProviderError(error)

    def _read_fd_path(self, path: Path) -> bytes:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            info = os.fstat(descriptor)
            self._require_regular_single_link(info, "artifact_preservation_archive_unsafe")
            parts: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                parts.append(chunk)
            return b"".join(parts)
        finally:
            os.close(descriptor)

    @staticmethod
    def _ensure_private_directory(path: Path) -> None:
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = path.lstat()
        except OSError as error:
            raise ArtifactPreservationProviderError("artifact_preservation_private_storage_unavailable") from error
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_private_storage_unsafe")
        try:
            os.chmod(path, 0o700)
        except OSError as error:
            raise ArtifactPreservationProviderError("artifact_preservation_private_storage_unavailable") from error

    def _private_canonical_directory(self, path: Path, error: str) -> Path:
        try:
            self._ensure_private_directory(path)
            raw = Path(os.path.abspath(os.fspath(path)))
            resolved = raw.resolve(strict=True)
        except ArtifactPreservationProviderError:
            raise
        except (OSError, RuntimeError, ValueError) as exception:
            raise ArtifactPreservationProviderError(error) from exception
        if raw != resolved:
            raise ArtifactPreservationProviderError(error)
        return resolved

    @staticmethod
    def _safe_private_file(path: Path) -> os.stat_result:
        try:
            info = path.lstat()
        except OSError as error:
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_unavailable") from error
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or info.st_mode & 0o077
        ):
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_unsafe")
        return info

    @staticmethod
    def _read_regular_file(path: Path, expected: os.stat_result) -> str:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            actual = os.fstat(descriptor)
            if (actual.st_dev, actual.st_ino, actual.st_nlink) != (
                expected.st_dev,
                expected.st_ino,
                expected.st_nlink,
            ):
                raise ArtifactPreservationProviderError("artifact_preservation_ledger_changed")
            return os.read(descriptor, actual.st_size + 1).decode("utf-8")
        finally:
            os.close(descriptor)

    def _atomic_write_locked(self, path: Path, value: Mapping[str, object]) -> None:
        self._ensure_private_directory(path.parent)
        temporary = path.parent / f".{path.name}.{secrets.token_hex(16)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        encoded = _canonical(value)
        try:
            descriptor = os.open(temporary, flags, 0o600)
            try:
                offset = 0
                while offset < len(encoded):
                    offset += os.write(descriptor, encoded[offset:])
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            os.replace(temporary, path)
            os.chmod(path, 0o600)
            self._safe_private_file(path)
            self._fsync_directory(path.parent)
        except OSError as error:
            with suppress(FileNotFoundError):
                temporary.unlink()
            raise ArtifactPreservationProviderError("artifact_preservation_ledger_unavailable") from error

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        with self._thread_lock:
            self._ensure_private_directory(self._state_dir)
            flags = os.O_RDWR | os.O_CREAT
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(self._lock_path, flags, 0o600)
            except OSError as error:
                raise ArtifactPreservationProviderError("artifact_preservation_lock_unavailable") from error
            try:
                info = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or info.st_nlink != 1
                    or info.st_mode & 0o077
                ):
                    raise ArtifactPreservationProviderError("artifact_preservation_lock_unsafe")
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                finally:
                    os.close(descriptor)
