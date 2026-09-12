"""Owner-private workspace placement decisions.

This module is deliberately an edge contract.  The JSON policy is read only
from a local, owner-protected file; it is never serialized into control-plane
state.  The only transferable result is an opaque, binding-specific decision
record that a launch integration must validate again immediately before use.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from .canonical import canonical_json_bytes as _canonical_json_bytes
from .directory_identity import canonical_directory_path, directory_identity
from .directory_identity import object_generation as _stat_object_generation

_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")
_RUNNERS = frozenset({"claude", "codex"})
_POLICY_KEYS = frozenset({"policy_id", "policy_version", "evidence_key", "runners"})
_POLICY_KEYS_WITH_WORKSPACES = _POLICY_KEYS | {"workspaces"}
_RULE_KEYS = frozenset({"allow_within", "deny_within"})
_DYNAMIC_WORKSPACE_REF = re.compile(r"^cao-dynamic-[0-9a-f]{64}$")
_WORKSPACE_REGISTRY_FORMAT = "cao-owner-private-workspaces/v1"
_WORKSPACE_REGISTRY_FIELDS = frozenset({"format", "key", "revision", "workspaces"})
_WORKSPACE_REGISTRY_ENTRY_FIELDS = frozenset(
    {"path", "device", "inode", "object_generation", "runner", "seal"}
)
_WORKSPACE_REGISTRY_MAX_BYTES = 4 * 1024 * 1024
_WORKSPACE_REGISTRY_THREAD_LOCK = threading.RLock()


class PrivatePolicyError(RuntimeError):
    """A fail-closed, path-free owner-private policy error."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self)}


@dataclass(frozen=True, slots=True)
class PlacementBinding:
    """Inputs that a policy decision seals without placing them in CP state."""

    principal_id: str
    runtime_id: str
    assignment_id: str
    work_item_id: str
    runner_adapter: Literal["claude", "codex"]
    launch_generation: int

    def __post_init__(self) -> None:
        if self.runner_adapter not in _RUNNERS:
            raise PrivatePolicyError("owner_private_policy_invalid_binding")
        if self.launch_generation < 0 or isinstance(self.launch_generation, bool):
            raise PrivatePolicyError("owner_private_policy_invalid_binding")
        for value in (
            self.principal_id,
            self.runtime_id,
            self.assignment_id,
            self.work_item_id,
        ):
            if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
                raise PrivatePolicyError("owner_private_policy_invalid_binding")


@dataclass(frozen=True, slots=True)
class PlacementDecision:
    """The complete, sanitized durable representation of one policy decision."""

    policy_id: str
    policy_version: str
    policy_digest: str
    decision: Literal["allow", "deny"]
    runner_adapter: Literal["claude", "codex"]
    workspace_identity_digest: str
    evidence_id: str
    expires_at: int
    revoked_at: int | None = None

    def as_durable(self) -> dict[str, str | int | None]:
        """Return exactly the fields permitted in control-plane durable state."""

        return asdict(self)


@dataclass(frozen=True, slots=True)
class _PolicySnapshot:
    policy_id: str
    policy_version: str
    digest: str
    evidence_key: bytes
    runner_rules: Mapping[str, tuple[tuple[Path, ...], tuple[Path, ...]]]
    workspaces: Mapping[str, Path]
    static_workspace_refs: frozenset[str]


class OwnerPrivatePolicyEdge:
    """Evaluate placement from a concrete owner-private file provider.

    ``policy_file`` is supplied by the owner-local deployment.  It has no
    default so tracked code never carries a private directory, root, or URL.
    The parent directory must be a canonical, owner-only ``0700`` directory;
    the policy itself must be a single-link, owner-only ``0600`` regular file.
    """

    def __init__(
        self,
        policy_file: Path,
        *,
        workspace_registry_file: Path | None = None,
        decision_ttl_seconds: int = 30,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if decision_ttl_seconds <= 0:
            raise ValueError("decision_ttl_seconds must be positive")
        self._policy_file = Path(policy_file)
        self._workspace_registry_file = (
            None if workspace_registry_file is None else Path(workspace_registry_file)
        )
        self._ttl = decision_ttl_seconds
        self._clock = clock

    def evaluate(self, binding: PlacementBinding, workspace: Path) -> PlacementDecision:
        """Produce a signed allow or deny result from the local policy provider."""

        snapshot = self._load_policy()
        return self._evaluate_snapshot(snapshot, binding, workspace)

    def register_workspace(
        self,
        workspace: Path,
        *,
        runner: Literal["claude", "codex"],
    ) -> str:
        """Register one existing borrowed Directory behind an opaque reference.

        The raw path and inode identity are written only to the owner-private
        registry.  Registration is deterministic for one concrete Directory
        and runner so a retried public idempotent request gets the same opaque
        reference without putting its locator in Control Plane state.
        """

        if not isinstance(runner, str) or runner not in _RUNNERS:
            raise PrivatePolicyError("owner_private_policy_invalid_binding")
        if self._workspace_registry_file is None:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        snapshot = self._load_policy()
        candidate, info = self._registered_workspace_target(workspace)
        self._require_workspace_separate_from_registry(candidate)
        _identity, authorized = self._workspace_identity(snapshot, candidate)
        if authorized != candidate:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        if not self._runner_allows(snapshot, runner, authorized):
            raise PrivatePolicyError("owner_private_policy_launch_denied")
        try:
            current = authorized.lstat()
        except OSError as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if _stat_identity(current) != _stat_identity(info):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        object_generation = _stat_object_generation(info)
        persistent_identity = self._persistent_identity(authorized)
        if _stat_object_generation(current) != object_generation:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")

        with self._locked_workspace_registry():
            registry = self._read_or_create_workspace_registry_locked()
            key = self._workspace_registry_key(registry)
            workspaces = registry["workspaces"]
            assert isinstance(workspaces, dict)
            for existing_ref, existing_entry in workspaces.items():
                if (existing_entry.get("runner") != runner
                    or existing_entry.get("inode") != int(info.st_ino)
                    or existing_entry.get("persistent_identity", persistent_identity) != persistent_identity):
                    continue
                try:
                    resolved = self._resolve_registered_workspace_entry(existing_entry)
                except PrivatePolicyError:
                    continue
                if self._persistent_identity(resolved) == persistent_identity:
                    self._upgrade_workspace_identity_locked(registry, existing_ref, existing_entry)
                    return str(existing_ref)
            workspace_ref = self._registered_workspace_ref(
                key,
                runner=runner,
                device=int(info.st_dev),
                inode=int(info.st_ino),
                path=os.fspath(authorized),
                object_generation=object_generation,
                persistent_identity=persistent_identity,
            )
            entry_without_seal: dict[str, object] = {
                "path": os.fspath(authorized),
                "device": int(info.st_dev),
                "inode": int(info.st_ino),
                "object_generation": object_generation,
                "runner": runner,
                "persistent_identity": persistent_identity,
            }
            entry = {
                **entry_without_seal,
                "seal": self._workspace_registry_seal(
                    key, workspace_ref, entry_without_seal
                ),
            }
            workspaces = registry["workspaces"]
            assert isinstance(workspaces, dict)
            existing = workspaces.get(workspace_ref)
            if existing is not None:
                if existing != entry:
                    raise PrivatePolicyError(
                        "owner_private_policy_workspace_unavailable"
                    ) from None
                return workspace_ref
            workspaces[workspace_ref] = entry
            registry["revision"] = int(registry["revision"]) + 1
            self._write_workspace_registry_locked(registry)
            return workspace_ref

    def workspace_request_fingerprint(
        self,
        workspace: Path,
        *,
        runner: Literal["claude", "codex"],
    ) -> str:
        """Bind an exact public Directory argument without resolving it.

        This fingerprint is used only for Control Plane idempotency.  Because
        it does not stat the Directory, an exact retry can return its already
        committed result after the borrowed Directory is moved or removed.
        A first request still passes the strict registration checks below.
        """

        if runner not in _RUNNERS or self._workspace_registry_file is None:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        try:
            supplied = Path(os.fspath(workspace))
            if not supplied.is_absolute() or "\x00" in os.fspath(supplied):
                raise ValueError
            normalized = os.fspath(Path(os.path.abspath(os.fspath(supplied))))
        except (OSError, TypeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        with self._locked_workspace_registry():
            registry = self._read_or_create_workspace_registry_locked()
            key = self._workspace_registry_key(registry)
        proof = f"workspace-request-v1\0{runner}\0{normalized}".encode()
        return "hmac-sha256:" + hmac.new(key, proof, hashlib.sha256).hexdigest()

    @staticmethod
    def is_registered_workspace_ref(workspace_ref: object) -> bool:
        """Return whether a reference belongs to the dynamic registry namespace."""

        return (
            isinstance(workspace_ref, str)
            and _DYNAMIC_WORKSPACE_REF.fullmatch(workspace_ref) is not None
        )

    def has_registered_workspace_ref(self, workspace_ref: object) -> bool:
        """Return whether the owner-private registry contains this exact ref."""

        if (
            not self.is_registered_workspace_ref(workspace_ref)
            or self._workspace_registry_file is None
            or not os.path.lexists(self._workspace_registry_file)
        ):
            return False
        assert isinstance(workspace_ref, str)
        return self._registered_workspace_entry_if_present(workspace_ref) is not None

    def has_static_workspace_ref(self, workspace_ref: object) -> bool:
        """Return whether the explicit policy owns this legacy static ref."""

        return (
            _safe_identifier(workspace_ref)
            and workspace_ref in self._load_policy().static_workspace_refs
        )

    def workspace_ref_is_dynamic(self, workspace_ref: object) -> bool:
        """Classify a resolved ref while preserving legacy static namespaces."""

        if not self.is_registered_workspace_ref(workspace_ref):
            return False
        assert isinstance(workspace_ref, str)
        # Static owner policy entries predate the dynamic namespace and keep
        # precedence even if their identifier happens to match its syntax. Do
        # not inspect an unrelated dynamic registry in that case: corruption
        # there must not disable a valid static Worker.
        if workspace_ref in self._load_policy().static_workspace_refs:
            return False
        return self.has_registered_workspace_ref(workspace_ref)

    def resolve_workspace(
        self,
        workspace_ref: str,
        *,
        runner: Literal["claude", "codex"] | None = None,
    ) -> Path:
        """Resolve an opaque CP reference through this owner-private provider.

        The returned path is intentionally process-local.  Callers must not
        serialize it, put it in an event, or pass it through a model prompt.
        """

        if not _safe_identifier(workspace_ref):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        snapshot = self._load_policy()
        if workspace_ref in snapshot.static_workspace_refs:
            try:
                return snapshot.workspaces[workspace_ref]
            except KeyError:
                # A static legacy target may have been deliberately removed.
                # Keep its reference reserved so a dynamic registration cannot
                # take over that identity, while allowing unrelated dynamic
                # Worker creation to continue under the runner root policy.
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                ) from None
        if self.is_registered_workspace_ref(workspace_ref):
            if (
                not isinstance(runner, str)
                or runner not in _RUNNERS
                or self._workspace_registry_file is None
            ):
                raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
            entry = self._registered_workspace_entry_if_present(workspace_ref)
            if not isinstance(entry, Mapping) or entry.get("runner") != runner:
                raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
            workspace = self._resolve_registered_workspace_entry(entry)
            self._require_workspace_separate_from_registry(workspace)
            if not self._runner_allows(snapshot, runner, workspace):
                raise PrivatePolicyError("owner_private_policy_launch_denied")
            return workspace
        raise PrivatePolicyError("owner_private_policy_workspace_unavailable")

    def _evaluate_snapshot(
        self,
        snapshot: _PolicySnapshot,
        binding: PlacementBinding,
        workspace: Path,
        *,
        expires_at: int | None = None,
    ) -> PlacementDecision:
        workspace_identity, workspace_path = self._workspace_identity(snapshot, workspace)
        allowed = self._runner_allows(
            snapshot, binding.runner_adapter, workspace_path
        )
        return self._make_decision(
            snapshot,
            binding,
            workspace_identity,
            "allow" if allowed else "deny",
            expires_at=expires_at,
        )

    def require_fresh_launch_allow(
        self,
        decision: PlacementDecision,
        binding: PlacementBinding,
        workspace: Path,
        *,
        launch_generation: int,
    ) -> None:
        """Require a same-generation, unexpired local re-evaluation before launch.

        This is a gate contract only: it intentionally does not start a
        process.  The caller must invoke it at its actual process-launch
        boundary after obtaining the current assignment generation.
        """

        if launch_generation != binding.launch_generation:
            raise PrivatePolicyError("owner_private_policy_stale_generation")
        if (
            decision.decision != "allow"
            or decision.revoked_at is not None
            or decision.expires_at <= int(self._clock())
        ):
            raise PrivatePolicyError("owner_private_policy_launch_denied")
        # Re-read the local provider but keep the submitted expiry while
        # reconstructing its proof.  A fresh evaluation must not reject a
        # valid record merely because a clock tick changed the next TTL.
        refreshed = self._evaluate_snapshot(
            self._load_policy(), binding, workspace, expires_at=decision.expires_at
        )
        if refreshed.decision != "allow" or not hmac.compare_digest(
            refreshed.evidence_id, decision.evidence_id
        ):
            raise PrivatePolicyError("owner_private_policy_launch_denied")

    def acquire_launch_workspace(
        self,
        decision: PlacementDecision,
        binding: PlacementBinding,
        workspace: Path,
        *,
        launch_generation: int,
        workspace_ref: str = "",
    ) -> tuple[Path, int]:
        """Open and verify the exact Directory object used for process launch.

        Returning an open descriptor closes the final path-name race: the
        caller can use ``/dev/fd/<descriptor>`` as ``cwd`` and keep the
        descriptor inherited until the child has changed directory.  A rename
        or symlink replacement after this method returns cannot redirect that
        child to another filesystem object.
        """

        if launch_generation != binding.launch_generation:
            raise PrivatePolicyError("owner_private_policy_stale_generation")
        if (
            decision.decision != "allow"
            or decision.revoked_at is not None
            or decision.expires_at <= int(self._clock())
        ):
            raise PrivatePolicyError("owner_private_policy_launch_denied")

        snapshot = self._load_policy()
        expected = Path(workspace)
        expected_entry: Mapping[str, object] | None = None
        if workspace_ref:
            if workspace_ref in snapshot.static_workspace_refs:
                try:
                    resolved = snapshot.workspaces[workspace_ref]
                except KeyError:
                    raise PrivatePolicyError(
                        "owner_private_policy_workspace_unavailable"
                    ) from None
            else:
                if (
                    not self.is_registered_workspace_ref(workspace_ref)
                    or self._workspace_registry_file is None
                ):
                    raise PrivatePolicyError(
                        "owner_private_policy_workspace_unavailable"
                    ) from None
                expected_entry = self._registered_workspace_entry_if_present(
                    workspace_ref
                )
                if (
                    not isinstance(expected_entry, Mapping)
                    or expected_entry.get("runner") != binding.runner_adapter
                ):
                    raise PrivatePolicyError(
                        "owner_private_policy_workspace_unavailable"
                    ) from None
                resolved = self._resolve_registered_workspace_entry(expected_entry)
                self._require_workspace_separate_from_registry(resolved)
            if resolved != expected:
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                )

        descriptor: int | None = None
        try:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(expected, flags)
            info = os.fstat(descriptor)
            if not stat.S_ISDIR(info.st_mode):
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                )
            if expected_entry is not None and not self._workspace_entry_matches_stat(
                expected_entry, info
            ):
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                )
            if not self._runner_allows(
                snapshot, binding.runner_adapter, expected
            ):
                raise PrivatePolicyError("owner_private_policy_launch_denied")
            refreshed = self._make_decision(
                snapshot,
                binding,
                _workspace_identity_digest(snapshot, expected, info),
                "allow",
                expires_at=decision.expires_at,
            )
            if not hmac.compare_digest(refreshed.evidence_id, decision.evidence_id):
                raise PrivatePolicyError("owner_private_policy_launch_denied")
            acquired = descriptor
            descriptor = None
            return expected, acquired
        except PrivatePolicyError:
            raise
        except OSError as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _load_policy(self) -> _PolicySnapshot:
        policy_path, data = _read_owner_private_file(self._policy_file)
        try:
            document = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PrivatePolicyError("owner_private_policy_unavailable") from error
        if not isinstance(document, dict) or set(document) not in {
            _POLICY_KEYS,
            _POLICY_KEYS_WITH_WORKSPACES,
        }:
            raise PrivatePolicyError("owner_private_policy_unavailable")
        policy_id = document.get("policy_id")
        policy_version = document.get("policy_version")
        if (
            not isinstance(policy_id, str)
            or not isinstance(policy_version, str)
            or not _safe_identifier(policy_id)
            or not _safe_identifier(policy_version)
        ):
            raise PrivatePolicyError("owner_private_policy_unavailable")
        evidence_key = _decode_evidence_key(document.get("evidence_key"))
        runners = document.get("runners")
        if not isinstance(runners, dict) or set(runners) != _RUNNERS:
            raise PrivatePolicyError("owner_private_policy_unavailable")
        rules: dict[str, tuple[tuple[Path, ...], tuple[Path, ...]]] = {}
        for runner in _RUNNERS:
            raw_rule = runners.get(runner)
            if not isinstance(raw_rule, dict) or set(raw_rule) != _RULE_KEYS:
                raise PrivatePolicyError("owner_private_policy_unavailable")
            allow = _load_roots(raw_rule.get("allow_within"))
            deny = _load_roots(raw_rule.get("deny_within"))
            # The installed runner kinds have an intentionally asymmetric
            # placement boundary.  Locations stay entirely in the local file,
            # but an empty rule must never silently turn either boundary into
            # an allow-all policy.
            if (runner == "claude" and not allow) or (runner == "codex" and not deny):
                raise PrivatePolicyError("owner_private_policy_unavailable")
            rules[runner] = (allow, deny)
        workspaces: dict[str, Path] = {}
        static_workspace_refs: set[str] = set()
        raw_workspaces = document.get("workspaces", {})
        if not isinstance(raw_workspaces, dict):
            raise PrivatePolicyError("owner_private_policy_unavailable")
        for workspace_ref, raw_workspace in raw_workspaces.items():
            if not _safe_identifier(workspace_ref) or not isinstance(raw_workspace, str):
                raise PrivatePolicyError("owner_private_policy_unavailable")
            static_workspace_refs.add(workspace_ref)
            workspace = _load_optional_static_workspace(raw_workspace)
            if workspace is not None:
                workspaces[workspace_ref] = workspace
        # Policy bytes, not a parsed re-rendering, make a tamper/change check
        # exact and make every semantically relevant revision invalidate prior
        # decisions at the launch gate.
        digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
        # A second canonical check detects a path replacement before returning
        # a snapshot.  Any later change is caught by launch re-evaluation.
        _assert_owner_private_file(policy_path)
        return _PolicySnapshot(
            policy_id,
            policy_version,
            digest,
            evidence_key,
            rules,
            workspaces,
            frozenset(static_workspace_refs),
        )

    def _workspace_identity(self, snapshot: _PolicySnapshot, workspace: Path) -> tuple[str, Path]:
        try:
            candidate = canonical_directory_path(Path(os.path.abspath(os.fspath(workspace))))
            info = candidate.stat()
        except (OSError, RuntimeError, TypeError, ValueError) as error:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable") from error
        if not stat.S_ISDIR(info.st_mode):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        # Bind both object generation and canonical path. Device/inode alone
        # can be reused after deletion; a path alone can be replaced in place.
        identity = _workspace_identity_digest(snapshot, candidate, info)
        return identity, candidate

    @staticmethod
    def _runner_allows(
        snapshot: _PolicySnapshot,
        runner: Literal["claude", "codex"],
        workspace: Path,
    ) -> bool:
        try:
            allow_roots, deny_roots = snapshot.runner_rules[runner]
            return (
                not allow_roots
                or any(_contains(root, workspace) for root in allow_roots)
            ) and not any(_contains(root, workspace) for root in deny_roots)
        except (KeyError, OSError, RuntimeError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error

    @staticmethod
    def _registered_workspace_target(
        workspace: Path,
    ) -> tuple[Path, os.stat_result]:
        try:
            supplied = Path(os.fspath(workspace))
            if not supplied.is_absolute():
                raise ValueError
            raw = Path(os.path.abspath(os.fspath(supplied)))
            raw_info = raw.lstat()
            canonical = canonical_directory_path(raw)
            canonical_info = canonical.lstat()
        except (OSError, RuntimeError, TypeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if (
            not stat.S_ISDIR(canonical_info.st_mode)
            or (
                not stat.S_ISLNK(raw_info.st_mode)
                and _stat_identity(raw_info) != _stat_identity(canonical_info)
            )
        ):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        return canonical, canonical_info

    def _resolve_registered_workspace_entry(self, entry: Mapping[str, object]) -> Path:
        raw_path = entry.get("path")
        device = entry.get("device")
        inode = entry.get("inode")
        object_generation = entry.get("object_generation")
        if (
            not isinstance(raw_path, str)
            or not raw_path
            or "\x00" in raw_path
            or not isinstance(device, int)
            or isinstance(device, bool)
            or device < 0
            or not isinstance(inode, int)
            or isinstance(inode, bool)
            or inode < 0
            or not isinstance(object_generation, str)
        ):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        try:
            path = Path(raw_path)
            if not path.is_absolute() or Path(os.path.abspath(raw_path)) != path:
                raise ValueError
            info = path.lstat()
            canonical = path.resolve(strict=True)
            canonical_info = canonical.lstat()
        except (OSError, RuntimeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if (
            canonical != path
            or stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(canonical_info.st_mode)
            or _stat_identity(info) != _stat_identity(canonical_info)
            or int(canonical_info.st_ino) != inode
            or _stat_object_generation(canonical_info) != object_generation
        ):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        persistent = entry.get("persistent_identity")
        if persistent is not None:
            if self._persistent_identity(canonical) != persistent:
                raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        elif int(canonical_info.st_dev) != device and object_generation == "stable-generation-unavailable":
            # Old registries lack volume UUIDs. Across a boot, migration needs
            # the unchanged canonical path, inode and a real birth/generation.
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        return canonical

    @staticmethod
    def _persistent_identity(path: Path) -> str:
        try:
            return directory_identity(path)
        except (OSError, RuntimeError, ValueError) as error:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable") from error

    def _upgrade_workspace_identity_locked(
        self, registry: dict[str, Any], workspace_ref: str, entry: Mapping[str, object]
    ) -> Mapping[str, object]:
        if "persistent_identity" in entry:
            return entry
        path = self._resolve_registered_workspace_entry(entry)
        upgraded = dict(entry)
        upgraded["persistent_identity"] = self._persistent_identity(path)
        payload = {key: value for key, value in upgraded.items() if key != "seal"}
        upgraded["seal"] = self._workspace_registry_seal(
            self._workspace_registry_key(registry), workspace_ref, payload
        )
        registry["workspaces"][workspace_ref] = upgraded
        registry["revision"] += 1
        self._write_workspace_registry_locked(registry)
        return upgraded

    @staticmethod
    def _workspace_entry_matches_stat(
        entry: Mapping[str, object], info: os.stat_result
    ) -> bool:
        return (
            (entry.get("persistent_identity") is not None or entry.get("device") == int(info.st_dev))
            and entry.get("inode") == int(info.st_ino)
            and entry.get("object_generation") == _stat_object_generation(info)
            and stat.S_ISDIR(info.st_mode)
        )

    def _require_workspace_separate_from_registry(self, workspace: Path) -> None:
        """Reject either direction of containment with Control Plane state."""

        registry_path, _lock_path = self._workspace_registry_paths()
        protected = registry_path.parent
        try:
            overlaps = _contains(protected, workspace) or _contains(
                workspace, protected
            )
        except (OSError, RuntimeError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if overlaps:
            raise PrivatePolicyError("owner_private_policy_launch_denied")

    @contextmanager
    def _locked_workspace_registry(self) -> Iterator[None]:
        registry_path, lock_path = self._workspace_registry_paths()
        descriptor: int | None = None
        with _WORKSPACE_REGISTRY_THREAD_LOCK:
            try:
                flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(lock_path, flags, 0o600)
                self._assert_workspace_registry_stat(os.fstat(descriptor), is_file=True)
                self._assert_workspace_registry_path(lock_path)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                # Keep the canonical path fixed for this critical section.
                self._workspace_registry_file = registry_path
                yield
            except PrivatePolicyError:
                raise
            except OSError as error:
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                ) from error
            finally:
                if descriptor is not None:
                    with suppress(OSError):
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    os.close(descriptor)

    def _workspace_registry_paths(self) -> tuple[Path, Path]:
        if self._workspace_registry_file is None:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        try:
            configured = Path(
                os.path.abspath(os.fspath(self._workspace_registry_file))
            )
            parent_info = configured.parent.lstat()
            parent = configured.parent.resolve(strict=True)
            canonical_parent_info = parent.lstat()
        except (OSError, RuntimeError, TypeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if (
            stat.S_ISLNK(parent_info.st_mode)
            or _stat_identity(parent_info) != _stat_identity(canonical_parent_info)
        ):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        self._assert_workspace_registry_stat(canonical_parent_info, is_file=False)
        registry_path = parent / configured.name
        lock_path = parent / f".{configured.name}.lock"
        return registry_path, lock_path

    def _read_or_create_workspace_registry_locked(self) -> dict[str, Any]:
        if self._workspace_registry_file is None:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        if not os.path.lexists(self._workspace_registry_file):
            self._write_workspace_registry_locked(
                {
                    "format": _WORKSPACE_REGISTRY_FORMAT,
                    "key": base64.urlsafe_b64encode(secrets.token_bytes(32))
                    .decode("ascii")
                    .rstrip("="),
                    "revision": 0,
                    "workspaces": {},
                }
            )
        registry = self._read_workspace_registry_locked()
        self._validate_workspace_registry(registry)
        return registry

    def _registered_workspace_entry_if_present(
        self, workspace_ref: str
    ) -> Mapping[str, object] | None:
        if self._workspace_registry_file is None:
            return None
        with self._locked_workspace_registry():
            if not os.path.lexists(self._workspace_registry_file):
                return None
            registry = self._read_workspace_registry_locked()
            self._validate_workspace_registry(registry)
            workspaces = registry["workspaces"]
            assert isinstance(workspaces, dict)
            entry = workspaces.get(workspace_ref)
            if not isinstance(entry, Mapping):
                return None
            # A removed Directory does not make registry membership invalid.
            # Its resolution still fails; unrelated records remain usable.
            with suppress(PrivatePolicyError):
                entry = self._upgrade_workspace_identity_locked(registry, workspace_ref, entry)
            return dict(entry)

    def _read_workspace_registry_locked(self) -> dict[str, Any]:
        if self._workspace_registry_file is None:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        path = self._workspace_registry_file
        self._assert_workspace_registry_path(path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor: int | None = None
        try:
            descriptor = os.open(path, flags)
            self._assert_workspace_registry_stat(os.fstat(descriptor), is_file=True)
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = None
                data = handle.read(_WORKSPACE_REGISTRY_MAX_BYTES + 1)
            if len(data) > _WORKSPACE_REGISTRY_MAX_BYTES:
                raise ValueError
            value = json.loads(data.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if not isinstance(value, dict):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        return value

    def _write_workspace_registry_locked(self, registry: Mapping[str, object]) -> None:
        if self._workspace_registry_file is None:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        try:
            data = _canonical_json_bytes(registry)
        except (TypeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if len(data) > _WORKSPACE_REGISTRY_MAX_BYTES:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        path = self._workspace_registry_file
        parent = path.parent
        if os.path.lexists(path):
            self._assert_workspace_registry_path(path)
        temporary = parent / f".{path.name}.{secrets.token_hex(12)}.tmp"
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(temporary, flags, 0o600)
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                descriptor = None
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            directory = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self._assert_workspace_registry_path(path)
        except (OSError, TypeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                temporary.unlink()

    def _validate_workspace_registry(self, registry: Mapping[str, object]) -> None:
        revision = registry.get("revision")
        if (
            set(registry) != _WORKSPACE_REGISTRY_FIELDS
            or registry.get("format") != _WORKSPACE_REGISTRY_FORMAT
            or not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision < 0
            or not isinstance(registry.get("workspaces"), dict)
        ):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        key = self._workspace_registry_key(registry)
        workspaces = registry["workspaces"]
        assert isinstance(workspaces, dict)
        for workspace_ref, raw_entry in workspaces.items():
            if (
                not self.is_registered_workspace_ref(workspace_ref)
                or not isinstance(raw_entry, dict)
                or set(raw_entry) not in (_WORKSPACE_REGISTRY_ENTRY_FIELDS, _WORKSPACE_REGISTRY_ENTRY_FIELDS | {"persistent_identity"})
            ):
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                )
            runner = raw_entry.get("runner")
            device = raw_entry.get("device")
            inode = raw_entry.get("inode")
            object_generation = raw_entry.get("object_generation")
            path = raw_entry.get("path")
            seal = raw_entry.get("seal")
            if (
                not isinstance(runner, str)
                or runner not in _RUNNERS
                or not isinstance(device, int)
                or isinstance(device, bool)
                or device < 0
                or not isinstance(inode, int)
                or isinstance(inode, bool)
                or inode < 0
                or not isinstance(object_generation, str)
                or not _valid_object_generation(object_generation)
                or not isinstance(path, str)
                or not path
                or "\x00" in path
                or not Path(path).is_absolute()
                or not isinstance(seal, str)
            ):
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                )
            persistent_identity = raw_entry.get("persistent_identity")
            if persistent_identity is not None and (
                not isinstance(persistent_identity, str)
                or re.fullmatch(r"[0-9a-f]{64}", persistent_identity) is None
            ):
                raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
            expected_ref = self._registered_workspace_ref(
                key,
                runner=runner,
                device=device,
                inode=inode,
                path=path,
                object_generation=object_generation,
            )
            entry_without_seal = {
                name: raw_entry[name]
                for name in (
                    "path",
                    "device",
                    "inode",
                    "object_generation",
                    "runner",
                )
            }
            if persistent_identity is not None:
                entry_without_seal["persistent_identity"] = persistent_identity
            stable_ref = self._registered_workspace_ref(
                key, runner=runner, device=device, inode=inode, path=path,
                object_generation=object_generation, persistent_identity=persistent_identity,
            )
            expected_seal = self._workspace_registry_seal(
                key, workspace_ref, entry_without_seal
            )
            if workspace_ref not in {expected_ref, stable_ref} or not hmac.compare_digest(
                seal, expected_seal
            ):
                raise PrivatePolicyError(
                    "owner_private_policy_workspace_unavailable"
                )

    @staticmethod
    def _workspace_registry_key(registry: Mapping[str, object]) -> bytes:
        raw = registry.get("key")
        if not isinstance(raw, str):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        try:
            key = base64.urlsafe_b64decode(
                raw.encode("ascii") + b"=" * (-len(raw) % 4)
            )
        except (UnicodeEncodeError, ValueError) as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if len(key) != 32:
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        return key

    @staticmethod
    def _registered_workspace_ref(
        key: bytes,
        *,
        runner: str,
        device: int,
        inode: int,
        path: str,
        object_generation: str,
        persistent_identity: str | None = None,
    ) -> str:
        # The canonical path is part of the opaque proof so filesystem inode
        # reuse can never silently redirect an older Worker to an unrelated
        # Directory. A rename gets a new ref; the old one fails closed.
        proof = (
            f"workspace-ref-v1\0{runner}\0{device}\0{inode}\0"
            f"{object_generation}\0{path}"
        ).encode()
        if persistent_identity is not None:
            proof = f"workspace-ref-v2\0{runner}\0{object_generation}\0{persistent_identity}".encode()
        return "cao-dynamic-" + hmac.new(key, proof, hashlib.sha256).hexdigest()

    @staticmethod
    def _workspace_registry_seal(
        key: bytes,
        workspace_ref: str,
        entry: Mapping[str, object],
    ) -> str:
        proof = _canonical_json_bytes(
            {
                "format": _WORKSPACE_REGISTRY_FORMAT,
                "workspace_ref": workspace_ref,
                "entry": entry,
            }
        )
        return "hmac-sha256:" + hmac.new(key, proof, hashlib.sha256).hexdigest()

    @staticmethod
    def _assert_workspace_registry_path(path: Path) -> None:
        try:
            info = path.lstat()
            canonical = path.resolve(strict=True)
        except OSError as error:
            raise PrivatePolicyError(
                "owner_private_policy_workspace_unavailable"
            ) from error
        if canonical != path or stat.S_ISLNK(info.st_mode):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")
        OwnerPrivatePolicyEdge._assert_workspace_registry_stat(info, is_file=True)

    @staticmethod
    def _assert_workspace_registry_stat(
        info: os.stat_result,
        *,
        is_file: bool,
    ) -> None:
        expected_type = stat.S_ISREG if is_file else stat.S_ISDIR
        expected_mode = 0o600 if is_file else 0o700
        if (
            not expected_type(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != expected_mode
            or (is_file and info.st_nlink != 1)
        ):
            raise PrivatePolicyError("owner_private_policy_workspace_unavailable")

    def _make_decision(
        self,
        snapshot: _PolicySnapshot,
        binding: PlacementBinding,
        workspace_identity: str,
        outcome: Literal["allow", "deny"],
        *,
        expires_at: int | None = None,
    ) -> PlacementDecision:
        if expires_at is None:
            expires_at = int(self._clock()) + self._ttl
        proof = "\0".join(
            (
                "owner-private-placement-v1",
                snapshot.policy_id,
                snapshot.policy_version,
                snapshot.digest,
                outcome,
                binding.principal_id,
                binding.runtime_id,
                binding.assignment_id,
                binding.work_item_id,
                binding.runner_adapter,
                str(binding.launch_generation),
                workspace_identity,
                str(expires_at),
            )
        )
        return PlacementDecision(
            policy_id=snapshot.policy_id,
            policy_version=snapshot.policy_version,
            policy_digest=snapshot.digest,
            decision=outcome,
            runner_adapter=binding.runner_adapter,
            workspace_identity_digest=workspace_identity,
            evidence_id=_hmac_id(snapshot.evidence_key, proof),
            expires_at=expires_at,
        )


def ensure_default_dynamic_workspace_policy(
    policy_file: Path,
    *,
    denied_root: Path,
) -> Path:
    """Create the safe owner-local default used only for dynamic Directories.

    The current CAO conversation is already a local owner capability.  The
    default therefore permits an existing Directory anywhere the owner can
    access except one that contains, equals, or descends from the Control Plane
    state root. Deployments that set an explicit policy retain that stricter
    policy unchanged.
    """

    try:
        path = Path(os.path.abspath(os.fspath(policy_file)))
        parent = _assert_owner_private_parent(path.parent)
        denied = Path(os.path.abspath(os.fspath(denied_root))).resolve(strict=True)
        denied_info = denied.stat()
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    if not stat.S_ISDIR(denied_info.st_mode):
        raise PrivatePolicyError("owner_private_policy_unavailable")
    path = parent / path.name
    lock_path = parent / f".{path.name}.lock"
    lock_descriptor: int | None = None
    temporary: Path | None = None
    try:
        lock_descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        OwnerPrivatePolicyEdge._assert_workspace_registry_stat(
            os.fstat(lock_descriptor), is_file=True
        )
        OwnerPrivatePolicyEdge._assert_workspace_registry_path(lock_path)
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        if os.path.lexists(path):
            return _assert_owner_private_file(path)
        document = {
            "policy_id": "dynamic-directory-default",
            "policy_version": "1",
            "evidence_key": base64.urlsafe_b64encode(secrets.token_bytes(32))
            .decode("ascii")
            .rstrip("="),
            "runners": {
                runner: {
                    "allow_within": [os.fspath(Path("/").resolve(strict=True))],
                    "deny_within": [os.fspath(denied)],
                }
                for runner in sorted(_RUNNERS)
            },
        }
        temporary = parent / f".{path.name}.{secrets.token_hex(12)}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.fchmod(descriptor, 0o600)
            data = _canonical_json_bytes(document)
            written = 0
            while written < len(data):
                written += os.write(descriptor, data[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        temporary = None
        directory = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return _assert_owner_private_file(path)
    except PrivatePolicyError:
        raise
    except OSError as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    finally:
        if lock_descriptor is not None:
            with suppress(OSError):
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
            os.close(lock_descriptor)
        if temporary is not None:
            with suppress(FileNotFoundError):
                temporary.unlink()


def _stat_identity(info: os.stat_result) -> tuple[int, int, int]:
    return (int(info.st_dev), int(info.st_ino), stat.S_IFMT(info.st_mode))


def _valid_object_generation(value: str) -> bool:
    if value.startswith("birthtime:"):
        try:
            parsed = float.fromhex(value.removeprefix("birthtime:"))
        except ValueError:
            return False
        return parsed >= 0
    if value.startswith("stat-generation:"):
        raw = value.removeprefix("stat-generation:")
        return raw.isdigit()
    return value == "stable-generation-unavailable"


def _workspace_identity_digest(
    snapshot: _PolicySnapshot,
    workspace: Path,
    info: os.stat_result,
) -> str:
    try:
        identity = directory_identity(workspace, expected=info)
    except (OSError, RuntimeError, ValueError) as error:
        raise PrivatePolicyError("owner_private_policy_workspace_unavailable") from error
    return _hmac_id(
        snapshot.evidence_key,
        "\0".join(
            (
                "workspace-v2",
                identity,
            )
        ),
    )


def _safe_identifier(value: object) -> bool:
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


def _decode_evidence_key(value: object) -> bytes:
    if not isinstance(value, str) or not value:
        raise PrivatePolicyError("owner_private_policy_unavailable")
    try:
        key = base64.urlsafe_b64decode(value.encode("ascii") + b"=" * (-len(value) % 4))
    except (UnicodeEncodeError, ValueError) as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    if len(key) < 32:
        raise PrivatePolicyError("owner_private_policy_unavailable")
    return key


def _load_roots(value: object) -> tuple[Path, ...]:
    if not isinstance(value, list):
        raise PrivatePolicyError("owner_private_policy_unavailable")
    roots: list[Path] = []
    seen: set[tuple[int, int]] = set()
    for raw in value:
        if not isinstance(raw, str) or not raw:
            raise PrivatePolicyError("owner_private_policy_unavailable")
        try:
            root = Path(os.path.abspath(raw)).resolve(strict=True)
            info = root.stat()
        except (OSError, RuntimeError, ValueError) as error:
            raise PrivatePolicyError("owner_private_policy_unavailable") from error
        if not stat.S_ISDIR(info.st_mode):
            raise PrivatePolicyError("owner_private_policy_unavailable")
        identity = (info.st_dev, info.st_ino)
        if identity not in seen:
            seen.add(identity)
            roots.append(root)
    return tuple(roots)


def _load_optional_static_workspace(value: object) -> Path | None:
    """Resolve a legacy static target without making its removal global failure.

    A missing static target remains reserved by its policy reference, but it
    is unavailable for launch.  Other malformed, inaccessible, or non-
    directory targets still fail closed.
    """

    if not isinstance(value, str) or not value:
        raise PrivatePolicyError("owner_private_policy_unavailable")
    try:
        root = Path(os.path.abspath(value)).resolve(strict=True)
        info = root.stat()
    except FileNotFoundError:
        return None
    except (OSError, RuntimeError, ValueError) as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    if not stat.S_ISDIR(info.st_mode):
        raise PrivatePolicyError("owner_private_policy_unavailable")
    return root


def _contains(root: Path, candidate: Path) -> bool:
    """Containment by inode ancestry, not textual path prefix.

    ``resolve`` removes symlink and ``..`` traversal.  Walking directory
    identities additionally makes aliases on case-insensitive macOS volumes
    equivalent and prevents a visually similar sibling from matching.
    """

    root_info = root.stat()
    root_identity = (root_info.st_dev, root_info.st_ino)
    current = candidate
    while True:
        current_info = current.stat()
        if (current_info.st_dev, current_info.st_ino) == root_identity:
            return True
        parent = current.parent
        if parent == current:
            return False
        current = parent


def _hmac_id(key: bytes, value: str) -> str:
    return "hmac-sha256:" + hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def _read_owner_private_file(configured: Path) -> tuple[Path, bytes]:
    candidate = Path(os.path.abspath(os.fspath(configured)))
    _assert_owner_private_parent(candidate.parent)
    policy_path = _assert_owner_private_file(candidate)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(policy_path, flags)
    except OSError as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    try:
        info = os.fstat(descriptor)
        _assert_owner_private_stat(info, is_file=True)
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            data = handle.read()
    except OSError as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    finally:
        os.close(descriptor)
    return policy_path, data


def _assert_owner_private_parent(parent: Path) -> Path:
    try:
        info = parent.lstat()
        canonical = parent.resolve(strict=True)
    except OSError as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    if canonical != parent or stat.S_ISLNK(info.st_mode):
        raise PrivatePolicyError("owner_private_policy_unavailable")
    _assert_owner_private_stat(info, is_file=False)
    return canonical


def _assert_owner_private_file(path: Path) -> Path:
    try:
        info = path.lstat()
        canonical = path.resolve(strict=True)
    except OSError as error:
        raise PrivatePolicyError("owner_private_policy_unavailable") from error
    if canonical != path or stat.S_ISLNK(info.st_mode):
        raise PrivatePolicyError("owner_private_policy_unavailable")
    _assert_owner_private_stat(info, is_file=True)
    return canonical


def _assert_owner_private_stat(info: os.stat_result, *, is_file: bool) -> None:
    expected_mode = 0o600 if is_file else 0o700
    expected_type = stat.S_ISREG if is_file else stat.S_ISDIR
    if (
        not expected_type(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) != expected_mode
        or (is_file and info.st_nlink != 1)
    ):
        raise PrivatePolicyError("owner_private_policy_unavailable")
