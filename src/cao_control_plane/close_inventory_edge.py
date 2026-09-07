"""Owner-private resource registry and executor for explicit close.

The control plane may persist only the sanitized records returned by
``prepare``.  Raw filesystem and Git locators remain in owner-only registry
and freeze-ledger files.  Those files protect against accidental corruption,
stale replay, and database/private-ledger substitution; they are not an OS
security boundary against malicious code already running as the same UID,
which can read both the data and its HMAC key.
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
import shutil
import stat
import subprocess
import threading
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

from .canonical import canonical_json_bytes as _canonical
from .close_contract import CleanupTargetKind


class CloseInventoryProviderError(RuntimeError):
    """A path-free failure from the owner-private cleanup edge."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


_KINDS = (
    CleanupTargetKind.WORKSPACE,
    CleanupTargetKind.TEMPORARY,
    CleanupTargetKind.LOG,
    CleanupTargetKind.BRANCH,
)
_KIND_VALUES = frozenset(kind.value for kind in _KINDS)
_EXECUTION_ORDER = {
    CleanupTargetKind.TEMPORARY.value: 0,
    CleanupTargetKind.LOG.value: 1,
    CleanupTargetKind.BRANCH.value: 2,
    CleanupTargetKind.WORKSPACE.value: 3,
}
_IDENTIFIER = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,255}$")
_BRANCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
_REGISTRY_FIELDS = frozenset({"version", "key", "revision", "works"})
_LEDGER_FIELDS = frozenset({"version", "preparations"})
_CATEGORY_NA_FIELDS = frozenset({"disposition"})
_CATEGORY_ENUM_FIELDS = frozenset({"disposition", "resources"})


def _hmac_hex(key: bytes, value: object) -> str:
    return hmac.new(key, _canonical(value), hashlib.sha256).hexdigest()


def _hmac_id(key: bytes, value: object) -> str:
    return "hmac-sha256:" + _hmac_hex(key, value)


class OwnerPrivateCloseInventory:
    """Explicit v2 registry, immutable freeze ledger, and local executor.

    ``allowed_cleanup_roots`` is required only when registering temporary or
    log resources.  The selected root and its inode identity are frozen into
    the private resource, so execution remains safe after a process restart.
    """

    def __init__(
        self,
        state_dir: Path,
        *,
        allowed_cleanup_roots: Iterable[Path] = (),
        protected_roots: Iterable[Path] = (),
    ) -> None:
        # Resolve stable aliases in ancestor components (macOS exposes
        # ``/var`` through ``/private/var``) once at the trust boundary.  The
        # owned leaf itself must still never be a symlink.  Persisting and
        # reusing the canonical path prevents a later alias retarget from
        # changing the cleanup target.
        self._state_dir = self._canonical_future_owned_directory(
            Path(state_dir), "cleanup_private_store_unavailable"
        )
        self._registry_path = self._state_dir / "close-cleanup-owner-private-v2.json"
        self._ledger_path = self._state_dir / "close-cleanup-freeze-ledger-v2.json"
        self._lock_path = self._state_dir / "close-cleanup-v2.lock"
        self._thread_lock = threading.RLock()
        self._allowed_roots = tuple(
            self._canonical_existing_directory(path, "cleanup_allowed_root_invalid")
            for path in allowed_cleanup_roots
        )
        self._protected_roots = tuple(
            self._canonical_existing_or_future(path)
            for path in (Path("/"), Path.home(), self._state_dir)
        )
        self._explicit_protected_roots = tuple(
            self._canonical_existing_or_future(path) for path in protected_roots
        )

    # ------------------------------------------------------------------
    # Explicit owner-private registry API
    # ------------------------------------------------------------------
    def declare_not_applicable(
        self, *, work_item_id: str, target_kind: CleanupTargetKind | str
    ) -> None:
        """Explicitly declare one category N/A; absence is never equivalent."""

        work = self._identifier(work_item_id, "cleanup_registry_invalid_work")
        kind = self._kind(target_kind)
        with self._locked():
            registry = self._read_or_create_registry_locked()
            category = self._work_categories(registry, work).get(kind.value)
            if category == {"disposition": "not-applicable"}:
                return
            if category is not None:
                raise CloseInventoryProviderError("cleanup_registry_category_conflict")
            self._work_categories(registry, work)[kind.value] = {
                "disposition": "not-applicable"
            }
            self._commit_registry_locked(registry)

    def register_resource(
        self,
        *,
        work_item_id: str,
        target_kind: CleanupTargetKind | str,
        action: str = "delete",
        locator: Path | None = None,
        repository: Path | None = None,
        branch: str = "",
    ) -> dict[str, str]:
        """Register one exact owned tmp/log/branch resource.

        Workspace registration is deliberately unavailable here: a workspace
        can be adopted only after proving it is a real linked Git worktree.
        """

        work = self._identifier(work_item_id, "cleanup_registry_invalid_work")
        kind = self._kind(target_kind)
        self._require_action(kind, action)
        if kind is CleanupTargetKind.WORKSPACE:
            raise CloseInventoryProviderError("cleanup_workspace_requires_adoption")
        if kind in {CleanupTargetKind.TEMPORARY, CleanupTargetKind.LOG}:
            if locator is None or repository is not None or branch:
                raise CloseInventoryProviderError("cleanup_registry_invalid_resource")
            resource = self._inspect_local_resource(kind, locator, action)
        else:
            if repository is None or locator is not None or not branch:
                raise CloseInventoryProviderError("cleanup_registry_invalid_resource")
            resource = self._inspect_branch_resource(repository, branch, action)
        with self._locked():
            registry = self._read_or_create_registry_locked()
            stored = self._register_resource_locked(registry, work, kind, resource)
            if stored:
                self._commit_registry_locked(registry)
        return {
            "target_kind": kind.value,
            "action": action,
            "resource_identity": str(resource["resource_identity"]),
        }

    def adopt_managed_worktree(
        self, *, work_item_id: str, workspace: Path
    ) -> dict[str, str]:
        """Adopt a linked worktree and explicitly mark non-owned kinds N/A.

        A repository's main worktree is never adoptable.  The workspace record
        owns both deterministic effects: exact worktree removal first, then
        deletion of its exact local branch.  Consequently the standalone
        branch category is explicitly N/A for this managed-work adoption.
        """

        work = self._identifier(work_item_id, "cleanup_registry_invalid_work")
        resource = self._inspect_linked_worktree(workspace)
        with self._locked():
            registry = self._read_or_create_registry_locked()
            changed = self._register_resource_locked(
                registry,
                work,
                CleanupTargetKind.WORKSPACE,
                resource,
                allow_identical=True,
            )
            categories = self._work_categories(registry, work)
            for kind in (
                CleanupTargetKind.TEMPORARY,
                CleanupTargetKind.LOG,
                CleanupTargetKind.BRANCH,
            ):
                existing = categories.get(kind.value)
                if existing is None:
                    categories[kind.value] = {"disposition": "not-applicable"}
                    changed = True
                elif (
                    kind in {CleanupTargetKind.TEMPORARY, CleanupTargetKind.LOG}
                    and isinstance(existing, dict)
                    and existing.get("disposition") == "enumerated"
                ):
                    continue
                elif existing != {"disposition": "not-applicable"}:
                    raise CloseInventoryProviderError(
                        "cleanup_registry_category_conflict"
                    )
            if changed:
                self._commit_registry_locked(registry)
        return {
            "target_kind": CleanupTargetKind.WORKSPACE.value,
            "action": "delete",
            "resource_identity": str(resource["resource_identity"]),
        }

    def adopt_managed_work(self, work_item_id: str, workspace: Path) -> None:
        """Atomically establish all-category coverage for runtime launch.

        A verified linked worktree is adopted together with its owned branch;
        existing temporary/log registrations are preserved, missing ones and
        standalone branch become explicit N/A.  A canonical main worktree is
        persistent and all four categories become explicit N/A.
        Missing/non-Git/ambiguous locations still fail closed.
        """

        work = self._identifier(work_item_id, "cleanup_registry_invalid_work")
        try:
            resource = self._inspect_linked_worktree(workspace)
        except CloseInventoryProviderError as error:
            if error.code != "cleanup_workspace_is_main_worktree":
                raise
            with self._locked():
                registry = self._read_or_create_registry_locked()
                categories = self._work_categories(registry, work)
                changed = False
                for kind in _KINDS:
                    existing = categories.get(kind.value)
                    if existing is None:
                        categories[kind.value] = {
                            "disposition": "not-applicable"
                        }
                        changed = True
                    elif existing != {"disposition": "not-applicable"}:
                        raise CloseInventoryProviderError(
                            "cleanup_registry_category_conflict"
                        ) from None
                if changed:
                    self._commit_registry_locked(registry)
            return
        with self._locked():
            registry = self._read_or_create_registry_locked()
            changed = self._register_resource_locked(
                registry,
                work,
                CleanupTargetKind.WORKSPACE,
                resource,
                allow_identical=True,
            )
            categories = self._work_categories(registry, work)
            for kind in (
                CleanupTargetKind.TEMPORARY,
                CleanupTargetKind.LOG,
                CleanupTargetKind.BRANCH,
            ):
                existing = categories.get(kind.value)
                if existing is None:
                    categories[kind.value] = {"disposition": "not-applicable"}
                    changed = True
                elif (
                    kind in {CleanupTargetKind.TEMPORARY, CleanupTargetKind.LOG}
                    and isinstance(existing, dict)
                    and existing.get("disposition") == "enumerated"
                ):
                    continue
                elif existing != {"disposition": "not-applicable"}:
                    raise CloseInventoryProviderError(
                        "cleanup_registry_category_conflict"
                    )
            if changed:
                self._commit_registry_locked(registry)

    # ------------------------------------------------------------------
    # Freeze, verification, and execution API
    # ------------------------------------------------------------------
    def prepare(
        self,
        *,
        preparation_id: str,
        work_item_id: str,
        generation: int,
        attachment_id: str,
    ) -> list[dict[str, str]]:
        """Freeze an explicit, complete registry snapshot idempotently."""

        binding = self._binding(
            preparation_id, work_item_id, generation, attachment_id
        )
        with self._locked():
            registry = self._read_or_create_registry_locked()
            key = self._key(registry)
            ledger = self._read_or_create_ledger_locked()
            existing = ledger["preparations"].get(preparation_id)
            if existing is not None:
                existing_frozen = self._validate_frozen_locked(existing, key, binding)
                return [dict(record) for record in existing_frozen["public_records"]]
            private_records = self._complete_private_records(registry, work_item_id)
            self._validate_physical_uniqueness(private_records)
            public_records, set_digest = self._build_public_records(
                key, binding, private_records
            )
            frozen: dict[str, Any] = {
                "binding": binding,
                "registry_revision": int(registry["revision"]),
                "set_digest": set_digest,
                "public_records": public_records,
                "private_records": private_records,
            }
            frozen["seal"] = _hmac_id(
                key, {"domain": "cao-close-freeze-v2", "frozen": frozen}
            )
            ledger["preparations"][preparation_id] = frozen
            self._atomic_write_locked(self._ledger_path, ledger)
            return [dict(record) for record in public_records]

    def verify_public_inventory(
        self,
        *,
        preparation_id: str,
        work_item_id: str,
        generation: int,
        attachment_id: str,
        public_inventory: Sequence[Mapping[str, object]],
    ) -> bool:
        """Verify the exact frozen public set, including coverage completeness."""

        binding = self._binding(
            preparation_id, work_item_id, generation, attachment_id
        )
        try:
            with self._locked():
                registry = self._read_or_create_registry_locked()
                ledger = self._read_or_create_ledger_locked(create=False)
                raw = ledger["preparations"].get(preparation_id)
                if raw is None:
                    return False
                frozen = self._validate_frozen_locked(raw, self._key(registry), binding)
                return hmac.compare_digest(
                    _canonical(list(public_inventory)),
                    _canonical(frozen["public_records"]),
                )
        except CloseInventoryProviderError:
            return False

    def verify_public_record(
        self,
        *,
        preparation_id: str,
        work_item_id: str,
        generation: int,
        attachment_id: str,
        record: Mapping[str, object],
    ) -> bool:
        """Compatibility verifier backed by the entire sealed set."""

        binding = self._binding(
            preparation_id, work_item_id, generation, attachment_id
        )
        try:
            with self._locked():
                registry = self._read_or_create_registry_locked()
                ledger = self._read_or_create_ledger_locked(create=False)
                raw = ledger["preparations"].get(preparation_id)
                if raw is None:
                    return False
                frozen = self._validate_frozen_locked(raw, self._key(registry), binding)
                encoded = _canonical(dict(record))
                return any(
                    hmac.compare_digest(encoded, _canonical(candidate))
                    for candidate in frozen["public_records"]
                )
        except CloseInventoryProviderError:
            return False

    def execute(
        self,
        *,
        preparation_id: str,
        work_item_id: str,
        generation: int,
        attachment_id: str,
        public_inventory: Sequence[Mapping[str, object]],
    ) -> list[dict[str, str]]:
        """Validate the entire frozen set before applying any local effect."""

        binding = self._binding(
            preparation_id, work_item_id, generation, attachment_id
        )
        with self._locked():
            registry = self._read_or_create_registry_locked()
            key = self._key(registry)
            ledger = self._read_or_create_ledger_locked(create=False)
            raw = ledger["preparations"].get(preparation_id)
            if raw is None:
                raise CloseInventoryProviderError("cleanup_freeze_missing")
            frozen = self._validate_frozen_locked(raw, key, binding)
            if not hmac.compare_digest(
                _canonical(list(public_inventory)),
                _canonical(frozen["public_records"]),
            ):
                raise CloseInventoryProviderError("cleanup_public_set_mismatch")
            enumerated = [
                dict(record)
                for record in frozen["private_records"]
                if record["coverage"] == "enumerated"
            ]
            # All targets are checked before the first deletion.  This is the
            # safe stopping boundary for stale inode, symlink, locator, branch,
            # or private-ledger substitution.
            for record in enumerated:
                self._revalidate_private_record(record)
            self._validate_physical_uniqueness(enumerated)
            results: list[dict[str, str]] = []
            for record in sorted(
                enumerated,
                key=lambda item: (
                    _EXECUTION_ORDER[str(item["target_kind"])],
                    str(item["resource_identity"]),
                ),
            ):
                self._execute_one(record)
                results.append(
                    self._execution_receipt(key, binding, frozen, record)
                )
            return results

    # ------------------------------------------------------------------
    # Registry construction and validation
    # ------------------------------------------------------------------
    def _work_categories(
        self, registry: dict[str, Any], work_item_id: str
    ) -> dict[str, Any]:
        works = registry["works"]
        value = works.setdefault(work_item_id, {})
        if not isinstance(value, dict):
            raise CloseInventoryProviderError("cleanup_registry_malformed")
        return value

    def _register_resource_locked(
        self,
        registry: dict[str, Any],
        work_item_id: str,
        kind: CleanupTargetKind,
        resource: dict[str, Any],
        *,
        allow_identical: bool = False,
    ) -> bool:
        categories = self._work_categories(registry, work_item_id)
        category = categories.get(kind.value)
        if category is None:
            category = {"disposition": "enumerated", "resources": []}
            categories[kind.value] = category
        if (
            not isinstance(category, dict)
            or set(category) != _CATEGORY_ENUM_FIELDS
            or category.get("disposition") != "enumerated"
            or not isinstance(category.get("resources"), list)
        ):
            raise CloseInventoryProviderError("cleanup_registry_category_conflict")
        identity = str(resource["resource_identity"])
        physical_ids = set(self._physical_ids(resource))
        for other_work, raw_categories in registry["works"].items():
            if not isinstance(raw_categories, dict):
                raise CloseInventoryProviderError("cleanup_registry_malformed")
            for raw_category in raw_categories.values():
                if not isinstance(raw_category, dict):
                    raise CloseInventoryProviderError("cleanup_registry_malformed")
                for other in raw_category.get("resources", []):
                    if not isinstance(other, dict):
                        raise CloseInventoryProviderError("cleanup_registry_malformed")
                    overlap = physical_ids.intersection(self._physical_ids(other))
                    if not overlap:
                        continue
                    if (
                        other_work == work_item_id
                        and str(other.get("resource_identity")) == identity
                        and hmac.compare_digest(_canonical(other), _canonical(resource))
                    ):
                        if allow_identical:
                            return False
                        raise CloseInventoryProviderError(
                            "cleanup_registry_duplicate_resource"
                        )
                    raise CloseInventoryProviderError(
                        "cleanup_registry_physical_target_conflict"
                    )
        category["resources"].append(resource)
        category["resources"].sort(key=lambda item: str(item["resource_identity"]))
        return True

    def _complete_private_records(
        self, registry: Mapping[str, Any], work_item_id: str
    ) -> list[dict[str, Any]]:
        works = registry.get("works")
        categories = works.get(work_item_id) if isinstance(works, dict) else None
        if not isinstance(categories, dict) or set(categories) != _KIND_VALUES:
            raise CloseInventoryProviderError("cleanup_registry_incomplete_coverage")
        records: list[dict[str, Any]] = []
        for kind in _KINDS:
            category = categories.get(kind.value)
            if not isinstance(category, dict):
                raise CloseInventoryProviderError("cleanup_registry_malformed")
            disposition = category.get("disposition")
            if disposition == "not-applicable":
                if set(category) != _CATEGORY_NA_FIELDS:
                    raise CloseInventoryProviderError("cleanup_registry_malformed")
                records.append(
                    {"target_kind": kind.value, "coverage": "not-applicable"}
                )
                continue
            if (
                disposition != "enumerated"
                or set(category) != _CATEGORY_ENUM_FIELDS
                or not isinstance(category.get("resources"), list)
                or not category["resources"]
            ):
                raise CloseInventoryProviderError("cleanup_registry_malformed")
            for raw in category["resources"]:
                if not isinstance(raw, dict) or raw.get("target_kind") != kind.value:
                    raise CloseInventoryProviderError("cleanup_registry_malformed")
                record = dict(raw)
                record["coverage"] = "enumerated"
                self._revalidate_private_record(record)
                records.append(record)
        return sorted(
            records,
            key=lambda item: (
                _EXECUTION_ORDER[str(item["target_kind"])],
                str(item.get("resource_identity", "")),
            ),
        )

    def _build_public_records(
        self,
        key: bytes,
        binding: Mapping[str, object],
        private_records: Sequence[Mapping[str, object]],
    ) -> tuple[list[dict[str, str]], str]:
        bases: list[dict[str, str]] = []
        for private in private_records:
            kind = str(private["target_kind"])
            if private["coverage"] == "not-applicable":
                bases.append({"target_kind": kind, "coverage": "not-applicable"})
                continue
            action = str(private["action"])
            fingerprint = _hmac_hex(
                key,
                {
                    "domain": "cao-close-resource-v2",
                    "binding": binding,
                    "target_kind": kind,
                    "action": action,
                    "private_resource": private,
                },
            )
            bases.append(
                {
                    "target_kind": kind,
                    "coverage": "enumerated",
                    "target_fingerprint": fingerprint,
                    "action": action,
                }
            )
        bases.sort(
            key=lambda item: (
                _EXECUTION_ORDER[item["target_kind"]],
                item.get("target_fingerprint", ""),
            )
        )
        set_digest = _hmac_id(
            key,
            {
                "domain": "cao-close-inventory-set-v2",
                "binding": binding,
                "records": bases,
            },
        )
        public: list[dict[str, str]] = []
        for base in bases:
            record = {**base, "inventory_set_digest": set_digest}
            if base["coverage"] == "enumerated":
                record["execution_digest"] = hashlib.sha256(
                    _canonical(
                        {
                            "domain": "cao-close-execution-v2",
                            "binding": binding,
                            "inventory_set_digest": set_digest,
                            **base,
                        }
                    )
                ).hexdigest()
            record["provider_evidence_id"] = _hmac_id(
                key,
                {
                    "domain": "cao-close-provider-evidence-v2",
                    "binding": binding,
                    "record": record,
                },
            )
            public.append(record)
        return public, set_digest

    def _validate_frozen_locked(
        self,
        raw: object,
        key: bytes,
        binding: Mapping[str, object],
    ) -> dict[str, Any]:
        if not isinstance(raw, dict) or set(raw) != {
            "binding",
            "registry_revision",
            "set_digest",
            "public_records",
            "private_records",
            "seal",
        }:
            raise CloseInventoryProviderError("cleanup_freeze_malformed")
        if not hmac.compare_digest(_canonical(raw["binding"]), _canonical(binding)):
            raise CloseInventoryProviderError("cleanup_freeze_binding_mismatch")
        unsigned = {key_name: raw[key_name] for key_name in raw if key_name != "seal"}
        expected_seal = _hmac_id(
            key, {"domain": "cao-close-freeze-v2", "frozen": unsigned}
        )
        if not isinstance(raw["seal"], str) or not hmac.compare_digest(
            raw["seal"], expected_seal
        ):
            raise CloseInventoryProviderError("cleanup_freeze_seal_invalid")
        private = raw["private_records"]
        public = raw["public_records"]
        if not isinstance(private, list) or not isinstance(public, list):
            raise CloseInventoryProviderError("cleanup_freeze_malformed")
        self._validate_private_record_shapes(private)
        rebuilt, set_digest = self._build_public_records(key, binding, private)
        if (
            not isinstance(raw["set_digest"], str)
            or not hmac.compare_digest(str(raw["set_digest"]), set_digest)
            or not hmac.compare_digest(_canonical(public), _canonical(rebuilt))
        ):
            raise CloseInventoryProviderError("cleanup_freeze_set_invalid")
        return raw

    def _validate_private_record_shapes(
        self, records: Sequence[object]
    ) -> None:
        coverage: set[str] = set()
        for raw in records:
            if not isinstance(raw, dict):
                raise CloseInventoryProviderError("cleanup_freeze_malformed")
            kind = str(raw.get("target_kind"))
            if kind not in _KIND_VALUES:
                raise CloseInventoryProviderError("cleanup_freeze_malformed")
            coverage.add(kind)
            if raw.get("coverage") == "not-applicable":
                if set(raw) != {"target_kind", "coverage"}:
                    raise CloseInventoryProviderError("cleanup_freeze_malformed")
            elif raw.get("coverage") == "enumerated":
                self._require_action(CleanupTargetKind(kind), str(raw.get("action")))
                if "resource_identity" not in raw:
                    raise CloseInventoryProviderError("cleanup_freeze_malformed")
            else:
                raise CloseInventoryProviderError("cleanup_freeze_malformed")
        if coverage != _KIND_VALUES:
            raise CloseInventoryProviderError("cleanup_registry_incomplete_coverage")

    # ------------------------------------------------------------------
    # Filesystem and Git inspection/execution
    # ------------------------------------------------------------------
    def _inspect_local_resource(
        self, kind: CleanupTargetKind, locator: Path, action: str
    ) -> dict[str, Any]:
        if not self._allowed_roots:
            raise CloseInventoryProviderError("cleanup_allowed_root_missing")
        path, info = self._canonical_target(locator, kind)
        root = next(
            (
                candidate
                for candidate in self._allowed_roots
                if path != candidate and self._is_within(candidate, path)
            ),
            None,
        )
        if root is None or self._is_protected_target(path):
            raise CloseInventoryProviderError("cleanup_target_outside_allowed_root")
        root_info = root.stat()
        return {
            "target_kind": kind.value,
            "action": action,
            "resource_identity": f"fs:{info.st_dev}:{info.st_ino}",
            "physical_ids": [f"fs:{info.st_dev}:{info.st_ino}"],
            "locator": os.fspath(path),
            "target_type": "directory" if stat.S_ISDIR(info.st_mode) else "file",
            "target_dev": int(info.st_dev),
            "target_ino": int(info.st_ino),
            "target_mode": int(stat.S_IFMT(info.st_mode)),
            "allowed_root": os.fspath(root),
            "allowed_root_dev": int(root_info.st_dev),
            "allowed_root_ino": int(root_info.st_ino),
        }

    def _inspect_branch_resource(
        self, repository: Path, branch: str, action: str
    ) -> dict[str, Any]:
        repo = self._canonical_git_directory(repository)
        branch_name = self._validated_branch_name(repo, branch)
        common = self._git_path(repo, "--git-common-dir")
        common_info = common.stat()
        oid = self._git(repo, "rev-parse", "--verify", f"refs/heads/{branch_name}")
        self._require_branch_not_checked_out(common, branch_name)
        identity = f"branch:{common_info.st_dev}:{common_info.st_ino}:{branch_name}"
        return {
            "target_kind": CleanupTargetKind.BRANCH.value,
            "action": action,
            "resource_identity": identity,
            "physical_ids": [identity],
            "repository": os.fspath(repo),
            "common_dir": os.fspath(common),
            "common_dev": int(common_info.st_dev),
            "common_ino": int(common_info.st_ino),
            "branch": branch_name,
            "branch_oid": oid,
        }

    def _inspect_linked_worktree(self, workspace: Path) -> dict[str, Any]:
        root = self._canonical_git_directory(workspace)
        top = self._git_path(root, "--show-toplevel")
        if top != root:
            raise CloseInventoryProviderError("cleanup_workspace_not_canonical_root")
        git_dir = self._git_path(root, "--git-dir")
        common = self._git_path(root, "--git-common-dir")
        if git_dir == common:
            raise CloseInventoryProviderError("cleanup_workspace_is_main_worktree")
        try:
            git_dir.relative_to(common / "worktrees")
        except ValueError as error:
            raise CloseInventoryProviderError(
                "cleanup_workspace_not_linked_worktree"
            ) from error
        branch_ref = self._git(root, "symbolic-ref", "--quiet", "HEAD")
        if not branch_ref.startswith("refs/heads/"):
            raise CloseInventoryProviderError("cleanup_workspace_detached_head")
        branch = branch_ref.removeprefix("refs/heads/")
        self._validated_branch_name(root, branch)
        oid = self._git(root, "rev-parse", "--verify", "HEAD")
        work_info = root.stat()
        git_info = git_dir.stat()
        common_info = common.stat()
        self._require_worktree_listing(common, root, branch_ref)
        workspace_id = f"fs:{work_info.st_dev}:{work_info.st_ino}"
        branch_id = f"branch:{common_info.st_dev}:{common_info.st_ino}:{branch}"
        return {
            "target_kind": CleanupTargetKind.WORKSPACE.value,
            "action": "delete",
            "resource_identity": f"worktree:{work_info.st_dev}:{work_info.st_ino}",
            "physical_ids": [workspace_id, branch_id],
            "locator": os.fspath(root),
            "target_dev": int(work_info.st_dev),
            "target_ino": int(work_info.st_ino),
            "git_dir": os.fspath(git_dir),
            "git_dir_dev": int(git_info.st_dev),
            "git_dir_ino": int(git_info.st_ino),
            "common_dir": os.fspath(common),
            "common_dev": int(common_info.st_dev),
            "common_ino": int(common_info.st_ino),
            "branch": branch,
            "branch_ref": branch_ref,
            "branch_oid": oid,
        }

    def _revalidate_private_record(self, record: Mapping[str, object]) -> None:
        if record.get("coverage") == "not-applicable":
            return
        kind = self._kind(str(record.get("target_kind")))
        self._require_action(kind, str(record.get("action")))
        if kind in {CleanupTargetKind.TEMPORARY, CleanupTargetKind.LOG}:
            path, info = self._canonical_target(Path(str(record["locator"])), kind)
            root = self._canonical_existing_directory(
                Path(str(record["allowed_root"])), "cleanup_allowed_root_invalid"
            )
            root_info = root.stat()
            if (
                path == root
                or not self._is_within(root, path)
                or self._is_protected_target(path)
                or (info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode))
                != (
                    self._stored_int(record, "target_dev"),
                    self._stored_int(record, "target_ino"),
                    self._stored_int(record, "target_mode"),
                )
                or (root_info.st_dev, root_info.st_ino)
                != (
                    self._stored_int(record, "allowed_root_dev"),
                    self._stored_int(record, "allowed_root_ino"),
                )
            ):
                raise CloseInventoryProviderError("cleanup_target_identity_changed")
            return
        if kind is CleanupTargetKind.BRANCH:
            common = self._canonical_existing_directory(
                Path(str(record["common_dir"])), "cleanup_git_identity_changed"
            )
            info = common.stat()
            branch = str(record["branch"])
            if (
                (info.st_dev, info.st_ino)
                != (
                    self._stored_int(record, "common_dev"),
                    self._stored_int(record, "common_ino"),
                )
                or self._git_git_dir(common, "rev-parse", "--verify", f"refs/heads/{branch}")
                != str(record["branch_oid"])
            ):
                raise CloseInventoryProviderError("cleanup_git_identity_changed")
            self._require_branch_not_checked_out(common, branch)
            return
        root, work_info = self._canonical_target(
            Path(str(record["locator"])), CleanupTargetKind.WORKSPACE
        )
        git_dir = self._canonical_existing_directory(
            Path(str(record["git_dir"])), "cleanup_git_identity_changed"
        )
        common = self._canonical_existing_directory(
            Path(str(record["common_dir"])), "cleanup_git_identity_changed"
        )
        git_info, common_info = git_dir.stat(), common.stat()
        branch = str(record["branch"])
        if (
            (work_info.st_dev, work_info.st_ino)
            != (
                self._stored_int(record, "target_dev"),
                self._stored_int(record, "target_ino"),
            )
            or (git_info.st_dev, git_info.st_ino)
            != (
                self._stored_int(record, "git_dir_dev"),
                self._stored_int(record, "git_dir_ino"),
            )
            or (common_info.st_dev, common_info.st_ino)
            != (
                self._stored_int(record, "common_dev"),
                self._stored_int(record, "common_ino"),
            )
            or self._git(root, "rev-parse", "--verify", "HEAD")
            != str(record["branch_oid"])
            or self._git(root, "symbolic-ref", "--quiet", "HEAD")
            != str(record["branch_ref"])
        ):
            raise CloseInventoryProviderError("cleanup_git_identity_changed")
        self._require_worktree_listing(common, root, f"refs/heads/{branch}")

    def _execute_one(self, record: Mapping[str, object]) -> None:
        kind = self._kind(str(record["target_kind"]))
        try:
            if kind in {CleanupTargetKind.TEMPORARY, CleanupTargetKind.LOG}:
                path = Path(str(record["locator"]))
                if record["target_type"] == "directory":
                    shutil.rmtree(path)
                else:
                    path.unlink()
                if os.path.lexists(path):
                    raise OSError("local cleanup postcondition failed")
                return
            if kind is CleanupTargetKind.BRANCH:
                common = Path(str(record["common_dir"]))
                branch = str(record["branch"])
                self._git_git_dir(common, "branch", "-D", "--", branch)
                self._require_branch_absent(common, branch)
                return
            workspace = Path(str(record["locator"]))
            common = Path(str(record["common_dir"]))
            branch = str(record["branch"])
            self._git_git_dir(
                common, "worktree", "remove", "--force", "--", os.fspath(workspace)
            )
            if os.path.lexists(workspace) or Path(str(record["git_dir"])).exists():
                raise OSError("worktree removal postcondition failed")
            self._git_git_dir(common, "branch", "-D", "--", branch)
            self._require_branch_absent(common, branch)
        except (
            OSError,
            subprocess.SubprocessError,
            CloseInventoryProviderError,
        ) as error:
            raise CloseInventoryProviderError(
                "cleanup_execution_outcome_unknown"
            ) from error

    def _execution_receipt(
        self,
        key: bytes,
        binding: Mapping[str, object],
        frozen: Mapping[str, object],
        private: Mapping[str, object],
    ) -> dict[str, str]:
        raw_public = frozen.get("public_records")
        if not isinstance(raw_public, list):
            raise CloseInventoryProviderError("cleanup_freeze_malformed")
        public = next(
            record
            for record in raw_public
            if isinstance(record, dict)
            if record.get("target_kind") == private["target_kind"]
            and record.get("coverage") == "enumerated"
            and hmac.compare_digest(
                str(record.get("target_fingerprint")),
                _hmac_hex(
                    key,
                    {
                        "domain": "cao-close-resource-v2",
                        "binding": binding,
                        "target_kind": private["target_kind"],
                        "action": private["action"],
                        "private_resource": private,
                    },
                ),
            )
        )
        receipt = {
            "target_kind": str(public["target_kind"]),
            "target_fingerprint": str(public["target_fingerprint"]),
            "action": str(public["action"]),
            "outcome": "succeeded",
            "execution_digest": str(public["execution_digest"]),
            "inventory_set_digest": str(public["inventory_set_digest"]),
        }
        receipt["provider_evidence_id"] = _hmac_id(
            key,
            {
                "domain": "cao-close-postcondition-v2",
                "binding": binding,
                "receipt": receipt,
            },
        )
        return receipt

    # ------------------------------------------------------------------
    # Git/path helpers
    # ------------------------------------------------------------------
    def _canonical_target(
        self, value: Path, kind: CleanupTargetKind
    ) -> tuple[Path, os.stat_result]:
        raw = Path(os.path.abspath(os.fspath(value)))
        try:
            info = raw.lstat()
            canonical = raw.resolve(strict=True)
            canonical_info = canonical.lstat()
        except (OSError, RuntimeError, ValueError) as error:
            raise CloseInventoryProviderError("cleanup_target_unavailable") from error
        if stat.S_ISLNK(info.st_mode):
            raise CloseInventoryProviderError("cleanup_target_symlink_forbidden")
        if self._stat_identity(info) != self._stat_identity(canonical_info):
            raise CloseInventoryProviderError("cleanup_target_identity_changed")
        info = canonical_info
        if kind is CleanupTargetKind.LOG and not stat.S_ISREG(info.st_mode):
            raise CloseInventoryProviderError("cleanup_log_must_be_regular_file")
        if kind is CleanupTargetKind.TEMPORARY and not (
            stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)
        ):
            raise CloseInventoryProviderError("cleanup_temporary_type_unsupported")
        if kind is CleanupTargetKind.WORKSPACE and not stat.S_ISDIR(info.st_mode):
            raise CloseInventoryProviderError("cleanup_workspace_not_directory")
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise CloseInventoryProviderError("cleanup_target_hardlink_forbidden")
        return canonical, info

    def _canonical_git_directory(self, value: Path) -> Path:
        path, info = self._canonical_target(value, CleanupTargetKind.WORKSPACE)
        if not stat.S_ISDIR(info.st_mode):
            raise CloseInventoryProviderError("cleanup_git_repository_unavailable")
        self._git(path, "rev-parse", "--is-inside-work-tree")
        return path

    @staticmethod
    def _canonical_existing_directory(value: Path, error_code: str) -> Path:
        raw = Path(os.path.abspath(os.fspath(value)))
        try:
            info = raw.lstat()
            canonical = raw.resolve(strict=True)
            canonical_info = canonical.lstat()
        except (OSError, RuntimeError, ValueError) as error:
            raise CloseInventoryProviderError(error_code) from error
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(canonical_info.st_mode)
            or OwnerPrivateCloseInventory._stat_identity(info)
            != OwnerPrivateCloseInventory._stat_identity(canonical_info)
        ):
            raise CloseInventoryProviderError(error_code)
        return canonical

    @staticmethod
    def _canonical_future_owned_directory(value: Path, error_code: str) -> Path:
        """Canonicalize ancestors while forbidding a symlink at the owned leaf."""

        raw = Path(os.path.abspath(os.fspath(value)))
        missing: list[str] = []
        probe = raw
        try:
            while not os.path.lexists(probe):
                if probe == probe.parent:
                    raise CloseInventoryProviderError(error_code)
                missing.append(probe.name)
                probe = probe.parent
            info = probe.lstat()
            # A symlink at the owned leaf is forbidden.  An already-existing
            # ancestor alias is resolved once and is never persisted as the
            # cleanup locator.
            if not missing and stat.S_ISLNK(info.st_mode):
                raise CloseInventoryProviderError(error_code)
            canonical = probe.resolve(strict=True)
            canonical_info = canonical.lstat()
        except CloseInventoryProviderError:
            raise
        except (OSError, RuntimeError, ValueError) as error:
            raise CloseInventoryProviderError(error_code) from error
        if (
            not stat.S_ISDIR(canonical_info.st_mode)
            or (
                not stat.S_ISLNK(info.st_mode)
                and OwnerPrivateCloseInventory._stat_identity(info)
                != OwnerPrivateCloseInventory._stat_identity(canonical_info)
            )
        ):
            raise CloseInventoryProviderError(error_code)
        return canonical.joinpath(*reversed(missing))

    @staticmethod
    def _stat_identity(info: os.stat_result) -> tuple[int, int, int]:
        return (int(info.st_dev), int(info.st_ino), stat.S_IFMT(info.st_mode))

    @staticmethod
    def _canonical_existing_or_future(value: Path) -> Path:
        raw = Path(os.path.abspath(os.fspath(value)))
        try:
            return raw.resolve(strict=True)
        except OSError:
            return raw

    @staticmethod
    def _is_within(root: Path, candidate: Path) -> bool:
        try:
            candidate.relative_to(root)
            return True
        except ValueError:
            return False

    def _is_protected_target(self, candidate: Path) -> bool:
        for protected in (*self._protected_roots, *self._allowed_roots):
            if candidate == protected or self._is_within(candidate, protected):
                return True
        for protected in self._explicit_protected_roots:
            if (
                candidate == protected
                or self._is_within(protected, candidate)
                or self._is_within(candidate, protected)
            ):
                return True
        return False

    def _git(self, repository: Path, *args: str) -> str:
        return self._run_git("-C", os.fspath(repository), *args)

    def _git_git_dir(self, git_dir: Path, *args: str) -> str:
        return self._run_git(f"--git-dir={os.fspath(git_dir)}", *args)

    @staticmethod
    def _run_git(*args: str) -> str:
        try:
            completed = subprocess.run(
                ["git", *args],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise CloseInventoryProviderError("cleanup_git_unavailable") from error
        if completed.returncode != 0:
            raise CloseInventoryProviderError("cleanup_git_command_failed")
        return completed.stdout.strip()

    def _git_path(self, repository: Path, selector: str) -> Path:
        raw = self._git(repository, "rev-parse", selector)
        path = Path(raw)
        if not path.is_absolute():
            path = repository / path
        return self._canonical_existing_directory(path, "cleanup_git_identity_changed")

    def _validated_branch_name(self, repository: Path, branch: str) -> str:
        if not _BRANCH.fullmatch(branch):
            raise CloseInventoryProviderError("cleanup_branch_invalid")
        checked = self._git(repository, "check-ref-format", "--branch", branch)
        if checked != branch:
            raise CloseInventoryProviderError("cleanup_branch_invalid")
        return branch

    def _require_branch_not_checked_out(self, common: Path, branch: str) -> None:
        needle = f"refs/heads/{branch}"
        for entry in self._worktree_entries(common):
            if entry.get("branch") == needle:
                raise CloseInventoryProviderError("cleanup_branch_is_checked_out")

    def _require_worktree_listing(
        self, common: Path, workspace: Path, branch_ref: str
    ) -> None:
        for entry in self._worktree_entries(common):
            if entry.get("worktree") == os.fspath(workspace) and entry.get("branch") == branch_ref:
                return
        raise CloseInventoryProviderError("cleanup_workspace_not_linked_worktree")

    def _worktree_entries(self, common: Path) -> list[dict[str, str]]:
        output = self._git_git_dir(common, "worktree", "list", "--porcelain")
        entries: list[dict[str, str]] = []
        current: dict[str, str] = {}
        for line in (*output.splitlines(), ""):
            if not line:
                if current:
                    entries.append(current)
                    current = {}
                continue
            key, _, value = line.partition(" ")
            current[key] = value
        return entries

    def _require_branch_absent(self, common: Path, branch: str) -> None:
        completed = subprocess.run(
            [
                "git",
                f"--git-dir={os.fspath(common)}",
                "show-ref",
                "--verify",
                "--quiet",
                f"refs/heads/{branch}",
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
        )
        if completed.returncode != 1:
            raise CloseInventoryProviderError("cleanup_postcondition_failed")

    # ------------------------------------------------------------------
    # Persistence, locking, and low-level validation
    # ------------------------------------------------------------------
    @contextmanager
    def _locked(self) -> Iterator[None]:
        with self._thread_lock:
            self._ensure_private_state_dir()
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self._lock_path, flags, 0o600)
                self._assert_private_stat(os.fstat(descriptor), is_file=True)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            except OSError as error:
                raise CloseInventoryProviderError("cleanup_private_store_unavailable") from error
            finally:
                if "descriptor" in locals():
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                    os.close(descriptor)

    def _read_or_create_registry_locked(self) -> dict[str, Any]:
        if not os.path.lexists(self._registry_path):
            self._atomic_write_locked(
                self._registry_path,
                {
                    "version": 2,
                    "key": base64.urlsafe_b64encode(secrets.token_bytes(32))
                    .decode("ascii")
                    .rstrip("="),
                    "revision": 0,
                    "works": {},
                },
            )
        value = self._read_private_json_locked(self._registry_path)
        if (
            not isinstance(value, dict)
            or set(value) != _REGISTRY_FIELDS
            or value.get("version") != 2
            or not isinstance(value.get("revision"), int)
            or not isinstance(value.get("works"), dict)
        ):
            raise CloseInventoryProviderError("cleanup_registry_malformed")
        self._key(value)
        return value

    def _read_or_create_ledger_locked(
        self, *, create: bool = True
    ) -> dict[str, Any]:
        if not os.path.lexists(self._ledger_path):
            if not create:
                raise CloseInventoryProviderError("cleanup_freeze_missing")
            self._atomic_write_locked(
                self._ledger_path, {"version": 2, "preparations": {}}
            )
        value = self._read_private_json_locked(self._ledger_path)
        if (
            not isinstance(value, dict)
            or set(value) != _LEDGER_FIELDS
            or value.get("version") != 2
            or not isinstance(value.get("preparations"), dict)
        ):
            raise CloseInventoryProviderError("cleanup_freeze_malformed")
        return value

    def _commit_registry_locked(self, registry: dict[str, Any]) -> None:
        registry["revision"] = int(registry["revision"]) + 1
        self._atomic_write_locked(self._registry_path, registry)

    def _read_private_json_locked(self, path: Path) -> Any:
        self._assert_private_path(path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
            self._assert_private_stat(os.fstat(descriptor), is_file=True)
            with os.fdopen(descriptor, "rb") as handle:
                return json.loads(handle.read().decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CloseInventoryProviderError("cleanup_private_store_unavailable") from error

    def _atomic_write_locked(self, path: Path, value: object) -> None:
        self._ensure_private_state_dir()
        if os.path.lexists(path):
            self._assert_private_path(path)
        temporary = self._state_dir / f".{path.name}.{secrets.token_hex(12)}.tmp"
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(temporary, flags, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                descriptor = None
                handle.write(_canonical(value))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            directory = os.open(self._state_dir, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self._assert_private_path(path)
        except OSError as error:
            raise CloseInventoryProviderError("cleanup_private_store_unavailable") from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                temporary.unlink()

    def _ensure_private_state_dir(self) -> None:
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            info = self._state_dir.lstat()
        except OSError as error:
            raise CloseInventoryProviderError("cleanup_private_store_unavailable") from error
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise CloseInventoryProviderError("cleanup_private_store_unavailable")

    def _assert_private_path(self, path: Path) -> None:
        try:
            info = path.lstat()
            canonical = path.resolve(strict=True)
        except OSError as error:
            raise CloseInventoryProviderError("cleanup_private_store_unavailable") from error
        if canonical != path or stat.S_ISLNK(info.st_mode):
            raise CloseInventoryProviderError("cleanup_private_store_unavailable")
        self._assert_private_stat(info, is_file=True)

    @staticmethod
    def _assert_private_stat(info: os.stat_result, *, is_file: bool) -> None:
        expected = stat.S_ISREG if is_file else stat.S_ISDIR
        mode = 0o600 if is_file else 0o700
        if (
            not expected(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != mode
            or (is_file and info.st_nlink != 1)
        ):
            raise CloseInventoryProviderError("cleanup_private_store_unavailable")

    @staticmethod
    def _key(registry: Mapping[str, Any]) -> bytes:
        raw = registry.get("key")
        if not isinstance(raw, str):
            raise CloseInventoryProviderError("cleanup_registry_malformed")
        try:
            key = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
        except (UnicodeEncodeError, ValueError) as error:
            raise CloseInventoryProviderError("cleanup_registry_malformed") from error
        if len(key) != 32:
            raise CloseInventoryProviderError("cleanup_registry_malformed")
        return key

    @staticmethod
    def _identifier(value: str, error_code: str) -> str:
        if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
            raise CloseInventoryProviderError(error_code)
        return value

    @classmethod
    def _binding(
        cls, preparation_id: str, work_item_id: str, generation: int, attachment_id: str
    ) -> dict[str, object]:
        if isinstance(generation, bool) or generation < 1:
            raise CloseInventoryProviderError("cleanup_freeze_binding_invalid")
        return {
            "preparation_id": cls._identifier(
                preparation_id, "cleanup_freeze_binding_invalid"
            ),
            "work_item_id": cls._identifier(
                work_item_id, "cleanup_freeze_binding_invalid"
            ),
            "generation": generation,
            "attachment_id": cls._identifier(
                attachment_id, "cleanup_freeze_binding_invalid"
            ),
        }

    @staticmethod
    def _kind(value: CleanupTargetKind | str) -> CleanupTargetKind:
        try:
            kind = value if isinstance(value, CleanupTargetKind) else CleanupTargetKind(value)
        except ValueError as error:
            raise CloseInventoryProviderError("cleanup_registry_kind_invalid") from error
        if kind not in _KINDS:
            raise CloseInventoryProviderError("cleanup_registry_kind_invalid")
        return kind

    @staticmethod
    def _require_action(kind: CleanupTargetKind, action: str) -> None:
        del kind
        if action != "delete":
            raise CloseInventoryProviderError("cleanup_action_unsupported")

    @staticmethod
    def _physical_ids(resource: Mapping[str, object]) -> tuple[str, ...]:
        raw = resource.get("physical_ids")
        if not isinstance(raw, list) or not raw or not all(
            isinstance(value, str) and value for value in raw
        ):
            raise CloseInventoryProviderError("cleanup_registry_malformed")
        return tuple(raw)

    @staticmethod
    def _stored_int(record: Mapping[str, object], key: str) -> int:
        value = record.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise CloseInventoryProviderError("cleanup_freeze_malformed")
        return value

    def _validate_physical_uniqueness(
        self, records: Sequence[Mapping[str, object]]
    ) -> None:
        seen: set[str] = set()
        for record in records:
            if record.get("coverage") != "enumerated":
                continue
            for identity in self._physical_ids(record):
                if identity in seen:
                    raise CloseInventoryProviderError(
                        "cleanup_registry_physical_target_conflict"
                    )
                seen.add(identity)
