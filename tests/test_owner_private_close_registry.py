from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from cao_control_plane.close_contract import CleanupTargetKind
from cao_control_plane.close_inventory_edge import (
    CloseInventoryProviderError,
    OwnerPrivateCloseInventory,
)


def _edge(
    tmp_path: Path,
    *,
    allowed: tuple[Path, ...] = (),
    protected: tuple[Path, ...] = (),
) -> OwnerPrivateCloseInventory:
    state = tmp_path / "state"
    return OwnerPrivateCloseInventory(
        state,
        allowed_cleanup_roots=allowed,
        protected_roots=protected,
    )


def _declare_all_na(edge: OwnerPrivateCloseInventory, work: str) -> None:
    for kind in (
        CleanupTargetKind.WORKSPACE,
        CleanupTargetKind.TEMPORARY,
        CleanupTargetKind.LOG,
        CleanupTargetKind.BRANCH,
    ):
        edge.declare_not_applicable(work_item_id=work, target_kind=kind)


def _prepare(
    edge: OwnerPrivateCloseInventory,
    *,
    preparation: str = "closeprep-one",
    work: str = "work-one",
    generation: int = 1,
    attachment: str = "attachment-one",
) -> list[dict[str, str]]:
    return edge.prepare(
        preparation_id=preparation,
        work_item_id=work,
        generation=generation,
        attachment_id=attachment,
    )


def _execute(
    edge: OwnerPrivateCloseInventory,
    public: list[dict[str, str]],
    *,
    preparation: str = "closeprep-one",
    work: str = "work-one",
    generation: int = 1,
    attachment: str = "attachment-one",
) -> list[dict[str, str]]:
    return edge.execute(
        preparation_id=preparation,
        work_item_id=work,
        generation=generation,
        attachment_id=attachment,
        public_inventory=public,
    )


def _git(*args: str, cwd: Path | None = None) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _repository_with_linked_worktree(tmp_path: Path) -> tuple[Path, Path, str]:
    repository = tmp_path / "main-repository"
    linked = tmp_path / "linked-worktree"
    repository.mkdir()
    _git("init", "-q", "-b", "main", cwd=repository)
    _git(
        "-c",
        "user.name=Close Test",
        "-c",
        "user.email=close@example.invalid",
        "commit",
        "--allow-empty",
        "-qm",
        "initial",
        cwd=repository,
    )
    branch = "managed-close-branch"
    _git("worktree", "add", "-qb", branch, str(linked), cwd=repository)
    return repository.resolve(), linked.resolve(), branch


def _registry_path(tmp_path: Path) -> Path:
    return tmp_path / "state" / "close-cleanup-owner-private-v2.json"


def _ledger_path(tmp_path: Path) -> Path:
    return tmp_path / "state" / "close-cleanup-freeze-ledger-v2.json"


def test_missing_category_is_not_implicit_not_applicable(tmp_path: Path) -> None:
    edge = _edge(tmp_path)
    edge.declare_not_applicable(
        work_item_id="work-one", target_kind=CleanupTargetKind.WORKSPACE
    )
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup registry incomplete coverage"
    ):
        _prepare(edge)


def test_stable_ancestor_alias_is_canonicalized_but_leaf_symlink_is_rejected(
    tmp_path: Path,
) -> None:
    physical = tmp_path / "physical"
    allowed = physical / "allowed"
    allowed.mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(physical, target_is_directory=True)

    edge = OwnerPrivateCloseInventory(
        alias / "state",
        allowed_cleanup_roots=(alias / "allowed",),
    )
    target = alias / "allowed" / "result.log"
    target.write_text("result", encoding="utf-8")
    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.LOG,
        locator=target,
    )
    edge.declare_not_applicable(
        work_item_id="work-one", target_kind=CleanupTargetKind.WORKSPACE
    )
    edge.declare_not_applicable(
        work_item_id="work-one", target_kind=CleanupTargetKind.TEMPORARY
    )
    edge.declare_not_applicable(
        work_item_id="work-one", target_kind=CleanupTargetKind.BRANCH
    )
    assert len(_prepare(edge)) == 4
    assert (physical / "state" / "close-cleanup-owner-private-v2.json").is_file()

    leaf_alias = physical / "allowed-link"
    leaf_alias.symlink_to(allowed, target_is_directory=True)
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup allowed root invalid"
    ):
        OwnerPrivateCloseInventory(
            physical / "other-state", allowed_cleanup_roots=(leaf_alias,)
        )

def test_explicit_not_applicable_is_complete_signed_and_idempotently_frozen(
    tmp_path: Path,
) -> None:
    edge = _edge(tmp_path)
    _declare_all_na(edge, "work-one")

    first = _prepare(edge)
    second = _prepare(edge)

    assert second == first
    assert len(first) == 4
    assert {record["target_kind"] for record in first} == {
        "workspace",
        "temporary",
        "log",
        "branch",
    }
    assert {record["coverage"] for record in first} == {"not-applicable"}
    assert len({record["inventory_set_digest"] for record in first}) == 1
    assert all(record["provider_evidence_id"].startswith("hmac-sha256:") for record in first)
    assert edge.verify_public_inventory(
        preparation_id="closeprep-one",
        work_item_id="work-one",
        generation=1,
        attachment_id="attachment-one",
        public_inventory=first,
    )
    assert _execute(edge, first) == []


def test_managed_linked_worktree_is_removed_before_its_branch_and_main_survives(
    tmp_path: Path,
) -> None:
    repository, linked, branch = _repository_with_linked_worktree(tmp_path)
    allowed = tmp_path / "owned-cleanup"
    temporary = allowed / "temporary"
    log = allowed / "worker.log"
    temporary.mkdir(parents=True)
    log.write_text("owned log", encoding="utf-8")
    edge = _edge(tmp_path, allowed=(allowed,))

    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.TEMPORARY,
        locator=temporary,
    )
    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.LOG,
        locator=log,
    )
    edge.adopt_managed_work("work-one", linked)
    public = _prepare(edge)
    serialized_public = json.dumps(public)
    assert str(linked) not in serialized_public
    assert str(repository) not in serialized_public
    assert branch not in serialized_public
    receipts = _execute(edge, public)

    assert [receipt["target_kind"] for receipt in receipts] == [
        "temporary",
        "log",
        "workspace",
    ]
    assert not temporary.exists() and not log.exists()
    assert not linked.exists()
    assert repository.exists()
    assert _git("branch", "--show-current", cwd=repository) == "main"
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "show-ref",
                "--verify",
                "--quiet",
                f"refs/heads/{branch}",
            ],
            check=False,
        ).returncode
        != 0
    )
    categories = {record["target_kind"]: record["coverage"] for record in public}
    assert categories == {
        "temporary": "enumerated",
        "log": "enumerated",
        "branch": "not-applicable",
        "workspace": "enumerated",
    }


def test_main_repository_is_never_adopted_and_requires_explicit_na(
    tmp_path: Path,
) -> None:
    repository, _linked, _branch = _repository_with_linked_worktree(tmp_path)
    edge = _edge(tmp_path)

    with pytest.raises(
        CloseInventoryProviderError, match="cleanup workspace is main worktree"
    ):
        edge.adopt_managed_worktree(work_item_id="work-one", workspace=repository)
    edge.adopt_managed_work("work-one", repository)
    assert {record["coverage"] for record in _prepare(edge)} == {"not-applicable"}
    assert repository.exists()


def test_tmp_and_log_require_safe_exact_targets(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    edge = _edge(tmp_path, allowed=(allowed,))
    outside = tmp_path / "outside.tmp"
    outside.write_text("outside", encoding="utf-8")
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup target outside allowed root"
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.TEMPORARY,
            locator=outside,
        )

    with pytest.raises(
        CloseInventoryProviderError, match="cleanup target outside allowed root"
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.TEMPORARY,
            locator=allowed,
        )

    target = allowed / "target.log"
    target.write_text("target", encoding="utf-8")
    symlink = allowed / "target-link.log"
    symlink.symlink_to(target)
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup target symlink forbidden"
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.LOG,
            locator=symlink,
        )

    hardlink = allowed / "target-hardlink.log"
    os.link(target, hardlink)
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup target hardlink forbidden"
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.LOG,
            locator=target,
        )

    protected = allowed / "protected"
    protected.mkdir()
    protected_target = protected / "must-survive.tmp"
    protected_target.write_text("protected", encoding="utf-8")
    protected_edge = _edge(
        tmp_path / "protected-case",
        allowed=(allowed,),
        protected=(protected,),
    )
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup target outside allowed root"
    ):
        protected_edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.TEMPORARY,
            locator=protected_target,
        )


@pytest.mark.parametrize("action", ["archive", "trash", "stop", "detach"])
def test_kind_action_matrix_accepts_only_delete(tmp_path: Path, action: str) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.tmp"
    target.write_text("target", encoding="utf-8")
    edge = _edge(tmp_path, allowed=(allowed,))

    with pytest.raises(
        CloseInventoryProviderError, match="cleanup action unsupported"
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.TEMPORARY,
            action=action,
            locator=target,
        )


def test_public_action_substitution_blocks_before_effect(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.tmp"
    target.write_text("target", encoding="utf-8")
    edge = _edge(tmp_path, allowed=(allowed,))
    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.TEMPORARY,
        locator=target,
    )
    for kind in (
        CleanupTargetKind.WORKSPACE,
        CleanupTargetKind.LOG,
        CleanupTargetKind.BRANCH,
    ):
        edge.declare_not_applicable(work_item_id="work-one", target_kind=kind)
    public = _prepare(edge)
    changed = [dict(record) for record in public]
    next(record for record in changed if record["target_kind"] == "temporary")[
        "action"
    ] = "archive"

    with pytest.raises(
        CloseInventoryProviderError, match="cleanup public set mismatch"
    ):
        _execute(edge, changed)
    assert target.exists()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda frozen: frozen["private_records"][0].update(
            {"locator": "/tampered/locator"}
        ),
        lambda frozen: frozen["private_records"].append(
            dict(frozen["private_records"][0])
        ),
    ],
    ids=["locator", "extra-entry"],
)
def test_frozen_private_ledger_tamper_blocks_before_effect(
    tmp_path: Path, mutate: Callable[[dict[str, Any]], None]
) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.tmp"
    target.write_text("target", encoding="utf-8")
    edge = _edge(tmp_path, allowed=(allowed,))
    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.TEMPORARY,
        locator=target,
    )
    for kind in (
        CleanupTargetKind.WORKSPACE,
        CleanupTargetKind.LOG,
        CleanupTargetKind.BRANCH,
    ):
        edge.declare_not_applicable(work_item_id="work-one", target_kind=kind)
    public = _prepare(edge)
    ledger_path = _ledger_path(tmp_path)
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    mutate(ledger["preparations"]["closeprep-one"])
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    ledger_path.chmod(0o600)

    with pytest.raises(
        CloseInventoryProviderError, match="cleanup freeze seal invalid"
    ):
        _execute(edge, public)
    assert target.exists()


def test_inode_replacement_after_freeze_blocks_before_effect(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.tmp"
    original = allowed / "original.tmp"
    target.write_text("original", encoding="utf-8")
    edge = _edge(tmp_path, allowed=(allowed,))
    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.TEMPORARY,
        locator=target,
    )
    for kind in (
        CleanupTargetKind.WORKSPACE,
        CleanupTargetKind.LOG,
        CleanupTargetKind.BRANCH,
    ):
        edge.declare_not_applicable(work_item_id="work-one", target_kind=kind)
    public = _prepare(edge)
    target.rename(original)
    target.write_text("replacement", encoding="utf-8")

    with pytest.raises(
        CloseInventoryProviderError, match="cleanup target identity changed"
    ):
        _execute(edge, public)
    assert target.read_text(encoding="utf-8") == "replacement"
    assert original.read_text(encoding="utf-8") == "original"


def test_duplicate_and_cross_work_physical_targets_are_rejected(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    target = allowed / "target.tmp"
    target.write_text("target", encoding="utf-8")
    edge = _edge(tmp_path, allowed=(allowed,))
    edge.register_resource(
        work_item_id="work-one",
        target_kind=CleanupTargetKind.TEMPORARY,
        locator=target,
    )
    with pytest.raises(
        CloseInventoryProviderError, match="cleanup registry duplicate resource"
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.TEMPORARY,
            locator=target,
        )

    with pytest.raises(
        CloseInventoryProviderError,
        match="cleanup registry physical target conflict",
    ):
        edge.register_resource(
            work_item_id="work-two",
            target_kind=CleanupTargetKind.TEMPORARY,
            locator=target,
        )
    with pytest.raises(
        CloseInventoryProviderError,
        match="cleanup registry physical target conflict",
    ):
        edge.register_resource(
            work_item_id="work-one",
            target_kind=CleanupTargetKind.LOG,
            locator=target,
        )


def test_private_store_is_owner_only_and_atomic_temp_files_do_not_remain(
    tmp_path: Path,
) -> None:
    edge = _edge(tmp_path)
    _declare_all_na(edge, "work-one")
    _prepare(edge)

    registry, ledger = _registry_path(tmp_path), _ledger_path(tmp_path)
    assert stat_mode(registry) == 0o600
    assert stat_mode(ledger) == 0o600
    assert list((tmp_path / "state").glob("*.tmp")) == []


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777
