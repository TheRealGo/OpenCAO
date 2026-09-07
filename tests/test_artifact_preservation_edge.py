from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

from cao_control_plane.artifact_preservation_edge import (
    ArtifactPreservationProviderError,
    OwnerPrivateArtifactPreservation,
)


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _edge(tmp_path: Path) -> OwnerPrivateArtifactPreservation:
    return OwnerPrivateArtifactPreservation(tmp_path / "owner-private-state")


def _register(
    edge: OwnerPrivateArtifactPreservation,
    source: Path,
    *,
    artifact_id: str = "artifact-one",
    preparation_id: str = "prepare-one",
    attachment_generation: int = 3,
) -> dict[str, str]:
    return edge.register_file(
        work_item_id="work-one",
        preparation_id=preparation_id,
        attachment_id="attachment-one",
        attachment_generation=attachment_generation,
        artifact_id=artifact_id,
        digest=_digest(source.read_bytes()),
        locator=source,
    )


def _seal(
    edge: OwnerPrivateArtifactPreservation,
    source: Path,
    *,
    artifact_id: str = "artifact-one",
    preparation_id: str = "prepare-one",
    attachment_generation: int = 3,
) -> list[dict[str, str]]:
    return edge.seal_receipt_set(
        work_item_id="work-one",
        preparation_id=preparation_id,
        attachment_id="attachment-one",
        attachment_generation=attachment_generation,
        artifacts=[{"artifact_id": artifact_id, "digest": _digest(source.read_bytes())}],
    )


def test_regular_file_is_archived_and_returns_only_sanitized_immutable_receipts(
    tmp_path: Path,
) -> None:
    source = tmp_path / "raw-artifact.bin"
    source.write_bytes(b"retained evidence")
    edge = _edge(tmp_path)

    observation = _register(edge, source)
    receipts = _seal(edge, source)
    serialized = json.dumps({"observation": observation, "receipts": receipts})

    assert str(source) not in serialized
    assert receipts[0]["artifact_id"] == "artifact-one"
    assert receipts[0]["digest"] == _digest(b"retained evidence")
    assert receipts[0]["preservation"] == "content-addressed-archive"
    assert receipts[0]["provider_evidence_id"].startswith("hmac-sha256:")
    assert edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-one",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=receipts,
    )

    source.unlink()
    assert edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-one",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=receipts,
    )
    ledger_path = tmp_path / "owner-private-state" / "artifact-preservation-owner-private-v1.json"
    info = ledger_path.stat()
    assert info.st_mode & 0o077 == 0
    assert str(source) not in ledger_path.read_text(encoding="utf-8")


def test_staged_file_hides_source_locator_and_can_be_bound_after_source_removal(
    tmp_path: Path,
) -> None:
    source = tmp_path / "ephemeral-artifact.txt"
    source.write_bytes(b"stage before close")
    digest = _digest(source.read_bytes())
    edge = _edge(tmp_path)

    staged = edge.stage_file(digest=digest, locator=source)
    assert staged == f"owner-private-artifact:{digest}"
    assert str(source) not in staged
    source.unlink()

    observation = edge.register_staged_file(
        work_item_id="work-one",
        preparation_id="prepare-staged",
        attachment_id="attachment-one",
        attachment_generation=3,
        artifact_id="artifact-one",
        digest=digest,
        staged_ref=staged,
    )
    receipts = edge.seal_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-staged",
        attachment_id="attachment-one",
        attachment_generation=3,
        artifacts=[{"artifact_id": "artifact-one", "digest": digest}],
    )

    assert str(source) not in json.dumps({"observation": observation, "receipts": receipts})
    assert edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-staged",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=receipts,
    )


def test_inline_bytes_are_verified_and_staged_without_a_source_locator(
    tmp_path: Path,
) -> None:
    content = b"bounded inline artifact"
    digest = _digest(content)
    edge = _edge(tmp_path)

    staged = edge.stage_bytes(digest=digest, content=content)

    assert staged == f"owner-private-artifact:{digest}"
    archive = tmp_path / "owner-private-state" / "artifact-archive-v1" / digest[:2] / digest
    assert archive.read_bytes() == content
    with pytest.raises(
        ArtifactPreservationProviderError,
        match="source digest mismatch",
    ):
        edge.stage_bytes(digest="0" * 64, content=content)


def test_workspace_relative_file_is_staged_through_a_pinned_directory_fd(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    nested = workspace / "results"
    nested.mkdir(parents=True)
    content = b"workspace artifact"
    source = nested / "result.txt"
    source.write_bytes(content)
    digest = _digest(content)
    edge = _edge(tmp_path)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    workspace_fd = os.open(workspace, flags)
    try:
        staged = edge.stage_workspace_file(
            digest=digest,
            workspace_fd=workspace_fd,
            relative_locator="results/result.txt",
        )
        assert staged == f"owner-private-artifact:{digest}"
        archive = (
            tmp_path
            / "owner-private-state"
            / "artifact-archive-v1"
            / digest[:2]
            / digest
        )
        assert archive.read_bytes() == content

        source.write_bytes(b"substituted workspace artifact")
        with pytest.raises(
            ArtifactPreservationProviderError,
            match="source digest mismatch",
        ):
            edge.stage_workspace_file(
                digest=digest,
                workspace_fd=workspace_fd,
                relative_locator="results/result.txt",
            )

        with pytest.raises(
            ArtifactPreservationProviderError,
            match="workspace locator invalid",
        ):
            edge.stage_workspace_file(
                digest=digest,
                workspace_fd=workspace_fd,
                relative_locator="../result.txt",
            )
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret.txt").write_bytes(content)
        (workspace / "escape").symlink_to(outside, target_is_directory=True)
        with pytest.raises(
            ArtifactPreservationProviderError,
            match="workspace source unavailable",
        ):
            edge.stage_workspace_file(
                digest=digest,
                workspace_fd=workspace_fd,
                relative_locator="escape/secret.txt",
            )
    finally:
        os.close(workspace_fd)


def test_workspace_special_file_is_opened_nonblocking_and_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    fifo = workspace / "result.pipe"
    os.mkfifo(fifo)
    edge = _edge(tmp_path)
    workspace_fd = os.open(
        workspace,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    real_open = os.open
    observed_flags: list[int] = []

    def checked_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if path == "result.pipe":
            observed_flags.append(flags)
            assert flags & os.O_NONBLOCK
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", checked_open)
    try:
        with pytest.raises(
            ArtifactPreservationProviderError,
            match="source unsafe",
        ):
            edge.stage_workspace_file(
                digest=_digest(b"never available from a fifo"),
                workspace_fd=workspace_fd,
                relative_locator="result.pipe",
            )
    finally:
        os.close(workspace_fd)
    assert observed_flags


def test_staged_registration_fails_closed_for_invalid_reference_or_missing_archive(
    tmp_path: Path,
) -> None:
    source = tmp_path / "artifact.txt"
    source.write_bytes(b"stage first")
    digest = _digest(source.read_bytes())
    edge = _edge(tmp_path)
    staged = edge.stage_file(digest=digest, locator=source)

    with pytest.raises(ArtifactPreservationProviderError, match="staged reference invalid"):
        edge.register_staged_file(
            work_item_id="work-one",
            preparation_id="prepare-staged",
            attachment_id="attachment-one",
            attachment_generation=3,
            artifact_id="artifact-one",
            digest=digest,
            staged_ref="owner-private-artifact:forged",
        )

    archive = (
        tmp_path / "owner-private-state" / "artifact-archive-v1" / digest[:2] / digest
    )
    archive.unlink()
    with pytest.raises(ArtifactPreservationProviderError, match="archive unavailable"):
        edge.register_staged_file(
            work_item_id="work-one",
            preparation_id="prepare-staged",
            attachment_id="attachment-one",
            attachment_generation=3,
            artifact_id="artifact-one",
            digest=digest,
            staged_ref=staged,
        )


def test_observation_and_seal_are_idempotent_but_bound_to_exact_context_and_set(
    tmp_path: Path,
) -> None:
    source = tmp_path / "artifact.txt"
    source.write_bytes(b"stable")
    edge = _edge(tmp_path)

    first = _register(edge, source)
    assert _register(edge, source) == first
    receipts = _seal(edge, source)
    assert _seal(edge, source) == receipts

    with pytest.raises(ArtifactPreservationProviderError, match="set mismatch"):
        edge.seal_receipt_set(
            work_item_id="work-one",
            preparation_id="prepare-one",
            attachment_id="attachment-one",
            attachment_generation=3,
            artifacts=[{"artifact_id": "artifact-one", "digest": _digest(b"different")}],
        )
    with pytest.raises(ArtifactPreservationProviderError, match="already sealed"):
        edge.register_file(
            work_item_id="work-one",
            preparation_id="prepare-one",
            attachment_id="attachment-one",
            attachment_generation=3,
            artifact_id="artifact-two",
            digest=_digest(source.read_bytes()),
            locator=source,
        )
    assert not edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-one",
        attachment_id="attachment-one",
        attachment_generation=4,
        public_receipts=receipts,
    )


def test_seal_requires_every_canonical_artifact_to_have_trusted_observation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "artifact.txt"
    source.write_bytes(b"known")
    edge = _edge(tmp_path)
    _register(edge, source)

    with pytest.raises(ArtifactPreservationProviderError, match="registration incomplete"):
        edge.seal_receipt_set(
            work_item_id="work-one",
            preparation_id="prepare-one",
            attachment_id="attachment-one",
            attachment_generation=3,
            artifacts=[
                {"artifact_id": "artifact-one", "digest": _digest(b"known")},
                {"artifact_id": "artifact-two", "digest": _digest(b"other")},
            ],
        )


def test_empty_canonical_manifest_can_be_explicitly_sealed(tmp_path: Path) -> None:
    edge = _edge(tmp_path)

    receipts = edge.seal_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-empty",
        attachment_id="attachment-one",
        attachment_generation=3,
        artifacts=[],
    )

    assert receipts == []
    assert edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-empty",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=[],
    )


def test_source_and_archive_symlink_or_hardlink_substitution_fail_closed(tmp_path: Path) -> None:
    target = tmp_path / "target.txt"
    target.write_bytes(b"content")
    symlink = tmp_path / "source-link.txt"
    symlink.symlink_to(target)
    hardlink = tmp_path / "source-hardlink.txt"
    os.link(target, hardlink)
    edge = _edge(tmp_path)

    with pytest.raises(ArtifactPreservationProviderError, match="source symlink forbidden"):
        _register(edge, symlink)
    with pytest.raises(ArtifactPreservationProviderError, match="source unsafe"):
        _register(edge, hardlink)

    source = tmp_path / "ordinary.txt"
    source.write_bytes(b"ordinary")
    _register(edge, source)
    ledger_path = tmp_path / "owner-private-state" / "artifact-preservation-owner-private-v1.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    record = ledger["preparations"]["prepare-one"]["registrations"]["artifact-one"]
    archive = Path(record["archive_locator"])
    archive.unlink()
    archive.symlink_to(source)
    with pytest.raises(ArtifactPreservationProviderError, match="archive unsafe"):
        _seal(edge, source)

    second_source = tmp_path / "ordinary-two.txt"
    second_source.write_bytes(b"ordinary two")
    second_edge = OwnerPrivateArtifactPreservation(tmp_path / "second-owner-private-state")
    _register(second_edge, second_source)
    second_ledger = json.loads(
        (
            tmp_path
            / "second-owner-private-state"
            / "artifact-preservation-owner-private-v1.json"
        ).read_text(encoding="utf-8")
    )
    second_archive = Path(
        second_ledger["preparations"]["prepare-one"]["registrations"]["artifact-one"][
            "archive_locator"
        ]
    )
    os.link(second_archive, tmp_path / "archive-hardlink.txt")
    with pytest.raises(ArtifactPreservationProviderError, match="archive unsafe"):
        _seal(second_edge, second_source)


def test_public_or_private_tampering_and_archive_content_change_block_verification(
    tmp_path: Path,
) -> None:
    source = tmp_path / "artifact.txt"
    source.write_bytes(b"original")
    edge = _edge(tmp_path)
    _register(edge, source)
    receipts = _seal(edge, source)

    altered = [dict(receipts[0])]
    altered[0]["digest"] = _digest(b"forged")
    assert not edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-one",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=altered,
    )

    ledger_path = tmp_path / "owner-private-state" / "artifact-preservation-owner-private-v1.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    archive = Path(
        ledger["preparations"]["prepare-one"]["registrations"]["artifact-one"]["archive_locator"]
    )
    archive.write_bytes(b"changed after preservation")
    assert not edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-one",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=receipts,
    )

    ledger_path.write_text("{}", encoding="utf-8")
    ledger_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    assert not edge.verify_public_receipt_set(
        work_item_id="work-one",
        preparation_id="prepare-one",
        attachment_id="attachment-one",
        attachment_generation=3,
        public_receipts=receipts,
    )
