from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import zipfile
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "publication_check", Path(__file__).resolve().parents[1] / "tools" / "check_publication.py"
)
assert _SPEC and _SPEC.loader
publication = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publication)


@pytest.fixture
def source_repo(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "README.md").write_text("Public example.\n")
    (tmp_path / ".gitignore").write_text(".cao/\n.venv/\n*.log\n/PUBLICATION-MANIFEST.json\n")
    return tmp_path


def test_archive_excludes_private_ignored_state_and_seals_every_source(source_repo, monkeypatch):
    (source_repo / ".cao").mkdir()
    (source_repo / ".cao" / "credentials.json").write_text("private runtime fixture")
    (source_repo / "run.log").write_text("private execution fixture")
    archive = source_repo / "export.zip"
    monkeypatch.setattr(publication, "ROOT", source_repo)
    assert publication.main(["--archive", str(archive)]) == 0
    with zipfile.ZipFile(archive) as package:
        assert set(package.namelist()) == {
            "OpenCAO/README.md",
            "OpenCAO/.gitignore",
            "OpenCAO/PUBLICATION-MANIFEST.json",
        }
        manifest = json.loads(package.read("OpenCAO/PUBLICATION-MANIFEST.json"))
        assert all(
            hashlib.sha256(package.read(f"OpenCAO/{name}")).hexdigest() == digest
            for name, digest in manifest.items()
        )
        extracted = source_repo.parent / (source_repo.name + "-extracted")
        package.extractall(extracted)
    restored = extracted / "OpenCAO"
    subprocess.run(["git", "init", "-q", str(restored)], check=True)
    payloads, errors = publication.checked_source(restored)
    assert not errors
    assert set(payloads) == set(manifest)


def test_publication_rejects_credentials_even_when_force_staged_without_echoing_values(
    source_repo, monkeypatch, capsys
):
    secret = "ghp_" + "A" * 36
    (source_repo / "README.md").write_text("accidental credential: " + secret)
    private_log = source_repo / "run.log"
    private_log.write_text("private log")
    subprocess.run(["git", "add", "--force", "run.log"], cwd=source_repo, check=True)
    monkeypatch.setattr(publication, "ROOT", source_repo)
    archive = source_repo / "blocked.zip"
    assert publication.main(["--archive", str(archive)]) == 1
    output = capsys.readouterr().err
    assert "possible GitHub credential" in output
    assert "run.log: not a distributable source file" in output
    assert secret not in output
    assert not archive.exists()


def test_publication_rejects_external_symlinks_and_preserves_existing_archive(
    source_repo, monkeypatch
):
    (source_repo / "docs").mkdir()
    (source_repo / "docs" / "private.md").symlink_to(source_repo / "README.md")
    _, errors = publication.checked_source(source_repo)
    assert errors == ["docs/private.md: not a distributable source file"]
    (source_repo / "docs" / "private.md").unlink()
    archive = source_repo.parent / "existing.zip"
    archive.write_bytes(b"existing artifact")
    monkeypatch.setattr(publication, "ROOT", source_repo)
    with pytest.raises(FileExistsError):
        publication.main(["--archive", str(archive)])
    assert archive.read_bytes() == b"existing artifact"


def test_publication_rejects_tracked_files_beneath_a_replaced_directory(source_repo):
    docs = source_repo / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text("public guide")
    subprocess.run(["git", "add", "docs/guide.md"], cwd=source_repo, check=True)
    outside = source_repo.parent / (source_repo.name + "-outside")
    docs.rename(outside)
    docs.symlink_to(outside, target_is_directory=True)
    payloads, errors = publication.checked_source(source_repo)
    assert "docs/guide.md: not a distributable source file" in errors
    assert "docs/guide.md" not in payloads
