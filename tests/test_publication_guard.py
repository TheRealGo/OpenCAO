from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from cao_control_plane.config import Settings
from cao_control_plane.publication_guard import (
    PublicationRejected,
    _check,
    claude_hook,
    claude_launch_policy,
    gh_publication,
)

TERMS = ("PRIVATE_ROLE", "Private Execution Officer")


def git(repo: Path, *args: str, env=None, check=True):
    return subprocess.run(["git", *args], cwd=repo, env=env, capture_output=True, text=True, check=check)


@pytest.fixture
def repository(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Fixture")
    git(tmp_path, "config", "user.email", "fixture@example.invalid")
    (tmp_path / "NOTICE.md").write_text("General explanation.\n")
    git(tmp_path, "add", "NOTICE.md")
    git(tmp_path, "commit", "-qm", "Initial")
    return tmp_path


def guarded_env():
    return claude_launch_policy(TERMS, os.environ)[1]


def test_added_path_rejected_but_old_text_removal_and_private_evidence_allowed(repository):
    repo = repository
    before = git(repo, "rev-parse", "HEAD").stdout
    (repo / "NOTICE.md").write_text("Example: .tmp/private_role-artifacts/review/\n")
    git(repo, "add", "NOTICE.md")
    result = git(repo, "commit", "-qm", "Explain exclusion", env=guarded_env(), check=False)
    assert result.returncode != 0 and "Publication blocked" in result.stderr
    assert "private_role-artifacts" not in result.stderr
    assert git(repo, "rev-parse", "HEAD").stdout == before
    git(repo, "commit", "-qm", "Old fixture record")
    (repo / "NOTICE.md").write_text("General reason without a local example.\n")
    git(repo, "add", "NOTICE.md")
    git(repo, "commit", "-qm", "Clarify explanation", env=guarded_env())
    (repo / "private_role-evidence.txt").write_text("Original private evidence")
    git(repo, "status", "--porcelain", env=guarded_env())


def test_commit_message_rejected_without_changing_head(repository):
    before = git(repository, "rev-parse", "HEAD").stdout
    r = git(repository, "commit", "--allow-empty", "-qm", "PRIVATE_ROLE approved", env=guarded_env(), check=False)
    assert r.returncode != 0 and git(repository, "rev-parse", "HEAD").stdout == before


@pytest.mark.parametrize("command", [
    'git commit --no-verify -m "Skip"',
    'git -c core.hooksPath=/dev/null commit -m "Skip"',
    'GIT_CONFIG_COUNT=0 git commit -m "Skip"',
    'git commit --no-"ver"ify -m "Skip"',
    'git -c core.hooks"Path"=/dev/null commit -m "Skip"',
    'GIT_CONFIG_"COUNT"=0 git commit -m "Skip"',
])
def test_normal_shell_shortcuts_cannot_skip_the_publication_check(repository, command):
    with pytest.raises(PublicationRejected):
        claude_hook({"tool_name": "Bash", "cwd": str(repository), "tool_input": {"command": command}}, TERMS)


@pytest.mark.parametrize("body", [
    "A tool's local note.",
    "git status isn't publication.",
    "PRIVATE_ROLE's original private evidence.",
])
def test_local_heredoc_preparation_is_not_publication(tmp_path, body):
    command = "cat > local.txt <<'LOCAL_TEXT'\n" + body + "\nLOCAL_TEXT\n"
    result = subprocess.run(
        [sys.executable, "-m", "cao_control_plane.publication_guard", "claude"],
        input=json.dumps({"tool_name": "Bash", "cwd": str(tmp_path),
                          "tool_input": {"command": command}}),
        env=guarded_env(), capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    subprocess.run(["bash", "-c", command], cwd=tmp_path, check=True)
    assert (tmp_path / "local.txt").read_text() == body + "\n"


def test_push_checks_intermediate_history_even_after_tip_removal(repository, tmp_path):
    repo = repository
    remote = tmp_path / "remote.git"
    git(repo, "init", "-q", "--bare", str(remote))
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-qu", "origin", "HEAD")
    before = git(repo, "ls-remote", "origin", "HEAD").stdout
    (repo / "NOTICE.md").write_text("PRIVATE_ROLE example\n")
    git(repo, "add", "NOTICE.md")
    git(repo, "commit", "-qm", "Intermediate fixture")
    (repo / "NOTICE.md").write_text("Clean current text\n")
    git(repo, "add", "NOTICE.md")
    git(repo, "commit", "-qm", "Remove example")
    r = git(repo, "push", "-q", "origin", "HEAD", env=guarded_env(), check=False)
    assert r.returncode != 0 and "Publication blocked" in r.stderr
    assert git(repo, "ls-remote", "origin", "HEAD").stdout == before


def test_original_git_hook_and_inherited_config_preserved(repository):
    hooks = repository / "existing-hooks"
    hooks.mkdir()
    h = hooks / "pre-commit"
    h.write_text("#!/bin/sh\nprintf ran > original-hook-ran\n")
    h.chmod(0o700)
    env = dict(os.environ, GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="core.hooksPath", GIT_CONFIG_VALUE_0=str(hooks))
    env = claude_launch_policy(TERMS, env)[1]
    git(repository, "commit", "--allow-empty", "-qm", "Valid checkpoint", env=env)
    assert (repository / "original-hook-ran").read_text() == "ran"


def test_other_native_git_hooks_are_forwarded_with_original_exit_status(repository):
    hooks = repository / ".git/hooks"
    h = hooks / "post-commit"
    h.write_text("#!/bin/sh\nprintf committed > original-post-commit\n")
    h.chmod(0o700)
    git(repository, "commit", "--allow-empty", "-qm", "Valid", env=guarded_env())
    assert (repository / "original-post-commit").read_text() == "committed"


@pytest.mark.parametrize("command", [
    'gh pr comment 1 --body "Example: .tmp/private_role-artifacts/review/"',
    'cd nested && gh pr comment 1 --body-file body.md',
    'gh api repos/example/project/issues/1/comments -f "body=PRIVATE_ROLE reviewed"',
    'gh pr merge 1 --squash --subject "PRIVATE_ROLE approval" --body "Result"',
    'gh pr create --title "Clean" --fill',
    'gh pr comment 1 --body-file -',
])
def test_public_text_rejected_before_submission(tmp_path, command):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/body.md").write_text("PRIVATE_ROLE reviewed\n")
    argv = shlex.split(command)
    cwd = tmp_path
    if argv[:2] == ["cd", "nested"]:
        cwd = tmp_path / "nested"
        argv = argv[3:]
    with pytest.raises(PublicationRejected):
        gh_publication(argv[1:], cwd, TERMS)


@pytest.mark.parametrize("command", [
    'git check-ignore .tmp/private_role-artifacts/review/',
    'gh pr view 1 --json body',
    'gh pr comment 1 --body "The exclusion also affects unrelated review folders."',
    'gh pr review 1 --approve',
    'gh pr close 1',
    'gh pr comment 1 --delete-last --yes',
    'gh api repos/example/project/issues/1/comments -X GET',
])
def test_diagnostics_clean_text_and_textless_operations_available(tmp_path, command):
    if command.startswith("gh "):
        gh_publication(shlex.split(command)[1:], tmp_path, TERMS)
    else:
        claude_hook({"tool_name": "Bash", "cwd": str(tmp_path), "tool_input": {"command": command}}, TERMS)


def test_body_file_contents_checked_without_publishing_private_filename(tmp_path):
    p = tmp_path / "private_role-evidence.md"
    p.write_text("A general reason and the current behavior.\n")
    argv = ["pr", "comment", "1", "--body-file", str(p)]
    forwarded, _ = gh_publication(argv, tmp_path, TERMS)
    assert forwarded == ["pr", "comment", "1", "--body", p.read_text()]
    p.write_text(".tmp/private_role-artifacts/review/\n")
    with pytest.raises(PublicationRejected):
        gh_publication(argv, tmp_path, TERMS)


def test_mcp_write_checked_and_reports_reads_not_blocked():
    with pytest.raises(PublicationRejected):
        claude_hook({"tool_name": "mcp__github__create_pull_request", "tool_input": {"body": "PRIVATE_ROLE approved"}}, TERMS)
    claude_hook({"tool_name": "mcp__github__get_issue", "tool_input": {"query": "PRIVATE_ROLE"}}, TERMS)
    claude_hook({"tool_name": "mcp__control_plane__report", "tool_input": {"summary": "PRIVATE_ROLE evidence"}}, TERMS)


def test_name_boundaries_and_unicode_normalization():
    _check("0123456789abcdef pprivate_roleholder", TERMS)
    fullwidth = "".join(chr(ord(c) + 0xFEE0) for c in "PRIVATE_ROLE")
    for text in (fullwidth, "private_role-artifacts", "private-execution-officer"):
        with pytest.raises(PublicationRejected):
            _check(text, TERMS)


def test_policy_session_scope_and_native_failure_block():
    settings, env = claude_launch_policy(TERMS, {})
    h = settings["hooks"]["PreToolUse"][0]["hooks"][0]
    assert h["onFailure"] == "block" and h["if"] == "Bash(git *)"
    assert json.loads(env["CAO_PUBLICATION_TERMS"]) == list(TERMS)
    assert claude_launch_policy((), {"ORIGINAL": "value"}) == ({}, {"ORIGINAL": "value"})


def test_invalid_input_returns_block_without_echoing_raw_data():
    r = subprocess.run([sys.executable, "-m", "cao_control_plane.publication_guard", "claude"],
        input='{"secret_raw_marker":', text=True, capture_output=True, env=guarded_env(), check=False)
    assert r.returncode == 2 and "secret_raw_marker" not in r.stderr


def test_owner_local_config_roundtrip(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text('[runtime]\nworker_publication_forbidden_terms = ["PRIVATE_ROLE"]\n')
    assert Settings.load(p, prepare_directories=False).worker_publication_forbidden_terms == ("PRIVATE_ROLE",)
    with pytest.raises(ValueError):
        Settings(worker_publication_forbidden_terms=("",))


def test_release_identity_includes_extensionless_native_hooks(tmp_path):
    from cao_control_plane.release_identity import package_content_digest

    hooks = tmp_path / "runtime_hooks/git"
    hooks.mkdir(parents=True)
    p = hooks / "pre-commit"
    p.write_text("#!/bin/sh\nexit 0\n")
    first = package_content_digest(tmp_path)
    p.write_text("#!/bin/sh\nexit 2\n")
    assert first != package_content_digest(tmp_path)


def test_python_subprocess_uses_the_same_gh_payload_check(tmp_path):
    tools = tmp_path / "tools"
    tools.mkdir()
    sink = tmp_path / "sink.json"
    gh = tools / "gh"
    gh.write_text("#!/usr/bin/env python3\nimport json,sys\nfrom pathlib import Path\nPath(" + repr(str(sink)) + ").write_text(json.dumps(sys.argv[1:]))\n")
    gh.chmod(0o700)
    env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ.get("PATH", ""))
    env = claude_launch_policy(TERMS, env)[1]
    script = "import subprocess,sys;sys.exit(subprocess.call(['gh','pr','comment','1','--body',sys.argv[1]]))"
    blocked = subprocess.run([sys.executable, "-c", script, "PRIVATE_ROLE reviewed"], env=env, capture_output=True, text=True, check=False)
    assert blocked.returncode == 2 and not sink.exists()
    subprocess.run([sys.executable, "-c", script, "A general explanation."], env=env, check=True)
    assert json.loads(sink.read_text())[-1] == "A general explanation."


def test_api_raw_fields_and_stdin_check_the_bytes_that_are_actually_forwarded(tmp_path):
    p = tmp_path / "private_role-evidence.txt"
    p.write_text("Clean bytes")
    with pytest.raises(PublicationRejected):
        gh_publication(["api", "endpoint", "-f", "body=@" + str(p)], tmp_path, TERMS)
    args, data = gh_publication(["api", "endpoint", "--input", "-"], tmp_path, TERMS, b'{"body":"Clean"}')
    assert args == ["api", "endpoint", "--input", "-"] and data == b'{"body":"Clean"}'
    with pytest.raises(PublicationRejected):
        gh_publication(["pr", "comment", "1", "--body-file", "-"], tmp_path, TERMS, b"PRIVATE_ROLE reviewed")


def test_uninspected_upload_route_is_not_treated_as_checked_text(tmp_path):
    with pytest.raises(PublicationRejected):
        gh_publication(["release", "upload", "v1", "uninspected.bin"], tmp_path, TERMS)
