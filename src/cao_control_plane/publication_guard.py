"""Check publication bytes at native Git and Claude tool boundaries.

No network, model call, persistent copy, approval or rewrite is performed here.
Private reports and diagnostic reads are not publication. This is an accidental
disclosure check on supported Git/GH/MCP writes, not a shell/network sandbox.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import unicodedata
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_PREFIX = "CAO_PUBLICATION_"
_HOOKS = Path(__file__).with_name("runtime_hooks") / "git"
_BIN = _HOOKS.parent / "bin"
_DENIED = (
    "Publication blocked: the proposed shared text contains private execution details. "
    "Keep original evidence private, use a neutral explanation, and correct the unpublished "
    "content before continuing the same authorized task. Do not disable these checks. "
    "No additional approval is required."
)
_UNREADABLE = (
    "Publication blocked: its exact text could not be checked. Materialize the proposed "
    "text in a UTF-8 file, then use an explicit body/title or body-file. "
    "This call was not submitted; do not retry an earlier unknown external operation."
)


class PublicationRejected(ValueError):
    pass


def _check(text: str, terms: Sequence[str]) -> None:
    value = unicodedata.normalize("NFKC", text).casefold()
    for term in terms:
        words = unicodedata.normalize("NFKC", term).casefold().split()
        pattern = r"[\s_-]+".join(re.escape(word) for word in words)
        if re.search(r"(?<![a-z0-9])" + pattern + r"(?![a-z0-9])", value):
            raise PublicationRejected(_DENIED)


def claude_launch_policy(
    terms: tuple[str, ...], environment: Mapping[str, str]
) -> tuple[dict[str, Any], dict[str, str]]:
    env = dict(environment)
    if not terms:
        return {}, env
    if any(not path.is_file() or not os.access(path, os.X_OK) for path in _HOOKS.iterdir()) or not all(
        (_HOOKS / name).is_file() for name in ("pre-commit", "commit-msg", "pre-push")
    ):
        raise ValueError("publication hook installation is unavailable")
    count = int(env.get("GIT_CONFIG_COUNT", "0"))
    if count < 0:
        raise ValueError("invalid inherited Git configuration count")
    env.update({
        _PREFIX + "TERMS": json.dumps(terms),
        _PREFIX + "PYTHON": sys.executable,
        _PREFIX + "GIT_SLOT": str(count),
        "GIT_CONFIG_COUNT": str(count + 1),
        f"GIT_CONFIG_KEY_{count}": "core.hooksPath",
        f"GIT_CONFIG_VALUE_{count}": str(_HOOKS),
    })
    gh = shutil.which("gh", path=env.get("PATH", os.defpath))
    if gh:
        if Path(gh).resolve() == (_BIN / "gh").resolve() or not os.access(_BIN / "gh", os.X_OK):
            raise ValueError("publication command installation is unavailable")
        env[_PREFIX + "GH"] = gh
        env["PATH"] = str(_BIN) + os.pathsep + env.get("PATH", os.defpath)
    handler = {
        "type": "command",
        "command": 'exec "$CAO_PUBLICATION_PYTHON" -m cao_control_plane.publication_guard claude',
        "timeout": 15,
        "onFailure": "block",
    }
    return {"hooks": {"PreToolUse": [
        {"matcher": "Bash", "hooks": [{**handler, "if": "Bash(git *)"}]},
        {"matcher": "mcp__.*(github|slack).*", "hooks": [handler]},
    ]}}, env


def _git(*args: str, env: Mapping[str, str] | None = None) -> str:
    result = subprocess.run(
        ["git", *args], env=env, capture_output=True, check=False, timeout=10
    )
    if result.returncode:
        raise PublicationRejected(_UNREADABLE)
    return result.stdout.decode("utf-8", errors="strict")


def _patch(text: str, terms: Sequence[str]) -> None:
    # Old/deleted lines remain evidence and must not prevent their removal.
    _check("\n".join(line for line in text.splitlines() if line.startswith("+")), terms)


def _previous_hook(name: str, args: list[str], stdin: str) -> int:
    env = dict(os.environ)
    slot = int(env[_PREFIX + "GIT_SLOT"])
    env["GIT_CONFIG_COUNT"] = str(slot)
    env.pop(f"GIT_CONFIG_KEY_{slot}", None)
    env.pop(f"GIT_CONFIG_VALUE_{slot}", None)
    configured = subprocess.run(
        ["git", "config", "--get", "core.hooksPath"], env=env,
        capture_output=True, text=True, check=False, timeout=10,
    )
    if configured.returncode not in (0, 1):
        raise PublicationRejected(_UNREADABLE)
    directory = configured.stdout.strip() if configured.returncode == 0 else _git(
        "rev-parse", "--git-path", "hooks", env=env
    ).strip()
    path = Path(directory) / name
    if path.resolve() == (_HOOKS / name).resolve():
        raise PublicationRejected(_UNREADABLE)
    if not path.is_file() or not os.access(path, os.X_OK):
        return 0
    return subprocess.run([str(path.absolute()), *args], input=stdin, text=True, check=False).returncode


def git_hook(name: str, args: list[str], stdin: str, terms: Sequence[str]) -> int:
    options = ("--no-ext-diff", "--no-textconv", "--unified=0", "--no-renames", "--diff-filter=ACMR")
    if name == "pre-commit":
        _patch(_git("diff", "--cached", *options), terms)
    elif name == "commit-msg":
        _check(Path(args[0]).read_text(encoding="utf-8"), terms)
    elif name == "pre-push":
        for line in stdin.splitlines():
            local_ref, local, remote_ref, remote = line.split()
            if set(local) == {"0"}:  # Deleting a remote ref publishes no bytes.
                continue
            _check(local_ref + "\n" + remote_ref, terms)
            if not all(re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", sha) for sha in (local, remote)):
                raise PublicationRejected(_UNREADABLE)
            if _git("cat-file", "-t", local).strip() == "tag":
                _check(_git("cat-file", "tag", local), terms)
            exclusions = ["--not", "--remotes"] if set(remote) == {"0"} else ["^" + remote]
            for commit in _git("rev-list", local, *exclusions).splitlines():
                _check(_git("show", "--no-patch", "--format=%B", commit), terms)
                _patch(_git("show", "--format=", "--root", *options, commit), terms)
    return _previous_hook(name, args, stdin)


def _read_body(value: str, cwd: Path, stdin: bytes | None) -> str:
    if value == "-":
        if stdin is None:
            raise PublicationRejected(_UNREADABLE)
        return stdin.decode("utf-8")
    path = Path(value)
    return (path if path.is_absolute() else cwd / path).read_text(encoding="utf-8")


def gh_publication(
    argv: list[str], cwd: Path, terms: Sequence[str], stdin: bytes | None = None,
) -> tuple[list[str], bytes | None]:
    tokens = ["gh", *argv]
    if len(tokens) < 3:
        return argv, stdin
    writes = {"create", "edit", "comment", "review", "merge", "upload", "close", "reopen"}
    if tokens[1] != "api" and tokens[2] not in writes:
        return argv, stdin
    if tokens[1] not in {"api", "pr", "issue"}:
        raise PublicationRejected(_UNREADABLE)
    content = {"--body", "-b", "--title", "-t", "--subject", "--comment"}
    api = tokens[1] == "api"
    if api and any(tokens[i:i + 2] in (["--method", "GET"], ["-X", "GET"]) for i in range(len(tokens))):
        return argv, stdin
    files = {"--input"} if api else {"--body-file", "-F"}
    fields = {"--field", "--raw-field", "-f", "-F"} if api else set()
    checked: set[str] = set()
    forwarded_stdin = stdin
    index = 2
    while index < len(tokens):
        token = tokens[index]
        key, equal, inline = token.partition("=")
        if key not in content | files | fields:
            index += 1
            continue
        if not equal and index + 1 >= len(tokens):
            raise PublicationRejected(_UNREADABLE)
        value = inline if equal else tokens[index + 1]
        if key in fields:
            if "=" not in value:
                raise PublicationRejected(_UNREADABLE)
            value = value.split("=", 1)[1]
            if key in {"--field", "-F"} and value.startswith("@"):
                value = _read_body(value[1:], cwd, stdin)
                field = (inline if equal else tokens[index + 1]).split("=", 1)[0]
                tokens[index] = "--raw-field" + ("=" + field + "=" + value if equal else "")
                if not equal:
                    tokens[index + 1] = field + "=" + value
        elif key in files:
            value = _read_body(value, cwd, stdin)
            replacement = "--input" if api else "--body"
            replacement_value = "-" if api else value
            tokens[index] = replacement + ("=" + replacement_value if equal else "")
            if not equal:
                tokens[index + 1] = replacement_value
            if api:
                forwarded_stdin = value.encode("utf-8")
        _check(value, terms)
        checked.add(key)
        index += 1 if equal else 2
    if not api and tokens[2] == "create" and not (
        checked & {"--title", "-t"} and checked & {"--body", "-b", "--body-file", "-F"}
    ):
        raise PublicationRejected(_UNREADABLE)
    if not api and tokens[2] == "merge" and not {"--subject", "--body"}.issubset(checked):
        raise PublicationRejected(_UNREADABLE)
    if not api and tokens[2] == "comment" and not checked and "--delete-last" not in tokens:
        raise PublicationRejected(_UNREADABLE)
    if any(t.startswith("--fill") or t in {"--editor", "--web"} for t in tokens):
        raise PublicationRejected(_UNREADABLE)
    return tokens[1:], forwarded_stdin


def claude_hook(event: Mapping[str, Any], terms: Sequence[str]) -> None:
    tool = str(event.get("tool_name", ""))
    data = event.get("tool_input", {})
    if not isinstance(data, Mapping):
        raise PublicationRejected(_UNREADABLE)
    if tool == "Bash":
        command = str(data.get("command", ""))
        # Native conditional hooks also run when their command filter is uncertain.
        # Git/GH check publication bytes; only possible Git overrides need this scan.
        unquoted = re.sub(r"['\"\\]", "", command)
        if not any(value in unquoted for value in ("--no-verify", "core.hooksPath=", "GIT_CONFIG_")):
            return
        cwd = Path(str(event.get("cwd", ".")))
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|\n")
        lexer.whitespace = " \t\r"
        tokens = list(lexer)
        start = 0
        for end in range(len(tokens) + 1):
            if end != len(tokens) and tokens[end] not in {";", "&&", "||", "|", "\n"}:
                continue
            part = tokens[start:end]
            if part and part[0] == "cd" and len(part) in (2, 3):
                cwd = (cwd / part[-1]).resolve()
            elif part:
                while part and (part[0] in {"env", "command"} or re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", part[0])):
                    if part[0].startswith("GIT_CONFIG_"):
                        raise PublicationRejected(_DENIED)
                    part = part[1:]
                if part and Path(part[0]).name == "git" and any(
                    t == "--no-verify" or "core.hooksPath=" in t for t in part
                ):
                    raise PublicationRejected(_DENIED)
            start = end + 1
    elif re.search(r"github|slack", tool, re.I) and re.search(
        r"(?:^|_)(create|update|edit|post|send|reply|add|upload|put|patch|commit)(?:_|$)", tool, re.I
    ):
        _check(json.dumps(data, ensure_ascii=False), terms)


def main() -> int:
    try:
        terms = json.loads(os.environ[_PREFIX + "TERMS"])
        if not isinstance(terms, list) or not terms or not all(isinstance(t, str) and t for t in terms):
            raise ValueError("invalid policy")
        if sys.argv[1] == "git":
            name = sys.argv[2]
            stdin_hooks = {"pre-push", "pre-receive", "post-receive", "post-rewrite", "proc-receive", "reference-transaction"}
            return git_hook(name, sys.argv[3:], sys.stdin.read() if name in stdin_hooks else "", terms)
        if sys.argv[1] == "gh":
            argv = sys.argv[2:]
            reads_stdin = any(
                (v in {"--body-file", "--input"} and argv[i + 1:i + 2] == ["-"])
                or v in {"--body-file=-", "--input=-"}
                or v.endswith("=@-") for i, v in enumerate(argv)
            )
            stdin = sys.stdin.buffer.read() if reads_stdin else None
            argv, forwarded = gh_publication(argv, Path.cwd(), terms, stdin)
            return subprocess.run([os.environ[_PREFIX + "GH"], *argv], input=forwarded, check=False).returncode
        event = json.load(sys.stdin)
        claude_hook(event, terms)
        return 0
    except PublicationRejected as error:
        print(str(error), file=sys.stderr)
        return 2
    except (OSError, ValueError, KeyError, IndexError, subprocess.SubprocessError):
        print(_UNREADABLE, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
