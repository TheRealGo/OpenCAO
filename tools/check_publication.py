"""Check and optionally archive only the reviewed source files of this checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = frozenset(
    {
        ".gitignore",
        "AGENTS.md",
        "LICENSE",
        "README.md",
        "pyproject.toml",
        "config.example.toml",
        "uv.lock",
    }
)
SOURCE_DIRS = frozenset({"src", "tests", "docs", "tools", ".github"})
SOURCE_EXTENSIONS = frozenset(
    {".py", ".js", ".mjs", ".css", ".html", ".md", ".json", ".toml", ".yml", ".yaml", ".typed"}
)
FORBIDDEN_NAME = re.compile(
    r"(?i)(?:^\.env(?:\.|$)|\.sqlite(?:3)?(?:-|$)|\.db$|\.log$|\.pid$|"
    r"access[-_]token|credentials\.json$|tunnel[-_]token|bootstrap-tokens|"
    r"managed-worker-(?:workspaces|dynamic-policy))"
)
PATTERNS = {
    "private key": re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"
        r"[\s\S]{40,}?-----END (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"
    ),
    "GitHub credential": re.compile(
        r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b"
    ),
    "provider credential": re.compile(r"\bsk-(?:proj-|ant-)?[A-Za-z0-9_-]{24,}"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "JWT": re.compile(r"\beyJ[A-Za-z0-9_-]{12,}\.eyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}"),
    "CAO credential": re.compile(
        r"\bcao\.(?:prn|rtc|csc|cab)_[A-Za-z0-9]{12,}\.[A-Za-z0-9_-]{24,}"
    ),
    "Slack credential": re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{16,}"),
    "owner home path": re.compile(
        r"/(?:Users|home)/(?!owner(?:/|\b)|example(?:/|\b)|alice(?:/|\b))[^\s\"'/]+"
    ),
    "personal email": re.compile(
        r"\b[A-Za-z0-9._%+-]+@(?![A-Za-z0-9.-]*example\.(?:com|test|invalid)\b)[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
    ),
}


def source_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return sorted(set(result.stdout.decode().strip("\0").split("\0")) - {""})


def checked_source(root: Path) -> tuple[dict[str, bytes], list[str]]:
    payloads: dict[str, bytes] = {}
    errors: list[str] = []
    for name in source_files(root):
        relative = Path(name)
        path = root / relative
        if (
            path.is_symlink()
            or path.resolve() != path.absolute()
            or not path.is_file()
            or ".." in relative.parts
            or any(part.startswith(".") for part in relative.parts[1:])
            or FORBIDDEN_NAME.search(relative.name)
            or (
                name not in ROOT_FILES
                and (
                    relative.parts[0] not in SOURCE_DIRS or relative.suffix not in SOURCE_EXTENSIONS
                )
            )
        ):
            errors.append(f"{name}: not a distributable source file")
            continue
        data = path.read_bytes()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            errors.append(f"{name}: unexpected binary content")
            continue
        for label, pattern in PATTERNS.items():
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                errors.append(f"{name}:{line}: possible {label}; inspect locally")
        payloads[name] = data
    if not payloads:
        errors.append("no distributable source files found")
    return payloads, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="create a new source zip after checks pass")
    args = parser.parse_args(argv)
    payloads, errors = checked_source(ROOT)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    manifest = {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()}
    if args.archive:
        with zipfile.ZipFile(args.archive, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data in payloads.items():
                archive.writestr(f"OpenCAO/{name}", data)
            archive.writestr("OpenCAO/PUBLICATION-MANIFEST.json", json.dumps(manifest, indent=2) + "\n")
    print(
        f"Publication source check passed: {len(payloads)} files; no runtime or Git data included."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
