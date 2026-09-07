"""Reject orphaned repository surfaces and known resource-ownership mistakes."""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "src" / "cao_control_plane"
DOCS = ROOT / "docs"

MARKDOWN_LINK = re.compile(r"\[[^]]*\]\(([^)]+)\)")
DEPLOYMENT_MANIFESTS = (
    "Dockerfile",
    "compose.yaml",
    "docker-compose.yml",
    "fly.toml",
    "Procfile",
    "wrangler.toml",
)


def _runtime_modules() -> dict[str, Path]:
    return {path.stem: path for path in PACKAGE.glob("*.py")}


def _local_imports(path: Path) -> set[str]:
    imported: set[str] = set()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 1:
            if node.module:
                imported.add(node.module.split(".", 1)[0])
            else:
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("cao_control_plane."):
                    imported.add(alias.name.split(".", 2)[1])
    return imported


def _entry_points() -> dict[str, str]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return dict(project.get("project", {}).get("scripts", {}))


def _python_surface_errors() -> list[str]:
    modules = _runtime_modules()
    graph = {name: _local_imports(path) for name, path in modules.items()}
    roots = {"__main__"}
    errors: list[str] = []
    for command, target in sorted(_entry_points().items()):
        module_name, separator, attribute = target.partition(":")
        if not separator or not attribute:
            errors.append(f"invalid console entry point {command}: {target}")
            continue
        module = module_name.removeprefix("cao_control_plane.").split(".", 1)[0]
        path = modules.get(module)
        if path is None:
            errors.append(f"console entry point {command} references missing module {module_name}")
            continue
        roots.add(module)
        definitions = {
            node.name
            for node in ast.parse(path.read_text(encoding="utf-8"), filename=str(path)).body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        }
        if attribute not in definitions:
            errors.append(
                f"console entry point {command} references missing attribute {target}"
            )

    reachable: set[str] = set()
    queue = deque(roots)
    while queue:
        module = queue.popleft()
        if module in reachable or module not in modules:
            continue
        reachable.add(module)
        queue.extend(graph[module])
    unreachable = sorted(set(modules) - reachable - {"__init__"})
    if unreachable:
        errors.append("runtime modules unreachable from an entry point: " + ", ".join(unreachable))
    return errors


def _markdown_surface_errors() -> list[str]:
    markdown = [ROOT / "README.md", *sorted(DOCS.glob("*.md"))]
    known = {path.resolve(): path for path in markdown}
    inbound: dict[Path, set[Path]] = defaultdict(set)
    errors: list[str] = []
    for source in markdown:
        for raw_target in MARKDOWN_LINK.findall(source.read_text(encoding="utf-8")):
            target = raw_target.split("#", 1)[0]
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            resolved = (source.parent / target).resolve()
            if resolved.suffix == ".md" and not resolved.exists():
                errors.append(f"broken Markdown link: {source.relative_to(ROOT)} -> {target}")
            if resolved in known and resolved != source.resolve():
                inbound[resolved].add(source.resolve())
    orphans = [path.relative_to(ROOT) for path in sorted(DOCS.glob("*.md")) if not inbound[path.resolve()]]
    if orphans:
        errors.append("documentation without an inbound link: " + ", ".join(map(str, orphans)))
    return errors


def _repository_surface_errors() -> list[str]:
    searchable = [ROOT / "README.md", *sorted(DOCS.glob("*.md"))]
    searchable.extend(sorted((ROOT / ".github" / "workflows").glob("*.yml")))
    references = "\n".join(path.read_text(encoding="utf-8") for path in searchable)
    errors: list[str] = []
    for name in DEPLOYMENT_MANIFESTS:
        if (ROOT / name).exists() and name not in references:
            errors.append(f"unreferenced deployment manifest: {name}")
    for directory in (ROOT / "scripts", ROOT / "tools"):
        if not directory.exists():
            continue
        for path in sorted(directory.iterdir()):
            if not path.is_file() or path.name == "__init__.py" or path == Path(__file__):
                continue
            relative = str(path.relative_to(ROOT))
            if relative not in references:
                errors.append(f"unreferenced repository script: {relative}")
    return errors


def _sqlite_resource_errors(paths: list[Path] | None = None) -> list[str]:
    """A raw SQLite transaction context does not close its connection."""
    if paths is None:
        paths = [
            path
            for directory in (ROOT / "src", ROOT / "tests", ROOT / "tools")
            for path in sorted(directory.rglob("*.py"))
        ]
    errors: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        modules: set[str] = set()
        connect_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(
                    alias.asname or alias.name for alias in node.names if alias.name == "sqlite3"
                )
            elif isinstance(node, ast.ImportFrom) and node.module == "sqlite3":
                connect_names.update(
                    alias.asname or alias.name for alias in node.names if alias.name == "connect"
                )
        for node in ast.walk(tree):
            if not isinstance(node, ast.With):
                continue
            for item in node.items:
                if not isinstance(item.context_expr, ast.Call):
                    continue
                function = item.context_expr.func
                raw_connect = (
                    isinstance(function, ast.Attribute)
                    and function.attr == "connect"
                    and isinstance(function.value, ast.Name)
                    and function.value.id in modules
                ) or (isinstance(function, ast.Name) and function.id in connect_names)
                if raw_connect:
                    label = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path.name
                    errors.append(
                        f"{label}:{node.lineno}: sqlite3 transaction context does not close "
                        "the connection; use closing(connect(...)) as connection, connection"
                    )
    return errors


def main() -> int:
    errors = [
        *_python_surface_errors(),
        *_markdown_surface_errors(),
        *_repository_surface_errors(),
        *_sqlite_resource_errors(),
    ]
    if errors:
        for error in errors:
            print(f"repository-hygiene: {error}", file=sys.stderr)
        return 1
    print("repository-hygiene: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
