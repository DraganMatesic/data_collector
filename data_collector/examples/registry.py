"""Discovery and selection helpers for runnable examples."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ExampleEntry:
    """Single runnable example discovered under `data_collector/examples`."""

    ref: str
    group: str
    module: str
    path: Path
    title: str


def _examples_root() -> Path:
    """Return the package-local examples root directory."""
    return Path(__file__).resolve().parent


def _is_candidate(path: Path) -> bool:
    """Return True when file path is eligible for runnable-example discovery."""
    if not path.is_file() or path.suffix != ".py":
        return False
    if "__pycache__" in path.parts:
        return False
    if path.name in {"__init__.py", "__main__.py"}:
        return False
    return not path.name.startswith("_")


def _module_from_path(path: Path, root: Path) -> str:
    """Build import path for a discovered example module."""
    rel_no_suffix = path.relative_to(root).with_suffix("")
    dotted_rel = ".".join(rel_no_suffix.parts)
    return f"data_collector.examples.{dotted_rel}"


def _ref_from_path(path: Path, root: Path) -> str:
    """Build CLI selector ref (`group/example_name`) from file path."""
    rel_no_suffix = path.relative_to(root).with_suffix("")
    return "/".join(rel_no_suffix.parts)


def _extract_title(path: Path) -> str:
    """Extract title from module docstring first line, fallback to file stem."""
    try:
        source = path.read_text(encoding="utf-8")
        module_ast = ast.parse(source)
        docstring = ast.get_docstring(module_ast, clean=True)
        if docstring:
            first_line = docstring.splitlines()[0].strip()
            if first_line:
                return first_line
    except (OSError, SyntaxError, UnicodeDecodeError):
        pass
    return path.stem


def _has_top_level_main(path: Path) -> bool:
    """Return True when module defines top-level sync or async `main`."""
    try:
        source = path.read_text(encoding="utf-8")
        module_ast = ast.parse(source)
    except (OSError, SyntaxError, UnicodeDecodeError):
        return False

    for node in module_ast.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "main":
            return True
    return False


def discover_examples(root: Path | None = None) -> list[ExampleEntry]:
    """Discover all runnable examples under the configured examples root."""
    root_path = (root if root is not None else _examples_root()).resolve()
    entries: list[ExampleEntry] = []

    for path in root_path.rglob("*.py"):
        if not _is_candidate(path):
            continue
        if not _has_top_level_main(path):
            continue

        ref = _ref_from_path(path, root_path)
        rel_parent = path.relative_to(root_path).parent
        group = "/".join(rel_parent.parts) if rel_parent.parts else "."
        entries.append(
            ExampleEntry(
                ref=ref,
                group=group,
                module=_module_from_path(path, root_path),
                path=path,
                title=_extract_title(path),
            )
        )

    return sorted(entries, key=lambda entry: (entry.group, entry.ref))


def filter_by_scope(entries: list[ExampleEntry], scope: str | None) -> list[ExampleEntry]:
    """Filter discovered examples by group/ref prefix scope."""
    if scope is None:
        return entries

    normalized = scope.strip().replace("\\", "/").strip("/")
    if not normalized:
        return entries

    selected = [
        entry
        for entry in entries
        if (
            entry.group == normalized
            or entry.group.startswith(f"{normalized}/")
            or entry.ref.startswith(f"{normalized}/")
        )
    ]
    return sorted(selected, key=lambda entry: (entry.group, entry.ref))


def resolve_target(entries: list[ExampleEntry], target: str) -> list[ExampleEntry]:
    """Resolve run target into a deterministic list of selected entries."""
    normalized = target.strip().replace("\\", "/").strip("/")
    if not normalized:
        return []

    if normalized == "all":
        return list(entries)

    if normalized.endswith("/all"):
        scope = normalized[: -len("/all")]
        return filter_by_scope(entries, scope)

    exact = [entry for entry in entries if entry.ref == normalized]
    return sorted(exact, key=lambda entry: (entry.group, entry.ref))
