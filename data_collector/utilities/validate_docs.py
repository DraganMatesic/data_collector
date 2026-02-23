#!/usr/bin/env python3
"""Repository documentation validator.

Checks:
- Broken markdown links
- Broken local/cross-document anchors
- Missing local asset links
- Heading level jumps (> 1)
- Disallowed emoji/decorative glyphs
- Unlabeled non-existent repo paths
- Legacy namespace/path patterns that violate documentation contracts
"""


from __future__ import annotations

import contextlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT / "docs"
README = ROOT / "README.md"

LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$")
A_ID_RE = re.compile(r'<a\s+id="([^"]+)"\s*></a>', flags=re.IGNORECASE)
INLINE_CODE_RE = re.compile(r"`([^`]+)`")
CODE_PATH_RE = re.compile(
    r"(?:data_collector|apps|\.github|tools|docs)(?:/[A-Za-z0-9_.-]+)+\.[A-Za-z0-9]+|docker-compose\.yml"
)
DISALLOWED_GLYPH_RE = re.compile(r"[\U00002600-\U000027BF\U0001F300-\U0001FAFF]")

PLANNED_LABELS = ("planned module path:", "planned file path:")
EXTERNAL_SCHEMES = ("http://", "https://", "mailto:", "tel:")
API_APPS_ROUTE_RE = re.compile(r"/api/v\d+/apps/")

LEGACY_NAMESPACE_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "legacy_apps_module_execution",
        re.compile(r"\bpython\s+-m\s+apps\."),
        "Legacy module execution path: use `python -m data_collector.<country>.<parent>.<app>.main`.",
    ),
    (
        "legacy_apps_from_import",
        re.compile(r"\bfrom\s+apps\."),
        "Legacy import namespace: use `from data_collector.<country>.<parent>.<app> ...`.",
    ),
    (
        "legacy_apps_import",
        re.compile(r"\bimport\s+apps\."),
        "Legacy import namespace: use `import data_collector.<country>.<parent>.<app> ...`.",
    ),
    (
        "legacy_actor_path",
        re.compile(r"actor_path\s*=\s*[\"']apps\."),
        "Legacy actor module path: use `actor_path=\"data_collector...\"`.",
    ),
    (
        "legacy_apps_path_template",
        re.compile(r"\bapps/\{group\}/\{parent\}/\{app_name\}/"),
        "Legacy app path template: use `data_collector/{group}/{parent}/{app_name}/`.",
    ),
    (
        "legacy_apps_path_concrete",
        re.compile(r"\bapps/[a-z0-9_]+/[a-z0-9_]+/[a-z0-9_]+/"),
        "Legacy app path: use `data_collector/<country>/<parent>/<app>/`.",
    ),
    (
        "legacy_request_import",
        re.compile(r"\bfrom\s+data_collector\.request\s+import\b"),
        "Legacy request import: use `from data_collector.utilities.request import ...`.",
    ),
    (
        "legacy_request_main_import",
        re.compile(r"\bfrom\s+data_collector\.utilities\.request\.main\s+import\b"),
        "Legacy request import: use `from data_collector.utilities.request import ...`.",
    ),
    (
        "legacy_constants_enums_path",
        re.compile(r"\bdata_collector/constants/enums/"),
        "Enums path is locked to `data_collector/enums/...`, not `data_collector/constants/enums/...`.",
    ),
    (
        "legacy_constants_enums_import",
        re.compile(r"\bdata_collector\.constants\.enums\b"),
        "Enums import namespace is locked to `data_collector.enums...`.",
    ),
)


@dataclass
class Issue:
    code: str
    file: Path
    line: int
    message: str


def iter_target_files() -> list[Path]:
    files = sorted(DOCS_DIR.glob("*.md"))
    if README.exists():
        files.append(README)
    return files


def normalize_anchor(text: str) -> str:
    anchor = text.strip()
    anchor = re.sub(r"\s+#+\s*$", "", anchor)
    anchor = re.sub(r"[`*_]", "", anchor)
    anchor = anchor.lower()
    anchor = re.sub(r"[^a-z0-9\s-]", "", anchor)
    anchor = re.sub(r"\s+", "-", anchor)
    anchor = re.sub(r"-+", "-", anchor).strip("-")
    return anchor


def collect_anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    in_code_block = False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()

        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            continue

        if not in_code_block:
            heading_match = HEADING_RE.match(line)
            if heading_match:
                anchor = normalize_anchor(heading_match.group(2))
                if anchor:
                    anchors.add(anchor)

        for match in A_ID_RE.finditer(line):
            anchors.add(match.group(1).lower())

    return anchors


def resolve_link_path(current_file: Path, raw_target: str) -> tuple[Path, str | None]:
    if "#" in raw_target:
        path_part, anchor = raw_target.split("#", 1)
    else:
        path_part, anchor = raw_target, None

    path_part = unquote(path_part).strip()
    target_file = current_file if not path_part else (current_file.parent / path_part).resolve()
    return target_file, anchor.lower() if anchor else None


def check_links(files: list[Path], anchors_map: dict[Path, set[str]]) -> list[Issue]:
    issues: list[Issue] = []

    for file_path in files:
        lines = file_path.read_text(encoding="utf-8").splitlines()
        in_code_block = False

        for idx, raw_line in enumerate(lines, start=1):
            line = raw_line.rstrip()

            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue

            if in_code_block:
                continue

            for match in LINK_RE.finditer(line):
                target = match.group(1).strip()
                if not target:
                    continue

                lower_target = target.lower()
                if lower_target.startswith(EXTERNAL_SCHEMES):
                    continue

                if target.startswith("#"):
                    anchor = target[1:].lower()
                    if anchor and anchor not in anchors_map.get(file_path.resolve(), set()):
                        issues.append(
                            Issue(
                                code="broken_anchor",
                                file=file_path,
                                line=idx,
                                message=f"Anchor not found: {target}",
                            )
                        )
                    continue

                target_file, anchor = resolve_link_path(file_path.resolve(), target)
                if not target_file.exists():
                    suffix = Path(unquote(target.split("#", 1)[0])).suffix.lower()
                    issue_code = "missing_asset" if suffix and suffix != ".md" else "broken_link"
                    issues.append(
                        Issue(
                            code=issue_code,
                            file=file_path,
                            line=idx,
                            message=f"Target not found: {target}",
                        )
                    )
                    continue

                if anchor:
                    anchor_set = anchors_map.get(target_file.resolve(), set())
                    if anchor not in anchor_set:
                        issues.append(
                            Issue(
                                code="broken_anchor",
                                file=file_path,
                                line=idx,
                                message=f"Anchor not found in {target_file.name}: #{anchor}",
                            )
                        )

    return issues


def check_heading_jumps(files: list[Path]) -> list[Issue]:
    issues: list[Issue] = []

    for file_path in files:
        in_code_block = False
        previous_level = 0

        for idx, raw_line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.rstrip()

            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue

            if in_code_block:
                continue

            heading_match = HEADING_RE.match(line)
            if not heading_match:
                continue

            level = len(heading_match.group(1))
            if previous_level and level > previous_level + 1:
                issues.append(
                    Issue(
                        code="heading_jump",
                        file=file_path,
                        line=idx,
                        message=f"Heading level jump from H{previous_level} to H{level}",
                    )
                )
            previous_level = level

    return issues


def check_disallowed_glyphs(files: list[Path]) -> list[Issue]:
    issues: list[Issue] = []

    for file_path in files:
        for idx, raw_line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1):
            match = DISALLOWED_GLYPH_RE.search(raw_line)
            if not match:
                continue

            issues.append(
                Issue(
                    code="disallowed_glyph",
                    file=file_path,
                    line=idx,
                    message=f"Disallowed glyph detected: {match.group(0)!r}",
                )
            )

    return issues


def check_unlabeled_missing_paths(files: list[Path]) -> list[Issue]:
    issues: list[Issue] = []

    for file_path in files:
        lines = file_path.read_text(encoding="utf-8").splitlines()
        in_code_block = False

        for idx, raw_line in enumerate(lines, start=1):
            line = raw_line.rstrip()
            lowered_line = line.lower()

            if line.strip().startswith("```"):
                in_code_block = not in_code_block
                continue

            candidates: list[str] = []
            candidates.extend(match.group(1) for match in INLINE_CODE_RE.finditer(line))

            if in_code_block:
                stripped = line.strip()
                if stripped.startswith("#"):
                    candidates.append(stripped[1:].strip())

            for candidate in candidates:
                for path_match in CODE_PATH_RE.finditer(candidate):
                    repo_path = path_match.group(0)
                    resolved = (ROOT / repo_path).resolve()
                    if resolved.exists():
                        continue

                    has_planned_label = any(label in lowered_line for label in PLANNED_LABELS)
                    if has_planned_label and repo_path.lower() in lowered_line:
                        continue

                    issues.append(
                        Issue(
                            code="unlabeled_missing_path",
                            file=file_path,
                            line=idx,
                            message=f"Non-existent path must be labeled as planned: {repo_path}",
                        )
                    )

    return issues


def is_legacy_namespace_exception(rule: str, line: str) -> bool:
    lowered = line.lower()

    legacy_rules = {"legacy_apps_path_template", "legacy_apps_path_concrete"}

    if API_APPS_ROUTE_RE.search(lowered) and rule in legacy_rules:
        return True

    if "apiversion:" in lowered and "apps/v" in lowered and rule in legacy_rules:
        return True

    if "apps.py" in lowered and rule in legacy_rules:
        return True

    return "apps.app" in lowered and rule in legacy_rules


def check_legacy_namespace(files: list[Path]) -> list[Issue]:
    issues: list[Issue] = []

    for file_path in files:
        for idx, raw_line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.rstrip()

            for rule, pattern, message in LEGACY_NAMESPACE_PATTERNS:
                if not pattern.search(line):
                    continue
                if is_legacy_namespace_exception(rule, line):
                    continue

                issues.append(
                    Issue(
                        code=rule,
                        file=file_path,
                        line=idx,
                        message=message,
                    )
                )

    return issues


def main() -> int:
    with contextlib.suppress(AttributeError):
        cast(Any, sys.stdout).reconfigure(encoding="utf-8", errors="replace")

    files = iter_target_files()
    if not files:
        print("No markdown files found for validation.")
        return 0

    anchors_map = {path.resolve(): collect_anchors(path) for path in files}

    issues: list[Issue] = []
    issues.extend(check_links(files, anchors_map))
    issues.extend(check_heading_jumps(files))
    issues.extend(check_disallowed_glyphs(files))
    issues.extend(check_legacy_namespace(files))
    issues.extend(check_unlabeled_missing_paths(files))

    if issues:
        issues.sort(key=lambda i: (str(i.file).lower(), i.line, i.code, i.message))
        print(f"Documentation validation failed: {len(issues)} issue(s)")
        for issue in issues:
            rel = issue.file.resolve().relative_to(ROOT).as_posix()
            print(f"- [{issue.code}] {rel}:{issue.line} {issue.message}")
        return 1

    print(f"Documentation validation passed for {len(files)} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
