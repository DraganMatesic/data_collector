import importlib.util
from pathlib import Path
import sys


def load_validate_docs_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "data_collector" / "utilities" / "validate_docs.py"
    spec = importlib.util.spec_from_file_location("validate_docs_module", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load validator module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_legacy_namespace_patterns_are_reported(tmp_path: Path) -> None:
    validator = load_validate_docs_module()
    doc_path = tmp_path / "legacy-patterns.md"
    doc_path.write_text(
        "\n".join(
            [
                "python -m apps.croatia.fina.sample.main",
                "from apps.croatia.fina.sample import run",
                'actor_path="apps.croatia.eoglasna.workers.ocr"',
                "apps/{group}/{parent}/{app_name}/",
                "apps/croatia/fina/sample/",
                "from data_collector.request import Request",
                "from data_collector.utilities.request.main import Request",
                "data_collector/constants/enums/pipeline.py",
                "from data_collector.constants.enums import PipelineStatus",
            ]
        ),
        encoding="utf-8",
    )

    issues = validator.check_legacy_namespace([doc_path])
    codes = {issue.code for issue in issues}

    assert "legacy_apps_module_execution" in codes
    assert "legacy_apps_from_import" in codes
    assert "legacy_actor_path" in codes
    assert "legacy_apps_path_template" in codes
    assert "legacy_apps_path_concrete" in codes
    assert "legacy_request_import" in codes
    assert "legacy_request_main_import" in codes
    assert "legacy_constants_enums_path" in codes
    assert "legacy_constants_enums_import" in codes


def test_legacy_namespace_exceptions_are_allowed(tmp_path: Path) -> None:
    validator = load_validate_docs_module()
    doc_path = tmp_path / "allowed-patterns.md"
    doc_path.write_text(
        "\n".join(
            [
                "Table reference: apps.app",
                "API route: /api/v1/apps/status",
                "Kubernetes manifest: apiVersion: apps/v1",
                "Python file: apps.py",
            ]
        ),
        encoding="utf-8",
    )

    issues = validator.check_legacy_namespace([doc_path])
    assert issues == []
