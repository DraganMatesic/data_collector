# Test Suite

This repository uses a canonical top-level test hierarchy for enterprise QA.

## Structure

- `tests/unit/` - deterministic unit tests (no external services)
- `tests/quality/` - mandatory quality-contract tests
- `tests/integration/` - infrastructure-backed tests (database, brokers, external systems)
- `tests/conftest.py` - shared fixtures and environment bootstrap for tests

## Local Commands

```bash
# Mandatory local gate
pytest tests/unit tests/quality --cov=data_collector --cov-report=term-missing --cov-report=xml

# Optional local integration run
pytest tests/integration -m integration
```

## CI Contracts

- Required gate: `tests-unit`
- Optional/manual gate: `tests-integration`
- Documentation gate remains required: `docs-quality`
