# Contributing

The full contributing guide, including development setup, code standards, git workflow,
testing requirements, and architecture rules, is maintained at:

[docs/51. contributing.md](docs/51.%20contributing.md)

## Quick Reference

### Prerequisites

- Python 3.13+
- PostgreSQL 14+
- Git

### Quality Gates

All contributions must pass these checks before review:

```bash
ruff check                                          # Linter and import sort
pyright                                             # Type checker (strict mode)
pytest tests/unit tests/quality                     # Unit and quality tests
python data_collector/utilities/validate_docs.py    # Doc validator (if docs changed)
```

### Branch Naming

```
feat/short-description     # New features
fix/short-description      # Bug fixes
docs/short-description     # Documentation changes
refactor/short-description # Code restructuring
test/short-description     # Test additions
```

### Commit Style

Follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
