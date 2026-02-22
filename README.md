# Data Collector Framework

## Overview

Data Collector is an enterprise-oriented Python framework for data ingestion, ETL, web scraping, API integration, and operational orchestration.

The repository includes both current implementation documentation and planned architecture documentation. Planned components are explicitly labeled in docs.

## Documentation Entry Point

Primary documentation index:

- [docs/1. index.md](docs/1.%20index.md)
- Documentation governance and validation rules are defined in [docs/51. contributing.md](docs/51.%20contributing.md).

## Requirements

- Python 3.13+

Install dependencies:

```bash
pip install -r requirements.txt
```

Install development dependencies:

```bash
pip install -e ".[dev]"
```

## Testing

Canonical automated tests are stored at repository-level `tests/`:

- `tests/unit/` (mandatory)
- `tests/quality/` (mandatory)
- `tests/integration/` (optional/manual)

Run mandatory local gate:

```bash
pytest tests/unit tests/quality --cov=data_collector --cov-report=term-missing --cov-report=xml
```

Run optional integration suite:

```bash
pytest tests/integration -m integration
```
