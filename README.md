# Data Collector

**Enterprise Python framework for data ingestion, ETL, web scraping, and operational orchestration.**

<p>
  <img src="https://img.shields.io/badge/python-3.13%2B-blue?logo=python&logoColor=white" alt="Python 3.13+">
  <img src="https://img.shields.io/badge/SQLAlchemy-2.x-orange?logo=sqlalchemy" alt="SQLAlchemy 2.x">
  <img src="https://img.shields.io/badge/databases-PostgreSQL%20%7C%20MSSQL-336791?logo=postgresql&logoColor=white" alt="PostgreSQL | MSSQL">
  <img src="https://img.shields.io/badge/license-proprietary-red" alt="License">
  <img src="https://img.shields.io/badge/tests-1142%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/docs-42%20files-blue" alt="Documentation">
  <img src="https://img.shields.io/badge/code%20style-ruff-000000" alt="Ruff">
</p>

---

## What is Data Collector?

Data Collector is a production-grade Python framework for building, orchestrating, and monitoring data ingestion pipelines. It provides a unified architecture for collecting data from web sources, REST/SOAP APIs, files, and OCR pipelines -- then transforming and loading it into relational databases.

Built from the ground up, drawing on over 10 years of professional experience in data engineering -- building and operating large-scale data collection systems, ETL pipelines, and integration platforms across multiple European markets.

See the full [Product Overview](docs/0.%20product-overview.md) for vision, scope, and design philosophy.

---

## Key Features

### Core Framework

| Feature | Description |
|---------|-------------|
| **Multi-database support** | PostgreSQL and MSSQL with pluggable `BaseDBConnector` pattern |
| **Change detection** | SHA-based deterministic hashing via `merge()`, `make_hash()`, `bulk_hash()` |
| **App lifecycle tracking** | Apps, AppGroups, AppParents, runtime metrics, database dependency mapping |
| **Structured logging** | Queue-based structlog pipeline with DatabaseHandler and Splunk HEC sinks |
| **Function profiling** | `@fun_watch` decorator -- per-function timing, error tracking, FunctionLog table |
| **Settings management** | Pydantic v2 settings with env var binding and validation |
| **Secret management** | AES-256-CBC encrypted secrets with PBKDF2 key derivation |
| **Schema deployment** | `Deploy` class for table creation, codebook seeding, and migrations |

### Data Collection

| Feature | Description |
|---------|-------------|
| **HTTP client** | Centralized `Request` class -- httpx (sync + async), sessions, retries, proxy, SOAP |
| **BaseScraper** | Lifecycle pattern: `prepare_list` -> `collect` -> `store` -> `cleanup` |
| **ThreadedScraper** | Parallel collection with progress tracking and abort signals |
| **Proxy management** | ProxyProvider interface, atomic IP reservation, rotation, ban detection, blacklist |
| **Captcha solving** | AntiCaptcha integration with balance checking and solve metrics |

### Orchestration and Messaging

| Feature | Description |
|---------|-------------|
| **Application Manager** | Scheduler, process lifecycle, command dispatch (start/stop/restart) |
| **RabbitMQ messaging** | Connection management, publish/consume, broadcast patterns via pika |
| **Dramatiq workers** | Distributed task processing with topic exchange routing and retry middleware |
| **Notifications** | Pluggable dispatch -- Telegram, Slack, Discord, Email, Webhooks |
| **Scaffold CLI** | Generate new apps with correct structure, boilerplate, and DB registration |

### Planned

| Feature | Description |
|---------|-------------|
| SOAP/XML client | WSDL-based client for government and enterprise web services (zeep) |
| PDF and OCR | Text extraction (pdfplumber), image OCR (Tesseract, PaddleOCR) |
| Deep Learning NER | Named entity recognition for structured data extraction (spaCy, GLiNER) |
| Data quality | Validation rules, anomaly detection, audit trails |

---

## Architecture

```
APPLICATION LAYER
  Developer-built apps: scrapers, API consumers, file processors, OCR pipelines

DOMAIN / CORE LAYER
  merge() + hash-based change detection       App lifecycle (Apps, Runtime, AppDbObjects)
  make_hash / bulk_hash / obj_diff             Dependency tracking
  SHAHashableMixin                             Enum codebook management

INFRASTRUCTURE LAYER
  Database            Request class            Messaging
  PostgreSQL, MSSQL   httpx (sync + async)     RabbitMQ (pika)
  (pluggable)         sessions, retries        Dramatiq workers
  SQLAlchemy 2.x      proxy support            Topic exchange routing

ADAPTERS / INTEGRATION LAYER
  Notifications       OCR Engines              Captcha Solvers
  Telegram, Slack     Tesseract, PaddleOCR     AntiCaptcha
  Discord, Email      PDF (pdfplumber)         SOAP/XML (zeep)
  Webhooks

PLATFORM / CROSS-CUTTING
  Logging (structlog + queue)    Settings (Pydantic)    Secret Loader (AES-256)
  @fun_watch profiling           Scheduler (croniter)   Deploy (schema management)
```

See [Architecture](docs/1.1.%20architecture.md) and [Data Model](docs/1.2.%20data-model.md) for the full specification.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.13+ |
| ORM | SQLAlchemy 2.x |
| Configuration | Pydantic v2 / pydantic-settings |
| Databases | PostgreSQL (psycopg2), MSSQL (pyodbc) |
| HTTP | httpx (sync + async), BeautifulSoup4, lxml |
| Logging | structlog with queue-based pipeline |
| Messaging | RabbitMQ (pika), Dramatiq |
| Task processing | Dramatiq + RabbitMQ with topic exchange routing |
| Testing | pytest, pytest-cov, pytest-asyncio, respx |
| Code quality | ruff, pyright (strict mode) |

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/DraganMatesic/data_collector.git
cd data_collector

# Install dependencies
pip install -r requirements.txt

# Run an example scraper
python -m data_collector.examples.scraping.books.main
```

See [Getting Started](docs/30.%20getting-started.md) and [First App Tutorial](docs/31.%20first-app.md) for a full walkthrough.

---

## Project Status

| Milestone | Status | Scope |
|-----------|--------|-------|
| **v0.1.0** | Complete | Database, Settings, Logging, Secrets, Hashing, ORM, Deploy, test infrastructure |
| **v0.2.0** | In progress | Enums, Request, @fun_watch, BaseScraper, Proxy, Notifications, Orchestration, Dramatiq |
| **v1.0.0** | Planned | SOAP/XML, PDF/OCR, NER, Data Quality, Dashboard, full production readiness |

See [Roadmap](docs/50.%20roadmap.md) for the full work package breakdown.

---

## Documentation

42 specification documents covering architecture, API reference, developer guides, and operational procedures.

| Category | Documents |
|----------|-----------|
| **Core reference** | [Architecture](docs/1.1.%20architecture.md), [Data Model](docs/1.2.%20data-model.md), [Database](docs/4.1.%20database.md), [Hashing](docs/4.2.%20hashing.md) |
| **Data collection** | [Request](docs/4.4.%20request.md), [Scraping](docs/11.%20scraping.md), [Proxy](docs/15.%20proxy.md), [Captcha](docs/12.%20captcha.md) |
| **Orchestration** | [Manager](docs/10.%20orchestration.md), [Dramatiq](docs/17.1.%20dramatiq.md), [Notifications](docs/18.%20notifications.md) |
| **Developer guides** | [Getting Started](docs/30.%20getting-started.md), [First App](docs/31.%20first-app.md), [App Patterns](docs/32.%20app-patterns.md) |

Full index: [docs/1. index.md](docs/1.%20index.md)

---

## Testing

```bash
# Run mandatory test suite (unit + quality)
pytest tests/unit tests/quality

# Run with coverage
pytest tests/unit tests/quality --cov=data_collector --cov-report=term-missing
```

---

## License

Proprietary. See [LICENSE.txt](LICENSE.txt) for full terms. No use without a commercial license.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/51. contributing.md](docs/51.%20contributing.md) for code standards, git workflow, and PR process.
