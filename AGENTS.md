# Data Collector -- Agent Instructions

Enterprise Python framework for data ingestion, ETL, web scraping, and API integration.
Python 3.13+ | PostgreSQL + MSSQL | Proprietary license (Zagreb jurisdiction)

## Project Memory

Shared project memory lives at `memory/MEMORY.md` (symlinked so Claude Code and Codex resolve the same file).
Read it at the start of every session. Do NOT write to it unless the user explicitly instructs you to memorize something.
Memory is for stable facts, conventions, and decisions -- not session notes or temporary state.

## Session Startup

1. Read project memory at `memory/MEMORY.md`
2. If the task references a work package (WP-XX), read `docs/50. roadmap.md` for scope
3. Read the related feature doc for specification (see `docs/1. index.md` for the full index)
4. Read existing code to understand patterns before proposing changes

## Implementation Workflow

1. Read: roadmap (scope), feature doc (spec), existing code (patterns), memory (context)
2. Plan: enter plan mode, present approach for user approval
3. Implement: write code + write tests
4. Validate: run all quality gates (see below)
5. Review: user reviews, then commit + PR on request
6. Memory: update `memory/MEMORY.md` only when user explicitly asks to memorize something

## Quality Gates (mandatory before review)

```bash
ruff check                                          # Linter + import sort
pyright                                             # Type checker (strict mode)
pytest tests/unit tests/quality                     # Unit + quality tests
python data_collector/utilities/validate_docs.py    # Doc validator (if docs changed)
```

All new code must pass all four gates. Do not skip any.

## Code Review Protocol (mandatory before PR)

After quality gates pass, perform the full self-review protocol defined in [`REVIEW.md`](REVIEW.md).

The protocol has seven phases executed in order. Fix issues as they are found. Do not declare code ready until all phases pass:

1. **Scope** -- diff matches request, no drift, no debug artifacts
2. **Conventions** -- naming, imports, ORM patterns, hashing, logging
3. **Data Integrity** -- scope filters, idempotency, guard clauses, type boundaries, API contract, configuration consistency, cross-file impact, workflow state
4. **Error Handling** -- recovery validity, partial failure, resource lifecycle, exception propagation
5. **Concurrency** -- thread safety, race conditions, abort signals
6. **Security** -- injection prevention, credential safety
7. **Test Adequacy** -- code path coverage, edge cases, idempotency tests, mock discipline

This protocol governs self-review only. External reviewers (Codex PR review, human reviewers) operate independently.

## Housekeeping

- Check for pip install artifacts in the project root (files like `=6.0.0`, `=2.1.3` created by malformed `pip install` commands). Delete any such files before committing.

## Code Rules

- Line length: 120 characters
- Quotes: double quotes
- Imports: always top-level, never inline. Pylint enforces this
- Pylint disables: only in `pyproject.toml` under `[tool.pylint."messages_control"]`, never inline `# pylint: disable=`
- No emojis in code or documentation
- No module-level mutable globals and no `global` keyword. Use singleton classes with double-checked locking or `threading.Event` guards
- Google-style docstrings for public APIs
- Variable names must be descriptive and self-explanatory. Abbreviations like `ctx`, `cb`, `evt`, `cfg` are not allowed. Use full words: `context`, `callback`, `event`, `config`. The reader should understand the variable's purpose without checking its assignment
- Prefer editing existing files over creating new ones
- Do not add features, refactor code, or make improvements beyond what was requested

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Modules | `snake_case` | `log_settings.py` |
| Classes | `PascalCase` | `DatabaseSettings` |
| Functions | `snake_case` | `make_hash()` |
| Constants | `UPPER_SNAKE` | `NAMING_CONVENTION` |
| ORM tables | `PascalCase` class, `snake_case` tablename | `class Apps`, `__tablename__ = "apps"` |
| Database columns | `snake_case` | `next_run`, `date_created` |
| DateTime columns | `_date` suffix (preferred) or `date_` prefix | `stabilized_date`, `date_created` (never `_at` suffix) |
| Enum members | `UPPER_SNAKE` | `RunStatus.RUNNING` |

## Architecture Rules

- ORM models extend `Base` from `data_collector.tables.shared`
- Codebook tables must have: `id` (PK), `description`, `sha` (String 64), `archive` (DateTime), `date_created` (DateTime, server_default=func.now())
- SHA hashing via `bulk_hash()` before `merge()`. Never override `compare_key` when `sha` is available
- Settings via Pydantic (`BaseSettings` with `env_prefix`). Never hardcode credentials or connection strings
- Logging via framework `LoggingService`. Never use `print()` or bare `logging.getLogger()` in app code
- Log messages: always use f-strings, never `%`-style positional formatting. structlog stores the raw format string as `msg` without interpolating `%s`/`%d` arguments. Example: `logger.info(f"Stored {filename} ({size} bytes)")` not `logger.info("Stored %s (%d bytes)", filename, size)`
- No direct SQL in application code. Use SQLAlchemy ORM and `Database` class methods
- All database operations must go through the `Database` object, never directly on the session:
  - **SELECT**: `database.query(select(...), session)` -- never `session.execute(select(...))`
  - **INSERT single**: `database.add(instance, session)` -- never `session.add(instance)`
  - **INSERT bulk**: `database.bulk_insert(objects, session)` -- never `session.add_all(objects)`
  - **UPDATE/DELETE DML**: `database.run(update/delete(...), session)` -- never `session.execute(update(...))`
  - **Stored procedures**: `database.execute(sql_text, session)` -- never `session.execute(text(...))`
  - `session.commit()` and `session.rollback()` are allowed directly (session lifecycle, not DB operations)
  - Reason: `Database` methods enable automatic dependency tracking via `AppDbObjects`
- New ORM columns: use `mapped_column()` with `Mapped[T]` annotations (SQLAlchemy 2.x). Existing `Column()` is accepted until migration
- App namespace: `data_collector/<country>/<parent>/<app>/`
- Enum namespace: `data_collector/enums/` (locked)
- Request import: `from data_collector.utilities.request import Request`
- Doc status values: `IMPLEMENTED`, `IN DEVELOPMENT`, `PLANNED`
- Non-existent paths in docs: prefix with `Planned module path:` or `Planned file path:`

## Git Workflow

- Branches: `feat/...`, `fix/...`, `docs/...`, `refactor/...`, `test/...`
- Commits: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`)
- Always read `docs/51. contributing.md` for the full PR process
- Do not commit unless explicitly asked. Do not push unless explicitly asked
- Do not amend commits unless explicitly asked
- Do not use `--no-verify` or `--force` unless explicitly asked
- Append a `Co-Authored-By` trailer to every commit message with your exact model name and vendor email:
  - Format: `Co-Authored-By: <model name> <noreply@vendor-domain>`
  - Examples: `Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>`, `Co-Authored-By: GPT-5.3 Codex <noreply@openai.com>`

### PR Description Template

Use this structure for all pull request descriptions:

```markdown
## Work Package
WP-XX: <title from docs/50. roadmap.md>

## Summary
<bullet list: what this PR does and why>

## Changes
<bullet list: file paths with one-line descriptions>

## Testing
<bullet list: quality gates run and their results>

## Related
<links to spec docs, roadmap, data model>

<agent attribution line -- see below>
```

Append an attribution line at the end of every PR description. Use whichever applies:
- Claude Code: `Generated with [Claude Code](https://claude.com/claude-code)`
- Codex: `Generated with [Codex](https://openai.com/codex/)`

## Key References

| File | Purpose |
|------|---------|
| `docs/50. roadmap.md` | Kanban work packages (WP-01 through WP-15) |
| `docs/51. contributing.md` | Full code standards, git workflow, PR process |
| `docs/1. index.md` | Documentation index (40+ files) |
| `docs/6. tables.md` | ORM table patterns, standard columns |
| `docs/4.2. hashing.md` | Merge/hash patterns, bulk_hash usage |
| `docs/1.2. data-model.md` | Full data model specification |
| `pyproject.toml` | All tool configuration (ruff, pyright, pylint, pytest) |
| `tests/conftest.py` | Test bootstrap, DC_DB_MAIN_* env defaults |

## Tone and Personality

- Concise, technical, formal. Match the tone of existing documentation
- No emojis, no filler phrases, no unnecessary commentary
- State what you will do, then do it. Do not narrate your thought process
- When referencing code, include file path and line number
- Do not give time estimates
