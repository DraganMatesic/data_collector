# Code Review Protocol

Self-review protocol for AI agents. Mandatory before declaring code ready for PR.
External reviewers (Codex PR review, human reviewers) operate independently -- this protocol does not constrain them.

After quality gates pass (ruff, pyright, pytest), perform all seven phases in order.
Follow the protocol internally -- do not produce a review report. Fix convention and correctness issues in your own new code as they are found. For issues requiring judgment calls, see Issue Resolution below.
Do not declare code ready until all phases pass. Do not skip phases.

## Phase 1: Scope

Verify the change implements exactly what was requested -- nothing more, nothing less.

1.1. **Diff audit.** Read every changed line in `git diff`. Confirm:
   - No unrelated changes (refactors, formatting outside touched functions, import reordering in untouched files)
   - No debug artifacts: `print()`, `breakpoint()`, `TODO`/`FIXME` in new code, hardcoded IPs/ports/passwords
   - No commented-out code blocks (delete dead code, do not comment it)
   - No pip install artifacts in the project root (files like `=6.0.0`)
   - No new files that duplicate existing functionality (check for existing utilities before creating new ones)
   - No over-engineering: code solves the current problem only. No unnecessary abstraction layers, premature generalization, or speculative future-proofing. Three similar lines of code is better than a premature abstraction

1.2. **Feature completeness.** Cross-reference the diff against the task requirements (WP scope, feature doc, user instructions). Confirm:
   - Every requested behavior is implemented
   - No partial implementations left without explicit user agreement to defer
   - Public API surface matches the specification (method names, parameters, return types)
   - When code changes behavior, spec docs (`docs/`) and docstrings are updated in the same PR. Do not merge code that contradicts its own documentation

## Phase 2: Conventions

Verify all new and modified code follows project standards. These are mechanical checks.

2.1. **Naming.** Verify all new identifiers:
   - Variables use full descriptive words (never `ctx`, `cfg`, `evt`, `cb`, `tmp`, `ret` -- use `context`, `config`, `event`, `callback`)
   - DateTime columns use `_date` suffix (never `_at`). Prefer suffix (`stabilized_date`) over prefix (`date_created`) for new columns
   - ORM classes are `PascalCase`, `__tablename__` is `snake_case`, enum members are `UPPER_SNAKE`
   - Constants are `UPPER_SNAKE`. Module names are `snake_case`
   - Boolean variables/parameters read as predicates (`is_active`, `has_permission`, `should_retry` -- never bare `active`, `retry`)

2.2. **Imports and database access.** Verify:
   - All imports are top-level, never inline (even in `if` branches or functions). Pylint enforces this
   - Import order: stdlib, third-party, `data_collector` framework
   - All DB reads use `database.query(select(...), session)` -- never `session.execute(select(...))`
   - All single inserts use `database.add(instance, session)` -- never `session.add(instance)`
   - All bulk inserts use `database.bulk_insert(objects, session)` -- never `session.add_all(objects)`
   - All DML (update/delete) uses `database.run(statement, session)` -- never `session.execute(update(...))`
   - Raw SQL (stored procedures only) uses `database.execute(text, session)` -- never `session.execute(text(...))`
   - Direct `session.commit()` and `session.rollback()` are allowed (session lifecycle, not DB operations)
   - SQLAlchemy comparisons with `None`, `True`, `False` use `.is_()` and `.is_not()` operators, not `==` and `!=`. Example: `Column.is_(None)` not `Column == None`, `Column.is_not(True)` not `Column != True`. Do not use the deprecated `.isnot()` form
   - All HTTP calls use the `Request` class from `data_collector.utilities.request`. No raw `httpx.Client()`, `httpx.AsyncClient()`, or `requests.get()` in application code (Request provides timeouts, retries, proxy rotation, and metrics)
   - Pydantic settings field names do not repeat the `env_prefix`. With `env_prefix="DC_RABBIT_"`, use field name `host` (env var `DC_RABBIT_HOST`), not `rabbit_host` (env var `DC_RABBIT_RABBIT_HOST`)
   - **Dependency verification**: every imported package exists in `pyproject.toml` dependencies and is spelled correctly. AI agents can fabricate package names or API signatures that do not exist (hallucinated imports). Verify new third-party imports are real, installable, and declared as project dependencies
   - **Historical awareness**: before modifying existing code, check `git log` / `git blame` for why it was written that way. Do not reintroduce previously fixed bugs. If a pattern looks wrong but has survived multiple PRs, investigate before changing it

2.3. **ORM compliance.** For every new or modified table class, verify:
   - Extends `Base` from `data_collector.tables.shared`
   - Codebook tables have all five standard columns: `id` (PK), `description`, `sha` (String 64), `archive` (DateTime), `date_created` (DateTime, `server_default=func.now()`)
   - All `DateTime` columns use `DateTime(timezone=True)`
   - All `ForeignKey` columns specify `ondelete` (`"CASCADE"` or `"RESTRICT"`)
   - All non-obvious columns have `comment=` explaining their purpose
   - Composite primary keys are declared in `__table_args__` via `PrimaryKeyConstraint`
   - Index and constraint names follow the naming convention metadata in `tables/shared.py`
   - New or modified columns use `mapped_column()` with `Mapped[T]` type annotations (SQLAlchemy 2.x modern style). `Mapped[str]` implies NOT NULL; `Mapped[str | None]` implies nullable. Existing `Column()` usage in untouched code is accepted until the dedicated migration

2.4. **Hashing.** For every call to `merge()` or `update_insert()`, verify:
   - ORM records have `sha` computed via `bulk_hash()` before the merge call
   - No `compare_key` parameter override when the table has a `sha` column
   - Hash columns listed in `__hash_columns__` match the business key (the columns that define uniqueness)

2.5. **Logging.** Verify all log statements in changed code:
   - Use f-strings: `logger.info(f"Stored {filename}")` -- never `%`-style: `logger.info("Stored %s", filename)`
   - No `print()` or bare `logging.getLogger()` in application code
   - Sensitive data never appears in log output: no credentials, no proxy URLs with embedded auth, no API keys
   - Exception logging uses `logger.exception()` for stack traces (which includes `exc_info` automatically)

2.6. **Library API currency.** Verify new code does not use deprecated classes, methods, or functions from any dependency:
   - Prefer current APIs over legacy alternatives (e.g., SQLAlchemy: `mapped_column()` over `Column()`, `DeclarativeBase` over `declarative_base()`, `select()` over `session.query()`, `.is_not()` over `.isnot()`; Pydantic: `model_validator` over `root_validator`, `field_validator` over `validator`, `BaseSettings` from `pydantic_settings` not `pydantic`)
   - Check library changelogs and deprecation warnings for any newly added imports
   - If a library emits `DeprecationWarning` at runtime, replace the deprecated call with the recommended alternative
   - Existing deprecated usage in untouched code is accepted -- only enforce on new or modified lines

## Phase 3: Data Integrity

Verify that all data operations are correct, scoped, and produce consistent results.

3.1. **Scope filters.** For every `select()` / `update()` / `delete()` on a scoped entity, verify:
   - App-owned data is filtered by `app_id` (e.g., `StorageFiles`, `FunctionLog`, `AppFunctions`)
   - Backend-scoped data is filtered by `location` (e.g., `StorageLocations`, backend-specific queries)
   - Run-scoped data is filtered by `runtime` or `runtime_id`
   - No query may return, modify, or delete data belonging to a different scope
   - Methods with optional filter parameters behave correctly when the filter is `None` (verify whether `None` means "all" or "none" and confirm that is intentional)

3.2. **Idempotency.** Verify that running the same operation twice produces identical final state:
   - INSERT-or-skip logic handles "already exists" without raising (catch `IntegrityError` or use `merge()`)
   - UPDATE logic handles "already updated" without side effects (compare `sha` before writing)
   - File operations handle "file already present" gracefully (check existence before write, or overwrite deterministically)
   - DELETE logic handles "already deleted" without raising (check rowcount or use `delete().where()` which succeeds on zero rows)

3.3. **Guard clauses.** Verify:
   - Operations that take source/target parameters (copy, move, transfer) reject `source == target` early with a clear error
   - Methods with optional parameters work correctly at their boundaries: `None` filters, empty lists, zero counts
   - Enum parameters are validated against known members before use (not after a DB query returns no results)

3.4. **Type boundaries.** At every boundary where values cross systems, verify:
   - Settings to code: Pydantic field types enforce correctness (`int`, not `str` that happens to contain digits). Add `field_validator` for values with constraints (e.g., retention days must be positive)
   - DB to code: nullable columns are handled (check for `None` before using). Integer columns that could be zero are not treated as falsy
   - JSON/API to ORM: deserialized dicts are validated before constructing ORM objects. Enum values from external sources use `Enum(value)` with error handling, not direct attribute access
   - Return types match signatures: if the signature says `-> Path`, never return `str`. If it says `-> T | None`, every code path (including error recovery) returns one of those two types

3.5. **API contract compliance.** For every public method, verify that all declared parameters are honored in every code path:
   - Parameters passed by the caller are never silently ignored (e.g., a `retention_category` parameter must be applied even when dedup finds an existing row, or when a move reuses a target row)
   - When a code path intentionally skips a parameter (e.g., `extension` is irrelevant for dedup hits), the docstring documents this explicitly
   - Default parameter values match the documented behavior and settings (e.g., if settings declare a default retention category, the method signature uses it)
   - Return values are consistent across all code paths: a method that returns a file path must return the *correct* path in every branch (including error recovery, dedup, and race-winner resolution)

3.6. **Configuration consistency.** When the same logical setting can come from multiple sources (environment, DB registry, in-memory cache), verify:
   - The precedence is explicit and documented (e.g., DB registry overrides env defaults)
   - Long-lived processes do not cache configuration indefinitely -- codebook/config queries are re-executed or caches have TTL/invalidation
   - Auto-include/fallback logic respects all state flags (e.g., do not auto-add a backend when it exists in the registry but is marked inactive)
   - Default constructors check the registry before falling back to hardcoded settings

3.7. **Cross-file impact.** When a shared interface changes (function signature, ORM model column, enum value, settings field), verify:
   - All callers and importers across the codebase still work. Use grep/search to find every usage of the changed interface
   - Downstream consumers of changed output (other apps, APIs, reports, Dramatiq workers) still receive the expected format
   - Changes to base classes (`BaseScraper`, `BaseNotifier`, `BaseDBConnector`) do not break any subclass that overrides the changed method

3.8. **Workflow state validation.** For multi-step processes (ETL pipelines, scraper lifecycle, task processing), verify:
   - State transitions follow the documented order (e.g., BaseScraper: `prepare_list` -> `collect` -> `store` -> `cleanup`; Dramatiq: PENDING -> RUNNING -> SUCCESS/FAILED)
   - Invalid state transitions are rejected (e.g., cannot `store()` before `collect()`, cannot mark RUNNING after SUCCESS)
   - Each step validates its preconditions (e.g., `store()` checks that collected data exists, `cleanup()` does not fail if `collect()` was skipped due to abort)

## Phase 4: Error Handling

Verify that every failure path recovers correctly and leaves the system in a consistent state.

4.1. **Error recovery validity.** After every `except` block that recovers (rollback, retry, skip), verify all three:
   - **(a) Return value validity**: The value returned after recovery references existing state. A rolled-back insert means any path/ID derived from that insert is invalid -- re-query or return a sentinel
   - **(b) No orphaned resources**: Files without corresponding DB rows, DB rows without corresponding files, partially written state across two backends. If the operation created resources before failing, clean them up in the `except` block
   - **(c) Exception classification accuracy**: The `except` block catches the correct exception type. Distinguish: FK violations (`ForeignKeyViolation`) from unique constraint races (`UniqueViolation`), timeouts from connection errors, HTTP 4xx from 5xx. Do not catch broad `Exception` when a specific subclass is available

4.2. **Partial failure.** For operations spanning two resources (DB + filesystem, DB + DB, two backends), verify:
   - Failure after the first write cleans up the partial state (use savepoints for DB, explicit delete for files)
   - If cleanup itself can fail, the error is logged but does not mask the original exception
   - Cross-resource ordering minimizes the inconsistency window: prefer DB-last for creates (so the DB row is only committed after the file/external resource exists), prefer DB-first for deletes (so the DB row is removed before the file, allowing retry from DB state)
   - The method's docstring documents atomicity guarantees (or lack thereof)

4.3. **Resource lifecycle.** For operations involving physical resources (files, network connections, external API calls), verify:
   - **Write ordering**: irreversible physical operations (file delete, network send) happen AFTER the durable state change (DB commit/flush) succeeds, not before. If the physical operation must happen first (write file before insert), the error handler must clean up the physical resource on DB failure
   - **Physical existence verification**: before returning a path or URL to a caller, verify the resource actually exists. Do not trust DB state alone -- if a DB row says a file exists, confirm with `backend.exists()` or equivalent. If the file is missing, either re-create it or raise an error
   - **Shared resource safety**: before deleting a physical resource (file, queue message), verify no other DB row references it. In race conditions where two writers create the same physical file, the loser's cleanup must not delete a file the winner's row points to
   - **Cleanup on failure**: if an operation writes a file and then the DB insert fails, the orphaned file must be cleaned up. If cleanup itself fails, log the error but do not mask the original exception
   - **Subprocess management**: processes created with `subprocess.PIPE` must have their stdout/stderr consumed (or use `subprocess.DEVNULL` when output is not needed). Unconsumed pipes deadlock the child when the OS buffer fills. Close file handles and release connections in all exit paths

4.4. **Exception propagation.** Verify:
   - Exceptions are never silently swallowed (every `except` block either re-raises, raises a different exception, logs and returns a sentinel, or has a clear comment explaining why swallowing is correct)
   - Retry logic has a bounded retry count and uses exponential backoff with jitter
   - After retry exhaustion, the original exception is raised (not a generic "retry failed" error)
   - Error context is preserved: use `raise NewError(...) from original` to chain exceptions, never bare `raise NewError(...)` that discards the traceback
   - **Finally block safety**: all variables referenced in `finally` or cleanup blocks must be initialized before the `try` block (use `variable = None` before `try`, then `if variable is not None: variable.cleanup()` in `finally`). An exception during initialization leaves variables unbound, causing `UnboundLocalError` in `finally` that masks the real failure

## Phase 5: Concurrency

Verify that shared state is protected and parallel execution is safe.

5.1. **Thread safety.** Identify every shared mutable state in changed code and verify:
   - Singleton classes use double-checked locking pattern (`if _instance is None: with _lock: if _instance is None:`)
   - Instance-level state accessed from multiple threads is protected by `threading.Lock` with `with self._lock:` for short critical sections
   - No module-level mutable globals. No `global` keyword
   - `ContextVar` is used for thread-local state that must propagate to child threads (not `threading.local()`)
   - **Async/sync boundary**: decorators and wrappers around functions must handle both sync and async callables. If the decorated function is `async def`, the wrapper must `await` the coroutine -- calling it synchronously returns a coroutine object without executing it, and the wrapper's `finally` block runs before the coroutine body

5.2. **Race conditions.** Verify:
   - Unique constraint violations from concurrent inserts (`IntegrityError`) are caught, the existing row is queried to find the race winner, and the winner's data is used (not the failed insert's)
   - Check-then-act patterns (query existence, then insert) are protected by either: database-level constraints (preferred) or application-level locks
   - Long-running loops and batch operations check abort signals (`threading.Event.is_set()`)
   - Worker thread exceptions are logged and do not crash the parent thread silently

## Phase 6: Security

Verify that the code is safe from injection, credential leaks, and unsafe operations.

6.1. **Injection prevention.** Verify:
   - No SQL string concatenation or f-string interpolation in query construction -- all queries via SQLAlchemy ORM with parameterized where clauses
   - XML from external sources uses `defusedxml.ElementTree`, never raw `lxml.etree.fromstring()` or `xml.etree.ElementTree`
   - No `shell=True` in subprocess calls -- use `subprocess.Popen([sys.executable, "-m", ...])`
   - File paths from external input are validated/sanitized (no path traversal via `../`)
   - HTML/text from external sources is never rendered or executed without sanitization
   - No `eval()`, `exec()`, `compile()`, or `__import__()` with data from external sources (scraped content, OCR text, API responses, user input). This becomes critical for WP-14 (PDF/OCR), WP-16 (NER), WP-17 (API)
   - Files from external sources (uploads, downloads, scraped content) are validated by reading file headers/magic bytes, not by trusting the file extension. A `.pdf` extension does not guarantee PDF content

6.2. **Credential safety.** Verify:
   - Credentials and connection strings come from Pydantic `BaseSettings` with `env_prefix`, never hardcoded
   - No secrets in log output, exception messages, or error responses
   - Proxy URLs are stripped of embedded credentials before logging or metrics recording
   - API keys, tokens, and passwords are never included in URLs, query parameters, or stack traces
   - No credentials committed to version control (check `.env` files, config files, test fixtures)

## Phase 7: Test Adequacy

Verify that tests cover the implemented behavior and its failure modes.

7.1. **Coverage of new code paths.** Verify:
   - Every public method added or modified has at least one unit test exercising the happy path
   - Every error recovery path (the `except` branches verified in Phase 4) has a test that triggers it -- mock the dependency to raise the expected exception and assert the recovery behavior
   - Every branch condition (if/elif/else) in new code has at least one test per branch
   - **Test sensitivity**: ask "would this test still pass if I introduced a bug in the code under test?" Tests that assert on mocked return values or trivially pass regardless of implementation are not testing anything. Tests must assert on observable behavior and outcomes, not implementation details

7.2. **Edge cases.** Verify tests exist for:
   - Empty input: empty list, empty string, empty dict
   - None/null values: `None` where a value is optional, null DB columns
   - Duplicate input: same record twice in a batch, same file stored twice
   - Single-element input: list with one item (boundary between scalar and batch logic)
   - Boundary values: 0 (not falsy when valid), negative numbers (if applicable), max-length strings
   - For ORM operations: `sha` collision (two different records with the same hash), missing FK target (insert with nonexistent parent)

7.3. **Idempotency test.** If the change includes a stateful operation (DB write, file write, external call):
   - At least one test runs the operation twice with the same input
   - The test asserts identical final state after the second run (same row count, same file content, no duplicates)
   - The test asserts no exceptions on the second run

7.4. **Mock discipline.** Verify:
   - Mocks replace external dependencies (DB, HTTP, filesystem), not internal business logic
   - `database.query` / `database.add` / `database.run` are mocked at the Database boundary, not at the session level
   - Session lifecycle methods (`commit`, `rollback`) are included in assertions -- verify that commit is called on success and rollback on failure, not mocked away
   - HTTP mocks (`respx`) return realistic responses, not empty stubs. Include status codes, headers, and body structure matching the real API
   - Mocks do not mask the behavior under test. If the test is for error handling, the mock must raise a realistic exception (e.g., `IntegrityError` with the correct constraint name), not a generic `Exception`

7.5. **Examples.** If examples exist for the changed module (under `examples/`):
   - Run them to verify they work with the changed code
   - If examples create persistent state (DB rows, files), run twice to verify idempotency
   - If examples were not updated but the API they use changed, inform the user and update them on approval

## Issue Resolution

- **Fix immediately** if the issue is within the scope of the current change and the fix is unambiguous (e.g., missing scope filter, wrong exception type, broken return value)
- **Inform the user** before acting if:
  - The issue exists in unchanged code or is outside the current scope
  - The fix requires a design decision (multiple valid approaches)
  - The fix would expand the PR scope significantly
  - The fix touches shared interfaces that other modules depend on
  Present: (1) what the issue is, (2) the risk if left unfixed, (3) whether it can be fixed within the current work package without destabilizing it. Let the user decide whether to fix now or document as a Known Limitation in the PR description. Do not silently defer
