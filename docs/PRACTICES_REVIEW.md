# Project Practices Review

Review of the breweries-api-data-pipeline against **Python**, **Software Engineering & Design**, and **Airflow** good practices. Based on the codebase as of the review date.

---

## 1. Python Good Practices

### What’s in place

- **Type hints**: Used consistently on function parameters and returns (`list[dict[str, Any]]`, `tuple[Path, int]`, `str | Path`, `Exception | None`). Improves readability and enables static checking.
- **Docstrings**: Modules have one-line descriptions; public functions have Args/Returns/Raises. Custom exceptions are documented.
- **Custom exceptions**: `APIError` and `ValidationError` with structured attributes (e.g. `status_code`, `invalid_count`, `sample_invalid`) support better error handling and debugging.
- **Standard library and common idioms**: `pathlib.Path`, `tempfile`, `json`, `logging` used appropriately. Atomic write in Bronze (temp file + `os.replace`) is correct.
- **Single responsibility**: Each module has a clear role (client, loader, transformer, aggregator).
- **Pytest**: Tests use `tmp_path`, `responses` for HTTP, and clear test names. `pyproject.toml` sets `testpaths` and `pythonpath` so tests run from repo root.

### Gaps / improvements

- **No linter or formatter**: No Ruff, Black, Flake8, or mypy. Adding at least Ruff (lint + format) and optionally mypy would enforce style and catch type issues.
- **No mypy (or strict typing)**: Type hints are present but not checked. A `[tool.mypy]` section and CI check would help.
- **Logging in API client**: `src/api/client.py` does not log requests, retries, or failures. Adding `logging.getLogger(__name__)` and log lines for start/retry/success/failure would help operations.
- **Magic numbers**: Retry counts, timeouts, and count tolerance are passed as parameters (good) but default values are hard-coded. Consider centralising in a small config module or env for easier tuning.
- **Gold test and Delta write**: `test_gold_aggregator.py` uses `write_deltalake(..., df, ...)` with a pandas DataFrame. Silver uses `pa.Table.from_pandas` before `write_deltalake` for compatibility. For consistency and to avoid future breakage, Gold tests could use the same PyArrow table pattern where they create Delta fixtures.

---

## 2. Software Engineering & Design

### What’s in place

- **Layered architecture**: Clear separation: API client → Bronze → Silver → Gold. Each layer has a single entry point and writes to the next layer’s paths.
- **Medallion model**: Bronze (raw, validated), Silver (curated, partitioned), Gold (aggregated) is implemented and documented in ARCHITECTURE.md.
- **No heavy DAG coupling**: Pipeline logic lives in `src/`; the DAG only orchestrates and passes paths. Good testability without Airflow.
- **Configuration via Airflow**: `data_lake_root` and `alert_email_to` use Variables, so config is not hard-coded in the DAG.
- **Documentation**: PLAN, ARCHITECTURE, DECISIONS, EMAIL_SETUP, and AI_INTERACTIONS provide context and history.
- **Dependency management**: Both `requirements.txt` and `pyproject.toml` list dependencies with version lower bounds. Optional dev deps in pyproject.

### Gaps / improvements

- **No shared config/constants**: `REQUIRED_FIELDS`, `PARTITION_KEYS`, `COLUMNS_TO_KEEP` are defined in Bronze/Silver. A small `src/config.py` or `src/constants.py` could hold schema and pipeline constants to avoid drift (e.g. Gold expecting columns that Silver drops).
- **No utils package**: No `src/utils` for cross-cutting helpers (e.g. path resolution, safe JSON read). Acceptable at current size; add if duplication appears.
- **conftest.py empty**: `tests/conftest.py` has only a docstring. Shared fixtures (e.g. sample brewery records, temp Bronze path) could live there to reduce duplication and keep tests consistent.
- **API base URL**: API client uses a default URL but no env variable. For different environments (e.g. staging), consider `os.getenv("OPEN_BREWERY_API_URL", default)`.
- **Version pinning**: Requirements use minimum versions (`>=`) rather than upper bounds. For reproducible builds, consider lock files (e.g. `pip-tools`) or at least documenting “tested with” versions.

---

## 3. Airflow Good Practices

### What’s in place

- **Task flow (decorators)**: Uses `@dag` and `@task`; linear dependency is clear: fetch → load_bronze → transform_silver → aggregate_gold. No redundant dependencies.
- **Small XCom payloads**: Only paths and small dicts (`{"path": ..., "total": ...}`) are passed between tasks. Large data is written to staging/Bronze and passed by path, avoiding large XCom payloads.
- **Imports inside tasks**: `src` is imported inside task functions, so the DAG file parses quickly and worker imports get the correct `PYTHONPATH` (e.g. in Docker). Reduces parse-time errors and keeps DAG lightweight.
- **Retries and alerting**: `default_args` sets `retries=2`, `retry_delay=5 min`, and optional `email_on_failure` when `alert_email_to` is set. Failures are retried and can be notified.
- **Schedule and catchup**: `schedule="@daily"`, `catchup=False` avoids backfilling and is appropriate for a daily snapshot pipeline.
- **Tags**: `tags=["breweries", "medallion"]` improve filtering in the UI.
- **Variables for config**: Data lake root and email are read from Variables with safe defaults, so the DAG works out of the box and can be overridden per environment.
- **Docker and executor**: Dockerfile sets `PYTHONPATH=/opt/airflow`; compose uses LocalExecutor, dedicated Postgres, healthchecks, and volume mounts for dags/plugins/data/logs. Good for local and assessment use.

### Gaps / improvements

- **Task IDs**: Rely on default task IDs from function names (`fetch_breweries_task`, etc.). Explicit `task_id` in decorators would make IDs stable if functions are renamed and would align with common Airflow style.
- **Idempotency**: Tasks are effectively idempotent (overwrite or run-scoped paths), but this is not documented in the DAG docstring or task docs. A short note in the DAG or in ARCHITECTURE would help operators.
- **Variable.get at module load**: `DATA_LAKE_ROOT = _get_data_lake_root()` is evaluated when the DAG file is loaded. If the Variable is changed in the UI, a DAG reload is needed. Acceptable; for dynamic-per-run config, the value could be read inside the first task instead.
- **No task grouping**: All tasks are at the same level. For longer pipelines, `@task_group` could group “ingest”, “transform”, “aggregate” for clearer UI and organisation.
- **Staging cleanup**: Fetch writes to `staging/run_id={run_id}/`. There is no task to delete staging after Bronze load. Consider a cleanup task or documenting that staging is left for debugging.
- **Sensors / deferral**: No sensors or deferred operators. Not required for this API; worth considering only if you add “wait for API ready” or external triggers later.

---

## 4. Summary Table

| Area              | Status   | Notes                                                                 |
|-------------------|----------|-----------------------------------------------------------------------|
| Type hints        | Good     | Consistent; add mypy to enforce                                      |
| Docstrings        | Good     | Present on public API and exceptions                                  |
| Error handling    | Good     | Custom exceptions, retries, atomic writes                             |
| Logging           | Partial  | Bronze/Silver/Gold log; API client does not                           |
| Linting/formatting| Missing  | Add Ruff (and optionally Black)                                      |
| Layered design    | Good     | Clear medallion and separation of concerns                           |
| Config            | Good     | Airflow Variables; consider env for API URL and shared constants      |
| Tests             | Good     | Pytest, one module per layer; conftest could hold shared fixtures     |
| DAG structure     | Good     | Linear flow, small XCom, retries, alerting                           |
| Idempotency       | Good     | Implicit; could be documented                                        |
| Task IDs / groups | Minor    | Explicit task_id and optional task_group would polish                |

---

## 5. Recommended Next Steps (priority order)

1. **Add Ruff** (or Black + Flake8) and run in CI to keep style consistent.
2. **Add logging** in the API client (request start, retries, success/failure).
3. **Optional: add mypy** with strict or moderate settings and fix any reported issues.
4. **Centralise schema/constants** (e.g. `src/config.py`) for required fields, partition keys, and column lists.
5. **Populate conftest.py** with shared fixtures (e.g. valid brewery record, minimal Bronze path) used by multiple tests.
6. **Document idempotency** in the DAG module docstring or ARCHITECTURE.
7. **Consider explicit task_id** and, if the pipeline grows, task groups.

This review reflects good practices for Python, design, and Airflow; the main improvements are tooling (lint/format/type-check), a bit more logging and config centralisation, and small DAG/documentation polish.
